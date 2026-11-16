# 0006 — TradeManager in Backtest, Synthetic Brackets, `/v1/trade-plan`, Pattern Scan Wiring

**Status**: Proposed

## 0. Executive Summary & Objective

Run `tantra-core`'s `TradeManager` identically in the backtest engine and live runtime; provide safe runtime-managed synthetic OCO/brackets
until exchange/OMS bracket support exists; expose stateless `POST /v1/trade-plan`; and wire `tantra-market-signals::PatternAnalyzer` into scans, backtest, and agent ports.
Roadmap: [root plan 0008](../../../docs/plans/0008-trade-signal-generator.md).

## 1. Context & Gaps Covered

- `crates/tantra-runtime-backtest/src/engine.rs` (L615–811):
  - `StopLoss`/`StopLimit` trigger on current price / bar open with gap-through and conservative stop-first ordering, but **no intrabar high/low evaluation, trailing, targets/partials, or brackets**.
  - Line 647 computes sizing `equity` from `Money::inr(self.account.cash.amount)` — ignoring open mark-to-market positions (breaks multi-position swing books like `qullamaggie`) and hardcoding `INR`.
  - Lines 633–666 blindly overwrite `order.quantity` when `self.position_sizer` is set, which would corrupt `TradePlanner`-sized entries and `PartialExit` quantities.
  - Line 865–871 maps only Indian `Exchange` variants for tax calculation (needs multi-market support before US `qullamaggie` runs).
- `crates/tantra-runtime-strategy/src/intent.rs`: doc says trailing unsupported, yet `tests/golden.rs` references `trailing_stop_amount_sell` (stale).
- `crates/tantra-runtime-actors/src/sizing.rs`: `PositionSizingActor`, `SimplePositionSizer`.
- Upstream: [`tantra-core` plan 0005](../../../tantra-core/docs/plans/0005-trade-planner-and-manager.md), [`tantra-market` plan 0002](../../../tantra-market/docs/plans/0002-chart-pattern-analyzer.md), [`tantra-risk` gap 0005](../../../tantra-risk/docs/gaps/0005-stop-protection-risk-validation.md), [`tantra-exec` gap 0003](../../../tantra-exec/docs/gaps/0003-bracket-order-slt-sl-execution.md), [`tantra-ledger` gap 0012](../../../tantra-ledger/docs/gaps/0012-bracket-order-oms-lifecycle.md). Sibling: plan [0007](0007-group-screener-alerts.md).

## 2. Component Specifications & Phases

- **P1 Backtest**:
  - Feed bars to `TradeManager` (`on_bar` for intrabar H/L stop/target resolution with conservative stop-first default; `on_session_close` for `CloseMa` / session-based `AfterSessions` / `time_stop`, executing close-triggered exits on the next bar open).
  - Fix `engine.rs` equity sizing bug (use mark-to-market `current_equity()` preserving account currency) and bypass the lines 633–666 order-quantity overwrite for `TradeManager`-owned orders.
  - Invoke `TradeManager::on_entry_fill(fill_price, fill_qty, ts)` once entry executes (after gap/L2 slippage) so `1R`, target ladder levels, and breakeven re-anchor accurately (validated by `RiskEngine` against `max_loss`).
  - Fix/remove stale `trailing_stop_amount_sell` golden.
- **P2 Journal**: Record R-multiples (anchored to actual entry fill), pattern kind/stage (when pattern-driven), and per-leg exits in the trade journal (`tantra-ledger-performance` store).
- **P3 Live synthetic OCO (`TradeManagerActor`)**:
  - Explicit per-position state machine (`PendingEntry` → `Active` ↔ `PartialExitInFlight` / `StopAmendPending` → `Closing`) driven through `RiskEngine` → `ExecutionActor` using deterministic idempotent `client_order_id`s.
  - **Over-exit / short-flip prevention**: whenever `PartialExit` fills `q`, atomically resize (`modify`/`cancel-replace`) the resting SL order down to `remaining_qty` (never leave full-size SL resting against a reduced position) and guard SL modification windows via `StopAmendPending` so neither unprotected gaps nor double-fills go untracked.
  - Bounded channels with explicit `BackpressurePolicy`; persisted state in `tantra-runtime-persistence` with startup reconciliation against broker open orders before re-arming. Retired when `tantra-exec` gap 0003 / `tantra-ledger` gap 0012 close.
- **P4 REST**: Stateless planning/preview endpoint `POST /v1/trade-plan` (preset/spec + overrides + symbol + optional pattern toggle → `TradePlan`), `Caller`-scoped, body-capped, OpenAPI derived from `tantra-core` schemas.
- **P5 Agent & Pattern Scan Wiring**:
  - Implement `tantra-agent` `TradePlanPort` and `PatternPort` here (agent rules R1/R4: market-data wiring lives in runtime, never in `tantra-agent-mcp`), sourcing ATR/ADR%/Chandelier/MAs/ADV20 from `tantra-market` and calling `tantra-market-signals::PatternAnalyzer` (`ChartPattern`).
  - Wire `PatternAnalyzer` into runtime signal scanning and backtest bar pipelines so `ChartPattern` (`FormingArmed` and `BreakoutConfirmed`) flows cleanly into `PlannerInputs.pattern`.

## 3. Test Plan (TDD)

`crates/*/tests/` only:
- Backtest engine tests: same-bar stop+target, gap-through, `on_entry_fill` R-re-anchoring under slippage, trailing, ladder partials, breakeven, time stop, `on_session_close` next-bar execution (no look-ahead), multi-position MTM equity sizing;
- Pattern-driven backtest test (`FormingArmed` VCP/flag resting stop entry + `MeasuredMove` partial + Chandelier runner);
- Live actor tests: partial-exit + atomic SL qty shrink (assert zero short-flip on immediate stop trigger), SL modify concurrency, bounded channel backpressure, restart + open-order reconciliation test;
- Golden replay vs `tantra-core` vectors; REST auth/tenant negative tests; every plan-derived order passes `RiskEngine`.
Gates: `make check`, targeted `cargo test --profile prod`, `make prod`.

## 4. Dependencies & Tier Notes

L3 over L0–L2; no concrete broker in strategy crates (inject `Arc<dyn BrokerExecutor>`). No `Utc::now()` in domain paths; clock injected at actor boundary.

## 5. Decisions & Open Questions

- Live `CloseMa` evaluation defaults to **session bar close only** (matching backtest `on_session_close`).
- Confirm non-Indian exchange tax/fee zero-cost or broker-specific schedule in `backtest/engine.rs` for US equities (`qullamaggie`).
