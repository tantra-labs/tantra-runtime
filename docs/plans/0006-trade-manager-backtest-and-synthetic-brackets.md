# 0006 — TradeManager in Backtest, Synthetic Brackets, `/v1/trade-plan`

**Status**: Proposed

## 0. Executive Summary & Objective

Run `tantra-core`'s `TradeManager` identically in the backtest engine and live runtime; provide runtime-managed synthetic OCO/brackets
until exchange support exists; expose `POST /v1/trade-plan`. Roadmap: [root plan 0008](../../../docs/plans/0008-trade-signal-generator.md).

## 1. Context & Gaps Covered

- `crates/tantra-runtime-backtest/src/engine.rs` (~L615-811): StopLoss/StopLimit trigger on current price/bar open, gap-through fills at open, stop-first on same bar;
  **no intrabar high/low, trailing, targets/partials, or brackets**.
- `crates/tantra-runtime-strategy/src/intent.rs`: doc says trailing unsupported, yet `tests/golden.rs` references `trailing_stop_amount_sell` (stale).
- `crates/tantra-runtime-actors/src/sizing.rs`: `PositionSizingActor`, `SimplePositionSizer`.
- Upstream: `tantra-core/docs/plans/0005-trade-planner-and-manager.md`, `tantra-exec/docs/gaps/0003` (submit_bracket/modify_stop), `tantra-ledger/docs/gaps/0012` (bracket OMS). Sibling: plan [0005](0005-group-screener-alerts.md).

## 2. Component Specifications & Phases

- **P1 Backtest**: engine feeds each bar to `TradeManager`; intrabar high/low stop/target resolution (conservative stop-first, configurable); trailing, partial exits, breakeven, time stops; gap-through at open retained. Fix/remove stale `trailing_stop_amount_sell` golden.
- **P2 Journal**: record R-multiples and per-leg exits in the trade journal (`tantra-ledger-performance` store).
- **P3 Live synthetic OCO**: a `TradeManagerActor` drives `TradeManager` on bars/ticks; emits stop modify/cancel + partial-exit orders through the normal risk -> exec path;
  bounded channels with explicit `BackpressurePolicy`; state persisted via `tantra-runtime-persistence` (restart-safe, idempotent client order ids). Retired when exec gap 0003 / ledger gap 0012 close.
- **P4 REST**: `POST /v1/trade-plan` (spec/preset + overrides + symbol -> `TradePlan`), `Caller`-scoped, body-capped, OpenAPI from core schemas.
- **P5 Agent wiring**: implement agent `TradePlanPort` here (agent rule R4: market-data wiring lives in runtime), sourcing ATR/ADR%/MAs from `tantra-market`.

## 3. Test Plan (TDD)

`crates/*/tests/` only: engine tests for same-bar stop+target, gap-through, trailing, ladder, time stop; golden replay vs core vectors; actor restart/recovery test;
REST auth/tenant negative tests; every plan-derived order passes `RiskEngine`. Gates: `make check`, targeted `cargo test --profile prod`, `make prod`.

## 4. Dependencies & Tier Notes

L3 over L0-L2; no concrete broker in strategy crates (inject `Arc<dyn BrokerExecutor>`). No `Utc::now()` in domain paths; clock injected at actor boundary.

## 5. Open Questions

- Tick-level vs bar-close evaluation in live for `CloseBelowMa` (default: bar close only).
