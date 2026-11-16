# 0005 — Paper Exchange Runtime Wiring & Backtest Unification

**Status**: **P1 implemented (2026-11-16); D1–D5 landed; T14.5–T14.9 done**.
**Track**: T14 (P1/P2) · **Phase**: 4 (runtime REST API & chat tools)
**Owning repo**: `tantra-runtime` · **Master plan**: root `docs/plans/0007-*.md`

## Why

`tantra-runtime` currently hosts **two** degenerate paper simulators that disagree with the
backtest's L2 book:

- `NextOpenSim` (`api-rest/src/exec.rs`) — market-orders-only, long-only, fills at next bar
  open. Limit orders are rejected (`unsupported_order_type`); shorts are rejected
  (`no_position`).
- `PaperAdapter` (`strategy/src/dispatch.rs`) — fills at limit price or last-known market
  price; no book, no partial fills.

Both contradict the platform's research-live parity invariant (§AGENTS 4.3.3): backtest fills
via `sim::l2_book` (engine C), paper fills via (A)/(B) — different economics for the same
strategy. This plan replaces both with the shared `tantra-exec-paper` exchange, adds a real-time
fill/depth push channel, unifies the backtester onto the same engine, and ships an independent
`paper serve` daemon.

## Deliverables

### D1 — REST `/orders` on the exchange (replaces `NextOpenSim`) ✅ 2026-11-16 (`027ebac`)

`api-rest/src/orders.rs` + `exec.rs`: `ExecGateway` is a thin adapter over
`tantra_exec_paper::PaperExchange`. The `/orders` risk gate (SEBI `RiskEngine` →
approval queue → `OrderLedger`) is unchanged; only the venue behind it changes.

- Limit / stop (`stop_market`/`stop_limit` via the wire `order_type`, `price` = trigger)
  / IOC orders accepted (previously rejected).
- Shorts allowed (per-account holdings tracked by the exchange).
- Fills arrive on market advance (`AppState::advance_market` / `spawn_bar_pump`) — same flow,
  now through the exchange's order lifecycle (partial → filled).
- `DELETE /orders/{id}` cancels a working order; `POST /positions/close` squares via exchange
  market orders (both directions).
- Tests: `tests/paper_orders_tests.rs` (R1/R2/R3/R7 + stop coverage) + `tests/orders_exec.rs`
  re-based onto the exchange semantics.

### D2 — Strategy dispatch on the exchange (replaces `PaperAdapter` fill math) ✅ 2026-11-16 (`a9d1d96`)

`strategy/src/dispatch.rs`: `PaperAdapter` keeps its public API (`cash()`, `positions()`,
`fills()`, `equity()`) but delegates its fill computation to a per-account `PaperExchange`
session. `ZerodhaKiteAdapter`/`AngelOneSmartApiAdapter` **simulation scaffolds** (the no-
credentials branch) delegate to the same exchange so paper/zerodha-sim/angelone-sim share one
fill engine. The sync port stays the `BrokerAdapter` surface (now re-exported from
`tantra-exec-core` per gap 0008 option 2). Real NSE delivery fees via a `FillCostFn` wrapper;
market orders fill at the seeded reference price; limit orders rest until crossed.

### D3 — `/ws/live` bidirectional streaming (fills, orders, depth, positions) ✅ 2026-11-16 (`18be257`)

New WebSocket route alongside `ws.rs`'s `/ws` + `/ws/market`:

- Client → server: `{"action":"subscribe|unsubscribe|ping|place_order|cancel_order",...}`.
- Server → client: `OrderAccepted` / `Fill(partial|full)` / `Cancelled` / `Rejected` /
  `StopTriggered` / `DepthChanged` / `PositionSnapshot` / `EquitySnapshot` — the same events
  the exchange emits (broadcast from `ExecGateway` on every drain).
- Powers `tantra-ui` web/CSR panels (see `tantra-ui/docs/plans/0008`) and the TUI.
- Tests: `tests/ws_live.rs` (plan R4 acceptance).

### D4 — Backtest unification (backtester drives the exchange) ✅ 2026-11-16 (`a1cbeda`)

`backtest/src/engine.rs`: the inline `sim::l2_book` usage is replaced with the
`tantra-exec-paper` engine driven on historical bars. Because the exchange is
timestamp-deterministic, a backtest is exactly a paper session replayed over a historical tape:
byte-identical fills (economics), fees, and FIFO settlement. Golden-vector parity test
`test_identical_fills_paper_vs_backtest` (`tests/backtest_paper_parity_tests.rs`) is a runtime
gate.

- The engine keeps its own latency gating (`pending_orders`), slippage-impact accounting, MIS,
  and kokoro settlement; the exchange is the pure matching venue.
- **Recorded deviation**: adverse-fill slippage is no longer applied on the L2 path — the
  crate-local `sim::slippage::AdverseFillModel` is a distinct type from the exchange's, and the
  exchange exposes no pre-fill contra-level read; `BacktestConfig::with_adverse_fill` keeps
  working on the direct (non-L2) path. Restore L2 adverse fill by threading the exchange's own
  `AdverseFillModel` into `PaperExchange::with_adverse_fill` in a future batch.
- E9 parity test `tests/paper_parity_tests.rs` (untouched) stays green; `sim/l2_book.rs` kept
  for it (now `#[allow(dead_code)]` for the engine-unused pieces).

### D5 — Standalone `paper serve` daemon ✅ 2026-11-16 (`f70e0e2`)

Independent process mode (no broker credentials required):

- `paper-serve --tape <file> [--addr ...] [--cash ...] [--real-time] [--mis-square-off HH:MM]`
  (new bin `src/bin/paper_serve.rs` + `src/paper_serve.rs` library core).
- Replay tape: replays a recorded bar stream at wall-clock speed (`--real-time`) or as fast as
  possible (default).
- MIS 15:20-style square-off via `--mis-square-off` (IST clock on the tape's bar timestamps;
  deterministic v5 close-order ids). NSE trading-calendar gate is a **documented follow-on**
  (the tape's timeframe is not detected).
- Serves the same REST + `/ws/live` surface; a separate paper account is created per API token
  (single account today).
- Tests: `tests/paper_serve.rs` (plan R6 acceptance: boots, fills a resting limit on the tape,
  serves `/ws/live`, exits on tape end).

## TDD / test slices (all in `tests/`) — R1–R7 landed 2026-11-16

| # | Red test (failing first) | Green deliverable |
|---|--------------------------|-------------------|
| R1 | POST a limit order → accepted (previously 422 `unsupported_order_type`); fills when a bar crosses the limit | ✅ `tests/paper_orders_tests.rs` (D1) |
| R2 | POST a short sell → fills; position is negative | ✅ `tests/paper_orders_tests.rs` (D1) |
| R3 | Place buy 100, cancel after partial fill → partial-fill row + `Cancelled` | ✅ `tests/paper_orders_tests.rs` (D1) |
| R4 | `/ws/live` client receives `Fill` + `DepthChanged` events for its own order | ✅ `tests/ws_live.rs` (D3) |
| R5 | Same backtest tape run through `backtest` and through `PaperAdapter` → byte-identical fills (golden) | ✅ `tests/backtest_paper_parity_tests.rs` (D4) |
| R6 | `paper serve --tape fixtures/*.json` boots, fills orders, serves REST/WS, exits on tape end | ✅ `tests/paper_serve.rs` (D5) |
| R7 | `POST /positions/close` squares longs *and* shorts via the exchange | ✅ `tests/paper_orders_tests.rs` (D1) |

## Verification

```bash
cd tantra-runtime && cargo check --workspace --profile prod
cargo test --workspace --profile prod
cargo clippy --all-targets -- -D warnings && cargo fmt --check
make prod   # end-of-turn invariant (§11)
```

## Cross-repo dependencies

- **Requires** `tantra-exec/docs/plans/0003` (P0) to land first: `tantra-exec-paper`,
  `PaperExchangeExecutor`, the lifted sync port.
- Consumes `tantra-market-finance` + `tantra-ledger-portfolio` (already workspace deps of the
  backtest crate).
- When D2 lands, runtime gap `0008` (broker-adapter boundary) closes (option 2 executed).
