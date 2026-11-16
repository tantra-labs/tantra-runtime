# 0005 — Paper Exchange Runtime Wiring & Backtest Unification

**Status**: Proposed (planning only; no development yet).
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

### D1 — REST `/orders` on the exchange (replaces `NextOpenSim`)

`api-rest/src/orders.rs` + `exec.rs`: `ExecGateway` becomes a thin adapter over
`tantra_exec_paper::PaperExchange`. The `/orders` risk gate (SEBI `RiskEngine` →
approval queue → `OrderLedger`) is unchanged; only the venue behind it changes.

- Limit / stop / IOC / GTD orders accepted (previously rejected).
- Shorts allowed (per-account holdings tracked by the exchange).
- Fills arrive on market advance (`AppState::advance_market` / `spawn_bar_pump`) — same flow,
  now through the exchange's order lifecycle (partial → filled).
- `DELETE /orders/{id}` cancels a working order; `POST /positions/close` squares via exchange
  market orders (both directions).

### D2 — Strategy dispatch on the exchange (replaces `PaperAdapter` fill math)

`strategy/src/dispatch.rs`: `PaperAdapter` keeps its public API (`cash()`, `positions()`,
`fills()`, `equity()`) but delegates its fill computation to a per-account `PaperExchange`
session. `ZerodhaKiteAdapter`/`AngelOneSmartApiAdapter` **simulation scaffolds** (the no-
credentials branch) delegate to the same exchange so paper/zerodha-sim/angelone-sim share one
fill engine. The sync port stays the `BrokerAdapter` surface (now re-exported from
`tantra-exec-core` per gap 0008 option 2).

### D3 — `/ws/live` bidirectional streaming (fills, orders, depth, positions)

New WebSocket route alongside `ws.rs`'s `/ws` + `/ws/market`:

- Client → server: subscribe/unsubscribe, place/cancel requests (paper mode).
- Server → client: `OrderAccepted` / `Fill(partial|full)` / `Cancelled` / `Rejected` /
  `DepthChanged` / position & equity snapshots — the same events the exchange emits.
- Powers `tantra-ui` web/CSR panels (see `tantra-ui/docs/plans/0008`) and the TUI.

### D4 — Backtest unification (backtester drives the exchange)

`backtest/src/engine.rs`: replace the inline `sim::l2_book` usage with the
`tantra-exec-paper` engine driven on historical bars. Because the exchange is
timestamp-deterministic, a backtest is exactly a paper session replayed over a historical tape:
byte-identical fills, fees, and FIFO settlement. Golden-vector parity test
`test_identical_fills_paper_vs_backtest` becomes a runtime gate.

- E9 parity test landed (`tests/paper_parity_tests.rs`, 2026-11-16): the paper
  exchange and the backtest L2 book produce byte-identical fills on a golden
  tape (book-level + end-to-end).

### D5 — Standalone `paper serve` daemon

Independent process mode (no broker credentials required):

- Replay tape: replay a recorded bar/tick stream at wall-clock speed (or as fast as possible).
- Live feed: consume real broker WS feeds (`tantra-market-dhan`/`zerodha`) into the exchange.
- NSE trading calendar + MIS 15:20 square-off (reuse `backtest/src/calendar.rs` /
  `sim::mis`).
- Serves the same REST + `/ws/live` surface; a separate paper account is created per API token.

## TDD / test slices (all in `tests/`)

| # | Red test (failing first) | Green deliverable |
|---|--------------------------|-------------------|
| R1 | POST a limit order → accepted (previously 422 `unsupported_order_type`); fills when a bar crosses the limit | Limit support on `/orders` |
| R2 | POST a short sell → fills; position is negative | Short support |
| R3 | Place buy 100, cancel after partial fill → partial-fill row + `Cancelled` | Partial fill + cancel lifecycle |
| R4 | `/ws/live` client receives `Fill` + `DepthChanged` events for its own order | WS push channel |
| R5 | Same backtest tape run through `backtest` and through `PaperAdapter` → byte-identical fills (golden) | Backtest/paper parity |
| R6 | `paper serve --tape fixtures/*.json` boots, fills orders, serves REST/WS, exits on tape end | Standalone daemon |
| R7 | `POST /positions/close` squares longs *and* shorts via the exchange | Emergency close both sides |

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
