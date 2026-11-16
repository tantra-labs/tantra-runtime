# Plans Index — `tantra-runtime`

Per workspace rule `.agents/rules/gap-tracking.md`:
- Gaps are individual features (`docs/gaps/XXXX-*.md`).
- Plans are collections of gaps (`docs/plans/XXXX-*.md`).
- When implemented, plan documents are cleaned up from `docs/plans/` and recorded in this index.

## Active Plans

| # | Title | Target Scope | Status |
|---|-------|--------------|--------|
| [0005](0005-group-screener-alerts.md) | Group Screener Alerts Engine (AlertRule store, scheduler, edge detection, sinks, `/v1/alerts/*`, MCP port wiring) | `tantra-runtime-persistence`, `tantra-runtime-api(-rest)` | Proposed — research `docs/research/trendlyne-screener-alerts-mcp.md`; root plan `docs/plans/0010` (F7, F8 wiring) |
| [0006](0006-trade-manager-backtest-and-synthetic-brackets.md) | TradeManager in Backtest, Synthetic Brackets, `/v1/trade-plan` | `tantra-runtime-backtest`, `tantra-runtime-actors`, `tantra-runtime-api-rest` | Proposed — root plan `docs/plans/0008` |

## Implemented Plans

| # | Title | Target Scope | Implemented In |
|---|-------|--------------|----------------|
| [0005](0005-paper-exchange-runtime-wiring.md) | Paper Exchange Runtime Wiring & Backtest Unification — REST `/orders` + strategy dispatch on the `tantra-exec-paper` engine (replaces `NextOpenSim`/`PaperAdapter`), `/ws/live` push, backtest drives the exchange (byte-identical fills), standalone `paper serve` daemon | `tantra-runtime-api-rest`, `tantra-runtime-strategy`, `tantra-runtime-backtest` | **D1–D5, 2026-11-16** (`027ebac`/`a9d1d96`/`18be257`/`a1cbeda`/`f70e0e2`); R1–R7 green; plan 0005 doc kept as the record of the D4 adverse-fill deviation + D5 calendar-gate follow-on. Master plan root `docs/plans/0007` (P1 done). |
| 0001 | Distributed Jobs Engine, Run Lifecycle, and Strategy Dispatch Runtime | `tantra-runtime-jobs`, `tantra-runtime-strategy`, `tantra-runtime-api-rest` | Stage 1 (RunId, journals, recovery) in `60a5bd9`; Stages 2–4 (pool, registry, budgeting/fencing, cron scheduler, typed manifests, broker dispatch) in `46817a0`; Stage 4 CLI/agent wiring in the `tantra-ui` repo (`tantra-cli` — `tantra jobs run/tick/logs`, tool-call journals, approvals queue) |
| 0002 | Comprehensive Statistical Gates, Microstructure Traps, and Overfitting Verification | `tantra-core::analytics`, `tantra-runtime-backtest` | `tantra-core::analytics` six-gate kernel (DSR, PBO/CSCV, walk-forward, holdout, block-bootstrap Monte Carlo, cost stress) + `ValidationBlock` (30 tests in `tests/analytics_gates_tests.rs`); backtest E2 gap-through-stop penalties + E3 conservative same-bar stop ordering (6 tests in `tests/stop_fill_tests.rs`); M5 freeze limits via `tantra-risk`. Closed with GAP-0002. |
| 0003 | Trade-Journal & Market-Reference REST Endpoints | `tantra-runtime-api-rest` | All routes registered in `crates/tantra-runtime-api-rest/src/v1.rs`: `trade_journal.rs` (accounts GET/POST, overview, trades GET + PATCH, calendar, notes GET/POST, compare) wrapping `tantra_ledger_performance::store::JournalStore`, plus `GET /market-data/symbol-profile/:id` and `GET /market-data/calendar` in `market.rs`. Market-reference handlers serve illustrative sample stores (see their doc comments) pending the `tantra-data` → Rust FFI bridge; OpenAPI entries added for all new routes. Test coverage in `tests/trade_journal.rs` + `tests/market_reference.rs`. |
| 0004 | Block/bulk deal chat tool and REST endpoint | `tantra-runtime-api-rest`, `tantra-agent-chat-tools` | Phases 1–3 (chat tool + honest stub) 2026-11-11; **Phases 4–5 (real data + REST endpoint) 2026-11-14** (`b561e12`): `tantra-data` `export_block_deals` (`ce7c392`), `tantra-market-store::BlockDealsProjectionReader` (`02fc20e`), `FeedMarketReader::block_deals` real read + `GET /market-data/block-deals`; tests green. |
