# Plans Index — `tantra-runtime`

Per workspace rule `.agents/rules/gap-tracking.md`:
- Gaps are individual features (`docs/gaps/XXXX-*.md`).
- Plans are collections of gaps (`docs/plans/XXXX-*.md`).
- When implemented, plan documents are cleaned up from `docs/plans/` and recorded in this index.

## Active Plans

| # | Title | Target Scope | Status |
|---|-------|--------------|--------|
| [0003](0003-trade-journal-and-market-reference-rest-endpoints.md) | REST endpoints for trade-journal analytics and market reference data (symbol profile, economic calendar) | `tantra-runtime-api-rest` | Proposed |
| [0004](0004-block-bulk-deal-chat-tool-and-rest-endpoint.md) | Block/bulk deal chat tool and REST endpoint | `tantra-runtime-api-rest`, `tantra-agent-chat-tools` | In progress — Phases 1–3 implemented (honest `unavailable` stub); Phase 4 blocked on `tantra-data` gap 0005 |

## Implemented Plans

| # | Title | Target Scope | Implemented In |
|---|-------|--------------|----------------|
| 0001 | Distributed Jobs Engine, Run Lifecycle, and Strategy Dispatch Runtime | `tantra-runtime-jobs`, `tantra-runtime-strategy`, `tantra-runtime-api-rest` | Stage 1 (RunId, journals, recovery) in `60a5bd9`; Stages 2–4 (pool, registry, budgeting/fencing, cron scheduler, typed manifests, broker dispatch) in `46817a0`; Stage 4 CLI/agent wiring in the `tantra-ui` repo (`tantra-cli` — `tantra jobs run/tick/logs`, tool-call journals, approvals queue) |
| 0002 | Comprehensive Statistical Gates, Microstructure Traps, and Overfitting Verification | `tantra-core::analytics`, `tantra-runtime-backtest` | `tantra-core::analytics` six-gate kernel (DSR, PBO/CSCV, walk-forward, holdout, block-bootstrap Monte Carlo, cost stress) + `ValidationBlock` (30 tests in `tests/analytics_gates_tests.rs`); backtest E2 gap-through-stop penalties + E3 conservative same-bar stop ordering (6 tests in `tests/stop_fill_tests.rs`); M5 freeze limits via `tantra-risk`. Closed with GAP-0002. |
