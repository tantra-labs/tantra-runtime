# `tantra-runtime` — Implementation Record

Dismantled from the former `docs/roadmap/` (`roadmap.md` + `migration-verification.md`).
Completed milestones are recorded here; remaining future work moved to
[`docs/gaps/`](../gaps/README.md) and [`docs/plans/`](../plans/README.md).

## Migration: `honba-ya` → `tantra-runtime` (Tier 3B, House)

| Milestone | Status |
|-----------|--------|
| Milestone 1: Config, Testing Harness & Pure Strategy Runtime (`tantra-runtime-config`, `tantra-runtime-testing`, `tantra-runtime-strategy`) | Complete |
| Milestone 2: Persistence, Backtester & Strategy Enhancer (`tantra-runtime-persistence`, `tantra-runtime-backtest`, `tantra-runtime-enhancer`) | Complete |
| Milestone 3: Supervised Actors, Cron Jobs & Axum REST/WS Gateway (`tantra-runtime-actors`, `tantra-runtime-api`, `tantra-runtime-api-rest`, `tantra-runtime-jobs`) | Complete |
| Zero inline `#[cfg(test)]` in `crates/*/src/` (all tests in `tests/`, parallel to `src/`) | Complete |
| `cargo fmt --check` | Clean |
| `cargo clippy --all-targets -- -D warnings` | Clean |
| `cargo test --workspace` | Passing |

The file-by-file mapping is in [`code-migration-map.md`](code-migration-map.md).

## Subsequent landings

| Landing | Status |
|---------|--------|
| Plan 0001 — Distributed Jobs Engine, Run Lifecycle, and Strategy Dispatch Runtime (RunId/journals, worker pool, registry, budgeting/fencing, Vixie cron scheduler, typed manifests, broker dispatch) | Complete — Stage 1 (`60a5bd9`), Stages 2–4 (`46817a0`); Stage-4 CLI/agent half in `tantra-ui` (`tantra-cli`) |
| Plan 0002 / Gap 0002 — Statistical gates, microstructure traps, overfitting verification (DSR, PBO/CSCV, walk-forward, holdout, block-bootstrap Monte Carlo, cost stress; backtest E2/E3 stop-fill semantics) | Complete |
| Gap 0003 — Zerodha Kite wire transport via `tantra-exec-zerodha` | Complete — real `POST/DELETE /orders/regular` delegation; Angel One remains a simulation scaffold pending `tantra-exec-angelone`; golden-vector conformance deferred |
| Plan 0003 — Trade-journal + market-reference REST endpoints | Complete — all trade-journal routes and `/market-data/symbol-profile` + `/market-data/calendar` registered in `v1.rs`; market-reference serves sample stores pending the `tantra-data` → Rust FFI bridge |
| Plan 0004 Phases 1–3 — block/bulk-deal chat tool | Complete — `FeedMarketReader::block_deals` honest `PortError::unavailable` stub citing `tantra-data` gap 0005; Phase 4 blocked on that gap; Phase 5 optional |
| Agent plan 0006 Phases 1–4 — price alerts, news/filings content tools, options-chain / corporate-actions chat adapters | Complete — `ProfileAlertStore`, `FeedContentReader`, `FeedMarketReader::{options_chain, corporate_actions}` honest stubs; data bridges tracked in `docs/gaps/0007` |
| Execution seam consolidation — `ExecutionActor::with_exec_stack` over `tantra_exec_core::RiskGatedExecutor<RateLimitedBroker<_>>`, `MockBrokerExecutor` consolidation into `tantra-runtime-testing` | Complete — commit `5fe6133` |
| Docs: per-crate design docs for all 14 crates | Complete — commit `555ce26` |
| Docs: former `docs/roadmap/` dismantled into `design/` / `gaps/` / `plans/` | Complete (this change) |

## Verification (docs batch)

```bash
# Run from the repository root
grep -rn "#\[cfg(test)\]" crates/*/src/   # zero inline tests in production source
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace --profile prod    # canonical prod profile (make test)
```

## Open future work

All open items live in [`docs/gaps/`](../gaps/README.md) (currently 0004–0008) and
[`docs/plans/`](../plans/README.md) (plan 0004 Phases 4–5; plan 0003's market-reference sample
stores pending the `tantra-data` → Rust FFI bridge). The chat-market/content-reader
approximations are itemized in gap 0007.
