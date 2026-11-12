# `tantra-runtime` — Migration Verification Milestone

Verification commands for the `honba-ya → tantra-runtime` migration:

```bash
# Run from the repository root
grep -rn "#\[cfg(test)\]" crates/*/src/ && exit 1 || true
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

## Migration Milestone Status

| Milestone | Status |
|-----------|--------|
| `tantra-runtime-config` — Typed TOML/env configuration | Complete |
| `tantra-runtime-testing` — Deterministic `MockBroker`, `ScriptedGateway` | Complete |
| `tantra-runtime-strategy` — `Strategy` trait, `StepPipeline` | Complete |
| `tantra-runtime-persistence` — SQLite/PostgreSQL ledger repositories | Complete |
| `tantra-runtime-backtest` — Event-driven backtester | Complete |
| `tantra-runtime-enhancer` — GA, Bayesian TPE, WFO, DSR optimizers | Complete |
| `tantra-runtime-actors` — Supervised Tokio actor pipeline | Complete |
| `tantra-runtime-jobs` — `JobScheduler`, `CronSchedule`, `JobSpec` | Complete |
| `tantra-runtime-api` — API service traits and DTOs | Complete |
| `tantra-runtime-api-rest` — Axum REST + `/ws/live` WebSocket | Complete |
| `tantra-runtime-feed`, `tantra-runtime-market`, `tantra-indicators` | Complete |
| `tantra-runtime-test-utils` — Shared test utilities | Complete |
| `cargo fmt --check` | Clean |
| `cargo clippy --all-targets -- -D warnings` | Clean |
| `cargo test --workspace` | Passing |
