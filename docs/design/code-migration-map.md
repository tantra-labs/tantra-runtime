# `tantra-runtime` — Historical Migration Map (honba-ya → tantra-runtime)

> **Tier**: Tier 3B (Depends on `tantra-core`, `tantra-risk`, `tantra-ledger`, `tantra-market`, `tantra-exec`)
> **Source**: `honba-workspace/honba-ya`
> **Target**: `tantra-runtime`
> **Status**: Migration complete (all 10 sub-crates ported).

## Sub-Crate Mapping Table (Migrated in Listed Order)

| Order | Source Crate (`honba-ya/crates/`) | Target Crate (`tantra-runtime/crates/`) | Key Refactoring & Renames |
| :---: | :--- | :--- | :--- |
| 1 | `honba-ya-config` | `tantra-runtime-config` | Port config loaders and TOML schemas. |
| 2 | `honba-ya-testing` | `tantra-runtime-testing` | Move `src/tests/*.rs` to `tests/`. Remove `#[cfg(test)] mod tests;` from `src/lib.rs`. |
| 3 | `honba-ya-strategy` | `tantra-runtime-strategy` | Move `src/tests/*.rs` to `tests/`. Remove `#[cfg(test)] mod tests;` from `src/lib.rs`. Wire to `tantra_risk`. |
| 4 | `honba-ya-persistence` | `tantra-runtime-persistence` | Copy `migrations/` and `src/`. Implement `tantra_ledger` repository traits. |
| 5 | `honba-ya-backtest` | `tantra-runtime-backtest` | Wire to `tantra_ledger_journal`, `tantra_ledger_portfolio`, `tantra_market_finance`, `tantra_runtime_strategy`. |
| 6 | `honba-ya-enhancer` | `tantra-runtime-enhancer` | Port GA, Bayesian TPE, WFO, DSR, and Odysseus optimizers. |
| 7 | `honba-ya-actors` | `tantra-runtime-actors` | Port actor pipeline and supervision tree. |
| 8 | *(New Crate)* | `tantra-runtime-jobs` | Implement deterministic `JobScheduler`, `CronSchedule`, `JobSpec` + `tests/unit_tests.rs`. |
| 9 | `honba-ya-api` | `tantra-runtime-api` | Port API service traits and models. |
| 10 | `honba-ya-api-rest` | `tantra-runtime-api-rest` | Port Axum REST routes, `/ws/live` WebSocket broadcaster, screener endpoints, and OpenAPI generator. |
| *(new)* | *(new)* | `tantra-runtime-feed` | Market data feed adapter crate. |
| *(new)* | *(new)* | `tantra-runtime-market` | Market data normalization crate. |
| *(new)* | *(new)* | `tantra-indicators` | Technical indicators crate. |
| *(new)* | *(new)* | `tantra-runtime-test-utils` | Shared test utilities crate. |

## Mandatory DDD Fixes During Migration

1. **Strict Parallel Test Directory Compliance**:
   - In `honba-ya-strategy`, `src/tests/` (`context.rs`, `dyn_strategy.rs`, `intent.rs`, `ir.rs`, `ledger.rs`, `look_ahead.rs`, `manifest.rs`, `mtf.rs`, `runner_*.rs`) lived inside `src/`. Move them into `crates/tantra-runtime-strategy/tests/`.
   - In `honba-ya-testing`, `src/tests/` (`assert.rs`, `feed.rs`, `fixtures.rs`, `ports.rs`, `recorder.rs`, `scripted_gateway.rs`) lived inside `src/`. Move them into `crates/tantra-runtime-testing/tests/`.
2. **Import Replacements**:
   - `honba_core` → `tantra_core`
   - `honba_mon` → `tantra_risk`
   - `honba_kokoro` → `tantra_ledger`
   - `honba_kokoro_oms` → `tantra_ledger_oms`
   - `honba_kokoro_portfolio` / `honba_portfolio` → `tantra_ledger_portfolio`
   - `honba_kokoro_ledger` → `tantra_ledger_journal`
   - `honba_me_*` → `tantra_market_*` (`honba_me_market` → `tantra_market_finance`)
   - `honba_te_*` → `tantra_exec_*`
   - `honba_ya_*` → `tantra_runtime_*`
3. Copy `honba-ya/configs/` into `tantra-runtime/configs/`.

## Verification Commands

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
| Milestone 1: Config, Testing Harness & Pure Strategy Runtime | Complete |
| Milestone 2: Persistence, Backtester & Strategy Enhancer | Complete |
| Milestone 3: Supervised Actors, Cron Jobs & Axum REST/WS Gateway | Complete |
| Zero inline `#[cfg(test)]` in `src/` | Complete |
| `cargo fmt --check` | Clean |
| `cargo clippy --all-targets -- -D warnings` | Clean |
| `cargo test --workspace` | Passing |
