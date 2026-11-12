# `tantra-runtime` — TDD & Engineering Roadmap

## Milestone 1: Config, Testing Harness & Pure Strategy Runtime (P0) — [COMPLETED]
- [x] Port `tantra-runtime-config`, `tantra-runtime-testing`, and `tantra-runtime-strategy`.
- [x] Relocate `src/tests/` out of `tantra-runtime-strategy/src/` and `tantra-runtime-testing/src/` into `tests/`.

## Milestone 2: Persistence, Backtester & Strategy Enhancer (P0) — [COMPLETED]
- [x] Port `tantra-runtime-persistence` implementing `tantra-ledger` repository traits.
- [x] Port `tantra-runtime-backtest` and `tantra-runtime-enhancer`.

## Milestone 3: Supervised Actors, Cron Jobs & Axum REST/WS Gateway (P0) — [COMPLETED]
- [x] Port `tantra-runtime-actors`, `tantra-runtime-api`, and `tantra-runtime-api-rest`.
- [x] Implement `tantra-runtime-jobs` (`JobScheduler`, `ScheduledJob`, `JobStatus`, deterministic tick-driven cron evaluator).

## Migration Verification — [COMPLETED]
- [x] `cargo fmt --check` — clean
- [x] `cargo clippy --all-targets -- -D warnings` — clean
- [x] `cargo test --workspace` — passing
- [x] Zero inline `#[cfg(test)]` blocks in `src/`
- [x] Zero-I/O domain logic — simulation clock stepped deterministically
- [x] Zero floats — all accounting and prices use `Decimal`

See `docs/roadmap/migration-verification.md` for full migration milestone status.
