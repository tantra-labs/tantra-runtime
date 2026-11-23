# 0032 — Jobs registry is a single JSON file: no multi-process single-runner guarantee, blocking fs, UTC-only cron

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: deployment · **Severity**: high
**Plan**: [0013](../plans/0013-engine-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-jobs/src/registry.rs:253` `FileJobRegistry { path, jobs: HashMap }`; load at `:263-290`; atomic tmp+rename save at `:308` using synchronous `std::fs`.
- `claim_job` (`crates/tantra-runtime-jobs/src/registry.rs:373`) checks leases only against the in-memory copy loaded by this process.
- `crates/tantra-runtime-jobs/Cargo.toml` pulls `sqlx` but the registry does not use it.
- Cron evaluation (`crates/tantra-runtime-jobs/src/lib.rs:122,167`) works in `DateTime<Utc>` only; there is no `Asia/Kolkata` zone parameter, so `"30 9 * * 1-5"` fires at 09:30 UTC.

## Why

Two replicas (or the CLI plus the server) load the file independently and both claim the same job; the last writer wins. Lease fencing tokens are meaningless without a shared atomic store. Cron entries intended for market hours fire 5.5 hours off. Synchronous file I/O on a Tokio worker breaks `.agents/rules/scalability-and-concurrency.md` §2.1.

## How to close it

1. Introduce a `JobRegistry` trait with a `SqlxJobRegistry` in `tantra-runtime-persistence` using an atomic claim, for example `UPDATE ... WHERE lease_expires < $now RETURNING fencing_counter`, as migration 008 (numbering per [0033](0033-audit-ledger-unification.md) and [0021](0021-durable-trading-state.md)).
2. Change `CronSchedule::next_execution(after, tz: chrono_tz::Tz)` with an IST default; document that cron expressions are interpreted in the schedule's zone.
3. Keep `FileJobRegistry` only for single-process/dev use and wrap its I/O in `spawn_blocking`.
4. Add a worker mode so sweeps can scale beyond one process (jobs table plus worker binary).

**Verification**: tests with two registries over one SQLite file asserting exactly one claim wins and fencing counters are monotonic; cron golden vectors for IST (`30 9 * * 1-5` fires 04:00 UTC); `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-jobs --profile prod`, `make prod`.

## Related

- Closed plan 0001 (this is a follow-up); [0021](0021-durable-trading-state.md) (job persistence in the durable-state gap).
- Root gap 0020 (time semantics), root plan 0022 (deployment architecture); plan [0012](../plans/0012-api-design-review-2026-11.md) R6 (ingest scheduling).
