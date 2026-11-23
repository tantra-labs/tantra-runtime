# 0033 — SEBI audit ledger is SQLite-only, outside migrations, schema-divergent and not tamper-evident

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0013](../plans/0013-engine-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-persistence/src/audit.rs:77,95` defines a concrete `SqliteAuditLedger` with its own `init_schema`, created outside the migration runner (`crates/tantra-runtime-persistence/src/migrations.rs:88-90`). The Supervisor API takes `Arc<SqliteAuditLedger>` (`crates/tantra-runtime-actors/src/supervisor.rs`, `spawn_with_observability`).
- `crates/tantra-runtime-persistence/migrations/sqlite/006_audit_ledger.up.sql` is an orphan file defining a different `sebi_audit_ledger` (double-entry columns `debit_account`, `amount`) from the runtime table in `audit.rs`. The Postgres `006_audit_ledger` is a third shape.
- Migration versions differ per backend: SQLite chat=6, Postgres chat=7 (`crates/tantra-runtime-persistence/src/migrations.rs:83,124`), so `schema_migrations` disagree.
- No `UPDATE`/`DELETE` triggers and no hash chaining exist in the persistence crate, while `tantra-exec` ships a hash-chained fsynced audit WAL.

## Why

"Append-only" is a comment, not an invariant. Postgres deployments have no audit sink via the actors at all, and retention/export for a SEBI inspection cannot be guaranteed. Two diverging audit trails (runtime table and exec WAL) cannot be reconciled.

## How to close it

1. Define an `AuditSink` port in `tantra-core` or `tantra-exec-core` (provider repo `tantra-exec`); the Supervisor and `ExecutionActor` depend on the trait, not `SqliteAuditLedger`.
2. Implement it over the `tantra-exec` hash-chained WAL and over a SQLite/Postgres table with `BEFORE UPDATE/DELETE` triggers that raise.
3. Unify migration numbering across backends and delete the orphan SQLite `006` file; bring the audit table under the migration runner.
4. Add a backup/retention/export job and a chain-verification command.

**Verification**: golden-vector test for chain verification (tamper a row, expect detection); trigger tests asserting `UPDATE`/`DELETE` fail on both backends; migration test asserting identical logical versions; `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-persistence --profile prod`, `make prod`.

## Related

- Root gap 0023 (tamper-evident audit unification); `tantra-exec` gap 0009 (audit WAL integrity); [0034](0034-discarded-oms-and-audit-errors.md) (audit write failures swallowed).
- Root plan 0019 §4.2 (hook not linked into a binary).
