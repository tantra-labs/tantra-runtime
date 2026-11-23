# 0025 — Sync file/NDJSON/audit I/O and wall-clock reads inside async request paths

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: medium
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-api-rest/src/auth.rs:196,205` — `std::fs::metadata`/`read_to_string` (token-file hot reload) and `:391` `OpenOptions` audit append run inside the auth middleware on every request.
- `crates/tantra-runtime-api-rest/src/streams.rs:3-6` — reads journal files with a blocking `BufReader` for SSE.
- `operator_journal.rs` — sync NDJSON append on `PUT /trading-state`.
- Wall clock inside handlers: `trade_journal.rs:625,887`, `alerts_routes.rs:140`, `exec.rs:284,444`, `chat_market.rs:465,584,1131` (and `:886` `Utc::now().date_naive()` in an adapter) — no injected `Clock`, so these paths cannot be tested deterministically.

## Why

Auth is on every request: a slow disk or NFS mount stalls a worker per call (`.agents/rules/scalability-and-concurrency.md` §2.1). The auth audit lines are not fsynced, not rotated and not hash-chained, unlike the `tantra-exec` audit WAL, which SEBI retention needs.

## How to close it

1. Add a `Clock` port to `AppState` (system clock at the composition root, fixed clock in tests) and replace the handler `Utc::now()` calls.
2. Reload the token store on a background task and publish via `ArcSwap`; the middleware only reads the swapped snapshot.
3. Route audit through a bounded mpsc to a single writer task, reusing the `tantra-exec-core` audit WAL.
4. Use `tokio::fs`/`spawn_blocking` for the SSE journal tail and the operator journal append.

**Verification**: tests with a fixed `Clock` (deterministic timestamps in journal/alerts/exec); token-file change picked up without per-request I/O; audit writer back-pressure test; hash-chain verification of the audit file. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted `cargo test -p tantra-runtime-api-rest --profile prod`.

## Related

- Root plan 0019 P2.7 already lists `auth.rs` among sync-I/O files, so only the audit-WAL and `Clock` parts are new here.
- Root gap 0023 (tamper-evident audit unification); gap 0033 (runtime audit ledger) and `tantra-exec` gap 0009 (audit WAL integrity).
- Gap 0019 (blocking compute on workers).
