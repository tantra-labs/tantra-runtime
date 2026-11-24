# 0018 — Backtest/sweep/journal runs and the global strategy catalog are not Caller-scoped

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-api-rest/src/runs.rs:44,74,93,127,151,181,200,218` — no handler takes `Caller`; `get_backtests`/`get_sweeps` list every run, `get_backtest_by_id`/`get_backtest_journal` read any run id.
- `crates/tantra-runtime-api-rest/src/state.rs:147` — `strategies: Arc<Mutex<StrategyCatalog>>` is one process-wide catalog; `POST /strategies` (`post_strategies` in `routes.rs`) writes into it with no owner.
- `crates/tantra-runtime-api-rest/src/run_store.rs:3` — runs are stored at `<journals_dir>/<run_id>/manifest.json`; the layout has no owner field.
- Contrast: orders (`crates/tantra-runtime-api-rest/src/orders.rs:1158` `is_owned_by`), alerts, profile, portfolio and chat threads all key by `Caller`.

## Why

With auth on, token B can list and read the full event journal (fills, equity) of token A's backtests and use A's compiled strategies by id. Gap 0013 scoped orders and the trade journal only; runs and strategies are a separate tenant-isolation hole (`.agents/rules/security-and-secrets.md` §2.1).

## How to close it

1. Add `owner: String` to the run manifest (`RunManifest` in `tantra-runtime-api`) and to `RunStore`.
2. Filter list/get/journal/cancel by `Caller`; foreign run ids answer 404.
3. Make `StrategyCatalog` a `BTreeMap<(caller, id), _>` (or a `CallerStrategies` wrapper in the style of `CallerJournals`).
4. Keep an `admin` scope bypass.

**Verification**: `tests/runs_tenant_isolation.rs` — B gets 404 for A's run id and journal; list excludes foreign runs; B cannot resolve A's strategy id; admin sees all. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted `cargo test -p tantra-runtime-api-rest --test runs_tenant_isolation --profile prod`.

## Related

- Follows closed gaps 0013 (journal/orders scoped) and 0017 (per-caller paper accounts); adjacent, not covering runs.
- Binding rule: `.agents/rules/security-and-secrets.md` §2.1.
