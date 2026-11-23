# 0028 — Strategy crate depends on concrete `ZerodhaExecutor`, builds a nested Tokio runtime and prints secrets

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0013](../plans/0013-engine-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-strategy/Cargo.toml:20` declares `tantra-exec-zerodha.workspace = true`; `crates/tantra-runtime-strategy/src/dispatch.rs:16` has `use tantra_exec_zerodha::ZerodhaExecutor;`.
- `crates/tantra-runtime-strategy/src/dispatch.rs:554` holds `executor: Option<Arc<ZerodhaExecutor>>` (a concrete broker type, not `Arc<dyn BrokerExecutor>`).
- `crates/tantra-runtime-strategy/src/dispatch.rs:629` builds `Runtime::new().expect(...)` inside library code; `:718` and `:732` call `runtime.block_on(exec.place_order/cancel_order)`. The comment at `:712` itself admits this panics when driven from an async actor.
- `crates/tantra-runtime-strategy/src/dispatch.rs:560-565` implements `Debug` for `ZerodhaKiteAdapter` and prints `api_key` and `access_token` in clear; `:758` repeats the pattern. No redaction, no `ZeroizeOnDrop`.
- `.expect` in a constructor at `crates/tantra-runtime-strategy/src/dispatch.rs:629` sits on a live-trading path.

## Why

- The strategy crate cannot be kept a pure domain crate (zero I/O, wasm-clean) while it links a concrete broker.
- A live Zerodha adapter driven from the async `StrategyActor` panics ("cannot start a runtime from within a runtime"), so the live path is unusable exactly where it is meant to run.
- Access tokens appear in any `{:?}` log line.
- Breaks `.agents/rules/architecture-and-coupling.md` §2.3 (no concrete executors in strategy crates), `.agents/rules/scalability-and-concurrency.md` §2.3 (no nested `Runtime::block_on`) and `.agents/rules/security-and-secrets.md` §3.3 (redacted `Debug`, zeroize).

## How to close it

1. Move `ZerodhaKiteAdapter` to a composition-root crate (a `tantra-exec` bridge or the `tantra-runtime-api-rest` bootstrap). It implements `BrokerAdapter` over `Arc<dyn BrokerExecutor>` and a Tokio `Handle` (not an owned `Runtime`), or the adapter trait becomes async.
2. Remove `tantra-exec-zerodha` from `crates/tantra-runtime-strategy/Cargo.toml` and delete all `Runtime::new`/`block_on` use from the crate.
3. Give credential-holding types a manual redacted `Debug` and `Zeroize`/`ZeroizeOnDrop` (reuse the secret types from `tantra-exec` gap 0007 once available).
4. Add a dependency-rule test in `crates/tantra-runtime-strategy/tests/` asserting no `tantra-exec-*` concrete dependency and no `Runtime::new` in `src/`.

**Verification**: new `tests/dependency_rules.rs` (Cargo.toml + source scan), a `Debug`-output test asserting `[REDACTED]`, an async-context test placing an order through the adapter without panic; `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-strategy --profile prod`, `make prod`.

## Related

- Root gap 0011 (credentials leak via `Debug`/logs); `tantra-exec` gap 0007 (secret types).
- Closed gap 0003 wired the concrete transport here; closed gap 0008 (dual execution-port boundary).
- Plan [0013](../plans/0013-engine-design-review-2026-11.md) Phase A.
