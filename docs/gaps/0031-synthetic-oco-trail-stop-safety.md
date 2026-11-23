# 0031 — Synthetic OCO `trail_stop` can leave a position unprotected and corrupts its own state on partial failure

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0013](../plans/0013-engine-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-strategy/src/synthetic_oco.rs:558-577`: the tracker `modify_stop` is applied first (`:560`), then `stop_broker_id.take()` (`:565`), then the old stop is cancelled, then the new one submitted. Any `?` after the `take()` returns with `stop_broker_id = None` and the tracker already advanced.
- The resubmission reuses the deterministic client id from `stop_leg_client_id` (`crates/tantra-runtime-strategy/src/synthetic_oco.rs:173`). A broker or exchange that dedups on `client_order_id` rejects it, and between cancel and submit no stop is resting.
- Errors are `Result<_, String>` (known item, tracked in root plan 0019 P3.3); the atomicity bug is separate.

## Why

A transient broker error during a trail leaves an open position with no protective stop while the bracket believes the stop exists (or the reverse). This is the classic OCO leg-cancel race, and it exposes the account to unbounded loss on exactly the positions the bracket was meant to protect.

## How to close it

1. Reorder to submit-new-then-cancel-old, using a versioned leg id (`#stop.{n}`) or a broker modify call where supported, so a resting stop exists at every instant.
2. Roll the tracker back on any failure; never `take()` the broker id before the replacement is confirmed.
3. Make the state machine two-phase (`TrailPending`) so an interrupted trail is resumable and reconciled against broker state.
4. Replace `String` errors with a typed error enum.

**Verification**: tests with a failing mock `BrokerAdapter` injected at each step (submit, cancel, modify) asserting a stop always exists or the tracker is unchanged; proptest over fill/cancel/trail interleavings in `crates/tantra-runtime-strategy/tests/`; `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-strategy --profile prod`, `make prod`.

## Related

- Plan [0006](../plans/0006-trade-manager-backtest-and-synthetic-brackets.md) (synthetic brackets) partially overlaps.
- Root gap 0010 (idempotency and unknown-outcome handling); `tantra-exec` gap 0004.
- [0028](0028-strategy-crate-concrete-executor-and-nested-runtime.md) (same crate, dispatch layer).
