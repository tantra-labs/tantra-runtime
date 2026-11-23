# 0034 — Execution actor discards OMS transitions and audit-write errors

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: medium
**Plan**: [0013](../plans/0013-engine-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-actors/src/execution.rs:212` and `:231`: `let _ = OrderStateMachine::transition(state, OrderStatus::New|Rejected);` The result is dropped and never written back to `self.oms`; the comment at `:206` says "for now, just mark as New".
- Audit write failures are swallowed: `crates/tantra-runtime-actors/src/execution.rs:176,203` use `let _ = audit.record_...await`.

## Why

The OMS inside the actor never reflects acks or rejects, so its view of order state is stale. SEBI audit records can silently be missing while the order still goes out, which is fail-open on the audit trail.

## How to close it

1. Make the OMS call `Oms::apply(client_order_id, event) -> Result<_, OmsError>`, store the new state, and propagate the error.
2. Treat an audit-write failure on `order_attempt` as order-blocking (fail closed: reject and alarm). Failures on later events raise an alarm plus a metric and are retried from a bounded queue.
3. Drop every `let _ =` on these paths.

**Verification**: tests in `crates/tantra-runtime-actors/tests/` with a failing audit sink asserting the order is not sent, and with ack/reject sequences asserting OMS state; `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-actors --profile prod`, `make prod`.

## Related

- [0033](0033-audit-ledger-unification.md) (audit sink port), [0030](0030-risk-actor-blocking-broker-ack.md), [0029](0029-actor-supervision-restart.md).
- `tantra-ledger` gap 0016 (OMS typestate transitions); root gap 0010.
