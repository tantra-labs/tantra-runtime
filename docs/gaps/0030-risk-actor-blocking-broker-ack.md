# 0030 — Risk actor blocks its mailbox while awaiting the broker ack

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0013](../plans/0013-engine-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-actors/src/risk.rs:182-200`: after approving an order, `RiskActor` sends `ExecuteOrder` and then awaits `exec_rx.await` inline inside the `while let Some(msg) = self.rx.recv()` loop at `:74`. The await has no timeout.
- `HaltAlgo` and `HaltGlobal` (`crates/tantra-runtime-actors/src/risk.rs:246,256`) are handled from the same mailbox, so a halt queues behind every outstanding broker round-trip.

## Why

Kill-switch latency equals the sum of in-flight broker latencies. A hung broker call stalls risk evaluation and the kill switch completely, which defeats the purpose of a pre-trade gate and halt. The failure appears precisely when a broker is degraded and an operator most needs to halt.

## How to close it

1. Spawn the ack await (`tokio::spawn` or a `FuturesUnordered` owned by the actor) so the main loop returns to `recv()` immediately.
2. Move `HaltAlgo`/`HaltGlobal` onto a priority control channel selected with `biased` ahead of the order mailbox.
3. Wrap the ack await in `tokio::time::timeout`; on timeout report an unknown-outcome rejection to the caller (do not assume the order failed) and raise an alarm metric.
4. Keep a bounded map of in-flight orders so duplicate submissions are refused while an ack is pending.

**Verification**: test in `crates/tantra-runtime-actors/tests/` with a mock broker that never replies, asserting `HaltGlobal` is applied in under 50 ms and the timed-out order is reported as unknown-outcome; `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-actors --profile prod`, `make prod`.

## Related

- Root gap 0013 (pre-trade risk gate not single/shared/fail-closed); root gap 0010 (unknown-outcome handling).
- [0029](0029-actor-supervision-restart.md), [0034](0034-discarded-oms-and-audit-errors.md).
