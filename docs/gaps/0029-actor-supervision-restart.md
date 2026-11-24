# 0029 — Supervisor never supervises: no restart loop, and restart cannot recover state

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0013](../plans/0013-engine-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-actors/src/supervisor.rs:347` `restart_actor_handle` only swaps a `JoinHandle` and increments counters. Nothing awaits the handles or detects an actor exit; the only callers are `crates/tantra-runtime-actors/tests/actor_runtime_tests.rs:277,287` with dummy handles.
- `crates/tantra-runtime-actors/src/supervisor.rs:78-95`: actors own their `mpsc::Receiver` and are moved into `tokio::spawn`. After a panic the receiver and its queued messages are gone, so a "restart" cannot reuse the same channels.
- `RestartStrategy::AllForOne` (`crates/tantra-runtime-actors/src/supervisor.rs:364-374`) is identical to `OneForOne`; siblings are not restarted.
- `crates/tantra-runtime-actors/src/supervisor.rs:276-331` `shutdown_gracefully` returns `Result<(), String>` and on timeout only logs; handles are not aborted.

## Why

A panic in `RiskActor` (for example inside `check_order`) leaves a dead risk channel. Every order then fails with an "unreachable"-style error, there is no auto-recovery and no halt-on-crash, and halted-algo state held inside `RiskEngine` is lost. A supervisor that does not supervise gives false assurance in a live-trading runtime.

## How to close it

1. Add a `Supervisor::run()` task that selects over a `JoinSet` of actor handles, with exponential backoff and a maximum restart intensity.
2. Change ownership: the supervisor owns the `Sender`s and a factory `Fn() -> (Actor, Receiver)` per actor, so a restarted actor gets a fresh receiver while senders held by peers stay valid.
3. Add a state-snapshot trait (`RiskEngine` halts, OMS) persisted via `tantra-runtime-persistence` (see [0021](0021-durable-trading-state.md)).
4. Fail closed: a crash of the risk or execution actor triggers a global halt until recovery succeeds.
5. Replace `Result<(), String>` with a typed `SupervisorError`/`ShutdownError`; abort handles on shutdown timeout. `AllForOne` must restart siblings.

**Verification**: chaos tests in `crates/tantra-runtime-actors/tests/` that kill each actor and assert restart, halt-on-crash and state recovery; `AllForOne` sibling-restart test; `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-actors --profile prod`, `make prod`.

## Related

- [0021](0021-durable-trading-state.md) (halt state persistence), [0022](0022-observability-health-readiness.md) (health snapshot, drain), [0030](0030-risk-actor-blocking-broker-ack.md).
- Root plan 0019 §4.2 (supervisor not linked into a binary) is independent of this design gap.
