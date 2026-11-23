# 0035 — Market-data backpressure silently drops bars, ignores send errors; native `StrategyActor` has no panic isolation

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: medium
**Plan**: [0013](../plans/0013-engine-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-actors/src/market_data.rs:225`: bars go through `try_send` and are dropped when the strategy channel is full (metric only). Backtest never drops bars.
- `crates/tantra-runtime-actors/src/market_data.rs:186`: Passthrough does `let _ = sender.send(...)`, ignoring a closed channel.
- `crates/tantra-runtime-actors/src/market_data.rs:208`: `self.tick_buffer.remove(0)` is O(n); a `VecDeque` is the right structure.
- `crates/tantra-runtime-actors/src/strategy.rs:95-120`: no per-message panic isolation (`catch_unwind`) in the native `StrategyActor`, so a poison tick kills the actor. `crates/tantra-runtime-actors/src/python.rs:~127` does catch.

## Why

A live strategy sees a bar stream with holes that never occur in research, so indicator state diverges between backtest and live. A closed channel goes unnoticed. A single poison tick terminates the strategy actor, and with no working supervisor ([0029](0029-actor-supervision-restart.md)) it does not come back.

## How to close it

1. Send bars with an awaited `send` (lossless) under a bounded timeout and raise an alarm on timeout; keep explicit `BackpressurePolicy` only for ticks (`CoalesceLatest`/`DropOldest`).
2. Handle a closed channel as a terminal error for the pipeline stage (log, metric, stop).
3. Replace the tick buffer with `VecDeque`.
4. Add a poison-message policy to `StrategyActor`: catch the panic, skip the message, increment a dead-letter counter, and halt the algo after N consecutive failures.

**Verification**: tests in `crates/tantra-runtime-actors/tests/` with a slow strategy asserting no bar loss, and with a panicking strategy asserting the actor survives and the dead-letter counter increments; `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-actors --profile prod`, `make prod`.

## Related

- `.agents/rules/scalability-and-concurrency.md` §1 (bounded channels with explicit policy); [0029](0029-actor-supervision-restart.md).
- Plan [0013](../plans/0013-engine-design-review-2026-11.md) R1 (research/live parity).
