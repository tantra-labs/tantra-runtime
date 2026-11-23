# 0019 — Heavy CPU study/sweep work runs inline on Tokio workers; no request timeout or concurrency limit

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-api-rest/src/strategy_loop.rs:1307,1438` — `async fn post_sweep` calls the synchronous `ParameterSweepRunner::run` (up to 64 candidates x IS/OOS evaluation) directly; same for `post_evaluate` (`:1083`). There is no `spawn_blocking` anywhere in `strategy_loop.rs` or `experimental_analysis.rs` (`post_returns` 981, `post_correlation` 1776, `post_forecast` 2192, `post_events` 2347 ...).
- `crates/tantra-runtime-api-rest/src/lib.rs:239-241` — layers are only `CompressionLayer`, `CorsLayer`, `TraceLayer`; no `TimeoutLayer`, `ConcurrencyLimitLayer`, `LoadShed` or per-caller rate limit (`rg TimeoutLayer|ConcurrencyLimit` over the crate: 0 hits).
- Only `RunService` (`service.rs`) has a bounded queue (`RateLimited` "run queue is full", `service.rs:283`).

## Why

A handful of concurrent `/studies/strategy-loop/sweep` calls pins every worker thread and starves `/orders`, `/stream` and `/health`; the risk-gated order path shares the runtime with unbounded research compute. A slow client or huge sweep has no deadline. Violates `.agents/rules/scalability-and-concurrency.md` §2.1 (no blocking work on Tokio workers).

## How to close it

1. Wrap CPU handlers in `spawn_blocking` behind a `tokio::sync::Semaphore` held in `AppState` (`ComputePool`); answer 429 `rate_limited` when saturated.
2. Add `tower::timeout`/`TimeoutLayer` with per-route classes (10 s default, 120 s studies) and a global `ConcurrencyLimitLayer`.
3. Add a per-Caller token-bucket middleware reading `TokenInfo`.

**Verification**: `tests/overload.rs` — N parallel sweeps while `/health` latency stays bounded; saturation returns 429; a slow handler hits the timeout. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted `cargo test -p tantra-runtime-api-rest --test overload --profile prod`.

## Related

- Root plan 0019 P3.1 only splits these files (different concern).
- Gap 0025 (sync I/O in auth/SSE paths) is the same rule on the I/O side.
