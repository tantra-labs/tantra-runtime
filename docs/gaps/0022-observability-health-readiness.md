# 0022 — No readiness/liveness split, no served metrics, no request-id, broker metrics invisible, no TLS/drain contract

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: deployment · **Severity**: medium
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-api-rest/src/routes.rs:42-52,197` — `/health` returns `{"status":"ok","library": ..}`, is public, and never checks stores, DB (`state.database`), bar pump, run queue or feed freshness.
- `rg "/metrics|prometheus" crates/tantra-runtime-api-rest/src` finds nothing; `rg "RequestId|x-request-id"` = 0 hits; `TraceLayer::new_for_http()` (`crates/tantra-runtime-api-rest/src/lib.rs:241`) uses default spans only.
- `crates/tantra-runtime-actors/src/metrics.rs:269` — `MetricsCollector::router()` exposes `GET /metrics`, but there is no non-test caller in any crate (`MetricsCollector::new` likewise only in tests).
- `crates/tantra-runtime-actors/src/supervisor.rs:42-48` — `Supervisor` exposes no health state (`ActorStatus` is returned only at task exit); `shutdown_gracefully` takes `self` by value and returns `Result<(), String>`.
- `tantra-exec/crates/tantra-exec-core/src/telemetry.rs:18-100` registers `broker_http_request_duration_seconds` and `rate_limit_*` on the default `prometheus` registry; `crates/tantra-runtime-actors/src/metrics.rs:11,269-299` serves its own `Registry::gather()`, and no bridge exists, so broker metrics are invisible. No OTLP exporter or tracing subscriber is installed anywhere.
- `crates/tantra-runtime-api-rest/src/lib.rs:384-387` — plain `axum::serve(TcpListener)`; no TLS, no trusted-proxy handling; shutdown covers SIGINT per `bootstrap.rs:4` (SIGTERM in `stop_signal` unconfirmed).
- `crates/tantra-runtime-api-rest/src/bootstrap.rs:306,600` — default bind `127.0.0.1:8000`; `println!("listening ..")` instead of tracing.

## Why

Kubernetes needs `/livez` and `/readyz` (gate traffic until DB, lakehouse and feed are up, and while draining), Prometheus scraping and trace correlation. Today a wedged order path still reports `ok`, operators cannot alert on rate-limit queueing or broker latency, and there is nothing to hang a SIGTERM drain on.

## How to close it

1. Add `/livez` (no deps) and `/readyz` (`Database::ping`, lakehouse root readable, run queue not saturated, feed lag, broker session via a `BrokerHealthProbe`).
2. Add `Supervisor::health() -> HealthSnapshot` (watch channel) and a typed `ShutdownError`.
3. Serve `/metrics` (admin/internal scope or a separate admin bind) from the actors registry merged with `tantra-exec` `telemetry::registry()` (own `Registry`, no globals; tantra-exec owns the counters).
4. Add `SetRequestIdLayer` + `PropagateRequestIdLayer` and a request-id span field; JSON log config via `tracing`; document OTLP setup.
5. Accept SIGTERM: stop accepting, drain, flush audit, exit non-zero on timeout. Document TLS termination at ingress or add a rustls acceptor.

**Verification**: `readyz` returns 503 when the DB is down; `/metrics` contains broker and actor series; request-id propagates; SIGTERM drain test. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted `cargo test -p tantra-runtime-api-rest --profile prod`.

## Related

- Root plan 0022 (observability, k8s probes); root plan 0019 P5.1.
- tantra-exec plan 0004 R1 (broker counters and probe); tantra-ui plan 0011 R4 (serve daemon basics, health/readiness/log consumers).
- Gap 0026 (single composition root hosts the shutdown/listen path).
