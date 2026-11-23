# 0012 — tantra-runtime API design review (2026-11): tenant scoping, fail-closed serving, bounded work & one composition root

**Status**: Proposed (2026-11-23)
**Scope**: `tantra-runtime` (`tantra-runtime-api`, `tantra-runtime-api-rest`, `tantra-runtime-client`) · **Umbrella**: root `docs/plans/0021-workspace-design-review-2026-11.md`

## 0. Executive Summary & Objective

The REST/API layer still serves several tenant, availability and deployment hazards: runs and strategies are visible across callers, CPU-heavy studies run on Tokio workers with no deadlines, an unauthenticated `0.0.0.0` bind only warns, order/approval/halt state is lost on restart, there are no readiness or metrics surfaces, route inventories are triplicated, Decimal money is cast to f64 on the wire, and two composition roots diverge. This plan sequences gaps 0018-0027 plus six plan-only items (R1-R6) so that the API can run as an authenticated, restart-safe, multi-replica service.

Evidence lines reference the 2026-11-23 working tree (line numbers in `crates/tantra-runtime-api-rest` include uncommitted edits from another session).

Engine-side items (actors, backtest, jobs, audit) are in sibling plan [0013](0013-engine-design-review-2026-11.md) (gaps 0028-0037). Non-goals: file splitting of `strategy_loop.rs`/`openapi.rs` and the P2.7 file-store wiring, which stay with root plan 0019.

## 1. Context & Gaps Covered

| Gap | Title | Kind | Severity |
|---|---|---|---|
| [0018](../gaps/0018-caller-scoping-runs-and-catalog.md) | Runs and strategy catalog not Caller-scoped | design-flaw | high |
| [0019](../gaps/0019-bounded-compute-and-request-limits.md) | CPU work on Tokio workers; no timeouts/concurrency limits | design-flaw | high |
| [0020](../gaps/0020-fail-closed-bind-and-cors.md) | Insecure non-loopback bind only warns; CORS lacks Authorization | design-flaw | high |
| [0021](../gaps/0021-durable-trading-state.md) | Order/approval/halt/job state in memory only | design-flaw | high |
| [0022](../gaps/0022-observability-health-readiness.md) | No readiness/metrics/request-id; broker metrics invisible | deployment | medium |
| [0023](../gaps/0023-single-route-inventory.md) | Three route inventories; stale capability manifest | duplication | medium |
| [0024](../gaps/0024-decimal-to-f64-on-wire.md) | Decimal money cast to f64 on wire and glue | design-flaw | medium |
| [0025](../gaps/0025-async-purity-auth-sse.md) | Sync I/O and wall clock in async request paths | design-flaw | medium |
| [0026](../gaps/0026-single-composition-root-serve.md) | Two composition roots; partial `tantra serve`; scattered env | design-flaw | high |
| [0027](../gaps/0027-options-chain-oi-change-and-iv-fields.md) | Options chain lacks OI-change/LTP-change/IV; web fabricates | design-flaw | high |

Plan-only items (no standalone gap doc):

- **R1 — Single global SEBI algo id and `AppState` god object**: `REST_ALGO_ID = "rest-api"` at `crates/tantra-runtime-api-rest/src/orders.rs:83` for every caller; `AppState` has 33 fields mixing ports, concrete stores, config and channels (`state.rs:133-237`), 11 `Option<..>` fields force per-handler 503 branching. `RiskEngine::default()` at `state.rs:247`.
- **R2 — Chat data adapters live in the REST crate**: 3806 LOC of `chat*.rs` in `tantra-runtime-api-rest`; production dependency on `tantra-runtime-testing` in its `Cargo.toml`.
- **R3 — Fire-and-forget background tasks**: three `tokio::spawn` per `/ws/chat` connection (`chat.rs:898,904,921`), detached supervisor in `library_client.rs:148,334`, bar pump handle not tracked (`bar_pump.rs:62`, `state.rs:207`); no `TaskTracker`/`CancellationToken`.
- **R4 — Hand-maintained client call layers**: Rust `ROUTES` in `crates/tantra-runtime-client/src/lib.rs:25-35` and Python call layer are hand-written; generation from OpenAPI is the follow-up to gap 0023 (root gap 0022).
- **R5 — Journal/portfolio/P&L computed in browser localStorage**: `tantra-ui-web/src/lib/portfolios-store.ts:187` and `tantra-ui-web/src/lib/journal-store.ts:108` persist whole arrays client-side and compute stats with `decimal.js`; server endpoints must become the source of truth.
- **R6 — Data ingest scheduling and provenance surface**: `tantra-data` has no orchestration, alerting or health; ingest jobs belong in `tantra-runtime-jobs` calling a unified `tantra-ingest` entry point.

## 2. Component Specifications & Phases

### Phase A — Security and quick fixes (gaps 0020, 0018, 0024, 0027)
1. **0020**: `allow_insecure_bind`, `Result`-returning router build, `AUTHORIZATION` in CORS.
2. **0018**: owner on `RunManifest`/`RunStore`, per-caller `StrategyCatalog`, admin bypass.
3. **0024**: decimal-string approvals, `Decimal` leverage/tape fields; file the `tantra-market-store` wire `Bar` gap.
4. **0027**: add OI-change, LTP-change, IV fields and `unavailable` markers; regenerate OpenAPI and TS types.
5. **R1 (part)**: derive algo id per `(caller, strategy)` via an `AlgoRegistry` port; stop sharing one `RiskEngine` OTR bucket across callers.

**Gate**: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted `cargo test -p tantra-runtime-api-rest --test runs_tenant_isolation --profile prod`, `--test bootstrap`, `--test openapi_conformance`, `make prod`.

### Phase B — Structural refactors (gaps 0019, 0023, 0025, 0026; R1-R4)
1. **0019**: `ComputePool` semaphore + `spawn_blocking`, `TimeoutLayer`, `ConcurrencyLimitLayer`, per-caller bucket.
2. **0026**: `ServerRuntime::run`, `EnvConfig::from_env()`, shipped `tantra-runtime` bin, `tantra serve` delegating.
3. **0025**: `Clock` port, `ArcSwap` token reload, audit writer task, async SSE tail.
4. **0023**: `RouteTable`-driven OpenAPI and `ENDPOINTS`; then **R4** client-call generation and CI schema-drift.
5. **R1**: split `AppState` into capability sub-states with `FromRef`; replace `Option<Arc<Concrete>>` with ports and an `Unavailable` impl.
6. **R2**: new `tantra-runtime-chat-adapters` crate (ports only); move `tantra-runtime-testing` to `[dev-dependencies]`; cargo-tree dependency test.
7. **R3**: `AppState.tasks` (`TaskTracker` + `CancellationToken`), per-caller chat-session `Semaphore`, drain in `serve_with_shutdown`.

**Gate**: Phase A gate plus `cargo test -p tantra-runtime-api-rest --test overload --profile prod`, golden `schema/openapi.v2.json` unchanged, config precedence test. If the full test-bin LTO link OOMs, run `make check` plus the targeted binaries and record the exact commands.

### Phase C — Deployment readiness (gaps 0021, 0022; R5, R6)
1. **0021**: repository ports + migrations (`trading_state`, `approval_queue`, `jobs`), boot restore fail-closed, retention, cross-replica halt.
2. **0022**: `/livez`, `/readyz`, `/metrics` (actors + broker registry), request-id, SIGTERM drain, `Supervisor::health()`.
3. **R5**: expose journal/portfolio/P&L endpoints (ledger performance) and make `tantra-ui-web` a write-through cache.
4. **R6**: define the ingest job in `tantra-runtime-jobs` (freshness SLA alerting) once `tantra-ingest` exists.

**Gate**: restart round-trip tests, `readyz` 503 on DB down, `cargo test -p tantra-runtime-persistence --profile prod`, `make prod`.

## 3. Relationship to Other Plans

- Not duplicated: root plan 0019 P3.1 (file splits), P2.7 (file-store/persistence wiring), P0.7 (JWT issuer name), P5.1.
- Feeds root plan 0022 (deployment: HA, observability, probes) and root plan 0023 (CI for this repo). Root gap 0022 (canonical OpenAPI + generated clients) is driven by gap 0023 and R4.
- Provider/consumer links: `tantra-ui` plan 0011 R1 (gap 0026) and R4 (gap 0022); `tantra-ui-web` gap 0079 (gap 0027) and plan 0018 (R5); `tantra-exec` plan 0004 R1 (broker metrics exposure, gap 0022); `tantra-data` plan 0011 (R6); `tantra-market` wire `Bar` (gap 0024).
- Engine items: sibling plan [0013](0013-engine-design-review-2026-11.md).

## 4. Exit Criteria

1. Runs, sweeps, journals and strategies are invisible across callers; `runs_tenant_isolation` green.
2. A non-loopback bind without auth fails startup unless the explicit override is set; CORS preflight allows `authorization`.
3. No CPU study runs on a Tokio worker without the `ComputePool`; `/health` latency bounded under `overload` tests.
4. Restarting the process preserves idempotency keys, halts and pending approvals; exec maps are bounded.
5. `/livez`, `/readyz` and `/metrics` exist and reflect DB, feed and broker health; SIGTERM drains cleanly.
6. OpenAPI paths and `ENDPOINTS` are generated from `RouteTable`; clients are drift-checked in CI.
7. No `to_f64().unwrap_or(0.0)` on monetary values in `tantra-runtime-api-rest`.
8. `tantra serve` and the runtime bin share one `ServerRuntime` and one config loader.
9. `/api/v2/options/chain` returns real OI-change/LTP-change/IV or explicit `unavailable`.
10. `make prod` green; closed gaps removed and recorded in `docs/gaps/README.md`.
