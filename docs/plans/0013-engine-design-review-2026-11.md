# 0013 — tantra-runtime engine design review (2026-11): supervised actors, safe synthetic OCO, durable jobs & audit

**Status**: Proposed (2026-11-23)
**Scope**: `tantra-runtime` (`tantra-runtime-actors`, `-strategy`, `-jobs`, `-persistence`, `-backtest`, `-enhancer`, `-config`, `-testing`, facade) · **Umbrella**: root `docs/plans/0021-workspace-design-review-2026-11.md`

## 0. Executive Summary & Objective

The engine half of `tantra-runtime` (actor pipeline, strategy dispatch, jobs, persistence, backtest, enhancer) works in the happy path but has structural weaknesses on the live-trading path: the supervisor does not restart anything, the risk actor blocks its own kill switch behind broker acks, the strategy crate links a concrete broker with a nested runtime and leaks tokens through `Debug`, synthetic OCO trailing can leave a position without a stop, and the jobs registry and audit ledger cannot support more than one process. This plan sequences the fixes in three phases: safety-critical quick fixes, structural unification, then deployment readiness.

Non-goals: items already owned by root plan 0019 (for example P3.3 typed errors, P4.2 WalkForward/MonteCarlo de-duplication) are referenced, not duplicated. REST/API-side findings live in sibling plan [0012](0012-api-design-review-2026-11.md); that plan owns durable trading state and observability as gaps [0021](../gaps/0021-durable-trading-state.md) and [0022](../gaps/0022-observability-health-readiness.md), which this plan only references. Evidence paths in this plan are repo-relative.

## 1. Context & Gaps Covered

| Gap | Title | Kind | Severity |
|---|---|---|---|
| 0028 | Strategy crate: concrete `ZerodhaExecutor`, nested runtime, secret `Debug` — **closed 2026-11-26** (see [gaps index](../gaps/README.md)) | design-flaw | high |
| [0029](../gaps/0029-actor-supervision-restart.md) | Supervisor never supervises; restart cannot recover state | design-flaw | high |
| [0030](../gaps/0030-risk-actor-blocking-broker-ack.md) | Risk actor blocks mailbox on broker ack | design-flaw | high |
| [0031](../gaps/0031-synthetic-oco-trail-stop-safety.md) | Synthetic OCO `trail_stop` unprotected window and state corruption | design-flaw | high |
| [0032](../gaps/0032-jobs-single-runner-and-ist-cron.md) | Jobs registry single JSON file; UTC-only cron | deployment | high |
| [0033](../gaps/0033-audit-ledger-unification.md) | Audit ledger SQLite-only, divergent schemas, not tamper-evident | design-flaw | high |
| [0034](../gaps/0034-discarded-oms-and-audit-errors.md) | Execution actor discards OMS transitions and audit errors | design-flaw | medium |
| [0035](../gaps/0035-strategy-actor-backpressure.md) | Bar drops, ignored send errors, no panic isolation in `StrategyActor` | design-flaw | medium |
| [0036](../gaps/0036-portfolio-actor-bounded-history.md) | `PortfolioActor` unbounded `fills_history` | design-flaw | medium |
| [0037](../gaps/0037-backtest-qty-truncation-and-venue-fees.md) | Backtest quantity truncation and NSE fees for all venues | design-flaw | medium |

Plan-only items (no standalone gap doc):

- R1 — No `Clock` port; backtest bypasses the actors (research/live parity) — wall-clock `Utc::now()` at `crates/tantra-runtime-actors/src/strategy.rs:157,169`, `crates/tantra-runtime-actors/src/python.rs:389,396`, `crates/tantra-runtime-actors/src/risk.rs:213,246,256`, `crates/tantra-runtime-actors/src/execution.rs:165`; backtest at `crates/tantra-runtime-backtest/src/engine.rs:52,262` reuses `StrategyRunner` but not the actors.
- R2 — Enhancer duplicates DSR/PBO with `f64` re-implementations — `crates/tantra-runtime-enhancer/src/overfitting_guardrails.rs:11,101` vs `tantra-core/src/analytics/pbo.rs:1` and `tantra-agent/crates/tantra-agent-analytics/src/{dsr,pbo}.rs`; GA/Odysseus in `crates/tantra-runtime-enhancer/src/{ga_optimizer,odysseus}.rs` vs `tantra-agent/crates/tantra-agent-genetic`; global rayon pool at `ga_optimizer.rs:5`/`odysseus.rs:18` is not bounded by `BoundedWorkerPool` (`crates/tantra-runtime-jobs/src/pool.rs:102`).
- R3 — Process config split — default root `"data/catalog"` is CWD-relative at `crates/tantra-runtime-config/src/lib.rs:46`; `std::env::var("TANTRA_DATA_DIR")` inside the library at `crates/tantra-runtime-backtest/src/calendar.rs:390`; `BacktestRunConfig` (`crates/tantra-runtime-config/src/lib.rs:20`) covers only a run.
- R4 — `tantra-runtime-testing` is a normal dependency of the facade — `crates/tantra-runtime/Cargo.toml` `[dependencies]` and `crates/tantra-runtime/src/lib.rs:16` (`pub use tantra_runtime_testing as testing;`).
- R5 — Durable trading state (approvals, halts, jobs, run records) is filed as gap [0021](../gaps/0021-durable-trading-state.md); `/metrics`, health and drain as gap [0022](../gaps/0022-observability-health-readiness.md). Engine-side work in those gaps is scheduled in Phase C below.

## 2. Component Specifications & Phases

### Phase A — Safety-critical, low-risk fixes
1. ~~**0028**~~ (closed 2026-11-26: executor injected as `Arc<dyn BrokerExecutor>` + `Handle`) — move `ZerodhaKiteAdapter` to the composition root, drop the `tantra-exec-zerodha` dependency and `Runtime::new`/`block_on`, redact `Debug`. Add the dependency-rule test first (TDD).
2. **0030** — spawn the ack await, add a priority control channel for halts, add a timeout.
3. **0034** — propagate OMS errors; make audit failure on `order_attempt` order-blocking.
4. **0037** — `QuantityError` and `unsupported_jurisdiction` instead of silent defaults.
5. **0036** — remove the `unwrap`, add `PortfolioError::InvalidQuantity`, bound the history.
6. **R4** — gate `tantra-runtime-testing` behind a default-off `testing` feature.

**Gate**: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-strategy --profile prod`, `cargo test -p tantra-runtime-actors --profile prod`, `cargo test -p tantra-runtime-backtest --profile prod`, `make prod`.

### Phase B — Structural refactors and de-duplication
1. **0029** — supervisor run loop, factory-owned channels, snapshot trait, halt-on-crash, typed errors. Depends on the snapshot persistence in 0021.
2. **0035** — lossless bar delivery, `VecDeque`, poison-message policy.
3. **0031** — submit-then-cancel trailing with versioned ids and a two-phase state machine.
4. **R1** — inject `Arc<dyn Clock>` (a `tantra-core` port) into actors; stamp event time from the tick/bar; add a replay test driving the actor pipeline from a recorded tick log and comparing against `BacktestEngine` fills (golden vector).
5. **R2** — enhancer calls the `tantra_core::analytics` gates and keeps only the search algorithms; run searches via `SweepContext.pool`; add a cross-implementation golden-vector test. Coordinates with root plan 0019 P4.2.
6. **R3** — one validated `RuntimeConfig` (env plus `tantra.jsonc`, schemars) in `tantra-runtime-config`; libraries take explicit absolute paths; precedence tests. Shares the composition root work in gap [0026](../gaps/0026-single-composition-root-serve.md).

**Gate**: Phase A commands plus `cargo test -p tantra-runtime-enhancer --profile prod`, `cargo test -p tantra-runtime-config --profile prod`, chaos tests in `crates/tantra-runtime-actors/tests/`.

### Phase C — Deployment readiness
1. **0032** — `JobRegistry` trait, `SqlxJobRegistry`, IST-aware cron, worker mode.
2. **0033** — `AuditSink` port, hash-chained and trigger-protected implementations, unified migration numbering, retention job.
3. **R5** — land the engine-side parts of 0021 (halt/approval/job stores, fail-closed load on boot) and 0022 (`Supervisor::health()`, SIGTERM drain, `/metrics` on an admin port) in coordination with plan 0012.
4. Set `RAYON_NUM_THREADS` or use `SweepContext.pool` for cgroup-limited containers; document single-process assumptions until 0032, 0033 and 0021 land.

**Gate**: Phase A commands plus `cargo test -p tantra-runtime-jobs --profile prod`, `cargo test -p tantra-runtime-persistence --profile prod`, two-registry and tamper-detection tests.

## 3. Relationship to Other Plans

- Root plan 0019: P3.3 (typed errors) and P4.2 (WalkForward/MonteCarlo) are not duplicated; 0031 and R2 extend them. Supervisor linking (§4.2) is independent of 0029.
- Root plan 0022 (deployment architecture) consumes 0032, 0033, R5; root plan 0023 (build/CI) is not touched here.
- Root gaps: 0010 (idempotency/unknown outcome, 0030, 0031), 0011 (credentials, 0028), 0013 (risk gate, 0030), 0020 (time semantics, 0032, R1), 0023 (audit, 0033), 0015 (pricing/quant math, R2).
- Provider repos: `tantra-exec` gap 0009 (audit WAL) and gap 0007 (secret types); `tantra-ledger` gap 0015 (`apply_fill` side) and gap 0016 (OMS typestate).
- Sibling plan [0012](0012-api-design-review-2026-11.md) owns API-side gaps 0018-0027.

## 4. Exit Criteria

1. `tantra-runtime-strategy` has no `tantra-exec-*` concrete dependency, no `Runtime::new`/`block_on`, and no secret in `Debug` output; a CI dependency-rule test enforces it.
2. Killing any actor in a chaos test results in restart with recovered state, or a global halt for risk/execution actors.
3. A hung broker does not delay `HaltGlobal` beyond 50 ms.
4. A failing step in `trail_stop` never leaves a position without a resting stop or a tracker out of sync.
5. Two registries over one store never both claim the same job; IST cron fires at the intended UTC time.
6. The audit trail is append-only and hash-chained on SQLite and Postgres with one migration numbering; tampering is detected by a golden-vector test.
7. No silent quantity truncation or NSE fee defaulting in backtests; bar delivery in live paths is lossless or explicitly alarmed.
8. Actor pipeline replay reproduces `BacktestEngine` fills for a recorded tick log.
9. `make prod` is green and every closed gap is moved to the Closed table in `docs/gaps/README.md`.
