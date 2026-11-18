# 0008 — FIRE Planning Persistence, Portfolio Hydration & REST API (`/planning/fire/*`)

**Status**: Proposed (2026-11-18) — Track **T19.2**

Master plan: root [`docs/plans/0013-fire-planning-and-cashflow-platform.md`](../../../docs/plans/0013-fire-planning-and-cashflow-platform.md).
Depends on: [`tantra-ledger` plan 0006](../../../tantra-ledger/docs/plans/0006-fire-bucket-glidepath-and-swr-metrics.md).

---

## 0. Executive Summary & Objective

Add authenticated, `Caller`-scoped persistence for user FIRE plans and expose versioned `/api/v1/planning/fire/*` & `/api/v2/planning/fire/*` endpoints in `tantra-runtime-api` + `tantra-runtime-api-rest` that combine `tantra_ledger_allocation::fire` simulation with live ledger net worth, options theta Greeks, Finance Act 2025 capital-gains tax drag, and backtest strategy performance — leaving `tantra-core` completely untouched.

---

## 1. Wire Contracts (`tantra-runtime-api`)

1. **`src/profile.rs`**:
   - Extend `Profile` with `#[serde(default)] pub fire_plans: BTreeMap<String, Value>` (or typed `FirePlanRecord` with forward-compatible `#[serde(flatten)]`) preserving revision CAS round-trips.
2. **`src/fire.rs`** (new module, re-exported in `lib.rs`, deriving `schemars::JsonSchema` for OpenAPI 3.1):
   - Wire DTOs + lossless conversions to `tantra_ledger_allocation::fire::*` (string-encoded `Decimal` / `Money` fields matching existing `tantra-runtime-api` conventions).
   - `FirePlanRecord`: `{ id: String, name: String, config: FirePlanConfigDto, cashflow_mix: CashflowBucketConfigDto, linked_account_id: Option<String>, include_tax_drag: bool, updated_at: DateTime<Utc> }`.
   - `FireSimulateRequest`: `{ config: FirePlanConfigDto, cashflow_mix: Option<CashflowBucketConfigDto>, sor_stress: Option<SorStressConfigDto>, strategy_id: Option<String> }` (`#[serde(deny_unknown_fields)]`).
   - `FireSimulateResponse`: `{ report: FireSimulationReportDto, bucket_drift: Option<FireBucketDriftDto>, strategy_benchmark: Option<StrategyYieldAnchor> }`.
   - `FirePortfolioSeedResponse`: `{ account_id: String, net_worth: Money, liquid_cash: Money, equity_market_value: Money, options_margin_used: Money, annualized_theta_estimate: Option<Money>, suggested_mix: CashflowBucketConfigDto, ltcg_exemption_remaining: Money }`.

---

## 2. REST Handlers, Persistence & Auth (`tantra-runtime-api-rest`)

1. **Profile Limits (`src/profile_store.rs`)**:
   - Enforce `MAX_FIRE_PLANS: usize = 25` and `MAX_EXPENSE_ITEMS_PER_PLAN: usize = 100` alongside existing `MAX_PORTFOLIOS` / `MAX_WORKSPACES`.
   - Reject oversized or malformed payloads cleanly with `422` `validation_invalid_request`.
2. **Routes (`src/planning_fire.rs`, mounted in `src/v1.rs` → inherited by `src/v2.rs`)**:
   - `GET /planning/fire/plans` & `PUT /planning/fire/plans/{id}` & `DELETE /planning/fire/plans/{id}`: strict `Caller` isolation via `ProfileStore`, compare-and-swap revision handling.
   - `POST /planning/fire/simulate`: executes `tantra_ledger_allocation::fire::simulate_fire_runway` (+ optional `run_sor_stress` bounded to `iterations <= 2000` inside `tokio::task::spawn_blocking` if >250 iterations so async handlers never block), returning typed `ApiResponse<FireSimulateResponse>`.
   - `GET /planning/fire/from-portfolio/{account}`: reads `PortfolioStore` / `AppState.shared_ledger`, extracts live net worth, buckets holdings (`Product::Cnc` equities vs cash/arbitrage vs NFO/options margin + `greeks.theta`), and computes remaining Finance Act 2025 ₹1.25L LTCG exemption headroom from closed lots.
3. **Auth & Security (`src/auth.rs`)**:
   - Register all `/planning/fire/*` routes in `ROUTE_SCOPES` under appropriate read/write scopes; fail closed whenever auth is enabled (`tokens_file` or JWT).
   - Never use `std::fs` synchronously on handlers or hold `std::sync::Mutex` across awaits.
4. **MCP Composition Root (Agent Rule R4)**:
   - Wire concrete `PortfolioReader` & `TaxReportReader` handles from `AppState` into the new `tantra-agent` chat/MCP FIRE tools.
5. **OpenAPI 3.1 Schema (`src/openapi.rs`, `schema/openapi.v2.json`)**:
   - Register all new paths + component schemas and pass `tests/openapi_conformance.rs`.

---

## 3. Tests & Verification

- `crates/tantra-runtime-api-rest/tests/fire_planning_tests.rs`: auth fail-closed checks, `Caller` tenant isolation, `MAX_FIRE_PLANS` bounds, `/planning/fire/simulate` golden parity, `/planning/fire/from-portfolio/{account}` live valuation + `greeks.theta` extraction, and OpenAPI schema conformance.
- Gate: `cargo fmt --check && cargo clippy --workspace --all-targets --profile prod -- -D warnings`, targeted `--test fire_planning_tests --test openapi_conformance --profile prod`, and `make prod`.
