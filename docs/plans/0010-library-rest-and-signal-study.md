# 0010 — Library REST API, Port Adapter & Signal Study

**Status**: Proposed (2026-11-20) — design only. Parent: root plan 0016 (Track T22, phases T22.3–T22.4).

## 0. Executive Summary & Objective

Implement `IndicatorLibraryPort` (agent plan 0017) at the composition root with a `LibraryAdapter` over
`tantra-market-library` (market plan 0005) and `tantra-market-finance::studies`, and expose it over REST:
library browse/describe, indicator compute, and Signal Study. Also reconcile the T20 experimental-analysis
v1/v2 route split (root plan 0016 §7.3).

## 1. Context & Gaps Covered

- Experimental-analysis routes are nested in `/api/v1` in `tantra-runtime-api-rest/src/v1.rs` (handlers in
  `src/experimental_analysis.rs`, also in `auth.rs` and `openapi.rs`): `GET /experimental-analysis/catalog`;
  `POST /experimental-analysis/{returns,intraday,correlation,options-overlay,forecast,events,outlier-news}`.
  `/api/v2` also exists; plan 0009 says v2 but code is v1.
- No indicator compute endpoint exists. R4 (agent plan 0017): market-needing wiring belongs here.

## 2. Component Specifications & Phases

### 2.1 `LibraryAdapter` (T22.3)

`tantra-runtime-api-rest/src/library.rs` (or a `tantra-runtime-library` crate if REST should not own it; confirm in T22.3):
`struct LibraryAdapter { registry: &'static Registry, bars: Arc<dyn BarSource>, results: Arc<dyn ResultStore>, limits: LibraryLimits }`
implementing `IndicatorLibraryPort`. `BarSource` is an existing-or-new port over the lakehouse (to confirm;
if absent, file a gap in the provider repo per gap-tracking). Wired in bootstrap and injected into the MCP
server as `Arc<dyn IndicatorLibraryPort>`; the REST handlers call the same adapter (one code path).

### 2.2 Library endpoints (T22.3) — `/api/v2`

| Method/path | Notes |
|---|---|
| `GET /api/v2/library/entries?kind&family&q&limit&cursor` | `EntryDescriptor[]` (no body); `q` searches id/aliases/concepts/body |
| `GET /api/v2/library/entries/{id}` | JSON descriptor + body + `dependents`; `?format=md` returns `text/markdown` (concept page "copy for LLM") |

### 2.3 `POST /api/v2/indicators/compute` (T22.3)

```json
{ "id": "ind.predictive-ranges", "params": {"length": 200, "factor": "6", "source": "close"},
  "instrument": "NSE:NIFTY50", "timeframe": "5m", "from": "2024-01-01", "to": "2024-06-30",
  "columns": ["r1","center","s1"], "data_commit": "lakehouse-v3@<ref>" }
```
Response: `{ "entry": {"id","version"}, "data_commit", "ts": [...], "columns": [{"name","unit","values":["Decimal as string"|int|bool]}], "warmup_rows": n }`
or `{ "result_id", "rows", "columns" }` when over the inline cap. Decimals are serialized as strings.
Closed bars only; HTF params resolve to last closed HTF bar.

### 2.4 `POST /api/v2/experimental-analysis/signal-study` (T22.4)

Request:
```json
{ "universe": ["NSE:NIFTY50"], "timeframe": "5m", "from": "2019-01-01", "to": "2026-06-30", "data_commit": "...",
  "detector": {"id": "det.pr-zone-touch", "params": {"side": "support"}},
  "filters": [{"id": "det.pr-trend-regime", "params": {"k": 2}, "expect": "up"}],
  "horizons": [1,3,5,10,20],
  "trigger_level": "pinned_prior_range",
  "stop_target": {"stop": "s2", "target": "r1"},
  "cost_model": {"enabled": true} }
```
Response: `event_count`, per-horizon `{hit_rate, mean_fwd_return, median_fwd_return}` (Decimal strings), `mfe_mae`
distribution, optional `r_multiple` distribution when `stop_target` given, `by_year`/`by_regime` breakdowns,
`events_result_id` (handle), `data_commit`, `entry_versions`, `warnings` (e.g. insufficient events).
Implementation: evaluate detector + filters through the library (events aligned on bar timestamps), then call the
T20 conditional-forward-move and distribution functions in `tantra-market-finance::studies` (only the event
source is new); cost model reuses T20 `SpreadCostModel`. **Pinned trigger level**: breakout detectors compare to
the range active before the bar (library `prev_*` outputs), since a close beyond R2 re-centres the indicator on
that same bar (root plan 0016 §5). Same request + same `data_commit` + entry versions => identical result.

### 2.5 Limits and caps

`LibraryLimits` (config-driven, defaults to confirm in T22.3): max bars per compute (e.g. 200k), max instruments per
study (e.g. 50), max entries per request (e.g. 5), max horizons (10), max DAG depth (8), request body <= 1 MiB via
explicit `to_bytes` cap, inline response <= ~2 MiB else handle. Violations -> HTTP 413/422 with typed error body.

### 2.6 Auth, errors, OpenAPI

All routes extract `Caller`; results handles are namespaced by `(caller_id)` so another tenant cannot read them;
mounted behind `auth_middleware` whenever `config.auth.is_enabled()` and added to the route list in `auth.rs`.
Error mapping from `LibraryPortError`: NotFound->404, InvalidParams->422, InputQuality->422 (code `input_quality`),
LimitExceeded->413/429, Unavailable->503. Register schemas (schemars from `tantra_core::library` + request/response
DTOs) in `openapi.rs`. Blocking compute runs via `spawn_blocking` with bounded concurrency (no sync work on Tokio workers).

### 2.7 v1/v2 reconciliation (T22.4)

Mount the existing T20 experimental-analysis router under `/api/v2` as canonical (handlers unchanged), keep `/api/v1`
paths as deprecated aliases for one release (`Deprecation` header), update plan 0009 text and `auth.rs`/`openapi.rs`
lists, and update `tantra-ui-web` client base path in ui-web plan 0017 T22.5. Exact current mounting confirmed in T22.3.

## 3. Tests & Verification

Tests in `crates/tantra-runtime-api-rest/tests/`: `library_routes.rs` (list/filter/describe/`?format=md`),
`indicators_compute.rs` (golden: known bars -> known PR columns; warm-up; handle path; limits; unknown param 422;
InputQuality 422), `signal_study.rs` (synthetic bars with known touches -> exact counts/forward returns; pinned-level
regression where a close re-centres; reproducibility across runs with pinned commit), `library_auth.rs` (no token 401,
cross-caller handle 404/403), openapi snapshot test. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
`make check`, `make test` (if LTO OOM: `make check` + `cargo test -p tantra-runtime-api-rest --test <bin> --profile prod`), `make prod`.

## 4. Open Questions

1. Does a bar-source port over the lakehouse already exist for REST handlers, or is a gap needed (provider: `tantra-data`)?
2. Adapter location: REST crate vs dedicated runtime crate.
3. Streaming/SSE for long Signal Studies vs async job + handle (handle proposed first).
4. Deprecation length for `/api/v1/experimental-analysis/*` aliases.
