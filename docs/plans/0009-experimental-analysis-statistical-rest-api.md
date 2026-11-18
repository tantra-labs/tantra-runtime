# 0009 — `Experimental-Analysis` Statistical Studies & Event News REST API (`tantra-runtime`)

**Status**: Proposed (2026-11-18) — Track **T20.3**

Master plan: root [`docs/plans/0014-experimental-analysis-statistical-workbench.md`](../../../docs/plans/0014-experimental-analysis-statistical-workbench.md).  
Depends on: [`tantra-data` plan 0010](../../../tantra-data/docs/plans/0010-experimental-analysis-multi-asset-and-events.md), [`tantra-market` plan 0003](../../../tantra-market/docs/plans/0003-experimental-analysis-period-and-markov-engine.md).

---

## 0. Executive Summary & Objective

Expose versioned, OpenAPI-documented REST endpoints under `/api/v2/experimental-analysis/*` in `tantra-runtime-api` and `tantra-runtime-api-rest` serving cross-asset statistical return, streak, Markov, distribution, outlier, intraday/ORB, correlation, options-overlay, and event-study reports computed by `tantra-market-finance` over Lakehouse v3 bars (`2014–2026+`) and live `tantra-adapters` feeds — without adding domain types to `tantra-core`.

---

## 1. Wire Schema (`tantra-runtime-api/src/experimental_analysis.rs`)

All monetary prices, returns, ratios, percentiles, and probabilities serialize as exact decimal strings (`rust_decimal::Decimal` with `schemars::JsonSchema` + `serde-with-str`), emitted into `openapi/v2.json` for `tantra-ui-web`'s `npm run codegen:api`:

- `AssetClassFilter`: `Index` | `FnoStock` | `Etf` | `Equity` | `Forex` | `Commodity`
- `PeriodScheduleKind`: `WeeklyExpiry` | `WeekPlusOne` | `MonthlyExpiry` | `CalendarWeekly` | `CalendarMonthly` | `FrontMonthCycle` | `McxExpiryCycle` | `Daily`
- `ReturnCalcKind`: `OpenToClose` (`oc`) | `HighToLow` (`hl`) | `CloseToClose` (`cc`)
- `ExperimentalCatalogResponse`: instruments grouped by asset class, supported schedules per asset class, data coverage bounds, loaded F&O contract count, and active broker adapter status (`connected` / `reauth_required` / `offline`).
- `ExperimentalReturnsRequest` & `ExperimentalReturnsResponse`:
  - Inputs: `instrument_id`, `asset_class`, `period`, `sub_cycle`, `calc_type`, `start_date`, `end_date`, `ma_window`, `outlier_coverage_pct`, `tail_threshold_pct`, `ohlc_page`, `ohlc_page_size`.
  - Outputs: `metadata` (including regime annotation & excluded neutral count), `kpis`, `period_series`, `streaks` (`summary`, `details`, `length_histogram`, `stats`), `markov` (`matrix_6x6`, `current_prediction`, `steady_state`), `percentiles`, `coarse_buckets`, `granular_10bps`, `probability_levels`, `green_vs_red`, `forward_conditional`, `outliers`, `daily_ohlc_summary`, and `daily_ohlc_page`.
- Additional DTOs: `ExperimentalIntradayRequest`/`Response`, `ExperimentalCorrelationRequest`/`Response`, `ExperimentalOptionsOverlayRequest`/`Response`, `ExperimentalEventStudyRequest`/`Response`, `ExperimentalOutlierNewsRequest`/`Response`.

---

## 2. REST Routes (`tantra-runtime-api-rest/src/experimental_analysis.rs` + `v2.rs`)

Mount under `/api/v2/experimental-analysis`:

| Route | Method | Phase | Backing Store / Engine |
| :--- | :---: | :---: | :--- |
| `/catalog` | `GET` | **P1** | Lakehouse v3 instrument catalog (`CatalogLoader`) + `MarketReader` live adapter status |
| `/returns` | `POST` | **P1** | Lakehouse v3 `1d` bars + `NseTradingCalendar` + `tantra-market-finance` returns/streaks/Markov/distributions/outliers |
| `/intraday` | `POST` | **P2** | Lakehouse v3 `5m` bars (`load_v3_bars_grouped`) → Intraday Movement / Gap / 5-Day / Cycle Progress / 15m ORB Flip |
| `/correlation` | `POST` | **P2** | Multi-symbol aligned `1d` returns → $N \times N$ correlation matrix & rolling pair series |
| `/options-overlay` | `POST` | **P3** | `tantra-adapters` live/snapshot option chains + `tantra-market-finance` Covered Call / Premium Lab |
| `/events` | `POST` | **P3** | `tantra-data` Buyback / Earnings / F&O Exclusion tables → CAR event-window evaluator |
| `/outlier-news` | `POST` | **P3** | `tantra-agent` / `ContentReader` (`news_search` + web/RSS search for `[window_start - 2d, window_end + 1d]`) |

---

## 3. Caching, Determinism & Parity Constraints

- **ResponseEnvelope & Status Codes**: `422 Unprocessable Entity` on invalid dates / empty range / unsupported schedule for an asset class; `404` for unknown instrument; `503` (honest availability error, never fake live quotes) when a broker option-chain adapter is offline.
- **Data Pinning**: Optional `data_ref` / `commit_sha` query parameter matching `BacktestRequest.data_ref` (Lakehouse v3 plan 0011) so statistical backtests are 100% reproducible.
- **OpenAPI Conformance**: Update static OpenAPI artifact and add route integration tests in `tantra-runtime-api-rest/tests/experimental_analysis.rs` covering indices, F&O stocks, ETFs, FX, and commodities.
