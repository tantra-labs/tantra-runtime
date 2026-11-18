# 0009 — `Experimental-Analysis` Statistical Studies & Event News REST API (`tantra-runtime`)

**Status**: Phase 1 Delivered; Phase 2–3 Planned (2026-11-18) — Track **T20.3**

Master plan: root [`docs/plans/0014-experimental-analysis-statistical-workbench.md`](../../../docs/plans/0014-experimental-analysis-statistical-workbench.md).  
Depends on: [`tantra-data` plan 0010](../../../tantra-data/docs/plans/0010-experimental-analysis-multi-asset-and-events.md), [`tantra-market` plan 0003](../../../tantra-market/docs/plans/0003-experimental-analysis-period-and-markov-engine.md).

---

## 0. Executive Summary & Objective

Expose versioned, OpenAPI-documented REST endpoints under `/api/v2/experimental-analysis/*` in `tantra-runtime-api` and `tantra-runtime-api-rest` serving cross-asset statistical return, **F&O bid-ask spread / roll-basis / statutory cost adjustment**, streak, Markov, distribution, outlier, intraday/ORB, correlation, multi-leg option-spread, and event-study reports computed by `tantra-market-finance` over Lakehouse v3 bars (`2014–2026+`) and live `tantra-adapters` feeds — without adding domain types to `tantra-core`.

---

## 1. Wire Schema (`tantra-runtime-api/src/experimental_analysis.rs`)

All monetary prices, returns, spreads, basis bps, ratios, percentiles, and probabilities serialize as exact decimal strings (`rust_decimal::Decimal` with `schemars::JsonSchema` + `serde-with-str`), emitted into `openapi/v2.json` for `tantra-ui-web`'s `npm run codegen:api`:

- `AssetClassFilter`: `Index` | `FnoStock` | `Etf` | `Equity` | `Forex` | `Commodity`
- `PeriodScheduleKind`: `WeeklyExpiry` | `WeekPlusOne` | `MonthlyExpiry` | `CalendarWeekly` | `CalendarMonthly` | `FrontMonthCycle` | `McxExpiryCycle` | `Daily`
- `ReturnCalcKind`: `OpenToClose` (`oc`) | `HighToLow` (`hl`) | `CloseToClose` (`cc`)
- `ExperimentalCatalogResponse`: instruments grouped by asset class (including F&O `lot_size` & `tick_size`), supported schedules per asset class, data coverage bounds, loaded F&O contract count, and active broker adapter status (`connected` / `reauth_required` / `offline`).
- `ExperimentalReturnsRequest` & `ExperimentalReturnsResponse`:
  - Inputs (Phase 1): `instrument_id`, `asset_class`, `period`, `sub_cycle`, `calc_type`, `start_date`, `end_date`, `ma_window`, `outlier_coverage_pct`, `tail_threshold_pct`, `ohlc_page`, `ohlc_page_size`.
  - Inputs (Phase 2 — F&O Spread & Friction): optional `spread_slippage_bps` (`Decimal`), `roll_basis_mode` (`GrossSpot` | `IncludeRollBasis` | `NetAfterSpreadAndCosts`), and `include_statutory_costs` (`bool`).
  - Outputs: `metadata` (including regime annotation & excluded neutral count), `kpis`, `period_series` (+ optional `net_return_pct`, `spread_cost_bps`, `futures_basis_pct` in Phase 2), `streaks`, `markov`, `percentiles`, `coarse_buckets`, `granular_10bps`, `probability_levels`, `green_vs_red`, `forward_conditional`, `outliers`, `daily_ohlc_summary`, and paginated `daily_ohlc_rows`.
- Additional DTOs (Phase 2–3):
  - `ExperimentalIntradayRequest`/`Response` (includes per-flip F&O spread & cost parameters for ORB-15)
  - `ExperimentalCorrelationRequest`/`Response`
  - `ExperimentalOptionsOverlayRequest`/`Response` (supports single-leg `CoveredCall`, vertical `BullCallSpread` / `BearPutSpread`, `IronCondor`, `CalendarSpread`, returning leg-by-leg `bid`, `ask`, `spread_abs`, `spread_bps`, net credit/debit after spread, statutory fees, PoP, max profit/loss, and strike liquidity flags)
  - `ExperimentalEventStudyRequest`/`Response` & `ExperimentalOutlierNewsRequest`/`Response`

---

## 2. REST Routes (`tantra-runtime-api-rest/src/experimental_analysis.rs` + `v2.rs`)

Mount under `/api/v1` and `/api/v2` (`/experimental-analysis`):

| Route | Method | Phase | Backing Store / Engine |
| :--- | :---: | :---: | :--- |
| `/catalog` | `GET` | **P1 (Live)** | Lakehouse v3 instrument catalog (`CatalogLoader`) + `MarketReader` live adapter status |
| `/returns` | `POST` | **P1 (Live) + P2 Spreads** | Lakehouse v3 `1d` bars + `NseTradingCalendar` + `tantra-market-finance` returns/streaks/Markov/distributions/outliers (+ optional F&O spread & basis deduction in P2) |
| `/intraday` | `POST` | **P2** | Lakehouse v3 `5m` bars (`load_v3_bars_grouped`) → Intraday Movement / Gap / 5-Day / Cycle Progress / 15m ORB Flip (with F&O spread + `costs.rs`) |
| `/correlation` | `POST` | **P2** | Multi-symbol aligned `1d` returns → $N \times N$ correlation matrix, rolling pair series & near–next futures spread |
| `/options-overlay` | `POST` | **P3** | `tantra-adapters` live/snapshot option chains & orderbook bid-ask depth → Covered Call, Multi-Leg Spreads (Vertical / Iron Condor / Calendar) & Premium Lab |
| `/events` | `POST` | **P3** | `tantra-data` Buyback / Earnings / F&O Exclusion tables → CAR event-window evaluator |
| `/outlier-news` | `POST` | **P3** | `tantra-agent` / `ContentReader` (`news_search` + web/RSS search for `[window_start - 2d, window_end + 1d]`) |

---

## 3. Caching, Determinism & Parity Constraints

- **ResponseEnvelope & Status Codes**: `422 Unprocessable Entity` on invalid dates / empty range / unsupported schedule for an asset class / negative spread input; `404` for unknown instrument; `503` (honest availability error, never fake live quotes or bid-ask spreads) when a broker option-chain or depth adapter is offline.
- **Data Pinning**: Optional `data_ref` / `commit_sha` query parameter matching `BacktestRequest.data_ref` (Lakehouse v3 plan 0011) so statistical backtests are 100% reproducible.
- **OpenAPI Conformance**: Static OpenAPI artifact conformance tests + route integration tests in `tantra-runtime-api-rest/tests/experimental_analysis.rs` covering indices, F&O stocks, ETFs, FX, and commodities (plus net-of-spread parity checks in Phase 2–3).
