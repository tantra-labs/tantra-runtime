# 0007 — Chat market/content-reader adapters have approximated fields and missing data sources

**Status**: open — items 1–5, 6/7/8/9/11/12/13/17/18/19 closed; items 10 and 14–16 remain (upstream-blocked).

## What's stubbed or hardcoded

`crates/tantra-runtime-api-rest/src/chat_market.rs`'s `FeedMarketReader`/`FeedSnapshotSource`
close the market/screener stand-in of
`docs/gaps/0006-chat-agent-session-persistence-and-portfolio-tools.md` by reading through to the
same `InstrumentMaster`/`BarReader`/`QuoteReader`/`RiskEngine` ports `AppState` already serves
`/market-data/*` and `/risk` from. A few fields still have no real backing source in this runtime
and are either approximated or answered with an explicit error rather than fabricated data:

1. **`MarketQuote::last_price` is the bid/ask midpoint, not a real last-trade print.**
   **CLOSED (plumbing)** (this batch): `FeedMarketReader` gained an optional `TradeReader`
   port (`with_trade_reader`) backed by `tantra-market-store::LiveQuoteStore::put_trade` +
   `ports::TradeReader`/`TradeSnapshot`; `quote` reports the latest print's price and the
   session-day volume (sum of trade sizes) when a reader is attached. The live broker trade
   feed itself is not implemented anywhere (real feeds remain `tantra-market` gaps
   0001/0002), so `ChatRuntime::from_env` wires no reader and an unwired reader honestly
   falls back to the bid/ask midpoint rather than fabricating a print.
2. **`MarketQuote::volume` is always `Decimal::ZERO`.** **CLOSED (plumbing)** (this batch):
   the same `TradeReader` path reports the accumulated session-day volume (`TradeSnapshot::day_volume`),
   so a wired live trade feed yields real volume; an unwired reader keeps the honest `ZERO`
   placeholder until broker trade feeds land.
3. **`MarketReader::indicators` and `MarketReader::macro_series` always answer
   `PortError::unavailable`.** **CLOSED** (this batch):
   - `indicators` now reads daily bars through `BarReader` and computes the requested named
     indicators (`sma_*`, `ema_*`, `rsi_*`, `roc_*`, `cmo_*`, `atr_*`, `adx_*`, `mfi_*`,
     `bb_*`, `obv`, `vwap`, `macd_*`) with `tantra-market-indicators`, converting each
     `f64` output to exact `Decimal` at the boundary. Unsupported/never-primed names yield an
     explicit `PortError::unavailable`, never an empty success.
   - `macro_series` reads the `tantra-data` economic-calendar projections
     (`{root}/catalog/economic_calendar`, `with_economic_calendar`) through
     `EconomicCalendarProjectionReader`, matching events by name/category and returning
     `(scheduled_at, actual|forecast|prior)` points (closes `tantra-data` gap 0018). The
     `economic_calendar` chat tool (`FeedCalendarReader::with_economic_calendar`) reads the
     same projections.
4. **`InstrumentSearchResult::name` falls back to the raw symbol.** **CLOSED** (this batch):
   `tantra_market_store::wire::Instrument` gained an optional `display_name` column
   (`snapshot_schema()`, tolerant read — catalogs without the column still load, name is
   `None`), and `search_instruments` returns the display name when present, falling back to
   the symbol otherwise. Producer-side population of the column from broker master data is a
   follow-on.
5. **`SnapshotData::regime` and `SnapshotData::risk_metrics` are always `None`/empty.**
   **CLOSED** (this batch): `FeedSnapshotSource` gained `with_risk_engine` — `risk_metrics`
   now surfaces the live `RiskEngine` OTR limits (`otr_max_ratio`, `otr_warning_threshold`)
   and `price_band_count` — and `with_regime(bars, instrument)`: a 20-SMA-slope classifier
   over the instrument's daily bars labels `trending_bull`/`trending_bear`/`range_bound`
   (configured via `TANTRA_REGIME_INSTRUMENT`; unset ⇒ `regime` stays `None`).
6. **`MarketReader::block_deals` — CLOSED** (`b561e12`, T7.2): `FeedMarketReader::block_deals`
   now reads the `tantra-data` block/bulk-deal projections (`{root}/catalog/block_deals`) through
   `tantra-market-store::BlockDealsProjectionReader` (`with_block_deals`), filtering by instrument
   and calendar-day date; `GET /market-data/block-deals` serves the same data (T7.3). The
   unconfigured fallback answers `PortError::unavailable`.
7. **`ContentReader::news_search` — CLOSED** (`776d145`, T7.7 item 7): news is ingested
   Python-side (`tantra-data` gap 0006 closed) and exported to `{root}/catalog/news` projections;
   `FeedContentReader::with_news` reads them through `tantra-market-store::NewsProjectionReader`,
   filtering by query/instrument and sorting newest first. `ContentReader::filings` (method name;
   chat tool `filings_search`) reads the filings projections as before.
8. **`MarketReader::options_chain` — CLOSED** (T3.1, `b1b085e`): `FeedMarketReader` builds an
   `InstrumentChainReader` over the same `instruments`/`quotes` ports and `options_chain` serves
   the enumerated chain (midpoint last-price, calendar-day expiry filter, 500 cap), with the
   unenumerable-master case mapped to `PortError::unavailable` (closes
   `tantra-market/docs/gaps/0001-options-chain-reader.md`).
9. **`MarketReader::corporate_actions` — CLOSED** (this batch, T7-remaining): the `tantra-data`
   corporate-actions projections (`{root}/catalog/corporate_actions`) were already readable
   through `tantra-market-store::CorporateActionsProjectionReader` (`with_corporate_actions`);
   `ChatRuntime::from_env` now wires it into the chat market reader, so the chat
   `corporate_actions` tool reads the real projections (closes
   `tantra-data/docs/gaps/0008-corporate-actions-rust-bridge.md`). The unconfigured fallback
   answers `PortError::unavailable`.
10. **`MarketReader::market_depth` always answers `PortError::unavailable`** in
    `FeedMarketReader`. Added with chat tool `market_depth` (`tantra-agent` plan 0007 Track 2
    Phase 4): the feed ports serve top-of-book quotes only; no L2 depth source exists
    (`tantra-market/docs/gaps/0006-l2-order-book-depth.md`). The seedable
    `RuntimeMarketReader` accepts a `set_market_depth` store for tests.
11. **`MarketReader::institutional_flows` — CLOSED** (this batch): the `tantra-data`
    institutional-flow projections (`{root}/catalog/institutional_flows`, exported by
    `LakehouseBridgeExporter::export_institutional_flows`) are read through
    `tantra-market-store::InstitutionalFlowsProjectionReader` (`with_institutional_flows`),
    filtered to the cash segment and optionally a date, and mapped to the chat
    `InstitutionalFlow` DTO (participant → `fii`/`dii`, exact `Money` values) — closes
    `tantra-data` gap 0017. The unconfigured fallback
    answers `PortError::unavailable`. The seedable `RuntimeMarketReader` `set_institutional_flows`
    store remains for tests.
12. **`MarketReader::delivery_stats` — CLOSED** (this batch): the `tantra-data` delivery
    projections (`{root}/catalog/delivery`, exported by
    `LakehouseBridgeExporter::export_delivery`) are read through
    `tantra-market-store::DeliveryProjectionReader` (`with_delivery`), filtered by instrument
    and date range, and mapped to the chat `DeliveryBar` DTO (exact `Quantity`/`Decimal`) —
    closes `tantra-data` gap 0019. The unconfigured fallback
    answers `PortError::unavailable`. The seedable `RuntimeMarketReader` `set_delivery_stats`
    store remains for tests.
13. **`MarketReader::oi_buildup` — CLOSED** (`776d145`, T7.7 item 13): `FeedMarketReader::with_oi_store`
    opens `ParquetOpenInterestStore::open(root)` and wires a `BuildupAnalyzer` over it and the
    daily-bar source; `oi_buildup` computes the four-quadrant classification plus absolute OI and
    percentage changes from the raw series. The seedable `RuntimeMarketReader` `set_oi_buildup`
    store remains for tests.
14. **`FundamentalReader` (chat tools `fundamental_ratios`/`financial_statements`) always
    answers `PortError::unavailable`** via `crate::chat_fundamental::FeedFundamentalReader`.
    Added with `tantra-agent` plan 0007 Track 3 Phase 6: no fundamentals/financial-statement
    data source exists anywhere yet (`tantra-data/docs/gaps/0020-fundamentals-ingestion.md`).
15. **`FundReader` (chat tools `fund_search`/`fund_holdings`) always answers
    `PortError::unavailable`** via `crate::chat_fund::FeedFundReader`. Added with plan 0007
    Track 3 Phase 7: no mutual-fund NAV/holdings data source exists anywhere yet
    (`tantra-data/docs/gaps/0021-mutual-fund-data-ingestion.md`).
16. **`IpoReader` (chat tools `ipo_calendar`/`ipo_subscription`) always answers
    `PortError::unavailable`** via `crate::chat_ipo::FeedIpoReader`. Added with plan 0007
    Track 3 Phase 8: no IPO calendar/subscription data source exists anywhere yet
    (`tantra-data/docs/gaps/0022-ipo-calendar-and-subscription-ingestion.md`).
17. **`TradeIntentSink` (chat tool `propose_trade`) — CLOSED** (this batch, T7-remaining):
    `FeedTradeIntentSink` enqueues validated intents into the `FileJobRegistry`-backed approval
    queue (`{TANTRA_JOBS_PATH}` or `~/.tantra/jobs.json`) as `JobStatus::PendingApproval` jobs
    with `approval_policy = Manual` and a `x-papertrading` metadata flag on paper intents — the
    NoFX human gate, never an autonomous live dispatch. `ChatRuntime::from_env` wires the
    registry handle; a registry that cannot open leaves the sink answering its gap citation.
18. **`StrategyStore` (chat tool `strategy_manage`) — CLOSED** (this batch, T7-remaining):
    `FeedStrategyStore` is an in-memory `StrategySpec` CRUD store (`save`/`list`/`get`/`delete`,
    `created_at` preserved on update) wired into `ChatRuntime::from_env`. Persistence across
    restarts is a follow-up.
19. **`TaxReportReader` (chat tools `tax_report`/`ledger_statement`) — CLOSED** (this batch,
    T7-remaining): `SharedLedger::record_fill` accumulates `tantra-ledger-portfolio`'s matched
    lots (`MatchedLot`) into the shared ledger's closed-lot history, and
    `FeedTaxReportReader` exposes them per account over a date range (docs/gaps/0006 design
    decision + `docs/design/code-shared-ledger.md`). The STCG/LTCG math itself is implemented
    and unit-tested in `tantra-agent-chat-tools`.

## Why

Closing docs/gaps/0006's market/screener stand-in only required wiring the tool catalog to data
AppState already has live access to (instrument reference data, quotes, bars, risk-engine price
bands). The fields above need either a new data source this runtime doesn't have yet (a live
broker last-trade/volume feed, an L2 depth source, fundamentals/mutual-fund/IPO ingestion) or a
product decision (which risk metrics to surface to the chat agent). The 2026-11-14 batch closed
every item whose data already existed somewhere: the indicator library and daily bars (item 3a),
the `tantra-data` institutional-flow/delivery/economic-calendar projections and their Rust
readers (items 3b/11/12), the instrument display-name column (item 4), the risk-engine metrics +
a SMA-based regime classifier (item 5), and the last-price/volume read-through plumbing (items
1–2). The trade/strategy/tax tools were runtime-side-only: they needed an approval-queue handle,
a strategy store, and the shared ledger — all wired earlier.

## How to close it (remaining)

- Item 10: close `tantra-market/docs/gaps/0006` (L2 depth source — the `DepthReader` port and
  `DepthSnapshot` shape already exist in `tantra-market-store`; only a data source implementing
  it is missing), then replace `FeedMarketReader::market_depth`'s stub with a real read and
  compute `liquidity_imbalance_pct`.
- Items 14–16: close `tantra-data/docs/gaps/0020`–`0022` (fundamentals / mutual-fund / IPO
  ingestion — vendor selection + ingestion first, then the bridge-exporter → projection-reader →
  runtime wiring chain), then wire `FeedFundamentalReader`/`FeedFundReader`/`FeedIpoReader` to
  real reads.
- Follow-on (no stub): wire a live broker trade feed (`tantra-market` gaps 0001/0002) into a
  `TradeReader` so `quote` reports real last prints/day volume instead of the honest midpoint
  fallback.
