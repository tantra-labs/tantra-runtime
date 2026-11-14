# 0007 — Chat market/content-reader adapters have approximated fields and missing data sources

**Status**: open

## What's stubbed or hardcoded

`crates/tantra-runtime-api-rest/src/chat_market.rs`'s `FeedMarketReader`/`FeedSnapshotSource`
close the market/screener stand-in of
`docs/gaps/0006-chat-agent-session-persistence-and-portfolio-tools.md` by reading through to the
same `InstrumentMaster`/`BarReader`/`QuoteReader`/`RiskEngine` ports `AppState` already serves
`/market-data/*` and `/risk` from. A few fields still have no real backing source in this runtime
and are either approximated or answered with an explicit error rather than fabricated data:

1. **`MarketQuote::last_price` is the bid/ask midpoint, not a real last-trade print.**
   `QuoteReader::read_quote` returns only a top-of-book `QuoteTick` (bid/ask price and size); there
   is no last-traded-price feed wired into it.
2. **`MarketQuote::volume` is always `Decimal::ZERO`.** `QuoteTick` carries no traded-volume field
   either, so there is nothing real to report; zero is a placeholder, not an observed value.
3. **`MarketReader::indicators` and `MarketReader::macro_series` always answer
   `PortError::unavailable`.** No indicator engine or macro-economic data source exists anywhere
   in `tantra-runtime` yet for the chat tools to read from.
4. **`InstrumentSearchResult::name` falls back to the raw symbol.** `tantra_market_store::wire::Instrument`
   has no human-readable display-name field, only `id()` (the `InstrumentId`), `kind()`,
   `currency()`, `lot_size()`, `tick_size()`.
5. **`SnapshotData::regime` and `SnapshotData::risk_metrics` are always `None`/empty.**
   `FeedSnapshotSource` only populates `universe` (from `InstrumentMaster::list_instruments`); no
   market-regime classifier or curated risk-metrics feed exists yet to back the other two fields.
6. **`MarketReader::block_deals` always answers `PortError::unavailable`.** Added with chat tool
   `block_deals` (plan 0004 Phases 1–3): no block/bulk deal disclosure data source is ingested
   anywhere yet — the upstream ingestion decision lives in
   `tantra-data/docs/gaps/0005-block-bulk-deal-disclosure-ingestion.md`. The tool reports the gap
   explicitly instead of the model fabricating deals.
7. **`ContentReader::news_search` and `ContentReader::filings` always answer
   `PortError::unavailable`.** Added with chat tools `news_search`/`filings_search`
   (`tantra-agent` plan 0006 Phase 3) via the new
   `crates/tantra-runtime-api-rest/src/chat_content.rs::FeedContentReader`: news has no data
   source anywhere yet (`tantra-data/docs/gaps/0006-news-ingestion.md`), and filings are ingested
   in `tantra-data`'s Python service (closed gap 0003) but nothing bridges that lakehouse into
   Rust consumers yet (`tantra-data/docs/gaps/0007-filings-rust-bridge.md`). Both report the gap
   explicitly instead of the model fabricating headlines or filings. (The `ContentReader` port's
   filings method is named `filings`; the chat tool that calls it is `filings_search`.)
8. **`MarketReader::options_chain` always answers `PortError::unavailable`.** Added with chat
   tool `options_chain` (`tantra-agent` plan 0006 Phase 4): `tantra-market` holds
   contract-reference columns and Greeks pricing but serves no chain enumeration/reader yet
   (`tantra-market/docs/gaps/0001-options-chain-reader.md`, that repo's first filed gap). The
   seedable `RuntimeMarketReader` (runtime-api) accepts a `set_options_chain` store for tests,
   but `FeedMarketReader` has nothing real to read.
9. **`MarketReader::corporate_actions` always answers `PortError::unavailable`** in
   `FeedMarketReader`. Added with chat tool `corporate_actions` (`tantra-agent` plan 0006
   Phase 4): the historical corporate-actions reader exists Python-side (`tantra_data`'s
   `CorporateActionsRepository`/`DuckDbCorporateActionsStore`) but has no Rust-consumer bridge
   (`tantra-data/docs/gaps/0008-corporate-actions-rust-bridge.md`), and its real-world
   population additionally waits on
   `tantra-data/docs/gaps/0009-corporate-actions-ingestion.md`. The seedable
   `RuntimeMarketReader` accepts a `set_corporate_actions` store for tests.
10. **`MarketReader::market_depth` always answers `PortError::unavailable`** in
    `FeedMarketReader`. Added with chat tool `market_depth` (`tantra-agent` plan 0007 Track 2
    Phase 4): the feed ports serve top-of-book quotes only; no L2 depth source exists
    (`tantra-market/docs/gaps/0006-l2-order-book-depth.md`). The seedable
    `RuntimeMarketReader` accepts a `set_market_depth` store for tests.
11. **`MarketReader::institutional_flows` always answers `PortError::unavailable`** in
    `FeedMarketReader`. Added with chat tool `institutional_flows` (plan 0007 Track 2 Phase 4):
    FII/DII daily flows are ingested Python-side (`tantra-data` gap 0013 closed) but have no
    Rust-consumer bridge (`tantra-data/docs/gaps/0017-institutional-flow-rust-bridge.md`). The
    seedable `RuntimeMarketReader` accepts a `set_institutional_flows` store for tests.
12. **`MarketReader::delivery_stats` always answers `PortError::unavailable`** in
    `FeedMarketReader`. Added with chat tool `delivery_analysis` (`tantra-agent` plan 0010
    Phase 2): delivery % is ingested Python-side (`tantra-data` gap 0014 closed) but has no
    Rust-consumer bridge (`tantra-data/docs/gaps/0019-delivery-rust-bridge.md`). The seedable
    `RuntimeMarketReader` accepts a `set_delivery_stats` store for tests.
13. **`MarketReader::oi_buildup` always answers `PortError::unavailable`** in
    `FeedMarketReader`. Added with chat tool `oi_buildup` (plan 0010 Phase 3): the OI
    time-series store + four-quadrant classifier live in `tantra-market-store`
    (`tantra-market/docs/gaps/0002` closed) but this runtime holds no OI store instance —
    wiring `ParquetOpenInterestStore::open(data_root)` + `BuildupAnalyzer` into
    `FeedMarketReader` is the remaining work. The seedable `RuntimeMarketReader` accepts a
    `set_oi_buildup` store for tests.
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
17. **`TradeIntentSink` (chat tool `propose_trade`) always answers `PortError::unavailable`**
    via `crate::chat_trade::FeedTradeIntentSink`. Added with plan 0007 Track 5 Phase 12
    (NoFX approval-queue pattern): the runtime has no `FileJobRegistry`-backed approval
    queue or paper-trading sandbox wired into the chat agent, so a proposed trade intent is
    never enqueued or executed.
18. **`StrategyStore` (chat tool `strategy_manage`) always answers `PortError::unavailable`**
    via `crate::chat_strategy_store::FeedStrategyStore`. Added with plan 0007 Track 5 Phase 13
    (Jesse-MCP strategy CRUD): the runtime has no strategy-manifest store wired into the
    chat agent.
19. **`TaxReportReader` (chat tools `tax_report`/`ledger_statement`) always answers
    `PortError::unavailable`** via `crate::chat_tax::FeedTaxReportReader`. Added with plan
    0007 Track 5 Phase 14: `tantra-ledger-portfolio` computes matched lots (`MatchedLot`) but
    no runtime bridge exposes them to the chat agent — the tax math itself is implemented and
    unit-tested in `tantra-agent-chat-tools`.

## Why

Closing docs/gaps/0006's market/screener stand-in only required wiring the tool catalog to data
AppState already has live access to (instrument reference data, quotes, bars, risk-engine price
bands). The fields above need either a new data source this runtime doesn't have yet (a
last-trade/volume feed, an indicator engine, a macro-economic data integration, a regime
classifier) or a product decision (which risk metrics to surface to the chat agent) — both out of
scope for a read-through wiring pass.

## How to close it

- Items 1–2: wire a last-trade/volume feed into `QuoteReader` (or a sibling port) and read it from
  `FeedMarketReader::quote`.
- Item 3: stand up an indicator engine (or reuse one from `tantra-market`'s 100+ indicator
  library) and a macro-economic data source, and implement `FeedMarketReader::indicators`/
  `macro_series` against them instead of returning `PortError::unavailable`.
- Item 4: add a display-name field to `Instrument` (`tantra-market-store`'s `wire.rs`) sourced
  from instrument reference data, and read it in `FeedMarketReader::search_instruments`.
- Item 5: decide which risk metrics (OTR counters, halt status, kill-switch state) the chat agent
  should see, and either build or find a market-regime classifier; wire both into
  `FeedSnapshotSource::snapshot`.
- Item 6: close `tantra-data` gap 0005 (disclosure ingestion), then replace
  `FeedMarketReader::block_deals`'s stub with a real read (plan 0004 Phase 4).
- Item 7: close `tantra-data` gaps 0006 (news ingestion) and 0007 (filings → Rust bridge),
  then replace `FeedContentReader`'s stubs with real reads through the bridge.
- Item 8: close `tantra-market/docs/gaps/0001` (chain enumeration/reader), then replace
  `FeedMarketReader::options_chain`'s stub with a real read.
- Item 9: close `tantra-data` gaps 0008 (corporate-actions → Rust bridge) and 0009
  (historical ingestion), then replace `FeedMarketReader::corporate_actions`'s stub with a
  real read through the bridge.
- Item 10: close `tantra-market/docs/gaps/0006` (L2 depth source), then replace
  `FeedMarketReader::market_depth`'s stub with a real read.
- Item 11: close `tantra-data/docs/gaps/0017` (institutional-flow → Rust bridge), then
  replace `FeedMarketReader::institutional_flows`'s stub with a real read through the bridge.
- Item 12: close `tantra-data/docs/gaps/0019` (delivery % → Rust bridge), then replace
  `FeedMarketReader::delivery_stats`'s stub with a real read through the bridge.
- Item 13: wire `ParquetOpenInterestStore::open(data_root)` + `BuildupAnalyzer` (both in
  `tantra-market-store`, gap 0002 closed) into `FeedMarketReader`, then replace its
  `oi_buildup` stub with a real read through `BuildupReader`.
- Item 17: wire a `FileJobRegistry`-backed `TradeIntentSink` (enqueue as
  `JobStatus::PendingApproval`; `x-papertrading` bypasses the human gate into the
  sandbox ledger) in `ChatRuntime::from_env`.
- Item 18: wire a strategy-manifest `StrategyStore` (persisted or in-memory) in
  `ChatRuntime::from_env`.
- Item 19: expose `tantra-ledger-portfolio`'s matched lots through a `TaxReportReader`
  implementor (per account/date range) in `ChatRuntime::from_env`.
