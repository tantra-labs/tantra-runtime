//! Unit tests for `FeedMarketReader`/`FeedSnapshotSource` — the read-through adapters that close
//! the market/screener stand-in of
//! docs/gaps/0006-chat-agent-session-persistence-and-portfolio-tools.md.

use std::sync::{Arc, Mutex};

use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tantra_agent_chat_tools::ports::error::PortErrorKind;
use tantra_agent_chat_tools::ports::market::MarketReader;
use tantra_agent_chat_tools::ports::portfolio::DateRange;
use tantra_agent_chat_tools::ports::snapshot::{SnapshotNeeds, SnapshotSource};
use tantra_core::{
    Currency, Exchange, InstrumentId, InstrumentKind, OptionType, Price, PriceBand, Quantity,
};
use tantra_market_store::{
    Dataset, DatasetReader, LiveQuoteStore, OpenInterestSnapshot, write_oi_file,
};
use tantra_risk::RiskEngine;
use tantra_runtime_api_rest::chat_market::{FeedMarketReader, FeedSnapshotSource};
use tantra_runtime_api_rest::market_feed::ports::{
    BarReader, BarRequest, DepthSnapshot, InstrumentMaster, PortError as FeedPortError, QuoteReader,
};
use tantra_runtime_api_rest::market_feed::wire::{
    AggressorSide, Bar, BarAggregation, BarSpecification, BarType, Instrument, PriceType,
    QuoteTick, TradeId, TradeTick, UnixNanos,
};
use tempfile::tempdir;

fn ts(hour: u32) -> UnixNanos {
    UnixNanos::from_datetime(Utc.with_ymd_and_hms(2026, 1, 5, hour, 0, 0).unwrap())
}

fn reliance() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "reliance")
}

/// Fake feed implementing all three read ports over a fixed, in-memory fixture — a standard
/// trait-level test double; no network or filesystem I/O.
#[derive(Clone, Default)]
struct FakeFeed {
    instruments: Vec<Instrument>,
    quote: Option<QuoteTick>,
    bars: Vec<Bar>,
}

#[async_trait::async_trait]
impl InstrumentMaster for FakeFeed {
    async fn get_instrument(&self, id: &InstrumentId) -> Result<Option<Instrument>, FeedPortError> {
        Ok(self.instruments.iter().find(|i| i.id() == id).cloned())
    }

    async fn list_instruments(&self) -> Result<Vec<Instrument>, FeedPortError> {
        Ok(self.instruments.clone())
    }

    async fn list_chain(
        &self,
        underlying: &InstrumentId,
        _as_of: Option<chrono::NaiveDate>,
    ) -> Result<Vec<Instrument>, FeedPortError> {
        let mut chain: Vec<Instrument> = self
            .instruments
            .iter()
            .filter(|i| {
                i.kind() == InstrumentKind::Option
                    && i.underlying()
                        .is_some_and(|u| u.eq_ignore_ascii_case(&underlying.symbol))
            })
            .cloned()
            .collect();
        chain.sort_by(|a, b| {
            let expiry = |i: &Instrument| i.expiry().unwrap_or(chrono::NaiveDate::MAX);
            let strike = |i: &Instrument| i.strike().unwrap_or(f64::INFINITY);
            let ot = |i: &Instrument| match i.option_type() {
                Some(OptionType::Call) => 0u8,
                Some(OptionType::Put) => 1u8,
                None => 2u8,
            };
            expiry(a)
                .cmp(&expiry(b))
                .then_with(|| {
                    strike(a)
                        .partial_cmp(&strike(b))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| ot(a).cmp(&ot(b)))
        });
        Ok(chain)
    }
}

#[async_trait::async_trait]
impl QuoteReader for FakeFeed {
    async fn read_quote(
        &self,
        _id: &InstrumentId,
        _as_of: Option<UnixNanos>,
    ) -> Result<Option<QuoteTick>, FeedPortError> {
        Ok(self.quote.clone())
    }
}

#[async_trait::async_trait]
impl BarReader for FakeFeed {
    async fn read_bars(&self, _request: &BarRequest) -> Result<Vec<Bar>, FeedPortError> {
        Ok(self.bars.clone())
    }
}

fn reader_over(feed: FakeFeed, risk_engine: RiskEngine) -> FeedMarketReader {
    let feed = Arc::new(feed);
    FeedMarketReader::new(
        feed.clone(),
        feed.clone(),
        feed,
        Arc::new(Mutex::new(risk_engine)),
    )
}

fn daily_bar(hour: u32, close: f64, volume: f64) -> Bar {
    let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
    let bar_type = BarType::new(reliance(), spec);
    Bar::new(
        bar_type,
        close,
        close,
        close,
        close,
        volume,
        ts(hour),
        ts(hour),
    )
}

/// `n` synthetic daily bars on consecutive days starting 2026-01-01 with strictly rising
/// closes (`close = start_close + i`, `high = close + 1`, `low = close - 1`), so a 20-SMA
/// trend over them classifies `trending_bull`.
fn rising_bars(n: usize, start_close: f64) -> Vec<Bar> {
    let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
    let first = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    (0..n)
        .map(|i| {
            let ts = UnixNanos::from_datetime(
                Utc.from_utc_datetime(
                    &(first + chrono::Days::new(i as u64))
                        .and_hms_opt(10, 0, 0)
                        .unwrap(),
                ),
            );
            let close = start_close + i as f64;
            Bar::new(
                BarType::new(reliance(), spec),
                close,
                close + 1.0,
                close - 1.0,
                close,
                1_000.0,
                ts,
                ts,
            )
        })
        .collect()
}

#[tokio::test]
async fn quote_reports_bid_ask_and_midpoint_last_price() {
    let instrument = reliance();
    let quote = QuoteTick::new(
        instrument.clone(),
        2_500.0,
        2_502.0,
        100.0,
        150.0,
        ts(9),
        ts(9),
    );
    let reader = reader_over(
        FakeFeed {
            quote: Some(quote),
            ..Default::default()
        },
        RiskEngine::default(),
    );

    let result = reader.quote(&instrument).await.unwrap();
    assert_eq!(result.bid.unwrap().value(), Decimal::from(2500));
    assert_eq!(result.ask.unwrap().value(), Decimal::from(2502));
    assert_eq!(result.last_price.value(), Decimal::from(2501));
}

#[tokio::test]
async fn quote_errors_when_feed_has_no_quote() {
    let reader = reader_over(FakeFeed::default(), RiskEngine::default());
    assert!(reader.quote(&reliance()).await.is_err());
}

#[tokio::test]
async fn history_caps_at_limit_and_flags_downsampled() {
    let bars = vec![daily_bar(9, 100.0, 1_000.0), daily_bar(10, 101.0, 2_000.0)];
    let reader = reader_over(
        FakeFeed {
            bars,
            ..Default::default()
        },
        RiskEngine::default(),
    );

    let history = reader
        .history(
            &reliance(),
            DateRange {
                start: None,
                end: None,
            },
            Some(1),
        )
        .await
        .unwrap();
    assert_eq!(history.bars.len(), 1);
    assert!(history.downsampled);
    assert_eq!(history.bars[0].close.value(), Decimal::from(100));
}

#[tokio::test]
async fn search_instruments_matches_symbol_case_insensitively() {
    let instrument = Instrument::new(reliance(), InstrumentKind::Equity, Currency::Inr, 1.0, 0.05);
    let reader = reader_over(
        FakeFeed {
            instruments: vec![instrument],
            ..Default::default()
        },
        RiskEngine::default(),
    );

    let hits = reader.search_instruments("rel").await.unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].exchange, "NSE");
    assert_eq!(hits[0].instrument, reliance());
}

#[tokio::test]
async fn price_band_reads_from_risk_engine() {
    let instrument = reliance();
    let band = PriceBand::new(
        Price::new(Decimal::from(2000)),
        Price::new(Decimal::from(3000)),
        Price::new(Decimal::from(2500)),
    );
    let mut engine = RiskEngine::default();
    engine.set_price_band(instrument.clone(), band);
    let reader = reader_over(FakeFeed::default(), engine);

    let result = reader.price_band(&instrument).await.unwrap();
    assert_eq!(result.lower.value(), Decimal::from(2000));
}

#[tokio::test]
async fn price_band_not_found_when_unregistered() {
    let reader = reader_over(FakeFeed::default(), RiskEngine::default());
    assert!(reader.price_band(&reliance()).await.is_err());
}

#[tokio::test]
async fn indicators_and_macro_series_are_explicitly_unavailable() {
    let reader = reader_over(FakeFeed::default(), RiskEngine::default());
    assert!(reader.indicators(&reliance(), &[]).await.is_err());
    assert!(reader.macro_series("nifty_pe").await.is_err());
}

#[tokio::test]
async fn block_deals_are_explicitly_unavailable() {
    let reader = reader_over(FakeFeed::default(), RiskEngine::default());
    let err = reader
        .block_deals(Some(&reliance()), None)
        .await
        .unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message
            .contains("no block/bulk-deal projections configured"),
        "unconfigured block deals must cite the missing projections, got: {err}"
    );
}

#[tokio::test]
async fn snapshot_source_reports_universe_from_instrument_catalog() {
    let instrument = Instrument::new(reliance(), InstrumentKind::Equity, Currency::Inr, 1.0, 0.05);
    let source = FeedSnapshotSource::new(Arc::new(FakeFeed {
        instruments: vec![instrument],
        ..Default::default()
    }));

    let snapshot = source
        .snapshot(&SnapshotNeeds::for_tool("screener"))
        .await
        .unwrap();
    assert_eq!(snapshot.universe, Some(vec!["RELIANCE".to_string()]));
}

#[tokio::test]
async fn snapshot_source_reports_no_universe_when_catalog_empty() {
    let source = FeedSnapshotSource::new(Arc::new(FakeFeed::default()));
    let snapshot = source
        .snapshot(&SnapshotNeeds::for_tool("screener"))
        .await
        .unwrap();
    assert_eq!(snapshot.universe, None);
}

fn option_instrument(symbol: &str, expiry: &str, strike: f64, ot: OptionType) -> Instrument {
    Instrument::new(
        InstrumentId::new(Exchange::Nfo, symbol),
        InstrumentKind::Option,
        Currency::Inr,
        25.0,
        0.05,
    )
    .with_derivative(
        Some(chrono::NaiveDate::parse_from_str(expiry, "%Y-%m-%d").unwrap()),
        Some(strike),
        Some(ot),
        Some("NIFTY".to_string()),
    )
}

#[tokio::test]
async fn oi_buildup_reads_real_store_and_bars() {
    let today = Utc::now().date_naive();
    let day2 = today - chrono::Days::new(2);
    let day1 = today - chrono::Days::new(1);

    // OI lakehouse: three dated snapshot files with rising open interest, so the
    // analyzer classifies a long buildup (price up, OI up) over the window.
    let dir = tempdir().unwrap();
    let snapshot = |date: chrono::NaiveDate, oi: i64| OpenInterestSnapshot {
        instrument: reliance(),
        as_of: date,
        open_interest: Quantity::new(Decimal::from(oi)),
    };
    write_oi_file(dir.path(), day2, &[snapshot(day2, 100)]).unwrap();
    write_oi_file(dir.path(), day1, &[snapshot(day1, 200)]).unwrap();
    write_oi_file(dir.path(), today, &[snapshot(today, 250)]).unwrap();

    // Daily bars on the same dates with rising closes (fixed-2026-01-05 `ts(hour)`
    // timestamps would fall outside [today-5, today], so build today-based ones).
    let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
    let bar_at = |date: chrono::NaiveDate, close: f64| {
        let ts =
            UnixNanos::from_datetime(Utc.from_utc_datetime(&date.and_hms_opt(10, 0, 0).unwrap()));
        Bar::new(
            BarType::new(reliance(), spec),
            close,
            close,
            close,
            close,
            1_000.0,
            ts,
            ts,
        )
    };
    let bars = vec![
        bar_at(day2, 100.0),
        bar_at(day1, 120.0),
        bar_at(today, 150.0),
    ];
    let feed = Arc::new(FakeFeed {
        bars,
        ..Default::default()
    });
    let reader = FeedMarketReader::new(
        feed.clone(),
        feed.clone(),
        feed,
        Arc::new(Mutex::new(RiskEngine::default())),
    )
    .with_oi_store(dir.path());

    let result = reader.oi_buildup(&reliance(), None).await.unwrap();
    assert_eq!(result.classification, "long_buildup");
    // The parquet store persists OI at scale 4, so 250 round-trips as 250.0000.
    assert_eq!(
        result.open_interest,
        Quantity::new(Decimal::new(2_500_000, 4))
    );
    // (250 - 100) / 100 * 100; computed identically to the adapter so scale matches.
    let first_oi = Decimal::new(1_000_000, 4);
    let last_oi = Decimal::new(2_500_000, 4);
    assert_eq!(
        result.oi_change_pct,
        Some((last_oi - first_oi) / first_oi * Decimal::from(100))
    );
    let first_close = Decimal::from_f64_retain(100.0).unwrap();
    let last_close = Decimal::from_f64_retain(150.0).unwrap();
    assert_eq!(
        result.price_change_pct,
        Some((last_close - first_close) / first_close * Decimal::from(100))
    );
}

#[tokio::test]
async fn oi_buildup_unavailable_without_store() {
    let reader = reader_over(FakeFeed::default(), RiskEngine::default());
    let err = reader.oi_buildup(&reliance(), None).await.unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message.contains("no open-interest store wired"),
        "unconfigured OI buildup must cite the missing store, got: {err}"
    );
}

#[tokio::test]
async fn options_chain_serves_catalog_contracts_with_quotes_and_filters() {
    let inst = InstrumentId::new(Exchange::Nfo, "NIFTY");
    let contract_id = |symbol: &str| InstrumentId::new(Exchange::Nfo, symbol);
    let quote = QuoteTick::new(
        contract_id("NIFTY26NOV24000CE"),
        235.0,
        236.0,
        100.0,
        150.0,
        ts(9),
        ts(9),
    );
    let mut banknifty = option_instrument(
        "BANKNIFTY26NOV24000CE",
        "2026-11-26",
        24000.0,
        OptionType::Call,
    );
    banknifty = banknifty.with_derivative(
        Some(chrono::NaiveDate::from_ymd_opt(2026, 11, 26).unwrap()),
        Some(24000.0),
        Some(OptionType::Call),
        Some("BANKNIFTY".to_string()),
    );
    let feed = FakeFeed {
        instruments: vec![
            option_instrument("NIFTY26NOV24500CE", "2026-11-26", 24500.0, OptionType::Call),
            option_instrument("NIFTY26NOV24000CE", "2026-11-26", 24000.0, OptionType::Call),
            option_instrument("NIFTY26NOV24000PE", "2026-11-26", 24000.0, OptionType::Put),
            banknifty,
        ],
        quote: Some(quote),
        ..Default::default()
    };
    let reader = reader_over(feed, RiskEngine::default());

    let chain = reader.options_chain(&inst, None, None).await.unwrap();
    assert_eq!(chain.underlying, inst);
    let symbols: Vec<String> = chain
        .contracts
        .iter()
        .map(|c| c.instrument.symbol.clone())
        .collect();
    assert_eq!(
        symbols,
        vec![
            "NIFTY26NOV24000CE",
            "NIFTY26NOV24000PE",
            "NIFTY26NOV24500CE"
        ],
        "ordered by strike then option type, other underlyings excluded"
    );
    assert_eq!(chain.contracts[0].strike.value(), Decimal::from(24000));
    assert_eq!(chain.contracts[0].option_type, "CE");
    assert_eq!(chain.contracts[1].option_type, "PE");
    assert_eq!(chain.contracts[0].lot_size.value(), Decimal::from(25));
    assert_eq!(chain.contracts[0].bid.unwrap().value(), Decimal::from(235));
    assert_eq!(chain.contracts[0].ask.unwrap().value(), Decimal::from(236));
    assert_eq!(
        chain.contracts[0].last_price.unwrap().value(),
        Decimal::from(2355) / Decimal::from(10),
        "last price is the bid/ask midpoint when both sides are quoted"
    );
    assert_eq!(chain.contracts[0].open_interest, None);

    // Expiry filter keeps only the requested expiry; a limit caps the result.
    let expiry = Utc.with_ymd_and_hms(2026, 11, 26, 0, 0, 0).unwrap();
    let filtered = reader
        .options_chain(&inst, Some(expiry), None)
        .await
        .unwrap();
    assert_eq!(filtered.contracts.len(), 3);
    let capped = reader.options_chain(&inst, None, Some(1)).await.unwrap();
    assert_eq!(capped.contracts.len(), 1);
}

#[tokio::test]
async fn options_chain_is_explicitly_unavailable_when_the_master_cannot_enumerate() {
    struct NoChain;
    #[async_trait::async_trait]
    impl InstrumentMaster for NoChain {
        async fn get_instrument(
            &self,
            _id: &InstrumentId,
        ) -> Result<Option<Instrument>, FeedPortError> {
            Ok(None)
        }
        async fn list_instruments(&self) -> Result<Vec<Instrument>, FeedPortError> {
            Ok(Vec::new())
        }
    }
    let feed = FakeFeed::default();
    let reader = FeedMarketReader::new(
        Arc::new(NoChain),
        Arc::new(feed.clone()),
        Arc::new(feed),
        Arc::new(Mutex::new(RiskEngine::default())),
    );
    let err = reader
        .options_chain(&InstrumentId::new(Exchange::Nfo, "NIFTY"), None, None)
        .await
        .unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message.contains("cannot enumerate options chains"),
        "stub must say enumeration is unsupported, got: {err}"
    );
}

#[tokio::test]
async fn market_depth_is_explicitly_unavailable_with_gap_citation() {
    let reader = reader_over(FakeFeed::default(), RiskEngine::default());
    let err = reader.market_depth(&reliance(), None).await.unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message.contains("0006-l2-order-book-depth.md"),
        "stub must cite the upstream gap, got: {err}"
    );
}

#[tokio::test]
async fn market_depth_reads_wired_ladder_and_computes_imbalance() {
    let instrument = reliance();
    let store = Arc::new(LiveQuoteStore::new(Arc::new(DatasetReader::from_dataset(
        Dataset::default(),
    ))));
    store.put_depth(
        instrument.clone(),
        DepthSnapshot {
            as_of: ts(9),
            bids: vec![
                (Price::new(dec!(100.0)), Quantity::new(dec!(500))),
                (Price::new(dec!(99.9)), Quantity::new(dec!(500))),
            ],
            asks: vec![(Price::new(dec!(100.1)), Quantity::new(dec!(300)))],
        },
    );

    let reader = FeedMarketReader::new(
        Arc::new(FakeFeed::default()),
        Arc::new(FakeFeed::default()),
        Arc::new(FakeFeed::default()),
        Arc::new(Mutex::new(RiskEngine::default())),
    )
    .with_depth_reader(store.clone());

    let depth = reader.market_depth(&instrument, Some(1)).await.unwrap();
    assert_eq!(depth.bids.len(), 1, "the levels cap is applied");
    assert_eq!(depth.asks.len(), 1);
    assert_eq!(depth.bids[0].price, Price::new(dec!(100.0)));
    assert_eq!(depth.bids[0].quantity, Quantity::new(dec!(500)));
    // bid_total 500 vs ask_total 300 → (500 − 300) / (500 + 300) × 100 = 25.
    assert_eq!(depth.liquidity_imbalance_pct, dec!(25));
}

#[tokio::test]
async fn institutional_flows_are_explicitly_unavailable_with_gap_citation() {
    let reader = reader_over(FakeFeed::default(), RiskEngine::default());
    let err = reader.institutional_flows(None).await.unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message.contains("0007 item 11"),
        "stub must cite the runtime gap item, got: {err}"
    );
}

#[tokio::test]
async fn quote_prefers_live_trade_print_when_wired() {
    let instrument = reliance();
    let store = Arc::new(LiveQuoteStore::new(Arc::new(DatasetReader::from_dataset(
        Dataset::default(),
    ))));
    store.put_quote(QuoteTick::new(
        instrument.clone(),
        100.0,
        101.0,
        1.0,
        1.0,
        ts(9),
        ts(9),
    ));
    store.put_trade(TradeTick::new(
        instrument.clone(),
        100.5,
        10.0,
        AggressorSide::Buyer,
        TradeId::new("t1"),
        ts(9),
        ts(9),
    ));

    let reader = FeedMarketReader::new(
        Arc::new(FakeFeed::default()),
        Arc::new(FakeFeed::default()),
        store.clone(),
        Arc::new(Mutex::new(RiskEngine::default())),
    )
    .with_trade_reader(store.clone());

    let quote = reader.quote(&instrument).await.unwrap();
    assert_eq!(quote.last_price, Price::new(dec!(100.5)));
    assert_eq!(quote.volume, Quantity::new(dec!(10.0)));
}

#[tokio::test]
async fn search_instruments_uses_display_name() {
    let named = Instrument::new(reliance(), InstrumentKind::Equity, Currency::Inr, 1.0, 0.05)
        .with_display_name("RELIANCE INDUSTRIES");
    let unnamed = Instrument::new(
        InstrumentId::new(Exchange::Nse, "TATAMOTORS"),
        InstrumentKind::Equity,
        Currency::Inr,
        1.0,
        0.05,
    );
    let reader = reader_over(
        FakeFeed {
            instruments: vec![named, unnamed],
            ..Default::default()
        },
        RiskEngine::default(),
    );

    let hits = reader.search_instruments("rel").await.unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].name, "RELIANCE INDUSTRIES");

    let hits = reader.search_instruments("tatamotors").await.unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].name, "TATAMOTORS",
        "no display name falls back to the symbol"
    );
}

#[tokio::test]
async fn indicators_computes_sma_and_rsi() {
    let reader = reader_over(
        FakeFeed {
            bars: rising_bars(40, 100.0),
            ..Default::default()
        },
        RiskEngine::default(),
    );

    let result = reader
        .indicators(
            &reliance(),
            &[
                "sma_20".to_string(),
                "rsi_14".to_string(),
                "unknown_9".to_string(),
            ],
        )
        .await
        .unwrap();
    // SMA(20) over closes 120..=139 = 129.5 exactly (integer closes).
    let expected_sma = Decimal::from_f64_retain(129.5).unwrap();
    assert!(
        (result.values["sma_20"] - expected_sma).abs() < dec!(0.01),
        "sma_20 was {}",
        result.values["sma_20"]
    );
    assert!(result.values.contains_key("rsi_14"));
    assert!(
        !result.values.contains_key("unknown_9"),
        "unsupported names are skipped"
    );
}

#[tokio::test]
async fn snapshot_source_reports_risk_metrics_and_regime() {
    let instrument = reliance();
    let band = PriceBand::new(
        Price::new(Decimal::from(2000)),
        Price::new(Decimal::from(3000)),
        Price::new(Decimal::from(2500)),
    );
    let mut engine = RiskEngine::default();
    engine.set_price_band(instrument.clone(), band);

    let bars = Arc::new(FakeFeed {
        bars: rising_bars(40, 100.0),
        ..Default::default()
    });
    let source = FeedSnapshotSource::new(Arc::new(FakeFeed::default()))
        .with_risk_engine(Arc::new(Mutex::new(engine)))
        .with_regime(bars, instrument);

    let snapshot = source.snapshot(&SnapshotNeeds::default()).await.unwrap();
    assert_eq!(snapshot.risk_metrics["price_band_count"], 1);
    assert_eq!(snapshot.risk_metrics["otr_max_ratio"], "50");
    assert_eq!(
        snapshot.regime,
        Some("trending_bull".to_string()),
        "rising closes above a rising 20-SMA classify trending_bull"
    );
}
