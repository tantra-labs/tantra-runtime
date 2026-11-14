//! Read-through `MarketReader`/`SnapshotSource` adapters bridging the feed and risk ports
//! `AppState` already holds to the chat tool catalog's market/screener tools (closes the third
//! stand-in of docs/gaps/0006-chat-agent-session-persistence-and-portfolio-tools.md).
//!
//! Every call reads straight through to the live port each time — no cache, so quotes and bars
//! are never stale and nothing needs a background refresh task. Items 1-5, 11 and 12 of
//! docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md are now closed:
//! institutional flows and delivery % read the `tantra-data` projections through
//! `tantra-market-store`, `macro_series` reads the economic-calendar projections, `indicators`
//! runs `tantra-market-indicators` over the daily-bar feed, search results prefer the catalog
//! display name, and the snapshot source reports the shared risk engine's OTR/price-band
//! metrics plus a 20-SMA regime classification. `market_depth` remains an explicit gap
//! (`tantra-market/docs/gaps/0006-l2-order-book-depth.md`), and the live trade print/volume
//! are only reported when a [`TradeReader`] is attached — real broker feeds remain
//! `tantra-market` gaps 0001/0002, so an unwired reader honestly falls back to the midpoint.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use crate::market_feed::ports::{
    BarReader, BarRequest, ChainContract, ChainReader, DepthReader, DepthSnapshot,
    InstrumentMaster, QuoteReader, TradeReader,
};
use crate::market_feed::wire::{BarAggregation, BarSpecification, PriceType, UnixNanos};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::market::{
    BlockDeal, CorporateAction, DeliveryBar, InstitutionalFlow, InstrumentSearchResult,
    MacroSeries, MarketBar, MarketDepth, MarketDepthLevel, MarketHistory, MarketIndicators,
    MarketQuote, MarketReader, OiBuildupSnapshot, OptionContract, OptionsChain,
};
use tantra_agent_chat_tools::ports::portfolio::DateRange;
use tantra_agent_chat_tools::ports::snapshot::{SnapshotData, SnapshotNeeds, SnapshotSource};
use tantra_core::{Bar, Currency, Exchange, InstrumentId, Money, Price, PriceBand, Quantity};
use tantra_market_indicators::adx::Adx;
use tantra_market_indicators::{
    Atr, BollingerBands, Cmo, Ema, Indicator, Macd, Mfi, Obv, Roc, Rsi, Sma, Vwap,
};
use tantra_market_store::{
    BlockDealProjection, BlockDealsProjectionReader, BuildupAnalyzer, BuildupReader,
    CorporateActionProjection, CorporateActionsProjectionReader, DeliveryProjectionReader,
    EconomicCalendarProjectionReader, InstitutionalFlowsProjectionReader, InstrumentChainReader,
    OpenInterestStore, ParquetOpenInterestStore,
};
use tantra_risk::RiskEngine;

/// Bar-count cap shared with `tantra_runtime_api::chat_adapters::RuntimeMarketReader`.
const MAX_BAR_CAP: usize = 500;

fn to_decimal(x: f64) -> Decimal {
    Decimal::from_f64_retain(x).unwrap_or_default()
}

/// Maps the feed's async port error onto the chat tool catalog's own `PortError`.
fn feed_port_error(e: crate::market_feed::ports::PortError) -> PortError {
    match e {
        crate::market_feed::ports::PortError::Unsupported(msg) => PortError::unavailable(msg),
        crate::market_feed::ports::PortError::Internal(msg) => PortError::internal(msg),
        crate::market_feed::ports::PortError::Terminal(msg) => PortError::internal(msg),
    }
}

/// Read-through `MarketReader` over the same feed ports `AppState` already serves
/// `/market-data/*` routes from, plus the shared risk engine for price bands.
pub struct FeedMarketReader {
    instruments: Arc<dyn InstrumentMaster>,
    bars: Arc<dyn BarReader>,
    quotes: Arc<dyn QuoteReader>,
    risk_engine: Arc<Mutex<RiskEngine>>,
    chain: Arc<dyn ChainReader>,
    corporate_actions: Option<CorporateActionsProjectionReader>,
    block_deals: Option<BlockDealsProjectionReader>,
    oi_store: Option<Arc<dyn OpenInterestStore>>,
    buildup: Option<Arc<dyn BuildupReader>>,
    institutional_flows: Option<InstitutionalFlowsProjectionReader>,
    delivery: Option<DeliveryProjectionReader>,
    economic_calendar: Option<EconomicCalendarProjectionReader>,
    trades: Option<Arc<dyn TradeReader>>,
    depth: Option<Arc<dyn DepthReader>>,
}

impl FeedMarketReader {
    /// Builds a [`FeedMarketReader`] over the given feed ports and shared risk engine.
    ///
    /// The options-chain reader is built from the same `instruments`/`quotes` ports the
    /// reader already serves from (closes `tantra-market/docs/gaps/0001-options-chain-reader.md`).
    pub fn new(
        instruments: Arc<dyn InstrumentMaster>,
        bars: Arc<dyn BarReader>,
        quotes: Arc<dyn QuoteReader>,
        risk_engine: Arc<Mutex<RiskEngine>>,
    ) -> Self {
        let chain: Arc<dyn ChainReader> = Arc::new(InstrumentChainReader::new(
            instruments.clone(),
            quotes.clone(),
        ));
        Self {
            instruments,
            bars,
            quotes,
            risk_engine,
            chain,
            corporate_actions: None,
            block_deals: None,
            oi_store: None,
            buildup: None,
            institutional_flows: None,
            delivery: None,
            economic_calendar: None,
            trades: None,
            depth: None,
        }
    }

    /// Builds a [`FeedMarketReader`] over the given feed ports and shared risk engine,
    /// additionally wiring the `tantra-data` corporate-actions projections
    /// (`{root}/catalog/corporate_actions`) through `tantra-market-store`'s projection
    /// reader (closes `tantra-data/docs/gaps/0008-corporate-actions-rust-bridge.md`).
    pub fn with_corporate_actions(mut self, root: &std::path::Path) -> Self {
        self.corporate_actions = Some(CorporateActionsProjectionReader::new(root));
        self
    }

    /// Builds a [`FeedMarketReader`] over the given feed ports and shared risk engine,
    /// additionally wiring the `tantra-data` block/bulk-deal projections
    /// (`{root}/catalog/block_deals`) through `tantra-market-store`'s projection reader
    /// (closes `tantra-data/docs/gaps/0005-block-bulk-deal-disclosure-ingestion.md`).
    pub fn with_block_deals(mut self, root: &std::path::Path) -> Self {
        self.block_deals = Some(BlockDealsProjectionReader::new(root));
        self
    }

    /// Opens the `tantra-data` OI lakehouse (`{root}/open_interest`) and wires a
    /// [`BuildupAnalyzer`] over it and this reader's daily-bar source (closes
    /// `tantra-runtime/docs/gaps/0007` item 13). A store that cannot be opened leaves
    /// `oi_buildup` answering its explicit gap error rather than failing the build.
    pub fn with_oi_store(mut self, root: &std::path::Path) -> Self {
        match ParquetOpenInterestStore::open(root) {
            Ok(store) => {
                let store: Arc<dyn OpenInterestStore> = Arc::new(store);
                let analyzer: Arc<dyn BuildupReader> =
                    Arc::new(BuildupAnalyzer::new(store.clone(), self.bars.clone()));
                self.oi_store = Some(store);
                self.buildup = Some(analyzer);
            }
            Err(e) => {
                tracing::warn!("open-interest store not wired: {e}");
            }
        }
        self
    }

    /// Builds a [`FeedMarketReader`] over the given feed ports and shared risk engine,
    /// additionally wiring the `tantra-data` institutional-flows projections
    /// (`{root}/catalog/institutional_flows`) through `tantra-market-store`'s projection
    /// reader (closes `tantra-data` gap 0017 and `tantra-runtime/docs/gaps/0007` item 11).
    pub fn with_institutional_flows(mut self, root: &std::path::Path) -> Self {
        self.institutional_flows = Some(InstitutionalFlowsProjectionReader::new(root));
        self
    }

    /// Builds a [`FeedMarketReader`] over the given feed ports and shared risk engine,
    /// additionally wiring the `tantra-data` delivery-percentage projections
    /// (`{root}/catalog/delivery`) through `tantra-market-store`'s projection reader
    /// (closes `tantra-data` gap 0019 and `tantra-runtime/docs/gaps/0007` item 12).
    pub fn with_delivery(mut self, root: &std::path::Path) -> Self {
        self.delivery = Some(DeliveryProjectionReader::new(root));
        self
    }

    /// Builds a [`FeedMarketReader`] over the given feed ports and shared risk engine,
    /// additionally wiring the `tantra-data` economic-calendar projections
    /// (`{root}/catalog/economic_calendar`) through `tantra-market-store`'s projection
    /// reader (closes `tantra-data` gap 0018 and `tantra-runtime/docs/gaps/0007` item 3b).
    pub fn with_economic_calendar(mut self, root: &std::path::Path) -> Self {
        self.economic_calendar = Some(EconomicCalendarProjectionReader::new(root));
        self
    }

    /// Builds a [`FeedMarketReader`] over the given feed ports and shared risk engine,
    /// additionally attaching a live [`TradeReader`] so `quote` reports the latest print
    /// and session-day volume instead of the bid/ask midpoint. The port stays
    /// unconfigured by default — real broker trade feeds remain `tantra-market` gaps
    /// 0001/0002 — and an unwired reader keeps answering the honest midpoint fallback.
    pub fn with_trade_reader(mut self, reader: Arc<dyn TradeReader>) -> Self {
        self.trades = Some(reader);
        self
    }

    /// Builds a [`FeedMarketReader`] over the given feed ports and shared risk engine,
    /// additionally attaching an L2 order-book [`DepthReader`] so `market_depth` serves the
    /// real ladder (the same port `GET /depth/{id}` reads from; closes
    /// `tantra-runtime/docs/gaps/0007` item 10 and `tantra-market/docs/gaps/0006`). The port
    /// stays unconfigured by default, and an unwired reader keeps answering the explicit
    /// gap error rather than fabricating an empty book.
    pub fn with_depth_reader(mut self, reader: Arc<dyn DepthReader>) -> Self {
        self.depth = Some(reader);
        self
    }
}

/// Converts a store `ChainContract` into the chat tool catalog's `OptionContract`,
/// with the midpoint as the last-traded price when a two-sided quote exists (the same
/// derivation [`FeedMarketReader::quote`] uses).
fn to_option_contract(contract: ChainContract) -> OptionContract {
    let last_price = match (contract.bid, contract.ask) {
        (Some(bid), Some(ask)) => Some(Price::new((bid.value() + ask.value()) / Decimal::from(2))),
        (Some(bid), None) => Some(bid),
        (None, Some(ask)) => Some(ask),
        (None, None) => contract.last_price,
    };
    OptionContract {
        instrument: contract.instrument,
        option_type: match contract.option_type {
            tantra_core::OptionType::Call => "CE",
            tantra_core::OptionType::Put => "PE",
        }
        .to_string(),
        strike: contract.strike,
        expiry: contract.expiry.and_hms_opt(0, 0, 0).unwrap().and_utc(),
        lot_size: contract.lot_size,
        last_price,
        bid: contract.bid,
        ask: contract.ask,
        open_interest: contract.open_interest,
    }
}

#[async_trait]
impl MarketReader for FeedMarketReader {
    async fn quote(&self, instrument: &InstrumentId) -> Result<MarketQuote, PortError> {
        let tick = self
            .quotes
            .read_quote(instrument, None)
            .await
            .map_err(feed_port_error)?
            .ok_or_else(|| PortError::not_found(format!("Quote not found for {instrument}")))?;
        let bid = to_decimal(tick.bid_price());
        let ask = to_decimal(tick.ask_price());
        let mid = (bid + ask) / Decimal::from(2);
        // The last-trade print and day volume are live only when a trade reader is attached
        // ([`FeedMarketReader::with_trade_reader`]); real broker feeds remain
        // `tantra-market` gaps 0001/0002, so an unwired reader honestly falls back to the
        // bid/ask midpoint rather than fabricating a print.
        let (last_price, volume) = match &self.trades {
            Some(reader) => match reader.read_trade_snapshot(instrument).await {
                Ok(Some(snapshot)) => (
                    Price::new(to_decimal(snapshot.last.price())),
                    Quantity::new(to_decimal(snapshot.day_volume)),
                ),
                _ => (Price::new(mid), Quantity::new(Decimal::ZERO)),
            },
            None => (Price::new(mid), Quantity::new(Decimal::ZERO)),
        };
        Ok(MarketQuote {
            instrument: instrument.clone(),
            last_price,
            bid: Some(Price::new(bid)),
            ask: Some(Price::new(ask)),
            volume,
            as_of: tick.ts_event().to_datetime(),
        })
    }

    async fn history(
        &self,
        instrument: &InstrumentId,
        range: DateRange,
        limit: Option<usize>,
    ) -> Result<MarketHistory, PortError> {
        let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
        let from = range.start.map(UnixNanos::from_datetime);
        let to = range.end.map(UnixNanos::from_datetime);
        let request =
            BarRequest::new(instrument.clone(), spec, from, to).map_err(feed_port_error)?;
        let bars = self
            .bars
            .read_bars(&request)
            .await
            .map_err(feed_port_error)?;

        let effective_limit = limit.map(|l| l.min(MAX_BAR_CAP)).unwrap_or(MAX_BAR_CAP);
        let downsampled = bars.len() > effective_limit;
        let bars: Vec<MarketBar> = bars
            .into_iter()
            .take(effective_limit)
            .map(|b| MarketBar {
                timestamp: b.ts_event().to_datetime(),
                open: Price::new(to_decimal(b.open())),
                high: Price::new(to_decimal(b.high())),
                low: Price::new(to_decimal(b.low())),
                close: Price::new(to_decimal(b.close())),
                volume: Quantity::new(to_decimal(b.volume())),
            })
            .collect();

        Ok(MarketHistory {
            instrument: instrument.clone(),
            bars,
            downsampled,
        })
    }

    async fn indicators(
        &self,
        instrument: &InstrumentId,
        names: &[String],
    ) -> Result<MarketIndicators, PortError> {
        let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
        let request =
            BarRequest::new(instrument.clone(), spec, None, None).map_err(feed_port_error)?;
        let wire_bars = self
            .bars
            .read_bars(&request)
            .await
            .map_err(feed_port_error)?;
        let core_bars: Vec<Bar> = wire_bars
            .iter()
            .map(|b| Bar {
                instrument: instrument.clone(),
                open: Price::new(to_decimal(b.open())),
                high: Price::new(to_decimal(b.high())),
                low: Price::new(to_decimal(b.low())),
                close: Price::new(to_decimal(b.close())),
                volume: Quantity::new(to_decimal(b.volume())),
                timestamp: b.ts_event().to_datetime(),
            })
            .collect();
        let closes: Vec<f64> = core_bars
            .iter()
            .map(|b| b.close.value().to_f64().unwrap_or(0.0))
            .collect();

        let mut values = HashMap::new();
        for name in names {
            let Some((kind, params)) = parse_indicator_name(name) else {
                // Unsupported indicator names are skipped, not errors by themselves: a mix
                // of supported and unknown names returns the supported set.
                continue;
            };
            // Indicator constructors panic on zero/inverted periods, so skip names that
            // would panic rather than propagate a panic from a model-supplied string.
            let valid = match kind {
                IndicatorKind::Macd => {
                    let fast = params.first().copied().unwrap_or(12);
                    let slow = params.get(1).copied().unwrap_or(26);
                    let signal = params.get(2).copied().unwrap_or(9);
                    fast > 0 && slow > 0 && signal > 0 && fast < slow
                }
                IndicatorKind::Obv | IndicatorKind::Vwap => true,
                _ => params[0] > 0,
            };
            if !valid {
                continue;
            }
            let value = match kind {
                IndicatorKind::Sma => {
                    let mut ind = Sma::new(params[0]);
                    for c in &closes {
                        let _ = ind.update(*c);
                    }
                    ind.value()
                }
                IndicatorKind::Ema => {
                    let mut ind = Ema::new(params[0]);
                    for c in &closes {
                        let _ = ind.update(*c);
                    }
                    ind.value()
                }
                IndicatorKind::Rsi => {
                    let mut ind = Rsi::new(params[0]);
                    for c in &closes {
                        let _ = ind.update(*c);
                    }
                    ind.value()
                }
                IndicatorKind::Roc => {
                    let mut ind = Roc::new(params[0]);
                    for c in &closes {
                        let _ = ind.update(*c);
                    }
                    ind.value()
                }
                IndicatorKind::Cmo => {
                    let mut ind = Cmo::new(params[0]);
                    for c in &closes {
                        let _ = ind.update(*c);
                    }
                    ind.value()
                }
                IndicatorKind::Macd => {
                    let mut ind = Macd::new(
                        params.first().copied().unwrap_or(12),
                        params.get(1).copied().unwrap_or(26),
                        params.get(2).copied().unwrap_or(9),
                    );
                    for c in &closes {
                        let _ = ind.update(*c);
                    }
                    ind.value().map(|v| v.macd)
                }
                IndicatorKind::Bb => {
                    let mut ind = BollingerBands::new(params[0], 2.0);
                    for c in &closes {
                        let _ = ind.update(*c);
                    }
                    ind.value().map(|v| v.middle)
                }
                IndicatorKind::Atr => {
                    let mut ind = Atr::new(params[0]);
                    for b in &core_bars {
                        let _ = ind.update(b);
                    }
                    ind.value()
                }
                IndicatorKind::Adx => {
                    let mut ind = Adx::new(params[0]);
                    for b in &core_bars {
                        let _ = ind.update(b);
                    }
                    ind.value()
                }
                IndicatorKind::Mfi => {
                    let mut ind = Mfi::new(params[0]);
                    for b in &core_bars {
                        let _ = ind.update(b);
                    }
                    ind.value()
                }
                IndicatorKind::Obv => {
                    let mut ind = Obv::new();
                    for b in &core_bars {
                        let _ = ind.update(b);
                    }
                    ind.value()
                }
                IndicatorKind::Vwap => {
                    let mut ind = Vwap::new();
                    for b in &core_bars {
                        let _ = ind.update(b);
                    }
                    ind.value()
                }
            };
            if let Some(v) = value {
                values.insert(name.clone(), to_decimal(v));
            }
        }
        if values.is_empty() {
            return Err(PortError::unavailable(format!(
                "no indicator values could be computed for {instrument}; see docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md"
            )));
        }
        Ok(MarketIndicators {
            instrument: instrument.clone(),
            values,
            as_of: Utc::now(),
        })
    }

    async fn search_instruments(
        &self,
        query: &str,
    ) -> Result<Vec<InstrumentSearchResult>, PortError> {
        let instruments = self
            .instruments
            .list_instruments()
            .await
            .map_err(feed_port_error)?;
        let q = query.trim().to_lowercase();
        let hits = instruments
            .into_iter()
            .filter(|inst| inst.id().symbol.to_lowercase().contains(&q))
            .map(|inst| InstrumentSearchResult {
                instrument: inst.id().clone(),
                name: inst
                    .display_name()
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .unwrap_or_else(|| inst.id().symbol.clone()),
                exchange: inst.id().exchange.as_str().to_string(),
                lot_size: Some(inst.lot_size().round() as u64),
            })
            .collect();
        Ok(hits)
    }

    async fn macro_series(&self, series: &str) -> Result<MacroSeries, PortError> {
        let Some(reader) = &self.economic_calendar else {
            return Err(PortError::unavailable(
                "economic-calendar reader not wired; see tantra-runtime/docs/gaps/0007 item 3b",
            ));
        };
        let projections = reader.read().map_err(|e| {
            PortError::internal(format!("failed to read economic-calendar projections: {e}"))
        })?;
        let needle = series.to_lowercase();
        let mut points: Vec<(DateTime<Utc>, Decimal)> = projections
            .into_iter()
            .filter(|row| {
                row.event_name.to_lowercase().contains(&needle)
                    || row.category.to_lowercase().contains(&needle)
            })
            .filter_map(|row| {
                let ts = chrono::DateTime::parse_from_rfc3339(&row.scheduled_at).ok()?;
                let value = row.actual.or(row.forecast).or(row.prior)?;
                Some((ts.with_timezone(&Utc), value))
            })
            .collect();
        points.sort_by_key(|(ts, _)| *ts);
        Ok(MacroSeries {
            series: series.to_string(),
            points,
        })
    }

    async fn block_deals(
        &self,
        instrument: Option<&InstrumentId>,
        date: Option<DateTime<Utc>>,
    ) -> Result<Vec<BlockDeal>, PortError> {
        let Some(reader) = &self.block_deals else {
            // No block-deal projections configured — the reader is only wired by
            // [`FeedMarketReader::with_block_deals`]. An explicit error, not an empty
            // success, so the model can't mistake "not wired up" for "no deals happened
            // today" (same pattern as `indicators`/`macro_series` above).
            return Err(PortError::unavailable(
                "no block/bulk-deal projections configured; see tantra-runtime/docs/gaps/0007 item 6",
            ));
        };
        let projections = reader.read().map_err(|e| {
            PortError::internal(format!("failed to read block-deal projections: {e}"))
        })?;
        let deals: Vec<BlockDeal> = projections
            .into_iter()
            .filter_map(to_block_deal)
            .filter(|d| instrument.is_none_or(|inst| d.instrument == *inst))
            .filter(|d| date.is_none_or(|ts| d.trade_date.date_naive() == ts.date_naive()))
            .collect();
        Ok(deals)
    }

    async fn price_band(&self, instrument: &InstrumentId) -> Result<PriceBand, PortError> {
        let engine = self
            .risk_engine
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        engine
            .price_bands
            .get(instrument)
            .copied()
            .ok_or_else(|| PortError::not_found(format!("Price band not found for {instrument}")))
    }

    async fn options_chain(
        &self,
        underlying: &InstrumentId,
        expiry: Option<DateTime<Utc>>,
        limit: Option<usize>,
    ) -> Result<OptionsChain, PortError> {
        const MAX_CHAIN_CAP: usize = 500;
        let effective_limit = limit.map(|l| l.min(MAX_CHAIN_CAP)).unwrap_or(MAX_CHAIN_CAP);
        // Calendar-day match, not exact-instant equality: callers pass an expiry trading
        // day, contracts carry their own expiry instant (same rule as `block_deals`).
        let expiry = expiry.map(|e| e.date_naive());
        let contracts = self
            .chain
            .options_chain(underlying, expiry, Some(effective_limit))
            .await
            .map_err(feed_port_error)?
            .into_iter()
            .map(to_option_contract)
            .collect();
        Ok(OptionsChain {
            underlying: underlying.clone(),
            as_of: Utc::now(),
            contracts,
        })
    }

    async fn corporate_actions(
        &self,
        instrument: Option<&InstrumentId>,
        action_type: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<CorporateAction>, PortError> {
        let Some(reader) = &self.corporate_actions else {
            // No corporate-actions projections configured — same explicit gap citation
            // as before the bridge landed (see
            // tantra-data/docs/gaps/0008-corporate-actions-rust-bridge.md). An explicit
            // error, not an empty success (same pattern as `indicators`/`macro_series`
            // above, per docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md).
            return Err(PortError::unavailable(
                "corporate-actions data is not served yet; see tantra-data/docs/gaps/0008-corporate-actions-rust-bridge.md",
            ));
        };
        let projections = reader.read().map_err(|e| {
            PortError::internal(format!("failed to read corporate-actions projections: {e}"))
        })?;
        let mut actions: Vec<CorporateAction> = projections
            .into_iter()
            .filter_map(to_corporate_action)
            .filter(|a| instrument.is_none_or(|inst| a.instrument == *inst))
            .filter(|a| action_type.is_none_or(|at| a.action_type == *at))
            .collect();
        actions.sort_by_key(|a| a.ex_date);
        actions.truncate(limit.unwrap_or(usize::MAX));
        Ok(actions)
    }

    async fn market_depth(
        &self,
        instrument: &InstrumentId,
        levels: Option<usize>,
    ) -> Result<MarketDepth, PortError> {
        let Some(reader) = &self.depth else {
            // No L2 order-book depth reader is wired — this runtime's `GET /depth/{id}` reads
            // from the same `DepthReader` port, and the parquet lakehouse / live stores only
            // answer once depth ticks are ingested. An explicit error, not an empty success,
            // so the model can't mistake "not wired up" for "this instrument really has an
            // empty book" (same pattern as `indicators`/`macro_series` above).
            return Err(PortError::unavailable(
                "L2 order-book depth reader not wired; see tantra-market/docs/gaps/0006-l2-order-book-depth.md",
            ));
        };
        let levels = levels.unwrap_or(5).clamp(1, 20);
        let DepthSnapshot { as_of, bids, asks } = reader
            .read_depth(instrument, levels)
            .await
            .map_err(feed_port_error)?;
        let bids: Vec<MarketDepthLevel> = bids
            .into_iter()
            .map(|(price, quantity)| MarketDepthLevel { price, quantity })
            .collect();
        let asks: Vec<MarketDepthLevel> = asks
            .into_iter()
            .map(|(price, quantity)| MarketDepthLevel { price, quantity })
            .collect();
        // Liquidity imbalance percent = (bid_total - ask_total) / (bid_total + ask_total) * 100,
        // exact Decimal, zero when the book is empty on one or both sides.
        let bid_total: Decimal = bids
            .iter()
            .fold(Decimal::ZERO, |acc, level| acc + level.quantity.value());
        let ask_total: Decimal = asks
            .iter()
            .fold(Decimal::ZERO, |acc, level| acc + level.quantity.value());
        let liquidity_imbalance_pct = if (bid_total + ask_total).is_zero() {
            Decimal::ZERO
        } else {
            (bid_total - ask_total) * Decimal::from(100) / (bid_total + ask_total)
        };
        Ok(MarketDepth {
            instrument: instrument.clone(),
            as_of: as_of.to_datetime(),
            bids,
            asks,
            liquidity_imbalance_pct,
        })
    }

    async fn institutional_flows(
        &self,
        date: Option<DateTime<Utc>>,
    ) -> Result<Vec<InstitutionalFlow>, PortError> {
        let Some(reader) = &self.institutional_flows else {
            // No institutional-flows projections configured — the reader is only wired by
            // [`FeedMarketReader::with_institutional_flows`]. An explicit error, not an
            // empty success, so the model can't mistake "not wired up" for "no flows
            // happened that day" (same pattern as `indicators`/`macro_series`).
            return Err(PortError::unavailable(
                "institutional-flow reader not wired; see tantra-runtime/docs/gaps/0007 item 11",
            ));
        };
        let projections = reader.read().map_err(|e| {
            PortError::internal(format!(
                "failed to read institutional-flow projections: {e}"
            ))
        })?;
        let flows: Vec<InstitutionalFlow> = projections
            .into_iter()
            .filter(|r| r.segment == "cash")
            .filter(|r| date.is_none_or(|d| r.trade_date == d.date_naive()))
            .map(|r| InstitutionalFlow {
                category: r.participant.to_lowercase(),
                buy_value: Money::new(r.gross_buy, Currency::Inr),
                sell_value: Money::new(r.gross_sell, Currency::Inr),
                net_value: Money::new(r.net, Currency::Inr),
                trade_date: Utc.from_utc_datetime(
                    &r.trade_date
                        .and_hms_opt(0, 0, 0)
                        .expect("midnight is always a valid time of day"),
                ),
            })
            .collect();
        Ok(flows)
    }

    async fn delivery_stats(
        &self,
        instrument: &InstrumentId,
        range: DateRange,
        limit: Option<usize>,
    ) -> Result<Vec<DeliveryBar>, PortError> {
        let Some(reader) = &self.delivery else {
            // No delivery projections configured — the reader is only wired by
            // [`FeedMarketReader::with_delivery`]. An explicit error, not an empty success,
            // so the model can't mistake "not wired up" for "no delivery prints" (same
            // pattern as `indicators`/`macro_series`).
            return Err(PortError::unavailable(
                "delivery reader not wired; see tantra-runtime/docs/gaps/0007 item 12",
            ));
        };
        let projections = reader.read().map_err(|e| {
            PortError::internal(format!("failed to read delivery projections: {e}"))
        })?;
        let mut bars: Vec<DeliveryBar> = projections
            .into_iter()
            .filter(|r| r.instrument.eq_ignore_ascii_case(&instrument.to_string()))
            .filter(|r| {
                range.start.is_none_or(|s| r.trade_date >= s.date_naive())
                    && range.end.is_none_or(|e| r.trade_date <= e.date_naive())
            })
            .map(|r| DeliveryBar {
                instrument: instrument.clone(),
                date: Utc.from_utc_datetime(
                    &r.trade_date
                        .and_hms_opt(0, 0, 0)
                        .expect("midnight is always a valid time of day"),
                ),
                traded_qty: Quantity::new(Decimal::from(r.traded_quantity)),
                delivered_qty: Quantity::new(Decimal::from(r.deliverable_quantity)),
                delivery_pct: r.delivery_pct,
            })
            .collect();
        bars.sort_by_key(|b| b.date);
        bars.truncate(limit.unwrap_or(usize::MAX));
        Ok(bars)
    }

    async fn oi_buildup(
        &self,
        instrument: &InstrumentId,
        lookback_days: Option<u32>,
    ) -> Result<OiBuildupSnapshot, PortError> {
        let Some(store) = &self.oi_store else {
            return Err(PortError::unavailable(
                "no open-interest store wired in the runtime chat adapters; see tantra-runtime/docs/gaps/0007 item 13",
            ));
        };
        let Some(buildup) = &self.buildup else {
            return Err(PortError::unavailable(
                "no open-interest store wired in the runtime chat adapters; see tantra-runtime/docs/gaps/0007 item 13",
            ));
        };
        let (start, end) = oi_window(lookback_days);
        let signal = buildup
            .buildup(instrument, start, end)
            .await
            .map_err(feed_port_error)?
            .ok_or_else(|| {
                PortError::unavailable(
                    "not enough OI or bar observations in the window; see tantra-runtime/docs/gaps/0007 item 13",
                )
            })?;

        // Absolute end-of-window OI and percentage changes need the raw series; the
        // analyzer only classifies the move.
        let history = store
            .history(instrument, start, Some(end))
            .await
            .map_err(feed_port_error)?;
        let open_interest = history.last().map(|s| s.open_interest).ok_or_else(|| {
            PortError::unavailable(
                "open-interest series is empty; see tantra-runtime/docs/gaps/0007 item 13",
            )
        })?;
        let first_oi = history
            .first()
            .map(|s| s.open_interest.value())
            .ok_or_else(|| {
                PortError::unavailable(
                    "open-interest series is empty; see tantra-runtime/docs/gaps/0007 item 13",
                )
            })?;
        let oi_change_pct = if first_oi.is_zero() {
            None
        } else {
            let last_oi = open_interest.value();
            Some((last_oi - first_oi) / first_oi * Decimal::from(100))
        };

        let spec = crate::market_feed::wire::BarSpecification::new(
            1,
            crate::market_feed::wire::BarAggregation::Day,
            crate::market_feed::wire::PriceType::Last,
        );
        let from = start.and_hms_opt(0, 0, 0).unwrap().and_utc();
        let to = end
            .checked_add_days(chrono::Days::new(1))
            .unwrap_or(end)
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let request = crate::market_feed::ports::BarRequest::new(
            instrument.clone(),
            spec,
            Some(crate::market_feed::wire::UnixNanos::from_datetime(from)),
            Some(crate::market_feed::wire::UnixNanos::from_datetime(to)),
        )
        .map_err(feed_port_error)?;
        let bars = self
            .bars
            .read_bars(&request)
            .await
            .map_err(feed_port_error)?;
        let first_close = bars.first().map(|b| to_decimal(b.close())).ok_or_else(|| {
            PortError::unavailable("not enough bar observations in the window; see tantra-runtime/docs/gaps/0007 item 13")
        })?;
        let last_close = bars
            .last()
            .map(|b| to_decimal(b.close()))
            .unwrap_or(first_close);
        let price_change_pct = if first_close.is_zero() {
            None
        } else {
            Some((last_close - first_close) / first_close * Decimal::from(100))
        };

        Ok(OiBuildupSnapshot {
            instrument: instrument.clone(),
            as_of: end.and_hms_opt(0, 0, 0).unwrap().and_utc(),
            open_interest,
            oi_change_pct,
            price_change_pct,
            classification: signal.class.to_string(),
        })
    }
}

/// The `[start, end]` analysis window for a lookback of `lookback_days` trading days
/// ending today (default 5), as `NaiveDate`s.
fn oi_window(lookback_days: Option<u32>) -> (chrono::NaiveDate, chrono::NaiveDate) {
    let end = chrono::Utc::now().date_naive();
    let start = end - chrono::Days::new(u64::from(lookback_days.unwrap_or(5)));
    (start, end)
}

/// Parses a canonical `EXCHANGE:SYMBOL` instrument string (the projection form, e.g.
/// `NSE:RELIANCE-EQ`) into an [`InstrumentId`]. `InstrumentId` has no `FromStr` impl in
/// `tantra-core` (its `Exchange` component does), so the canonical projection form is split
/// here; rows with an unknown exchange or no symbol are rejected and skipped by the caller.
/// `pub(crate)` so the chat runtime can resolve the `TANTRA_REGIME_INSTRUMENT` env var the
/// same way the projection rows are parsed.
pub(crate) fn parse_instrument(s: &str) -> Option<InstrumentId> {
    let (exchange, symbol) = s.split_once(':')?;
    if symbol.is_empty() {
        return None;
    }
    let exchange = exchange.parse::<Exchange>().ok()?;
    Some(InstrumentId::new(exchange, symbol))
}

/// The indicator families [`FeedMarketReader::indicators`] recognizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndicatorKind {
    Sma,
    Ema,
    Rsi,
    Roc,
    Cmo,
    Atr,
    Adx,
    Mfi,
    Bb,
    Obv,
    Vwap,
    Macd,
}

/// Parses an indicator name (`sma_20`, `macd_12_26_9`, `obv`, ...) into its kind and
/// numeric parameters. The base name is the first `_`-separated token; the rest must be
/// `usize` parameters. `macd` defaults to 12/26/9 and `bb` to 20 when no parameters are
/// given; the other parameterized indicators require theirs, and unknown base names are
/// rejected so the caller can skip them.
fn parse_indicator_name(name: &str) -> Option<(IndicatorKind, Vec<usize>)> {
    let mut parts = name.split('_');
    let base = parts.next()?;
    let params: Vec<usize> = parts.filter_map(|p| p.parse().ok()).collect();
    let kind = match base {
        "sma" => IndicatorKind::Sma,
        "ema" => IndicatorKind::Ema,
        "rsi" => IndicatorKind::Rsi,
        "roc" => IndicatorKind::Roc,
        "cmo" => IndicatorKind::Cmo,
        "atr" => IndicatorKind::Atr,
        "adx" => IndicatorKind::Adx,
        "mfi" => IndicatorKind::Mfi,
        "bb" => IndicatorKind::Bb,
        "obv" => IndicatorKind::Obv,
        "vwap" => IndicatorKind::Vwap,
        "macd" => IndicatorKind::Macd,
        _ => return None,
    };
    match kind {
        IndicatorKind::Macd => Some((
            kind,
            if params.is_empty() {
                vec![12, 26, 9]
            } else {
                params
            },
        )),
        IndicatorKind::Bb => Some((kind, if params.is_empty() { vec![20] } else { params })),
        IndicatorKind::Obv | IndicatorKind::Vwap => Some((kind, params)),
        _ if params.is_empty() => None,
        _ => Some((kind, params)),
    }
}

/// Maps one lakehouse corporate-action projection row onto the chat tool catalog's
/// record. Rows whose instrument string does not parse as a canonical
/// [`InstrumentId`], or whose action type is outside the canonical
/// {split, bonus, dividend, rights} set, are skipped. The projections are
/// INR-denominated, so per-share dividend amounts map to `Currency::Inr`.
fn to_corporate_action(projection: CorporateActionProjection) -> Option<CorporateAction> {
    let instrument = parse_instrument(&projection.instrument)?;
    if !matches!(
        projection.action_type.as_str(),
        "split" | "bonus" | "dividend" | "rights"
    ) {
        return None;
    }
    let amount = if projection.action_type == "dividend" {
        projection
            .per_share
            .map(|per_share| Money::new(per_share, Currency::Inr))
    } else {
        None
    };
    Some(CorporateAction {
        instrument,
        action_type: projection.action_type,
        ex_date: Utc.from_utc_datetime(
            &projection
                .ex_date
                .and_hms_opt(0, 0, 0)
                .expect("midnight is always a valid time of day"),
        ),
        record_date: None,
        amount,
        ratio_from: projection
            .old_shares
            .map(|old| Quantity::new(Decimal::from(old))),
        ratio_to: projection
            .new_shares
            .map(|new| Quantity::new(Decimal::from(new))),
    })
}

/// Maps one lakehouse block/bulk-deal projection row onto the chat tool catalog's record.
/// Rows whose instrument string does not parse as a canonical [`InstrumentId`] are skipped.
fn to_block_deal(projection: BlockDealProjection) -> Option<BlockDeal> {
    let instrument = parse_instrument(&projection.instrument)?;
    Some(BlockDeal {
        instrument,
        exchange: projection.exchange,
        client_name: projection.client_name,
        side: projection.side.to_lowercase(), // "BUY"/"SELL" → "buy"/"sell"
        category: projection.category,
        quantity: Quantity::new(projection.quantity),
        price: Price::new(projection.price),
        trade_date: Utc.from_utc_datetime(
            &projection
                .trade_date
                .and_hms_opt(0, 0, 0)
                .expect("midnight is always a valid time of day"),
        ),
    })
}

/// Read-through `SnapshotSource` grounding screener/universe tools in the live instrument
/// catalog `AppState` already holds, optionally enriched with the shared risk engine's
/// OTR/price-band metrics and a daily-bar regime classification.
pub struct FeedSnapshotSource {
    instruments: Arc<dyn InstrumentMaster>,
    risk_engine: Option<Arc<Mutex<RiskEngine>>>,
    bars: Option<Arc<dyn BarReader>>,
    regime_instrument: Option<InstrumentId>,
}

impl FeedSnapshotSource {
    /// Builds a [`FeedSnapshotSource`] over the given instrument catalog port.
    pub fn new(instruments: Arc<dyn InstrumentMaster>) -> Self {
        Self {
            instruments,
            risk_engine: None,
            bars: None,
            regime_instrument: None,
        }
    }

    /// Attaches the shared risk engine so `snapshot` reports its OTR limits and
    /// price-band count (closes `tantra-runtime/docs/gaps/0007` item 5b).
    pub fn with_risk_engine(mut self, risk_engine: Arc<Mutex<RiskEngine>>) -> Self {
        self.risk_engine = Some(risk_engine);
        self
    }

    /// Attaches a daily-bar source and the instrument whose closes classify the snapshot's
    /// market regime (closes `tantra-runtime/docs/gaps/0007` item 5a).
    pub fn with_regime(mut self, bars: Arc<dyn BarReader>, instrument: InstrumentId) -> Self {
        self.bars = Some(bars);
        self.regime_instrument = Some(instrument);
        self
    }
}

/// Classifies the market regime from daily closes: `trending_bull` when the last close sits
/// above its 20-SMA and that SMA is rising, `trending_bear` below a falling SMA, otherwise
/// `range_bound`. Needs at least 30 closes so the SMA-trend comparison has depth; a shorter
/// series classifies `None`.
fn classify_regime(closes: &[f64]) -> Option<String> {
    if closes.len() < 30 {
        return None;
    }
    let mut sma = Sma::new(20);
    let mut values = Vec::with_capacity(closes.len());
    for close in closes {
        if let Some(v) = sma.update(*close) {
            values.push(v);
        }
    }
    let last = closes[closes.len() - 1];
    let sma_now = values[values.len() - 1];
    let sma_prev = values[values.len() - 2];
    if last > sma_now && sma_now > sma_prev {
        Some("trending_bull".to_string())
    } else if last < sma_now && sma_now < sma_prev {
        Some("trending_bear".to_string())
    } else {
        Some("range_bound".to_string())
    }
}

#[async_trait]
impl SnapshotSource for FeedSnapshotSource {
    async fn snapshot(&self, _needs: &SnapshotNeeds) -> Result<SnapshotData, PortError> {
        let instruments = self
            .instruments
            .list_instruments()
            .await
            .map_err(feed_port_error)?;
        let universe: Vec<String> = instruments
            .into_iter()
            .map(|i| i.id().symbol.clone())
            .collect();

        let mut risk_metrics = HashMap::new();
        if let Some(engine) = &self.risk_engine {
            let engine = engine.lock().unwrap_or_else(PoisonError::into_inner);
            risk_metrics.insert(
                "otr_max_ratio".to_string(),
                serde_json::json!(engine.otr_config.max_otr.to_string()),
            );
            risk_metrics.insert(
                "otr_warning_threshold".to_string(),
                serde_json::json!(engine.otr_config.warning_threshold.to_string()),
            );
            risk_metrics.insert(
                "price_band_count".to_string(),
                serde_json::json!(engine.price_bands.len()),
            );
        }

        let regime = match (&self.bars, &self.regime_instrument) {
            (Some(bars), Some(instrument)) => {
                let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
                let request = BarRequest::new(instrument.clone(), spec, None, None)
                    .map_err(feed_port_error)?;
                let wire_bars = bars.read_bars(&request).await.map_err(feed_port_error)?;
                let closes: Vec<f64> = wire_bars.iter().map(|b| b.close()).collect();
                classify_regime(&closes)
            }
            _ => None,
        };

        Ok(SnapshotData {
            as_of: Utc::now(),
            universe: if universe.is_empty() {
                None
            } else {
                Some(universe)
            },
            regime,
            risk_metrics,
        })
    }
}
