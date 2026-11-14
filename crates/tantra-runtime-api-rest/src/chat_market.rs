//! Read-through `MarketReader`/`SnapshotSource` adapters bridging the feed and risk ports
//! `AppState` already holds to the chat tool catalog's market/screener tools (closes the third
//! stand-in of docs/gaps/0006-chat-agent-session-persistence-and-portfolio-tools.md).
//!
//! Every call reads straight through to the live port each time — no cache, so quotes and bars
//! are never stale and nothing needs a background refresh task. A few fields have no backing
//! data source anywhere in this runtime yet and are reported as explicit gaps rather than
//! fabricated values; see docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md.

use std::sync::{Arc, Mutex, PoisonError};

use crate::market_feed::ports::{
    BarReader, BarRequest, ChainContract, ChainReader, InstrumentMaster, QuoteReader,
};
use crate::market_feed::wire::{BarAggregation, BarSpecification, PriceType, UnixNanos};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::market::{
    BlockDeal, CorporateAction, DeliveryBar, InstitutionalFlow, InstrumentSearchResult,
    MacroSeries, MarketBar, MarketDepth, MarketHistory, MarketIndicators, MarketQuote,
    MarketReader, OiBuildupSnapshot, OptionContract, OptionsChain,
};
use tantra_agent_chat_tools::ports::portfolio::DateRange;
use tantra_agent_chat_tools::ports::snapshot::{SnapshotData, SnapshotNeeds, SnapshotSource};
use tantra_core::{Currency, Exchange, InstrumentId, Money, Price, PriceBand, Quantity};
use tantra_market_store::{
    CorporateActionProjection, CorporateActionsProjectionReader, InstrumentChainReader,
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
        }
    }

    /// Builds a [`FeedMarketReader`] over the given feed ports and shared risk engine,
    /// additionally wiring the `tantra-data` corporate-actions projections
    /// (`{root}/catalog/corporate_actions`) through `tantra-market-store`'s projection
    /// reader (closes `tantra-data/docs/gaps/0008-corporate-actions-rust-bridge.md`).
    pub fn with_corporate_actions(
        root: &std::path::Path,
        instruments: Arc<dyn InstrumentMaster>,
        bars: Arc<dyn BarReader>,
        quotes: Arc<dyn QuoteReader>,
        risk_engine: Arc<Mutex<RiskEngine>>,
    ) -> Self {
        Self {
            corporate_actions: Some(CorporateActionsProjectionReader::new(root)),
            ..Self::new(instruments, bars, quotes, risk_engine)
        }
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
        // No last-trade print or traded-volume field is wired into `QuoteReader` (top-of-book
        // only) — see docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md.
        let mid = (bid + ask) / Decimal::from(2);
        Ok(MarketQuote {
            instrument: instrument.clone(),
            last_price: Price::new(mid),
            bid: Some(Price::new(bid)),
            ask: Some(Price::new(ask)),
            volume: Quantity::new(Decimal::ZERO),
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
        _instrument: &InstrumentId,
        _names: &[String],
    ) -> Result<MarketIndicators, PortError> {
        // No indicator engine is wired into this runtime's chat tools yet — see
        // docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md. An explicit
        // error, not an empty success, so the model can't mistake "not wired up" for "this
        // instrument really has no indicator values."
        Err(PortError::unavailable(
            "indicator engine not wired up; see docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md",
        ))
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
                // No instrument display-name master-data field exists yet — see
                // docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md.
                name: inst.id().symbol.clone(),
                exchange: inst.id().exchange.as_str().to_string(),
                lot_size: Some(inst.lot_size().round() as u64),
            })
            .collect();
        Ok(hits)
    }

    async fn macro_series(&self, _series: &str) -> Result<MacroSeries, PortError> {
        // No macro-economic data source is wired into this runtime yet — see
        // docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md.
        Err(PortError::unavailable(
            "macro data source not wired up; see docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md",
        ))
    }

    async fn block_deals(
        &self,
        _instrument: Option<&InstrumentId>,
        _date: Option<DateTime<Utc>>,
    ) -> Result<Vec<BlockDeal>, PortError> {
        // No block/bulk deal disclosure data source is ingested anywhere yet — see
        // tantra-data/docs/gaps/0005-block-bulk-deal-disclosure-ingestion.md. An explicit
        // error, not an empty success, so the model can't mistake "not wired up" for "no
        // deals happened today" (same pattern as `indicators`/`macro_series` above, per
        // docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md).
        Err(PortError::unavailable(
            "block/bulk deal disclosures are not yet ingested; see tantra-data/docs/gaps/0005-block-bulk-deal-disclosure-ingestion.md",
        ))
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
        _instrument: &InstrumentId,
        _levels: Option<usize>,
    ) -> Result<MarketDepth, PortError> {
        // No L2 order-book depth source is wired into this runtime yet — the feed ports
        // serve top-of-book quotes only — see
        // tantra-market/docs/gaps/0006-l2-order-book-depth.md. An explicit error, not an
        // empty success, so the model can't mistake "not wired up" for "this instrument
        // really has an empty book" (same pattern as `indicators`/`macro_series` above).
        Err(PortError::unavailable(
            "L2 order-book depth source not wired; see tantra-market/docs/gaps/0006-l2-order-book-depth.md",
        ))
    }

    async fn institutional_flows(
        &self,
        _date: Option<DateTime<Utc>>,
    ) -> Result<Vec<InstitutionalFlow>, PortError> {
        // FII/DII daily flows are ingested Python-side (`tantra-data` gap 0013 closed) but
        // have no Rust-consumer bridge yet — see
        // tantra-data/docs/gaps/0017-institutional-flow-rust-bridge.md. An explicit error,
        // not an empty success, so the model can't mistake "not wired up" for "no flows
        // happened that day" (same pattern as `indicators`/`macro_series` above).
        Err(PortError::unavailable(
            "FII/DII institutional-flow Rust bridge not wired; see tantra-data/docs/gaps/0017-institutional-flow-rust-bridge.md",
        ))
    }

    async fn delivery_stats(
        &self,
        _instrument: &InstrumentId,
        _range: DateRange,
        _limit: Option<usize>,
    ) -> Result<Vec<DeliveryBar>, PortError> {
        // Delivery % is ingested Python-side (`tantra-data` gap 0014 closed) but has no
        // Rust-consumer bridge yet — see
        // tantra-data/docs/gaps/0019-delivery-rust-bridge.md. An explicit error, not an
        // empty success, so the model can't mistake "not wired up" for "no delivery
        // prints" (same pattern as `indicators`/`macro_series` above).
        Err(PortError::unavailable(
            "delivery % Rust bridge not wired; see tantra-data/docs/gaps/0019-delivery-rust-bridge.md",
        ))
    }

    async fn oi_buildup(
        &self,
        _instrument: &InstrumentId,
        _lookback_days: Option<u32>,
    ) -> Result<OiBuildupSnapshot, PortError> {
        // The OI time-series store + four-quadrant classifier exist in `tantra-market-store`
        // (`tantra-market/docs/gaps/0002` closed) but this runtime holds no OI store
        // instance — see docs/gaps/0007 item 12. An explicit error, not an empty success,
        // so the model can't mistake "not wired up" for "no position movement" (same
        // pattern as `indicators`/`macro_series` above).
        Err(PortError::unavailable(
            "no open-interest store wired in the runtime chat adapters; see tantra-runtime/docs/gaps/0007 item 12",
        ))
    }
}

/// Parses a canonical `EXCHANGE:SYMBOL` instrument string (the projection form, e.g.
/// `NSE:RELIANCE-EQ`) into an [`InstrumentId`]. `InstrumentId` has no `FromStr` impl in
/// `tantra-core` (its `Exchange` component does), so the canonical projection form is split
/// here; rows with an unknown exchange or no symbol are rejected and skipped by the caller.
fn parse_instrument(s: &str) -> Option<InstrumentId> {
    let (exchange, symbol) = s.split_once(':')?;
    if symbol.is_empty() {
        return None;
    }
    let exchange = exchange.parse::<Exchange>().ok()?;
    Some(InstrumentId::new(exchange, symbol))
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

/// Read-through `SnapshotSource` grounding screener/universe tools in the live instrument
/// catalog `AppState` already holds.
pub struct FeedSnapshotSource {
    instruments: Arc<dyn InstrumentMaster>,
}

impl FeedSnapshotSource {
    /// Builds a [`FeedSnapshotSource`] over the given instrument catalog port.
    pub fn new(instruments: Arc<dyn InstrumentMaster>) -> Self {
        Self { instruments }
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
        Ok(SnapshotData {
            as_of: Utc::now(),
            universe: if universe.is_empty() {
                None
            } else {
                Some(universe)
            },
            // No regime classifier or risk-metrics feed is wired into chat tools yet — see
            // docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md.
            regime: None,
            risk_metrics: std::collections::HashMap::new(),
        })
    }
}
