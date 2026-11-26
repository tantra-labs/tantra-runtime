//! Strategy intent to multi-broker dispatch boundary.
//!
//! Provides the [`BrokerAdapter`] boundary, [`Dispatcher`] coordinator, and
//! paper/Zerodha/AngelOne execution adapters and scaffolds. Live brokers are
//! injected as `Arc<dyn BrokerExecutor>` by the composition root.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tantra_core::uuid::Uuid;
use tantra_core::{FillReport, InstrumentId, OrderRequest, OrderSide, Price, Quantity, Tick};
use tantra_exec_core::{BrokerError, BrokerExecutor};
use tantra_exec_paper::{FillCostFn, OrderState, PaperExchange};
use tantra_market_finance::india::costs::NseCashEquitySchedule;
use tantra_market_finance::{CostSchedule, MarketSegment};
use tantra_risk::{RiskEngine, RiskRefusal};
use tokio::runtime::{Handle, RuntimeFlavor};

use crate::intent::{IntentError, OrderIntent};
use crate::synthetic_oco::SyntheticOco;
use tantra_exec_core::bracket::BracketOrderSpec;

/// Re-export of the deterministic simulation execution port (exec-core
/// `SimExecutionPort`), lifted here per gap 0008 option 2; runtime's dual-port
/// boundary is now a thin alias.
pub use tantra_exec_core::sim::SimExecutionPort as BrokerAdapter;

/// Errors arising during intent dispatch.
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    /// Intent validation failure (e.g. non-positive quantity).
    #[error("intent error: {0}")]
    Intent(#[from] IntentError),

    /// Pre-trade SEBI / exchange risk check refusal.
    #[error("SEBI risk gate refusal: {0}")]
    RiskRefusal(Box<RiskRefusal>),

    /// Broker venue rejected or failed order execution.
    #[error("broker rejection: {0}")]
    BrokerRejection(String),

    /// Broker session expired or authentication failure.
    #[error("session expired: {0}")]
    SessionExpired(String),

    /// Market is closed or instrument not traded.
    #[error("market closed: {0}")]
    MarketClosed(String),

    /// Rate limit exceeded on broker API.
    #[error("rate limited: {0}")]
    RateLimited(String),

    /// Network connectivity or transport error.
    #[error("network error: {0}")]
    Network(String),

    /// A synthetic bracket (OCO) spec or lifecycle rejection.
    #[error("bracket rejection: {0}")]
    Bracket(String),
}

impl From<RiskRefusal> for DispatchError {
    fn from(err: RiskRefusal) -> Self {
        Self::RiskRefusal(Box::new(err))
    }
}

/// Map a [`tantra_exec_core::BrokerError`] produced by a real wire transport
/// into a human-readable string carried by [`DispatchError::BrokerRejection`].
///
/// The [`BrokerAdapter`] trait returns `Result<String, String>` to keep the
/// boundary sync and temporally deterministic, so the broker error taxonomy is
/// surfaced in the message text. A future trait refactor can promote these to
/// fully typed [`DispatchError`] variants (`SessionExpired`, `RateLimited`,
/// `Network`, `MarketClosed`).
fn map_broker_error(err: BrokerError) -> String {
    match err {
        BrokerError::Auth(msg) => format!("session expired: {msg}"),
        BrokerError::RateLimited(msg) => format!("rate limited: {msg}"),
        BrokerError::Transport(msg) => format!("network error: {msg}"),
        BrokerError::Timeout => "network error: request timed out".to_string(),
        BrokerError::OutcomeUnknown {
            client_order_id,
            message,
        } => format!("network error: order outcome unknown ({client_order_id}): {message}"),
        BrokerError::OrderNotFound(msg)
        | BrokerError::Rejected(msg)
        | BrokerError::InvalidRequest(msg) => {
            format!("broker rejection: {msg}")
        }
        BrokerError::RiskRefusal(_) => "broker rejection: pre-trade risk refusal".to_string(),
    }
}

/// High-throughput synchronous dispatcher connecting strategy intents to broker execution venues
/// via the synchronous SEBI pre-trade risk gate.
pub struct Dispatcher {
    risk_engine: RiskEngine,
    broker: Box<dyn BrokerAdapter>,
    algo_id: String,
}

impl Dispatcher {
    /// Creates a new [`Dispatcher`] with the given risk engine and broker adapter.
    ///
    /// Automatically registers the default `algo_id` (`"STRATEGY_DISPATCH"`) in the
    /// risk engine's SEBI algorithm registry.
    pub fn new(mut risk_engine: RiskEngine, broker: Box<dyn BrokerAdapter>) -> Self {
        let algo_id = "STRATEGY_DISPATCH".to_string();
        risk_engine.register_algo(&algo_id);
        Self {
            risk_engine,
            broker,
            algo_id,
        }
    }

    /// Sets a custom SEBI algorithm identifier and registers it with the risk engine.
    pub fn with_algo_id(mut self, algo_id: impl Into<String>) -> Self {
        self.algo_id = algo_id.into();
        self.risk_engine.register_algo(&self.algo_id);
        self
    }

    /// Returns the active SEBI algo ID for this dispatcher.
    pub fn algo_id(&self) -> &str {
        &self.algo_id
    }

    /// Returns an immutable reference to the underlying broker adapter.
    pub fn broker(&self) -> &dyn BrokerAdapter {
        &*self.broker
    }

    /// Returns an immutable reference to the SEBI risk engine.
    pub fn risk_engine(&self) -> &RiskEngine {
        &self.risk_engine
    }

    /// Returns a mutable reference to the SEBI risk engine.
    pub fn risk_engine_mut(&mut self) -> &mut RiskEngine {
        &mut self.risk_engine
    }

    /// Validates an [`OrderIntent`], passes it through the synchronous SEBI pre-trade
    /// risk engine (<15 µs evaluation SLO), and dispatches it to the broker adapter.
    ///
    /// Mints a fresh random [`OrderRequest::client_order_id`]; this is the
    /// interactive convenience path. Journal replays, seeded backtests, and
    /// restored sessions must use [`Dispatcher::dispatch_intent_with_id`] so
    /// the run is byte-for-byte reproducible.
    pub fn dispatch_intent(
        &mut self,
        intent: &OrderIntent,
        now: DateTime<Utc>,
    ) -> Result<String, DispatchError> {
        let order = intent.to_order_request_with_algo(&self.algo_id)?;
        self.dispatch_order(&order, now)
    }

    /// Validates an [`OrderIntent`], passes it through the synchronous SEBI pre-trade
    /// risk engine (<15 µs evaluation SLO), and dispatches it to the broker adapter,
    /// stamping the order with the caller-supplied deterministic
    /// `client_order_id`.
    ///
    /// This is the deterministic-replay entry point: given the same intent
    /// sequence, IDs, and clock, every fill produced by a simulated adapter is
    /// byte-identical.
    pub fn dispatch_intent_with_id(
        &mut self,
        intent: &OrderIntent,
        client_order_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<String, DispatchError> {
        let order = intent.to_order_request_with_id(client_order_id, &self.algo_id)?;
        self.dispatch_order(&order, now)
    }

    /// Evaluates an existing [`OrderRequest`] against the SEBI pre-trade risk engine
    /// and submits it to the broker adapter upon approval.
    pub fn dispatch_order(
        &mut self,
        order: &OrderRequest,
        now: DateTime<Utc>,
    ) -> Result<String, DispatchError> {
        self.risk_engine
            .check_order(order, now)
            .map_err(DispatchError::from)?;

        self.broker
            .submit_order(order, now)
            .map_err(DispatchError::BrokerRejection)
    }

    /// Requests cancellation of an order through the broker adapter and records
    /// the event in the SEBI risk engine for OTR tracking.
    pub fn cancel_order(
        &mut self,
        broker_order_id: &str,
        now: DateTime<Utc>,
    ) -> Result<(), DispatchError> {
        self.broker
            .cancel_order(broker_order_id, now)
            .map_err(DispatchError::BrokerRejection)?;

        self.risk_engine.record_cancellation(&self.algo_id, now);
        Ok(())
    }

    /// Dispatch a synthetic OCO bracket (gap 0003 / T12.5 follow-on).
    ///
    /// Passes the bracket's entry leg through the SEBI pre-trade risk gate,
    /// then constructs a [`SyntheticOco`] driver and submits the entry through
    /// the broker adapter, returning the entry broker order id. The entry is
    /// re-stamped with this dispatcher's registered SEBI algo id (the same way
    /// [`Dispatcher::dispatch_intent`] stamps intents) so the pre-trade gate
    /// passes. The bracket's entry must carry a deterministic
    /// `client_order_id` (see [`crate::synthetic_oco::plan_to_spec_with_id`])
    /// so the seed derived from it replays byte-identically.
    ///
    /// This is a thin convenience over constructing a [`SyntheticOco`] on top
    /// of the dispatcher's broker; callers that want to drive the full bracket
    /// lifecycle (fills, stops, targets) should construct and keep the
    /// [`SyntheticOco`] themselves.
    ///
    /// # Errors
    /// - [`DispatchError::RiskRefusal`] when the pre-trade risk gate rejects
    ///   the entry leg.
    /// - [`DispatchError::BrokerRejection`] when the broker refuses the entry.
    /// - [`DispatchError::Bracket`] when the bracket spec is invalid or its
    ///   lifecycle invariants fail.
    pub fn dispatch_bracket(
        &mut self,
        mut spec: BracketOrderSpec,
        now: DateTime<Utc>,
    ) -> Result<String, DispatchError> {
        spec.entry.algo_id = self.algo_id.clone();
        self.risk_engine
            .check_order(&spec.entry, now)
            .map_err(DispatchError::from)?;

        let seed = spec.entry.client_order_id.as_u128();
        let mut oco = SyntheticOco::new(spec, self.broker.as_ref(), seed, now)
            .map_err(DispatchError::Bracket)?;
        oco.submit_entry(now)
            .map_err(DispatchError::BrokerRejection)
    }
}

/// Shared paper-exchange venue behind the simulated broker adapters.
///
/// Owns a [`tantra_exec_paper::PaperExchange`] as the single fill engine — all
/// fill math, positions, and statutory fee computation live in the exchange
/// (T14.6, plan 0005 D2) — plus the adapter-local reference-price mirrors and
/// the broker-id → request map backing the read APIs.
struct PaperVenue {
    /// The paper exchange that executes fills.
    exchange: PaperExchange,
    /// Adapter reference prices keyed by instrument; drives depth seeding at
    /// submit time so market orders fill at the reference price.
    market_prices: HashMap<InstrumentId, Decimal>,
    /// Last-seen reference price mirror for mark-to-market equity. The
    /// exchange has no public last-price reader, so the adapter keeps its own
    /// mirror updated by `set_market_price` and submit-time depth seeding.
    last_prices: HashMap<InstrumentId, Decimal>,
    /// broker_id → submitted [`OrderRequest`], backing the read API and the
    /// fill `algo_id` rewrite.
    orders: HashMap<String, OrderRequest>,
}

/// Real NSE cash-equity delivery cost model, plugged into the paper exchange
/// through its [`FillCostFn`] hook so simulated fills carry the same statutory
/// fees the previous inline `execute_order` charged (real fees, never zero).
///
/// The only f64 boundary is this fee-table computation, converted back with
/// `Decimal::from_f64_retain` — exactly the pattern the previous inline cost
/// path used; all ledger arithmetic stays in `Decimal`.
struct NseDeliveryFillCost;

impl FillCostFn for NseDeliveryFillCost {
    fn compute(&self, side: OrderSide, notional: Decimal) -> Decimal {
        let notional_f64 = notional.to_f64().unwrap_or(0.0);
        let schedule = NseCashEquitySchedule::delivery();
        let seg = MarketSegment::from("equity");
        let raw_fees = schedule.compute_costs(&seg, side, notional_f64);
        let total_f64 = [
            "brokerage",
            "stt",
            "exchange_fee",
            "gst",
            "stamp_duty",
            "sebi_fee",
        ]
        .into_iter()
        .map(|key| raw_fees.get(key).unwrap_or(0.0))
        .sum::<f64>();
        Decimal::from_f64_retain(total_f64).unwrap_or(Decimal::ZERO)
    }
}

/// Builds a paper exchange seeded with the given cash and the real NSE
/// cash-equity delivery cost model.
fn exchange_with_fees(initial_cash: Decimal) -> PaperExchange {
    PaperExchange::new(initial_cash).with_cost_fn(Box::new(NseDeliveryFillCost))
}

/// Submits an order through the shared paper venue.
///
/// When the adapter holds a reference price for the instrument, the exchange
/// is primed with it: the last price is set and (only when no depth exists
/// yet) synthetic depth is seeded at the reference price via a synthetic tick,
/// so a market order fills fully at the reference price and a limit order
/// rests until the market crosses it. Orders without a reference price pass
/// through untouched (a market order then fails with `NoMarketPrice`; a limit
/// order rests `Working`).
fn submit_to_venue(
    venue: &Mutex<PaperVenue>,
    order: &OrderRequest,
    now: DateTime<Utc>,
) -> Result<String, String> {
    let mut guard = venue.lock().expect("poisoned lock");

    if let Some(px) = guard.market_prices.get(&order.instrument).copied() {
        let price = Price::try_new(px).map_err(|e| format!("invalid market price: {e}"))?;
        guard
            .exchange
            .set_market_price(order.instrument.clone(), price);
        // Seed synthetic depth on both sides at the reference price once per
        // instrument. `feed_tick` never sets avg daily volume, so the
        // participation cap is off and market orders fill in full.
        if guard.exchange.depth(&order.instrument).is_none() {
            guard.exchange.feed_tick(&Tick::Ltp {
                instrument: order.instrument.clone(),
                price,
                volume: Quantity::from_u64(1_000_000),
                exchange_ts: now,
                receive_ts: now,
            });
        }
        guard.last_prices.insert(order.instrument.clone(), px);
    }

    let broker_id = guard
        .exchange
        .submit_order(order.clone(), now)
        .map_err(|e| e.to_string())?;
    guard.orders.insert(broker_id.clone(), order.clone());
    Ok(broker_id)
}

/// Cancels an order through the shared paper venue.
fn cancel_on_venue(
    venue: &Mutex<PaperVenue>,
    broker_order_id: &str,
    now: DateTime<Utc>,
) -> Result<(), String> {
    venue
        .lock()
        .expect("poisoned lock")
        .exchange
        .cancel_order(broker_order_id, now)
        .map_err(|e| e.to_string())
}

/// Sets/updates the adapter reference price, the equity price mirror, and the
/// exchange last price for an instrument. Depth is seeded lazily at submit.
fn venue_set_market_price(venue: &Mutex<PaperVenue>, instrument: InstrumentId, price: Decimal) {
    let mut guard = venue.lock().expect("poisoned lock");
    let Ok(px) = Price::try_new(price) else {
        return;
    };
    guard.market_prices.insert(instrument.clone(), price);
    guard.last_prices.insert(instrument.clone(), price);
    guard.exchange.set_market_price(instrument, px);
}

/// Returns the current cash balance in INR.
fn venue_cash(venue: &Mutex<PaperVenue>) -> Decimal {
    venue.lock().expect("poisoned lock").exchange.cash()
}

/// Returns the current open positions.
fn venue_positions(venue: &Mutex<PaperVenue>) -> HashMap<InstrumentId, Decimal> {
    venue
        .lock()
        .expect("poisoned lock")
        .exchange
        .positions()
        .clone()
}

/// Returns all execution fills recorded by the venue.
///
/// The exchange stamps fills with its own `algo_id`/`account_id`; rewrite
/// `algo_id` from the submitted request (looked up by broker id) and report
/// `account_id` as `None`, matching the adapter boundary contract.
fn venue_fills(venue: &Mutex<PaperVenue>) -> Vec<FillReport> {
    let guard = venue.lock().expect("poisoned lock");
    let orders = &guard.orders;
    guard
        .exchange
        .fills()
        .iter()
        .map(|fill| {
            let mut fill = fill.clone();
            if let Some(order) = orders.get(&fill.order_id) {
                fill.algo_id = order.algo_id.clone();
            }
            fill.account_id = None;
            fill
        })
        .collect()
}

/// Returns all orders submitted to the venue.
fn venue_orders(venue: &Mutex<PaperVenue>) -> HashMap<String, OrderRequest> {
    venue.lock().expect("poisoned lock").orders.clone()
}

/// Returns the paper exchange's lifecycle state for a broker order id, when
/// tracked (`OrderState::Cancelled` after a successful cancel).
fn venue_order_state(venue: &Mutex<PaperVenue>, broker_order_id: &str) -> Option<OrderState> {
    venue
        .lock()
        .expect("poisoned lock")
        .exchange
        .order(broker_order_id)
        .map(|o| o.state)
}

/// Computes current total portfolio equity (cash + mark-to-market positions).
fn venue_equity(venue: &Mutex<PaperVenue>) -> Decimal {
    venue.lock().expect("poisoned lock").exchange.equity()
}

/// Computes portfolio equity using an explicit set of market prices, falling
/// back to the venue's last-seen reference price per position.
fn venue_equity_with_prices(
    venue: &Mutex<PaperVenue>,
    prices: &HashMap<InstrumentId, Decimal>,
) -> Decimal {
    let guard = venue.lock().expect("poisoned lock");
    let mut total = guard.exchange.cash();
    for (inst, qty) in guard.exchange.positions() {
        let price = prices
            .get(inst)
            .or_else(|| guard.last_prices.get(inst))
            .copied()
            .unwrap_or(Decimal::ZERO);
        total += *qty * price;
    }
    total
}

/// In-memory paper trading broker adapter.
///
/// A thin driver over the shared [`tantra_exec_paper::PaperExchange`] venue;
/// all fill math lives in the exchange (T14.6, plan 0005 D2).
#[derive(Clone)]
pub struct PaperAdapter {
    venue: Arc<Mutex<PaperVenue>>,
}

impl std::fmt::Debug for PaperAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaperAdapter")
            .field("venue", &"paper-exchange")
            .finish()
    }
}

impl PaperAdapter {
    /// Creates a new [`PaperAdapter`] with the specified initial cash balance in INR.
    pub fn new(initial_cash: Decimal) -> Self {
        Self {
            venue: Arc::new(Mutex::new(PaperVenue {
                exchange: exchange_with_fees(initial_cash),
                market_prices: HashMap::new(),
                last_prices: HashMap::new(),
                orders: HashMap::new(),
            })),
        }
    }

    /// Sets or updates the market price for an instrument.
    pub fn set_market_price(&self, instrument: InstrumentId, price: Decimal) {
        venue_set_market_price(&self.venue, instrument, price);
    }

    /// Returns the current cash balance in INR.
    pub fn cash(&self) -> Decimal {
        venue_cash(&self.venue)
    }

    /// Returns the current open positions.
    pub fn positions(&self) -> HashMap<InstrumentId, Decimal> {
        venue_positions(&self.venue)
    }

    /// Returns all execution fills recorded by this adapter.
    pub fn fills(&self) -> Vec<FillReport> {
        venue_fills(&self.venue)
    }

    /// Returns all orders submitted to this adapter.
    pub fn orders(&self) -> HashMap<String, OrderRequest> {
        venue_orders(&self.venue)
    }

    /// Returns the paper exchange's lifecycle state for a broker order id,
    /// when tracked (e.g. `OrderState::Cancelled` after a successful cancel).
    pub fn order_state(&self, broker_order_id: &str) -> Option<OrderState> {
        venue_order_state(&self.venue, broker_order_id)
    }

    /// Computes current total portfolio equity (cash + mark-to-market positions).
    pub fn equity(&self) -> Decimal {
        venue_equity(&self.venue)
    }

    /// Computes portfolio equity using an explicit set of market prices.
    pub fn equity_with_prices(&self, prices: &HashMap<InstrumentId, Decimal>) -> Decimal {
        venue_equity_with_prices(&self.venue, prices)
    }
}

impl Default for PaperAdapter {
    fn default() -> Self {
        Self::new(Decimal::new(1_000_000, 0))
    }
}

impl BrokerAdapter for PaperAdapter {
    fn broker_name(&self) -> &'static str {
        "paper"
    }

    fn submit_order(&self, order: &OrderRequest, now: DateTime<Utc>) -> Result<String, String> {
        submit_to_venue(&self.venue, order, now)
    }

    fn cancel_order(&self, broker_order_id: &str, now: DateTime<Utc>) -> Result<(), String> {
        cancel_on_venue(&self.venue, broker_order_id, now)
    }
}

/// Live broker executor plus the Tokio handle used to drive it from the sync
/// [`BrokerAdapter`] boundary. Built by the composition root; holds no
/// credentials itself.
#[derive(Clone)]
struct LiveExecutor {
    executor: Arc<dyn BrokerExecutor>,
    handle: Handle,
}

/// Bridges an async executor call onto the sync adapter boundary without
/// panicking when called from inside a Tokio runtime.
fn block_on_executor<F: std::future::Future>(handle: &Handle, fut: F) -> Result<F::Output, String> {
    match Handle::try_current() {
        Err(_) => Ok(handle.block_on(fut)),
        Ok(current) => match current.runtime_flavor() {
            RuntimeFlavor::MultiThread => {
                Ok(tokio::task::block_in_place(|| handle.block_on(fut)))
            }
            _ => Err("live broker adapter cannot block a current-thread Tokio runtime; drive it from a multi-thread runtime worker or a plain thread".to_string()),
        },
    }
}

/// Concrete implementation for Zerodha (Kite Connect v3) execution adapter.
///
/// Live trading is injected as an `Arc<dyn BrokerExecutor>` via
/// [`ZerodhaKiteAdapter::with_executor`]; this crate holds no credentials.
#[derive(Clone)]
pub struct ZerodhaKiteAdapter {
    venue: Arc<Mutex<PaperVenue>>,
    /// Live executor, only present when injected by the composition root.
    /// When `None` the adapter runs in simulation/scaffold mode.
    live: Option<LiveExecutor>,
}

impl std::fmt::Debug for ZerodhaKiteAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZerodhaKiteAdapter")
            .field("venue", &"paper-exchange")
            .field("live", &self.live.as_ref().map(|_| "BrokerExecutor"))
            .finish()
    }
}

impl ZerodhaKiteAdapter {
    /// Creates a new [`ZerodhaKiteAdapter`] in simulation/scaffold mode with the given initial cash.
    pub fn new(initial_cash: Decimal) -> Self {
        Self {
            venue: Arc::new(Mutex::new(PaperVenue {
                exchange: exchange_with_fees(initial_cash),
                market_prices: HashMap::new(),
                last_prices: HashMap::new(),
                orders: HashMap::new(),
            })),
            live: None,
        }
    }

    /// Creates a live-mode adapter around an injected executor.
    ///
    /// The composition root builds the concrete executor (e.g.
    /// `ZerodhaExecutor`) and passes a handle to a multi-thread runtime it
    /// owns; this crate never constructs brokers or runtimes. The caller must
    /// keep that runtime alive for the adapter's lifetime: a handle to a shut
    /// down runtime cannot drive the executor's I/O. Request timeouts belong to
    /// the executor's HTTP client.
    pub fn with_executor(
        executor: Arc<dyn BrokerExecutor>,
        handle: Handle,
        initial_cash: Decimal,
    ) -> Self {
        let mut adapter = Self::new(initial_cash);
        adapter.live = Some(LiveExecutor { executor, handle });
        adapter
    }

    /// Sets or updates the market price for an instrument.
    pub fn set_market_price(&self, instrument: InstrumentId, price: Decimal) {
        venue_set_market_price(&self.venue, instrument, price);
    }

    /// Returns the current cash balance in INR.
    pub fn cash(&self) -> Decimal {
        venue_cash(&self.venue)
    }

    /// Returns the current open positions.
    pub fn positions(&self) -> HashMap<InstrumentId, Decimal> {
        venue_positions(&self.venue)
    }

    /// Returns all execution fills recorded by this adapter.
    pub fn fills(&self) -> Vec<FillReport> {
        venue_fills(&self.venue)
    }

    /// Returns all orders submitted to this adapter.
    pub fn orders(&self) -> HashMap<String, OrderRequest> {
        venue_orders(&self.venue)
    }

    /// Computes current total portfolio equity (cash + mark-to-market positions).
    pub fn equity(&self) -> Decimal {
        venue_equity(&self.venue)
    }

    /// Computes portfolio equity using an explicit set of market prices.
    pub fn equity_with_prices(&self, prices: &HashMap<InstrumentId, Decimal>) -> Decimal {
        venue_equity_with_prices(&self.venue, prices)
    }
}

impl Default for ZerodhaKiteAdapter {
    fn default() -> Self {
        Self::new(Decimal::new(1_000_000, 0))
    }
}

impl BrokerAdapter for ZerodhaKiteAdapter {
    fn broker_name(&self) -> &'static str {
        "zerodha"
    }

    fn submit_order(&self, order: &OrderRequest, now: DateTime<Utc>) -> Result<String, String> {
        if let Some(live) = &self.live {
            let ack = block_on_executor(&live.handle, live.executor.place_order(order.clone()))?
                .map_err(map_broker_error)?;
            return Ok(ack.order_id);
        }
        submit_to_venue(&self.venue, order, now)
    }

    fn cancel_order(&self, broker_order_id: &str, now: DateTime<Utc>) -> Result<(), String> {
        if let Some(live) = &self.live {
            return block_on_executor(&live.handle, live.executor.cancel_order(broker_order_id))?
                .map_err(map_broker_error);
        }
        cancel_on_venue(&self.venue, broker_order_id, now)
    }
}

/// Concrete implementation for Angel One (SmartAPI) execution adapter.
///
/// NOTE (GAP-0003): `tantra-exec-angelone` is a placeholder crate
/// (`tantra-exec/crates/tantra-exec-angelone`), so no real SmartAPI
/// `placeOrder`/`cancelOrder` transport exists yet. This adapter therefore
/// remains a simulation/scaffold identical in economics to [`PaperAdapter`].
/// The real SmartAPI wire transport belongs in `tantra-exec`/`tantra-adapters`
/// and will plug into this same [`BrokerAdapter`] boundary once available.
///
/// Zeroize-on-drop of the credential fields is deferred to the shared secret
/// types of `tantra-exec/docs/gaps/0007-*.md` (tantra-exec gap 0007).
#[derive(Clone)]
pub struct AngelOneSmartApiAdapter {
    api_key: Option<String>,
    client_code: Option<String>,
    jwt_token: Option<String>,
    venue: Arc<Mutex<PaperVenue>>,
}

impl std::fmt::Debug for AngelOneSmartApiAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AngelOneSmartApiAdapter")
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field(
                "client_code",
                &self.client_code.as_ref().map(|_| "[REDACTED]"),
            )
            .field("jwt_token", &self.jwt_token.as_ref().map(|_| "[REDACTED]"))
            .field("venue", &"paper-exchange")
            .finish()
    }
}

impl AngelOneSmartApiAdapter {
    /// Creates a new [`AngelOneSmartApiAdapter`] in simulation/scaffold mode with the given initial cash.
    pub fn new(initial_cash: Decimal) -> Self {
        Self {
            api_key: None,
            client_code: None,
            jwt_token: None,
            venue: Arc::new(Mutex::new(PaperVenue {
                exchange: exchange_with_fees(initial_cash),
                market_prices: HashMap::new(),
                last_prices: HashMap::new(),
                orders: HashMap::new(),
            })),
        }
    }

    /// Creates a new [`AngelOneSmartApiAdapter`] configured with SmartAPI credentials.
    pub fn with_credentials(
        api_key: impl Into<String>,
        client_code: impl Into<String>,
        jwt_token: impl Into<String>,
        initial_cash: Decimal,
    ) -> Self {
        Self {
            api_key: Some(api_key.into()),
            client_code: Some(client_code.into()),
            jwt_token: Some(jwt_token.into()),
            venue: Arc::new(Mutex::new(PaperVenue {
                exchange: exchange_with_fees(initial_cash),
                market_prices: HashMap::new(),
                last_prices: HashMap::new(),
                orders: HashMap::new(),
            })),
        }
    }

    /// Returns the configured SmartAPI key if set.
    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }

    /// Returns the configured SmartAPI client code if set.
    pub fn client_code(&self) -> Option<&str> {
        self.client_code.as_deref()
    }

    /// Returns the configured SmartAPI JWT session token if set.
    pub fn jwt_token(&self) -> Option<&str> {
        self.jwt_token.as_deref()
    }

    /// Sets or updates the market price for an instrument.
    pub fn set_market_price(&self, instrument: InstrumentId, price: Decimal) {
        venue_set_market_price(&self.venue, instrument, price);
    }

    /// Returns the current cash balance in INR.
    pub fn cash(&self) -> Decimal {
        venue_cash(&self.venue)
    }

    /// Returns the current open positions.
    pub fn positions(&self) -> HashMap<InstrumentId, Decimal> {
        venue_positions(&self.venue)
    }

    /// Returns all execution fills recorded by this adapter.
    pub fn fills(&self) -> Vec<FillReport> {
        venue_fills(&self.venue)
    }

    /// Returns all orders submitted to this adapter.
    pub fn orders(&self) -> HashMap<String, OrderRequest> {
        venue_orders(&self.venue)
    }

    /// Computes current total portfolio equity (cash + mark-to-market positions).
    pub fn equity(&self) -> Decimal {
        venue_equity(&self.venue)
    }

    /// Computes portfolio equity using an explicit set of market prices.
    pub fn equity_with_prices(&self, prices: &HashMap<InstrumentId, Decimal>) -> Decimal {
        venue_equity_with_prices(&self.venue, prices)
    }
}

impl Default for AngelOneSmartApiAdapter {
    fn default() -> Self {
        Self::new(Decimal::new(1_000_000, 0))
    }
}

impl BrokerAdapter for AngelOneSmartApiAdapter {
    fn broker_name(&self) -> &'static str {
        "angelone"
    }

    fn submit_order(&self, order: &OrderRequest, now: DateTime<Utc>) -> Result<String, String> {
        submit_to_venue(&self.venue, order, now)
    }

    fn cancel_order(&self, broker_order_id: &str, now: DateTime<Utc>) -> Result<(), String> {
        cancel_on_venue(&self.venue, broker_order_id, now)
    }
}

/// Type alias for [`ZerodhaKiteAdapter`].
pub type ZerodhaAdapter = ZerodhaKiteAdapter;

/// Type alias for [`AngelOneSmartApiAdapter`].
pub type AngelOneAdapter = AngelOneSmartApiAdapter;
