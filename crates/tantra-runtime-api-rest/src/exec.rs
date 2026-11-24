//! The execution gateway behind the order routes: a thin adapter over the shared
//! `tantra-exec-paper` exchange (plan 0005 D1, T14.5), partitioned per authenticated caller.
//!
//! A risk-approved order is recorded in the caller's [`OrderLedger`] as `Submitted` and handed to
//! their [`ExecGateway`], which converts the ledger row into a canonical [`OrderRequest`] and submits
//! it to that caller's [`PaperExchange`]. The exchange owns the order lifecycle — resting limits,
//! armed stops, fills, cancellations, rejections — and is the single source of fills. The
//! gateway drains the exchange's fill and lifecycle-event streams back into the caller's ledger
//! (and the shared ledger repositories, docs/gaps/0006) on every submit and market advance, so the
//! ledger row follows whatever the exchange did: a marketable order fills immediately, a
//! market order with no contra liquidity is refused (`no market price`), a limit rests as
//! `Accepted` until a bar crosses it, and a stop arms until its trigger is breached.
//!
//! Event time is the ledger row's `created_at` and the bars' `timestamp` — nothing here reads
//! the wall clock except the cancel path and portfolio snapshot timestamps (this crate is the
//! I/O edge; the no-clock rule binds the zero-I/O domain crates, not this adapter).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use serde_json::json;
use tantra_core::rust_decimal::Decimal;
use tantra_core::{
    AccountId, Bar, OrderRequest, OrderSide, OrderType as CoreOrderType, Price, Quantity,
    TimeInForce,
};
use tantra_exec_paper::{ExchangeError, ExchangeEvent, FillCostFn, OrderState, PaperExchange};
use tantra_market_finance::india::costs::NseCashEquitySchedule;
use tantra_market_finance::{CostSchedule, MarketSegment};
use tantra_runtime_api::{ErrorCode, ErrorDetail, SharedLedger};

use crate::OrderLedger;
use crate::orders::{Order, OrderStatus, OrderType, REST_ALGO_ID, RejectReason, to_core_status};

/// Maximum number of per-caller paper accounts held in memory (matching `MAX_JOURNAL_PARTITIONS`).
pub(crate) const MAX_ACCOUNT_PARTITIONS: usize = 4096;

/// Maximum number of market bars kept in replay history for lazily-created caller exchanges.
const MAX_BAR_HISTORY: usize = 10_000;

/// The real NSE cash-equity delivery schedule as the exchange's [`FillCostFn`]: per-fill fees
/// are the schedule's total across its charge lines, converted from the fee table's `f64` to
/// `Decimal` at this boundary (mirroring `dispatch.rs`'s `f64` fee path — the exchange itself
/// stays exact).
#[derive(Clone)]
struct NseCashEquityFillCost(NseCashEquitySchedule);

impl FillCostFn for NseCashEquityFillCost {
    fn compute(&self, side: OrderSide, notional: Decimal) -> Decimal {
        let costs = self.0.compute_costs(
            &MarketSegment::from("equity"),
            side,
            notional.to_f64().unwrap_or(0.0),
        );
        Decimal::from_f64_retain(costs.total()).unwrap_or_default()
    }
}

/// One caller's isolated paper trading account: its own write ledger and paper exchange.
#[derive(Clone)]
pub(crate) struct PaperAccount {
    pub(crate) owner: String,
    pub(crate) orders: OrderLedger,
    pub(crate) exec: ExecGateway,
}

impl PaperAccount {
    /// Canonical account identifier used when mirroring this caller's fills into [`SharedLedger`].
    ///
    /// Keeps the default unauthenticated/`DEFAULT_USER_ID` partition on `"rest-api"` for chat
    /// portfolio tools and isolates named callers onto `"rest-api:<owner>"`.
    pub(crate) fn shared_account_id(&self) -> AccountId {
        if self.owner == crate::profile_store::DEFAULT_USER_ID {
            AccountId::from(REST_ALGO_ID)
        } else {
            AccountId::from(format!("{REST_ALGO_ID}:{}", self.owner))
        }
    }
}

/// Per-caller paper trading accounts (`(OrderLedger, ExecGateway)`), created lazily and bounded
/// by [`MAX_ACCOUNT_PARTITIONS`], with market bar fan-out and historical bar replay so newly
/// active callers share live market depth without sharing cash, orders, or positions.
#[derive(Clone)]
pub(crate) struct CallerPaperAccounts {
    default_owner: String,
    inner: Arc<RwLock<CallerPaperAccountsInner>>,
}

struct CallerPaperAccountsInner {
    template_cash: Option<Decimal>,
    accounts: HashMap<String, PaperAccount>,
    claimed_ids: HashMap<String, String>,
    bar_history: VecDeque<Bar>,
}

impl CallerPaperAccounts {
    /// Creates the account table seeded with `default_owner` backed by `orders` and `exec`.
    pub(crate) fn seeded(default_owner: &str, orders: OrderLedger, exec: ExecGateway) -> Self {
        Self::seeded_inner(default_owner, orders, exec, None)
    }

    /// Creates the account table seeded with `default_owner` and custom `initial_cash` for
    /// lazily created caller exchanges (e.g. `AppState::with_execution_sim`).
    pub(crate) fn seeded_with_cash(
        default_owner: &str,
        orders: OrderLedger,
        exec: ExecGateway,
        initial_cash: Decimal,
    ) -> Self {
        Self::seeded_inner(default_owner, orders, exec, Some(initial_cash))
    }

    fn seeded_inner(
        default_owner: &str,
        orders: OrderLedger,
        exec: ExecGateway,
        template_cash: Option<Decimal>,
    ) -> Self {
        let mut accounts = HashMap::new();
        accounts.insert(
            default_owner.to_string(),
            PaperAccount {
                owner: default_owner.to_string(),
                orders,
                exec,
            },
        );
        Self {
            default_owner: default_owner.to_string(),
            inner: Arc::new(RwLock::new(CallerPaperAccountsInner {
                template_cash,
                accounts,
                claimed_ids: HashMap::new(),
                bar_history: VecDeque::new(),
            })),
        }
    }

    /// Returns `caller`'s paper account, lazily creating one seeded with current market bars
    /// when first accessed.
    pub(crate) fn for_caller(&self, caller: &str) -> Result<PaperAccount, ErrorDetail> {
        {
            let guard = self.inner.read().unwrap_or_else(PoisonError::into_inner);
            if let Some(acct) = guard.accounts.get(caller) {
                return Ok(acct.clone());
            }
        }
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(acct) = guard.accounts.get(caller) {
            return Ok(acct.clone());
        }
        if guard.accounts.len() >= MAX_ACCOUNT_PARTITIONS {
            return Err(ErrorDetail::new(
                ErrorCode::ServiceUnavailable,
                "too many paper-account partitions",
            )
            .with_context(json!({ "reason": "paper_account_partitions_exhausted" })));
        }
        let exec = match guard.template_cash {
            Some(cash) => ExecGateway::with_exchange(PaperExchange::new(cash)),
            None => ExecGateway::default_sim(),
        };
        exec.seed_bars(guard.bar_history.iter());
        let acct = PaperAccount {
            owner: caller.to_string(),
            orders: OrderLedger::new(),
            exec,
        };
        guard.accounts.insert(caller.to_string(), acct.clone());
        Ok(acct)
    }

    /// Ensures `order_id` has not already been claimed by a different caller.
    pub(crate) fn check_client_order_id(
        &self,
        order_id: &str,
        owner: &str,
    ) -> Result<(), ErrorDetail> {
        let guard = self.inner.read().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing_owner) = guard.claimed_ids.get(order_id) {
            if existing_owner != owner {
                return Err(conflict_client_order_id());
            }
        } else if owner != self.default_owner
            && guard
                .accounts
                .get(&self.default_owner)
                .is_some_and(|a| a.orders.get(order_id).is_some())
        {
            return Err(conflict_client_order_id());
        }
        Ok(())
    }

    /// Atomically claims `order_id` for `owner`, rejecting if another caller claimed it first.
    pub(crate) fn claim_client_order_id(
        &self,
        order_id: &str,
        owner: &str,
    ) -> Result<(), ErrorDetail> {
        let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing_owner) = guard.claimed_ids.get(order_id) {
            if existing_owner != owner {
                return Err(conflict_client_order_id());
            }
            return Ok(());
        }
        if owner != self.default_owner
            && guard
                .accounts
                .get(&self.default_owner)
                .is_some_and(|a| a.orders.get(order_id).is_some())
        {
            return Err(conflict_client_order_id());
        }
        guard
            .claimed_ids
            .insert(order_id.to_string(), owner.to_string());
        Ok(())
    }

    /// Feeds `bars` to every active caller exchange and appends them to the bounded replay
    /// history for callers activated later.
    pub(crate) fn advance_all(
        &self,
        shared: &SharedLedger,
        bars: &[Bar],
    ) -> Result<(), ErrorDetail> {
        let accounts: Vec<PaperAccount> = {
            let mut guard = self.inner.write().unwrap_or_else(PoisonError::into_inner);
            for bar in bars {
                guard.bar_history.push_back(bar.clone());
                while guard.bar_history.len() > MAX_BAR_HISTORY {
                    guard.bar_history.pop_front();
                }
            }
            guard.accounts.values().cloned().collect()
        };
        for acct in accounts {
            acct.exec
                .advance(&acct.orders, &acct.shared_account_id(), shared, bars)?;
        }
        Ok(())
    }

    /// Squares off open positions across all materialized caller accounts.
    pub(crate) fn mis_square_off_all(&self, shared: &SharedLedger, at: DateTime<Utc>) -> usize {
        let accounts: Vec<PaperAccount> = {
            let guard = self.inner.read().unwrap_or_else(PoisonError::into_inner);
            guard.accounts.values().cloned().collect()
        };
        accounts
            .into_iter()
            .map(|acct| {
                acct.exec
                    .mis_square_off(&acct.orders, &acct.shared_account_id(), shared, at)
            })
            .sum()
    }

    /// Total fills recorded across all materialized caller accounts.
    pub(crate) fn total_fills(&self) -> usize {
        let accounts: Vec<PaperAccount> = {
            let guard = self.inner.read().unwrap_or_else(PoisonError::into_inner);
            guard.accounts.values().cloned().collect()
        };
        accounts.into_iter().map(|a| a.exec.fill_count()).sum()
    }

    /// Total orders recorded across all materialized caller accounts.
    pub(crate) fn total_orders(&self) -> usize {
        let accounts: Vec<PaperAccount> = {
            let guard = self.inner.read().unwrap_or_else(PoisonError::into_inner);
            guard.accounts.values().cloned().collect()
        };
        accounts.into_iter().map(|a| a.orders.list().len()).sum()
    }
}

fn conflict_client_order_id() -> ErrorDetail {
    ErrorDetail::new(ErrorCode::Conflict, "client_order_id is already in use")
        .with_context(json!({ "reason": "client_order_id_in_use" }))
}

/// Shared handle on one caller's paper exchange.
#[derive(Clone)]
pub(crate) struct ExecGateway {
    exchange: Arc<Mutex<PaperExchange>>,
    /// Ledger order id -> broker order id (`PAPER-0001`-style) as returned by the exchange.
    ledger_to_broker: Arc<Mutex<HashMap<String, String>>>,
    /// Cumulative filled quantity per ledger order id, so partial fills report a running
    /// cumulative quantity to the ledger.
    cum_qty: Arc<Mutex<HashMap<String, Decimal>>>,
    /// How many of the exchange's fills and events have already been drained into the ledger.
    /// The exchange appends monotonically to both streams, so the drained count is just the
    /// length seen at the last drain.
    last_seen_fills: Arc<Mutex<usize>>,
    last_seen_events: Arc<Mutex<usize>>,
    /// Broadcast of the exchange's lifecycle events, fills, depth updates and account
    /// snapshots as wire JSON, fanned out to `/api/v2/stream` clients (plan 0005 D3, T14.7). A full
    /// channel drops the oldest payload for lagged receivers; `send` never blocks.
    events_tx: tokio::sync::broadcast::Sender<serde_json::Value>,
}

impl ExecGateway {
    /// Wraps `exchange` behind a shared, lockable handle.
    pub(crate) fn new(exchange: PaperExchange) -> Self {
        Self {
            exchange: Arc::new(Mutex::new(exchange)),
            ledger_to_broker: Arc::default(),
            cum_qty: Arc::default(),
            last_seen_fills: Arc::default(),
            last_seen_events: Arc::default(),
            events_tx: tokio::sync::broadcast::channel(256).0,
        }
    }

    /// A fresh exchange with a ₹10,00,000 account and the real NSE cash-equity delivery fee
    /// schedule charging per fill.
    pub(crate) fn default_sim() -> Self {
        Self::new(PaperExchange::new(dec!(1_000_000)).with_cost_fn(Box::new(
            NseCashEquityFillCost(NseCashEquitySchedule::delivery()),
        )))
    }

    /// A gateway over an explicitly-built exchange (tests inject `ZeroFillCost` for exact cash
    /// this way).
    pub(crate) fn with_exchange(exchange: PaperExchange) -> Self {
        Self::new(exchange)
    }

    /// Replays historical bars into a brand-new exchange so it matches live market depth/prices
    /// without emitting historical stream frames.
    fn seed_bars<'a>(&self, bars: impl IntoIterator<Item = &'a Bar>) {
        let mut exchange = self.exchange.lock().unwrap_or_else(PoisonError::into_inner);
        for bar in bars {
            exchange.feed_bar(bar);
        }
    }

    /// A fresh `/api/v2/stream` event stream subscription, starting at the next published payload.
    pub(crate) fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<serde_json::Value> {
        self.events_tx.subscribe()
    }

    /// Publishes one wire payload to every `/api/v2/stream` subscriber. A full channel drops the
    /// oldest payload for lagged receivers and returns `Err`; `send` never blocks.
    fn publish(&self, payload: serde_json::Value) {
        let _ = self.events_tx.send(payload);
    }

    /// Converts the ledger row into a canonical [`OrderRequest`] and submits it to the
    /// exchange, applying any immediate fill or rejection to `ledger` (and the shared ledger).
    pub(crate) fn submit(
        &self,
        ledger: &OrderLedger,
        account_id: &AccountId,
        shared: &SharedLedger,
        order: Order,
    ) {
        let ledger_id = order.order_id().to_owned();
        let instrument = order.instrument_id().clone();
        let side = order.side();
        let order_type = order.order_type();
        let quantity = order.quantity();
        let tif = order.time_in_force();
        let created_at = order.created_at();
        let client_order_id = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, ledger_id.as_bytes());
        let core_type = match order_type {
            OrderType::Market => CoreOrderType::Market,
            // The local `Limit` carries no embedded price (the row tracks it separately).
            OrderType::Limit => {
                CoreOrderType::Limit(order.price().map(Price::new).unwrap_or(Price::ZERO))
            }
            OrderType::StopMarket { trigger } => CoreOrderType::StopLoss(Price::new(trigger)),
            OrderType::StopLimit { trigger, limit } => CoreOrderType::StopLimit {
                trigger: Price::new(trigger),
                limit: Price::new(limit),
            },
        };
        let mut request = OrderRequest::with_id(
            client_order_id,
            instrument,
            side,
            core_type,
            Quantity::new(quantity),
            REST_ALGO_ID,
        );
        // The canonical TIF is `Day | Ioc | Gtc`; the row's own `Day`/`Gtc` both read as a Day
        // order on the exchange (GTD expiry is not modeled by the wire yet).
        request.tif = match tif {
            TimeInForce::Ioc => TimeInForce::Ioc,
            TimeInForce::Day | TimeInForce::Gtc => TimeInForce::Day,
        };
        let result = self
            .exchange
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .submit_order(request, created_at);
        match result {
            Ok(broker_id) => {
                self.ledger_to_broker
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(ledger_id.clone(), broker_id.clone());
                // A marketable order fills on submit; a resting one (or an armed stop) must at
                // least read as `Accepted` in the ledger.
                self.drain_fills(ledger, account_id, shared);
                let resting = self
                    .exchange
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .order(&broker_id)
                    .is_some_and(|w| {
                        matches!(
                            w.state,
                            OrderState::New | OrderState::Working | OrderState::StopTriggered
                        )
                    });
                if resting {
                    ledger.set_status(&ledger_id, OrderStatus::Accepted);
                }
            }
            Err(ExchangeError::NoMarketPrice(instrument)) => {
                ledger.reject(
                    &ledger_id,
                    RejectReason::new(
                        ErrorCode::Conflict,
                        format!("no market price available for market order on {instrument}"),
                    ),
                );
            }
            Err(e) => {
                ledger.reject(
                    &ledger_id,
                    RejectReason::new(ErrorCode::NotImplemented, e.to_string()),
                );
            }
        }
    }

    /// Feeds `bars` (ascending event time) to the exchange and drains any fills/events into the
    /// ledger and the shared ledger repositories.
    pub(crate) fn advance(
        &self,
        ledger: &OrderLedger,
        account_id: &AccountId,
        shared: &SharedLedger,
        bars: &[Bar],
    ) -> Result<(), ErrorDetail> {
        let mut depth_updates = Vec::new();
        {
            let mut exchange = self.exchange.lock().unwrap_or_else(PoisonError::into_inner);
            for bar in bars {
                exchange.feed_bar(bar);
                // Capture the bar's resulting spread inside the lock and publish outside it.
                let (bid, ask) = exchange.depth(&bar.instrument).unwrap_or((None, None));
                depth_updates.push((bar.instrument.clone(), bid, ask, bar.timestamp));
            }
        }
        for (instrument, bid, ask, timestamp) in depth_updates {
            self.publish(serde_json::json!({
                "type": "DepthChanged",
                "instrument": instrument,
                "bid": bid.map(|price| price.0.to_string()),
                "ask": ask.map(|price| price.0.to_string()),
                "timestamp": timestamp.to_rfc3339(),
            }));
        }
        self.drain_fills(ledger, account_id, shared);
        Ok(())
    }

    /// Withdraws a working order from the exchange (the ledger row's own cancellation is the
    /// caller's job; see `orders::delete_order`).
    pub(crate) fn cancel(&self, ledger: &OrderLedger, order_id: &str) {
        let Some(broker_id) = self
            .ledger_to_broker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(order_id)
            .cloned()
        else {
            return;
        };
        let now = ledger
            .get(order_id)
            .map(|o| o.created_at())
            .unwrap_or_else(Utc::now);
        let result = self
            .exchange
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancel_order(&broker_id, now);
        match result {
            Ok(()) => {
                // The exchange's own `Cancelled` event stays undrained (this path takes only
                // `ledger`, not `shared`), so notify `/api/v2/stream` subscribers directly.
                self.publish(serde_json::json!({
                    "type": "Cancelled",
                    "broker_order_id": broker_id,
                    "timestamp": now.to_rfc3339(),
                }));
            }
            // The ledger row is already terminal (the caller cancelled it); a terminal exchange
            // order or an unknown id is nothing to do.
            Err(ExchangeError::OrderNotFound(_)) | Err(ExchangeError::Terminal(_)) => {}
            Err(e) => {
                tracing::warn!(order_id, error = %e, "paper exchange cancel failed");
            }
        }
        self.ledger_to_broker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(order_id);
    }

    /// Total fills recorded by the exchange, drained or not (the `paper serve` daemon's exit
    /// report, plan 0005 D5).
    pub(crate) fn fill_count(&self) -> usize {
        self.exchange
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .fills()
            .len()
    }

    /// Squares off every non-zero exchange position with an opposite-side market order (the
    /// `paper serve` MIS close, plan 0005 D5).
    ///
    /// Submits directly to the exchange — the daemon's own close path, not the REST risk gate
    /// — then drains fills/events so the ledger and `/api/v2/stream` subscribers see the closes
    /// exactly as any other fill. Returns the number of close orders submitted.
    pub(crate) fn mis_square_off(
        &self,
        ledger: &OrderLedger,
        account_id: &AccountId,
        shared: &SharedLedger,
        at: DateTime<Utc>,
    ) -> usize {
        let positions: Vec<(tantra_core::InstrumentId, Decimal)> = {
            let exchange = self.exchange.lock().unwrap_or_else(PoisonError::into_inner);
            exchange
                .positions()
                .iter()
                .map(|(id, qty)| (id.clone(), *qty))
                .collect()
        };
        let mut submitted = 0;
        for (instrument, qty) in positions {
            if qty.is_zero() {
                continue;
            }
            let side = if qty.is_sign_negative() {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            // Deterministic client order id (no wall clock, no RNG): a v5 hash of the
            // instrument + square-off time, so replaying the same tape mints the same closes
            // byte-for-byte (plan 0005 D5 keeps the daemon deterministic).
            let key = format!("mis-close-{instrument}-{at}");
            let request = OrderRequest::with_id(
                uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, key.as_bytes()),
                instrument,
                side,
                CoreOrderType::Market,
                Quantity::new(qty.abs()),
                REST_ALGO_ID,
            );
            let result = self
                .exchange
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .submit_order(request, at);
            if result.is_ok() {
                submitted += 1;
            }
        }
        self.drain_fills(ledger, account_id, shared);
        submitted
    }

    /// Drains the exchange's new fills and lifecycle events (since the last drain) into the
    /// ledger and, for fills, the shared ledger repositories. Called on submit (immediate
    /// fills of marketable orders) and on advance (bar-driven fills).
    fn drain_fills(&self, ledger: &OrderLedger, account_id: &AccountId, shared: &SharedLedger) {
        let exchange = self.exchange.lock().unwrap_or_else(PoisonError::into_inner);
        let mapping = self
            .ledger_to_broker
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // The exchange streams fills/events keyed by broker order id; the ledger is keyed by
        // the REST id, so look the other way.
        let broker_to_ledger: HashMap<&str, &str> = mapping
            .iter()
            .map(|(ledger_id, broker_id)| (broker_id.as_str(), ledger_id.as_str()))
            .collect();
        let mut cum = self.cum_qty.lock().unwrap_or_else(PoisonError::into_inner);
        let mut fills_cursor = self
            .last_seen_fills
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut events_cursor = self
            .last_seen_events
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        let fills = exchange.fills();
        for fill in &fills[*fills_cursor..] {
            let Some(ledger_id) = broker_to_ledger.get(fill.order_id.as_str()) else {
                continue;
            };
            let ledger_id = ledger_id.to_string();
            let entry = cum.entry(ledger_id.clone()).or_default();
            *entry += fill.quantity.0;
            let cumulative = *entry;
            let state = exchange.order(&fill.order_id).map(|w| w.state);
            let status = match state {
                Some(OrderState::Filled) => OrderStatus::Filled,
                _ => OrderStatus::PartiallyFilled,
            };
            ledger.apply_fill(
                &ledger_id,
                fill.quantity.0,
                fill.price.0,
                cumulative,
                fill.timestamp,
                status,
            );
            // Mirror the fill into the shared ledger repositories under the caller's REST
            // account and ledger order id (the exchange's broker id would not match the order
            // row).
            let mut mirrored = fill.clone();
            mirrored.order_id = ledger_id.clone();
            mirrored.account_id = Some(account_id.to_string());
            mirrored.algo_id = REST_ALGO_ID.to_string();
            shared.record_fill(&mirrored);
            shared.update_order_fill(
                &ledger_id,
                fill.quantity,
                fill.price,
                to_core_status(status),
                fill.timestamp,
            );
        }
        *fills_cursor = fills.len();

        // The account book follows every drain: publish fresh snapshots so `/api/v2/stream` clients
        // can track positions and equity without polling (plan 0005 D3, T14.7).
        let snapshot_ts = Utc::now();
        self.publish(serde_json::json!({
            "type": "PositionSnapshot",
            "positions": exchange
                .positions()
                .iter()
                .map(|(id, qty)| (id.symbol.clone(), serde_json::Value::String(qty.to_string())))
                .collect::<serde_json::Map<String, serde_json::Value>>(),
            "timestamp": snapshot_ts.to_rfc3339(),
        }));
        self.publish(serde_json::json!({
            "type": "EquitySnapshot",
            "cash": exchange.cash().to_string(),
            "equity": exchange.equity().to_string(),
            "timestamp": snapshot_ts.to_rfc3339(),
        }));

        let events = exchange.events();
        let new_events = &events[*events_cursor..];
        for event in new_events {
            match event {
                ExchangeEvent::Cancelled {
                    broker_order_id, ..
                } => {
                    if let Some(ledger_id) = broker_to_ledger.get(broker_order_id.as_str()) {
                        // The ledger row is already `Cancelled` when this came from
                        // `DELETE /orders/{id}`; set_status is a no-op on a terminal row.
                        ledger.set_status(ledger_id, OrderStatus::Cancelled);
                    }
                }
                ExchangeEvent::Rejected {
                    broker_order_id,
                    reason,
                    ..
                } => {
                    if let Some(ledger_id) = broker_to_ledger.get(broker_order_id.as_str()) {
                        ledger.reject(
                            ledger_id,
                            RejectReason::new(ErrorCode::Conflict, reason.clone()),
                        );
                    }
                }
                _ => {}
            }
        }
        // Fan every new lifecycle event out to `/api/v2/stream` subscribers as wire JSON, exactly as
        // the exchange reported it.
        for event in new_events {
            self.publish(exchange_event_to_json(event));
        }
        *events_cursor = events.len();
    }
}

/// Serializes one exchange lifecycle event to the canonical `/api/v2/stream` wire shape (plan 0005
/// D3, T14.7): a `type` tag plus the event's fields, decimals as strings. A fill payload is the
/// canonical `tantra_core` `FillReport` DTO as-is.
fn exchange_event_to_json(event: &ExchangeEvent) -> serde_json::Value {
    match event {
        ExchangeEvent::OrderAccepted {
            broker_order_id,
            instrument,
            side,
            quantity,
            timestamp,
        } => serde_json::json!({
            "type": "OrderAccepted",
            "broker_order_id": broker_order_id,
            "instrument": instrument,
            "side": live_side(*side),
            "quantity": quantity.0.to_string(),
            "timestamp": timestamp.to_rfc3339(),
        }),
        ExchangeEvent::Fill { fill } => serde_json::json!({
            "type": "Fill",
            "fill": fill,
        }),
        ExchangeEvent::PartialFill { fill } => serde_json::json!({
            "type": "PartialFill",
            "fill": fill,
        }),
        ExchangeEvent::Cancelled {
            broker_order_id,
            timestamp,
        } => serde_json::json!({
            "type": "Cancelled",
            "broker_order_id": broker_order_id,
            "timestamp": timestamp.to_rfc3339(),
        }),
        ExchangeEvent::Rejected {
            broker_order_id,
            reason,
            timestamp,
        } => serde_json::json!({
            "type": "Rejected",
            "broker_order_id": broker_order_id,
            "reason": reason,
            "timestamp": timestamp.to_rfc3339(),
        }),
        ExchangeEvent::StopTriggered {
            broker_order_id,
            timestamp,
        } => serde_json::json!({
            "type": "StopTriggered",
            "broker_order_id": broker_order_id,
            "timestamp": timestamp.to_rfc3339(),
        }),
    }
}

/// The `/api/v2/stream` wire spelling of an order side: lowercase `buy`/`sell` (the canonical core
/// [`OrderSide`] serde spells it `BUY`/`SELL`).
fn live_side(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}
