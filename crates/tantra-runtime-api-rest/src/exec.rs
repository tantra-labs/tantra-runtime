//! The embedded execution simulator behind the order routes (ADR 0016, E3-S7).
//!
//! A risk-approved order is recorded in the [`OrderLedger`] as `Submitted` and handed to
//! [`ExecGateway`]'s [`NextOpenSim`]. It fills market orders at the open of the instrument's
//! first bar in a later session, so a fill needs the market to advance: the composition root
//! (or a test) pushes bars with [`AppState::advance_market`](crate::AppState::advance_market).
//! Event time is the bar's own `timestamp`; nothing here reads the wall clock.
//!
//! Scope: market orders only. A limit order is refused (`unsupported_order_type`) and its
//! ledger row becomes `Rejected`. The simulator is long-only (it tracks its own simulated
//! holding per instrument, plus the ledger's working long exposure, so a sell may reduce a
//! working buy that has not filled yet); a sell beyond both is `Rejected` (`no_position`). No
//! real broker gateway is wired into `tantra serve` (E3-S7).
//!
//! # Rebuilt on this migration, not a rename
//!
//! This used to wrap `tantra_sim::NextOpenSim` behind a crate-local `ExecutionVenue` trait
//! extending `tantra_engine::ExecutionEngine`, dispatching `tantra_entities::ExecutionEvent`
//! (`Fill`/`Rejected`/`Cancelled` variants carrying their own instrument/side/quantity) into the
//! ledger. `tantra_engine`, `tantra_sim` and `tantra_entities` are gone from the workspace with no
//! successor (`tantra-ya-backtest::sim` only absorbed part of `tantra-ya-sim` — it has no
//! `NextOpenSim`-equivalent bar-driven market-order simulator), and `tantra_core::ExecutionEvent`
//! is a different, much leaner cross-subsystem audit event (`Submitted`/`RiskRefused`/
//! `Order(OrderEvent)`) with no `Fill`/`Rejected`/`Cancelled` variants carrying their own
//! instrument/side/quantity. There was also a second, dead implementor of the old
//! `ExecutionVenue` trait (`GatewayVenue`, bridging to an async execution-gateway port) that
//! nothing in this crate ever constructed — see the removed `gateway_venue.rs`'s history for
//! that gap, which this migration pass did not attempt to restore since it was unreachable.
//!
//! So this file is now a small, self-contained simulator built directly on `tantra_core` types
//! (`Bar`, `Price`, `Quantity`) and this crate's own ledger row (`crate::orders::Order`), with
//! no generic venue trait at all (there is only ever one simulator; a generic trait added
//! complexity for a second implementor that was never reachable).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tantra_core::rust_decimal::Decimal;
use tantra_core::{Bar, FillReport, InstrumentId, OrderSide, Price, Quantity};
use tantra_runtime_api::{ErrorCode, ErrorDetail, SharedLedger};

use crate::OrderLedger;
use crate::orders::{Order, OrderStatus, OrderType, RejectReason};

/// One order the simulator is holding, keyed by its ledger order id.
struct Working {
    instrument_id: InstrumentId,
    side: OrderSide,
    quantity: Decimal,
}

/// A simple bar-driven market-order simulator: a market order fills in full at the open of the
/// next bar seen for its instrument. Long-only: tracks its own simulated holding per instrument
/// (independent of the ledger's own signed position), and refuses a sell beyond it.
#[derive(Default)]
pub struct NextOpenSim {
    working: HashMap<String, Working>,
    holdings: HashMap<InstrumentId, Decimal>,
}

impl NextOpenSim {
    /// A simulator with nothing working and no simulated holdings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Accepts `order` as working, or immediately rejects it onto `ledger` (a non-market order,
    /// or a sell beyond the simulated holding).
    fn submit(&mut self, ledger: &OrderLedger, order: &Order) {
        if order.order_type() != OrderType::Market {
            ledger.reject(
                order.order_id(),
                RejectReason::new(
                    ErrorCode::NotImplemented,
                    "unsupported_order_type: only market orders are simulated",
                ),
            );
            return;
        }
        if order.side() == OrderSide::Sell {
            // The simulated holding plus the ledger's working long exposure (submitted buys
            // that may still fill) is what a sell may reduce; a close of a working position
            // therefore passes the long-only gate, while a naked short still cannot.
            let simulated = self
                .holdings
                .get(order.instrument_id())
                .copied()
                .unwrap_or(Decimal::ZERO);
            let working_long: Decimal = ledger
                .list()
                .iter()
                .filter(|o| {
                    o.instrument_id() == order.instrument_id() && o.side() == OrderSide::Buy
                })
                .map(crate::orders::exposure)
                .sum();
            if simulated + working_long < order.quantity() {
                ledger.reject(
                    order.order_id(),
                    RejectReason::new(
                        ErrorCode::Conflict,
                        "no_position: the simulator is long-only",
                    ),
                );
                return;
            }
        }
        self.working.insert(
            order.order_id().to_owned(),
            Working {
                instrument_id: order.instrument_id().clone(),
                side: order.side(),
                quantity: order.quantity(),
            },
        );
    }

    /// Fills every working order on `bar.instrument` in full, at `bar.open`.
    fn on_bar(&mut self, ledger: &OrderLedger, shared: &SharedLedger, bar: &Bar) {
        let matching: Vec<String> = self
            .working
            .iter()
            .filter(|(_, w)| w.instrument_id == bar.instrument)
            .map(|(id, _)| id.clone())
            .collect();
        for order_id in matching {
            let Some(w) = self.working.remove(&order_id) else {
                continue;
            };
            let instrument = w.instrument_id.clone();
            let signed = match w.side {
                OrderSide::Buy => w.quantity,
                OrderSide::Sell => -w.quantity,
            };
            let entry = self
                .holdings
                .entry(w.instrument_id)
                .or_insert(Decimal::ZERO);
            *entry += signed;
            ledger.apply_fill(
                &order_id,
                w.quantity,
                bar.open.0,
                w.quantity,
                bar.timestamp,
                OrderStatus::Filled,
            );
            // The fill also lands in the shared ledger repositories (docs/gaps/0006 design
            // decision): FIFO lot matching + realized P&L + cash for the chat portfolio/tax
            // tools, and the order row's fill state so `orders` stays accurate.
            let fill = FillReport::new(
                format!("{order_id}-fill"),
                &order_id,
                uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, order_id.as_bytes()),
                instrument,
                w.side,
                Price::new(bar.open.0),
                Quantity::new(w.quantity),
                crate::orders::REST_ALGO_ID,
                bar.timestamp,
                crate::orders::REST_ALGO_ID,
            );
            shared.record_fill(&fill);
            shared.update_order_fill(
                &order_id,
                Quantity::new(w.quantity),
                Price::new(bar.open.0),
                tantra_core::OrderStatus::Filled,
                bar.timestamp,
            );
        }
    }

    /// Drops `order_id` from the working set, if present.
    fn cancel(&mut self, order_id: &str) {
        self.working.remove(order_id);
    }
}

/// Shared handle on the embedded simulator.
#[derive(Clone)]
pub(crate) struct ExecGateway {
    sim: Arc<Mutex<NextOpenSim>>,
}

impl ExecGateway {
    /// Wraps `sim` behind a shared, lockable handle.
    pub(crate) fn new(sim: NextOpenSim) -> Self {
        Self {
            sim: Arc::new(Mutex::new(sim)),
        }
    }

    /// A fresh simulator with nothing working and no simulated holdings.
    pub(crate) fn default_sim() -> Self {
        Self::new(NextOpenSim::new())
    }

    /// Hands `order` to the simulator; a rejection (unsupported type, or `no_position`) is
    /// applied to `ledger` immediately.
    pub(crate) fn submit(&self, ledger: &OrderLedger, order: Order) {
        let mut sim = self.sim.lock().unwrap_or_else(PoisonError::into_inner);
        sim.submit(ledger, &order);
    }

    /// Feeds `bars` (ascending event time) to the simulator and applies any resulting fills.
    pub(crate) fn advance(
        &self,
        ledger: &OrderLedger,
        shared: &SharedLedger,
        bars: &[Bar],
    ) -> Result<(), ErrorDetail> {
        let mut sim = self.sim.lock().unwrap_or_else(PoisonError::into_inner);
        for bar in bars {
            sim.on_bar(ledger, shared, bar);
        }
        Ok(())
    }

    /// Withdraws a working order from the simulator (the ledger row's own cancellation is the
    /// caller's job; see `orders::delete_order`).
    pub(crate) fn cancel(&self, _ledger: &OrderLedger, order_id: &str) {
        let mut sim = self.sim.lock().unwrap_or_else(PoisonError::into_inner);
        sim.cancel(order_id);
    }
}
