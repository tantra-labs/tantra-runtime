//! Synthetic one-cancels-other (OCO) bracket emulation over exec's
//! [`BracketTracker`].
//!
//! Until a broker/OMS natively hosts bracket orders, the runtime drives a
//! [`TradePlan`] as a *synthetic bracket*: the entry order is submitted, and on
//! its fill a resting stop child plus one limit child per target leg are armed
//! as independent broker orders. The driver (`SyntheticOco`) is the OCO
//! coordinator: it decides stop-vs-target fills conservatively (stop first),
//! cancels the resting stop sibling when a target leg fills, cancels all
//! remaining target siblings when the stop triggers, and trails the stop
//! through cancel+resubmit with a deterministic client order id.
//!
//! # Honesty
//!
//! This is a *synthetic* bracket: the real wire transport still cannot express
//! a native BO/CO bracket, so the emulation exists until `tantra-exec` gap 0003
//! / `tantra-ledger` gap 0012 native bracket support lands (see
//! `tantra-exec/docs/gaps/0003-bracket-order-slt-sl-execution.md` and the
//! closed record in `tantra-exec/docs/gaps/README.md`). Every fill decision is
//! made here deterministically, `Decimal`-only, with timestamps injected — zero
//! I/O, zero clock reads — and every lifecycle transition journals a
//! [`BracketEvent`] through the underlying pure state machine.
//!
//! # Determinism
//!
//! The bracket id (`BRK-<seed>`) keys every leg's *client* order id: the entry
//! leg, the `#stop` child, and the `#target{i}` children are derived with a
//! SHA-256 v4-style UUID so a replay with the same seed produces byte-identical
//! broker order sequences (the paper venue mints sequential broker ids for the
//! same submission sequence).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use tantra_core::uuid::Uuid;
use tantra_core::{OrderRequest, OrderSide, OrderType, PlannedTarget, Price, Quantity, TradePlan};
use tantra_exec_core::bracket::{
    BracketError, BracketEvent, BracketOrderSpec, BracketTracker, stop_client_order_id,
};
use tantra_ledger_oms::TradeSnapshot;

use crate::dispatch::BrokerAdapter;

/// Default SEBI algo id stamped on entry orders built from a plan. The
/// [`crate::dispatch::Dispatcher::dispatch_bracket`] path overrides this with
/// the dispatcher's registered algo id before the pre-trade gate.
pub const SYNTHETIC_OCO_ALGO_ID: &str = "SYNTHETIC_OCO";

/// A broker-visible action the driver wants the OCO emulation to reflect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OcoAction {
    /// The resting stop triggered and the runner filled.
    StopTriggered {
        /// Price at which the stop filled.
        price: Price,
        /// Quantity filled by the stop.
        qty: Quantity,
    },
    /// A target ladder leg filled (partial exit).
    TargetFilled {
        /// Limit fill price of the leg.
        price: Price,
        /// Quantity exited at this leg.
        qty: Quantity,
        /// Human-readable leg reason (`"target 1 at 1R"`, ...).
        reason: String,
    },
}

/// Deterministic client correlation id for a bracket leg.
///
/// Mirrors exec's `{bracket_id}#stop` string convention (see
/// [`stop_client_order_id`]) but fits the `Uuid` slot of
/// [`OrderRequest::client_order_id`]: `sha256("{bracket_id}#{leg}")` truncated
/// to a v4-style 128-bit UUID. Identical seeds/plans produce byte-identical
/// client ids, so journal replays and seeded backtests reproduce the exact
/// order stream.
pub fn child_client_id(bracket_id: &str, leg: &str) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(bracket_id.as_bytes());
    hasher.update(b"#");
    hasher.update(leg.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

/// The entry leg's deterministic client order id for a bracket.
pub fn entry_client_id(bracket_id: &str) -> Uuid {
    child_client_id(bracket_id, "entry")
}

/// Build the entry [`OrderRequest`] of a bracket from a completed plan.
///
/// The order type is pinned deterministically: every [`TradePlan`] carries a
/// conservative entry reference price, so the entry rests as a
/// [`OrderType::Limit`] at `plan.entry`. A plan whose author wants a market
/// entry must override `spec.entry.order_type` to [`OrderType::Market`] after
/// [`plan_to_spec`] — the plan itself does not record the entry rule, so the
/// translation is deliberately conservative (never chases price).
///
/// Side and quantity come straight from the plan; the SEBI algo id defaults to
/// [`SYNTHETIC_OCO_ALGO_ID`] (the dispatch path re-stamps the dispatcher's
/// registered algo id).
pub fn plan_to_entry_request(plan: &TradePlan, client_order_id: Uuid) -> OrderRequest {
    OrderRequest::with_id(
        client_order_id,
        plan.instrument.clone(),
        plan.side,
        OrderType::Limit(plan.entry),
        plan.quantity,
        SYNTHETIC_OCO_ALGO_ID,
    )
}

/// Deterministic client order id derived from a plan.
///
/// SHA-256 over the plan's canonical wire form, truncated to a v4-style UUID —
/// identical plans produce identical entry client ids, so a replay is
/// byte-identical with no externally supplied seed.
pub fn plan_client_order_id(plan: &TradePlan) -> Uuid {
    let bytes = serde_json::to_vec(plan).unwrap_or_default();
    let digest = Sha256::digest(&bytes);
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(out)
}

/// Translate a completed [`TradePlan`] into a [`BracketOrderSpec`].
///
/// The entry order is built via [`plan_to_entry_request`] with the plan-derived
/// deterministic client order id ([`plan_client_order_id`]), the initial stop
/// and target ladder pass through, and the trailing rule is lifted from
/// `plan.trail` (`TrailRule::Off` maps to `None`).
///
/// # Errors
/// Returns a `String` when the spec fails the bracket contract (a stop on the
/// wrong side of the entry price, or a target ladder exceeding the entry
/// quantity).
pub fn plan_to_spec(plan: &TradePlan) -> Result<BracketOrderSpec, String> {
    plan_to_spec_with_id(plan, plan_client_order_id(plan))
}

/// [`plan_to_spec`] with an explicit, caller-supplied entry client order id.
///
/// Use this when the bracket is keyed by an external seed/uuid so replay is
/// byte-identical across runs (journal replays, seeded backtests).
///
/// # Errors
/// Returns a `String` when the spec fails the bracket contract.
pub fn plan_to_spec_with_id(
    plan: &TradePlan,
    client_order_id: Uuid,
) -> Result<BracketOrderSpec, String> {
    let entry = plan_to_entry_request(plan, client_order_id);
    let trail = match plan.trail.rule {
        tantra_core::TrailRule::Off => None,
        ref rule => Some(rule.clone()),
    };
    BracketOrderSpec::new(entry, plan.initial_stop, plan.targets.clone(), trail)
        .map_err(|e| e.to_string())
}

/// A bracket's deterministic client order id for the `#stop` leg.
///
/// The stop child is idempotent across trail/cancel+resubmit cycles: its
/// client order id derives only from the bracket id, never a fresh UUID, so a
/// broker reject + retry cannot double-submit the stop (exec convention,
/// [`stop_client_order_id`]).
pub fn stop_leg_client_id(bracket_id: &str) -> Uuid {
    child_client_id(bracket_id, "stop")
}

/// A target leg's deterministic client order id.
pub fn target_leg_client_id(bracket_id: &str, index: usize) -> Uuid {
    child_client_id(bracket_id, &format!("target{index}"))
}

/// Wraps an exec [`BracketTracker`] plus the broker-order bookkeeping the OCO
/// emulation needs: the entry broker order id, the resting stop broker order
/// id, and the per-target broker order ids, all keyed deterministically.
///
/// This is the read surface ("bracket view"): open quantity, resting stop,
/// closed flag, the journaled events, and the order-id map. The state machine
/// itself stays pure — every transition is driven through
/// [`crate::dispatch::BrokerAdapter`] by [`SyntheticOco`].
#[derive(Debug, Clone)]
pub struct SyntheticBracket {
    /// The pure bracket lifecycle state machine.
    tracker: BracketTracker,
    /// The validated bracket spec (entry order, stop, target ladder).
    spec: BracketOrderSpec,
    /// Deterministic bracket id (`BRK-<seed>`), keying every leg's client id.
    bracket_id: String,
    /// Broker order id of the submitted entry, once submitted.
    entry_broker_id: Option<String>,
    /// Broker order id of the resting stop child, while it rests.
    stop_broker_id: Option<String>,
    /// Broker order ids of the target children, parallel to `spec.targets`
    /// (filled/cancelled legs become `None`).
    target_broker_ids: Vec<Option<String>>,
}

impl SyntheticBracket {
    /// Open a synthetic bracket for a validated spec.
    ///
    /// # Errors
    /// Returns [`BracketError`] when the spec fails validation (stop on the
    /// wrong side of the entry, malformed target ladder).
    pub fn new(
        spec: BracketOrderSpec,
        bracket_id: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<Self, BracketError> {
        let bracket_id = bracket_id.into();
        let target_broker_ids = vec![None; spec.targets.len()];
        let tracker = BracketTracker::new(spec.clone(), now)?;
        Ok(Self {
            tracker,
            spec,
            bracket_id,
            entry_broker_id: None,
            stop_broker_id: None,
            target_broker_ids,
        })
    }

    /// The deterministic bracket id (`BRK-<seed>`).
    pub fn bracket_id(&self) -> &str {
        &self.bracket_id
    }

    /// Quantity still open (not yet exited).
    pub fn open_qty(&self) -> Quantity {
        self.tracker.open_qty()
    }

    /// The current resting stop price.
    pub fn resting_stop(&self) -> Price {
        self.tracker.resting_stop()
    }

    /// Whether the bracket has closed.
    pub fn is_closed(&self) -> bool {
        self.tracker.is_closed()
    }

    /// The journaled lifecycle events, in order (forwarded).
    pub fn events(&self) -> &[BracketEvent] {
        self.tracker.events()
    }

    /// The validated bracket spec this bracket was opened from.
    pub fn spec(&self) -> &BracketOrderSpec {
        &self.spec
    }

    /// Broker order id of the submitted entry, if submitted.
    pub fn entry_broker_id(&self) -> Option<&str> {
        self.entry_broker_id.as_deref()
    }

    /// Broker order id of the resting stop child, while it rests.
    pub fn stop_broker_id(&self) -> Option<&str> {
        self.stop_broker_id.as_deref()
    }

    /// Broker order ids of the target children, parallel to the spec's target
    /// ladder (filled/cancelled legs are `None`).
    pub fn target_broker_ids(&self) -> &[Option<String>] {
        &self.target_broker_ids
    }

    /// The exec-style deterministic stop-leg string id (`{bracket}#stop`).
    pub fn stop_client_order_id(&self) -> String {
        stop_client_order_id(&self.bracket_id)
    }

    /// Target legs not yet filled, in ladder order (forwarded).
    pub fn remaining_targets(&self) -> &[PlannedTarget] {
        self.tracker.remaining_targets()
    }

    /// Target legs already filled, in fill order (forwarded).
    pub fn filled_targets(&self) -> &[PlannedTarget] {
        self.tracker.filled_targets()
    }
}

/// The synthetic-OCO driver: coordinates a [`BracketTracker`] against a broker
/// [`BrokerAdapter`] boundary.
///
/// Owns the bracket plus the realized P&L accumulator; drives the entry
/// submission, the child-order arming, the conservative bar-by-bar stop-vs-
/// target decisions, and the OCO sibling cancellations. Zero I/O and zero clock
/// reads: every timestamp is injected by the caller.
pub struct SyntheticOco<'a> {
    /// The bracket state plus broker-order bookkeeping.
    bracket: SyntheticBracket,
    /// The synchronous simulation broker port (`submit`/`cancel`).
    broker: &'a dyn BrokerAdapter,
    /// The entry fill (price, qty), once filled — anchors P&L math.
    entry_fill: Option<(Price, Quantity)>,
    /// Accumulated realized P&L over closed exits, exact `Decimal`.
    realized_pnl: Decimal,
}

impl<'a> std::fmt::Debug for SyntheticOco<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyntheticOco")
            .field("bracket", &self.bracket)
            .field("realized_pnl", &self.realized_pnl)
            .finish()
    }
}

impl<'a> SyntheticOco<'a> {
    /// Construct a synthetic-OCO driver for a validated spec.
    ///
    /// `seed` keys the bracket id (`BRK-<seed>`), which derives every leg's
    /// deterministic client order id — same seed, same spec, same clock ⇒
    /// byte-identical order stream.
    ///
    /// # Errors
    /// Returns a `String` when the spec fails the bracket contract (propagated
    /// from [`BracketTracker::new`]).
    pub fn new(
        spec: BracketOrderSpec,
        broker: &'a dyn BrokerAdapter,
        seed: u128,
        now: DateTime<Utc>,
    ) -> Result<Self, String> {
        let bracket_id = format!("BRK-{seed:032x}");
        let bracket = SyntheticBracket::new(spec, bracket_id, now).map_err(|e| e.to_string())?;
        Ok(Self {
            bracket,
            broker,
            entry_fill: None,
            realized_pnl: Decimal::ZERO,
        })
    }

    /// Construct a driver wrapping an already-open [`SyntheticBracket`].
    ///
    /// Useful when the bracket id is owned by the caller (e.g. restored from a
    /// persisted session).
    pub fn from_bracket(bracket: SyntheticBracket, broker: &'a dyn BrokerAdapter) -> Self {
        Self {
            bracket,
            broker,
            entry_fill: None,
            realized_pnl: Decimal::ZERO,
        }
    }

    /// The wrapped bracket state (read surface).
    pub fn bracket(&self) -> &SyntheticBracket {
        &self.bracket
    }

    /// Mutate the wrapped bracket state (mainly for the dispatcher to recover
    /// the bracket after submission).
    pub fn bracket_mut(&mut self) -> &mut SyntheticBracket {
        &mut self.bracket
    }

    /// Whether the bracket has closed.
    pub fn is_closed(&self) -> bool {
        self.bracket.is_closed()
    }

    /// Quantity still open (not yet exited).
    pub fn open_qty(&self) -> Quantity {
        self.bracket.open_qty()
    }

    /// The current resting stop price.
    pub fn resting_stop(&self) -> Price {
        self.bracket.resting_stop()
    }

    /// The journaled lifecycle events, in order (forwarded).
    pub fn events(&self) -> &[BracketEvent] {
        self.bracket.events()
    }

    /// Accumulated realized P&L over closed exits, exact `Decimal` (losses are
    /// negative).
    pub fn realized_pnl(&self) -> Decimal {
        self.realized_pnl
    }

    /// Submit the bracket's entry order through the broker and record its
    /// broker order id.
    ///
    /// The tracker is *armed* on this id (the position opens only on the
    /// later [`SyntheticOco::on_entry_fill`]).
    ///
    /// # Errors
    /// Returns the broker rejection when the entry submission fails.
    pub fn submit_entry(&mut self, now: DateTime<Utc>) -> Result<String, String> {
        if self.bracket.entry_broker_id.is_some() {
            return Err("bracket entry already submitted".to_string());
        }
        let entry = self.bracket.spec.entry.clone();
        let broker_id = self.broker.submit_order(&entry, now)?;
        self.bracket.entry_broker_id = Some(broker_id.clone());
        Ok(broker_id)
    }

    /// Record the entry fill and arm the bracket's child orders.
    ///
    /// On success the bracket is open, the resting stop child (`#stop`) is
    /// submitted through the broker, and every target leg is submitted as a
    /// limit child (`#target{i}`); their broker order ids are recorded.
    ///
    /// # Errors
    /// - [`BracketError::NotActive`] when the bracket is closed or already
    ///   filled (surfaced as `"bracket not active: ..."`).
    /// - [`BracketError::InvalidSpec`] on an entry-fill quantity that does not
    ///   match the bracket entry quantity, or a fill on the wrong side of the
    ///   stop.
    /// - The broker rejection when a child order submission fails.
    pub fn on_entry_fill(
        &mut self,
        fill_price: Price,
        fill_qty: Quantity,
        ts: DateTime<Utc>,
    ) -> Result<(), String> {
        self.bracket
            .tracker
            .on_entry_fill(fill_price, fill_qty, ts)
            .map_err(|e| e.to_string())?;
        self.entry_fill = Some((fill_price, fill_qty));

        // Arm the resting stop child.
        let stop_req = self.stop_child_request();
        let stop_id = self
            .broker
            .submit_order(&stop_req, ts)
            .map_err(|e| format!("stop child submission failed: {e}"))?;
        self.bracket.stop_broker_id = Some(stop_id);

        // Arm every target leg as a limit child.
        for (index, target) in self.bracket.spec.targets.iter().enumerate() {
            let req = self.target_child_request(index, target);
            let id = self
                .broker
                .submit_order(&req, ts)
                .map_err(|e| format!("target child {index} submission failed: {e}"))?;
            self.bracket.target_broker_ids[index] = Some(id);
        }
        Ok(())
    }

    /// Drive the bracket with one bar: conservative stop-vs-target fills.
    ///
    /// Mirrors the intrabar policy of `tantra-ledger-oms`'s `TradeManager`:
    /// 1. The stop is checked first — when both the stop and a target sit
    ///    inside the bar's high/low, the stop wins.
    /// 2. Target-ladder legs fill front-first.
    ///
    /// Side effects through the broker:
    /// - On a target fill: the filled leg's order is closed, the resting stop
    ///   sibling is **cancelled** (OCO), and any remaining target order that
    ///   can no longer fill is shrunk away.
    /// - On a stop trigger: every remaining target sibling is cancelled.
    ///
    /// The `snapshot` is accepted for parity with the ledger manager's
    /// signature; the pure trigger logic consumes only the bar's high/low.
    pub fn on_bar(&mut self, bar: &tantra_core::Bar, _snapshot: &TradeSnapshot) -> Vec<OcoAction> {
        let mut actions = Vec::new();
        if self.bracket.is_closed() || self.entry_fill.is_none() {
            return actions;
        }
        let side = self.bracket.spec.entry.side;
        let stop = self.bracket.resting_stop();
        let ts = bar.timestamp;

        // 1. Stop-first.
        let stop_hit = match side {
            OrderSide::Buy => bar.low <= stop,
            OrderSide::Sell => bar.high >= stop,
        };
        if stop_hit {
            let qty = self.bracket.open_qty();
            // The stop filled: its own broker order is done, and every
            // remaining target sibling is cancelled (OCO close).
            if let Some(stop_id) = self.bracket.stop_broker_id.take() {
                let _ = self.broker.cancel_order(&stop_id, ts);
            }
            self.cancel_remaining_targets(ts);
            self.bracket
                .tracker
                .on_stop_trigger(stop, ts)
                .expect("stop trigger on an active, filled bracket");
            self.accrue_pnl(stop, qty);
            actions.push(OcoAction::StopTriggered { price: stop, qty });
            return actions;
        }

        // 2. Target ladder, front-first (FIFO).
        for index in 0..self.bracket.spec.targets.len() {
            if self.bracket.target_broker_ids[index].is_none() {
                continue; // already filled/cancelled
            }
            let (target_price, target_qty, target_reason) = {
                let target = &self.bracket.spec.targets[index];
                (target.price, target.qty, target.reason.clone())
            };
            let touched = match side {
                OrderSide::Buy => bar.high >= target_price,
                OrderSide::Sell => bar.low <= target_price,
            };
            if !touched {
                continue;
            }
            self.bracket
                .tracker
                .partial_exit(target_qty, target_price, &target_reason, ts)
                .expect("partial exit within the open quantity (validated ladder)");
            // The filled leg's own broker order is done.
            if let Some(id) = self.bracket.target_broker_ids[index].take() {
                let _ = self.broker.cancel_order(&id, ts);
            }
            // OCO: a target fill cancels the resting stop sibling.
            if let Some(stop_id) = self.bracket.stop_broker_id.take() {
                let _ = self.broker.cancel_order(&stop_id, ts);
            }
            // Shrink any remaining target order that can no longer fill.
            self.shrink_remaining_targets(ts);
            self.accrue_pnl(target_price, target_qty);
            actions.push(OcoAction::TargetFilled {
                price: target_price,
                qty: target_qty,
                reason: target_reason,
            });
        }
        actions
    }

    /// Trail the resting stop to `new_stop` (a never-widening move) and push
    /// the move to the broker.
    ///
    /// The sync [`BrokerAdapter`] port has no modify verb, so the broker-side
    /// update is modelled as **cancel + resubmit** of the `#stop` child with
    /// the same deterministic client order id (never a fresh UUID) and the
    /// remaining open quantity.
    ///
    /// # Errors
    /// - [`BracketError::StopWidening`] (`"stop move widens risk: ..."`) when
    ///   the move would widen risk.
    /// - [`BracketError::NotActive`] when the bracket is closed or unfilled.
    /// - The broker rejection when a cancel/resubmit fails.
    pub fn trail_stop(&mut self, new_stop: Price, ts: DateTime<Utc>) -> Result<(), String> {
        let qty = self.bracket.open_qty();
        self.bracket
            .tracker
            .modify_stop(new_stop, Some(qty), ts)
            .map_err(|e| e.to_string())?;

        if let Some(old_id) = self.bracket.stop_broker_id.take() {
            self.broker
                .cancel_order(&old_id, ts)
                .map_err(|e| format!("stop sibling cancel failed: {e}"))?;
        }
        let req = self.stop_child_request();
        let new_id = self
            .broker
            .submit_order(&req, ts)
            .map_err(|e| format!("stop child resubmission failed: {e}"))?;
        self.bracket.stop_broker_id = Some(new_id);
        Ok(())
    }

    /// Build the resting stop child order (exit side, current resting stop,
    /// remaining open quantity, deterministic `#stop` client id).
    fn stop_child_request(&self) -> OrderRequest {
        OrderRequest::with_id(
            stop_leg_client_id(&self.bracket.bracket_id),
            self.bracket.spec.entry.instrument.clone(),
            self.bracket.spec.entry.side.opposite(),
            OrderType::StopLoss(self.bracket.resting_stop()),
            self.bracket.open_qty(),
            self.bracket.spec.entry.algo_id.clone(),
        )
    }

    /// Build one target leg's limit child order (exit side, leg limit price
    /// and quantity, deterministic `#target{i}` client id).
    fn target_child_request(&self, index: usize, target: &PlannedTarget) -> OrderRequest {
        OrderRequest::with_id(
            target_leg_client_id(&self.bracket.bracket_id, index),
            self.bracket.spec.entry.instrument.clone(),
            self.bracket.spec.entry.side.opposite(),
            OrderType::Limit(target.price),
            target.qty,
            self.bracket.spec.entry.algo_id.clone(),
        )
    }

    /// Cancel every remaining target sibling through the broker (stop-trigger
    /// OCO close). Tolerant of already-closed legs.
    fn cancel_remaining_targets(&mut self, ts: DateTime<Utc>) {
        for slot in &mut self.bracket.target_broker_ids {
            if let Some(id) = slot.take() {
                let _ = self.broker.cancel_order(&id, ts);
            }
        }
    }

    /// Shrink (cancel) any remaining target order whose quantity can no longer
    /// fill against the remaining open quantity. A validated ladder never
    /// needs this; it is a defensive OCO consistency guard.
    fn shrink_remaining_targets(&mut self, ts: DateTime<Utc>) {
        let open = self.bracket.open_qty();
        for index in 0..self.bracket.spec.targets.len() {
            if self.bracket.target_broker_ids[index].is_none() {
                continue;
            }
            if self.bracket.spec.targets[index].qty > open {
                if let Some(id) = self.bracket.target_broker_ids[index].take() {
                    let _ = self.broker.cancel_order(&id, ts);
                }
            }
        }
    }

    /// Accrue realized P&L for a closed exit: `(exit − entry) × qty` for longs,
    /// `(entry − exit) × qty` for shorts — exact `Decimal`.
    fn accrue_pnl(&mut self, exit: Price, qty: Quantity) {
        let (entry, _) = self
            .entry_fill
            .expect("entry fill recorded before any exit is accrued");
        let delta = match self.bracket.spec.entry.side {
            OrderSide::Buy => exit.value() - entry.value(),
            OrderSide::Sell => entry.value() - exit.value(),
        };
        self.realized_pnl += delta * qty.value();
    }
}

/// Convenience `Arc` alias for the owned-broker construction style.
pub type ArcBrokerAdapter = Arc<dyn BrokerAdapter>;
