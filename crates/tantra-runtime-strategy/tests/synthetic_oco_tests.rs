//! Synthetic-OCO bracket driver tests (T12.5 follow-on).
//!
//! Verifies the `SyntheticOco` / `SyntheticBracket` emulation over exec's
//! `BracketTracker`:
//! - plan → spec translation (entry / stop / targets / trail round-trip),
//! - the full long lifecycle on `PaperAdapter` (arm → target fill → OCO stop
//!   cancel → monotone trail → stop trigger → close) with exact realized P&L,
//! - conservative stop-first bars (stop wins when both stop and target are
//!   inside the bar),
//! - deterministic client order ids (same seed ⇒ byte-identical broker order
//!   id sequences),
//! - the mirrored short-side lifecycle,
//! - the `Dispatcher::dispatch_bracket` risk-gated wiring.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tantra_core::{
    Bar, Exchange, InstrumentId, Money, OrderSide, OrderType, PlannedTarget, Price, Quantity,
    TradePlan, TrailRule, TrailSpec, uuid::Uuid,
};
use tantra_exec_paper::OrderState;
use tantra_ledger_oms::TradeSnapshot;
use tantra_risk::RiskEngine;
use tantra_runtime_strategy::{
    DispatchError, Dispatcher, OcoAction, PaperAdapter, SyntheticOco, child_client_id,
    entry_client_id, plan_to_spec, plan_to_spec_with_id, stop_leg_client_id, target_leg_client_id,
};

/// Fixed injected timestamps keep every lifecycle byte-deterministic.
fn at(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).expect("valid timestamp")
}

fn nifty() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "NIFTY")
}

fn price(s: &str) -> Price {
    Price::from_rupees(s).unwrap()
}

fn qty(n: u64) -> Quantity {
    Quantity::from_u64(n)
}

fn dec(s: &str) -> Decimal {
    Decimal::from_str_exact(s).unwrap()
}

/// A 2-leg long `TradePlan`: entry 100, stop 95, targets 110 (10) + 120 (5),
/// 30 shares, percent trailing.
fn long_plan() -> TradePlan {
    TradePlan {
        instrument: nifty(),
        side: OrderSide::Buy,
        entry: price("100.00"),
        initial_stop: price("95.00"),
        risk_per_unit: dec("5.00"),
        quantity: qty(30),
        risk_at_stake: Money::inr(dec("150.00")),
        targets: vec![
            PlannedTarget {
                price: price("110.00"),
                qty: qty(10),
                reason: "target 1 at 1R".into(),
            },
            PlannedTarget {
                price: price("120.00"),
                qty: qty(5),
                reason: "target 2 at 2R".into(),
            },
        ],
        trail: TrailSpec {
            rule: TrailRule::Percent(dec("2.00")),
            activation_after_r: dec("1.00"),
            breakeven_after: None,
            time_stop: None,
        },
        reward_risk: dec("2.00"),
        spec_version: 1,
        rationale: "test long bracket".into(),
    }
}

/// The mirrored 2-leg short `TradePlan`: sell entry 100, stop 105 (above),
/// cover targets 90 (10) + 80 (5).
fn short_plan() -> TradePlan {
    TradePlan {
        instrument: nifty(),
        side: OrderSide::Sell,
        entry: price("100.00"),
        initial_stop: price("105.00"),
        risk_per_unit: dec("5.00"),
        quantity: qty(30),
        risk_at_stake: Money::inr(dec("150.00")),
        targets: vec![
            PlannedTarget {
                price: price("90.00"),
                qty: qty(10),
                reason: "target 1 at 1R".into(),
            },
            PlannedTarget {
                price: price("80.00"),
                qty: qty(5),
                reason: "target 2 at 2R".into(),
            },
        ],
        trail: TrailSpec {
            rule: TrailRule::Percent(dec("2.00")),
            activation_after_r: dec("1.00"),
            breakeven_after: None,
            time_stop: None,
        },
        reward_risk: dec("2.00"),
        spec_version: 1,
        rationale: "test short bracket".into(),
    }
}

/// A bar with the given open/high/low/close at `t` seconds.
fn make_bar(open: &str, high: &str, low: &str, close: &str, t: i64) -> Bar {
    Bar {
        instrument: nifty(),
        open: price(open),
        high: price(high),
        low: price(low),
        close: price(close),
        volume: qty(1_000),
        timestamp: at(t),
    }
}

/// A minimal injected snapshot for `on_bar` (the trigger logic consumes only
/// the bar's high/low).
fn snapshot(t: i64) -> TradeSnapshot {
    TradeSnapshot {
        last_price: price("0.00"),
        atr: None,
        ma_values: Vec::new(),
        high: None,
        low: None,
        timestamp: at(t),
    }
}

// ---------------------------------------------------------------------------
// (a) plan → spec translation
// ---------------------------------------------------------------------------

#[test]
fn plan_to_spec_translates_entry_stop_targets_trail() {
    let plan = long_plan();
    let spec = plan_to_spec(&plan).expect("plan translates");

    // Entry: side, quantity, deterministic limit at plan.entry.
    assert_eq!(spec.entry.side, OrderSide::Buy);
    assert_eq!(spec.entry.quantity, qty(30));
    assert_eq!(spec.entry.order_type, OrderType::Limit(price("100.00")));
    // Deterministic client order id derived from the plan.
    let plan_id = tantra_runtime_strategy::plan_client_order_id(&plan);
    assert_eq!(spec.entry.client_order_id, plan_id);

    // Stop.
    assert_eq!(spec.initial_stop, price("95.00"));

    // Target ladder round-trips exactly.
    assert_eq!(spec.targets, plan.targets);

    // Trail rule is lifted from the plan's TrailSpec.
    assert_eq!(spec.trail, Some(TrailRule::Percent(dec("2.00"))));
}

#[test]
fn plan_to_spec_with_id_keys_children_deterministically() {
    let plan = long_plan();
    let bracket_id = "BRK-00000000000000000000000000000042";
    let spec = plan_to_spec_with_id(&plan, entry_client_id(bracket_id)).expect("translates");

    // Child client ids derive from the bracket id, never a fresh UUID.
    assert_eq!(spec.entry.client_order_id, entry_client_id(bracket_id));
    assert_eq!(
        stop_leg_client_id(bracket_id),
        child_client_id(bracket_id, "stop")
    );
    assert_eq!(
        target_leg_client_id(bracket_id, 1),
        child_client_id(bracket_id, "target1")
    );
}

// ---------------------------------------------------------------------------
// (b) full long lifecycle on PaperAdapter
// ---------------------------------------------------------------------------

#[test]
fn full_long_lifecycle_target_fill_oco_cancel_trail_stop_trigger() {
    let seed = 0x0000_0000_0000_0000_0000_0000_0000_0001u128;
    let paper = PaperAdapter::new(dec("1000000.00"));
    let paper_view = paper.clone();
    paper.set_market_price(nifty(), dec("100.00"));

    let spec = plan_to_spec(&long_plan()).expect("translates");
    let mut oco = SyntheticOco::new(spec, &paper, seed, at(1000)).expect("bracket opens");

    // 1. Submit the entry.
    let entry_id = oco.submit_entry(at(1001)).expect("entry submits");
    assert!(paper_view.orders().contains_key(&entry_id));
    assert!(!oco.is_closed());
    assert_eq!(oco.open_qty(), Quantity::ZERO);

    // 2. Entry fills: stop child + both target legs are armed.
    oco.on_entry_fill(price("100.00"), qty(30), at(1002))
        .expect("entry fills");
    let stop_id = oco
        .bracket()
        .stop_broker_id()
        .expect("stop child armed")
        .to_string();
    let _t1_id = oco.bracket().target_broker_ids()[0]
        .clone()
        .expect("target 1 armed");
    let t2_id = oco.bracket().target_broker_ids()[1]
        .clone()
        .expect("target 2 armed");
    assert_eq!(oco.open_qty(), qty(30));
    // Stop child carries the deterministic #stop client id.
    let bracket_id = oco.bracket().bracket_id().to_string();
    let orders = paper_view.orders();
    let stop_req = orders.get(&stop_id).expect("stop order on venue");
    assert_eq!(stop_req.client_order_id, stop_leg_client_id(&bracket_id));
    drop(orders);

    // 3. A bar pierces target 1 (but not the stop): TargetFilled, and the OCO
    //    stop sibling is CANCELED via the broker.
    let bar = make_bar("105.00", "111.00", "104.00", "110.50", 1003);
    let actions = oco.on_bar(&bar, &snapshot(1003));
    assert_eq!(
        actions,
        vec![OcoAction::TargetFilled {
            price: price("110.00"),
            qty: qty(10),
            reason: "target 1 at 1R".into(),
        }]
    );
    assert_eq!(
        paper_view.order_state(&stop_id),
        Some(OrderState::Cancelled),
        "stop sibling must be cancelled by the broker when a target leg fills"
    );
    assert!(
        oco.bracket().stop_broker_id().is_none(),
        "driver must forget the cancelled stop sibling"
    );
    // Bracket still open for the runner.
    assert!(!oco.is_closed());
    assert_eq!(oco.open_qty(), qty(20));
    // Realized P&L after the first leg: (110 - 100) × 10 = 100.
    assert_eq!(oco.realized_pnl(), dec("100.00"));

    // 4. Trail the stop tighter (monotone: 95 → 98) — cancel+resubmit the
    //    #stop child with the same deterministic client id.
    oco.trail_stop(price("98.00"), at(1004))
        .expect("trail tightens");
    assert_eq!(oco.resting_stop(), price("98.00"));
    let new_stop_id = oco.bracket().stop_broker_id().expect("stop re-armed");
    assert_ne!(new_stop_id, stop_id);
    let orders = paper_view.orders();
    let rearmed = orders.get(new_stop_id).expect("re-armed stop");
    assert_eq!(rearmed.client_order_id, stop_leg_client_id(&bracket_id));
    assert_eq!(
        rearmed.quantity,
        qty(20),
        "re-armed stop must cover only the runner quantity"
    );
    // A widening move is refused (monotone).
    assert!(
        oco.trail_stop(price("96.00"), at(1005)).is_err(),
        "stop must never widen risk"
    );

    // 5. A later bar hits the stop: StopTriggered, remaining targets canceled,
    //    bracket closed, exact realized P&L.
    let bar = make_bar("99.00", "99.50", "97.00", "97.50", 1006);
    let actions = oco.on_bar(&bar, &snapshot(1006));
    assert_eq!(
        actions,
        vec![OcoAction::StopTriggered {
            price: price("98.00"),
            qty: qty(20),
        }]
    );
    assert_eq!(
        paper_view.order_state(&t2_id),
        Some(OrderState::Cancelled),
        "remaining target sibling must be canceled on stop trigger"
    );
    assert!(oco.is_closed());
    assert_eq!(oco.open_qty(), Quantity::ZERO);
    // Realized P&L: target 1 (110-100)×10 = +100; stop (98-100)×20 = -40.
    assert_eq!(oco.realized_pnl(), dec("60.00"));
}

// ---------------------------------------------------------------------------
// (c) conservative stop-first
// ---------------------------------------------------------------------------

#[test]
fn bar_containing_stop_and_target_triggers_stop_only() {
    let seed = 0x0000_0000_0000_0000_0000_0000_0000_0003u128;
    let paper = PaperAdapter::new(dec("1000000.00"));
    paper.set_market_price(nifty(), dec("100.00"));

    let spec = plan_to_spec(&long_plan()).expect("translates");
    let mut oco = SyntheticOco::new(spec, &paper, seed, at(1000)).expect("opens");
    oco.submit_entry(at(1001)).expect("entry submits");
    oco.on_entry_fill(price("100.00"), qty(30), at(1002))
        .expect("fills");

    // A bar whose high pierces target 1 (110) AND whose low pierces the stop
    // (95): the stop wins — conservative, no target fill.
    let bar = make_bar("105.00", "112.00", "94.00", "96.00", 1003);
    let actions = oco.on_bar(&bar, &snapshot(1003));
    assert_eq!(
        actions,
        vec![OcoAction::StopTriggered {
            price: price("95.00"),
            qty: qty(30),
        }]
    );
    assert!(oco.is_closed());
    // No target leg realized: stop closes the full 30 at (95-100) → -150.
    assert_eq!(oco.realized_pnl(), dec("-150.00"));
}

// ---------------------------------------------------------------------------
// (d) deterministic client order ids / replay
// ---------------------------------------------------------------------------

/// The client-order-id sequence produced by one full lifecycle on a fresh
/// venue, keyed by seed: entry id, stop id, target ids, then the re-armed
/// stop id. This is the true replay key — the venue mints sequential broker
/// ids regardless of seed, while every *client* id derives from the seed.
fn lifecycle_client_id_sequence(seed: u128) -> Vec<Uuid> {
    let paper = PaperAdapter::new(dec("1000000.00"));
    paper.set_market_price(nifty(), dec("100.00"));
    let spec = plan_to_spec_with_id(&long_plan(), entry_client_id(&format!("BRK-{seed:032x}")))
        .expect("translates");
    let mut oco = SyntheticOco::new(spec, &paper, seed, at(1000)).expect("opens");

    let mut broker_ids = vec![oco.submit_entry(at(1001)).expect("entry submits")];
    oco.on_entry_fill(price("100.00"), qty(30), at(1002))
        .expect("fills");
    broker_ids.push(
        oco.bracket()
            .stop_broker_id()
            .expect("stop armed")
            .to_string(),
    );
    broker_ids.extend(
        oco.bracket()
            .target_broker_ids()
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>(),
    );
    oco.trail_stop(price("98.00"), at(1003)).expect("trail");
    broker_ids.push(
        oco.bracket()
            .stop_broker_id()
            .expect("stop re-armed")
            .to_string(),
    );

    let orders = paper.orders();
    broker_ids
        .into_iter()
        .map(|id| orders[&id].client_order_id)
        .collect()
}

#[test]
fn same_seed_produces_byte_identical_client_order_id_sequences() {
    let seed = 0x0000_0000_0000_0000_0000_0000_0000_0004u128;
    let first = lifecycle_client_id_sequence(seed);
    let second = lifecycle_client_id_sequence(seed);
    assert_eq!(
        first, second,
        "same seed ⇒ identical client order id sequence (replay)"
    );
    assert_eq!(first.len(), 5);

    // Different seeds must diverge (every leg's client id derives from the
    // bracket id, which derives from the seed).
    let other = lifecycle_client_id_sequence(seed + 1);
    assert_ne!(first, other);
}

// ---------------------------------------------------------------------------
// (e) short side
// ---------------------------------------------------------------------------

#[test]
fn full_short_lifecycle_mirrors_long() {
    let seed = 0x0000_0000_0000_0000_0000_0000_0000_0005u128;
    let paper = PaperAdapter::new(dec("1000000.00"));
    let paper_view = paper.clone();
    paper.set_market_price(nifty(), dec("100.00"));

    let spec = plan_to_spec(&short_plan()).expect("translates");
    let mut oco = SyntheticOco::new(spec, &paper, seed, at(1000)).expect("opens");
    oco.submit_entry(at(1001)).expect("entry submits");
    oco.on_entry_fill(price("100.00"), qty(30), at(1002))
        .expect("fills");

    // Entry leg is a sell limit at plan.entry.
    let entry_req = oco.bracket().spec().entry.clone();
    assert_eq!(entry_req.side, OrderSide::Sell);
    assert_eq!(entry_req.order_type, OrderType::Limit(price("100.00")));
    let stop_id = oco
        .bracket()
        .stop_broker_id()
        .expect("stop armed")
        .to_string();

    // A bar pierces cover target 1 (90) from above: TargetFilled.
    let bar = make_bar("95.00", "102.00", "88.00", "91.00", 1003);
    let actions = oco.on_bar(&bar, &snapshot(1003));
    assert_eq!(
        actions,
        vec![OcoAction::TargetFilled {
            price: price("90.00"),
            qty: qty(10),
            reason: "target 1 at 1R".into(),
        }]
    );
    assert_eq!(
        paper_view.order_state(&stop_id),
        Some(OrderState::Cancelled),
        "short stop sibling must be cancelled on a cover-target fill"
    );
    assert_eq!(oco.open_qty(), qty(20));
    // Realized so far: (100 - 90) × 10 = +100.
    assert_eq!(oco.realized_pnl(), dec("100.00"));

    // Short stop tightens downward (105 → 103).
    oco.trail_stop(price("103.00"), at(1004))
        .expect("trail tightens");
    assert_eq!(oco.resting_stop(), price("103.00"));

    // A bar whose high pierces the 103 stop: StopTriggered closes the runner.
    let bar = make_bar("100.00", "104.00", "101.00", "103.50", 1005);
    let actions = oco.on_bar(&bar, &snapshot(1005));
    assert_eq!(
        actions,
        vec![OcoAction::StopTriggered {
            price: price("103.00"),
            qty: qty(20),
        }]
    );
    assert!(oco.is_closed());
    // Realized: +100 + (100 - 103) × 20 = 100 - 60 = 40.
    assert_eq!(oco.realized_pnl(), dec("40.00"));
}

// ---------------------------------------------------------------------------
// Dispatcher wiring
// ---------------------------------------------------------------------------

#[test]
fn dispatch_bracket_risk_gates_and_submits_entry() {
    let paper = PaperAdapter::new(dec("1000000.00"));
    let paper_view = paper.clone();
    paper.set_market_price(nifty(), dec("100.00"));

    let mut dispatcher = Dispatcher::new(RiskEngine::default(), Box::new(paper));
    let spec = plan_to_spec(&long_plan()).expect("translates");

    let entry_id = dispatcher
        .dispatch_bracket(spec, at(1000))
        .expect("bracket entry dispatches");
    assert!(entry_id.starts_with("PAPER-"));
    assert!(paper_view.orders().contains_key(&entry_id));
}

#[test]
fn dispatch_bracket_halted_risk_gate_refuses_entry() {
    let paper = PaperAdapter::new(dec("1000000.00"));
    let paper_view = paper.clone();

    let risk_engine = RiskEngine {
        global_state: tantra_risk::GlobalState::Halted {
            reason: "emergency kill switch".into(),
            halted_at: Utc::now(),
        },
        ..Default::default()
    };
    let mut dispatcher = Dispatcher::new(risk_engine, Box::new(paper));
    let spec = plan_to_spec(&long_plan()).expect("translates");

    let err = dispatcher
        .dispatch_bracket(spec, at(1000))
        .expect_err("must be refused by the risk gate");
    match err {
        DispatchError::RiskRefusal(refusal) => {
            assert_eq!(refusal.rule(), "global_halt");
        }
        other => panic!("expected RiskRefusal, got {other:?}"),
    }
    assert_eq!(paper_view.orders().len(), 0);
}
