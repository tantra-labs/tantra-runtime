//! The REST `/orders` routes against the shared `tantra-exec-paper` exchange (plan 0005 D1,
//! T14.5): the paper exchange now stands behind every order route, so a market order needs
//! feed depth to fill, a resting limit waits for the market to cross it, stops arm until
//! their trigger breaches, a partial fill keeps its filled quantity across a cancel, and
//! `/positions/close` squares the ledger back to flat.
//!
//! Acceptance rows R1/R2/R3/R7 of
//! `tantra-runtime/docs/plans/0005-paper-exchange-runtime-wiring.md`.

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use tantra_core::{Currency, Exchange, InstrumentId, InstrumentKind};
use tantra_runtime_api_rest::market_feed::wire::{
    BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use tantra_runtime_api_rest::{AppState, api_router_with};
use tower::util::ServiceExt;

fn iid() -> InstrumentId {
    InstrumentId::try_new(Exchange::Nse, "X").unwrap()
}

/// One instrument `X.NSE` (INR equity, lot 25, tick 0.05) with one bar closing at 100. The
/// dataset bar only feeds the bar *reader*: the paper exchange holds no depth until a bar is
/// pushed into it with `advance_market`.
fn state() -> AppState {
    let id = iid();
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::from_u64(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
}

/// A flat bar on `X.NSE` at event time `ts`, close `open`, volume 1000. Feeding it to the
/// exchange seeds synthetic depth at `open` with that volume (a 1000-lot book participates up
/// to a 500-lot cap per order).
fn bar(ts: u64, open: f64) -> tantra_core::Bar {
    let px = tantra_core::Price::try_new(Decimal::from_f64_retain(open).unwrap()).unwrap();
    tantra_core::Bar {
        instrument: iid(),
        open: px,
        high: px,
        low: px,
        close: px,
        volume: tantra_core::Quantity::from_u64(1000),
        timestamp: chrono::DateTime::from_timestamp_nanos(ts as i64),
    }
}

async fn call(app: Router, method: Method, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(v2(uri));
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    let res = app.oneshot(builder.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn order(side: &str, qty: f64, id: &str, limit: Option<f64>) -> Value {
    let mut body = json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": side,
        "qty": qty,
        "client_order_id": id,
    });
    if let Some(px) = limit {
        body["order_type"] = json!("limit");
        body["price"] = json!(px);
    }
    body
}

fn status_of(state: &AppState, id: &str) -> String {
    serde_json::to_value(state.orders.get(id).unwrap()).unwrap()["status"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn row(state: &AppState, id: &str) -> Value {
    serde_json::to_value(state.orders.get(id).unwrap()).unwrap()
}

fn num(v: &Value) -> f64 {
    if let Some(n) = v.as_f64() {
        n
    } else if let Some(s) = v.as_str() {
        s.parse::<f64>().unwrap()
    } else {
        panic!("not a number: {v}")
    }
}

// R1: a limit order rests as `accepted` with no depth, then fills when a bar crosses the limit.
#[tokio::test]
async fn post_limit_order_is_accepted_and_fills_when_a_bar_crosses_the_limit() {
    let st = state();
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 100.0, "l1", Some(90.0))),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(status_of(&st, "l1"), "accepted");

    st.advance_market(&[bar(2_000, 85.0)]).unwrap();
    assert_eq!(status_of(&st, "l1"), "filled");
    assert_eq!(st.orders.position(&iid()), dec!(100.0));
}

// R2: a market sell with depth fills immediately and the ledger position turns negative.
#[tokio::test]
async fn short_sell_fills_and_position_is_negative() {
    let st = state();
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("sell", 25.0, "s1", None)),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(status_of(&st, "s1"), "filled");
    assert_eq!(st.orders.position(&iid()), dec!(-25.0));
}

// R3: a market order larger than the participation-capped book partially fills; cancelling the
// remainder keeps the filled quantity and marks the row cancelled.
#[tokio::test]
async fn partial_fill_then_cancel_keeps_filled_qty_and_marks_cancelled() {
    let st = state();
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 5000.0, "p1", None)),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(status_of(&st, "p1"), "partially_filled");
    let filled = num(&row(&st, "p1")["filled_qty"]);
    assert!(filled > 0.0, "a partial fill books some quantity");
    assert!(st.orders.position(&iid()) > dec!(0.0));

    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders/p1/cancel",
        Some(json!({})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let r = row(&st, "p1");
    assert_eq!(r["status"], "cancelled");
    assert!(
        num(&r["filled_qty"]) > 0.0,
        "the cancel keeps the filled quantity"
    );
}

// R7: `/positions/close` squares a long back to flat with a real second order that fills.
#[tokio::test]
async fn close_squares_both_long_and_short() {
    let st = state();
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 50.0, "o1", None)),
    )
    .await;
    assert_eq!(status_of(&st, "o1"), "filled");
    assert_eq!(st.orders.position(&iid()), dec!(50.0));

    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/positions/close",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(st.orders.positions().is_empty());
    assert!(
        st.orders
            .list()
            .iter()
            .all(|o| serde_json::to_value(o).unwrap()["status"] == "filled")
    );
}

// Stop orders: a buy stop arms in `New` (accepted), then triggers and fills once the last
// price breaches the trigger.
#[tokio::test]
async fn stop_market_arms_then_triggers_and_fills() {
    let st = state();
    let body = json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": "buy", "qty": 25.0, "client_order_id": "st1",
        "order_type": "stop_market", "price": 105.0,
    });
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(body),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(status_of(&st, "st1"), "accepted");

    st.advance_market(&[bar(1_000, 110.0)]).unwrap();
    assert_eq!(status_of(&st, "st1"), "filled");
    assert_eq!(st.orders.position(&iid()), dec!(25.0));
}

// A stop-limit parses and arms too (plan 0005 D1 keeps the wire's single price as both the
// trigger and the limit until the wire struct gains a second price field).
#[tokio::test]
async fn stop_limit_is_accepted_and_arms() {
    let st = state();
    let body = json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": "sell", "qty": 25.0, "client_order_id": "sl1",
        "order_type": "stop_limit", "price": 95.0,
    });
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(body),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(status_of(&st, "sl1"), "accepted");
}

// The injection seam: `with_execution_sim` swaps the default (real-fee) exchange for one the
// test built itself (here a zero-fee exchange for exact accounting).
#[tokio::test]
async fn with_execution_sim_swaps_in_a_zero_fee_exchange() {
    use tantra_exec_paper::{PaperExchange, ZeroFillCost};
    let st = state().with_execution_sim(
        PaperExchange::new(dec!(1_000_000)).with_cost_fn(Box::new(ZeroFillCost)),
    );
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "z1", None)),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(status_of(&st, "z1"), "filled");
    assert_eq!(st.orders.position(&iid()), dec!(25.0));
}

fn v2(uri: &str) -> String {
    if uri.starts_with("/api/") {
        uri.to_string()
    } else {
        format!("/api/v2{uri}")
    }
}
