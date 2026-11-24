//! Approved orders reach the shared `tantra-exec-paper` exchange (plan 0005 D1, T14.5): a
//! market order needs feed depth to fill (with none, it is refused `no market price`), a
//! resting limit fills once a bar crosses it, and the ledger position follows. No network, no
//! wall clock: event time is the bar's `ts_event`.

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
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
    InstrumentId::new(Exchange::Nse, "X")
}

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

fn bar(ts: u64, open: f64) -> tantra_core::Bar {
    let px = tantra_core::Price::new(rust_decimal::Decimal::from_f64_retain(open).unwrap());
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

#[tokio::test]
async fn market_order_fills_immediately_when_depth_exists() {
    let st = state();
    // The market needs feed depth first: with none, a market order is refused. Once the
    // first bar has printed, a market order fills at once.
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1", None)),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(status_of(&st, "o1"), "filled");
    assert_eq!(st.orders.position(&iid()), dec!(25.0));
}

#[tokio::test]
async fn limit_order_fills_when_a_bar_crosses_the_limit() {
    let st = state();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "l1", Some(99.0))),
    )
    .await;
    assert_eq!(status_of(&st, "l1"), "accepted");
    st.advance_market(&[bar(1_000, 90.0)]).unwrap();
    assert_eq!(status_of(&st, "l1"), "filled");
    assert_eq!(st.orders.position(&iid()), dec!(25.0));
}

#[tokio::test]
async fn short_sell_fills_against_depth() {
    let st = state();
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("sell", 25.0, "s1", None)),
    )
    .await;
    assert_eq!(status_of(&st, "s1"), "filled");
    assert_eq!(st.orders.position(&iid()), dec!(-25.0));
}

#[tokio::test]
async fn close_after_a_fill_flattens_the_position() {
    let st = state();
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1", None)),
    )
    .await;
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/positions/close",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    // The close sell squares the position against the depth the seed bar left resting.
    assert!(st.orders.positions().is_empty());
    assert!(
        st.orders
            .list()
            .iter()
            .all(|o| serde_json::to_value(o).unwrap()["status"] == "filled")
    );
}

#[tokio::test]
async fn approval_queue_approve_submits_to_the_exchange() {
    let st = state();
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    let st = st.with_approval_queue(10);
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "q1", None)),
    )
    .await;
    let (_, q) = call(api_router_with(st.clone()), Method::GET, "/approvals", None).await;
    let d = &q["data"];
    let items = d
        .as_array()
        .or_else(|| d["queue"].as_array())
        .or_else(|| d["approvals"].as_array())
        .cloned()
        .unwrap_or_default();
    let id = items[0]["id"]
        .as_str()
        .or_else(|| items[0]["queue_id"].as_str())
        .unwrap()
        .to_string();
    call(
        api_router_with(st.clone()),
        Method::POST,
        &format!("/approvals/{id}/approve"),
        Some(json!({})),
    )
    .await;
    assert_eq!(status_of(&st, "q1"), "filled");
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

#[tokio::test]
async fn a_filled_market_order_reports_filled_qty_and_the_fill_price() {
    let st = state();
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1", None)),
    )
    .await;

    // The market order filled against the seeded depth at 101.
    let after = row(&st, "o1");
    assert_eq!(after["status"], "filled");
    assert_eq!(num(&after["filled_qty"]), 25.0);
    assert!((num(&after["avg_fill_price"]) - 101.0).abs() < 0.5);
    assert!(after["reject_reason"].is_null() || after.get("reject_reason").is_none());
    // `filled_at` is the fill's event time: the order's submission (a marketable fill).
    assert!(!after["filled_at"].is_null() && after.get("filled_at").is_some());

    // GET /orders serves the same fields.
    let (_, body) = call(api_router_with(st.clone()), Method::GET, "/orders", None).await;
    assert_eq!(num(&body["data"]["orders"][0]["filled_qty"]), 25.0);
}

#[tokio::test]
async fn simulator_rejections_carry_a_stable_code_and_the_reason() {
    let st = state();
    // A market order with no feed depth is refused by the paper exchange.
    call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "m1", None)),
    )
    .await;
    let market = row(&st, "m1");
    assert_eq!(market["status"], "rejected");
    assert_eq!(market["reject_reason"]["code"], "conflict");
    assert!(
        market["reject_reason"]["message"]
            .as_str()
            .unwrap()
            .contains("no market price")
    );

    // A non-positive quantity never reaches the exchange: `parse_order` refuses it.
    let (status, body) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(json!({
            "instrument_id": {"symbol": "X", "exchange": "NSE"},
            "side": "buy", "qty": 0.0, "client_order_id": "q0",
        })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], "validation_invalid_request");
}

fn v2(uri: &str) -> String {
    if uri.starts_with("/api/") {
        uri.to_string()
    } else {
        format!("/api/v2{uri}")
    }
}
