//! The REST order routes against the shared `tantra-exec-paper` exchange (plan 0005 D1, T14.5).
//!
//! # Rebuilt on this migration, not a rename
//!
//! This used to inject a crate-local `ExecutionVenue` trait object via
//! `AppState::with_execution_gateway` — a `Fake` test double recording `submit`/`cancel` calls,
//! and an `L2Venue` wrapping `tantra_sim::L2Sim` — so the order-route plumbing could be tested
//! independently of whatever venue was behind it, including synthetic partial fills and
//! depth-level matching. `exec.rs`'s module doc explains why none of that survives: the shared
//! paper exchange is the one venue, injected by value through `AppState::with_execution_sim`.
//!
//! `tests/orders_exec.rs` and `tests/paper_orders_tests.rs` cover the straightforward
//! submit/fill/reject paths through the real exchange, so this file keeps only what those
//! don't: that a cancel actually withdraws the order from the exchange's own book (not just
//! flips the ledger's status label), and that `/positions/close` reaches the exchange with a
//! second, real order.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tantra_core::{Currency, InstrumentKind};
use tantra_runtime_api_rest::market_feed::wire::{
    BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use tantra_runtime_api_rest::{AppState, api_router_with};
use tantra_runtime_testing::fixtures::any_instrument;
use tower::util::ServiceExt;

/// A flat bar on `X.NSE` at event time `ts`, close `open`, volume 1000 — a book the exchange
/// can actually fill against (the `flat_bar` fixture's volume-1 book participates up to zero).
fn bar(ts: u64, open: f64) -> tantra_core::Bar {
    let px = tantra_core::Price::try_new(Decimal::from_f64_retain(open).unwrap()).unwrap();
    tantra_core::Bar {
        instrument: any_instrument(),
        open: px,
        high: px,
        low: px,
        close: px,
        volume: tantra_core::Quantity::from_u64(1000),
        timestamp: chrono::DateTime::from_timestamp_nanos(ts as i64),
    }
}

fn state() -> AppState {
    let id = any_instrument();
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::from_u64(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
}

async fn call(st: &AppState, method: Method, uri: &str, body: Option<Value>) -> StatusCode {
    let mut builder = Request::builder().method(method).uri(v2(uri));
    let body = match body {
        Some(value) => {
            builder = builder.header("content-type", "application/json");
            Body::from(value.to_string())
        }
        None => Body::empty(),
    };
    api_router_with(st.clone())
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap()
        .status()
}

fn order(side: &str, qty: f64, id: &str) -> Value {
    json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": side, "qty": qty, "client_order_id": id,
    })
}

fn status_of(st: &AppState, id: &str) -> String {
    serde_json::to_value(st.orders.get(id).unwrap()).unwrap()["status"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn delete_order_withdraws_it_from_the_paper_exchange_book() {
    let st = state();
    // A resting limit is acknowledged as working on the exchange's book.
    let mut body = order("buy", 25.0, "o1");
    body["order_type"] = json!("limit");
    body["price"] = json!(99.0);
    assert_eq!(
        call(&st, Method::POST, "/orders", Some(body)).await,
        StatusCode::OK
    );
    assert_eq!(status_of(&st, "o1"), "accepted");

    assert_eq!(
        call(&st, Method::POST, "/orders/o1/cancel", Some(json!({}))).await,
        StatusCode::OK
    );
    assert_eq!(status_of(&st, "o1"), "cancelled");

    // If the cancel only flipped the ledger's label and left the order resting in the
    // exchange, this bar (which crosses the limit) would fill it. It must not: the row stays
    // cancelled (terminal, so `apply_fill` is a no-op) and the ledger's position stays flat.
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    assert_eq!(status_of(&st, "o1"), "cancelled");
    assert!(st.orders.positions().is_empty());
}

#[tokio::test]
async fn close_positions_reaches_the_exchange_with_a_second_real_order() {
    let st = state();
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();
    call(&st, Method::POST, "/orders", Some(order("buy", 25.0, "o1"))).await;
    assert_eq!(status_of(&st, "o1"), "filled");

    assert_eq!(
        call(&st, Method::POST, "/positions/close", None).await,
        StatusCode::OK
    );

    // The close order is submitted to the exchange too (not merely queued): one more row,
    // already filled against the depth the seed bar left resting.
    let rows = st.orders.list();
    assert_eq!(rows.len(), 2, "the closing order is recorded alongside o1");
    let closing_filled = rows
        .iter()
        .any(|o| serde_json::to_value(o).unwrap()["status"] == "filled");
    assert!(closing_filled, "the closing order reached the exchange");

    assert!(st.orders.positions().is_empty());
    assert!(
        st.orders
            .list()
            .iter()
            .all(|o| serde_json::to_value(o).unwrap()["status"] == "filled")
    );
}

fn v2(uri: &str) -> String {
    if uri.starts_with("/api/") {
        uri.to_string()
    } else {
        format!("/api/v2{uri}")
    }
}
