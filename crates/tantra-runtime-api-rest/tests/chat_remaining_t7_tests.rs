//! Remaining T7 wiring tests (docs/gaps/0006 design decision + docs/gaps/0007 items 17–19):
//! the REST order flow populates the shared ledger the chat portfolio/tax tools read; the
//! strategy store answers CRUD; the trade-intent sink enqueues into the jobs registry.

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use tantra_agent_chat_tools::ports::strategy_store::{
    StrategyParameterSpec, StrategySpec, StrategyStore,
};
use tantra_agent_chat_tools::ports::tax::TaxReportReader;
use tantra_agent_chat_tools::ports::trade_intent::{TradeIntent, TradeIntentSink};
use tantra_core::{AccountId, Currency, Exchange, InstrumentId, InstrumentKind, Price, Quantity};
use tantra_runtime_api_rest::chat_strategy_store::FeedStrategyStore;
use tantra_runtime_api_rest::chat_tax::FeedTaxReportReader;
use tantra_runtime_api_rest::chat_trade::FeedTradeIntentSink;
use tantra_runtime_api_rest::market_feed::wire::{
    BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use tantra_runtime_api_rest::{AppState, api_router_with};
use tantra_runtime_jobs::{FileJobRegistry, JobStatus};
use tempfile::tempdir;
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
    let mut builder = Request::builder().method(method).uri(uri);
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

fn order(side: &str, qty: f64, id: &str) -> Value {
    json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": side,
        "qty": qty,
        "client_order_id": id,
    })
}

#[tokio::test]
async fn order_flow_populates_the_shared_ledger_and_tax_reader() {
    let st = state();
    // Buy 25, filled at the next bar open (101).
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("buy", 25.0, "o1")),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    st.advance_market(&[bar(1_000, 101.0)]).unwrap();

    // The shared ledger (docs/gaps/0006) now holds the default account, the approved order
    // (advanced to Filled), and the opened position the chat portfolio tools read.
    let acct = AccountId::from("rest-api");
    let account = st
        .shared_ledger
        .account(&acct)
        .expect("order flow opens the default account");
    assert_eq!(account.cash, tantra_core::Money::inr(dec!(-2525)));
    let positions = st.shared_ledger.open_lots(&acct);
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].quantity, Quantity::new(dec!(25)));
    assert_eq!(positions[0].entry_price, Price::new(dec!(101)));

    // The order row advanced to Filled alongside the REST ledger.
    let orders = st.shared_ledger.closed_lots(&acct); // (empty — no sells yet)
    assert!(orders.is_empty());

    // Sell the full position (the 25-lot size): FIFO match realizes (103 - 101) × 25 = 50,
    // visible through the tax reader.
    let (s, _) = call(
        api_router_with(st.clone()),
        Method::POST,
        "/orders",
        Some(order("sell", 25.0, "s1")),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    st.advance_market(&[bar(2_000, 103.0)]).unwrap();

    let tax = FeedTaxReportReader::new(st.shared_ledger.clone());
    let closed = tax.closed_lots(&acct, Default::default()).await.unwrap();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].instrument, iid());
    assert_eq!(closed[0].entry_price, Price::new(dec!(101)));
    assert_eq!(closed[0].exit_price, Price::new(dec!(103)));
    assert_eq!(closed[0].realized_pnl, tantra_core::Money::inr(dec!(50)));
    assert_eq!(closed[0].quantity, Quantity::new(dec!(25)));

    // Cash: -2525 (buy) + 2575 (sell) = 50.
    assert_eq!(
        st.shared_ledger.account(&acct).unwrap().cash,
        tantra_core::Money::inr(dec!(50))
    );
}

#[tokio::test]
async fn strategy_store_answers_crud() {
    let store = FeedStrategyStore::new();
    let spec = StrategySpec {
        strategy_id: "turtle_breakout".to_string(),
        name: "Turtle Breakout".to_string(),
        instruments: vec!["NSE:NIFTY50-INDEX".to_string()],
        parameters: vec![StrategyParameterSpec {
            name: "atr_period".to_string(),
            default: "14".to_string(),
            min: "5".to_string(),
            max: "30".to_string(),
            step: "1".to_string(),
        }],
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    let saved = store.save_strategy(&spec).await.unwrap();
    assert_eq!(saved.strategy_id, "turtle_breakout");

    let listed = store.list_strategies().await.unwrap();
    assert_eq!(listed.len(), 1);

    let got = store.get_strategy("turtle_breakout").await.unwrap();
    assert_eq!(got.name, "Turtle Breakout");
    assert!(store.get_strategy("missing").await.is_err());

    assert!(store.delete_strategy("turtle_breakout").await.unwrap());
    assert!(store.get_strategy("turtle_breakout").await.is_err());
}

#[tokio::test]
async fn trade_intent_sink_enqueues_into_the_jobs_registry() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("jobs.json");
    let registry = Arc::new(Mutex::new(FileJobRegistry::load_or_create(&path).unwrap()));
    let sink = FeedTradeIntentSink::new(Some(registry.clone()));

    let status = sink
        .propose_trade(&TradeIntent {
            algo_id: "my-algo-1".to_string(),
            instrument: iid(),
            side: "buy".to_string(),
            order_type: "market".to_string(),
            quantity: Quantity::new(dec!(25)),
            price: None,
            paper_trading: true,
            rationale: Some("test intent".to_string()),
        })
        .await
        .unwrap();

    assert_eq!(status.status, "pending_approval");

    let registry = registry.lock().unwrap();
    let jobs = registry.list_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].status, JobStatus::PendingApproval);
    assert_eq!(
        jobs[0].metadata.get("kind").map(String::as_str),
        Some("trade-intent")
    );
    assert_eq!(
        jobs[0].metadata.get("x-papertrading").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        jobs[0].metadata.get("instrument").map(String::as_str),
        Some("NSE:X")
    );
}
