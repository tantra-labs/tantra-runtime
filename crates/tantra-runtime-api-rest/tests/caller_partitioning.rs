//! Per-caller partitioning: the trade journal, paper-order ownership,
//! paper cash balances, positions, `/positions/close`, and `/api/v2/stream` `portfolio` snapshots
//! are keyed by the authenticated `Caller`.

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tantra_core::{Currency, Exchange, InstrumentId, InstrumentKind};
use tantra_ledger_performance::store::JournalStore;
use tantra_runtime_api::TradingState;
use tantra_runtime_api_rest::market_feed::wire::{
    BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use tantra_runtime_api_rest::{ApiConfig, AppState, api_router_with_config, serve_with_config};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tower::util::ServiceExt;

fn token_toml(entries: &[(&str, &str, &str)]) -> String {
    entries
        .iter()
        .map(|(id, plain, scopes)| {
            let hex = hex::encode(Sha256::digest(plain.as_bytes()));
            format!(
                "[[tokens]]\ntoken_id = \"{id}\"\ntoken_hash = \"sha256:{hex}\"\nscopes = [{scopes}]\nlabel = \"t\"\ncreated_at = \"2026-01-01T00:00:00Z\"\n"
            )
        })
        .collect()
}

fn bar(ts: u64, px: f64) -> tantra_core::Bar {
    let price = tantra_core::Price::try_new(Decimal::from_f64_retain(px).unwrap()).unwrap();
    tantra_core::Bar {
        instrument: InstrumentId::try_new(Exchange::Nse, "X").unwrap(),
        open: price,
        high: price,
        low: price,
        close: price,
        volume: tantra_core::Quantity::from_u64(1000),
        timestamp: chrono::DateTime::from_timestamp_nanos(ts as i64),
    }
}

fn build_state_and_config() -> (AppState, ApiConfig, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens.toml");
    std::fs::write(
        &path,
        token_toml(&[
            ("alice", "alice-secret", "\"read\", \"trade\""),
            ("bob", "bob-secret", "\"read\", \"trade\""),
        ]),
    )
    .unwrap();
    let mut config = ApiConfig::default();
    config.auth.tokens_file = Some(path);
    let id = InstrumentId::try_new(Exchange::Nse, "X").unwrap();
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::from_u64(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    let state = AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
        .with_journal_store(JournalStore::new());
    (state, config, dir)
}

/// Auth-on router with two `read`+`trade` tokens: `alice-secret` and `bob-secret`.
fn router() -> (Router, tempfile::TempDir) {
    let (state, config, dir) = build_state_and_config();
    (api_router_with_config(state, &config).unwrap(), dir)
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    token: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(match body {
            Some(b) => Body::from(b.to_string()),
            None => Body::empty(),
        })
        .unwrap();
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn order(id: &str) -> Value {
    json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": "buy",
        "qty": 25.0,
        "order_type": "limit",
        "price": 99.0,
        "client_order_id": id,
    })
}

fn market_order(side: &str, qty: f64, id: &str) -> Value {
    json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": side,
        "qty": qty,
        "order_type": "market",
        "client_order_id": id,
    })
}

type WsClient =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(addr: std::net::SocketAddr, token: &str) -> WsClient {
    let mut req = format!("ws://{addr}/api/v2/stream")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    tokio_tungstenite::connect_async(req).await.unwrap().0
}

async fn ws_send(ws: &mut WsClient, v: &Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

async fn ws_drain_until(ws: &mut WsClient, mut done: impl FnMut(&Value) -> bool) -> Vec<Value> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                let Ok(v) = msg
                    .to_text()
                    .map_err(|_| ())
                    .and_then(|t| serde_json::from_str::<Value>(t).map_err(|_| ()))
                else {
                    continue;
                };
                let hit = done(&v);
                out.push(v);
                if hit {
                    break;
                }
            }
            _ => break,
        }
    }
    out
}

#[tokio::test]
async fn journal_accounts_are_per_caller() {
    let (router, _dir) = router();
    let (status, _) = call(
        &router,
        "POST",
        "/api/v2/trade-journal/accounts",
        "alice-secret",
        Some(json!({
            "display_name": "Alice Only",
            "provenance": "Manual",
            "currency": "INR",
            "starting_balance": "1000.00"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (_, mine) = call(
        &router,
        "GET",
        "/api/v2/trade-journal/accounts",
        "alice-secret",
        None,
    )
    .await;
    assert!(mine.to_string().contains("Alice Only"), "{mine}");
    let (status, theirs) = call(
        &router,
        "GET",
        "/api/v2/trade-journal/accounts",
        "bob-secret",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!theirs.to_string().contains("Alice Only"), "{theirs}");
}

#[tokio::test]
async fn orders_are_owned_by_their_caller() {
    let (router, _dir) = router();
    let (status, _) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "alice-secret",
        Some(order("own-1")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, mine) = call(&router, "GET", "/api/v2/orders", "alice-secret", None).await;
    assert!(mine.to_string().contains("own-1"), "{mine}");
    let (status, theirs) = call(&router, "GET", "/api/v2/orders", "bob-secret", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!theirs.to_string().contains("own-1"), "{theirs}");

    let (status, _) = call(&router, "GET", "/api/v2/orders/own-1", "bob-secret", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(&router, "GET", "/api/v2/orders/own-1", "alice-secret", None).await;
    assert_eq!(status, StatusCode::OK);

    // Bob's cancel is a 200 no-op that leaves Alice's row untouched.
    let (status, _) = call(
        &router,
        "POST",
        "/api/v2/orders/own-1/cancel",
        "bob-secret",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, row) = call(&router, "GET", "/api/v2/orders/own-1", "alice-secret", None).await;
    assert!(
        !row.to_string().to_lowercase().contains("cancelled"),
        "{row}"
    );
}

#[tokio::test]
async fn client_order_id_cannot_collide_across_callers() {
    let (router, _dir) = router();
    let (status, _) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "alice-secret",
        Some(order("dup-1")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "bob-secret",
        Some(order("dup-1")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body.to_string().contains("conflict"), "{body}");
    // Alice's own replay stays idempotent.
    let (status, _) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "alice-secret",
        Some(order("dup-1")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn per_caller_positions_close_and_reduce_only_are_isolated() {
    let (state, config, _dir) = build_state_and_config();
    state.advance_market(&[bar(1_000, 100.0)]).unwrap();
    let router = api_router_with_config(state.clone(), &config).unwrap();

    // Alice buys 25, Bob shorts 25 (net across whole server is 0!).
    let (s, alice_buy) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "alice-secret",
        Some(market_order("buy", 25.0, "alice-buy-1")),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{alice_buy}");
    assert_eq!(alice_buy["data"]["status"], "filled");

    let (s, bob_sell) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "bob-secret",
        Some(market_order("sell", 25.0, "bob-sell-1")),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{bob_sell}");
    assert_eq!(bob_sell["data"]["status"], "filled");

    // In Reducing state: Alice can only sell (reduce her +25), Bob can only buy (reduce his -25).
    state.set_trading_state(TradingState::Reducing);
    let (s, bob_bad_sell) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "bob-secret",
        Some(market_order("sell", 25.0, "bob-reduce-err")),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{bob_bad_sell}");
    assert_eq!(bob_bad_sell["error"]["code"], "risk_reduce_only_violation");

    // Bob closes ONLY his own -25 short via POST /positions/close.
    let (s, close_res) = call(
        &router,
        "POST",
        "/api/v2/positions/close",
        "bob-secret",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{close_res}");

    // Bob now has 3 rows (filled sell 25, rejected sell `bob-reduce-err`, filled closing buy 25)
    // and a flat position; Alice STILL holds her +25 long untouched (1 order).
    let (_, bob_orders) = call(&router, "GET", "/api/v2/orders", "bob-secret", None).await;
    let bob_list = bob_orders["data"]["orders"].as_array().unwrap();
    assert_eq!(
        bob_list.len(),
        3,
        "bob close must create bob's buy close: {bob_orders}"
    );
    let bob_close = bob_list
        .iter()
        .find(|o| {
            o["order_id"]
                .as_str()
                .is_some_and(|id| id.starts_with("close-bob-"))
        })
        .expect("bob close order");
    assert_eq!(bob_close["side"], "buy");
    assert_eq!(bob_close["status"], "filled");
    let bob_rejected = bob_list
        .iter()
        .find(|o| o["order_id"] == "bob-reduce-err")
        .expect("bob rejected row");
    assert_eq!(bob_rejected["status"], "rejected");

    let (_, alice_orders) = call(&router, "GET", "/api/v2/orders", "alice-secret", None).await;
    let alice_list = alice_orders["data"]["orders"].as_array().unwrap();
    assert_eq!(
        alice_list.len(),
        1,
        "alice's orders untouched: {alice_orders}"
    );

    // Now Bob is flat -> further reduce-only buy is refused for Bob, while Alice can sell her 25.
    let (s, bob_flat_err) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "bob-secret",
        Some(market_order("buy", 25.0, "bob-flat-err")),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{bob_flat_err}");
    assert_eq!(bob_flat_err["error"]["code"], "risk_reduce_only_violation");

    let (s, alice_sell) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "alice-secret",
        Some(market_order("sell", 25.0, "alice-sell-1")),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{alice_sell}");
    assert_eq!(alice_sell["data"]["status"], "filled");
}

#[tokio::test]
async fn per_caller_stream_portfolio_snapshots_and_market_fanout_are_isolated() {
    let (state, config, _dir) = build_state_and_config();
    state.advance_market(&[bar(1_000, 100.0)]).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let srv_state = state.clone();
    let handle = tokio::spawn(async move {
        serve_with_config(listener, srv_state, &config, async move {
            let _ = stopped.await;
        })
        .await
    });

    let mut ws_alice = ws_connect(addr, "alice-secret").await;
    let mut ws_bob = ws_connect(addr, "bob-secret").await;

    ws_send(
        &mut ws_alice,
        &json!({"op": "subscribe", "channel": "portfolio"}),
    )
    .await;
    ws_send(
        &mut ws_bob,
        &json!({"op": "subscribe", "channel": "portfolio"}),
    )
    .await;
    ws_drain_until(&mut ws_alice, |v| v["type"] == "ack").await;
    ws_drain_until(&mut ws_bob, |v| v["type"] == "ack").await;

    // Alice places a buy limit/market order on WS while Bob watches portfolio.
    ws_send(
        &mut ws_alice,
        &json!({
            "op": "place_order",
            "request_id": "a1",
            "order": market_order("buy", 25.0, "ws-alice-1")
        }),
    )
    .await;
    let alice_frames = ws_drain_until(&mut ws_alice, |v| {
        v["type"] == "event" && v["channel"] == "portfolio" && v["data"]["type"] == "EquitySnapshot"
    })
    .await;

    let alice_pos = alice_frames
        .iter()
        .find(|v| v["data"]["type"] == "PositionSnapshot")
        .expect("alice PositionSnapshot");
    assert_eq!(alice_pos["data"]["positions"]["X"], "25");

    let alice_eq = alice_frames
        .iter()
        .find(|v| v["data"]["type"] == "EquitySnapshot")
        .expect("alice EquitySnapshot");
    let alice_cash: Decimal = alice_eq["data"]["cash"].as_str().unwrap().parse().unwrap();
    assert!(
        alice_cash < dec!(1_000_000),
        "alice cash decreased: {alice_cash}"
    );

    // Advance market to 102.0: fans out to every caller's PaperExchange.
    state.advance_market(&[bar(2_000, 102.0)]).unwrap();

    let alice_mtm = ws_drain_until(&mut ws_alice, |v| {
        v["type"] == "event" && v["channel"] == "portfolio" && v["data"]["type"] == "EquitySnapshot"
    })
    .await;
    let alice_eq2 = alice_mtm
        .iter()
        .find(|v| v["data"]["type"] == "EquitySnapshot")
        .unwrap();
    let alice_equity2: Decimal = alice_eq2["data"]["equity"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    // Bought 25 at ~100, now 102 -> +50 unrealized gain above starting equity (~1_000_050 minus tiny fees).
    assert!(
        alice_equity2 > dec!(1_000_030),
        "alice MTM equity updated: {alice_equity2}"
    );

    let bob_frames = ws_drain_until(&mut ws_bob, |v| {
        v["type"] == "event" && v["channel"] == "portfolio" && v["data"]["type"] == "EquitySnapshot"
    })
    .await;
    let bob_pos = bob_frames
        .iter()
        .find(|v| v["data"]["type"] == "PositionSnapshot")
        .expect("bob PositionSnapshot");
    assert_eq!(
        bob_pos["data"]["positions"],
        json!({}),
        "bob has no positions: {bob_pos}"
    );
    let bob_eq = bob_frames
        .iter()
        .find(|v| v["data"]["type"] == "EquitySnapshot")
        .expect("bob EquitySnapshot");
    let bob_cash: Decimal = bob_eq["data"]["cash"].as_str().unwrap().parse().unwrap();
    let bob_equity: Decimal = bob_eq["data"]["equity"].as_str().unwrap().parse().unwrap();
    assert_eq!(bob_cash, dec!(1_000_000), "bob cash untouched: {bob_cash}");
    assert_eq!(
        bob_equity,
        dec!(1_000_000),
        "bob equity untouched: {bob_equity}"
    );

    drop(ws_alice);
    drop(ws_bob);
    stop.send(()).unwrap();
    handle.await.unwrap().unwrap();
}
