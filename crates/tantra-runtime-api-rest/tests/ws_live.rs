//! Integration tests for the `/ws/live` paper-exchange event stream (plan 0005 D3, T14.7): the
//! server pushes the paper exchange's fills, order lifecycle, depth, position and equity events
//! to every connected client and accepts paper-mode place/cancel requests over the socket.
//!
//! The harness mirrors `tests/ws.rs` (serve on an ephemeral port, `tokio_tungstenite` client)
//! with the D1-style state helpers from `tests/orders_exec.rs`; a kept `AppState` clone drives
//! fills via `advance_market`, which shares the `Arc` handles with the served state. Events are
//! asynchronous, so every assertion drains the socket and checks membership rather than exact
//! first-message order.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tantra_core::{Currency, Exchange, InstrumentId, InstrumentKind};
use tantra_runtime_api_rest::market_feed::wire::{
    BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use tantra_runtime_api_rest::{AppState, serve};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// A connected `/ws/live` client stream.
type LiveStream = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

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

/// A resting limit buy on the paper exchange via the socket.
fn limit_buy() -> serde_json::Value {
    serde_json::json!({
        "action": "place_order",
        "order": {
            "instrument_id": {"symbol": "X", "exchange": "NSE"},
            "side": "buy",
            "qty": "25",
            "order_type": "limit",
            "price": "90",
            "client_order_id": "w1",
        },
    })
}

async fn ws_handshake(addr: std::net::SocketAddr, path: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    let status_line = response.lines().next().expect("response status line");
    status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status")
}

async fn send_json(ws: &mut LiveStream, payload: &serde_json::Value) {
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        payload.to_string(),
    ))
    .await
    .unwrap();
}

/// Drains text frames until `done` matches one (up to 5s), returning every JSON frame seen,
/// including the matching one.
async fn drain_until<F>(ws: &mut LiveStream, mut done: F) -> Vec<Value>
where
    F: FnMut(&Value) -> bool,
{
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(message))) => {
                let Ok(text) = message.to_text() else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<Value>(text) else {
                    continue;
                };
                if done(&value) {
                    out.push(value);
                    break;
                }
                out.push(value);
            }
            _ => break,
        }
    }
    out
}

#[tokio::test]
async fn ws_live_endpoint_responds_with_101() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, AppState::default(), async move {
        let _ = stopped.await;
    }));

    let status = ws_handshake(addr, "/ws/live").await;
    assert_eq!(status, 101);

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn r4_client_receives_fill_and_depth_changed_for_its_own_order() {
    let st = state();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, st.clone(), async move {
        let _ = stopped.await;
    }));

    let (mut ws_stream, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws/live"))
        .await
        .unwrap();

    // A resting limit buy is accepted by the paper exchange (no depth needed to rest).
    send_json(&mut ws_stream, &limit_buy()).await;
    let msgs = drain_until(&mut ws_stream, |m| m["type"] == "PlaceResult").await;
    let placed = msgs
        .iter()
        .find(|m| m["type"] == "PlaceResult")
        .expect("place result");
    assert_eq!(placed["status"], 200, "place result: {placed}");

    // A bar crosses the limit (close 85 <= limit 90): the resting buy fills, the order
    // acceptance streams, and the depth update streams. Order events and the snapshots
    // published by the drain are buffered on the broadcast and delivered in order.
    st.advance_market(&[bar(1_000, 85.0)]).unwrap();

    let mut after = Vec::new();
    let mut saw_fill = false;
    let mut saw_depth = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !(saw_fill && saw_depth) && tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, ws_stream.next()).await {
            Ok(Some(Ok(message))) => {
                if let Ok(text) = message.to_text() {
                    if let Ok(value) = serde_json::from_str::<Value>(text) {
                        match value["type"].as_str() {
                            Some("Fill" | "PartialFill") => saw_fill = true,
                            Some("DepthChanged") => saw_depth = true,
                            _ => {}
                        }
                        after.push(value);
                    }
                }
            }
            _ => break,
        }
    }

    assert!(
        saw_fill,
        "no fill event streamed after the bar; saw: {after:#?}"
    );
    assert!(
        saw_depth,
        "no DepthChanged streamed after the bar; saw: {after:#?}"
    );
    assert!(
        after.iter().any(|m| m["type"] == "OrderAccepted"),
        "expected the OrderAccepted event to stream, saw: {after:#?}"
    );

    let fill = after
        .iter()
        .find(|m| m["type"] == "Fill")
        .or_else(|| after.iter().find(|m| m["type"] == "PartialFill"))
        .expect("fill event");
    // The fill embeds the canonical `FillReport` DTO; its `OrderSide` serde spells UPPERCASE.
    assert_eq!(fill["fill"]["side"], "BUY", "fill event: {fill}");
    assert_eq!(
        fill["fill"]["instrument"]["symbol"], "X",
        "fill event: {fill}"
    );

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn place_order_refusal_streams_a_reject_result() {
    let st = state();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, st.clone(), async move {
        let _ = stopped.await;
    }));

    let (mut ws_stream, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws/live"))
        .await
        .unwrap();

    // A market order with no feed depth is refused by the paper exchange (`no market price`);
    // the refusal surfaces as the rejected ledger row in the PlaceResult envelope.
    send_json(
        &mut ws_stream,
        &serde_json::json!({
            "action": "place_order",
            "order": {
                "instrument_id": {"symbol": "X", "exchange": "NSE"},
                "side": "buy",
                "qty": "25",
                "client_order_id": "w1",
            },
        }),
    )
    .await;

    let msgs = drain_until(&mut ws_stream, |m| m["type"] == "PlaceResult").await;
    let placed = msgs
        .iter()
        .find(|m| m["type"] == "PlaceResult")
        .expect("place result");
    assert_eq!(placed["status"], 200, "place result: {placed}");
    // The envelope nests the ledger row under `data.data`.
    assert_eq!(
        placed["data"]["data"]["status"], "rejected",
        "place result: {placed}"
    );
    assert_eq!(
        placed["data"]["data"]["reject_reason"]["code"], "conflict",
        "place result: {placed}"
    );

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancel_order_action_cancels_a_resting_limit() {
    let st = state();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(listener, st.clone(), async move {
        let _ = stopped.await;
    }));

    let (mut ws_stream, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws/live"))
        .await
        .unwrap();

    // Place a resting limit buy.
    send_json(&mut ws_stream, &limit_buy()).await;
    let msgs = drain_until(&mut ws_stream, |m| m["type"] == "PlaceResult").await;
    let placed = msgs
        .iter()
        .find(|m| m["type"] == "PlaceResult")
        .expect("place result");
    assert_eq!(placed["status"], 200, "place result: {placed}");

    // Cancel it over the socket: the cancel result answers, and the exchange's cancellation
    // streams as a `Cancelled` event.
    send_json(
        &mut ws_stream,
        &serde_json::json!({"action": "cancel_order", "order_id": "w1"}),
    )
    .await;

    let msgs = drain_until(&mut ws_stream, |m| m["type"] == "CancelResult").await;
    let cancelled = msgs
        .iter()
        .find(|m| m["type"] == "CancelResult")
        .expect("cancel result");
    assert_eq!(cancelled["status"], 200, "cancel result: {cancelled}");

    let cancelled_events = drain_until(&mut ws_stream, |m| m["type"] == "Cancelled").await;
    assert!(
        cancelled_events.iter().any(|m| m["type"] == "Cancelled"),
        "expected a Cancelled event, saw: {cancelled_events:#?}"
    );

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}
