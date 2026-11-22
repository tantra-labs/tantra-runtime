//! Integration tests for the multiplexed `GET /api/v2/stream` socket (design 0006 R2d) and the
//! `/ws/live` keyless-order regression (a constant ledger key collapsed every keyless order).

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tantra_core::{Currency, Exchange, InstrumentId, InstrumentKind};
use tantra_runtime_api_rest::market_feed::wire::{
    BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use tantra_runtime_api_rest::{ApiConfig, AppState, serve, serve_with_config};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn state() -> AppState {
    let id = InstrumentId::new(Exchange::Nse, "X");
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::from_u64(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
}

/// A keyless resting limit buy.
fn keyless_limit(op_key: &str) -> Value {
    let mut v = json!({
        "order": {
            "instrument_id": {"symbol": "X", "exchange": "NSE"},
            "side": "buy",
            "qty": "25",
            "order_type": "limit",
            "price": "90",
        },
    });
    v[op_key] = json!("place_order");
    v
}

struct Server {
    addr: std::net::SocketAddr,
    stop: oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<std::io::Result<()>>,
    _dir: Option<tempfile::TempDir>,
}

impl Server {
    async fn plain() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let handle = tokio::spawn(serve(listener, state(), async move {
            let _ = stopped.await;
        }));
        Self {
            addr,
            stop,
            handle,
            _dir: None,
        }
    }

    async fn with_auth() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let hex: String = Sha256::digest(b"secret-r")
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let toml = format!(
            "[[tokens]]\ntoken_id = \"r\"\ntoken_hash = \"sha256:{hex}\"\nscopes = [\"read\"]\nlabel = \"t\"\ncreated_at = \"2026-01-01T00:00:00Z\"\n"
        );
        let path = dir.path().join("tokens.toml");
        std::fs::write(&path, toml).unwrap();
        let mut config = ApiConfig::default();
        config.auth.tokens_file = Some(path);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            serve_with_config(listener, state(), &config, async move {
                let _ = stopped.await;
            })
            .await
        });
        Self {
            addr,
            stop,
            handle,
            _dir: Some(dir),
        }
    }

    async fn connect(
        &self,
        path: &str,
        token: Option<&str>,
    ) -> Result<
        (
            Client,
            tokio_tungstenite::tungstenite::http::Response<Option<Vec<u8>>>,
        ),
        WsError,
    > {
        let mut req = format!("ws://{}{path}", self.addr)
            .into_client_request()
            .unwrap();
        if let Some(t) = token {
            req.headers_mut()
                .insert("authorization", format!("Bearer {t}").parse().unwrap());
        }
        tokio_tungstenite::connect_async(req).await
    }

    async fn shutdown(self) {
        self.stop.send(()).unwrap();
        self.handle.await.unwrap().unwrap();
    }
}

async fn send_json(ws: &mut Client, v: &Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

/// Reads frames until one matches (5s cap), returning every frame seen including the match.
async fn drain_until(ws: &mut Client, mut done: impl FnMut(&Value) -> bool) -> Vec<Value> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(m))) => {
                let Ok(v) = m
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

fn find(frames: &[Value], pred: impl Fn(&Value) -> bool) -> &Value {
    frames
        .iter()
        .find(|v| pred(v))
        .unwrap_or_else(|| panic!("frame not found in {frames:#?}"))
}

#[tokio::test]
async fn place_order_returns_result_and_streams_orders_event() {
    let server = Server::plain().await;
    let (mut ws, _) = server.connect("/api/v2/stream", None).await.unwrap();

    send_json(&mut ws, &json!({"op": "subscribe", "channel": "orders"})).await;
    let frames = drain_until(&mut ws, |v| v["type"] == "ack").await;
    let ack = find(&frames, |v| v["type"] == "ack");
    assert_eq!(ack["channel"], "orders");
    assert_eq!(ack["op"], "subscribe");

    let mut place = keyless_limit("op");
    place["request_id"] = json!("r1");
    send_json(&mut ws, &place).await;
    let frames = drain_until(&mut ws, |v| v["type"] == "result").await;
    let result = find(&frames, |v| v["type"] == "result");
    assert_eq!(result["status"], 200, "{result}");
    assert_eq!(result["op"], "place_order");
    assert_eq!(result["request_id"], "r1");

    let mut all = frames.clone();
    if !all
        .iter()
        .any(|v| v["type"] == "event" && v["channel"] == "orders")
    {
        all.extend(
            drain_until(&mut ws, |v| {
                v["type"] == "event" && v["channel"] == "orders"
            })
            .await,
        );
    }
    assert!(
        all.iter()
            .any(|v| v["type"] == "event" && v["channel"] == "orders"),
        "no orders event: {all:#?}"
    );

    drop(ws);
    server.shutdown().await;
}

async fn order_ids(ws: &mut Client, place: &Value, result_type: &str) -> Vec<String> {
    let mut ids = Vec::new();
    for _ in 0..2 {
        send_json(ws, place).await;
        let frames = drain_until(ws, |v| v["type"] == result_type).await;
        let r = find(&frames, |v| v["type"] == result_type);
        assert_eq!(r["status"], 200, "{r}");
        ids.push(
            r["data"]["data"]["order_id"]
                .as_str()
                .expect("order_id")
                .to_string(),
        );
    }
    ids
}

#[tokio::test]
async fn keyless_orders_get_distinct_ids_on_stream_and_ws_live() {
    let server = Server::plain().await;

    let (mut ws, _) = server.connect("/api/v2/stream", None).await.unwrap();
    let ids = order_ids(&mut ws, &keyless_limit("op"), "result").await;
    assert_ne!(ids[0], ids[1], "stream keyless orders collapsed");

    let (mut live, _) = server.connect("/api/v2/ws/live", None).await.unwrap();
    let mut place = keyless_limit("action");
    place["action"] = json!("place_order");
    let ids = order_ids(&mut live, &place, "PlaceResult").await;
    assert_ne!(ids[0], ids[1], "ws/live keyless orders collapsed");

    drop(ws);
    drop(live);
    server.shutdown().await;
}

#[tokio::test]
async fn read_only_token_connects_but_commands_are_forbidden() {
    let server = Server::with_auth().await;
    let (mut ws, _) = server
        .connect("/api/v2/stream", Some("secret-r"))
        .await
        .unwrap();
    send_json(&mut ws, &keyless_limit("op")).await;
    let frames = drain_until(&mut ws, |v| v["type"] == "error").await;
    assert_eq!(find(&frames, |v| v["type"] == "error")["code"], "forbidden");

    // The socket stays open.
    send_json(&mut ws, &json!({"op": "ping"})).await;
    let frames = drain_until(&mut ws, |v| v["type"] == "pong").await;
    find(&frames, |v| v["type"] == "pong");

    drop(ws);
    server.shutdown().await;
}

#[tokio::test]
async fn upgrade_without_token_is_401() {
    let server = Server::with_auth().await;
    match server.connect("/api/v2/stream", None).await {
        Err(WsError::Http(resp)) => assert_eq!(resp.status().as_u16(), 401),
        other => panic!("expected HTTP 401, got {:?}", other.map(|_| ())),
    }
    server.shutdown().await;
}

#[tokio::test]
async fn unknown_channel_and_id_cap_are_errors() {
    let server = Server::plain().await;
    let (mut ws, _) = server.connect("/api/v2/stream", None).await.unwrap();

    send_json(&mut ws, &json!({"op": "subscribe", "channel": "nope"})).await;
    let frames = drain_until(&mut ws, |v| v["type"] == "error").await;
    assert_eq!(
        find(&frames, |v| v["type"] == "error")["code"],
        "validation_invalid_request"
    );

    let ids: Vec<String> = (0..257).map(|i| format!("S{i}")).collect();
    send_json(
        &mut ws,
        &json!({"op": "subscribe", "channel": "depth", "ids": ids}),
    )
    .await;
    let frames = drain_until(&mut ws, |v| v["type"] == "error").await;
    assert_eq!(
        find(&frames, |v| v["type"] == "error")["code"],
        "rate_limited"
    );

    send_json(&mut ws, &json!({"op": "not json op"})).await;
    let frames = drain_until(&mut ws, |v| v["type"] == "error").await;
    find(&frames, |v| v["type"] == "error");

    drop(ws);
    server.shutdown().await;
}

#[tokio::test]
async fn ping_answers_pong() {
    let server = Server::plain().await;
    let (mut ws, _) = server.connect("/api/v2/stream", None).await.unwrap();
    send_json(&mut ws, &json!({"op": "ping"})).await;
    let frames = drain_until(&mut ws, |v| v["type"] == "pong").await;
    find(&frames, |v| v["type"] == "pong");
    drop(ws);
    server.shutdown().await;
}

#[tokio::test]
async fn quotes_without_feed_reports_unavailable_once() {
    let server = Server::plain().await;
    let (mut ws, _) = server.connect("/api/v2/stream", None).await.unwrap();
    send_json(
        &mut ws,
        &json!({"op": "subscribe", "channel": "quotes", "ids": ["NSE:X"]}),
    )
    .await;
    let frames = drain_until(&mut ws, |v| v["type"] == "ack").await;
    assert_eq!(
        find(&frames, |v| v["type"] == "error")["code"],
        "service_unavailable"
    );
    assert_eq!(find(&frames, |v| v["type"] == "ack")["ids"], json!(["X"]));
    drop(ws);
    server.shutdown().await;
}

#[tokio::test]
async fn deprecated_ws_live_carries_deprecation_headers() {
    let server = Server::plain().await;
    let (ws, resp) = server.connect("/api/v2/ws/live", None).await.unwrap();
    assert_eq!(resp.status().as_u16(), 101);
    assert!(
        resp.headers().contains_key("deprecation"),
        "{:?}",
        resp.headers()
    );
    let link = resp
        .headers()
        .get("link")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(link.contains("/stream"), "link: {link}");
    drop(ws);
    server.shutdown().await;
}
