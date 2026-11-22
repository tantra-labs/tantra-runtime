//! `GET /api/v2/stream`: one authenticated, multiplexed WebSocket with JSON channel frames
//! (design 0006 R2d), replacing `/ws`, `/ws/market` and `/ws/live`.
//!
//! Client frames are JSON objects tagged by `op` (`subscribe`, `unsubscribe`, `ping`,
//! `place_order`, `cancel_order`, `close`). Server frames are tagged by `type` (`ack`, `event`,
//! `result`, `error`, `lagged`, `pong`). Prices and quantities travel as decimal strings, never
//! floats. One `select!` loop owns the socket sink: there is no intermediate queue, so a slow
//! client backpressures into the bounded broadcast receivers, which report `lagged`.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use axum::{
    Extension,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tantra_core::Tick;
use tantra_core::rust_decimal::Decimal;
use tantra_runtime_api::OrdersRequest;
use tokio::sync::broadcast::{Receiver, error::RecvError};

use crate::auth::{AuthEnforced, TokenInfo};
use crate::caller::Caller;
use crate::route_table::{RouteEntry, RouteTable};
use crate::{AppState, RouteScope};

/// Largest inbound text frame.
const MAX_MESSAGE_BYTES: usize = 64 * 1024;
/// Most ids a socket may hold across all channels.
const MAX_IDS: usize = 256;
/// Largest response body buffered for a `result` frame.
const MAX_RESULT_BYTES: usize = 1024 * 1024;

pub(crate) fn v2_only_routes() -> axum::Router<Arc<AppState>> {
    table().finish().0
}

/// Manifest of the v2-only stream route (`:param` spelling).
pub(crate) fn v2_only_manifest() -> Vec<RouteEntry> {
    table().finish().1
}

fn table() -> RouteTable {
    RouteTable::new_v2_only().get("/stream", stream_handler, RouteScope::Required("read"))
}

/// Upgrades to the multiplexed stream socket.
async fn stream_handler(
    _caller: Caller,
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    token: Option<Extension<TokenInfo>>,
    enforced: Option<Extension<AuthEnforced>>,
) -> Response {
    // Commands need the `trade` scope whenever auth is on.
    let can_trade = enforced.is_none()
        || token
            .as_ref()
            .is_some_and(|Extension(t)| t.scopes.iter().any(|s| s == "trade"));
    ws.max_message_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| run(socket, state, can_trade))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Channel {
    Quotes,
    Depth,
    Orders,
    Fills,
    Portfolio,
}

impl Channel {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "quotes" => Self::Quotes,
            "depth" => Self::Depth,
            "orders" => Self::Orders,
            "fills" => Self::Fills,
            "portfolio" => Self::Portfolio,
            _ => return None,
        })
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Quotes => "quotes",
            Self::Depth => "depth",
            Self::Orders => "orders",
            Self::Fills => "fills",
            Self::Portfolio => "portfolio",
        }
    }

    /// Whether an empty id list means "everything" on this channel.
    fn allows_all(self) -> bool {
        matches!(self, Self::Orders | Self::Fills | Self::Portfolio)
    }
}

/// One channel's subscription: everything, and/or specific symbols.
#[derive(Debug, Default)]
struct Sub {
    all: bool,
    ids: BTreeSet<String>,
}

impl Sub {
    fn wants(&self, symbols: &[String]) -> bool {
        self.all || symbols.iter().any(|s| self.ids.contains(s))
    }
}

/// One client command, tagged by `op`.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Command {
    Subscribe {
        channel: String,
        #[serde(default)]
        ids: Vec<String>,
    },
    Unsubscribe {
        channel: String,
        #[serde(default)]
        ids: Vec<String>,
    },
    Ping,
    PlaceOrder {
        order: Value,
        #[serde(default)]
        request_id: Option<String>,
    },
    CancelOrder {
        order_id: String,
        #[serde(default)]
        request_id: Option<String>,
    },
    Close,
}

type Sink = SplitSink<WebSocket, Message>;

struct Conn {
    state: Arc<AppState>,
    can_trade: bool,
    subs: HashMap<Channel, Sub>,
    market_rx: Option<Receiver<Tick>>,
    market_warned: bool,
}

async fn send(sender: &mut Sink, frame: Value) -> bool {
    sender.send(Message::Text(frame.to_string())).await.is_err()
}

fn error_frame(code: &str, message: impl Into<String>) -> Value {
    json!({"type": "error", "code": code, "message": message.into()})
}

async fn recv_market(rx: &mut Option<Receiver<Tick>>) -> Result<Tick, RecvError> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

async fn run(socket: WebSocket, state: Arc<AppState>, can_trade: bool) {
    let (mut sender, mut receiver) = socket.split();
    let mut exec_rx = state.exec.subscribe_events();
    let mut conn = Conn {
        state,
        can_trade,
        subs: HashMap::new(),
        market_rx: None,
        market_warned: false,
    };

    loop {
        tokio::select! {
            biased;

            msg = receiver.next() => {
                let Some(Ok(msg)) = msg else { break };
                let close = match msg {
                    Message::Text(text) => conn.command(&mut sender, &text).await,
                    Message::Ping(bytes) => sender.send(Message::Pong(bytes)).await.is_err(),
                    Message::Close(_) => true,
                    _ => false,
                };
                if close {
                    break;
                }
            }

            tick = recv_market(&mut conn.market_rx) => {
                let stop = match tick {
                    Ok(tick) => conn.forward_tick(&mut sender, &tick).await,
                    Err(RecvError::Lagged(n)) => {
                        conn.lagged(&mut sender, &[Channel::Quotes, Channel::Depth], n).await
                    }
                    Err(RecvError::Closed) => {
                        conn.market_rx = None;
                        false
                    }
                };
                if stop {
                    break;
                }
            }

            event = exec_rx.recv() => {
                let stop = match event {
                    Ok(payload) => conn.forward_exec(&mut sender, &payload).await,
                    Err(RecvError::Lagged(n)) => {
                        conn.lagged(
                            &mut sender,
                            &[Channel::Orders, Channel::Fills, Channel::Depth, Channel::Portfolio],
                            n,
                        )
                        .await
                    }
                    Err(RecvError::Closed) => true,
                };
                if stop {
                    break;
                }
            }
        }
    }
}

/// `NSE:RELIANCE` becomes `RELIANCE`.
fn normalize(id: &str) -> String {
    id.rsplit(':').next().unwrap_or(id).trim().to_string()
}

impl Conn {
    /// Handles one inbound text frame; `true` closes the connection.
    async fn command(&mut self, sender: &mut Sink, text: &str) -> bool {
        let command = match serde_json::from_str::<Command>(text) {
            Ok(c) => c,
            Err(e) => {
                return send(
                    sender,
                    error_frame("validation_invalid_request", e.to_string()),
                )
                .await;
            }
        };
        match command {
            Command::Ping => send(sender, json!({"type": "pong"})).await,
            Command::Close => true,
            Command::Subscribe { channel, ids } => {
                self.subscribe(sender, "subscribe", &channel, ids, true)
                    .await
            }
            Command::Unsubscribe { channel, ids } => {
                self.subscribe(sender, "unsubscribe", &channel, ids, false)
                    .await
            }
            Command::PlaceOrder { order, request_id } => {
                if !self.can_trade {
                    return send(sender, forbidden()).await;
                }
                let order: OrdersRequest = match serde_json::from_value(order) {
                    Ok(o) => o,
                    Err(e) => {
                        return send(
                            sender,
                            error_frame(
                                "validation_invalid_request",
                                format!("invalid order: {e}"),
                            ),
                        )
                        .await;
                    }
                };
                // A fresh key per keyless order, so unkeyed orders never collapse into one.
                let key = uuid::Uuid::new_v4().to_string();
                let response = crate::orders::post_orders(
                    State(self.state.clone()),
                    crate::ApiJson(order),
                    key,
                )
                .await;
                send(
                    sender,
                    result_frame("place_order", request_id, response).await,
                )
                .await
            }
            Command::CancelOrder {
                order_id,
                request_id,
            } => {
                if !self.can_trade {
                    return send(sender, forbidden()).await;
                }
                let response = crate::orders::delete_order(
                    State(self.state.clone()),
                    axum::extract::Path(order_id),
                )
                .await;
                send(
                    sender,
                    result_frame("cancel_order", request_id, response).await,
                )
                .await
            }
        }
    }

    async fn subscribe(
        &mut self,
        sender: &mut Sink,
        op: &'static str,
        channel: &str,
        ids: Vec<String>,
        add: bool,
    ) -> bool {
        let Some(ch) = Channel::parse(channel) else {
            return send(
                sender,
                error_frame(
                    "validation_invalid_request",
                    format!("unknown channel `{channel}`"),
                ),
            )
            .await;
        };
        let ids: Vec<String> = ids.iter().map(|i| normalize(i)).collect();
        if add && ids.is_empty() && !ch.allows_all() {
            return send(
                sender,
                error_frame(
                    "validation_invalid_request",
                    format!("channel `{channel}` requires ids"),
                ),
            )
            .await;
        }
        if add {
            let held: usize = self.subs.values().map(|s| s.ids.len()).sum();
            let fresh = ids
                .iter()
                .filter(|i| !self.subs.get(&ch).is_some_and(|s| s.ids.contains(*i)))
                .collect::<BTreeSet<_>>()
                .len();
            if held + fresh > MAX_IDS {
                return send(
                    sender,
                    error_frame("rate_limited", format!("at most {MAX_IDS} ids per socket")),
                )
                .await;
            }
            let sub = self.subs.entry(ch).or_default();
            if ids.is_empty() {
                sub.all = true;
            }
            sub.ids.extend(ids);
            if matches!(ch, Channel::Quotes | Channel::Depth) && self.market_rx.is_none() {
                self.market_rx = self.state.subscribe_market_data();
                if self.market_rx.is_none() && !self.market_warned {
                    self.market_warned = true;
                    let mut frame =
                        error_frame("service_unavailable", "no market data feed is configured");
                    frame["channel"] = json!(ch.as_str());
                    if send(sender, frame).await {
                        return true;
                    }
                }
            }
        } else if ids.is_empty() {
            self.subs.remove(&ch);
        } else if let Some(sub) = self.subs.get_mut(&ch) {
            for id in &ids {
                sub.ids.remove(id);
            }
            if !sub.all && sub.ids.is_empty() {
                self.subs.remove(&ch);
            }
        }
        let current: Vec<&String> = self
            .subs
            .get(&ch)
            .map(|s| s.ids.iter().collect())
            .unwrap_or_default();
        send(
            sender,
            json!({"type": "ack", "op": op, "channel": ch.as_str(), "ids": current}),
        )
        .await
    }

    async fn lagged(&self, sender: &mut Sink, channels: &[Channel], skipped: u64) -> bool {
        for ch in channels {
            if self.subs.contains_key(ch)
                && send(
                    sender,
                    json!({"type": "lagged", "channel": ch.as_str(), "skipped": skipped}),
                )
                .await
            {
                return true;
            }
        }
        false
    }

    async fn emit(&self, sender: &mut Sink, ch: Channel, symbols: &[String], data: Value) -> bool {
        match self.subs.get(&ch) {
            Some(sub) if sub.wants(symbols) => {
                send(
                    sender,
                    json!({"type": "event", "channel": ch.as_str(), "data": data}),
                )
                .await
            }
            _ => false,
        }
    }

    async fn forward_tick(&self, sender: &mut Sink, tick: &Tick) -> bool {
        let q = quote_of(tick);
        let symbols = [q.symbol.clone()];
        let mut data = json!({
            "symbol": q.symbol,
            "price": q.price.to_string(),
            "ts_ms": q.ts_ms,
            "kind": q.kind,
        });
        if let Some(exchange) = q.exchange {
            data["exchange"] = json!(exchange);
        }
        if let Some(volume) = q.volume {
            data["volume"] = json!(volume.to_string());
        }
        if self.emit(sender, Channel::Quotes, &symbols, data).await {
            return true;
        }
        if let Tick::Depth {
            bids,
            asks,
            exchange_ts,
            ..
        } = tick
        {
            let level = |l: &tantra_core::DepthLevel| json!({"price": l.price.0.to_string(), "quantity": l.quantity.0.to_string()});
            let data = json!({
                "symbol": symbols[0],
                "bids": bids.iter().map(level).collect::<Vec<_>>(),
                "asks": asks.iter().map(level).collect::<Vec<_>>(),
                "ts_ms": exchange_ts.timestamp_millis(),
            });
            return self.emit(sender, Channel::Depth, &symbols, data).await;
        }
        false
    }

    async fn forward_exec(&self, sender: &mut Sink, payload: &Value) -> bool {
        let Some(kind) = payload.get("type").and_then(Value::as_str) else {
            return false;
        };
        let (ch, symbols) = match kind {
            "OrderAccepted" => (Channel::Orders, payload_symbols(payload)),
            // Cancel/reject/stop events carry only a broker order id, no symbol: deliver them to
            // every orders subscriber (as `/ws/live` does) so a symbol-filtered client still sees
            // its own order's lifecycle end.
            "Cancelled" | "Rejected" | "StopTriggered" => (
                Channel::Orders,
                self.subs
                    .get(&Channel::Orders)
                    .map(|sub| sub.ids.iter().cloned().collect())
                    .unwrap_or_default(),
            ),
            "Fill" | "PartialFill" => (Channel::Fills, payload_symbols(payload)),
            "DepthChanged" => (Channel::Depth, payload_symbols(payload)),
            "PositionSnapshot" => (
                Channel::Portfolio,
                payload
                    .get("positions")
                    .and_then(Value::as_object)
                    .map(|m| m.keys().cloned().collect())
                    .unwrap_or_default(),
            ),
            "EquitySnapshot" => (Channel::Portfolio, Vec::new()),
            _ => return false,
        };
        self.emit(sender, ch, &symbols, payload.clone()).await
    }
}

fn forbidden() -> Value {
    error_frame(
        "forbidden",
        "the `trade` scope is required for order commands",
    )
}

/// The reply frame for a place/cancel command: HTTP status plus the response envelope.
async fn result_frame(op: &str, request_id: Option<String>, response: Response) -> Value {
    let status = response.status().as_u16();
    let data = axum::body::to_bytes(response.into_body(), MAX_RESULT_BYTES)
        .await
        .ok()
        .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
        .unwrap_or(Value::Null);
    json!({"type": "result", "op": op, "request_id": request_id, "status": status, "data": data})
}

/// Symbols an exec payload concerns, from the first location that carries one.
fn payload_symbols(payload: &Value) -> Vec<String> {
    [
        "/instrument/symbol",
        "/fill/instrument/symbol",
        "/order/instrument/symbol",
        "/instrument_id/symbol",
    ]
    .iter()
    .find_map(|p| payload.pointer(p).and_then(Value::as_str))
    .map(|s| vec![s.to_string()])
    .unwrap_or_default()
}

struct QuoteView {
    symbol: String,
    exchange: Option<String>,
    price: Decimal,
    volume: Option<Decimal>,
    ts_ms: i64,
    kind: &'static str,
}

fn exchange_name(instrument: &tantra_core::InstrumentId) -> Option<String> {
    serde_json::to_value(instrument.exchange)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
}

fn quote_of(tick: &Tick) -> QuoteView {
    match tick {
        Tick::Ltp {
            instrument,
            price,
            volume,
            exchange_ts,
            ..
        } => QuoteView {
            symbol: instrument.symbol.clone(),
            exchange: exchange_name(instrument),
            price: price.0,
            volume: Some(volume.0),
            ts_ms: exchange_ts.timestamp_millis(),
            kind: "ltp",
        },
        Tick::Quote {
            instrument,
            bid,
            ask,
            last_price,
            bid_qty,
            ask_qty,
            exchange_ts,
            ..
        } => QuoteView {
            symbol: instrument.symbol.clone(),
            exchange: exchange_name(instrument),
            price: last_price
                .map(|p| p.0)
                .unwrap_or((bid.0 + ask.0) / Decimal::TWO),
            volume: Some(bid_qty.0 + ask_qty.0),
            ts_ms: exchange_ts.timestamp_millis(),
            kind: "quote",
        },
        Tick::Depth {
            instrument,
            bids,
            last_price,
            exchange_ts,
            ..
        } => QuoteView {
            symbol: instrument.symbol.clone(),
            exchange: exchange_name(instrument),
            price: last_price
                .map(|p| p.0)
                .or_else(|| bids.first().map(|b| b.price.0))
                .unwrap_or(Decimal::ZERO),
            volume: None,
            ts_ms: exchange_ts.timestamp_millis(),
            kind: "depth",
        },
    }
}
