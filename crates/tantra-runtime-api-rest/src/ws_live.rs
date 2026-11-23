//! Zero-polling WebSocket gateway for the paper exchange's event stream (`GET /ws/live`,
//! plan 0005 D3, T14.7).
//!
//! Every order lifecycle event, fill, depth update and account snapshot the [`crate::exec`]
//! gateway publishes is fanned out to every connected client (one paper account). Clients can
//! also place and cancel paper orders over the same socket, and subscribe to a symbol set that
//! filters only the depth/position/equity snapshots — order events always stream.

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tantra_runtime_api::OrdersRequest;
use tracing::debug;

use crate::AppState;
use crate::caller::Caller;

/// Largest response body buffered for a result frame.
const MAX_RESULT_BYTES: usize = 1024 * 1024;

/// Upgrades an HTTP connection to a WebSocket streaming the paper exchange's events.
pub async fn ws_live_handler(
    caller: Caller,
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_live_socket(socket, state, caller.0))
}

/// One client command on the `/ws/live` socket, tagged by `action`.
#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum WsLiveCommand {
    /// Connection liveness; replied with a `pong` payload.
    Ping,
    /// Restrict depth/position/equity snapshots to these symbols (empty clears the filter).
    Subscribe {
        #[serde(default)]
        symbols: Option<Vec<String>>,
    },
    /// Lift the symbol filter for these symbols.
    Unsubscribe {
        #[serde(default)]
        symbols: Option<Vec<String>>,
    },
    /// Place a paper order; `order` is the canonical `OrdersRequest` body, deserialized
    /// separately so its `deny_unknown_fields` cannot fight the internally-tagged command.
    PlaceOrder {
        /// The raw `OrdersRequest` body.
        order: Value,
    },
    /// Cancel the paper order `order_id`.
    CancelOrder {
        /// Ledger order id to cancel.
        order_id: String,
    },
    /// Close the socket.
    Close,
}

/// Handles one `/ws/live` connection: the client's commands and the broadcast event stream.
async fn handle_live_socket(socket: WebSocket, state: Arc<AppState>, owner: String) {
    let (mut sender, mut receiver) = socket.split();
    let mut events_rx = state.exec.subscribe_events();
    let mut subscribed: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            biased;

            // Client messages: subscriptions, pings, place/cancel, disconnects.
            client_msg = receiver.next() => {
                let Some(msg) = client_msg else {
                    debug!("WebSocket client disconnected");
                    break;
                };
                let Ok(msg) = msg else {
                    debug!("WebSocket connection closed");
                    break;
                };
                if handle_client_message(&state, &owner, &mut sender, &mut subscribed, msg).await {
                    break;
                }
            }

            // The paper exchange's event stream, forwarded as-is.
            event = events_rx.recv() => {
                match event {
                    Ok(payload) => {
                        // Order lifecycle and fills are private to the placing caller (gap 0013);
                        // account-wide snapshots: docs/gaps/0017-per-caller-paper-accounts.md.
                        let private = matches!(
                            payload.get("type").and_then(Value::as_str),
                            Some("OrderAccepted" | "Cancelled" | "Rejected" | "StopTriggered" | "Fill" | "PartialFill")
                        );
                        if (!private || state.exec.event_owned_by(&state.orders, &payload, &owner))
                            && should_stream(&payload, &subscribed)
                            && sender.send(Message::Text(payload.to_string())).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // A lagged receiver skipped payloads; keep streaming from here.
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

/// Handles one inbound frame, replying on `sender`. Returns `true` when the connection should
/// close (a close frame, or a reply that could not be sent).
async fn handle_client_message(
    state: &Arc<AppState>,
    owner: &str,
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    subscribed: &mut HashSet<String>,
    msg: Message,
) -> bool {
    match msg {
        Message::Text(text) => dispatch_command(state, owner, sender, subscribed, &text).await,
        Message::Ping(bytes) => sender.send(Message::Pong(bytes.clone())).await.is_err(),
        Message::Close(_) => {
            debug!("WebSocket connection closed");
            true
        }
        _ => false,
    }
}

/// Handles one inbound text command, replying on `sender`. Returns `true` when the connection
/// should close (the command was `close`, or the reply could not be sent).
async fn dispatch_command(
    state: &Arc<AppState>,
    owner: &str,
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    subscribed: &mut HashSet<String>,
    text: &str,
) -> bool {
    let command = match serde_json::from_str::<WsLiveCommand>(text) {
        Ok(command) => command,
        Err(err) => {
            let reply = json!({"type": "error", "message": err.to_string()});
            return sender.send(Message::Text(reply.to_string())).await.is_err();
        }
    };
    match command {
        WsLiveCommand::Ping => {
            let reply = json!({"type": "pong", "timestamp": chrono::Utc::now().timestamp_millis()});
            sender.send(Message::Text(reply.to_string())).await.is_err()
        }
        WsLiveCommand::Subscribe { symbols } => {
            if let Some(symbols) = symbols {
                subscribed.extend(symbols);
            }
            false
        }
        WsLiveCommand::Unsubscribe { symbols } => {
            if let Some(symbols) = symbols {
                for symbol in symbols {
                    subscribed.remove(&symbol);
                }
            }
            false
        }
        WsLiveCommand::PlaceOrder { order } => {
            let order: OrdersRequest = match serde_json::from_value(order) {
                Ok(order) => order,
                Err(err) => {
                    let reply =
                        json!({"type": "error", "message": format!("invalid order: {err}")});
                    return sender.send(Message::Text(reply.to_string())).await.is_err();
                }
            };
            let client_order_id = order.client_order_id.clone();
            let response = crate::orders::post_orders(
                State(state.clone()),
                crate::ApiJson(order),
                // A fresh key per keyless order; a constant would collapse them into one.
                uuid::Uuid::new_v4().to_string(),
                owner,
            )
            .await;
            let status = response.status().as_u16();
            let body = response_body(response).await;
            let mut reply = json!({"type": "PlaceResult", "status": status, "data": body});
            if let Some(client_order_id) = client_order_id {
                if let Some(broker_id) = state.exec.broker_id_for(&client_order_id) {
                    reply["broker_order_id"] = json!(broker_id);
                }
            }
            sender.send(Message::Text(reply.to_string())).await.is_err()
        }
        WsLiveCommand::CancelOrder { order_id } => {
            let response = crate::orders::delete_order(
                State(state.clone()),
                Caller(owner.to_string()),
                axum::extract::Path(order_id),
            )
            .await;
            let status = response.status().as_u16();
            let body = response_body(response).await;
            let reply = json!({"type": "CancelResult", "status": status, "data": body});
            sender.send(Message::Text(reply.to_string())).await.is_err()
        }
        WsLiveCommand::Close => true,
    }
}

/// The response envelope body as JSON (or `null` when it is not JSON).
async fn response_body(response: Response) -> Value {
    axum::body::to_bytes(response.into_body(), MAX_RESULT_BYTES)
        .await
        .map(|bytes| serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null))
        .unwrap_or(Value::Null)
}

/// Whether a published payload is delivered to this client: order lifecycle and fill events
/// always stream; depth/position/equity snapshots are filtered by the client's subscription set
/// when one is non-empty. Equity is account-global and has no symbol, so a symbol-filtered
/// client does not receive it.
fn should_stream(payload: &Value, subscribed: &HashSet<String>) -> bool {
    let Some(event_type) = payload.get("type").and_then(Value::as_str) else {
        return true;
    };
    if !matches!(
        event_type,
        "DepthChanged" | "PositionSnapshot" | "EquitySnapshot"
    ) {
        return true;
    }
    if subscribed.is_empty() {
        return true;
    }
    match event_type {
        "DepthChanged" => payload
            .pointer("/instrument/symbol")
            .and_then(Value::as_str)
            .map(|symbol| subscribed.contains(symbol))
            .unwrap_or(false),
        "PositionSnapshot" => payload
            .get("positions")
            .and_then(Value::as_object)
            .map(|positions| positions.keys().any(|symbol| subscribed.contains(symbol)))
            .unwrap_or(false),
        _ => false,
    }
}
