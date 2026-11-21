//! `/ws/chat` per-turn `model` override + multimodal image attachments (gap 0009 — T17.2).
//!
//! Boots the real WebSocket handler over an in-memory [`AppState`] with a fake provider
//! registry, exactly like `tests/ws_tests.rs` boots `/ws/market`, and proves:
//! 1. wire round-trip — the legacy `{ "text": ... }` shape still parses (`model`/`attachments`
//!    default to `None`), and the new shape with `model` + `attachments` round-trips;
//! 2. per-turn model routing — an override differing from the server default routes the turn
//!    to the registered override provider, an unavailable model gets the explicit capability
//!    notice, and the server-default path is untouched;
//! 3. multimodal — image attachments reach the LLM boundary for a vision-capable provider,
//!    while a text-only provider answers the honest capability notice.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tantra_agent_llm::ScriptedFakeLlm;
use tantra_runtime_api_rest::{
    AppState, ChatAttachmentInput, ChatClientMessage, ChatRuntime, LlmProviderRegistry,
    chat_ws_handler,
};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

type WsStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const PNG_BASE64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgYGBgAAAABQABh6FO1AAAAABJRU5ErkJggg==";

/// A base64 PNG attachment payload as the web client sends it.
fn png_attachment_json() -> Value {
    serde_json::json!({
        "mime_type": "image/png",
        "data": PNG_BASE64,
    })
}

/// Reads the next JSON event frame from the socket.
async fn next_event(ws: &mut WsStream) -> Value {
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout waiting for ws event")
        .expect("ws stream closed")
        .expect("ws error");
    serde_json::from_str(msg.to_text().expect("text frame")).expect("valid JSON event")
}

/// Drains events until `turn_ended`, returning the streamed assistant text.
async fn read_turn(ws: &mut WsStream) -> (String, Value) {
    let mut text = String::new();
    loop {
        let event = next_event(ws).await;
        match event.get("event").and_then(|v| v.as_str()) {
            Some("text_delta") => {
                if let Some(delta) = event.get("text").and_then(|v| v.as_str()) {
                    text.push_str(delta);
                }
            }
            Some("turn_ended") => return (text, event),
            _ => {}
        }
    }
}

/// Boots an in-memory `/ws/chat` server with the given provider registry and connects to it.
async fn boot_chat_ws(registry: LlmProviderRegistry) -> WsStream {
    let chat = ChatRuntime::with_provider_registry(registry).expect("chat runtime");
    let state = AppState::default().with_chat_agent(chat);
    let router = axum::Router::new()
        .route("/ws/chat", axum::routing::get(chat_ws_handler))
        .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let url = format!("ws://{}/ws/chat", addr);
    let (ws, _) = connect_async(&url).await.expect("connect ws");
    ws
}

/// A registry with a text-only default (`default-llm`), a vision-capable override
/// (`vision-model`) and a text-only override (`textonly-model`).
fn test_registry(
    default: Arc<ScriptedFakeLlm>,
    vision: Arc<ScriptedFakeLlm>,
    text_only: Arc<ScriptedFakeLlm>,
) -> LlmProviderRegistry {
    let mut overrides: HashMap<String, Arc<dyn tantra_agent_llm::LlmProvider>> = HashMap::new();
    overrides.insert("vision-model".to_string(), vision.clone());
    overrides.insert("textonly-model".to_string(), text_only.clone());
    LlmProviderRegistry::new(default, overrides)
}

// ---------------------------------------------------------------------------
// 1. Wire round-trip
// ---------------------------------------------------------------------------

#[test]
fn test_chat_client_message_wire_round_trip() {
    // Legacy shape: `{ "text": ... }` keeps parsing with model/attachments defaulting to None.
    let legacy: ChatClientMessage =
        serde_json::from_str(r#"{ "text": "hello" }"#).expect("legacy shape parses");
    assert_eq!(legacy.text, "hello");
    assert!(legacy.model.is_none());
    assert!(legacy.attachments.is_none());

    // New shape: model + base64 image attachments round-trip.
    let new_shape = serde_json::json!({
        "text": "analyze this chart",
        "model": "vision-model",
        "attachments": [png_attachment_json()],
    });
    let parsed: ChatClientMessage = serde_json::from_value(new_shape).expect("new shape parses");
    assert_eq!(parsed.text, "analyze this chart");
    assert_eq!(parsed.model.as_deref(), Some("vision-model"));
    let atts = parsed.attachments.expect("attachments present");
    assert_eq!(atts.len(), 1);
    assert_eq!(atts[0].mime_type, "image/png");
    assert_eq!(atts[0].data, PNG_BASE64);
}

#[test]
fn test_chat_attachment_input_keeps_base64_safe() {
    // The payload bytes travel through the pipeline unmodified (never decoded/re-encoded).
    let input: ChatAttachmentInput = serde_json::from_value(png_attachment_json()).unwrap();
    assert_eq!(input.data, PNG_BASE64);
    assert_eq!(input.mime_type, "image/png");
}

// ---------------------------------------------------------------------------
// 2. Per-turn model routing over the WebSocket
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ws_chat_default_path_serves_default_provider() {
    let default = Arc::new(ScriptedFakeLlm::new("default-llm"));
    default.push_text_response("default reply");
    let vision = Arc::new(ScriptedFakeLlm::new("vision-model").with_vision());
    let text_only = Arc::new(ScriptedFakeLlm::new("textonly-model"));
    let registry = test_registry(default.clone(), vision, text_only);

    let mut ws = boot_chat_ws(registry).await;

    // SessionOpened reports the server-default model.
    let opened = next_event(&mut ws).await;
    assert_eq!(
        opened.get("event").and_then(|v| v.as_str()),
        Some("session_opened")
    );
    assert_eq!(
        opened.get("model").and_then(|v| v.as_str()),
        Some("default-llm")
    );

    // Legacy `{ "text": ... }` turn routes to the default provider with a byte-identical reply.
    ws.send(Message::Text(r#"{ "text": "hi" }"#.to_string()))
        .await
        .unwrap();
    let (text, ended) = read_turn(&mut ws).await;
    assert_eq!(text, "default reply");
    assert_eq!(
        ended
            .get("outcome")
            .and_then(|v| v.get("outcome"))
            .and_then(|v| v.as_str()),
        Some("completed")
    );
    assert_eq!(default.served_requests().len(), 1);
}

#[tokio::test]
async fn test_ws_chat_model_override_routes_to_override_provider() {
    let default = Arc::new(ScriptedFakeLlm::new("default-llm"));
    let vision = Arc::new(ScriptedFakeLlm::new("vision-model").with_vision());
    vision.push_text_response("vision reply");
    let text_only = Arc::new(ScriptedFakeLlm::new("textonly-model"));
    let registry = test_registry(default.clone(), vision.clone(), text_only);

    let mut ws = boot_chat_ws(registry).await;
    let _ = next_event(&mut ws).await; // SessionOpened

    ws.send(Message::Text(
        serde_json::json!({ "text": "analyze", "model": "vision-model" }).to_string(),
    ))
    .await
    .unwrap();

    // The client is told the model switched, then the override provider answers.
    let switched = next_event(&mut ws).await;
    assert_eq!(
        switched.get("event").and_then(|v| v.as_str()),
        Some("provider_changed")
    );
    assert_eq!(
        switched.get("model").and_then(|v| v.as_str()),
        Some("vision-model")
    );

    let (text, _ended) = read_turn(&mut ws).await;
    assert_eq!(text, "vision reply");

    // The override provider served the turn; the default provider never did.
    assert_eq!(vision.served_requests().len(), 1);
    assert!(default.served_requests().is_empty());
}

#[tokio::test]
async fn test_ws_chat_unavailable_model_returns_capability_notice() {
    let default = Arc::new(ScriptedFakeLlm::new("default-llm"));
    let vision = Arc::new(ScriptedFakeLlm::new("vision-model").with_vision());
    let text_only = Arc::new(ScriptedFakeLlm::new("textonly-model"));
    let registry = test_registry(default.clone(), vision, text_only);

    let mut ws = boot_chat_ws(registry).await;
    let _ = next_event(&mut ws).await; // SessionOpened

    ws.send(Message::Text(
        serde_json::json!({ "text": "x", "model": "no-such-model" }).to_string(),
    ))
    .await
    .unwrap();

    let (text, ended) = read_turn(&mut ws).await;
    assert!(text.contains("no-such-model"));
    assert!(text.contains("is not configured"));
    assert_eq!(
        ended
            .get("outcome")
            .and_then(|v| v.get("outcome"))
            .and_then(|v| v.as_str()),
        Some("failed")
    );
    // No provider was invoked: the default provider is untouched.
    assert!(default.served_requests().is_empty());
}

// ---------------------------------------------------------------------------
// 3. Multimodal image attachments over the WebSocket
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ws_chat_image_attachment_flows_to_vision_provider() {
    let default = Arc::new(ScriptedFakeLlm::new("default-llm"));
    let vision = Arc::new(ScriptedFakeLlm::new("vision-model").with_vision());
    vision.push_text_response("I see the chart crop.");
    let text_only = Arc::new(ScriptedFakeLlm::new("textonly-model"));
    let registry = test_registry(default.clone(), vision.clone(), text_only);

    let mut ws = boot_chat_ws(registry).await;
    let _ = next_event(&mut ws).await; // SessionOpened

    ws.send(Message::Text(
        serde_json::json!({
            "text": "analyze this chart",
            "model": "vision-model",
            "attachments": [png_attachment_json()],
        })
        .to_string(),
    ))
    .await
    .unwrap();

    let (text, _ended) = read_turn(&mut ws).await;
    assert_eq!(text, "I see the chart crop.");

    // The vision provider received exactly one request carrying the base64 image part.
    let served = vision.served_requests();
    assert_eq!(served.len(), 1);
    assert!(served[0].has_image_parts());
    match &served[0].messages[0].parts[0] {
        tantra_agent_llm::MessagePart::Image { mime_type, data } => {
            assert_eq!(mime_type, "image/png");
            assert_eq!(data, PNG_BASE64);
        }
        other => panic!("expected an image part, got {other:?}"),
    }
}

#[tokio::test]
async fn test_ws_chat_text_only_model_returns_capability_notice_for_image() {
    let default = Arc::new(ScriptedFakeLlm::new("default-llm"));
    let vision = Arc::new(ScriptedFakeLlm::new("vision-model").with_vision());
    let text_only = Arc::new(ScriptedFakeLlm::new("textonly-model"));
    let registry = test_registry(default.clone(), vision, text_only.clone());

    let mut ws = boot_chat_ws(registry).await;
    let _ = next_event(&mut ws).await; // SessionOpened

    ws.send(Message::Text(
        serde_json::json!({
            "text": "read this chart",
            "model": "textonly-model",
            "attachments": [png_attachment_json()],
        })
        .to_string(),
    ))
    .await
    .unwrap();

    let (text, ended) = read_turn(&mut ws).await;
    assert!(text.contains("textonly-model does not support image attachments"));
    assert_eq!(
        ended
            .get("outcome")
            .and_then(|v| v.get("outcome"))
            .and_then(|v| v.as_str()),
        Some("failed")
    );
    // The text-only provider was never invoked (the turn gates before the LLM boundary).
    assert!(text_only.served_requests().is_empty());
}

#[tokio::test]
async fn test_ws_chat_unsupported_attachment_mime_returns_capability_notice() {
    let default = Arc::new(ScriptedFakeLlm::new("default-llm"));
    let vision = Arc::new(ScriptedFakeLlm::new("vision-model").with_vision());
    let text_only = Arc::new(ScriptedFakeLlm::new("textonly-model"));
    let registry = test_registry(default.clone(), vision, text_only);

    let mut ws = boot_chat_ws(registry).await;
    let _ = next_event(&mut ws).await; // SessionOpened

    ws.send(Message::Text(
        serde_json::json!({
            "text": "read this",
            "attachments": [{ "mime_type": "application/pdf", "data": "JVBERi0=" }],
        })
        .to_string(),
    ))
    .await
    .unwrap();

    let (text, _ended) = read_turn(&mut ws).await;
    assert!(text.contains("unsupported attachment MIME type"));
    assert!(text.contains("application/pdf"));
    // No provider was invoked.
    assert!(default.served_requests().is_empty());
}
