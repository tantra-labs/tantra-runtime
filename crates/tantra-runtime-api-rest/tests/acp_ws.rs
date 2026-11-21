//! `GET /v1/acp` — ACP agent WebSocket (agent plan 0019 Phase 2).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tantra_agent_llm::ScriptedFakeLlm;
use tantra_runtime_api_rest::{AppState, ChatRuntime, LlmProviderRegistry, acp_ws_handler};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, connect_async};

type WsStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn serve(state: AppState) -> std::net::SocketAddr {
    let router = axum::Router::new()
        .route("/v1/acp", axum::routing::get(acp_ws_handler))
        .with_state(Arc::new(state));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

async fn next_json(ws: &mut WsStream) -> Value {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("timeout")
            .expect("closed")
            .expect("ws error");
        if let Message::Text(t) = msg {
            return serde_json::from_str(&t).expect("json");
        }
    }
}

async fn call(ws: &mut WsStream, id: u64, method: &str, params: Value) -> (Value, String) {
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}).to_string(),
    ))
    .await
    .unwrap();
    let mut text = String::new();
    loop {
        let v = next_json(ws).await;
        if v.get("id") == Some(&json!(id)) {
            assert!(v.get("error").is_none(), "json-rpc error: {v}");
            return (v, text);
        }
        if v["method"] == "session/update"
            && v["params"]["update"]["sessionUpdate"] == "agent_message_chunk"
        {
            if let Some(t) = v["params"]["update"]["content"]["text"].as_str() {
                text.push_str(t);
            }
        }
    }
}

#[tokio::test]
async fn test_acp_initialize_new_session_prompt() {
    let llm = Arc::new(ScriptedFakeLlm::new("default-llm"));
    llm.push_text_response("hello from acp");
    let registry = LlmProviderRegistry::new(llm, HashMap::new());
    let chat = ChatRuntime::with_provider_registry(registry).expect("chat");
    let addr = serve(AppState::default().with_chat_agent(chat)).await;
    let (mut ws, _) = connect_async(format!("ws://{addr}/v1/acp"))
        .await
        .expect("connect");

    let (init, _) = call(
        &mut ws,
        1,
        "initialize",
        json!({"protocolVersion":1,"clientCapabilities":{}}),
    )
    .await;
    assert!(init["result"]["protocolVersion"].is_number());

    let (new, _) = call(
        &mut ws,
        2,
        "session/new",
        json!({"cwd":"/","mcpServers":[]}),
    )
    .await;
    let sid = new["result"]["sessionId"]
        .as_str()
        .expect("sessionId")
        .to_owned();

    let (done, text) = call(
        &mut ws,
        3,
        "session/prompt",
        json!({"sessionId":sid,"prompt":[{"type":"text","text":"hi"}]}),
    )
    .await;
    assert_eq!(done["result"]["stopReason"], "end_turn");
    assert!(text.contains("hello from acp"), "got: {text:?}");
}

#[tokio::test]
async fn test_acp_without_chat_returns_503() {
    let addr = serve(AppState::default()).await;
    let err = connect_async(format!("ws://{addr}/v1/acp"))
        .await
        .expect_err("handshake must fail");
    match err {
        tokio_tungstenite::tungstenite::Error::Http(resp) => {
            assert_eq!(resp.status().as_u16(), 503);
        }
        other => panic!("expected HTTP error, got {other:?}"),
    }
}
