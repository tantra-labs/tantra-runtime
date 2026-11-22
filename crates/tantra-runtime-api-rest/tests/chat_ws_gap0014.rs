//! `/ws/chat` structured trace streaming, fail-closed `thread_id` resumption, and
//! caller/workspace-scoped `/api/v2/chat/threads` CRUD persistence (gap 0014).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tantra_agent_chat::{CallId, InMemorySessionStore, SessionEvent, ToolOutcome, TurnId};
use tantra_agent_core::{CodeDiffHunk, DisplayBlock, SessionId};
use tantra_agent_llm::{
    ChatMessage, ChatStreamEvent, LlmToolCall, ScriptedFakeLlm, StopReason, Usage,
};
use tantra_runtime_api_rest::{
    ApiConfig, AppState, ChatRuntime, LlmProviderRegistry, api_router_with, api_router_with_config,
};
use tantra_runtime_persistence::{Database, SqlxSessionStore, migrate};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tower::util::ServiceExt;

type WsStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

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

fn auth_config_with_users() -> (ApiConfig, tempfile::TempDir) {
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
    (config, dir)
}

async fn spawn_router(router: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    addr
}

async fn connect_ws(addr: SocketAddr, path_and_query: &str, bearer: Option<&str>) -> WsStream {
    let url = format!("ws://{addr}{path_and_query}");
    let mut req = url.into_client_request().expect("valid ws request");
    if let Some(token) = bearer {
        req.headers_mut().insert(
            "authorization",
            format!("Bearer {token}").parse().expect("valid header"),
        );
    }
    let (ws, _) = connect_async(req).await.expect("connect ws");
    ws
}

async fn next_event(ws: &mut WsStream) -> Value {
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("timeout waiting for ws frame")
        .expect("ws closed unexpectedly")
        .expect("ws frame error");
    serde_json::from_str(msg.to_text().expect("text frame")).expect("valid JSON frame")
}

async fn collect_until_turn_ended(ws: &mut WsStream) -> Vec<Value> {
    let mut events = Vec::new();
    loop {
        let ev = next_event(ws).await;
        let is_end = ev.get("event").and_then(Value::as_str) == Some("turn_ended");
        events.push(ev);
        if is_end {
            return events;
        }
    }
}

async fn http_call(
    router: &Router,
    method: &str,
    uri: &str,
    bearer: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(token) = bearer {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let payload = match body {
        Some(v) => Body::from(v.to_string()),
        None => Body::empty(),
    };
    let res = router
        .clone()
        .oneshot(builder.body(payload).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, headers, json)
}

#[tokio::test]
async fn ws_chat_streams_thinking_usage_and_raw_tool_payloads() {
    let fake = Arc::new(ScriptedFakeLlm::new("trace-llm"));
    let args = json!({
        "kind": "analysis",
        "title": "NIFTY Setup",
        "symbols": ["NIFTY"]
    });
    let raw_args = args.to_string();
    fake.push_stream(vec![
        ChatStreamEvent::ThinkingDelta {
            text: "Inspecting NIFTY levels".to_string(),
        },
        ChatStreamEvent::Usage(Usage {
            input_tokens: 12,
            output_tokens: 5,
            cache_read_tokens: Some(4),
            cache_write_tokens: None,
        }),
        ChatStreamEvent::ToolCallStreaming {
            call_id: "call_1".to_string(),
            name: "present_view".to_string(),
        },
        ChatStreamEvent::ToolCallChunk {
            call_id: "call_1".to_string(),
            arguments_delta: raw_args.clone(),
        },
        ChatStreamEvent::ToolCallReady {
            call_id: "call_1".to_string(),
            name: "present_view".to_string(),
            arguments: args.clone(),
            raw_arguments: raw_args.clone(),
            is_malformed: false,
        },
        ChatStreamEvent::MessageEnd {
            stop_reason: StopReason::ToolUse,
            message: ChatMessage::assistant_with_tools(
                "",
                vec![LlmToolCall {
                    id: "call_1".to_string(),
                    name: "present_view".to_string(),
                    arguments: raw_args,
                }],
            ),
            usage: Usage {
                input_tokens: 12,
                output_tokens: 5,
                cache_read_tokens: Some(4),
                cache_write_tokens: None,
            },
        },
    ]);
    fake.push_stream(vec![
        ChatStreamEvent::TextDelta {
            text: "Ready.".to_string(),
        },
        ChatStreamEvent::MessageEnd {
            stop_reason: StopReason::EndTurn,
            message: ChatMessage::assistant("Ready."),
            usage: Usage {
                input_tokens: 20,
                output_tokens: 8,
                ..Default::default()
            },
        },
    ]);

    let registry = LlmProviderRegistry::new(fake, HashMap::new());
    let chat = ChatRuntime::with_provider_registry(registry).expect("chat runtime");
    let state = AppState::default().with_chat_agent(chat);
    let addr = spawn_router(api_router_with(state)).await;

    let mut ws = connect_ws(addr, "/api/v2/ws/chat?workspace=alpha", None).await;
    let opened = next_event(&mut ws).await;
    assert_eq!(opened["event"], "session_opened");

    ws.send(Message::Text(
        json!({ "text": "analyze nifty" }).to_string(),
    ))
    .await
    .unwrap();
    let events = collect_until_turn_ended(&mut ws).await;

    let thinking = events
        .iter()
        .find(|e| e["event"] == "thinking_delta")
        .expect("thinking_delta event missing");
    assert_eq!(thinking["text"], "Inspecting NIFTY levels");

    let usage_ev = events
        .iter()
        .find(|e| e["event"] == "usage")
        .expect("usage event missing");
    assert_eq!(usage_ev["usage"]["input_tokens"], 12);
    assert_eq!(usage_ev["usage"]["output_tokens"], 5);
    assert_eq!(usage_ev["usage"]["cache_read_tokens"], 4);

    let tool_started = events
        .iter()
        .find(|e| e["event"] == "tool_started")
        .expect("tool_started event missing");
    assert_eq!(tool_started["name"], "present_view");
    assert_eq!(tool_started["raw_input"]["title"], "NIFTY Setup");

    let tool_finished = events
        .iter()
        .find(|e| e["event"] == "tool_finished")
        .expect("tool_finished event missing");
    assert_eq!(tool_finished["name"], "present_view");
    assert_eq!(tool_finished["raw_output"]["schema"], "tantra/result/v1");
    assert_eq!(tool_finished["blocks"][0]["type"], "view");

    let ended = events.last().unwrap();
    assert_eq!(ended["usage"]["input_tokens"], 32);
    assert_eq!(ended["usage"]["output_tokens"], 13);
}

#[test]
fn tool_finished_serializes_code_diff_display_block() {
    let event = SessionEvent::ToolFinished {
        turn: TurnId(1),
        call_id: CallId::new("c1"),
        name: "strategy_patch".to_string(),
        outcome: ToolOutcome::Success,
        elapsed_ms: 9,
        display: None,
        blocks: vec![DisplayBlock::CodeDiff {
            title: "EMA Crossover v2".to_string(),
            lang: "python".to_string(),
            code: "fast = 9\nslow = 21\n".to_string(),
            hunks: vec![CodeDiffHunk {
                id: "hunk_1".to_string(),
                header: "@@ -1,2 +1,2 @@".to_string(),
                lines: vec!["-fast = 12".to_string(), "+fast = 9".to_string()],
            }],
        }],
        raw_output: Some(json!({"patched": true})),
    };
    let wire = serde_json::to_value(&event).expect("serialize ToolFinished");
    assert_eq!(wire["event"], "tool_finished");
    assert_eq!(wire["blocks"][0]["type"], "code_diff");
    assert_eq!(wire["blocks"][0]["title"], "EMA Crossover v2");
    assert_eq!(wire["blocks"][0]["hunks"][0]["id"], "hunk_1");
    assert_eq!(wire["raw_output"]["patched"], true);
}

#[tokio::test]
async fn thread_crud_lifecycle_and_ws_resumption_over_sqlite() {
    let db = Database::connect("sqlite::memory:")
        .await
        .expect("connect sqlite::memory:");
    migrate(&db).await.expect("sqlite migrations");
    let store = Arc::new(SqlxSessionStore::new(&db));

    let fake = Arc::new(ScriptedFakeLlm::new("persist-llm"));
    fake.push_text_response("First turn answer.");
    fake.push_text_response("Second turn answer.");
    let registry = LlmProviderRegistry::new(fake, HashMap::new());
    let chat = ChatRuntime::with_provider_registry_and_store(registry, store)
        .expect("chat runtime with sqlite store");

    let state = AppState::default().with_database(db).with_chat_agent(chat);
    let router = api_router_with(state);
    let addr = spawn_router(router.clone()).await;

    // 1. v2-only check: v1 returns 404, v2 returns 200 with envelope + header.
    let (v1_status, _, _) = http_call(&router, "GET", "/api/v1/chat/threads", None, None).await;
    assert_eq!(v1_status, StatusCode::NOT_FOUND);

    let (status, headers, list_body) =
        http_call(&router, "GET", "/api/v2/chat/threads", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("x-tantra-version").unwrap(), "2");
    assert_eq!(list_body["api_version"], "v2");
    assert_eq!(list_body["data"]["threads"].as_array().unwrap().len(), 0);

    // 2. Create a thread in workspace `desk-a`.
    let (status, _, created) = http_call(
        &router,
        "POST",
        "/api/v2/chat/threads",
        None,
        Some(json!({
            "workspace": "desk-a",
            "title": "Nifty Momentum",
            "model": "persist-llm"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let thread_id = created["data"]["id"].as_str().unwrap().to_string();
    assert_eq!(created["data"]["workspace"], "desk-a");
    assert_eq!(created["data"]["title"], "Nifty Momentum");
    assert_eq!(created["data"]["archived"], false);

    // 3. Connect `/ws/chat?workspace=desk-a&thread_id=<uuid>`, verify SessionOpened uses that exact ID.
    let mut ws1 = connect_ws(
        addr,
        &format!("/api/v2/ws/chat?workspace=desk-a&thread_id={thread_id}"),
        None,
    )
    .await;
    let opened1 = next_event(&mut ws1).await;
    assert_eq!(opened1["event"], "session_opened");
    assert_eq!(opened1["session"], thread_id);
    assert_eq!(opened1["resumed"], true);
    assert_eq!(opened1["history"], 0);

    ws1.send(Message::Text(json!({ "text": "hello desk-a" }).to_string()))
        .await
        .unwrap();
    let turn1 = collect_until_turn_ended(&mut ws1).await;
    assert_eq!(turn1.last().unwrap()["persisted"], true);
    drop(ws1);

    // 4. Reconnect with the same `thread_id` and confirm history count reflects turn 1 + turn 2 works.
    let mut ws2 = connect_ws(
        addr,
        &format!("/api/v2/ws/chat?workspace=desk-a&thread_id={thread_id}"),
        None,
    )
    .await;
    let opened2 = next_event(&mut ws2).await;
    assert_eq!(opened2["session"], thread_id);
    assert_eq!(opened2["resumed"], true);
    assert_eq!(opened2["history"], 2);

    ws2.send(Message::Text(json!({ "text": "follow up" }).to_string()))
        .await
        .unwrap();
    let turn2 = collect_until_turn_ended(&mut ws2).await;
    assert_eq!(turn2.last().unwrap()["persisted"], true);
    drop(ws2);

    // 5. Fetch thread detail and confirm all 4 messages persisted chronologically.
    let (status, _, detail) = http_call(
        &router,
        "GET",
        &format!("/api/v2/chat/threads/{thread_id}?workspace=desk-a"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    let msgs = detail["data"]["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 4);
    assert_eq!(msgs[0]["role"], "user");
    assert_eq!(msgs[1]["role"], "assistant");
    assert_eq!(msgs[2]["role"], "user");
    assert_eq!(msgs[3]["role"], "assistant");

    // 6. PATCH title / archived / model + 422 on blank fields.
    let (bad_status, _, _) = http_call(
        &router,
        "PATCH",
        &format!("/api/v2/chat/threads/{thread_id}"),
        None,
        Some(json!({ "title": "   " })),
    )
    .await;
    assert_eq!(bad_status, StatusCode::UNPROCESSABLE_ENTITY);

    let (status, _, patched) = http_call(
        &router,
        "PATCH",
        &format!("/api/v2/chat/threads/{thread_id}"),
        None,
        Some(json!({
            "title": "Nifty Renamed",
            "archived": true,
            "model": "Claude 3.5 Sonnet"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["data"]["title"], "Nifty Renamed");
    assert_eq!(patched["data"]["archived"], true);
    assert_eq!(patched["data"]["model"], "Claude 3.5 Sonnet");

    let (_, _, archived_list) = http_call(
        &router,
        "GET",
        "/api/v2/chat/threads?workspace=desk-a&archived=true",
        None,
        None,
    )
    .await;
    assert_eq!(
        archived_list["data"]["threads"].as_array().unwrap().len(),
        1
    );

    let (_, _, active_list) = http_call(
        &router,
        "GET",
        "/api/v2/chat/threads?workspace=desk-a&archived=false",
        None,
        None,
    )
    .await;
    assert_eq!(active_list["data"]["threads"].as_array().unwrap().len(), 0);

    // 7. DELETE cascades messages and turns future GET into 404.
    let (status, _, deleted) = http_call(
        &router,
        "DELETE",
        &format!("/api/v2/chat/threads/{thread_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    assert_eq!(deleted["data"]["deleted"], true);
    assert_eq!(deleted["data"]["id"], thread_id);

    let (status, _, _) = http_call(
        &router,
        "GET",
        &format!("/api/v2/chat/threads/{thread_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ws_chat_and_threads_enforce_caller_isolation_and_fail_closed_thread_id() {
    let fake = Arc::new(ScriptedFakeLlm::new("auth-llm"));
    let registry = LlmProviderRegistry::new(fake, HashMap::new());
    let store: Arc<dyn tantra_agent_core::ports::SessionStore> =
        Arc::new(InMemorySessionStore::new());
    let chat =
        ChatRuntime::with_provider_registry_and_store(registry, store).expect("chat runtime");
    let state = AppState::default().with_chat_agent(chat);

    let (config, _dir) = auth_config_with_users();
    let router = api_router_with_config(state, &config);
    let addr = spawn_router(router.clone()).await;

    // 1. Unauthenticated HTTP CRUD is 401.
    let (status, _, _) = http_call(&router, "GET", "/api/v2/chat/threads", None, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // 2. Alice creates a thread in `ws1`.
    let (status, _, created) = http_call(
        &router,
        "POST",
        "/api/v2/chat/threads",
        Some("alice-secret"),
        Some(json!({ "workspace": "ws1", "title": "Alice secret strategy" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let alice_id = created["data"]["id"].as_str().unwrap().to_string();
    assert_eq!(created["data"]["workspace"], "ws1");

    // 3. Bob cannot list, read, patch, or delete Alice's thread.
    let (status, _, bob_list) = http_call(
        &router,
        "GET",
        "/api/v2/chat/threads",
        Some("bob-secret"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(bob_list["data"]["threads"].as_array().unwrap().is_empty());

    for (method, body) in [
        ("GET", None),
        ("PATCH", Some(json!({ "title": "hijacked" }))),
        ("DELETE", None),
    ] {
        let (status, _, _) = http_call(
            &router,
            method,
            &format!("/api/v2/chat/threads/{alice_id}"),
            Some("bob-secret"),
            body,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "bob {method} should 404");
    }

    // 4. `/ws/chat?thread_id=...` fails closed (never falls back to Continue) for:
    //    - malformed UUID
    //    - nonexistent UUID
    //    - wrong workspace for Alice
    //    - right workspace (`ws1`) but wrong Caller (`bob`)
    let unknown_id = SessionId::new().to_string();
    for (query, bearer, expected_substr) in [
        (
            "?workspace=ws1&thread_id=not-a-uuid",
            "alice-secret",
            "invalid thread_id",
        ),
        (
            &format!("?workspace=ws1&thread_id={unknown_id}"),
            "alice-secret",
            "not found in workspace scope",
        ),
        (
            &format!("?workspace=ws2&thread_id={alice_id}"),
            "alice-secret",
            "not found in workspace scope",
        ),
        (
            &format!("?workspace=ws1&thread_id={alice_id}"),
            "bob-secret",
            "not found in workspace scope",
        ),
    ] {
        let mut ws = connect_ws(addr, &format!("/api/v2/ws/chat{query}"), Some(bearer)).await;
        let frame = next_event(&mut ws).await;
        assert_eq!(frame["type"], "error", "expected error frame, got {frame}");
        let msg = frame["message"].as_str().unwrap_or_default();
        assert!(
            msg.contains(expected_substr),
            "expected '{expected_substr}' in '{msg}'"
        );
    }
}
