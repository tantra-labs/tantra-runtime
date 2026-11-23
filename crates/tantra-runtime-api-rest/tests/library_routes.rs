//! Tests for the v2 library / signal-study routes, studies consolidation and deprecation headers.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use std::time::Duration;
use tantra_runtime_api_rest::library_client::{LibraryClientConfig, LibraryMcpClient};
use tantra_runtime_api_rest::{AppState, api_router, api_router_with};
use tower::util::ServiceExt;

fn client(mode: &str, timeout_ms: u64) -> std::sync::Arc<LibraryMcpClient> {
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/fake_library_mcp.py"
    );
    LibraryMcpClient::spawn(LibraryClientConfig {
        command: "python3".into(),
        args: vec![script.to_string(), mode.to_string()],
        request_timeout: Duration::from_millis(timeout_ms),
        restart_backoff_initial: Duration::from_millis(20),
        restart_backoff_max: Duration::from_millis(100),
        ..LibraryClientConfig::default()
    })
}

async fn send(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, axum::http::HeaderMap, Value) {
    let mut b = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(v) => {
            b = b.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = app.oneshot(b.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = axum::body::to_bytes(res.into_body(), 8 * 1024 * 1024)
        .await
        .unwrap();
    (
        status,
        headers,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn app(mode: &str) -> axum::Router {
    api_router_with(AppState::default().with_library_client(client(mode, 5000)))
}

#[tokio::test]
async fn search_and_describe_forward_args() {
    let (s, _, b) = send(
        app("echo"),
        "GET",
        "/api/v2/studies/library/entries?query=rsi&kind=indicator&limit=5",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["api_version"], "v2");
    assert_eq!(b["data"]["tool"], "tantra_library_search");
    assert_eq!(b["data"]["arguments"]["query"], "rsi");
    assert_eq!(b["data"]["arguments"]["limit"], 5);

    let (s, _, b) = send(
        app("echo"),
        "GET",
        "/api/v2/studies/library/entries/rsi?format=json",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["data"]["arguments"]["id"], "rsi");
    assert_eq!(b["data"]["arguments"]["format"], "json");
}

#[tokio::test]
async fn compute_and_study_echo_with_overlay_scene() {
    let (s, _, b) = send(
        app("echo"),
        "POST",
        "/api/v2/studies/indicators/compute",
        Some(json!({"id":"rsi","params":{"n":14},"bars":[{"c":1}],"tail":10,"overlay_scene":true})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["data"]["tool"], "tantra_indicator_compute");
    assert_eq!(b["data"]["arguments"]["overlay_scene"], true);
    assert_eq!(b["data"]["arguments"]["params"]["n"], 14);

    let (s, _, b) = send(
        app("echo"),
        "POST",
        "/api/v2/studies/signal-study",
        Some(json!({"id":"rsi","bars_file":"main","horizons":[1,5],"direction":-1,"overlay_scene":true})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["data"]["tool"], "tantra_signal_study");
    assert_eq!(b["data"]["arguments"]["bars_file"], "main");
    assert_eq!(b["data"]["arguments"]["overlay_scene"], true);
}

#[tokio::test]
async fn no_client_is_503() {
    let (s, _, b) = send(api_router(), "GET", "/api/v2/studies/library/entries", None).await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(b["error"]["message"], "library_unavailable");
}

#[tokio::test]
async fn timeout_is_504_and_tool_error_is_422() {
    let slow = api_router_with(AppState::default().with_library_client(client("sleep", 150)));
    let (s, _, _) = send(slow, "GET", "/api/v2/studies/library/entries", None).await;
    assert_eq!(s, StatusCode::GATEWAY_TIMEOUT);

    let (s, _, b) = send(
        app("tool_error"),
        "GET",
        "/api/v2/studies/library/entries/x",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
}

#[tokio::test]
async fn validation_rejects_bad_inputs() {
    for bars_file in [
        "/etc/passwd",
        "../x",
        "a/../b",
        "a\u{0}b",
        "C:x",
        "a\\b",
        "refs:main",
    ] {
        let (s, _, _) = send(
            app("echo"),
            "POST",
            "/api/v2/studies/indicators/compute",
            Some(json!({"id":"rsi","bars_file":bars_file})),
        )
        .await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "{bars_file:?}");
    }
    let bars: Vec<Value> = (0..5001).map(|i| json!({"c": i})).collect();
    let (s, _, _) = send(
        app("echo"),
        "POST",
        "/api/v2/studies/indicators/compute",
        Some(json!({"id":"rsi","bars":bars})),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    let (s, _, _) = send(
        app("echo"),
        "POST",
        "/api/v2/studies/signal-study",
        Some(json!({"id":"rsi","bars_file":"main","horizons":[1,2,3,4,5,6,7,8,9,10,11]})),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
    let (s, _, _) = send(
        app("echo"),
        "POST",
        "/api/v2/studies/indicators/compute",
        Some(json!({"id":"rsi","bars_file":"main","tail":2001})),
    )
    .await;
    assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn new_routes_absent_under_v1() {
    let (s, _, _) = send(app("echo"), "GET", "/api/v1/studies/library/entries", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (s, _, _) = send(
        app("echo"),
        "POST",
        "/api/v1/studies/signal-study",
        Some(json!({"id":"x","bars_file":"main"})),
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn deprecation_headers_on_legacy_study_paths_only() {
    let (s_old, h_old, _) = send(
        api_router(),
        "GET",
        "/api/v1/experimental-analysis/catalog",
        None,
    )
    .await;
    assert_eq!(h_old.get("deprecation").unwrap(), "true");
    assert_eq!(
        h_old.get("link").unwrap(),
        "</api/v2/studies/experimental-analysis/catalog>; rel=\"successor-version\""
    );
    let (_, h_v2, _) = send(
        api_router(),
        "GET",
        "/api/v2/experimental-analysis/catalog",
        None,
    )
    .await;
    assert_eq!(h_v2.get("deprecation").unwrap(), "true");
    let (s_new, h_new, _) = send(
        api_router(),
        "GET",
        "/api/v2/studies/experimental-analysis/catalog",
        None,
    )
    .await;
    assert!(h_new.get("deprecation").is_none());
    assert_eq!(s_old, s_new);
}

#[tokio::test]
async fn v2_envelope_rewrites_api_version_and_health_reports_library() {
    let (s, _, b) = send(api_router(), "GET", "/api/v2/health", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["api_version"], "v2");
    assert_eq!(b["data"]["library"], "disabled");
}

#[tokio::test]
async fn dotdot_substring_in_a_component_is_allowed() {
    let (s, _, b) = send(
        app("echo"),
        "POST",
        "/api/v2/studies/indicators/compute",
        Some(json!({"id":"rsi","bars_file":"data/a..b.csv"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
}

#[tokio::test]
async fn jwt_only_auth_fails_closed_without_token() {
    use tantra_runtime_api_rest::{ApiConfig, AuthConfig, JwtConfig, api_router_with_config};
    let mut config = ApiConfig::default();
    config.auth = AuthConfig {
        jwt_config: Some(JwtConfig {
            hmac_secret: Some(b"0123456789abcdef0123456789abcdef".to_vec()),
            rsa_public_key: None,
            issuer: "iss".into(),
            audience: "aud".into(),
        }),
        ..AuthConfig::default()
    };
    for (m, uri, body) in [
        ("GET", "/api/v2/studies/library/entries", None),
        ("GET", "/api/v2/portfolio", None),
        ("GET", "/api/v2/studies/experimental-analysis/catalog", None),
        (
            "POST",
            "/api/v2/studies/signal-study",
            Some(json!({"id":"x","bars_file":"main"})),
        ),
    ] {
        let state = AppState::default().with_library_client(client("echo", 5000));
        let (s, _, _) = send(api_router_with_config(state, &config), m, uri, body).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED, "{m} {uri}");
    }
}
