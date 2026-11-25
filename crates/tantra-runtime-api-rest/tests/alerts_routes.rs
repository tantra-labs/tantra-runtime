//! `/api/v2/alerts` (design 0006 R2c): caller-scoped CRUD, 503 without profiles, auth negatives.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tantra_runtime_api_rest::{ApiConfig, AppState, api_router_with, api_router_with_config};
use tower::util::ServiceExt;

fn auth_off(dir: &std::path::Path) -> Router {
    api_router_with(AppState::default().with_profiles_dir(dir.join("profiles")))
}

fn auth_on(dir: &std::path::Path) -> Router {
    let mut toml = String::new();
    for id in ["alice", "bob"] {
        let hex: String = Sha256::digest(format!("secret-{id}").as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        toml.push_str(&format!(
            "[[tokens]]\ntoken_id = \"{id}\"\ntoken_hash = \"sha256:{hex}\"\nscopes = []\nlabel = \"t\"\ncreated_at = \"2026-01-01T00:00:00Z\"\n"
        ));
    }
    let path = dir.join("tokens.toml");
    std::fs::write(&path, toml).unwrap();
    let mut config = ApiConfig::default();
    config.auth.tokens_file = Some(path);
    api_router_with_config(
        AppState::default().with_profiles_dir(dir.join("profiles")),
        &config,
    )
    .unwrap()
}

async fn call(
    app: &Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer secret-{t}"));
    }
    let req = match body {
        Some(v) => b
            .header("content-type", "application/json")
            .body(Body::from(v.to_string()))
            .unwrap(),
        None => b.body(Body::empty()).unwrap(),
    };
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn new_alert(id: &str, ws: &str) -> Value {
    json!({"id": id, "workspaceId": ws, "instrumentId": "NSE:RELIANCE", "price": "2500.50"})
}

#[tokio::test]
async fn create_list_filter_delete_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let app = auth_off(dir.path());
    let (s, v) = call(
        &app,
        "POST",
        "/api/v2/alerts",
        None,
        Some(new_alert("al_1", "ws-a")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{v}");
    assert_eq!(v["data"]["id"], "al_1");
    assert_eq!(v["data"]["status"], "active");
    assert!(v["data"]["triggeredAt"].is_null());

    // Keyless id is minted.
    let mut keyless = new_alert("x", "ws-b");
    keyless.as_object_mut().unwrap().remove("id");
    let (s, v) = call(&app, "POST", "/api/v2/alerts", None, Some(keyless)).await;
    assert_eq!(s, StatusCode::CREATED, "{v}");
    assert!(v["data"]["id"].as_str().unwrap().starts_with("al_"));

    // Duplicate id conflicts.
    let (s, _) = call(
        &app,
        "POST",
        "/api/v2/alerts",
        None,
        Some(new_alert("al_1", "ws-a")),
    )
    .await;
    assert_eq!(s, StatusCode::CONFLICT);

    let (s, v) = call(&app, "GET", "/api/v2/alerts", None, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["data"]["alerts"].as_array().unwrap().len(), 2);
    let (_, v) = call(&app, "GET", "/api/v2/alerts?workspace=ws-a", None, None).await;
    assert_eq!(v["data"]["alerts"].as_array().unwrap().len(), 1);

    let (s, v) = call(&app, "DELETE", "/api/v2/alerts/al_1", None, None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["data"]["id"], "al_1");
    let (s, _) = call(&app, "DELETE", "/api/v2/alerts/al_1", None, None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn without_profiles_is_503() {
    let app = api_router_with(AppState::default());
    let (s, v) = call(&app, "GET", "/api/v2/alerts", None, None).await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(v["error"]["code"], "service_unavailable");
}

#[tokio::test]
async fn alerts_are_isolated_per_caller() {
    let dir = tempfile::tempdir().unwrap();
    let app = auth_on(dir.path());
    let (s, v) = call(
        &app,
        "POST",
        "/api/v2/alerts",
        Some("alice"),
        Some(new_alert("al_a", "ws")),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{v}");
    let (s, v) = call(&app, "GET", "/api/v2/alerts", Some("bob"), None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(v["data"]["alerts"].as_array().unwrap().is_empty());
    let (s, _) = call(&app, "DELETE", "/api/v2/alerts/al_a", Some("bob"), None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    let (_, v) = call(&app, "GET", "/api/v2/alerts", Some("alice"), None).await;
    assert_eq!(v["data"]["alerts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn auth_on_without_token_is_401() {
    let dir = tempfile::tempdir().unwrap();
    let app = auth_on(dir.path());
    let (s, _) = call(&app, "GET", "/api/v2/alerts", None, None).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn invalid_price_is_422() {
    let dir = tempfile::tempdir().unwrap();
    let app = auth_off(dir.path());
    for bad in ["-1", "abc", "0"] {
        let mut a = new_alert("al_x", "ws");
        a["price"] = json!(bad);
        let (s, _) = call(&app, "POST", "/api/v2/alerts", None, Some(a)).await;
        assert_eq!(s, StatusCode::UNPROCESSABLE_ENTITY, "price {bad}");
    }
}
