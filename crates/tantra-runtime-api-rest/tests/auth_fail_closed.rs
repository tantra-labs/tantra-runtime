//! Deny-by-default auth (design 0006 R0): a route with no scope row, and the previously
//! unscoped market-data / `/ws/live` / trade-journal routes, never answer anonymously.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use sha2::{Digest, Sha256};
use tantra_ledger_performance::store::JournalStore;
use tantra_runtime_api_rest::{
    ApiConfig, AppState, DEFAULT_USER_ID, api_router_with, api_router_with_config, chat_scope_name,
};
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

/// Auth-on router with tokens `reader-secret` (`read`) and `trader-secret` (`read`, `trade`).
fn auth_router() -> (Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens.toml");
    std::fs::write(
        &path,
        token_toml(&[
            ("reader", "reader-secret", "\"read\""),
            ("trader", "trader-secret", "\"read\", \"trade\""),
        ]),
    )
    .unwrap();
    let mut config = ApiConfig::default();
    config.auth.tokens_file = Some(path);
    (api_router_with_config(AppState::default(), &config), dir)
}

async fn status(router: &Router, method: &str, uri: &str, token: Option<&str>) -> StatusCode {
    let mut req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    router
        .clone()
        .oneshot(req.body(Body::from("{}")).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn anonymous_requests_to_formerly_unscoped_routes_are_401() {
    let (router, _dir) = auth_router();
    let cases = [
        ("POST", "/api/v2/trade-journal/accounts"),
        ("PATCH", "/api/v2/trade-journal/trades/x"),
        ("POST", "/api/v2/trade-journal/notes"),
        ("GET", "/api/v2/trade-journal/overview"),
        ("GET", "/api/v2/calendar"),
        ("GET", "/api/v2/block-deals"),
    ];
    for (method, uri) in cases {
        assert_eq!(
            status(&router, method, uri, None).await,
            StatusCode::UNAUTHORIZED,
            "{method} {uri}"
        );
    }
}

#[tokio::test]
async fn read_only_token_is_403_on_trade_routes() {
    let (router, _dir) = auth_router();
    let cases = [
        ("POST", "/api/v2/trade-journal/accounts"),
        ("PATCH", "/api/v2/trade-journal/trades/x"),
        ("POST", "/api/v2/trade-journal/notes"),
    ];
    for (method, uri) in cases {
        assert_eq!(
            status(&router, method, uri, Some("reader-secret")).await,
            StatusCode::FORBIDDEN,
            "{method} {uri}"
        );
    }
    let s = status(&router, "GET", "/api/v2/calendar", Some("reader-secret")).await;
    assert_ne!(s, StatusCode::UNAUTHORIZED);
    assert_ne!(s, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn unknown_route_is_denied_by_default() {
    let (router, _dir) = auth_router();
    assert_eq!(
        status(&router, "GET", "/api/v2/nope", None).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        status(&router, "GET", "/api/v2/nope", Some("trader-secret")).await,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn cors_preflight_is_not_denied() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens.toml");
    std::fs::write(
        &path,
        token_toml(&[("reader", "reader-secret", "\"read\"")]),
    )
    .unwrap();
    let mut config = ApiConfig::default()
        .with_cors_origins(["https://app.example"])
        .unwrap();
    config.auth.tokens_file = Some(path);
    let router = api_router_with_config(AppState::default(), &config);
    let res = router
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/v2/orders")
                .header("origin", "https://app.example")
                .header("access-control-request-method", "POST")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(res.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(res.status(), StatusCode::FORBIDDEN);
}

/// Only a real CORS preflight (with `Access-Control-Request-Method`) skips auth; a bare
/// `OPTIONS` is an unscoped request like any other and is denied.
#[tokio::test]
async fn bare_options_without_preflight_header_is_denied() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens.toml");
    std::fs::write(
        &path,
        token_toml(&[("reader", "reader-secret", "\"read\"")]),
    )
    .unwrap();
    let mut config = ApiConfig::default();
    config.auth.tokens_file = Some(path);
    let router = api_router_with_config(AppState::default(), &config);
    let res = router
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/v2/orders")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn chat_scope_is_namespaced_by_caller() {
    assert_eq!(chat_scope_name(DEFAULT_USER_ID, Some("ws1")), "ws1");
    assert_eq!(chat_scope_name(DEFAULT_USER_ID, None), "default");
    assert_eq!(chat_scope_name("alice", Some("ws1")), "alice/ws1");
    assert_eq!(chat_scope_name("alice", Some("  ")), "alice/default");
    assert_eq!(chat_scope_name("alice", None), "alice/default");
}

#[tokio::test]
async fn trade_journal_post_bodies_are_enveloped() {
    let router = api_router_with(AppState::default().with_journal_store(JournalStore::new()));
    let post = |uri: &'static str, body: &'static str| {
        let router = router.clone();
        async move {
            let res = router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = res.status();
            let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
                .await
                .unwrap();
            (
                status,
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
            )
        }
    };
    let (status, body) = post(
        "/api/v2/trade-journal/accounts",
        r#"{"display_name":"A","provenance":"Manual","currency":"INR","starting_balance":"1000"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(body["data"]["id"].is_string(), "{body}");
    let account_id = body["data"]["id"].as_str().unwrap().to_owned();

    let note = format!(r#"{{"account_id":"{account_id}","day":"2026-03-15","text":"x"}}"#);
    let note: &'static str = Box::leak(note.into_boxed_str());
    let (status, body) = post("/api/v2/trade-journal/notes", note).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["data"]["ok"], true, "{body}");
}
