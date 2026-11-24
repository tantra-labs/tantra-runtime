//! Design 0006 R5: a path is `/api/v2/...` or it does not exist (404 envelope).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tantra_runtime_api_rest::api_router;
use tower::util::ServiceExt;

async fn get(path: &str) -> (StatusCode, serde_json::Value) {
    let res = api_router()
        .oneshot(Request::get(path).body(Body::empty()).expect("request"))
        .await
        .expect("response");
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 32 * 1024 * 1024)
        .await
        .expect("body");
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn removed_paths_answer_404_envelope() {
    for path in [
        "/health",
        "/api/v1/health",
        "/ledger",
        "/api/v2/ledger",
        "/api/v2/experimental-analysis/catalog",
        "/api/v2/ws/live",
        "/api/v2/ws",
        "/schema/openapi/v1.json",
    ] {
        let (status, body) = get(path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
        assert_eq!(body["error"]["code"], "not_found", "{path}: {body}");
    }
}

#[tokio::test]
async fn canonical_paths_answer_200() {
    for path in ["/api/v2/health", "/api/v2/openapi.json"] {
        let (status, _) = get(path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
    }
}
