//! CORS is off unless origins are allow-listed, over the real router including preflight.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use tantra_runtime_api_rest::{ApiConfig, AppState, api_router_with, api_router_with_config};
use tower::util::ServiceExt;

fn preflight(origin: &str) -> Request<Body> {
    Request::builder()
        .method(Method::OPTIONS)
        .uri("/api/v2/instruments")
        .header(header::ORIGIN, origin)
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
        .body(Body::empty())
        .unwrap()
}

fn write_token_file(dir: &tempfile::TempDir) -> std::path::PathBuf {
    use sha2::{Digest, Sha256};
    let path = dir.path().join("tokens.toml");
    let digest = hex::encode(Sha256::digest(b"reader-secret"));
    std::fs::write(
        &path,
        format!(
            "[[tokens]]\ntoken_id = \"reader\"\ntoken_hash = \"sha256:{digest}\"\n\
             scopes = [\"read\"]\nlabel = \"test\"\ncreated_at = \"2026-01-01T00:00:00Z\"\n"
        ),
    )
    .unwrap();
    path
}

#[tokio::test]
async fn preflight_is_refused_without_an_allow_list() {
    let res = api_router_with(AppState::default())
        .oneshot(preflight("https://app.example"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert!(
        res.headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );
    assert!(
        res.headers()
            .get(header::ACCESS_CONTROL_ALLOW_METHODS)
            .is_none()
    );
    assert!(
        res.headers()
            .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
            .is_none()
    );
}

#[tokio::test]
async fn preflight_is_answered_for_an_allow_listed_origin_only() {
    let config = ApiConfig::default()
        .with_cors_origins(["https://app.example"])
        .unwrap();
    let app = || api_router_with_config(AppState::default(), &config).unwrap();
    let res = app()
        .oneshot(preflight("https://app.example"))
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "https://app.example"
    );
    let allow_headers = res
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
        .expect("cors preflight must advertise allowed headers")
        .to_str()
        .unwrap()
        .to_ascii_lowercase();
    assert!(allow_headers.contains("content-type"), "{allow_headers}");
    assert!(!allow_headers.contains("authorization"), "{allow_headers}");

    let res = app()
        .oneshot(preflight("https://evil.example"))
        .await
        .unwrap();
    assert!(
        res.headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );
}

#[tokio::test]
async fn preflight_with_auth_enabled_allows_authorization_header() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ApiConfig::default()
        .with_cors_origins(["https://app.example"])
        .unwrap();
    config.auth.tokens_file = Some(write_token_file(&dir));

    let router = api_router_with_config(AppState::default(), &config).unwrap();
    let req = Request::builder()
        .method(Method::OPTIONS)
        .uri("/api/v2/orders")
        .header(header::ORIGIN, "https://app.example")
        .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(
            header::ACCESS_CONTROL_REQUEST_HEADERS,
            "authorization, content-type",
        )
        .body(Body::empty())
        .unwrap();
    let res = router.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "https://app.example"
    );
    let allow_headers = res
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
        .expect("authenticated CORS preflight must emit access-control-allow-headers")
        .to_str()
        .unwrap()
        .to_ascii_lowercase();
    assert!(allow_headers.contains("authorization"), "{allow_headers}");
    assert!(allow_headers.contains("content-type"), "{allow_headers}");
}
