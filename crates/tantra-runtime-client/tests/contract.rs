//! Contract tests: the client's routes exist (and are not deprecated) in the checked-in
//! OpenAPI artifact, and envelope parsing works against a real runtime router.

use tantra_runtime_api::ScreenerQuery;
use tantra_runtime_client::{ClientError, ROUTES, RuntimeClient};

#[test]
fn routes_exist_in_openapi_and_are_not_deprecated() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tantra-runtime-api-rest/schema/openapi.v2.json"
    );
    let text = std::fs::read_to_string(path).expect("read openapi.v2.json");
    let doc: serde_json::Value = serde_json::from_str(&text).expect("parse openapi.v2.json");
    for (method, route) in ROUTES {
        let op = &doc["paths"][*route][method.to_lowercase()];
        assert!(
            op.is_object(),
            "{method} {route} missing from openapi.v2.json"
        );
        assert_ne!(
            op["deprecated"].as_bool(),
            Some(true),
            "{method} {route} is deprecated"
        );
    }
}

#[test]
fn debug_redacts_token() {
    let c = RuntimeClient::new("http://127.0.0.1:1", Some("s3cret".into())).expect("client");
    let d = format!("{c:?}");
    assert!(d.contains("[REDACTED]") && !d.contains("s3cret"));
}

#[test]
fn ws_urls_and_invalid_base() {
    let c = RuntimeClient::new("https://h:9/", None).expect("client");
    assert_eq!(c.acp_ws_url(), "wss://h:9/api/v2/acp");
    assert_eq!(c.stream_ws_url(), "wss://h:9/api/v2/stream");
    assert!(matches!(
        RuntimeClient::new("ftp://x", None),
        Err(ClientError::InvalidBase(_))
    ));
}

#[tokio::test]
async fn live_router_envelope_parsing() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = tantra_runtime_api_rest::api_router();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let client = RuntimeClient::new(&format!("http://{addr}"), None).expect("client");
    client.health().await.expect("health ok");
    match client.screener_scans(&ScreenerQuery::default()).await {
        Ok(_) | Err(ClientError::Http { code: Some(_), .. }) => {}
        other => panic!("unexpected screener result: {other:?}"),
    }
}
