//! Every `/api/v2` response is enveloped, and error statuses match their code
//! (design 0006 R1c).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tantra_runtime_api::ErrorCode;
use tantra_runtime_api_rest::{
    ApiConfig, AppState, api_router, api_router_with_config, route_registry,
};
use tower::util::ServiceExt;

/// `(method, path, actual_status, code)` where the handler's status disagrees with its code.
// tracked: tantra-runtime/docs/gaps/0015-error-code-status-mismatches.md
const KNOWN_STATUS_DEVIATIONS: &[(&str, &str, u16, &str)] = &[
    ("GET", "/profile", 503, "not_implemented"),
    ("GET", "/profile/layouts", 503, "not_implemented"),
    ("GET", "/profile/modules", 503, "not_implemented"),
    ("GET", "/portfolio", 503, "not_implemented"),
    ("GET", "/portfolio/accounts", 503, "not_implemented"),
    ("GET", "/portfolio/positions", 503, "not_implemented"),
    ("GET", "/portfolio/holdings", 503, "not_implemented"),
    ("GET", "/portfolio/trades", 503, "not_implemented"),
    ("GET", "/portfolio/chart", 503, "not_implemented"),
    ("GET", "/portfolio/concentration", 503, "not_implemented"),
    ("GET", "/portfolio/wac", 503, "not_implemented"),
    ("GET", "/ledger", 503, "not_implemented"),
    ("GET", "/tax/report", 503, "not_implemented"),
    ("GET", "/fees", 503, "not_implemented"),
    ("GET", "/positions", 503, "not_implemented"),
    ("GET", "/positions/audit", 503, "not_implemented"),
    ("GET", "/trades", 503, "not_implemented"),
    ("GET", "/reports/ledger", 503, "not_implemented"),
    ("GET", "/reports/tax", 503, "not_implemented"),
    ("GET", "/reports/snapshots", 503, "not_implemented"),
    ("GET", "/reports/audit", 503, "not_implemented"),
    ("GET", "/reports/concentration", 503, "not_implemented"),
    ("GET", "/reports/wac", 503, "not_implemented"),
    ("GET", "/reports/fees", 503, "not_implemented"),
    ("GET", "/reports/equity-curve", 503, "not_implemented"),
    ("GET", "/reports/fills/:order_id", 503, "not_implemented"),
    ("GET", "/backtests/:id/events", 503, "not_implemented"),
    ("GET", "/orders/:id/fills", 503, "not_implemented"),
    ("GET", "/trade-journal/accounts", 503, "not_implemented"),
    ("GET", "/trade-journal/overview", 503, "not_implemented"),
    ("GET", "/trade-journal/trades", 503, "not_implemented"),
    (
        "GET",
        "/trade-journal/calendar",
        400,
        "validation_invalid_request",
    ),
    ("GET", "/trade-journal/notes", 503, "not_implemented"),
    ("GET", "/trade-journal/compare", 503, "not_implemented"),
];

async fn call(router: axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let res = router.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024 * 1024)
        .await
        .unwrap();
    let v = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("non-JSON body: {}", String::from_utf8_lossy(&bytes)));
    (status, v)
}

fn fill(path: &str) -> String {
    path.replace(":id", "X.NSE").replace(":name", "main")
}

#[tokio::test]
async fn every_registry_get_is_enveloped_with_matching_status() {
    let mut failures = Vec::new();
    for e in route_registry() {
        if e.method != "GET"
            || e.path.starts_with("/ws")
            || e.path == "/acp"
            || e.path.starts_with("/schema/openapi")
            || e.path == "/docs"
            || e.path.starts_with("/swagger-ui")
        {
            continue;
        }
        let uri = format!("/api/v2{}", fill(e.path));
        let req = Request::get(&uri).body(Body::empty()).unwrap();
        let res = api_router().oneshot(req).await.unwrap();
        let status = res.status();
        let ctype = res
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if !ctype.starts_with("application/json") {
            failures.push(format!("GET {} -> {status} non-JSON ({ctype})", e.path));
            continue;
        }
        let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024 * 1024)
            .await
            .unwrap();
        let v: Value = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(_) => {
                failures.push(format!("GET {} -> {status} unparsable", e.path));
                continue;
            }
        };
        if v["api_version"] != "v2" {
            failures.push(format!(
                "GET {} -> {status} api_version {}",
                e.path, v["api_version"]
            ));
            continue;
        }
        let has_data = !v["data"].is_null();
        let has_err = !v["error"].is_null();
        if has_data == has_err {
            failures.push(format!(
                "GET {} -> {status} data/error not exclusive",
                e.path
            ));
            continue;
        }
        if has_err {
            let code = v["error"]["code"].as_str().unwrap_or("").to_string();
            let parsed: Result<ErrorCode, _> = serde_json::from_value(v["error"]["code"].clone());
            let Ok(parsed) = parsed else {
                failures.push(format!("GET {} -> unknown code {code}", e.path));
                continue;
            };
            let allowed = KNOWN_STATUS_DEVIATIONS.iter().any(|(m, p, s, c)| {
                *m == "GET" && *p == e.path && *s == status.as_u16() && *c == code
            });
            if status.as_u16() != parsed.http_status() && !allowed {
                failures.push(format!(
                    "GET {} -> {} but code {code} wants {}",
                    e.path,
                    status.as_u16(),
                    parsed.http_status()
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn oversized_order_body_is_an_enveloped_413() {
    let body = vec![b'a'; 1024 * 1024 + 1];
    let req = Request::post("/api/v2/orders")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let (status, v) = call(api_router(), req).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(v["api_version"], "v2");
    assert_eq!(v["error"]["code"], "validation_invalid_request");
    assert_eq!(v["error"]["context"]["reason"], "payload_too_large");
}

#[tokio::test]
async fn bare_schema_document_is_wrapped() {
    let req = Request::get("/api/v2/schema").body(Body::empty()).unwrap();
    let (status, v) = call(api_router(), req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["api_version"], "v2");
    assert!(v["data"]["openapi"].is_string(), "data: {}", v["data"]);
}

#[tokio::test]
async fn scope_denial_is_forbidden() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens.toml");
    let hex = hex::encode(Sha256::digest(b"reader-secret"));
    std::fs::write(
        &path,
        format!(
            "[[tokens]]\ntoken_id = \"reader\"\ntoken_hash = \"sha256:{hex}\"\nscopes = [\"read\"]\nlabel = \"t\"\ncreated_at = \"2026-01-01T00:00:00Z\"\n"
        ),
    )
    .unwrap();
    let mut config = ApiConfig::default();
    config.auth.tokens_file = Some(path);
    let router = api_router_with_config(AppState::default(), &config);
    let req = Request::post("/api/v2/orders")
        .header("authorization", "Bearer reader-secret")
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    let (status, v) = call(router, req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(v["error"]["code"], "forbidden");
}
