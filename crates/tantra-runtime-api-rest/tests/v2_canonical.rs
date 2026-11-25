//! Design 0006 R2a/R5: canonical `/api/v2` resources (the superseded paths are gone since R5,
//! see `r5_removed_paths.rs`) and unique `POST /orders` idempotency keys.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::{Value, json};
use tantra_core::{Currency, Exchange, InstrumentId, InstrumentKind};
use tantra_runtime_api_rest::market_feed::wire::{
    BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use tantra_runtime_api_rest::{ApiConfig, AppState, api_router_with, api_router_with_config};
use tower::util::ServiceExt;

fn state() -> AppState {
    let id = InstrumentId::try_new(Exchange::Nse, "X").unwrap();
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::from_u64(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
}

struct Reply {
    status: StatusCode,
    body: Value,
}

async fn send(
    router: axum::Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
    extra: &[(&str, &str)],
) -> Reply {
    let mut b = Request::builder().method(method).uri(v2(uri));
    for (k, v) in extra {
        b = b.header(*k, *v);
    }
    let body = match body {
        Some(v) => {
            b = b.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = router.oneshot(b.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    Reply {
        status,
        body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    }
}

fn order_body() -> Value {
    json!({"instrument_id":{"symbol":"X","exchange":"NSE"},"side":"buy","qty":25.0})
}

fn order_id(r: &Reply) -> String {
    r.body["data"]["order_id"]
        .as_str()
        .unwrap_or_else(|| panic!("no order id: {}", r.body))
        .to_string()
}

#[tokio::test]
async fn keyless_orders_get_distinct_ids() {
    let st = state();
    let a = send(
        api_router_with(st.clone()),
        Method::POST,
        "/api/v2/orders",
        Some(order_body()),
        &[],
    )
    .await;
    let b = send(
        api_router_with(st.clone()),
        Method::POST,
        "/api/v2/orders",
        Some(order_body()),
        &[],
    )
    .await;
    eprintln!("keyless: {} {} / {} {}", a.status, a.body, b.status, b.body);
    assert_ne!(order_id(&a), order_id(&b));
}

#[tokio::test]
async fn idempotency_key_header_dedupes_and_conflicts_are_422() {
    let st = state();
    let post = |body: Value, key: &'static str| {
        let st = st.clone();
        async move {
            send(
                api_router_with(st),
                Method::POST,
                "/api/v2/orders",
                Some(body),
                &[("idempotency-key", key)],
            )
            .await
        }
    };
    let a = post(order_body(), "k-1").await;
    let b = post(order_body(), "k-1").await;
    assert_eq!(order_id(&a), "k-1");
    assert_eq!(order_id(&a), order_id(&b));
    let mut body = order_body();
    body["client_order_id"] = json!("other");
    let c = post(body, "k-2").await;
    assert_eq!(c.status, StatusCode::UNPROCESSABLE_ENTITY, "{}", c.body);
    assert_eq!(c.body["error"]["context"]["field"], "Idempotency-Key");
}

#[tokio::test]
async fn approvals_and_order_lookup() {
    let r = send(
        api_router_with(state()),
        Method::GET,
        "/api/v2/approvals",
        None,
        &[],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r.body["data"]["queue"].is_array());

    let st = state().with_approval_queue(4);
    let r = send(
        api_router_with(st),
        Method::POST,
        "/api/v2/approvals/nope/approve",
        None,
        &[],
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.body);

    let r = send(
        api_router_with(state()),
        Method::GET,
        "/api/v2/orders/unknown",
        None,
        &[],
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.body["error"]["code"], "not_found");
}

/// Two keyless orders held in the approval queue and both approved become two distinct ledger
/// orders (the approval path used to re-key keyless orders to the constant `"approved"`).
#[tokio::test]
async fn approved_keyless_orders_stay_distinct() {
    let st = state().with_approval_queue(4);
    for _ in 0..2 {
        let r = send(
            api_router_with(st.clone()),
            Method::POST,
            "/api/v2/orders",
            Some(order_body()),
            &[],
        )
        .await;
        assert_eq!(r.body["data"]["status"], "queued", "{}", r.body);
    }
    let mut ids = Vec::new();
    for _ in 0..2 {
        let q = send(
            api_router_with(st.clone()),
            Method::GET,
            "/api/v2/approvals",
            None,
            &[],
        )
        .await;
        let pending = q.body["data"]
            .as_array()
            .or_else(|| q.body["data"]["queue"].as_array())
            .and_then(|a| a.iter().find(|i| i["status"] == "pending"))
            .unwrap_or_else(|| panic!("no pending approval: {}", q.body))
            .clone();
        let approval_id = pending["queue_id"].as_str().unwrap().to_string();
        let r = send(
            api_router_with(st.clone()),
            Method::POST,
            &format!("/api/v2/approvals/{approval_id}/approve"),
            Some(json!({})),
            &[],
        )
        .await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.body);
        ids.push(order_id(&r));
    }
    assert_ne!(
        ids[0], ids[1],
        "approved keyless orders collapsed onto one key"
    );
}

// sha256("test")
const TEST_HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

#[tokio::test]
async fn approving_needs_the_admin_scope() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens.toml");
    std::fs::write(
        &path,
        format!(
            "[[tokens]]\ntoken_id = \"t1\"\ntoken_hash = \"sha256:{TEST_HASH}\"\n\
             scopes = [\"read\"]\nlabel = \"t\"\ncreated_at = \"2026-01-01T00:00:00Z\"\n"
        ),
    )
    .unwrap();
    let mut config = ApiConfig::default();
    config.auth.tokens_file = Some(path);
    let r = send(
        api_router_with_config(state(), &config).unwrap(),
        Method::POST,
        "/api/v2/approvals/x/approve",
        None,
        &[("authorization", "Bearer test")],
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.body);
}

fn v2(uri: &str) -> String {
    if uri.starts_with("/api/") {
        uri.to_string()
    } else {
        format!("/api/v2{uri}")
    }
}
