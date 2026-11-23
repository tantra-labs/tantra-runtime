//! Per-caller partitioning (gap 0013): the trade journal and paper-order ownership are keyed by
//! the authenticated `Caller`.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tantra_core::{Currency, Exchange, InstrumentId, InstrumentKind};
use tantra_ledger_performance::store::JournalStore;
use tantra_runtime_api_rest::market_feed::wire::{
    BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use tantra_runtime_api_rest::{ApiConfig, AppState, api_router_with_config};
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

/// Auth-on router with two `read`+`trade` tokens: `alice-secret` and `bob-secret`.
fn router() -> (Router, tempfile::TempDir) {
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
    let id = InstrumentId::new(Exchange::Nse, "X");
    let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(id.clone(), spec);
    builder
        .push(UnixNanos::from_u64(1), 100.0, 100.0, 100.0, 100.0, 1.0)
        .unwrap();
    let dataset = Dataset::from_slices(vec![builder.finish().unwrap()]).unwrap();
    let instrument = Instrument::new(id, InstrumentKind::Equity, Currency::Inr, 25.0, 0.05);
    let state = AppState::from_reader(DatasetReader::new(dataset, vec![instrument]))
        .with_journal_store(JournalStore::new());
    (api_router_with_config(state, &config), dir)
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    token: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(match body {
            Some(b) => Body::from(b.to_string()),
            None => Body::empty(),
        })
        .unwrap();
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn order(id: &str) -> Value {
    json!({
        "instrument_id": {"symbol": "X", "exchange": "NSE"},
        "side": "buy",
        "qty": 25.0,
        "order_type": "limit",
        "price": 99.0,
        "client_order_id": id,
    })
}

#[tokio::test]
async fn journal_accounts_are_per_caller() {
    let (router, _dir) = router();
    let (status, _) = call(
        &router,
        "POST",
        "/api/v2/trade-journal/accounts",
        "alice-secret",
        Some(json!({
            "display_name": "Alice Only",
            "provenance": "Manual",
            "currency": "INR",
            "starting_balance": "1000.00"
        })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    let (_, mine) = call(
        &router,
        "GET",
        "/api/v2/trade-journal/accounts",
        "alice-secret",
        None,
    )
    .await;
    assert!(mine.to_string().contains("Alice Only"), "{mine}");
    let (status, theirs) = call(
        &router,
        "GET",
        "/api/v2/trade-journal/accounts",
        "bob-secret",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!theirs.to_string().contains("Alice Only"), "{theirs}");
}

#[tokio::test]
async fn orders_are_owned_by_their_caller() {
    let (router, _dir) = router();
    let (status, _) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "alice-secret",
        Some(order("own-1")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (_, mine) = call(&router, "GET", "/api/v2/orders", "alice-secret", None).await;
    assert!(mine.to_string().contains("own-1"), "{mine}");
    let (status, theirs) = call(&router, "GET", "/api/v2/orders", "bob-secret", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!theirs.to_string().contains("own-1"), "{theirs}");

    let (status, _) = call(&router, "GET", "/api/v2/orders/own-1", "bob-secret", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(&router, "GET", "/api/v2/orders/own-1", "alice-secret", None).await;
    assert_eq!(status, StatusCode::OK);

    // Bob's cancel is a 200 no-op that leaves Alice's row untouched.
    let (status, _) = call(
        &router,
        "POST",
        "/api/v2/orders/own-1/cancel",
        "bob-secret",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, row) = call(&router, "GET", "/api/v2/orders/own-1", "alice-secret", None).await;
    assert!(
        !row.to_string().to_lowercase().contains("cancelled"),
        "{row}"
    );
}

#[tokio::test]
async fn client_order_id_cannot_collide_across_callers() {
    let (router, _dir) = router();
    let (status, _) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "alice-secret",
        Some(order("dup-1")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "bob-secret",
        Some(order("dup-1")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(body.to_string().contains("conflict"), "{body}");
    // Alice's own replay stays idempotent.
    let (status, _) = call(
        &router,
        "POST",
        "/api/v2/orders",
        "alice-secret",
        Some(order("dup-1")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}
