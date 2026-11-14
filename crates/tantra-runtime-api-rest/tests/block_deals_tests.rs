//! `/market-data/block-deals` route tests: block/bulk-deal disclosures read through the
//! `tantra-data` projections (`{root}/catalog/block_deals`, T7.3).

use std::sync::Arc;

use arrow::array::{Date32Array, Decimal128Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use parquet::arrow::arrow_writer::ArrowWriter;
use serde_json::Value;
use tantra_runtime_api_rest::AppState;
use tantra_runtime_api_rest::api_router_with;
use tantra_runtime_api_rest::market_feed::DatasetReader;
use tempfile::tempdir;
use tower::util::ServiceExt;

/// An empty in-memory `DatasetReader` (same construction `AppState::default` uses).
fn empty_reader() -> DatasetReader {
    DatasetReader::from_dataset(Default::default())
}

/// Arrow date32 day count of 2026-11-06 (days since 1970-01-01), the projection
/// trade date the tests filter on.
fn days_2026_11_06() -> i32 {
    (chrono::NaiveDate::from_ymd_opt(2026, 11, 6).unwrap()
        - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
    .num_days() as i32
}

/// Writes one block-deals projection file (two deals on 2026-11-06) into
/// `dir/catalog/block_deals/2026-11-06.parquet`, the shape `tantra-data` exports.
fn write_block_deals_projection(dir: &std::path::Path) {
    let block_deals_dir = dir.join("catalog").join("block_deals");
    std::fs::create_dir_all(&block_deals_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("trade_date", DataType::Date32, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("instrument", DataType::Utf8, false),
        Field::new("security_name", DataType::Utf8, false),
        Field::new("client_name", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("quantity", DataType::Decimal128(18, 4), false),
        Field::new("price", DataType::Decimal128(18, 4), false),
        Field::new("remarks", DataType::Utf8, true),
    ]));
    let quantities = Decimal128Array::from(vec![Some(2_500_000_000_i128), Some(100_000_000_i128)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let prices = Decimal128Array::from(vec![Some(25_005_000_i128), Some(40_000_000_i128)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let day = days_2026_11_06();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![day, day])),
            Arc::new(StringArray::from(vec!["NSE", "NSE"])),
            Arc::new(StringArray::from(vec!["NSE:RELIANCE-EQ", "NSE:TCS-EQ"])),
            Arc::new(StringArray::from(vec!["RELIANCE", "TCS"])),
            Arc::new(StringArray::from(vec!["CLIENT A", "CLIENT B"])),
            Arc::new(StringArray::from(vec!["BUY", "SELL"])),
            Arc::new(StringArray::from(vec!["block", "bulk"])),
            Arc::new(quantities),
            Arc::new(prices),
            Arc::new(StringArray::from(vec![Some("Block deal window"), None])),
        ],
    )
    .unwrap();

    let path = block_deals_dir.join("2026-11-06.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn call(state: AppState, uri: &str) -> (StatusCode, Value) {
    let res = api_router_with(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
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
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn block_deals_route_serves_projection() {
    let dir = tempdir().unwrap();
    write_block_deals_projection(dir.path());

    let state = AppState::from_reader(empty_reader()).with_block_deals(dir.path());
    let (status, body) = call(state, "/api/v1/market-data/block-deals?date=2026-11-06").await;
    assert_eq!(status, StatusCode::OK);
    let deals = &body["data"]["deals"];
    assert_eq!(deals.as_array().unwrap().len(), 2);
    assert_eq!(deals[0]["instrument"]["symbol"], "RELIANCE-EQ");
    assert_eq!(deals[0]["side"], "buy");
    assert_eq!(deals[0]["category"], "block");
    assert_eq!(deals[0]["price"], "2500.5000");
    assert_eq!(deals[1]["side"], "sell");
    assert_eq!(deals[1]["category"], "bulk");
}

#[tokio::test]
async fn block_deals_route_filters_by_instrument() {
    let dir = tempdir().unwrap();
    write_block_deals_projection(dir.path());

    let state = AppState::from_reader(empty_reader()).with_block_deals(dir.path());
    let (status, body) = call(
        state,
        "/api/v1/market-data/block-deals?instrument=RELIANCE-EQ.NSE",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let deals = &body["data"]["deals"];
    assert_eq!(deals.as_array().unwrap().len(), 1);
    assert_eq!(deals[0]["instrument"]["symbol"], "RELIANCE-EQ");
    assert_eq!(deals[0]["side"], "buy");
}

#[tokio::test]
async fn block_deals_route_rejects_bad_date() {
    let dir = tempdir().unwrap();
    write_block_deals_projection(dir.path());

    let state = AppState::from_reader(empty_reader()).with_block_deals(dir.path());
    let (status, _body) = call(state, "/api/v1/market-data/block-deals?date=not-a-date").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn block_deals_route_unconfigured_is_not_implemented() {
    let state = AppState::default();
    let (status, body) = call(state, "/api/v1/market-data/block-deals").await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["error"]["code"], "not_implemented");
}
