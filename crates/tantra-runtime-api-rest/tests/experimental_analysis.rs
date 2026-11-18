//! Integration tests for `GET /experimental-analysis/catalog` and
//! `POST /experimental-analysis/returns` through the real v1/v2 routers.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::NaiveDate;
use serde_json::{Value, json};
use tantra_core::{Exchange, InstrumentId};
use tantra_runtime_api_rest::market_feed::wire::{
    BarAggregation, BarSpecification, PriceType, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use tantra_runtime_api_rest::{AppState, api_router_with};
use tower::util::ServiceExt;

fn ts_for(date: NaiveDate) -> UnixNanos {
    let dt = date.and_hms_opt(10, 0, 0).expect("valid time").and_utc();
    UnixNanos::from_datetime(dt)
}

fn synthetic_daily_slice(
    exchange: Exchange,
    symbol: &str,
    start: NaiveDate,
    days: usize,
) -> tantra_runtime_api_rest::market_feed::ColumnarSlice {
    let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(InstrumentId::new(exchange, symbol), spec);
    let mut cur = start;
    let mut count = 0usize;
    let mut price = 24000.0_f64;
    while count < days {
        let wd = cur.weekday();
        if wd != chrono::Weekday::Sat && wd != chrono::Weekday::Sun {
            // Deterministic oscillating + trending OHLC pattern including tail swings.
            let step = match count % 9 {
                0 => 145.0,
                1 => 95.0,
                2 => -210.0,
                3 => 60.0,
                4 => 180.0,
                5 => -130.0,
                6 => 320.0,
                7 => -390.0,
                _ => 45.0,
            };
            let open = price;
            let close = (price + step).max(100.0);
            let high = open.max(close) + 75.0;
            let low = open.min(close) - 65.0;
            builder
                .push(
                    ts_for(cur),
                    open,
                    high,
                    low,
                    close,
                    125_000.0 + (count as f64) * 500.0,
                )
                .unwrap();
            price = close;
            count += 1;
        }
        cur = cur.succ_opt().unwrap();
    }
    builder.finish().unwrap()
}

use chrono::Datelike as _;

fn test_app() -> Router {
    let start = NaiveDate::from_ymd_opt(2025, 6, 16).unwrap();
    let dataset = Dataset::from_slices(vec![
        synthetic_daily_slice(Exchange::Nse, "RELIANCE", start, 95),
        synthetic_daily_slice(Exchange::Nse, "NIFTYBEES", start, 30),
        synthetic_daily_slice(Exchange::Nse, "TANLA", start, 30),
        synthetic_daily_slice(Exchange::Cds, "USDINR", start, 30),
        synthetic_daily_slice(Exchange::Mcx, "GOLD", start, 30),
    ])
    .unwrap();
    api_router_with(AppState::from_reader(DatasetReader::from_dataset(dataset)))
}

async fn get_json(app: &Router, uri: &str) -> (StatusCode, Value) {
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn post_json(app: &Router, uri: &str, payload: Value) -> (StatusCode, Value) {
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn catalog_groups_multi_asset_universe_and_schedules() {
    let app = test_app();
    let (status, body) = get_json(&app, "/api/v2/experimental-analysis/catalog").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["api_version"], "v2");
    assert!(body["error"].is_null());

    let groups = body["data"]["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 6);

    let find_group = |key: &str| {
        groups
            .iter()
            .find(|g| g["asset_class"] == key)
            .unwrap_or_else(|| panic!("missing group {key}"))
    };

    let fno_syms: Vec<&str> = find_group("fno_stock")["instruments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["symbol"].as_str().unwrap())
        .collect();
    assert!(fno_syms.contains(&"RELIANCE"), "got {fno_syms:?}");

    let etf_syms: Vec<&str> = find_group("etf")["instruments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["symbol"].as_str().unwrap())
        .collect();
    assert!(etf_syms.contains(&"NIFTYBEES"));

    let fx_syms: Vec<&str> = find_group("forex")["instruments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["symbol"].as_str().unwrap())
        .collect();
    assert!(fx_syms.contains(&"USDINR"));

    let comm_syms: Vec<&str> = find_group("commodity")["instruments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["symbol"].as_str().unwrap())
        .collect();
    assert!(comm_syms.contains(&"GOLD"));
    assert_eq!(
        find_group("commodity")["default_schedule"],
        "front_month_cycle"
    );
}

#[tokio::test]
async fn returns_validation_rejects_invalid_dates_windows_and_coverage() {
    let app = test_app();

    let (status, body) = post_json(
        &app,
        "/api/v2/experimental-analysis/returns",
        json!({
            "instrument_id": "NSE:RELIANCE",
            "asset_class": "fno_stock",
            "period": "weekly_expiry",
            "sub_cycle": "current",
            "calc_type": "open_to_close",
            "start_date": "2025-10-01",
            "end_date": "2025-07-01"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"]["code"], "validation_invalid_request");

    let (status, body) = post_json(
        &app,
        "/api/v2/experimental-analysis/returns",
        json!({
            "instrument_id": "NSE:RELIANCE",
            "asset_class": "fno_stock",
            "period": "weekly_expiry",
            "sub_cycle": "current",
            "calc_type": "open_to_close",
            "start_date": "2025-06-16",
            "end_date": "2025-10-20",
            "ma_window": 1
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    let (status, body) = post_json(
        &app,
        "/api/v2/experimental-analysis/returns",
        json!({
            "instrument_id": "NSE:RELIANCE",
            "asset_class": "fno_stock",
            "period": "weekly_expiry",
            "sub_cycle": "current",
            "calc_type": "open_to_close",
            "start_date": "2025-06-16",
            "end_date": "2025-10-20",
            "outlier_coverage_pct": 10
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
}

#[tokio::test]
async fn returns_unknown_instrument_answers_404() {
    let app = test_app();
    let (status, body) = post_json(
        &app,
        "/api/v2/experimental-analysis/returns",
        json!({
            "instrument_id": "UNKNOWN.NSE",
            "asset_class": "equity",
            "period": "calendar_weekly",
            "sub_cycle": "current",
            "calc_type": "open_to_close",
            "start_date": "2025-06-16",
            "end_date": "2025-10-20"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], "not_found");
}

#[tokio::test]
async fn returns_evaluates_full_study_and_respects_v1_v2_envelopes() {
    let app = test_app();
    let payload = json!({
        "instrument_id": "RELIANCE.NSE",
        "asset_class": "fno_stock",
        "period": "weekly_expiry",
        "sub_cycle": "current",
        "calc_type": "open_to_close",
        "start_date": "2025-06-16",
        "end_date": "2025-11-15",
        "ma_window": 5,
        "outlier_coverage_pct": 90,
        "tail_threshold_pct": 2,
        "ohlc_page": 1,
        "ohlc_page_size": 25
    });

    let (status_v2, body_v2) = post_json(
        &app,
        "/api/v2/experimental-analysis/returns",
        payload.clone(),
    )
    .await;
    assert_eq!(status_v2, StatusCode::OK, "{body_v2}");
    assert_eq!(body_v2["api_version"], "v2");
    let data = &body_v2["data"];

    assert_eq!(data["instrument_id"], "NSE:RELIANCE");
    assert!(data["period_count"].as_u64().unwrap() >= 10);
    assert!(
        data["regime_note"]
            .as_str()
            .unwrap_or_default()
            .contains("Sep 3, 2025"),
        "expected NSE cutover notice, got {:?}",
        data["regime_note"]
    );
    assert_eq!(data["coarse_buckets"].as_array().unwrap().len(), 16);
    assert_eq!(data["probability_levels"].as_array().unwrap().len(), 11);
    assert_eq!(
        data["markov"]["transition_matrix"]
            .as_array()
            .unwrap()
            .len(),
        6
    );
    assert_eq!(data["markov"]["steady_state"].as_array().unwrap().len(), 6);
    assert!(data["forward_conditional_plus_one"].is_object());
    assert_eq!(data["daily_ohlc_total_rows"].as_u64().unwrap(), 95);
    assert_eq!(data["daily_ohlc_rows"].as_array().unwrap().len(), 25);

    // Also test v1 envelope & EXCHANGE:SYMBOL alternate id syntax
    let mut payload_v1 = payload;
    payload_v1["instrument_id"] = json!("NSE:RELIANCE");
    let (status_v1, body_v1) =
        post_json(&app, "/api/v1/experimental-analysis/returns", payload_v1).await;
    assert_eq!(status_v1, StatusCode::OK, "{body_v1}");
    assert_eq!(body_v1["api_version"], "v1");
    assert_eq!(body_v1["data"]["period_count"], data["period_count"]);
}
