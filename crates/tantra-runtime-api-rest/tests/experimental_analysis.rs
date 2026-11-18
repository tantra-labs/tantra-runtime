//! Integration tests for `GET /experimental-analysis/catalog`,
//! `POST /experimental-analysis/returns`, `POST /experimental-analysis/intraday`,
//! and `POST /experimental-analysis/correlation` through the real v1/v2 routers.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{Datelike as _, NaiveDate, TimeZone as _, Utc};
use serde_json::{Value, json};
use std::sync::Arc;
use tantra_core::{Exchange, InstrumentId};
use tantra_runtime_api_rest::market_feed::ports::{BarReader, BarRequest, PortResult};
use tantra_runtime_api_rest::market_feed::wire::{
    Bar, BarAggregation, BarSpecification, PriceType, UnixNanos,
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

fn synthetic_5m_slice(
    exchange: Exchange,
    symbol: &str,
    start: NaiveDate,
    days: usize,
) -> tantra_runtime_api_rest::market_feed::ColumnarSlice {
    let spec = BarSpecification::new(5, BarAggregation::Minute, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(InstrumentId::new(exchange, symbol), spec);
    let mut cur = start;
    let mut count = 0usize;
    let ist_offset = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
    while count < days {
        let wd = cur.weekday();
        if wd != chrono::Weekday::Sat && wd != chrono::Weekday::Sun {
            // Build 8 x 5m bars from 09:15 IST to 09:50 IST with breakout + flip + close action.
            let base_p = 2400.0 + (count as f64) * 10.0;
            let offsets = [
                (0.0, 6.0, -4.0, 2.0),        // 09:15 OR
                (2.0, 8.0, -2.0, 4.0),        // 09:20 OR
                (4.0, 7.0, -3.0, 5.0),        // 09:25 OR (ORH = base+8, ORL = base-4)
                (5.0, 18.0, 2.0, 15.0),       // 09:30 breaks above ORH -> enters long @ base+15
                (15.0, 16.0, -9.0, -6.0),     // 09:35 hits stop (ORL) -> flips short
                (-6.0, -2.0, -22.0, -18.0),   // 09:40 rallies lower -> short in profit
                (-18.0, -10.0, -25.0, -20.0), // 09:45 continuation
                (-20.0, -14.0, -24.0, -16.0), // 09:50 EOD exit
            ];
            for (slot_idx, (o, h, l, c)) in offsets.iter().enumerate() {
                let minute = 9 * 60 + 15 + (slot_idx as u32) * 5;
                let dt_ist = ist_offset
                    .from_local_datetime(&cur.and_hms_opt(minute / 60, minute % 60, 0).unwrap())
                    .unwrap();
                let dt_utc = dt_ist.with_timezone(&Utc);
                builder
                    .push(
                        UnixNanos::from_datetime(dt_utc),
                        base_p + o,
                        base_p + h,
                        base_p + l,
                        base_p + c,
                        15_000.0 + (slot_idx as f64) * 1_000.0,
                    )
                    .unwrap();
            }
            count += 1;
        }
        cur = cur.succ_opt().unwrap();
    }
    builder.finish().unwrap()
}

#[derive(Clone, Debug)]
struct MultiTfBarReader {
    daily: Arc<DatasetReader>,
    min5: Arc<DatasetReader>,
}

#[axum::async_trait]
impl BarReader for MultiTfBarReader {
    async fn read_bars(&self, request: &BarRequest) -> PortResult<Vec<Bar>> {
        if request.spec().aggregation() == BarAggregation::Minute && request.spec().step() == 5 {
            self.min5.read_bars(request).await
        } else {
            self.daily.read_bars(request).await
        }
    }
}

fn test_app() -> Router {
    let start = NaiveDate::from_ymd_opt(2025, 6, 16).unwrap();
    let daily_dataset = Dataset::from_slices(vec![
        synthetic_daily_slice(Exchange::Nse, "RELIANCE", start, 95),
        synthetic_daily_slice(Exchange::Nse, "NIFTYBEES", start, 30),
        synthetic_daily_slice(Exchange::Nse, "TANLA", start, 95),
        synthetic_daily_slice(Exchange::Cds, "USDINR", start, 30),
        synthetic_daily_slice(Exchange::Mcx, "GOLD", start, 95),
    ])
    .unwrap();
    let min5_dataset = Dataset::from_slices(vec![synthetic_5m_slice(
        Exchange::Nse,
        "RELIANCE",
        start,
        12,
    )])
    .unwrap();
    let daily_reader = Arc::new(DatasetReader::from_dataset(daily_dataset));
    let min5_reader = Arc::new(DatasetReader::from_dataset(min5_dataset));
    let bar_reader = Arc::new(MultiTfBarReader {
        daily: Arc::clone(&daily_reader),
        min5: min5_reader,
    });
    api_router_with(AppState::new(
        Arc::clone(&daily_reader)
            as Arc<dyn tantra_runtime_api_rest::market_feed::ports::InstrumentMaster>,
        bar_reader,
        Arc::clone(&daily_reader)
            as Arc<dyn tantra_runtime_api_rest::market_feed::ports::QuoteReader>,
        daily_reader as Arc<dyn tantra_runtime_api_rest::market_feed::ports::DepthReader>,
    ))
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
async fn returns_evaluates_full_study_and_fno_spread_reconciliation() {
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
    assert!(data["friction_summary"].is_null());
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

    // Also test F&O spread + statutory fee + futures roll basis reconciliation
    let mut spread_payload = payload.clone();
    spread_payload["roll_basis_mode"] = json!("net_after_spread_and_costs");
    spread_payload["spread_slippage_bps"] = json!("10");
    spread_payload["futures_roll_basis_bps"] = json!("5");
    spread_payload["include_statutory_costs"] = json!(true);

    let (status_spread, body_spread) = post_json(
        &app,
        "/api/v2/experimental-analysis/returns",
        spread_payload,
    )
    .await;
    assert_eq!(status_spread, StatusCode::OK, "{body_spread}");
    let fs = &body_spread["data"]["friction_summary"];
    assert!(
        fs.is_object(),
        "expected friction_summary object: {body_spread}"
    );
    assert_eq!(fs["mode"], "net_after_spread_and_costs");
    assert_eq!(fs["per_period_spread_bps"], "10");
    assert_eq!(fs["per_period_basis_bps"], "5");
    assert!(
        fs["per_period_statutory_bps"]
            .as_str()
            .unwrap()
            .parse::<f64>()
            .unwrap()
            > 0.0
    );

    // Also test v1 envelope & EXCHANGE:SYMBOL alternate id syntax
    let mut payload_v1 = payload;
    payload_v1["instrument_id"] = json!("NSE:RELIANCE");
    let (status_v1, body_v1) =
        post_json(&app, "/api/v1/experimental-analysis/returns", payload_v1).await;
    assert_eq!(status_v1, StatusCode::OK, "{body_v1}");
    assert_eq!(body_v1["api_version"], "v1");
    assert_eq!(body_v1["data"]["period_count"], data["period_count"]);
}

#[tokio::test]
async fn intraday_evaluates_5m_orb_and_reports_honest_unavailable_without_5m() {
    let app = test_app();

    // 1. RELIANCE has both 1d and 5m bars -> full intraday + ORB simulation available
    let (status, body) = post_json(
        &app,
        "/api/v2/experimental-analysis/intraday",
        json!({
            "instrument_id": "NSE:RELIANCE",
            "asset_class": "fno_stock",
            "period": "weekly_expiry",
            "start_date": "2025-06-16",
            "end_date": "2025-11-15",
            "tail_threshold_pct": "2",
            "opening_range_minutes": 15,
            "max_flips_per_day": 2,
            "spread_slippage_bps": "8",
            "include_statutory_costs": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let d = &body["data"];
    assert_eq!(d["intraday"]["available"], true);
    assert!(d["intraday"]["total_sessions"].as_u64().unwrap() >= 10);
    assert!(d["gaps"]["total_sessions"].as_u64().unwrap() >= 80);
    assert!(d["five_day"]["sample_count"].as_u64().unwrap() >= 80);
    assert_eq!(d["cycle_progress"]["deciles"].as_array().unwrap().len(), 11);
    assert_eq!(d["orb_backtest"]["available"], true);
    assert!(d["orb_backtest"]["total_trades"].as_u64().unwrap() >= 10);

    // 2. TANLA has only 1d bars -> gaps/5-day/cycle_progress work, intraday & orb report available=false
    let (status2, body2) = post_json(
        &app,
        "/api/v2/experimental-analysis/intraday",
        json!({
            "instrument_id": "NSE:TANLA",
            "asset_class": "equity",
            "period": "calendar_weekly",
            "start_date": "2025-06-16",
            "end_date": "2025-11-15"
        }),
    )
    .await;
    assert_eq!(status2, StatusCode::OK, "{body2}");
    let d2 = &body2["data"];
    assert_eq!(d2["intraday"]["available"], false);
    assert_eq!(d2["orb_backtest"]["available"], false);
    assert!(d2["gaps"]["total_sessions"].as_u64().unwrap() >= 80);
    assert_eq!(
        d2["cycle_progress"]["deciles"].as_array().unwrap().len(),
        11
    );
}

#[tokio::test]
async fn correlation_computes_symmetric_matrices_and_rolling_spread() {
    let app = test_app();
    let (status, body) = post_json(
        &app,
        "/api/v2/experimental-analysis/correlation",
        json!({
            "primary_id": "NSE:RELIANCE",
            "benchmark_id": "MCX:GOLD",
            "symbols": ["NSE:RELIANCE", "NSE:TANLA", "MCX:GOLD"],
            "start_date": "2025-06-16",
            "end_date": "2025-11-15",
            "rolling_window": 15
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let d = &body["data"];
    assert_eq!(d["labels"].as_array().unwrap().len(), 3);
    assert_eq!(d["pearson_matrix"].as_array().unwrap().len(), 3);
    assert_eq!(d["spearman_matrix"].as_array().unwrap().len(), 3);
    assert_eq!(d["rolling_window"], 15);
    assert!(d["aligned_sessions"].as_u64().unwrap() >= 80);
    assert!(!d["rolling_series"].as_array().unwrap().is_empty());
}
