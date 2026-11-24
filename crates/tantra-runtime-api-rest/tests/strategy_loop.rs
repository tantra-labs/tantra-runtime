//! Integration tests for Phase 3 (`Track T21.3`):
//! - `POST /api/v2/studies/strategy-loop/evaluate`
//! - `POST /api/v2/studies/strategy-loop/sweep`
//! - `POST /api/v2/studies/strategy-loop/recommend-hedge`
//! - Stateful `LoopSessionStore` session progression & delta tracking
//! - Live option chain (`ChainReader`) vs honest `"theoretical_model"` fallback
//! - Real `RuntimeStrategyRunner` replacing hardcoded stub outputs

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{Datelike as _, Duration, NaiveDate, TimeZone as _, Utc};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use tantra_agent_chat_tools::ports::strategy::StrategyRunner;
use tantra_core::rust_decimal::Decimal;
use tantra_core::{Currency, Exchange, InstrumentId, InstrumentKind, OptionType};
use tantra_runtime_api_rest::market_feed::ports::{BarReader, BarRequest, PortResult, QuoteReader};
use tantra_runtime_api_rest::market_feed::wire::{
    Bar, BarAggregation, BarSpecification, Instrument, PriceType, QuoteTick, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{
    ColumnarSliceBuilder, Dataset, DatasetReader, LiveQuoteStore,
};
use tantra_runtime_api_rest::{AppState, RuntimeStrategyRunner, api_router_with};
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
    let mut builder =
        ColumnarSliceBuilder::new(InstrumentId::try_new(exchange, symbol).unwrap(), spec);
    let mut cur = start;
    let mut count = 0usize;
    let mut price = 2400.0_f64;
    while count < days {
        let wd = cur.weekday();
        if wd != chrono::Weekday::Sat && wd != chrono::Weekday::Sun {
            // Deterministic oscillating + sweep-reclaim swing pattern so zones, touches,
            // outer-buffer stops, and indicators all fire cleanly.
            let (open_off, high_off, low_off, close_off) = match count % 10 {
                0 => (0.0, 28.0, -12.0, 18.0),
                1 => (18.0, 42.0, -8.0, 32.0),
                2 => (32.0, 40.0, -35.0, -24.0),
                3 => (-24.0, 10.0, -45.0, -36.0),
                4 => (-36.0, 18.0, -52.0, 12.0), // Sweep + bullish reclaim
                5 => (12.0, 58.0, 4.0, 48.0),
                6 => (48.0, 72.0, 22.0, 62.0),
                7 => (62.0, 78.0, 15.0, 22.0),
                8 => (22.0, 34.0, -18.0, -12.0),
                _ => (-12.0, 15.0, -22.0, 8.0),
            };
            let base = price;
            let open = (base + open_off).max(100.0);
            let close = (base + close_off).max(100.0);
            let high = open.max(close).max(base + high_off) + 12.0;
            let low = open.min(close).min(base + low_off).max(50.0) - 10.0;
            builder
                .push(
                    ts_for(cur),
                    open,
                    high,
                    low,
                    close,
                    180_000.0 + (count as f64) * 1_500.0,
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
    let mut builder =
        ColumnarSliceBuilder::new(InstrumentId::try_new(exchange, symbol).unwrap(), spec);
    let mut cur = start;
    let mut count = 0usize;
    let ist_offset = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
    while count < days {
        let wd = cur.weekday();
        if wd != chrono::Weekday::Sat && wd != chrono::Weekday::Sun {
            let base_p = 2400.0 + (count as f64) * 8.0;
            let offsets = [
                (0.0, 6.0, -4.0, 2.0),
                (2.0, 8.0, -2.0, 4.0),
                (4.0, 7.0, -3.0, 5.0),
                (5.0, 18.0, 2.0, 15.0),
                (15.0, 16.0, -9.0, -6.0),
                (-6.0, -2.0, -22.0, -18.0),
                (-18.0, -10.0, -25.0, -20.0),
                (-20.0, -14.0, -24.0, -16.0),
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

fn build_state(with_live_chain: bool, start: NaiveDate) -> AppState {
    let daily_dataset = Dataset::from_slices(vec![
        synthetic_daily_slice(Exchange::Nse, "RELIANCE", start, 120),
        synthetic_daily_slice(Exchange::Nse, "NIFTYBEES", start, 60),
        synthetic_daily_slice(Exchange::Mcx, "GOLD", start, 90),
    ])
    .unwrap();
    let min5_dataset = Dataset::from_slices(vec![synthetic_5m_slice(
        Exchange::Nse,
        "RELIANCE",
        start,
        15,
    )])
    .unwrap();

    let mut instruments = vec![
        Instrument::new(
            InstrumentId::try_new(Exchange::Nse, "RELIANCE").unwrap(),
            InstrumentKind::Equity,
            Currency::Inr,
            250.0,
            0.05,
        ),
        Instrument::new(
            InstrumentId::try_new(Exchange::Nse, "NIFTYBEES").unwrap(),
            InstrumentKind::Index,
            Currency::Inr,
            50.0,
            0.05,
        ),
        Instrument::new(
            InstrumentId::try_new(Exchange::Mcx, "GOLD").unwrap(),
            InstrumentKind::Future,
            Currency::Inr,
            100.0,
            1.0,
        ),
    ];

    let expiry = start + Duration::days(180);
    if with_live_chain {
        for strike in [2200.0, 2300.0, 2400.0, 2500.0, 2600.0, 2700.0] {
            let ce_id =
                InstrumentId::try_new(Exchange::Nse, format!("RELIANCE25DEC{}CE", strike as u64))
                    .unwrap();
            let pe_id =
                InstrumentId::try_new(Exchange::Nse, format!("RELIANCE25DEC{}PE", strike as u64))
                    .unwrap();
            instruments.push(
                Instrument::new(ce_id, InstrumentKind::Option, Currency::Inr, 250.0, 0.05)
                    .with_derivative(
                        Some(expiry),
                        Some(strike),
                        Some(OptionType::Call),
                        Some("RELIANCE".to_string()),
                    ),
            );
            instruments.push(
                Instrument::new(pe_id, InstrumentKind::Option, Currency::Inr, 250.0, 0.05)
                    .with_derivative(
                        Some(expiry),
                        Some(strike),
                        Some(OptionType::Put),
                        Some("RELIANCE".to_string()),
                    ),
            );
        }
    }

    let daily_reader = Arc::new(DatasetReader::new(daily_dataset, instruments));
    let min5_reader = Arc::new(DatasetReader::from_dataset(min5_dataset));
    let bar_reader = Arc::new(MultiTfBarReader {
        daily: Arc::clone(&daily_reader),
        min5: min5_reader,
    });

    let live_quotes = Arc::new(LiveQuoteStore::new(Arc::clone(&daily_reader)));
    if with_live_chain {
        let now_ns = UnixNanos::from_datetime(Utc::now());
        for strike in [2200.0, 2300.0, 2400.0, 2500.0, 2600.0, 2700.0] {
            let ce_id =
                InstrumentId::try_new(Exchange::Nse, format!("RELIANCE25DEC{}CE", strike as u64))
                    .unwrap();
            let pe_id =
                InstrumentId::try_new(Exchange::Nse, format!("RELIANCE25DEC{}PE", strike as u64))
                    .unwrap();
            let ce_prem = ((2600.0_f64 - strike).max(20.0) * 0.25).max(12.0);
            let pe_prem = ((strike - 2300.0_f64).max(20.0) * 0.25).max(12.0);
            live_quotes.put_quote(QuoteTick::new(
                ce_id,
                ce_prem - 0.5,
                ce_prem + 0.5,
                500.0,
                500.0,
                now_ns,
                now_ns,
            ));
            live_quotes.put_quote(QuoteTick::new(
                pe_id,
                pe_prem - 0.5,
                pe_prem + 0.5,
                500.0,
                500.0,
                now_ns,
                now_ns,
            ));
        }
    }

    AppState::new(
        Arc::clone(&daily_reader)
            as Arc<dyn tantra_runtime_api_rest::market_feed::ports::InstrumentMaster>,
        bar_reader,
        live_quotes as Arc<dyn QuoteReader>,
        daily_reader as Arc<dyn tantra_runtime_api_rest::market_feed::ports::DepthReader>,
    )
}

async fn post_v2(app: &Router, uri: &str, payload: Value) -> (StatusCode, Option<String>, Value) {
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
    let ver_hdr = res
        .headers()
        .get("x-tantra-version")
        .and_then(|v| v.to_str().ok())
        .map(ToOwned::to_owned);
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, ver_hdr, serde_json::from_slice(&bytes).unwrap())
}

fn assert_scene_bounds(scene: &Value, bar_count: usize) {
    assert!(bar_count > 0, "expected non-empty bars");
    for sig in scene["signals"].as_array().unwrap() {
        let idx = sig["index"].as_u64().unwrap() as usize;
        assert!(
            idx < bar_count,
            "signal index {idx} >= bar_count {bar_count}"
        );
    }
    for conn in scene["connectors"].as_array().unwrap() {
        let from = conn["from_index"].as_u64().unwrap() as usize;
        let to = conn["to_index"].as_u64().unwrap() as usize;
        assert!(
            from <= to && to < bar_count,
            "connector {from}..={to} >= {bar_count}"
        );
    }
    for band in scene["bands"].as_array().unwrap() {
        let x0 = band["x0"].as_u64().unwrap() as usize;
        let x1 = band["x1"].as_u64().unwrap() as usize;
        assert!(
            x0 <= x1 && x1 < bar_count,
            "band {x0}..={x1} >= {bar_count}"
        );
    }
    for lvl in scene["levels"].as_array().unwrap() {
        let x0 = lvl["x0"].as_u64().unwrap() as usize;
        let x1 = lvl["x1"].as_u64().unwrap() as usize;
        assert!(
            x0 <= x1 && x1 < bar_count,
            "level {x0}..={x1} >= {bar_count}"
        );
    }
    for stop in scene["stops"].as_array().unwrap() {
        let x0 = stop["x0"].as_u64().unwrap() as usize;
        let x1 = stop["x1"].as_u64().unwrap() as usize;
        assert!(
            x0 <= x1 && x1 < bar_count,
            "stop {x0}..={x1} >= {bar_count}"
        );
    }
    for hint in scene["hints"].as_array().unwrap() {
        let idx = hint["bar_index"].as_u64().unwrap() as usize;
        assert!(idx < bar_count, "hint {idx} >= {bar_count}");
    }
    for hedge in scene["hedges"].as_array().unwrap() {
        let x0 = hedge["x0"].as_u64().unwrap() as usize;
        let x1 = hedge["x1"].as_u64().unwrap() as usize;
        assert!(
            x0 <= x1 && x1 < bar_count,
            "hedge {x0}..={x1} >= {bar_count}"
        );
    }
    let indicators = scene["indicators"].as_array().unwrap();
    assert!(
        indicators.len() >= 3,
        "expected EMA(20), RSI(14), ATR(N) indicators, got {}",
        indicators.len()
    );
}

#[tokio::test]
async fn evaluate_returns_scene_stop_comparison_hedge_and_stateful_session_deltas() {
    let start = NaiveDate::from_ymd_opt(2025, 6, 16).unwrap();
    let app = api_router_with(build_state(false, start));

    // 1. Evaluate elephant_edge_v3 (1d) — first run in session
    let req1 = json!({
        "strategy_id": "elephant_edge_v3",
        "instrument_id": "RELIANCE.NSE",
        "asset_class": "fno_stock",
        "timeframe": "1d",
        "start_date": "2025-06-16",
        "end_date": "2025-12-31",
        "sl_buffer_atr": "0.25",
        "horizon_bars": 5,
        "target_r": "3",
        "stop_policy_mode": "outer_atr_buffer",
        "expiry_horizon": "weekly",
        "max_chart_bars": 80
    });
    let (s1, hdr1, b1) =
        post_v2(&app, "/api/v2/studies/strategy-loop/evaluate", req1.clone()).await;
    assert_eq!(s1, StatusCode::OK, "{b1}");
    assert_eq!(b1["api_version"], "v2");
    assert_eq!(hdr1.as_deref(), Some("2"));

    let d1 = &b1["data"];
    let sid = d1["session_id"].as_str().unwrap().to_string();
    assert!(sid.starts_with("loop-"), "unexpected session_id: {sid}");
    assert_eq!(d1["run_seq"], 1);
    assert!(d1["previous_run"].is_null());
    assert!(d1["delta_vs_previous"].is_null());
    assert_eq!(d1["pipeline"]["strategy_id"], "elephant_edge_v3");
    assert_eq!(d1["stop_comparison"]["models"].as_array().unwrap().len(), 4);
    assert_eq!(d1["hedge"]["chain_status"], "theoretical_model");

    let bars1 = d1["bars"].as_array().unwrap();
    assert_eq!(bars1.len(), 80);
    assert_scene_bounds(&d1["scene"], bars1.len());

    // 2. Re-evaluate with same session_id & TightCenterLine -> run_seq increments & delta populated
    let mut req2 = req1;
    req2["session_id"] = json!(sid);
    req2["stop_policy_mode"] = json!("tight_center_line");
    req2["sl_buffer_atr"] = json!("0");
    let (s2, _, b2) = post_v2(&app, "/api/v2/studies/strategy-loop/evaluate", req2).await;
    assert_eq!(s2, StatusCode::OK, "{b2}");
    let d2 = &b2["data"];
    assert_eq!(d2["session_id"], sid);
    assert_eq!(d2["run_seq"], 2);
    assert_eq!(d2["previous_run"]["run_seq"], 1);
    assert!(d2["delta_vs_previous"].is_object());

    // 3. Evaluate orb_15 on 5m bars
    let (s_orb, _, b_orb) = post_v2(
        &app,
        "/api/v2/studies/strategy-loop/evaluate",
        json!({
            "strategy_id": "orb_15",
            "instrument_id": "NSE:RELIANCE",
            "asset_class": "fno_stock",
            "timeframe": "5m",
            "start_date": "2025-06-16",
            "end_date": "2025-12-31",
            "sl_buffer_atr": "0.15",
            "horizon_bars": 4,
            "target_r": "2",
            "stop_policy_mode": "outer_atr_buffer"
        }),
    )
    .await;
    assert_eq!(s_orb, StatusCode::OK, "{b_orb}");
    let d_orb = &b_orb["data"];
    assert_eq!(d_orb["pipeline"]["strategy_id"], "orb_15");
    assert_eq!(d_orb["timeframe"], "5m");
    let orb_bars = d_orb["bars"].as_array().unwrap();
    assert!(!orb_bars.is_empty());
    assert_scene_bounds(&d_orb["scene"], orb_bars.len());
}

#[tokio::test]
async fn sweep_enforces_bound_computes_is_oos_wfe_dsr_and_scenes() {
    let start = NaiveDate::from_ymd_opt(2025, 6, 16).unwrap();
    let app = api_router_with(build_state(false, start));

    let (status, hdr, body) = post_v2(
        &app,
        "/api/v2/studies/strategy-loop/sweep",
        json!({
            "base": {
                "strategy_id": "elephant_edge_v3",
                "instrument_id": "NSE:RELIANCE",
                "asset_class": "fno_stock",
                "timeframe": "1d",
                "start_date": "2025-06-16",
                "end_date": "2025-12-31"
            },
            "bounds": {
                "sl_buffer_atr_grid": ["0", "0.25", "0.50"],
                "horizon_bars_grid": [3, 5],
                "target_r_grid": ["2", "3"],
                "max_candidates": 12
            }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["api_version"], "v2");
    assert_eq!(hdr.as_deref(), Some("2"));

    let d = &body["data"];
    let trial_count = d["trial_count"].as_u64().unwrap();
    assert!((1..=64).contains(&trial_count));
    assert_eq!(
        d["candidates"].as_array().unwrap().len(),
        trial_count as usize
    );

    let winner = &d["winner"];
    assert!(winner["is_kpis"].is_object());
    assert!(winner["oos_kpis"].is_object());
    assert!(winner["walk_forward_efficiency_pct"].is_string());
    assert!(winner["deflated_sharpe_ratio"].is_string());
    assert!(d["delta_winner_vs_baseline"].is_object());

    let bar_len = d["bars"].as_array().unwrap().len();
    assert_scene_bounds(&d["baseline_scene"], bar_len);
    assert_scene_bounds(&d["winner_scene"], bar_len);
}

#[tokio::test]
async fn recommend_hedge_uses_live_chain_when_present_and_theoretical_when_absent() {
    let start = NaiveDate::from_ymd_opt(2025, 6, 16).unwrap();

    // 1. Offline / no option chain -> honest theoretical_model
    let offline_app = api_router_with(build_state(false, start));
    let (s_off, _, b_off) = post_v2(
        &offline_app,
        "/api/v2/studies/strategy-loop/recommend-hedge",
        json!({
            "instrument_id": "NSE:RELIANCE",
            "asset_class": "fno_stock",
            "timeframe": "1d",
            "start_date": "2025-06-16",
            "end_date": "2025-12-31",
            "expiry_horizon": "weekly"
        }),
    )
    .await;
    assert_eq!(s_off, StatusCode::OK, "{b_off}");
    let d_off = &b_off["data"];
    assert_eq!(d_off["live_chain_available"], false);
    assert_eq!(d_off["live_contract_count"], 0);
    assert_eq!(d_off["recommended"]["chain_status"], "theoretical_model");
    assert_eq!(d_off["all_horizons"].as_array().unwrap().len(), 4);

    // 2. Live chain populated -> live_quotes and non-zero live_contract_count
    let live_app = api_router_with(build_state(true, start));
    let (s_live, _, b_live) = post_v2(
        &live_app,
        "/api/v2/studies/strategy-loop/recommend-hedge",
        json!({
            "instrument_id": "NSE:RELIANCE",
            "asset_class": "fno_stock",
            "timeframe": "1d",
            "start_date": "2025-06-16",
            "end_date": "2025-12-31",
            "expiry_horizon": "weekly",
            "direction": "long"
        }),
    )
    .await;
    assert_eq!(s_live, StatusCode::OK, "{b_live}");
    let d_live = &b_live["data"];
    assert_eq!(d_live["live_chain_available"], true);
    assert!(d_live["live_contract_count"].as_u64().unwrap() >= 10);
    assert_eq!(d_live["recommended"]["chain_status"], "live_quotes");
    assert_eq!(d_live["all_horizons"].as_array().unwrap().len(), 4);
    let quote_sources: Vec<&str> = d_live["recommended"]["legs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["quote_source"].as_str().unwrap())
        .collect();
    assert!(
        quote_sources
            .iter()
            .any(|s| *s == "live_bid_ask" || *s == "live_ltp"),
        "expected live option quote source, got {quote_sources:?}"
    );
}

#[tokio::test]
async fn error_handling_returns_404_for_missing_instrument_and_422_for_invalid_inputs() {
    let start = NaiveDate::from_ymd_opt(2025, 6, 16).unwrap();
    let app = api_router_with(build_state(false, start));

    let (s_missing, _, b_missing) = post_v2(
        &app,
        "/api/v2/studies/strategy-loop/evaluate",
        json!({
            "strategy_id": "elephant_edge_v3",
            "instrument_id": "NSE:UNKNOWN_XYZ",
            "asset_class": "fno_stock",
            "start_date": "2025-06-16",
            "end_date": "2025-12-31"
        }),
    )
    .await;
    assert_eq!(s_missing, StatusCode::NOT_FOUND, "{b_missing}");
    assert_eq!(b_missing["error"]["code"], "not_found");

    let (s_bad_strat, _, b_bad_strat) = post_v2(
        &app,
        "/api/v2/studies/strategy-loop/evaluate",
        json!({
            "strategy_id": "does_not_exist",
            "instrument_id": "NSE:RELIANCE",
            "asset_class": "fno_stock",
            "start_date": "2025-06-16",
            "end_date": "2025-12-31"
        }),
    )
    .await;
    assert_eq!(
        s_bad_strat,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{b_bad_strat}"
    );
    assert_eq!(b_bad_strat["error"]["code"], "validation_invalid_request");

    let (s_bad_buf, _, b_bad_buf) = post_v2(
        &app,
        "/api/v2/studies/strategy-loop/evaluate",
        json!({
            "strategy_id": "elephant_edge_v3",
            "instrument_id": "NSE:RELIANCE",
            "asset_class": "fno_stock",
            "start_date": "2025-06-16",
            "end_date": "2025-12-31",
            "sl_buffer_atr": "9.5"
        }),
    )
    .await;
    assert_eq!(s_bad_buf, StatusCode::UNPROCESSABLE_ENTITY, "{b_bad_buf}");
    assert_eq!(b_bad_buf["error"]["code"], "validation_invalid_request");
}

#[tokio::test]
async fn runtime_strategy_runner_computes_real_metrics_and_errors_without_data() {
    // Use recent dates inside RuntimeStrategyRunner's 5-year lookback window
    let start = Utc::now().date_naive() - Duration::days(200);
    let state = build_state(true, start);
    let runner = RuntimeStrategyRunner::new(
        state.instruments.clone(),
        state.bars.clone(),
        state.quotes.clone(),
        None,
    );

    let mut params = BTreeMap::new();
    params.insert("sl_buffer_atr".to_string(), Decimal::new(25, 2));
    let bt = runner
        .run_backtest(
            "elephant_edge_v3",
            "RELIANCE",
            Decimal::from(1_000_000),
            Some(params),
        )
        .await
        .expect("backtest should succeed on real bars");
    assert_eq!(bt["canonical_strategy"], "elephant_edge_v3");
    assert_eq!(bt["symbol"], "NSE:RELIANCE");
    assert!(bt["total_trades"].as_u64().unwrap() >= 1);
    assert_eq!(bt["chain_status"], "live_quotes");

    let tuned = runner
        .tune_strategy(
            "elephant_edge_v3",
            "NSE:RELIANCE",
            Decimal::from(1_000_000),
            "sharpe",
        )
        .await
        .expect("tune should succeed on real bars");
    assert_eq!(tuned["canonical_strategy"], "elephant_edge_v3");
    assert!(tuned["total_trials"].as_u64().unwrap() >= 1);
    assert!(tuned["overfitting_verdict"].is_string());

    let suggestion = runner
        .generate_trade_suggestion(
            "elephant_edge_v3",
            "NSE:RELIANCE",
            Decimal::from(500_000),
            Decimal::ONE,
        )
        .await
        .expect("suggestion should succeed on real bars");
    assert_eq!(suggestion["symbol"], "NSE:RELIANCE");
    assert!(suggestion["signal"]["entry_price"].is_string());
    assert!(suggestion["sizing"]["quantity"].is_string());
    assert!(suggestion["statutory_charges"]["total_charges"].is_string());
    assert_eq!(suggestion["hedge"]["chain_status"], "live_quotes");

    // Never fabricates prices/bars when symbol is missing
    let err = runner
        .run_backtest(
            "elephant_edge_v3",
            "MISSING_SYMBOL",
            Decimal::from(500_000),
            None,
        )
        .await
        .expect_err("missing symbol must return Err");
    assert!(
        err.contains("not found"),
        "expected explicit not-found error, got: {err}"
    );
}
