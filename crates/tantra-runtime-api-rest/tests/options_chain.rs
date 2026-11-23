//! End-to-end REST tests for `GET /api/v2/options/chain` (gap 0016).
//!
//! Exercises query validation, fail-closed auth scoping, theoretical Black-Scholes fallback
//! with zero fabricated bid/ask liquidity, and live multi-expiry chain assembly over
//! `InstrumentChainReader` + `LiveQuoteStore` + `InMemoryOpenInterestStore`.

use std::str::FromStr;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tantra_core::{Currency, Exchange, InstrumentId, InstrumentKind, OptionType, Quantity};
use tantra_market_store::{InMemoryOpenInterestStore, OpenInterestSnapshot, OpenInterestStore};
use tantra_runtime_api_rest::market_feed::wire::{
    AggressorSide, BarAggregation, BarSpecification, Instrument, PriceType, QuoteTick, TradeId,
    TradeTick, UnixNanos,
};
use tantra_runtime_api_rest::market_feed::{ColumnarSliceBuilder, Dataset, DatasetReader};
use tantra_runtime_api_rest::{
    ApiConfig, AppState, api_router, api_router_with, api_router_with_config,
};
use tower::util::ServiceExt;

const DAY_NANOS: u64 = 86_400_000_000_000;

fn day_ns(year: i32, month: u32, day: u32) -> UnixNanos {
    let dt = NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .and_hms_opt(10, 0, 0)
        .unwrap()
        .and_utc();
    UnixNanos::from_datetime(dt)
}

async fn get(app: Router, uri: &str) -> (StatusCode, Value) {
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| panic!("{uri}: non-JSON {:?}", String::from_utf8_lossy(&bytes)));
    (status, body)
}

fn build_daily_spot_slice(
    id: InstrumentId,
    closes: &[f64],
) -> tantra_runtime_api_rest::market_feed::ColumnarSlice {
    let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
    let mut builder = ColumnarSliceBuilder::new(id, spec);
    let base = day_ns(2026, 11, 13).as_u64();
    for (idx, &close) in closes.iter().enumerate() {
        let ts = UnixNanos::from_u64(base + (idx as u64) * DAY_NANOS);
        builder
            .push(
                ts,
                close - 20.0,
                close + 25.0,
                close - 30.0,
                close,
                100_000.0,
            )
            .unwrap();
    }
    builder.finish().unwrap()
}

fn token_toml(id: &str, plain: &str, scopes: &str) -> String {
    let hex = hex::encode(Sha256::digest(plain.as_bytes()));
    format!(
        "[[tokens]]\ntoken_id = \"{id}\"\ntoken_hash = \"sha256:{hex}\"\nscopes = [{scopes}]\nlabel = \"t\"\ncreated_at = \"2026-01-01T00:00:00Z\"\n"
    )
}

#[tokio::test]
async fn options_chain_rejects_invalid_queries_with_422() {
    let app = api_router();

    for uri in [
        "/api/v2/options/chain",
        "/api/v2/options/chain?symbol=NIFTY&unknown=1",
        "/api/v2/options/chain?symbol=NIFTY&expiry=24-11-2026",
        "/api/v2/options/chain?symbol=NIFTY&as_of=not-a-date",
    ] {
        let (status, body) = get(app.clone(), uri).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{uri}: {body}");
        assert_eq!(
            body["error"]["code"],
            json!("validation_invalid_request"),
            "{uri}"
        );
        assert_eq!(body["api_version"], json!("v2"));
    }
}

#[tokio::test]
async fn options_chain_returns_404_when_underlying_has_no_data() {
    let (status, body) = get(api_router(), "/api/v2/options/chain?symbol=NSE:NOPE-EQ").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"]["code"], json!("not_found"));
    assert_eq!(body["api_version"], json!("v2"));
}

#[tokio::test]
async fn options_chain_requires_read_scope_when_auth_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tokens.toml");
    std::fs::write(&path, token_toml("reader", "reader-secret", "\"read\"")).unwrap();
    let mut config = ApiConfig::default();
    config.auth.tokens_file = Some(path);
    let router = api_router_with_config(AppState::default(), &config);

    let anon = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v2/options/chain?symbol=NIFTY")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(anon.status(), StatusCode::UNAUTHORIZED);

    let authed = router
        .oneshot(
            Request::builder()
                .uri("/api/v2/options/chain?symbol=NIFTY")
                .header("authorization", "Bearer reader-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(authed.status(), StatusCode::UNAUTHORIZED);
    assert_ne!(authed.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn offline_underlying_returns_theoretical_bs_with_null_bid_ask() {
    let rel_id = InstrumentId::new(Exchange::Nse, "RELIANCE-EQ");
    let slice = build_daily_spot_slice(rel_id.clone(), &[1350.0, 1362.5, 1370.0, 1368.2, 1382.4]);
    let dataset = Dataset::from_slices(vec![slice]).unwrap();
    let instruments = vec![Instrument::new(
        rel_id,
        InstrumentKind::Equity,
        Currency::Inr,
        500.0,
        0.05,
    )];
    let state = AppState::from_reader(DatasetReader::new(dataset, instruments));
    let app = api_router_with(state);

    let (status, body) = get(
        app,
        "/api/v2/options/chain?symbol=NSE:RELIANCE-EQ&as_of=2026-11-22",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["api_version"], json!("v2"));
    assert!(body["error"].is_null());

    let data = &body["data"];
    assert_eq!(data["instrument_id"], json!("NSE:RELIANCE-EQ"));
    assert_eq!(data["currency"], json!("INR"));
    assert_eq!(data["source"], json!("theoretical_bs"));
    assert_eq!(data["live_contract_count"], json!(0));
    assert_eq!(data["spot_price"], json!("1382.40"));
    assert_eq!(data["spot_change"], json!("14.20"));
    assert_eq!(data["lot_size"], json!("500"));
    assert_eq!(data["atm_strike"], json!("1380"));

    let rows = data["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 29);
    for row in rows {
        for side in ["call", "put"] {
            let leg = &row[side];
            assert!(
                leg["bid"].is_null(),
                "theoretical bid must stay null: {leg}"
            );
            assert!(
                leg["ask"].is_null(),
                "theoretical ask must stay null: {leg}"
            );
            assert_eq!(leg["live_quoted"], json!(false));
            let ltp = Decimal::from_str(leg["ltp"].as_str().unwrap()).unwrap();
            assert!(ltp > Decimal::ZERO);
        }
    }
    let atm = rows.iter().find(|r| r["is_atm"] == json!(true)).unwrap();
    assert_eq!(atm["strike"], json!("1380"));
    let call_delta = Decimal::from_str(atm["call"]["delta"].as_str().unwrap()).unwrap();
    let put_delta = Decimal::from_str(atm["put"]["delta"].as_str().unwrap()).unwrap();
    assert!(call_delta > Decimal::ZERO && call_delta < Decimal::ONE);
    assert!(put_delta < Decimal::ZERO && put_delta > dec!(-1));
}

#[tokio::test]
async fn live_chain_joins_contracts_quotes_and_oi_history_across_expiries() {
    let nifty_id = InstrumentId::new(Exchange::Nse, "NIFTY50-INDEX");
    let slice = build_daily_spot_slice(
        nifty_id.clone(),
        &[25300.0, 25410.0, 25480.0, 25514.3, 25800.6],
    );
    let dataset = Dataset::from_slices(vec![slice]).unwrap();

    let exp1 = NaiveDate::from_ymd_opt(2026, 11, 24).unwrap();
    let exp2 = NaiveDate::from_ymd_opt(2026, 12, 1).unwrap();

    let mut instruments = vec![Instrument::new(
        nifty_id,
        InstrumentKind::Index,
        Currency::Inr,
        75.0,
        0.05,
    )];

    let mut add_opt = |sym: &str, exp: NaiveDate, strike: f64, kind: OptionType| -> InstrumentId {
        let id = InstrumentId::new(Exchange::Nfo, sym);
        instruments.push(
            Instrument::new(
                id.clone(),
                InstrumentKind::Option,
                Currency::Inr,
                75.0,
                0.05,
            )
            .with_derivative(
                Some(exp),
                Some(strike),
                Some(kind),
                Some("NIFTY".to_string()),
            ),
        );
        id
    };

    let ce_25750 = add_opt("NIFTY24NOV25750CE", exp1, 25750.0, OptionType::Call);
    let pe_25750 = add_opt("NIFTY24NOV25750PE", exp1, 25750.0, OptionType::Put);
    let ce_25800 = add_opt("NIFTY24NOV25800CE", exp1, 25800.0, OptionType::Call);
    let pe_25800 = add_opt("NIFTY24NOV25800PE", exp1, 25800.0, OptionType::Put);
    let ce_25850 = add_opt("NIFTY24NOV25850CE", exp1, 25850.0, OptionType::Call);
    let pe_25850 = add_opt("NIFTY24NOV25850PE", exp1, 25850.0, OptionType::Put);
    let _dec_ce_25800 = add_opt("NIFTY01DEC25800CE", exp2, 25800.0, OptionType::Call);
    let _dec_pe_25800 = add_opt("NIFTY01DEC25800PE", exp2, 25800.0, OptionType::Put);

    let oi_store = Arc::new(InMemoryOpenInterestStore::new());
    let prev_day = NaiveDate::from_ymd_opt(2026, 11, 20).unwrap();
    let curr_day = NaiveDate::from_ymd_opt(2026, 11, 22).unwrap();
    for (id, prev_oi, curr_oi) in [
        (&ce_25750, dec!(100000), dec!(120000)),
        (&pe_25750, dec!(200000), dec!(250000)),
        (&ce_25800, dec!(400000), dec!(500000)), // +25%
        (&pe_25800, dec!(500000), dec!(600000)), // +20%
        (&ce_25850, dec!(300000), dec!(330000)),
        (&pe_25850, dec!(150000), dec!(120000)), // -20%
    ] {
        oi_store
            .put(OpenInterestSnapshot {
                instrument: id.clone(),
                as_of: prev_day,
                open_interest: Quantity::new(prev_oi),
            })
            .await
            .unwrap();
        oi_store
            .put(OpenInterestSnapshot {
                instrument: id.clone(),
                as_of: curr_day,
                open_interest: Quantity::new(curr_oi),
            })
            .await
            .unwrap();
    }

    let state =
        AppState::from_reader(DatasetReader::new(dataset, instruments)).with_oi_store(oi_store);
    let live = state.live_quotes().unwrap();
    let ts = day_ns(2026, 11, 22);
    live.put_quote(QuoteTick::new(
        ce_25800.clone(),
        198.0,
        202.0,
        150.0,
        225.0,
        ts,
        ts,
    ));
    live.put_trade(TradeTick::new(
        ce_25800.clone(),
        200.0,
        12000.0,
        AggressorSide::Buyer,
        TradeId::new("t1"),
        ts,
        ts,
    ));
    live.put_quote(QuoteTick::new(
        pe_25800.clone(),
        148.0,
        152.0,
        300.0,
        150.0,
        ts,
        ts,
    ));
    live.put_trade(TradeTick::new(
        pe_25800.clone(),
        150.0,
        9000.0,
        AggressorSide::Seller,
        TradeId::new("t2"),
        ts,
        ts,
    ));

    let app = api_router_with(state);

    // Query with NIFTY alias and first expiry.
    let (status, body) = get(
        app.clone(),
        "/api/v2/options/chain?symbol=NIFTY&expiry=2026-11-24&as_of=2026-11-22",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let data = &body["data"];
    assert_eq!(data["instrument_id"], json!("NSE:NIFTY50-INDEX"));
    assert_eq!(data["source"], json!("live_chain"));
    assert_eq!(data["active_expiry"], json!("2026-11-24"));
    assert_eq!(data["live_contract_count"], json!(6));
    assert_eq!(data["atm_strike"], json!("25800"));
    assert_eq!(data["expiries"].as_array().unwrap().len(), 2);

    // Put OI = 250k + 600k + 120k = 970k; Call OI = 120k + 500k + 330k = 950k => PCR = 1.02
    assert_eq!(data["pcr"], json!("1.02"));
    assert_eq!(data["max_pain_strike"], json!("25800"));

    let rows = data["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);

    // Quoted ATM 25800 row carries exact bid/ask, volume, OI, ΔOI%, and solved IV.
    let atm = &rows[1];
    assert_eq!(atm["strike"], json!("25800"));
    assert_eq!(atm["is_atm"], json!(true));
    assert_eq!(
        Decimal::from_str(atm["call"]["bid"].as_str().unwrap()).unwrap(),
        dec!(198)
    );
    assert_eq!(
        Decimal::from_str(atm["call"]["ask"].as_str().unwrap()).unwrap(),
        dec!(202)
    );
    assert_eq!(
        Decimal::from_str(atm["call"]["ltp"].as_str().unwrap()).unwrap(),
        dec!(200)
    );
    assert_eq!(atm["call"]["volume"], json!("12000"));
    assert_eq!(atm["call"]["oi"], json!("500000"));
    assert_eq!(atm["call"]["oi_change_pct"], json!("25.00"));
    assert_eq!(atm["call"]["live_quoted"], json!(true));
    assert_eq!(
        Decimal::from_str(atm["put"]["bid"].as_str().unwrap()).unwrap(),
        dec!(148)
    );
    assert_eq!(
        Decimal::from_str(atm["put"]["ask"].as_str().unwrap()).unwrap(),
        dec!(152)
    );
    assert_eq!(atm["put"]["oi_change_pct"], json!("20.00"));
    assert_eq!(atm["put"]["live_quoted"], json!(true));

    // Unquoted 25750 contracts carry real OI/ΔOI but preserve null bid/ask.
    let otm_put = &rows[0];
    assert_eq!(otm_put["strike"], json!("25750"));
    assert!(otm_put["call"]["bid"].is_null());
    assert!(otm_put["call"]["ask"].is_null());
    assert_eq!(otm_put["call"]["live_quoted"], json!(false));
    assert_eq!(otm_put["call"]["oi"], json!("120000"));

    // Filtering by second expiry returns December contracts cleanly.
    let (status2, body2) = get(
        app,
        "/api/v2/options/chain?symbol=NSE:NIFTY50-INDEX&expiry=2026-12-01&as_of=2026-11-22",
    )
    .await;
    assert_eq!(status2, StatusCode::OK, "{body2}");
    assert_eq!(body2["data"]["active_expiry"], json!("2026-12-01"));
    assert_eq!(body2["data"]["rows"].as_array().unwrap().len(), 1);
}
