use chrono::NaiveDate;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tantra_core::{Exchange, InstrumentId};
use tantra_runtime_api::{
    OptionChainRowDto, OptionLegSnapshotDto, OptionsChainQuery, build_tuesday_expiry_schedule,
    compute_atm_strike, compute_max_pain_strike, compute_pcr, format_contract_symbol,
    iv_regime_label, resolve_underlying_symbol, strike_step_for,
};

fn leg(oi: Decimal, contract_symbol: &str) -> OptionLegSnapshotDto {
    OptionLegSnapshotDto {
        contract_symbol: contract_symbol.to_string(),
        ltp: dec!(100),
        ltp_change_pct: Decimal::ZERO,
        bid: None,
        ask: None,
        oi,
        oi_change_pct: Decimal::ZERO,
        volume: Decimal::ZERO,
        iv_pct: dec!(14.00),
        delta: dec!(0.50),
        gamma: dec!(0.001),
        theta: dec!(-10.0),
        vega: dec!(12.0),
        live_quoted: false,
    }
}

fn row(strike: Decimal, call_oi: Decimal, put_oi: Decimal) -> OptionChainRowDto {
    OptionChainRowDto {
        strike,
        is_atm: false,
        call_itm: false,
        put_itm: false,
        call: leg(call_oi, &format!("NFO:NIFTY24NOV{strike}CE")),
        put: leg(put_oi, &format!("NFO:NIFTY24NOV{strike}PE")),
    }
}

#[test]
fn resolves_common_underlying_aliases_to_canonical_id_and_fno_root() {
    assert_eq!(
        resolve_underlying_symbol("NSE:NIFTY50-INDEX").unwrap(),
        (
            InstrumentId::new(Exchange::Nse, "NIFTY50-INDEX"),
            "NIFTY".to_string()
        )
    );
    assert_eq!(
        resolve_underlying_symbol("NIFTY").unwrap(),
        (
            InstrumentId::new(Exchange::Nse, "NIFTY50-INDEX"),
            "NIFTY".to_string()
        )
    );
    assert_eq!(
        resolve_underlying_symbol("RELIANCE.NSE").unwrap(),
        (
            InstrumentId::new(Exchange::Nse, "RELIANCE"),
            "RELIANCE".to_string()
        )
    );
    assert_eq!(
        resolve_underlying_symbol("banknifty").unwrap(),
        (
            InstrumentId::new(Exchange::Nse, "BANKNIFTY-INDEX"),
            "BANKNIFTY".to_string()
        )
    );
}

#[test]
fn validates_query_fields_without_clock_or_io() {
    let missing = OptionsChainQuery::default().resolve().unwrap_err();
    assert_eq!(
        missing.context.as_ref().unwrap()["reason"],
        serde_json::json!("missing_symbol")
    );

    let bad_expiry = OptionsChainQuery {
        symbol: Some("NSE:NIFTY50-INDEX".to_string()),
        expiry: Some("24-11-2026".to_string()),
        as_of: None,
    }
    .resolve()
    .unwrap_err();
    assert_eq!(
        bad_expiry.context.as_ref().unwrap()["reason"],
        serde_json::json!("invalid_expiry")
    );

    let ok = OptionsChainQuery {
        symbol: Some("NSE:NIFTY50-INDEX".to_string()),
        expiry: Some("2026-11-24".to_string()),
        as_of: Some("2026-11-22".to_string()),
    }
    .resolve()
    .unwrap();
    assert_eq!(
        ok.expiry,
        Some(NaiveDate::from_ymd_opt(2026, 11, 24).unwrap())
    );
    assert_eq!(
        ok.as_of,
        Some(NaiveDate::from_ymd_opt(2026, 11, 22).unwrap())
    );
}

#[test]
fn builds_deterministic_tuesday_expiry_schedule_matching_ui() {
    let as_of = NaiveDate::from_ymd_opt(2026, 11, 22).unwrap();
    let schedule = build_tuesday_expiry_schedule(as_of);
    assert_eq!(schedule.len(), 6);
    assert_eq!(schedule[0].expiry, "2026-11-24");
    assert_eq!(schedule[0].short_label, "24 Nov");
    assert!(!schedule[0].is_weekly); // last Tuesday of Nov
    assert_eq!(schedule[0].dte, 2);

    assert_eq!(schedule[1].expiry, "2026-12-01");
    assert!(schedule[1].is_weekly);
    assert_eq!(schedule[1].dte, 9);
}

#[test]
fn computes_pcr_atm_max_pain_and_contract_symbols() {
    let rows = vec![
        row(dec!(25700), dec!(1000), dec!(9000)),
        row(dec!(25800), dec!(5000), dec!(5000)),
        row(dec!(25900), dec!(9000), dec!(1000)),
    ];
    assert_eq!(compute_atm_strike(&rows, dec!(25812)), dec!(25800));
    // (9000+5000+1000) / (1000+5000+9000) = 1.00
    assert_eq!(compute_pcr(&rows), dec!(1.00));
    // Settle at 25700 -> calls payout = 0, puts payout = (25800-25700)*5000 + (25900-25700)*1000 = 700_000
    // Settle at 25800 -> calls payout = 100*1000 = 100_000, puts payout = 100*1000 = 100_000 => total 200_000 (minimum)
    // Settle at 25900 -> calls payout = 200*1000 + 100*5000 = 700_000
    assert_eq!(compute_max_pain_strike(&rows), dec!(25800));

    let exp = NaiveDate::from_ymd_opt(2026, 11, 24).unwrap();
    assert_eq!(
        format_contract_symbol("NIFTY", exp, dec!(25800), true),
        "NFO:NIFTY24NOV25800CE"
    );
    assert_eq!(strike_step_for("NIFTY", dec!(25800)), dec!(50));
    assert_eq!(strike_step_for("BANKNIFTY", dec!(53420)), dec!(100));
    assert_eq!(iv_regime_label(dec!(50)), "High");
    assert_eq!(iv_regime_label(dec!(30)), "Mid");
    assert_eq!(iv_regime_label(dec!(15)), "Low");
}
