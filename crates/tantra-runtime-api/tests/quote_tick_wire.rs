//! `QuoteTick` wire conformance (gap 0010, T17.3): the optional session-summary
//! fields (`last_price`, `day_high`, `day_low`, `prev_close`, `volume`) are
//! backward compatible — absent from the wire when unset, parsed from both old
//! and new payloads, and decimal-string-valued when present.

use chrono::{DateTime, TimeZone, Utc};
use rust_decimal_macros::dec;
use serde_json::json;
use tantra_core::{Exchange, InstrumentId, Price, Quantity};
use tantra_runtime_api::wire::QuoteTick;

/// 2026-11-17T09:30:00Z.
fn ts() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 17, 9, 30, 0).unwrap()
}

fn instrument() -> InstrumentId {
    InstrumentId::try_new(Exchange::Nse, "TCS").unwrap()
}

fn plain() -> QuoteTick {
    QuoteTick::new(
        instrument(),
        dec!(2450.00),
        dec!(2451.00),
        dec!(10),
        dec!(12),
        ts(),
        ts(),
    )
}

/// A quote carrying every session-summary field.
fn summarized() -> QuoteTick {
    plain().with_session_summary(
        Some(Price::try_new(dec!(2450.25)).unwrap()),
        Some(Price::try_new(dec!(2460.00)).unwrap()),
        Some(Price::try_new(dec!(2440.00)).unwrap()),
        Some(Price::try_new(dec!(2430.00)).unwrap()),
        Some(Quantity::try_new(dec!(12500)).unwrap()),
    )
}

#[test]
fn a_quote_without_a_session_summary_serializes_the_old_shape() {
    let value = serde_json::to_value(plain()).unwrap();
    let object = value.as_object().unwrap();
    assert_eq!(object["bid"], json!("2450.00"));
    assert_eq!(object["ask"], json!("2451.00"));
    assert_eq!(object["bid_qty"], json!("10"));
    assert_eq!(object["ask_qty"], json!("12"));
    // The optional summary fields must not appear at all (backward compatible).
    for field in ["last_price", "day_high", "day_low", "prev_close", "volume"] {
        assert!(
            !object.contains_key(field),
            "unset summary field {field} must be skipped on the wire"
        );
    }
}

#[test]
fn a_summarized_quote_round_trips_as_decimal_strings() {
    let value = serde_json::to_value(summarized()).unwrap();
    let object = value.as_object().unwrap();
    assert_eq!(object["last_price"], json!("2450.25"));
    assert_eq!(object["day_high"], json!("2460.00"));
    assert_eq!(object["day_low"], json!("2440.00"));
    assert_eq!(object["prev_close"], json!("2430.00"));
    assert_eq!(object["volume"], json!("12500"));
    // Round trip: parsing the serialized form restores the exact quote.
    let back: QuoteTick = serde_json::from_value(value).unwrap();
    assert_eq!(back, summarized());
    assert_eq!(
        back.last_price,
        Some(Price::try_new(dec!(2450.25)).unwrap())
    );
    assert_eq!(back.volume, Some(Quantity::try_new(dec!(12500)).unwrap()));
}

#[test]
fn a_legacy_payload_without_the_summary_fields_still_parses() {
    let wire = json!({
        "instrument": {"exchange": "NSE", "symbol": "TCS"},
        "bid": "2450.00",
        "ask": "2451.00",
        "bid_qty": "10",
        "ask_qty": "12",
        "exchange_ts": "2026-11-17T09:30:00Z",
        "receive_ts": "2026-11-17T09:30:00Z",
    });
    let back: QuoteTick = serde_json::from_value(wire).unwrap();
    assert_eq!(back.bid, dec!(2450.00));
    assert_eq!(back.last_price, None);
    assert_eq!(back.day_high, None);
    assert_eq!(back.day_low, None);
    assert_eq!(back.prev_close, None);
    assert_eq!(back.volume, None);
}

#[test]
fn a_payload_with_the_summary_fields_parses_them() {
    let wire = json!({
        "instrument": {"exchange": "NSE", "symbol": "TCS"},
        "bid": "2450.00",
        "ask": "2451.00",
        "bid_qty": "10",
        "ask_qty": "12",
        "exchange_ts": "2026-11-17T09:30:00Z",
        "receive_ts": "2026-11-17T09:30:00Z",
        "last_price": "2450.25",
        "day_high": "2460.00",
        "day_low": "2440.00",
        "prev_close": "2430.00",
        "volume": "12500",
    });
    let back: QuoteTick = serde_json::from_value(wire).unwrap();
    assert_eq!(
        back.last_price,
        Some(Price::try_new(dec!(2450.25)).unwrap())
    );
    assert_eq!(back.day_high, Some(Price::try_new(dec!(2460.00)).unwrap()));
    assert_eq!(back.day_low, Some(Price::try_new(dec!(2440.00)).unwrap()));
    assert_eq!(
        back.prev_close,
        Some(Price::try_new(dec!(2430.00)).unwrap())
    );
    assert_eq!(back.volume, Some(Quantity::try_new(dec!(12500)).unwrap()));
}
