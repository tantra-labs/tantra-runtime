use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Date32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, NaiveTime};
use parquet::arrow::ArrowWriter;

use tantra_runtime_backtest::{NseTradingCalendar, SessionWindow, TradingHolidayError};

/// Write a `tantra-data`-shaped trading-holiday projection file
/// (`holiday_date`, `description`, `kind`) into `dir/2026.parquet`.
fn write_holiday_parquet(dir: &Path, rows: &[(NaiveDate, &str, &str)]) {
    fs::create_dir_all(dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("holiday_date", DataType::Date32, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
    ]));
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days: Vec<i32> = rows
        .iter()
        .map(|(date, _, _)| (*date - epoch).num_days() as i32)
        .collect();
    let descriptions: Vec<&str> = rows
        .iter()
        .map(|(_, description, _)| *description)
        .collect();
    let kinds: Vec<&str> = rows.iter().map(|(_, _, kind)| *kind).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(days)),
            Arc::new(StringArray::from(descriptions)),
            Arc::new(StringArray::from(kinds)),
        ],
    )
    .unwrap();
    let path = dir.join("2026.parquet");
    let mut writer = ArrowWriter::try_new(fs::File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn test_regular_weekday_is_trading_day() {
    let cal = NseTradingCalendar::new();
    // Wednesday, Jan 15, 2025
    let d = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    assert!(cal.is_trading_day(d));
    let session = cal.session_for_date(d).unwrap();
    assert_eq!(session, SessionWindow::regular());
}

#[test]
fn test_weekend_and_holidays_excluded() {
    let cal = NseTradingCalendar::new();
    // Saturday, Jan 18, 2025
    let sat = NaiveDate::from_ymd_opt(2025, 1, 18).unwrap();
    assert!(!cal.is_trading_day(sat));

    // Independence Day: Aug 15, 2025 (Friday)
    let holiday = NaiveDate::from_ymd_opt(2025, 8, 15).unwrap();
    assert!(cal.is_holiday(holiday));
    assert!(!cal.is_trading_day(holiday));

    // Next trading day after Thursday Aug 14 is Monday Aug 18
    let aug14 = NaiveDate::from_ymd_opt(2025, 8, 14).unwrap();
    let next = cal.next_trading_day(aug14);
    assert_eq!(next, NaiveDate::from_ymd_opt(2025, 8, 18).unwrap());
}

#[test]
fn test_muhurat_trading_session() {
    let cal = NseTradingCalendar::new();
    let diwali = NaiveDate::from_ymd_opt(2025, 10, 21).unwrap();
    assert!(cal.is_muhurat_session(diwali));
    assert!(cal.is_trading_day(diwali));

    let session = cal.session_for_date(diwali).unwrap();
    assert_eq!(session, SessionWindow::muhurat());
    assert_eq!(session.open, NaiveTime::from_hms_opt(18, 15, 0).unwrap());
    assert_eq!(session.close, NaiveTime::from_hms_opt(19, 15, 0).unwrap());
}

/// Gap 0015 (b): the offline fallback `new()` must keep yielding the same
/// dates as the historic seed table, so backtests stay deterministic offline.
#[test]
fn test_offline_fallback_matches_seed_table_dates() {
    let cal = NseTradingCalendar::new();
    for date in [
        NaiveDate::from_ymd_opt(2025, 1, 26).unwrap(), // Republic Day
        NaiveDate::from_ymd_opt(2025, 8, 15).unwrap(), // Independence Day
        NaiveDate::from_ymd_opt(2025, 10, 21).unwrap(), // Diwali Laxmi Pujan (Muhurat)
        NaiveDate::from_ymd_opt(2025, 10, 22).unwrap(), // Diwali Balipratipada
        NaiveDate::from_ymd_opt(2025, 12, 25).unwrap(), // Christmas
        NaiveDate::from_ymd_opt(2026, 1, 26).unwrap(), // Republic Day
        NaiveDate::from_ymd_opt(2026, 4, 3).unwrap(),  // Good Friday
        NaiveDate::from_ymd_opt(2026, 8, 15).unwrap(), // Independence Day
        NaiveDate::from_ymd_opt(2026, 11, 8).unwrap(), // Diwali Laxmi Pujan (Muhurat)
        NaiveDate::from_ymd_opt(2026, 12, 25).unwrap(), // Christmas
    ] {
        assert!(cal.is_holiday(date), "expected seed holiday {date}");
    }
    assert!(cal.is_muhurat_session(NaiveDate::from_ymd_opt(2026, 11, 8).unwrap()));
    // A non-holiday weekday stays a trading day.
    let monday = NaiveDate::from_ymd_opt(2025, 6, 16).unwrap();
    assert!(cal.is_trading_day(monday));
}

/// Gap 0015 (a): holidays load from a fixture parquet and `is_trading_day`
/// reflects them, including the Muhurat mapping.
#[test]
fn test_calendar_loads_holidays_from_parquet_bridge() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("trading_holidays");
    write_holiday_parquet(
        &dir,
        &[
            (
                NaiveDate::from_ymd_opt(2026, 1, 26).unwrap(),
                "Republic Day",
                "holiday",
            ),
            (
                NaiveDate::from_ymd_opt(2026, 4, 3).unwrap(),
                "Good Friday",
                "holiday",
            ),
            (
                NaiveDate::from_ymd_opt(2026, 11, 8).unwrap(),
                "Diwali Laxmi Pujan (Muhurat trading)",
                "muhurat",
            ),
        ],
    );
    let cal = NseTradingCalendar::from_parquet_dir(&dir).unwrap();

    // Monday Jan 26, 2026 is a declared holiday -> not a trading day.
    let republic_day = NaiveDate::from_ymd_opt(2026, 1, 26).unwrap();
    assert!(cal.is_holiday(republic_day));
    assert!(!cal.is_trading_day(republic_day));

    // Diwali Laxmi Pujan (Sunday Nov 8, 2026) is a Muhurat session -> trading
    // day with the Muhurat window, and a clearing holiday like the seed table.
    let diwali = NaiveDate::from_ymd_opt(2026, 11, 8).unwrap();
    assert!(cal.is_muhurat_session(diwali));
    assert!(cal.is_trading_day(diwali));
    assert_eq!(cal.session_for_date(diwali), Some(SessionWindow::muhurat()));
    assert!(cal.is_holiday(diwali));

    // A regular Monday is a trading day and a date that is neither holiday nor
    // weekend gets the regular session.
    let monday = NaiveDate::from_ymd_opt(2026, 11, 16).unwrap();
    assert!(cal.is_trading_day(monday));
    assert_eq!(cal.session_for_date(monday), Some(SessionWindow::regular()));
}

#[test]
fn test_from_parquet_dir_missing_data_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let empty = tmp.path().join("no_holidays_here");
    assert!(matches!(
        NseTradingCalendar::from_parquet_dir(&empty),
        Err(TradingHolidayError::NotFound { .. })
    ));
}

#[test]
fn test_from_data_root_prefers_bridge_and_falls_back_offline() {
    let tmp = tempfile::tempdir().unwrap();
    // No data anywhere -> deterministic offline seed fallback.
    let offline = NseTradingCalendar::from_data_root(tmp.path());
    assert!(offline.is_holiday(NaiveDate::from_ymd_opt(2025, 8, 15).unwrap()));

    // With the bridge projection under {root}/catalog/trading_holidays the
    // calendar becomes data-driven and no longer sees the seed's stale rows.
    let data_dir = tmp.path().join("catalog").join("trading_holidays");
    write_holiday_parquet(
        &data_dir,
        &[
            (
                NaiveDate::from_ymd_opt(2026, 1, 26).unwrap(),
                "Republic Day",
                "holiday",
            ),
            (
                NaiveDate::from_ymd_opt(2026, 11, 8).unwrap(),
                "Diwali Laxmi Pujan (Muhurat trading)",
                "muhurat",
            ),
        ],
    );
    let driven = NseTradingCalendar::from_data_root(tmp.path());
    assert!(driven.is_holiday(NaiveDate::from_ymd_opt(2026, 1, 26).unwrap()));
    assert!(driven.is_muhurat_session(NaiveDate::from_ymd_opt(2026, 11, 8).unwrap()));
    // The seed's 2025-08-15 holiday is NOT part of the data-driven calendar.
    assert!(!driven.is_holiday(NaiveDate::from_ymd_opt(2025, 8, 15).unwrap()));
}

#[test]
fn test_from_data_root_env_reads_tantra_data_dir() {
    // Unset -> offline deterministic fallback.
    // SAFETY: this test owns `TANTRA_DATA_DIR` (no other test reads it).
    unsafe { std::env::remove_var("TANTRA_DATA_DIR") };
    let cal = NseTradingCalendar::from_data_root_env();
    assert!(cal.is_holiday(NaiveDate::from_ymd_opt(2025, 8, 15).unwrap()));

    // Set to a data root carrying the bridge projection -> data-driven.
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("catalog").join("trading_holidays");
    write_holiday_parquet(
        &data_dir,
        &[(
            NaiveDate::from_ymd_opt(2026, 4, 3).unwrap(),
            "Good Friday",
            "holiday",
        )],
    );
    // SAFETY: this test owns `TANTRA_DATA_DIR` (no other test reads it).
    unsafe { std::env::set_var("TANTRA_DATA_DIR", tmp.path()) };
    let driven = NseTradingCalendar::from_data_root_env();
    assert!(driven.is_holiday(NaiveDate::from_ymd_opt(2026, 4, 3).unwrap()));
    assert!(!driven.is_holiday(NaiveDate::from_ymd_opt(2025, 8, 15).unwrap()));
    // SAFETY: restore so a later test's `from_data_root_env` call is unaffected.
    unsafe { std::env::remove_var("TANTRA_DATA_DIR") };
}
