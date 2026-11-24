//! Integration tests for Lakehouse v3 manifest pinning, RowGroup pruning,
//! and symbology alias continuity (`tantra-runtime/docs/gaps/0011`).

use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, LargeStringArray, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rust_decimal_macros::dec;
use tantra_core::{Exchange, InstrumentId, Quantity};
use tantra_market_store::{ManifestEntry, commit_entries, create_tag, write_cas_batch};
use tantra_risk::{OtrConfig, RiskEngine, RiskLimits};
use tantra_runtime_backtest::{
    BacktestConfig, BacktestEngine, CatalogError, CatalogLoader, NseTradingCalendar,
};
use tantra_runtime_strategy::{OrderIntent, Strategy, StrategyContext};
use tempfile::tempdir;

const UNIX_EPOCH_DAYS_FROM_CE: i64 = 719_163;

fn utc(y: i32, m: u32, d: u32, hh: u32, mm: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, hh, mm, 0).unwrap()
}

fn date32_days(d: NaiveDate) -> i32 {
    (i64::from(d.num_days_from_ce()) - UNIX_EPOCH_DAYS_FROM_CE) as i32
}

struct ScriptedMomentumBuyer {
    instrument: InstrumentId,
    bar_idx: usize,
}

impl Strategy for ScriptedMomentumBuyer {
    fn name(&self) -> &str {
        "scripted_momentum_buyer"
    }

    fn on_bar(
        &mut self,
        ctx: &mut dyn StrategyContext,
        _bar: &tantra_core::Bar,
    ) -> tantra_runtime_strategy::Result<()> {
        self.bar_idx += 1;
        if self.bar_idx == 1 {
            ctx.submit(OrderIntent::market_buy(
                self.instrument.clone(),
                Quantity::from_u64(10),
            ));
        } else if self.bar_idx == 3 && !ctx.busy(&self.instrument) {
            ctx.submit(OrderIntent::market_sell(
                self.instrument.clone(),
                Quantity::from_u64(10),
            ));
        }
        Ok(())
    }
}

fn risk_engine_for(algo_id: &str) -> RiskEngine {
    let mut risk = RiskEngine::new(RiskLimits::default(), OtrConfig::default());
    risk.register_algo(algo_id);
    risk
}

fn bar_batch(rows: &[(&str, &str, DateTime<Utc>, i128)]) -> RecordBatch {
    let dec = |vals: Vec<i128>| -> ArrayRef {
        Arc::new(
            Decimal128Array::from(vals)
                .with_precision_and_scale(18, 4)
                .unwrap(),
        )
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("instrument_uid", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("open", DataType::Decimal128(18, 4), false),
        Field::new("high", DataType::Decimal128(18, 4), false),
        Field::new("low", DataType::Decimal128(18, 4), false),
        Field::new("close", DataType::Decimal128(18, 4), false),
        Field::new("volume", DataType::Decimal128(18, 4), true),
    ]));

    let uids: Vec<&str> = rows.iter().map(|r| r.0).collect();
    let syms: Vec<&str> = rows.iter().map(|r| r.1).collect();
    let tss: Vec<i64> = rows
        .iter()
        .map(|r| r.2.timestamp_nanos_opt().unwrap())
        .collect();
    let opens: Vec<i128> = rows.iter().map(|r| r.3).collect();
    let highs: Vec<i128> = rows.iter().map(|r| r.3 + 10_000).collect();
    let lows: Vec<i128> = rows.iter().map(|r| r.3 - 10_000).collect();
    let closes: Vec<i128> = rows.iter().map(|r| r.3).collect();
    let vols: Vec<i128> = vec![100_000_000; rows.len()];

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(uids)),
            Arc::new(StringArray::from(syms)),
            Arc::new(TimestampNanosecondArray::from(tss).with_timezone("UTC")),
            dec(opens),
            dec(highs),
            dec(lows),
            dec(closes),
            dec(vols),
        ],
    )
    .unwrap()
}

fn write_multi_rg_cas(root: &Path, batch: &RecordBatch, max_rg_rows: usize) -> String {
    let tmp_dir = root.join("objects/sha256/.tmp");
    fs::create_dir_all(&tmp_dir).unwrap();
    let tmp_file = tmp_dir.join("multi_rg.parquet");
    {
        let file = File::create(&tmp_file).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(max_rg_rows)
            .build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    let bytes = fs::read(&tmp_file).unwrap();
    let sha = {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(&bytes);
        hex::encode(hasher.finalize())
    };
    let dest = root
        .join("objects/sha256")
        .join(&sha[..2])
        .join(format!("{sha}.parquet"));
    fs::create_dir_all(dest.parent().unwrap()).unwrap();
    fs::rename(&tmp_file, &dest).unwrap();
    sha
}

fn write_zomato_eternal_aliases(root: &Path) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("exchange", DataType::LargeUtf8, false),
        Field::new("symbol", DataType::LargeUtf8, false),
        Field::new("series", DataType::LargeUtf8, true),
        Field::new("valid_from", DataType::Date32, true),
        Field::new("valid_to", DataType::Date32, true),
        Field::new("instrument_uid", DataType::LargeUtf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(LargeStringArray::from(vec![
                Some("NSE"),
                Some("NSE"),
                Some("NSE"),
            ])),
            Arc::new(LargeStringArray::from(vec![
                Some("ZOMATO"),
                Some("ETERNAL"),
                Some("RELIANCE"),
            ])),
            Arc::new(LargeStringArray::from(vec![
                Some("EQ"),
                Some("EQ"),
                Some("EQ"),
            ])),
            Arc::new(Date32Array::from(vec![
                Some(date32_days(NaiveDate::from_ymd_opt(2021, 7, 23).unwrap())),
                Some(date32_days(NaiveDate::from_ymd_opt(2025, 4, 9).unwrap())),
                None,
            ])),
            Arc::new(Date32Array::from(vec![
                Some(date32_days(NaiveDate::from_ymd_opt(2025, 4, 8).unwrap())),
                None,
                None,
            ])),
            Arc::new(LargeStringArray::from(vec![
                Some("INE758T01015"),
                Some("INE758T01015"),
                Some("INE002A01018"),
            ])),
        ],
    )
    .unwrap();
    write_cas_batch(root, &batch, Compression::SNAPPY).unwrap()
}

fn write_holidays_cas(root: &Path) -> String {
    let schema = Arc::new(Schema::new(vec![
        Field::new("holiday_date", DataType::Date32, false),
        Field::new("kind", DataType::LargeUtf8, false),
        Field::new("description", DataType::LargeUtf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![date32_days(
                NaiveDate::from_ymd_opt(2025, 1, 26).unwrap(),
            )])),
            Arc::new(LargeStringArray::from(vec!["clearing"])),
            Arc::new(LargeStringArray::from(vec!["Republic Day"])),
        ],
    )
    .unwrap();
    write_cas_batch(root, &batch, Compression::SNAPPY).unwrap()
}

#[test]
fn pinned_tag_produces_bit_identical_backtest_across_restatement() {
    let tmp = tempdir().unwrap();
    let root = tmp.path();
    let inst = InstrumentId::try_new(Exchange::Nse, "RELIANCE").unwrap();

    let alias_sha = write_zomato_eternal_aliases(root);
    let holiday_sha = write_holidays_cas(root);

    // Commit 1 (baseline): close on day 3 = 1100.00
    let batch1 = bar_batch(&[
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 5, 5, 3, 45),
            1000_0000,
        ),
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 5, 6, 3, 45),
            1050_0000,
        ),
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 5, 7, 3, 45),
            1100_0000,
        ),
    ]);
    let bars_sha1 = write_cas_batch(root, &batch1, Compression::SNAPPY).unwrap();
    let d1 = NaiveDate::from_ymd_opt(2025, 5, 7).unwrap();
    let c1 = commit_entries(
        root,
        "baseline snapshot",
        d1,
        vec![
            ManifestEntry {
                domain: "reference".into(),
                dataset: "aliases".into(),
                exchange: Some("NSE".into()),
                asset_class: None,
                timeframe: None,
                time_bucket: "snapshot=2025".into(),
                object_sha: alias_sha.clone(),
                min_ts: utc(2025, 5, 5, 0, 0),
                max_ts: utc(2025, 5, 7, 23, 59),
                row_count: 3,
                sort_key: "symbol".into(),
            },
            ManifestEntry {
                domain: "reference".into(),
                dataset: "trading_holidays".into(),
                exchange: Some("NSE".into()),
                asset_class: None,
                timeframe: None,
                time_bucket: "year=2025".into(),
                object_sha: holiday_sha.clone(),
                min_ts: utc(2025, 1, 1, 0, 0),
                max_ts: utc(2025, 12, 31, 23, 59),
                row_count: 1,
                sort_key: "holiday_date".into(),
            },
            ManifestEntry {
                domain: "market".into(),
                dataset: "bars".into(),
                exchange: Some("NSE".into()),
                asset_class: Some("equity".into()),
                timeframe: Some("1d".into()),
                time_bucket: "year=2025".into(),
                object_sha: bars_sha1,
                min_ts: utc(2025, 5, 5, 3, 45),
                max_ts: utc(2025, 5, 7, 3, 45),
                row_count: 3,
                sort_key: "(instrument_uid, ts)".into(),
            },
        ],
        None,
        None,
    )
    .unwrap();
    create_tag(root, "baseline", &c1.commit_sha).unwrap();

    // Run 1 at baseline tag before mutation
    let run_with_loader = |loader: &CatalogLoader| {
        let cal = Arc::new(NseTradingCalendar::from_data_root_at(
            root,
            loader.data_ref(),
        ));
        let mut engine = BacktestEngine::new(
            ScriptedMomentumBuyer {
                instrument: inst.clone(),
                bar_idx: 0,
            },
            risk_engine_for("scripted_momentum_buyer"),
            BacktestConfig::default(),
            cal,
        );
        engine
            .run_catalog(loader, std::slice::from_ref(&inst), "1d", None, None)
            .unwrap()
    };

    let pinned_loader = CatalogLoader::new(root).with_ref("refs/tags/baseline");
    let before = run_with_loader(&pinned_loader);
    assert_eq!(before.total_trades, 2);

    // Commit 2 (restatement): day 3 close adjusted from 1100 to 1300 (splits/backfill correction)
    let batch2 = bar_batch(&[
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 5, 5, 3, 45),
            1000_0000,
        ),
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 5, 6, 3, 45),
            1050_0000,
        ),
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 5, 7, 3, 45),
            1300_0000,
        ),
    ]);
    let bars_sha2 = write_cas_batch(root, &batch2, Compression::SNAPPY).unwrap();
    let d2 = NaiveDate::from_ymd_opt(2025, 5, 8).unwrap();
    let c2 = commit_entries(
        root,
        "restated snapshot",
        d2,
        vec![
            ManifestEntry {
                domain: "reference".into(),
                dataset: "aliases".into(),
                exchange: Some("NSE".into()),
                asset_class: None,
                timeframe: None,
                time_bucket: "snapshot=2025".into(),
                object_sha: alias_sha,
                min_ts: utc(2025, 5, 5, 0, 0),
                max_ts: utc(2025, 5, 7, 23, 59),
                row_count: 3,
                sort_key: "symbol".into(),
            },
            ManifestEntry {
                domain: "market".into(),
                dataset: "bars".into(),
                exchange: Some("NSE".into()),
                asset_class: Some("equity".into()),
                timeframe: Some("1d".into()),
                time_bucket: "year=2025".into(),
                object_sha: bars_sha2,
                min_ts: utc(2025, 5, 5, 3, 45),
                max_ts: utc(2025, 5, 7, 3, 45),
                row_count: 3,
                sort_key: "(instrument_uid, ts)".into(),
            },
        ],
        Some(&c1),
        None,
    )
    .unwrap();
    assert_ne!(c1.commit_sha, c2.commit_sha);

    // Run 2 pinned at baseline tag: MUST be bit-identical to `before`!
    let after_pinned = run_with_loader(&pinned_loader);
    assert_eq!(after_pinned.fills, before.fills);
    assert_eq!(after_pinned.equity_curve, before.equity_curve);
    assert_eq!(after_pinned.net_pnl, before.net_pnl);

    // Run 3 on unpinned `main`: sees the restated +200/share higher exit price!
    let main_loader = CatalogLoader::new(root);
    let after_main = run_with_loader(&main_loader);
    assert!(after_main.net_pnl > before.net_pnl);

    // Verification report captures commit SHA and as_of_ist
    let report = pinned_loader.verify_integrity(Some("1d")).unwrap();
    assert!(report.is_clean(), "{report:?}");
    assert_eq!(
        report.data_commit_sha.as_deref(),
        Some(c1.commit_sha.as_str())
    );
    assert_eq!(report.data_as_of_ist, Some(d1));

    let main_report = main_loader.verify_integrity(Some("1d")).unwrap();
    assert_eq!(
        main_report.data_commit_sha.as_deref(),
        Some(c2.commit_sha.as_str())
    );
    assert_eq!(main_report.data_as_of_ist, Some(d2));
}

#[test]
fn multi_instrument_row_group_pruning_and_alias_continuity() {
    let tmp = tempdir().unwrap();
    let root = tmp.path();

    let alias_sha = write_zomato_eternal_aliases(root);

    // 4 instruments sorted by (instrument_uid, ts), 4 bars each, max 4 rows per RowGroup => 4 RowGroups!
    let rows = vec![
        // RG 0: INE001A01010 (ALPHA)
        ("INE001A01010", "ALPHA", utc(2025, 1, 2, 3, 45), 100_0000),
        ("INE001A01010", "ALPHA", utc(2025, 1, 3, 3, 45), 101_0000),
        ("INE001A01010", "ALPHA", utc(2025, 1, 6, 3, 45), 102_0000),
        ("INE001A01010", "ALPHA", utc(2025, 1, 7, 3, 45), 103_0000),
        // RG 1: INE002A01018 (RELIANCE)
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 1, 2, 3, 45),
            2500_0000,
        ),
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 1, 3, 3, 45),
            2510_0000,
        ),
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 1, 6, 3, 45),
            2520_0000,
        ),
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 1, 7, 3, 45),
            2530_0000,
        ),
        // RG 2: INE758T01015 (ZOMATO -> ETERNAL rename!)
        ("INE758T01015", "ZOMATO", utc(2025, 4, 7, 3, 45), 210_0000),
        ("INE758T01015", "ZOMATO", utc(2025, 4, 8, 3, 45), 215_0000),
        ("INE758T01015", "ETERNAL", utc(2025, 4, 9, 3, 45), 220_0000),
        ("INE758T01015", "ETERNAL", utc(2025, 4, 10, 3, 45), 225_0000),
        // RG 3: INE999Z01099 (OMEGA)
        ("INE999Z01099", "OMEGA", utc(2025, 1, 2, 3, 45), 500_0000),
        ("INE999Z01099", "OMEGA", utc(2025, 1, 3, 3, 45), 505_0000),
        ("INE999Z01099", "OMEGA", utc(2025, 1, 6, 3, 45), 510_0000),
        ("INE999Z01099", "OMEGA", utc(2025, 1, 7, 3, 45), 515_0000),
    ];
    let batch = bar_batch(&rows);
    let bars_sha = write_multi_rg_cas(root, &batch, 4);

    let d1 = NaiveDate::from_ymd_opt(2025, 4, 10).unwrap();
    commit_entries(
        root,
        "multi-instrument 4 row groups",
        d1,
        vec![
            ManifestEntry {
                domain: "reference".into(),
                dataset: "aliases".into(),
                exchange: Some("NSE".into()),
                asset_class: None,
                timeframe: None,
                time_bucket: "snapshot=2025".into(),
                object_sha: alias_sha,
                min_ts: utc(2025, 1, 1, 0, 0),
                max_ts: utc(2025, 4, 10, 23, 59),
                row_count: 3,
                sort_key: "symbol".into(),
            },
            ManifestEntry {
                domain: "market".into(),
                dataset: "bars".into(),
                exchange: Some("NSE".into()),
                asset_class: Some("equity".into()),
                timeframe: Some("1d".into()),
                time_bucket: "year=2025".into(),
                object_sha: bars_sha,
                min_ts: utc(2025, 1, 2, 3, 45),
                max_ts: utc(2025, 4, 10, 3, 45),
                row_count: rows.len(),
                sort_key: "(instrument_uid, ts)".into(),
            },
        ],
        None,
        None,
    )
    .unwrap();

    let loader = CatalogLoader::new(root);

    // 1. RowGroup pruning: loading RELIANCE opens 4 total RowGroups and reads ONLY 1!
    let reliance = InstrumentId::try_new(Exchange::Nse, "RELIANCE").unwrap();
    let (rel_bars, stats) = loader
        .load_bars_with_stats(&reliance, "1d", None, None)
        .unwrap();
    assert_eq!(stats.row_groups_total, 4);
    assert_eq!(stats.row_groups_read, 1);
    assert_eq!(rel_bars.len(), 4);
    assert_eq!(rel_bars[0].close.value(), dec!(2500.0000));

    // 2. Time-range pruning: querying ZOMATO window that falls outside all RG ts bounds reads 0 RowGroups!
    let (_empty_bars, ts_pruned_stats) = loader
        .load_bars_with_stats(
            &InstrumentId::try_new(Exchange::Nse, "ETERNAL").unwrap(),
            "1d",
            Some(utc(2025, 1, 1, 0, 0)),
            Some(utc(2025, 3, 1, 0, 0)),
        )
        .unwrap();
    assert_eq!(ts_pruned_stats.row_groups_total, 4);
    assert_eq!(ts_pruned_stats.row_groups_read, 0);

    // 3. Alias continuity: querying NSE:ETERNAL returns BOTH historical ZOMATO bars AND post-rename ETERNAL bars!
    let eternal = InstrumentId::try_new(Exchange::Nse, "ETERNAL").unwrap();
    let (et_bars, et_stats) = loader
        .load_bars_with_stats(&eternal, "1d", None, None)
        .unwrap();
    assert_eq!(et_stats.row_groups_total, 4);
    assert_eq!(et_stats.row_groups_read, 1);
    assert_eq!(et_bars.len(), 4);
    assert_eq!(et_bars[0].timestamp, utc(2025, 4, 7, 3, 45));
    assert_eq!(et_bars[3].timestamp, utc(2025, 4, 10, 3, 45));
    assert!(et_bars.iter().all(|b| b.instrument == eternal));

    // 4. Listing instruments resolves INE758T01015 to canonical NSE:ETERNAL
    let instruments = loader.list_instruments("1d").unwrap();
    assert!(instruments.contains(&reliance));
    assert!(instruments.contains(&eternal));
    assert!(!instruments.contains(&InstrumentId::try_new(Exchange::Nse, "ZOMATO").unwrap()));
}

#[test]
fn legacy_fallback_can_be_disabled() {
    let tmp = tempdir().unwrap();
    let sym_dir = tmp.path().join("catalog/1D/NSE/TCS");
    fs::create_dir_all(&sym_dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("open", DataType::Decimal128(18, 4), false),
        Field::new("high", DataType::Decimal128(18, 4), false),
        Field::new("low", DataType::Decimal128(18, 4), false),
        Field::new("close", DataType::Decimal128(18, 4), false),
        Field::new("volume", DataType::Decimal128(18, 4), false),
    ]));
    let dec = |v: i128| -> ArrayRef {
        Arc::new(
            Decimal128Array::from(vec![Some(v)])
                .with_precision_and_scale(18, 4)
                .unwrap(),
        )
    };
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![
                utc(2025, 1, 2, 3, 45).timestamp_nanos_opt().unwrap(),
            ])),
            dec(3500_0000),
            dec(3520_0000),
            dec(3490_0000),
            dec(3510_0000),
            dec(100_0000),
        ],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(
        File::create(sym_dir.join("2025.parquet")).unwrap(),
        schema,
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let tcs = InstrumentId::try_new(Exchange::Nse, "TCS").unwrap();
    assert_eq!(
        CatalogLoader::new(tmp.path())
            .load_bars(&tcs, "1D", None, None)
            .unwrap()
            .len(),
        1
    );

    let err = CatalogLoader::new(tmp.path())
        .with_legacy_fallback(false)
        .load_bars(&tcs, "1D", None, None)
        .unwrap_err();
    assert!(matches!(err, CatalogError::NotFound { .. }));
}
