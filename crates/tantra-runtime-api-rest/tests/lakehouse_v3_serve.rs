//! End-to-end integration tests verifying `tantra serve` (`prepare_server` + REST routes)
//! detects and serves Lakehouse v3 catalogs, ignores placeholder `catalog/.gitkeep`,
//! resolves symbology aliases, serves v3 block-deal disclosures, and supports pinned
//! `data_ref` backtests across dataset restatements.

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, LargeStringArray, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use parquet::basic::Compression;
use serde_json::Value;
use tantra_market_store::{ManifestEntry, commit_entries, create_tag, write_cas_batch};
use tantra_runtime_api_rest::{AppState, ServeConfig, ServeOutcome, prepare_server};
use tokio::sync::oneshot;

const UNIX_EPOCH_DAYS_FROM_CE: i64 = 719_163;

fn utc(y: i32, m: u32, d: u32, hh: u32, mm: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, hh, mm, 0).unwrap()
}

fn date32_days(d: NaiveDate) -> i32 {
    (i64::from(d.num_days_from_ce()) - UNIX_EPOCH_DAYS_FROM_CE) as i32
}

struct RunningServer {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    join: Option<std::thread::JoinHandle<anyhow::Result<ServeOutcome>>>,
}

impl RunningServer {
    fn start(mut cfg: ServeConfig) -> Self {
        cfg.runs.journals_dir = cfg.data_dir.join("_journals");
        cfg.runs.profiles_dir = cfg.data_dir.join("_profiles");
        cfg.runs.portfolios_dir = cfg.data_dir.join("_portfolios");
        let prepared = {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(prepare_server(&cfg)).unwrap()
        };
        let (bound_tx, bound_rx) = std::sync::mpsc::channel::<SocketAddr>();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();

        let join = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(prepared.serve_with_shutdown(
                move |addr, _| {
                    let _ = bound_tx.send(addr);
                },
                async move {
                    let _ = stop_rx.await;
                },
            ))
        });

        let addr = bound_rx.recv().unwrap();
        Self {
            addr,
            stop: Some(stop_tx),
            join: Some(join),
        }
    }

    fn shutdown(mut self) -> ServeOutcome {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        self.join.take().unwrap().join().unwrap().unwrap()
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn get(addr: SocketAddr, path: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).unwrap();
    write!(stream, "GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n").unwrap();
    let mut text = String::new();
    stream.read_to_string(&mut text).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

fn post(addr: SocketAddr, path: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).unwrap();
    write!(
        stream,
        "POST {path} HTTP/1.0\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut text = String::new();
    stream.read_to_string(&mut text).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

fn wait_terminal(addr: SocketAddr, id: &str) -> Value {
    for _ in 0..500 {
        let (_, body) = get(addr, &format!("/backtests/{id}"));
        if matches!(
            body["data"]["status"].as_str(),
            Some("completed" | "failed" | "cancelled")
        ) {
            return body;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!("run {id} never reached terminal state");
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
    let vols: Vec<Option<i128>> = rows
        .iter()
        .enumerate()
        .map(|(i, _)| (i % 5 != 0).then_some(100_000_000))
        .collect();

    let vol_arr: ArrayRef = Arc::new(
        Decimal128Array::from(vols)
            .with_precision_and_scale(18, 4)
            .unwrap(),
    );

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
            vol_arr,
        ],
    )
    .unwrap()
}

fn write_aliases(root: &Path) -> String {
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

fn write_block_deals(root: &Path) -> String {
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
    let dec_col = |v: i128| -> ArrayRef {
        Arc::new(
            Decimal128Array::from(vec![v])
                .with_precision_and_scale(18, 4)
                .unwrap(),
        )
    };
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![date32_days(
                NaiveDate::from_ymd_opt(2025, 5, 6).unwrap(),
            )])),
            Arc::new(StringArray::from(vec!["NSE"])),
            Arc::new(StringArray::from(vec!["NSE:RELIANCE"])),
            Arc::new(StringArray::from(vec!["Reliance Industries Ltd"])),
            Arc::new(StringArray::from(vec![" Sovereign Fund "])),
            Arc::new(StringArray::from(vec!["BUY"])),
            Arc::new(StringArray::from(vec!["block"])),
            dec_col(500_000_000),
            dec_col(14_000_000),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .unwrap();
    write_cas_batch(root, &batch, Compression::SNAPPY).unwrap()
}

#[test]
fn prepare_server_serves_lakehouse_v3_with_empty_catalog_gitkeep_and_pinned_backtests() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    // Mirror live `tantra-data/data/` where `catalog/.gitkeep` exists alongside v3 layout.
    fs::create_dir_all(root.join("catalog")).unwrap();
    fs::write(root.join("catalog/.gitkeep"), b"").unwrap();
    assert!(!AppState::has_catalog(root));

    let alias_sha = write_aliases(root);
    let block_sha = write_block_deals(root);

    // 60 daily bars starting 2025-02-01 for RELIANCE + 6 ZOMATO->ETERNAL bars.
    let start_day = utc(2025, 2, 1, 3, 45);
    let mut rows_v1 = Vec::new();
    for i in 0..60_i64 {
        let ts = start_day + Duration::days(i);
        // Oscillating curve that finishes mildly positive under sma_crossover
        let px = i128::from(1000_0000 + (i % 12) * 25_0000);
        rows_v1.push(("INE002A01018", "RELIANCE", ts, px));
    }
    for i in 0..6_i64 {
        let ts = utc(2025, 4, 5 + u32::try_from(i).unwrap(), 3, 45);
        let sym = if i < 3 { "ZOMATO" } else { "ETERNAL" };
        rows_v1.push(("INE758T01015", sym, ts, i128::from(220_0000 + i * 5_0000)));
    }
    let bars_1d_v1_sha = write_cas_batch(root, &bar_batch(&rows_v1), Compression::SNAPPY).unwrap();

    let rows_5m = vec![
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 4, 2, 3, 45),
            1100_0000_i128,
        ),
        (
            "INE002A01018",
            "RELIANCE",
            utc(2025, 4, 2, 3, 50),
            1105_0000_i128,
        ),
    ];
    let bars_5m_sha = write_cas_batch(root, &bar_batch(&rows_5m), Compression::SNAPPY).unwrap();

    let d1 = NaiveDate::from_ymd_opt(2025, 4, 15).unwrap();
    let c1 = commit_entries(
        root,
        "v3 baseline snapshot",
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
                min_ts: utc(2025, 1, 1, 0, 0),
                max_ts: utc(2025, 12, 31, 23, 59),
                row_count: 3,
                sort_key: "symbol".into(),
            },
            ManifestEntry {
                domain: "disclosures".into(),
                dataset: "block_deals".into(),
                exchange: Some("NSE".into()),
                asset_class: None,
                timeframe: None,
                time_bucket: "year=2025".into(),
                object_sha: block_sha.clone(),
                min_ts: utc(2025, 5, 6, 0, 0),
                max_ts: utc(2025, 5, 6, 23, 59),
                row_count: 1,
                sort_key: "trade_date".into(),
            },
            ManifestEntry {
                domain: "market".into(),
                dataset: "bars".into(),
                exchange: Some("NSE".into()),
                asset_class: Some("equity".into()),
                timeframe: Some("1d".into()),
                time_bucket: "year=2025".into(),
                object_sha: bars_1d_v1_sha,
                min_ts: utc(2025, 2, 1, 3, 45),
                max_ts: utc(2025, 4, 15, 3, 45),
                row_count: rows_v1.len(),
                sort_key: "(instrument_uid, ts)".into(),
            },
            ManifestEntry {
                domain: "market".into(),
                dataset: "bars".into(),
                exchange: Some("NSE".into()),
                asset_class: Some("equity".into()),
                timeframe: Some("5m".into()),
                time_bucket: "year=2025/month=04".into(),
                object_sha: bars_5m_sha.clone(),
                min_ts: utc(2025, 4, 2, 3, 45),
                max_ts: utc(2025, 4, 2, 3, 50),
                row_count: rows_5m.len(),
                sort_key: "(instrument_uid, ts)".into(),
            },
        ],
        None,
        None,
    )
    .unwrap();
    create_tag(root, "baseline", &c1.commit_sha).unwrap();

    // Commit 2 (restatement on main): double exit prices for final 20 days of RELIANCE
    let mut rows_v2 = Vec::new();
    for i in 0..60_i64 {
        let ts = start_day + Duration::days(i);
        let px = if i >= 40 {
            i128::from(2500_0000 + (i - 40) * 50_0000)
        } else {
            i128::from(1000_0000 + (i % 12) * 25_0000)
        };
        rows_v2.push(("INE002A01018", "RELIANCE", ts, px));
    }
    for i in 0..6_i64 {
        let ts = utc(2025, 4, 5 + u32::try_from(i).unwrap(), 3, 45);
        let sym = if i < 3 { "ZOMATO" } else { "ETERNAL" };
        rows_v2.push(("INE758T01015", sym, ts, i128::from(220_0000 + i * 5_0000)));
    }
    let bars_1d_v2_sha = write_cas_batch(root, &bar_batch(&rows_v2), Compression::SNAPPY).unwrap();

    let d2 = NaiveDate::from_ymd_opt(2025, 4, 16).unwrap();
    let c2 = commit_entries(
        root,
        "restated main snapshot",
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
                min_ts: utc(2025, 1, 1, 0, 0),
                max_ts: utc(2025, 12, 31, 23, 59),
                row_count: 3,
                sort_key: "symbol".into(),
            },
            ManifestEntry {
                domain: "disclosures".into(),
                dataset: "block_deals".into(),
                exchange: Some("NSE".into()),
                asset_class: None,
                timeframe: None,
                time_bucket: "year=2025".into(),
                object_sha: block_sha,
                min_ts: utc(2025, 5, 6, 0, 0),
                max_ts: utc(2025, 5, 6, 23, 59),
                row_count: 1,
                sort_key: "trade_date".into(),
            },
            ManifestEntry {
                domain: "market".into(),
                dataset: "bars".into(),
                exchange: Some("NSE".into()),
                asset_class: Some("equity".into()),
                timeframe: Some("1d".into()),
                time_bucket: "year=2025".into(),
                object_sha: bars_1d_v2_sha,
                min_ts: utc(2025, 2, 1, 3, 45),
                max_ts: utc(2025, 4, 15, 3, 45),
                row_count: rows_v2.len(),
                sort_key: "(instrument_uid, ts)".into(),
            },
            ManifestEntry {
                domain: "market".into(),
                dataset: "bars".into(),
                exchange: Some("NSE".into()),
                asset_class: Some("equity".into()),
                timeframe: Some("5m".into()),
                time_bucket: "year=2025/month=04".into(),
                object_sha: bars_5m_sha,
                min_ts: utc(2025, 4, 2, 3, 45),
                max_ts: utc(2025, 4, 2, 3, 50),
                row_count: rows_5m.len(),
                sort_key: "(instrument_uid, ts)".into(),
            },
        ],
        Some(&c1),
        None,
    )
    .unwrap();
    assert_ne!(c1.commit_sha, c2.commit_sha);
    assert!(AppState::has_lakehouse_v3(root));

    let mut cfg = ServeConfig::new(root);
    cfg.addr = "127.0.0.1:0".parse().unwrap();
    cfg.runs.max_concurrent_runs = Some(1);
    let srv = RunningServer::start(cfg);

    // 1. Canonical instruments list includes ETERNAL and RELIANCE, NOT legacy ZOMATO
    let (status, body) = get(srv.addr, "/instruments");
    assert_eq!(status, 200, "{body}");
    let inst_ids: Vec<&str> = body["data"]["instruments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["id"].as_str().unwrap())
        .collect();
    assert_eq!(inst_ids, vec!["NSE:ETERNAL", "NSE:RELIANCE"]);

    // 2. Alias continuity through REST `/bars/ETERNAL.NSE?tf=1d` (includes pre-rename ZOMATO bars)
    let (status, body) = get(srv.addr, "/bars/ETERNAL.NSE?tf=1d");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["bars"].as_array().unwrap().len(), 6);

    // 3. Multi-timeframe 5m bars
    let (status, body) = get(srv.addr, "/bars/RELIANCE.NSE?tf=5m");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["bars"].as_array().unwrap().len(), 2);

    // 4. Quotes work over v3
    let (status, body) = get(srv.addr, "/quotes?symbols=RELIANCE");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["quotes"].as_array().unwrap().len(), 1);

    // 5. Block-deal disclosures work over v3 (`disclosures/block_deals`)
    let (status, body) = get(srv.addr, "/market-data/block-deals");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["deals"].as_array().unwrap().len(), 1);

    // 6. Empty `data_ref` rejected with 422
    let (status, body) = post(
        srv.addr,
        "/backtests",
        r#"{"strategy":"buy_and_hold","universe":"RELIANCE.NSE","start":"2025-02-01","end":"2025-04-05","bar_spec":"1d","seed":7,"data_ref":"   "}"#,
    );
    assert_eq!(status, 422, "{body}");
    assert_eq!(body["error"]["context"]["field"], "data_ref");
    assert_eq!(body["error"]["context"]["reason"], "empty");

    // 7. Pinned backtest at `refs/tags/baseline` executes against commit 1
    let (status, body) = post(
        srv.addr,
        "/backtests",
        r#"{"strategy":"buy_and_hold","universe":"RELIANCE.NSE","start":"2025-02-01","end":"2025-04-05","bar_spec":"1d","seed":7,"data_ref":"refs/tags/baseline"}"#,
    );
    assert_eq!(status, 200, "{body}");
    let baseline_id = body["data"]["run_id"].as_str().unwrap().to_owned();
    let baseline_done = wait_terminal(srv.addr, &baseline_id);
    assert_eq!(
        baseline_done["data"]["status"], "completed",
        "{baseline_done}"
    );
    assert_eq!(
        baseline_done["data"]["assumptions"]["data_ref"],
        "refs/tags/baseline"
    );
    assert_eq!(
        baseline_done["data"]["assumptions"]["data_commit_sha"],
        c1.commit_sha
    );
    assert_eq!(
        baseline_done["data"]["assumptions"]["data_as_of_ist"],
        d1.to_string()
    );
    let baseline_pnl = baseline_done["data"]["metrics"]["net_pnl"]
        .as_str()
        .unwrap()
        .to_owned();

    // Verify `manifest.json` on disk also persists `data_commit_sha` & `data_as_of_ist`
    let baseline_manifest: Value = serde_json::from_str(
        &fs::read_to_string(
            root.join("_journals")
                .join(&baseline_id)
                .join("manifest.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(baseline_manifest["data_commit_sha"], c1.commit_sha);
    assert_eq!(baseline_manifest["data_as_of_ist"], d1.to_string());

    // 8. Default backtest (unpinned) runs against `main` (commit 2), sees restated exit prices
    let (status, body) = post(
        srv.addr,
        "/backtests",
        r#"{"strategy":"buy_and_hold","universe":"RELIANCE.NSE","start":"2025-02-01","end":"2025-04-05","bar_spec":"1d","seed":7}"#,
    );
    assert_eq!(status, 200, "{body}");
    let main_id = body["data"]["run_id"].as_str().unwrap().to_owned();
    let main_done = wait_terminal(srv.addr, &main_id);
    assert_eq!(main_done["data"]["status"], "completed", "{main_done}");
    assert_eq!(
        main_done["data"]["assumptions"]["data_commit_sha"],
        c2.commit_sha
    );
    assert_eq!(
        main_done["data"]["assumptions"]["data_as_of_ist"],
        d2.to_string()
    );
    let main_pnl = main_done["data"]["metrics"]["net_pnl"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(
        main_pnl, baseline_pnl,
        "unpinned main (commit 2) should differ from restated baseline tag (commit 1)"
    );

    // 9. Unknown `data_ref` fails with clear error
    let (status, body) = post(
        srv.addr,
        "/backtests",
        r#"{"strategy":"buy_and_hold","universe":"RELIANCE.NSE","start":"2025-02-01","end":"2025-04-05","bar_spec":"1d","seed":7,"data_ref":"refs/tags/missing"}"#,
    );
    assert_eq!(status, 200, "{body}");
    let missing_id = body["data"]["run_id"].as_str().unwrap().to_owned();
    let missing_done = wait_terminal(srv.addr, &missing_id);
    assert_eq!(missing_done["data"]["status"], "failed", "{missing_done}");
    assert_eq!(
        missing_done["data"]["error"]["context"]["reason"],
        "data_ref_not_found"
    );

    srv.shutdown();
}
