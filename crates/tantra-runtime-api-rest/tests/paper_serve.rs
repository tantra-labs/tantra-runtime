//! R6 acceptance tests for the standalone `paper serve` daemon (plan 0005, D5, T14.9).

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::json;
use tantra_core::Bar;
use tantra_runtime_api_rest::paper_serve::{
    PaperFeed, PaperServeConfig, parse_tape, run_tape_serve,
};
use tempfile::TempDir;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// Writes a 3-bar RELIANCE tape: closes 100 / 101 / 102 with bar 1 dipping to a low of 99, so
/// a resting buy limit @ 100 is crossed by the first replay bar under either close- or
/// range-based matching.
fn write_tape(dir: &Path) -> PathBuf {
    let tape = json!({
        "instruments": [{
            "symbol": "RELIANCE", "exchange": "NSE", "kind": "equity",
            "currency": "INR", "lot_size": 1, "tick_size": 0.05
        }],
        "bars": [
            {"symbol": "RELIANCE", "exchange": "NSE", "open": "100", "high": "101",
             "low": "99", "close": "100", "volume": "1000", "timestamp": "2026-01-15T09:15:00Z"},
            {"symbol": "RELIANCE", "exchange": "NSE", "open": "100", "high": "102",
             "low": "100", "close": "101", "volume": "1000", "timestamp": "2026-01-15T09:30:00Z"},
            {"symbol": "RELIANCE", "exchange": "NSE", "open": "101", "high": "103",
             "low": "101", "close": "102", "volume": "1000", "timestamp": "2026-01-15T09:45:00Z"}
        ]
    });
    let path = dir.join("tape.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&tape).unwrap()).unwrap();
    path
}

/// The daemon boots, serves `/ws/live`, fills a resting limit when the tape crosses it, and
/// exits cleanly once the tape ends.
#[tokio::test]
async fn tape_daemon_boots_fills_orders_and_exits_on_tape_end() {
    let dir = TempDir::new().unwrap();
    let tape = write_tape(dir.path());

    let config = PaperServeConfig {
        tape,
        addr: "127.0.0.1:0".parse().unwrap(),
        initial_cash: Decimal::new(1_000_000, 0),
        real_time: false,
        mis_square_off: None,
        feed: PaperFeed::Tape(Vec::new()),
    };

    let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
    let runner = tokio::spawn(run_tape_serve(config, move |addr| {
        let _ = addr_tx.send(addr);
    }));

    let addr = tokio::time::timeout(Duration::from_secs(5), addr_rx)
        .await
        .expect("daemon never bound a listener")
        .expect("on_bound never fired");

    let (mut ws, _) = connect_async(format!("ws://{addr}/ws/live"))
        .await
        .expect("connect /ws/live");
    let order = json!({
        "action": "place_order",
        "order": {
            "instrument_id": {"symbol": "RELIANCE", "exchange": "NSE"},
            "side": "buy",
            "qty": "25",
            "order_type": "limit",
            "price": "100",
            "client_order_id": "tape-1"
        }
    });
    ws.send(Message::Text(order.to_string()))
        .await
        .expect("send place_order");

    let mut saw_place_result = false;
    let mut place_status: Option<u64> = None;
    let mut saw_fill = false;
    let mut fill_event = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), ws.next()).await {
            Ok(Some(Ok(message))) => {
                let Ok(text) = message.to_text() else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
                    continue;
                };
                match value.get("type").and_then(|t| t.as_str()) {
                    Some("PlaceResult") => {
                        saw_place_result = true;
                        place_status = value.get("status").and_then(|s| s.as_u64());
                    }
                    Some("Fill") => {
                        saw_fill = true;
                        fill_event = value.to_string();
                        break;
                    }
                    _ => {}
                }
            }
            _ => break,
        }
    }

    // Release the server's graceful shutdown by closing our only WebSocket; the daemon cannot
    // exit until the in-flight connection finishes.
    drop(ws);

    let outcome = tokio::time::timeout(Duration::from_secs(15), runner)
        .await
        .expect("daemon did not exit after the tape ended")
        .expect("daemon task panicked")
        .expect("daemon returned an error");

    println!("R6 outcome: {outcome:?}");
    println!("R6 fill event: {fill_event}");

    assert!(
        saw_place_result,
        "expected a PlaceResult reply from /ws/live"
    );
    assert_eq!(place_status, Some(200), "expected PlaceResult status 200");
    assert!(saw_fill, "expected a Fill broadcast for the resting limit");
    assert_eq!(outcome.bars_fed, 3, "all three tape bars should be fed");
    assert!(outcome.fills >= 1, "the crossed limit should fill");
    assert_eq!(outcome.orders, 1, "exactly one order was placed");
}

/// The `Live` feed seam (plan 0005 D5 follow-on): bars pushed through an injected channel are
/// replayed through the same advance / square-off pipeline as a tape — a resting buy limit is
/// crossed by a live bar — and the daemon exits cleanly once the sender is dropped.
#[tokio::test]
async fn live_feed_replays_channel_bars_until_sender_dropped() {
    let dir = TempDir::new().unwrap();
    let tape_path = write_tape(dir.path());
    let bars = parse_tape(&tape_path).unwrap().bars;

    let (tx, rx) = tokio::sync::mpsc::channel::<Bar>(4);
    let config = PaperServeConfig {
        tape: tape_path,
        addr: "127.0.0.1:0".parse().unwrap(),
        initial_cash: Decimal::new(1_000_000, 0),
        real_time: false,
        mis_square_off: None,
        feed: PaperFeed::Live(rx),
    };

    let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
    let runner = tokio::spawn(run_tape_serve(config, move |addr| {
        let _ = addr_tx.send(addr);
    }));

    let addr = tokio::time::timeout(Duration::from_secs(5), addr_rx)
        .await
        .expect("daemon never bound a listener")
        .expect("on_bound never fired");

    let (mut ws, _) = connect_async(format!("ws://{addr}/ws/live"))
        .await
        .expect("connect /ws/live");
    let order = json!({
        "action": "place_order",
        "order": {
            "instrument_id": {"symbol": "RELIANCE", "exchange": "NSE"},
            "side": "buy",
            "qty": "25",
            "order_type": "limit",
            "price": "100",
            "client_order_id": "live-1"
        }
    });
    ws.send(Message::Text(order.to_string()))
        .await
        .expect("send place_order");

    // Wait for the PlaceResult ack so the limit rests before the feed moves, then drive the
    // channel: bar 1 (low 99) crosses the buy limit @ 100 under the same matching rule the R6
    // tape exercises, bar 2 extends the run.
    let mut placed = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), ws.next()).await {
            Ok(Some(Ok(message))) => {
                let Ok(text) = message.to_text() else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
                    continue;
                };
                if value.get("type").and_then(|t| t.as_str()) == Some("PlaceResult") {
                    placed = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(placed, "expected a PlaceResult ack for the live order");

    tx.send(bars[0].clone()).await.unwrap();
    tx.send(bars[1].clone()).await.unwrap();
    drop(tx);

    let mut saw_fill = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), ws.next()).await {
            Ok(Some(Ok(message))) => {
                let Ok(text) = message.to_text() else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<serde_json::Value>(text) else {
                    continue;
                };
                if value.get("type").and_then(|t| t.as_str()) == Some("Fill") {
                    saw_fill = true;
                    break;
                }
            }
            _ => break,
        }
    }

    // Release the daemon's graceful shutdown by closing our only WebSocket.
    drop(ws);

    let outcome = tokio::time::timeout(Duration::from_secs(15), runner)
        .await
        .expect("daemon did not exit after the live feed closed")
        .expect("daemon task panicked")
        .expect("daemon returned an error");

    println!("Live outcome: {outcome:?}");
    assert_eq!(outcome.bars_fed, 2, "both live bars should be fed");
    assert!(saw_fill, "expected a Fill broadcast for the crossed limit");
    assert!(outcome.fills >= 1, "the crossed limit should fill");
    assert_eq!(outcome.orders, 1, "exactly one order was placed");
}

/// The NSE trading-calendar gate the square-off pipeline now applies (plan 0005 D5
/// follow-on): 2026-01-15 (Thu) and 2026-01-16 (Fri) are trading days, 2026-01-17 (Sat) is
/// not — so closes are never submitted on weekends/holidays.
#[test]
fn square_off_gate_uses_nse_trading_calendar() {
    use chrono::NaiveDate;
    use tantra_runtime_backtest::NseTradingCalendar;

    let calendar = NseTradingCalendar::new();
    assert!(calendar.is_trading_day(NaiveDate::from_ymd_opt(2026, 1, 15).unwrap()));
    assert!(calendar.is_trading_day(NaiveDate::from_ymd_opt(2026, 1, 16).unwrap()));
    assert!(!calendar.is_trading_day(NaiveDate::from_ymd_opt(2026, 1, 17).unwrap()));
}

/// The compiled `paper-serve` binary answers `--help` with its usage and exits 0.
///
/// `CARGO_BIN_EXE_*` is only set by cargo when the binary is built in the same invocation;
/// targeted `--test` runs skip this, while a full `cargo test` (or `cargo test --bins`) run
/// exercises it.
#[test]
fn paper_serve_binary_exists_and_answers_help() {
    let Some(exe) = std::env::var("CARGO_BIN_EXE_paper_serve").ok() else {
        eprintln!("skipping: CARGO_BIN_EXE_paper_serve not set (binary not built in this run)");
        return;
    };
    let output = std::process::Command::new(exe)
        .arg("--help")
        .output()
        .expect("run paper-serve --help");
    assert!(output.status.success(), "--help should exit 0");
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.contains("--tape"),
        "usage should mention --tape: {text}"
    );
    assert!(
        text.contains("--mis-square-off"),
        "usage should mention --mis-square-off: {text}"
    );
}
