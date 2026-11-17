//! Standalone paper-exchange tape-serving daemon (`paper serve`).
//!
//! Plan 0005 D5 (T14.9): replays a recorded bar tape into the shared paper exchange and
//! serves the standard REST + `/ws/live` surface until the tape ends, then exits cleanly.
//!
//! # Tape format
//!
//! `--tape` names a JSON file:
//!
//! ```json
//! {
//!   "instruments": [
//!     {"symbol": "RELIANCE", "exchange": "NSE", "kind": "equity",
//!      "currency": "INR", "lot_size": 1, "tick_size": 0.05}
//!   ],
//!   "bars": [
//!     {"symbol": "RELIANCE", "exchange": "NSE", "open": "100",
//!      "high": "101", "low": "99", "close": "100.5", "volume": "1000",
//!      "timestamp": "2026-01-15T09:15:00Z"}
//!   ]
//! }
//! ```
//!
//! Prices, quantities and cash are fixed-precision decimal strings (zero-floats invariant;
//! the only `f64` fields are the wire instrument's lot/tick, the existing market-feed
//! pattern). `exchange` (`NSE`), `kind` (`equity`) and `currency` (`INR`) map onto the
//! market-feed wire enums; anything else is rejected. Bars replay in file order.
//!
//! # MIS square-off and calendar gate
//!
//! [`PaperServeConfig::mis_square_off`] (`"HH:MM"`, IST) submits an opposite-side market
//! close for every non-zero exchange position on the first bar whose UTC+05:30 clock reads
//! `HH:MM`. The closes are submitted directly to the exchange — not through the REST risk
//! gate — and then drained into the ledger and `/ws/live` like any other fill (a documented
//! deviation from the REST path). The NSE trading-calendar gate is now applied (plan 0005 D5
//! follow-on, closed): closes are only submitted when the bar's date is a trading day
//! (`tantra_runtime_backtest::NseTradingCalendar::is_trading_day`), so weekends and exchange
//! holidays never square off.
//!
//! # Live feed seam
//!
//! [`PaperServeConfig::feed`] selects the bar source. The default [`PaperFeed::Tape`] replays
//! [`PaperServeConfig::tape`] exactly as before. [`PaperFeed::Live`] consumes bars from an
//! injected `tokio` channel until the sender is dropped, running the same advance /
//! square-off / pacing pipeline per bar. In `Live` mode the tape file is optional and is used
//! only to pre-register instruments (a real broker feed supplies its own). This seam is where
//! a real `tantra-market` broker feed (`tantra-market-dhan` / `tantra-market-zerodha`) plugs
//! in — see `tantra-market` `docs/gaps/0003-*.md` (real broker WebSocket credentials).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use tantra_core::{Bar, Currency, Exchange, InstrumentId, InstrumentKind, Price, Quantity};
use tantra_exec_paper::PaperExchange;
use tantra_runtime_api::{ErrorCode, ErrorDetail};
use tokio::net::TcpListener;

use crate::market_feed::wire::Instrument;
use crate::market_feed::{Dataset, DatasetReader};
use crate::state::AppState;
use crate::{ApiConfig, serve_with_config};

/// Bar feed source for a `paper serve` run.
///
/// [`PaperFeed::Tape`] is the default: replay the recorded tape file exactly as before. The
/// `Vec<Bar>` payload is a pre-parsed override; an empty payload selects the tape file from
/// [`PaperServeConfig::tape`].
///
/// [`PaperFeed::Live`] consumes bars from an injected channel until the sender is dropped. It
/// is the seam where a real `tantra-market` broker feed plugs in (`tantra-market-dhan` /
/// `tantra-market-zerodha`, see `tantra-market` `docs/gaps/0003-*.md` — real broker WebSocket
/// credentials).
pub enum PaperFeed {
    /// Recorded tape replay (default; the tape file supplies the bars).
    Tape(Vec<Bar>),
    /// Live bar stream consumed from an injected channel.
    Live(tokio::sync::mpsc::Receiver<Bar>),
}

/// Configuration for a `paper serve` run.
pub struct PaperServeConfig {
    /// Path to the tape JSON file to replay (see the [module docs](crate::paper_serve)).
    pub tape: PathBuf,
    /// Bind address of the REST/WebSocket server (default `127.0.0.1:8001`).
    pub addr: SocketAddr,
    /// Starting cash (INR) of the paper account; seeds a fresh paper exchange.
    pub initial_cash: Decimal,
    /// When `true`, pace the replay to each bar's wall-clock delta (each sleep capped at
    /// 1 second so a `real_time` run stays resumable); when `false` (default), replay as fast
    /// as possible.
    pub real_time: bool,
    /// Optional IST square-off time (`"HH:MM"`, e.g. `Some("15:20")`).
    pub mis_square_off: Option<String>,
    /// Bar feed source: a recorded [`PaperFeed::Tape`] (default) or a [`PaperFeed::Live`]
    /// channel (real `tantra-market` broker feed seam, see `tantra-market` `docs/gaps/0003-*.md`).
    pub feed: PaperFeed,
}

impl Default for PaperServeConfig {
    fn default() -> Self {
        Self {
            tape: PathBuf::new(),
            addr: "127.0.0.1:8001".parse().expect("valid default addr"),
            initial_cash: Decimal::new(1_000_000, 0),
            real_time: false,
            mis_square_off: None,
            feed: PaperFeed::Tape(Vec::new()),
        }
    }
}

/// What a tape-serving run accomplished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperServeOutcome {
    /// Bars fed into the paper exchange (a bar that errors is still counted as fed).
    pub bars_fed: u64,
    /// Fills recorded by the exchange.
    pub fills: usize,
    /// Orders recorded on the ledger.
    pub orders: usize,
}

/// A parsed tape: the instruments to register plus the bars to replay.
pub struct Tape {
    /// Instruments declared by the tape.
    pub instruments: Vec<Instrument>,
    /// Bars to feed, in replay order.
    pub bars: Vec<Bar>,
}

/// Parses a tape JSON file (see the [module docs](crate::paper_serve) for the format) into a
/// [`Tape`].
///
/// # Errors
///
/// Returns an [`ErrorDetail`] (`ValidationInvalidRequest`) when the file cannot be read or is
/// not a valid tape: unknown exchange/kind/currency, a non-decimal price/quantity, or a bad
/// bar timestamp.
pub fn parse_tape(path: &Path) -> Result<Tape, ErrorDetail> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| err(format!("cannot read tape {}: {e}", path.display())))?;
    let raw: TapeFile = serde_json::from_str(&text)
        .map_err(|e| err(format!("invalid tape JSON {}: {e}", path.display())))?;

    let instruments = raw
        .instruments
        .into_iter()
        .map(|ti| {
            Ok(Instrument::new(
                InstrumentId::new(parse_exchange(&ti.exchange)?, ti.symbol),
                parse_kind(&ti.kind)?,
                parse_currency(&ti.currency)?,
                ti.lot_size,
                ti.tick_size,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let bars = raw
        .bars
        .into_iter()
        .map(|tb| {
            Ok(Bar {
                instrument: InstrumentId::new(parse_exchange(&tb.exchange)?, tb.symbol),
                open: Price::new(parse_decimal(&tb.open)?),
                high: Price::new(parse_decimal(&tb.high)?),
                low: Price::new(parse_decimal(&tb.low)?),
                close: Price::new(parse_decimal(&tb.close)?),
                volume: Quantity::new(parse_decimal(&tb.volume)?),
                timestamp: DateTime::parse_from_rfc3339(&tb.timestamp)
                    .map_err(|e| err(format!("bad bar timestamp {}: {e}", tb.timestamp)))?
                    .with_timezone(&Utc),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Tape { instruments, bars })
}

/// Runs the tape-serving daemon to completion.
///
/// Parses [`PaperServeConfig::tape`], builds an [`AppState`] over an empty dataset with the
/// tape's instruments (bars are fed live via [`AppState::advance_market`]), seeds a fresh
/// paper exchange with [`PaperServeConfig::initial_cash`], binds
/// [`PaperServeConfig::addr`], calls `on_bound` with the bound address, serves the REST +
/// `/ws/live` surface while replaying the feed, and returns a [`PaperServeOutcome`] once the
/// feed ends and the server has shut down.
///
/// The feed is selected by [`PaperServeConfig::feed`]: [`PaperFeed::Tape`] replays the tape
/// file exactly as before, while [`PaperFeed::Live`] consumes bars from the injected channel
/// until its sender is dropped (the real `tantra-market` broker-feed seam — see
/// `tantra-market` `docs/gaps/0003-*.md`). In `Live` mode the tape file is parsed when
/// present (to pre-register instruments) and ignored otherwise.
///
/// The server shuts down gracefully on tape end: it stops accepting and waits for in-flight
/// connections, so a connected WebSocket client keeps the daemon alive until it disconnects.
///
/// # Errors
///
/// Returns an [`ErrorDetail`] (`ValidationInvalidRequest`) when the tape cannot be parsed or
/// the listener cannot be bound, or when the server task itself fails.
pub async fn run_tape_serve(
    config: PaperServeConfig,
    on_bound: impl FnOnce(SocketAddr) + Send + 'static,
) -> Result<PaperServeOutcome, ErrorDetail> {
    // Instruments are always read from the tape file (a `Live` run may carry one to
    // pre-register instruments; an absent tape registers none).
    let tape = match &config.feed {
        PaperFeed::Tape(_) => parse_tape(&config.tape)?,
        PaperFeed::Live(_) if config.tape.as_os_str().is_empty() => Tape {
            instruments: Vec::new(),
            bars: Vec::new(),
        },
        PaperFeed::Live(_) => parse_tape(&config.tape)?,
    };
    let state = build_state(&tape, config.initial_cash);

    let listener = TcpListener::bind(config.addr)
        .await
        .map_err(|e| err(format!("cannot bind {}: {e}", config.addr)))?;
    let bound = listener
        .local_addr()
        .map_err(|e| err(format!("cannot read bound address: {e}")))?;
    on_bound(bound);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let state_serve = state.clone();
    // `serve_with_config` takes `&ApiConfig`, but a spawned task needs `'static`; the daemon
    // lives for the whole run, so leaking one default config is fine.
    let config_ref: &'static ApiConfig = Box::leak(Box::new(ApiConfig::default()));
    let server_task = tokio::spawn(serve_with_config(
        listener,
        state_serve,
        config_ref,
        async move {
            let _ = shutdown_rx.await;
        },
    ));

    // Warm-up window so early WebSocket clients can connect and place resting orders before
    // the tape starts replaying; keeps the R6 acceptance flow deterministic.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let calendar = tantra_runtime_backtest::NseTradingCalendar::new();
    let mut bars_fed: u64 = 0;
    let mut prev_ts: Option<DateTime<Utc>> = None;
    match config.feed {
        PaperFeed::Tape(bars) if !bars.is_empty() => {
            for bar in &bars {
                process_bar(
                    &state,
                    config.real_time,
                    &config.mis_square_off,
                    &calendar,
                    bar,
                    &mut bars_fed,
                    &mut prev_ts,
                )
                .await;
            }
        }
        PaperFeed::Tape(_) => {
            // Existing tape-replay semantics (unchanged).
            for bar in &tape.bars {
                process_bar(
                    &state,
                    config.real_time,
                    &config.mis_square_off,
                    &calendar,
                    bar,
                    &mut bars_fed,
                    &mut prev_ts,
                )
                .await;
            }
        }
        PaperFeed::Live(mut rx) => {
            // Live-feed seam (plan 0005 D5 follow-on, closed): consume bars until the sender
            // is dropped. A real `tantra-market` broker feed (`tantra-market-dhan` /
            // `tantra-market-zerodha`) plugs in here — stand-in for
            // tantra-market/docs/gaps/0003-*.md (real broker WebSocket credentials).
            while let Some(bar) = rx.recv().await {
                process_bar(
                    &state,
                    config.real_time,
                    &config.mis_square_off,
                    &calendar,
                    &bar,
                    &mut bars_fed,
                    &mut prev_ts,
                )
                .await;
            }
        }
    }

    drop(shutdown_tx);
    server_task
        .await
        .map_err(|e| err(format!("server task failed: {e}")))?
        .map_err(|e| err(format!("server error: {e}")))?;

    Ok(PaperServeOutcome {
        bars_fed,
        fills: state.exec.fill_count(),
        orders: state.orders.list().len(),
    })
}

/// Advance one bar through the shared `paper serve` pipeline (plan 0005 D5): feed the
/// exchange, count the bar, run the calendar-gated MIS square-off check, and apply real-time
/// pacing. Used by both the tape and the live feed loops.
async fn process_bar(
    state: &AppState,
    real_time: bool,
    mis_square_off: &Option<String>,
    calendar: &tantra_runtime_backtest::NseTradingCalendar,
    bar: &Bar,
    bars_fed: &mut u64,
    prev_ts: &mut Option<DateTime<Utc>>,
) {
    if let Err(e) = state.advance_market(std::slice::from_ref(bar)) {
        eprintln!(
            "paper-serve: skipping bar {} at {}: {e}",
            bar.instrument, bar.timestamp
        );
    }
    *bars_fed += 1;

    if let Some(hhmm) = mis_square_off {
        // NSE trading-calendar gate (plan 0005 D5 follow-on, closed): never submit MIS
        // square-off closes on non-trading days (weekends / exchange holidays).
        if calendar.is_trading_day(bar.timestamp.date_naive()) && ist_hhmm(bar.timestamp) == *hhmm {
            let closes =
                state
                    .exec
                    .mis_square_off(&state.orders, &state.shared_ledger, bar.timestamp);
            eprintln!("paper-serve: MIS square-off {hhmm} submitted {closes} close orders");
        }
    }

    if real_time {
        if let Some(prev) = *prev_ts {
            let delta = (bar.timestamp - prev).to_std().unwrap_or_default();
            tokio::time::sleep(delta.min(std::time::Duration::from_secs(1))).await;
        }
    }
    *prev_ts = Some(bar.timestamp);
}

/// Builds the daemon's [`AppState`]: an empty dataset (bars are fed live) carrying the tape's
/// instruments, with a fresh [`PaperExchange`] seeded at `initial_cash`.
fn build_state(tape: &Tape, initial_cash: Decimal) -> AppState {
    let reader = DatasetReader::new(Dataset::default(), tape.instruments.clone());
    AppState::from_reader(reader).with_execution_sim(PaperExchange::new(initial_cash))
}

/// The bar's IST (`UTC+05:30`) wall-clock as `"HH:MM"`.
fn ist_hhmm(ts: DateTime<Utc>) -> String {
    let shifted = (ts.timestamp() + 19_800).rem_euclid(86_400);
    format!("{:02}:{:02}", shifted / 3600, (shifted % 3600) / 60)
}

fn parse_exchange(s: &str) -> Result<Exchange, ErrorDetail> {
    match s.to_ascii_uppercase().as_str() {
        "NSE" => Ok(Exchange::Nse),
        other => Err(err(format!(
            "unsupported exchange {other:?} in tape (only NSE)"
        ))),
    }
}

fn parse_kind(s: &str) -> Result<InstrumentKind, ErrorDetail> {
    match s.to_ascii_lowercase().as_str() {
        "equity" => Ok(InstrumentKind::Equity),
        other => Err(err(format!(
            "unsupported instrument kind {other:?} in tape (only equity)"
        ))),
    }
}

fn parse_currency(s: &str) -> Result<Currency, ErrorDetail> {
    match s.to_ascii_uppercase().as_str() {
        "INR" => Ok(Currency::Inr),
        other => Err(err(format!(
            "unsupported currency {other:?} in tape (only INR)"
        ))),
    }
}

fn parse_decimal(s: &str) -> Result<Decimal, ErrorDetail> {
    s.parse::<Decimal>()
        .map_err(|e| err(format!("invalid decimal {s:?}: {e}")))
}

fn err(msg: impl Into<String>) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, msg.into())
}

#[derive(Deserialize)]
struct TapeFile {
    instruments: Vec<TapeInstrument>,
    bars: Vec<TapeBar>,
}

#[derive(Deserialize)]
struct TapeInstrument {
    symbol: String,
    exchange: String,
    kind: String,
    currency: String,
    lot_size: f64,
    tick_size: f64,
}

#[derive(Deserialize)]
struct TapeBar {
    symbol: String,
    exchange: String,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    timestamp: String,
}
