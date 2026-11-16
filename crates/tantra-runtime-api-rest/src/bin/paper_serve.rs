//! `paper serve` daemon binary: replays a bar tape into the paper exchange and serves the
//! REST + `/ws/live` surface, exiting cleanly when the tape ends.
//!
//! Arguments are parsed manually (no `clap` in the workspace):
//! `paper-serve --tape <file> [--addr <ip:port>] [--cash <decimal>] [--real-time]
//! [--mis-square-off <HH:MM>]`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use rust_decimal::Decimal;
use tantra_runtime_api_rest::paper_serve::{PaperServeConfig, run_tape_serve};

fn usage() -> String {
    [
        "usage: paper-serve --tape <file> [options]",
        "options:",
        "  --tape <file>            tape JSON to replay (required)",
        "  --addr <ip:port>         bind address (default 127.0.0.1:8001)",
        "  --cash <decimal>         paper account starting cash (default 1000000)",
        "  --real-time              pace replay to the tape's wall-clock deltas",
        "  --mis-square-off <HH:MM> IST time to square off open positions",
        "  --help                   print this usage and exit",
    ]
    .join("\n")
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("{}", usage());
        return ExitCode::SUCCESS;
    }

    let mut tape: Option<PathBuf> = None;
    let mut addr: Option<SocketAddr> = None;
    let mut cash: Option<Decimal> = None;
    let mut real_time = false;
    let mut mis_square_off: Option<String> = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--tape" => {
                let Some(v) = args.get(i + 1) else {
                    return arg_error("--tape needs a value");
                };
                tape = Some(PathBuf::from(v));
                i += 1;
            }
            "--addr" => {
                let Some(raw) = args.get(i + 1) else {
                    return arg_error("--addr needs a value");
                };
                match raw.parse::<SocketAddr>() {
                    Ok(a) => addr = Some(a),
                    Err(e) => {
                        eprintln!("{}", usage());
                        eprintln!("bad --addr {raw:?}: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--cash" => {
                let Some(raw) = args.get(i + 1) else {
                    return arg_error("--cash needs a value");
                };
                match raw.parse::<Decimal>() {
                    Ok(c) => cash = Some(c),
                    Err(e) => {
                        eprintln!("{}", usage());
                        eprintln!("bad --cash {raw:?}: {e}");
                        return ExitCode::from(2);
                    }
                }
                i += 1;
            }
            "--real-time" => real_time = true,
            "--mis-square-off" => {
                let Some(v) = args.get(i + 1) else {
                    return arg_error("--mis-square-off needs a value");
                };
                mis_square_off = Some(v.clone());
                i += 1;
            }
            other => {
                eprintln!("{}", usage());
                eprintln!("unknown argument: {other}");
                return ExitCode::from(2);
            }
        }
        i += 1;
    }

    let Some(tape) = tape else {
        eprintln!("{}", usage());
        eprintln!("missing required --tape <file>");
        return ExitCode::from(2);
    };

    let config = PaperServeConfig {
        tape,
        addr: addr.unwrap_or_else(|| "127.0.0.1:8001".parse().expect("valid default addr")),
        initial_cash: cash.unwrap_or_else(|| Decimal::new(1_000_000, 0)),
        real_time,
        mis_square_off,
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("failed to start tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run_tape_serve(config, |addr| {
        println!("paper exchange listening on http://{addr}");
    })) {
        Ok(outcome) => {
            println!(
                "paper serve finished: bars_fed={} fills={} orders={}",
                outcome.bars_fed, outcome.fills, outcome.orders
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("paper serve failed: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Prints the usage plus a message and returns exit code 2.
fn arg_error(msg: &str) -> ExitCode {
    eprintln!("{}", usage());
    eprintln!("{msg}");
    ExitCode::from(2)
}
