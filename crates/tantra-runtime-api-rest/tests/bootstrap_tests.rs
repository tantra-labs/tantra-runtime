use std::path::{Path, PathBuf};

use rust_decimal::Decimal;

use tantra_core::TradingState;
use tantra_risk::{OrderRateLimit, RiskLimits};
use tantra_runtime_api_rest::bootstrap::{
    ServeConfig, StartupError, load_risk_limits, parse_allow_insecure_bind_flag,
    parse_trading_state, parse_trading_state_command, validate_bind_security,
};
use tantra_runtime_api_rest::{ApiConfig, AppState, api_router_with_config};

#[test]
fn loopback_addresses_are_permitted_without_auth() {
    assert_eq!(
        validate_bind_security("127.0.0.1:8080".parse().unwrap(), false, false),
        Ok(())
    );
    assert_eq!(
        validate_bind_security("[::1]:8080".parse().unwrap(), false, false),
        Ok(())
    );
}

#[test]
fn unauthenticated_non_loopback_addresses_fail_closed_unless_overridden() {
    for raw in ["0.0.0.0:8080", "192.168.1.5:80", "[::]:80"] {
        let addr = raw.parse().unwrap();
        let err = validate_bind_security(addr, false, false).unwrap_err();
        assert_eq!(err, StartupError::UnauthenticatedNonLoopback { addr });
        let msg = err.to_string();
        assert!(msg.contains(raw), "{msg}");
        assert!(msg.contains("TANTRA_ALLOW_INSECURE_BIND"), "{msg}");

        assert_eq!(validate_bind_security(addr, true, false), Ok(()));
        assert_eq!(validate_bind_security(addr, false, true), Ok(()));
    }
}

#[test]
fn allow_insecure_bind_env_flag_parses_strict_booleans() {
    for truthy in ["1", "true", "TRUE", "yes", "on"] {
        assert_eq!(parse_allow_insecure_bind_flag(truthy), Ok(true), "{truthy}");
    }
    for falsy in ["", "0", "false", "FALSE", "no", "off"] {
        assert_eq!(parse_allow_insecure_bind_flag(falsy), Ok(false), "{falsy}");
    }
    let err = parse_allow_insecure_bind_flag("maybe").unwrap_err();
    assert_eq!(
        err,
        StartupError::InvalidInsecureBindEnv {
            value: "maybe".to_string()
        }
    );
}

#[test]
fn api_router_with_config_returns_err_on_missing_or_malformed_token_file() {
    let dir = tempfile::tempdir().unwrap();

    let missing = dir.path().join("missing-tokens.toml");
    let mut cfg_missing = ApiConfig::default();
    cfg_missing.auth.tokens_file = Some(missing.clone());
    let err = api_router_with_config(AppState::default(), &cfg_missing).unwrap_err();
    assert!(matches!(
        err,
        StartupError::TokenFileLoad { ref path, .. } if path == &missing
    ));

    let bad = dir.path().join("malformed-tokens.toml");
    std::fs::write(&bad, "[[tokens]\nnot valid toml =").unwrap();
    let mut cfg_bad = ApiConfig::default();
    cfg_bad.auth.tokens_file = Some(bad.clone());
    let err = api_router_with_config(AppState::default(), &cfg_bad).unwrap_err();
    assert!(matches!(
        err,
        StartupError::TokenFileLoad { ref path, .. } if path == &bad
    ));
}

#[test]
fn trading_state_parses_the_wire_spellings_and_refuses_others() {
    assert_eq!(parse_trading_state("active").unwrap(), TradingState::Active);
    assert_eq!(
        parse_trading_state("reducing").unwrap(),
        TradingState::Paused { resume_at: None }
    );
    assert_eq!(
        parse_trading_state("halted").unwrap(),
        TradingState::Halted {
            reason: "operator halt via `--trading-state halted`".to_string(),
        }
    );
    let err = parse_trading_state("paused").unwrap_err();
    assert!(err.contains("paused"), "{err}");
}

#[test]
fn set_spellings_map_to_trading_state_and_refuse_unknown() {
    for (raw, expected) in [
        ("active", TradingState::Active),
        ("reducing", TradingState::Paused { resume_at: None }),
        (
            "halted",
            TradingState::Halted {
                reason: "operator halt via `tantra trading-state set`".to_string(),
            },
        ),
        ("REDUCING", TradingState::Paused { resume_at: None }),
    ] {
        assert_eq!(parse_trading_state_command(raw).unwrap(), expected, "{raw}");
    }
    assert!(parse_trading_state_command("paused").is_err());
    assert!(parse_trading_state_command("").is_err());
}

#[test]
fn serve_config_defaults_and_bounds_match_adr_values() {
    let cfg = ServeConfig::new("bars");
    assert_eq!(cfg.addr, "127.0.0.1:8000".parse().unwrap());
    assert!(!cfg.allow_insecure_bind);
    assert!(cfg.cors_origins.is_empty());
    assert_eq!(cfg.trading_state, TradingState::Active);
    assert_eq!(cfg.runs.journals_dir, PathBuf::from("data/journals"));
    assert_eq!(cfg.runs.profiles_dir, PathBuf::from("data/profiles"));
    assert_eq!(cfg.runs.portfolios_dir, PathBuf::from("data/portfolios"));
    assert_eq!(cfg.runs.max_concurrent_runs, None);
    assert_eq!(cfg.runs.max_queued_runs, 64);
    assert_eq!(cfg.runs.shutdown_grace_secs, 10);
    assert_eq!(cfg.runs.keep_runs, 1_000);
    assert_eq!(cfg.runs.keep_days, 30);
    assert!(!cfg.ingest.bar_ingest);
    assert_eq!(cfg.ingest.bar_ingest_capacity, 1_024);

    let mut zero_pool = cfg.clone();
    zero_pool.runs.max_concurrent_runs = Some(0);
    assert!(zero_pool.runs.validate().is_err());

    let mut zero_queue = cfg.clone();
    zero_queue.runs.max_queued_runs = 0;
    assert!(zero_queue.runs.validate().is_err());

    let mut zero_ingest = cfg;
    zero_ingest.ingest.bar_ingest_capacity = 0;
    assert!(zero_ingest.ingest.validate().is_err());
}

fn risk_file(dir: &Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

#[test]
fn risk_config_parses_the_backtest_risk_shape() {
    let dir = tempfile::tempdir().unwrap();
    let path = risk_file(
        dir.path(),
        "full.toml",
        "[risk]\nmax_notional_per_order = \"500000.0\"\nprice_collar_bps = 200\n\n\
         [risk.order_rate_limit]\nmax_orders = 30\nwindow_ms = 1000\n",
    );
    assert_eq!(
        load_risk_limits(&path).unwrap(),
        RiskLimits {
            max_notional_per_order: Some(Decimal::from(500_000)),
            order_rate_limit: Some(OrderRateLimit {
                max_orders: 30,
                window_ms: 1000
            }),
            price_collar_bps: Some(200),
            ..RiskLimits::default()
        }
    );
}

#[test]
fn risk_config_without_a_risk_table_means_no_limits() {
    let dir = tempfile::tempdir().unwrap();
    let path = risk_file(dir.path(), "empty.toml", "");
    assert_eq!(load_risk_limits(&path).unwrap(), RiskLimits::default());
}

#[test]
fn risk_config_errors_name_the_field_and_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let cases = [
        (
            "zero-collar.toml",
            "[risk]\nprice_collar_bps = 0\n",
            "price_collar_bps",
        ),
        (
            "neg-notional.toml",
            "[risk]\nmax_notional_per_order = \"-1.0\"\n",
            "max_notional_per_order",
        ),
        ("syntax.toml", "[risk\n", "syntax.toml"),
    ];
    for (name, text, needle) in cases {
        let path = risk_file(dir.path(), name, text);
        let err = format!("{:#}", load_risk_limits(&path).unwrap_err());
        assert!(err.contains(needle), "{name}: {err}");
        assert!(err.contains(name), "{name}: {err}");
    }
    let err = format!(
        "{:#}",
        load_risk_limits(Path::new("/nonexistent/risk.toml")).unwrap_err()
    );
    assert!(err.contains("risk.toml"), "{err}");
}
