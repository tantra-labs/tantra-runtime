//! Tests for `LibraryMcpClient` against a fake Python stdio MCP server.

use serde_json::json;
use std::path::Path;
use std::time::Duration;
use tantra_runtime_api_rest::library_client::{
    LibraryClientConfig, LibraryError, LibraryMcpClient,
};

fn cfg(mode: &str, extra: &[String]) -> LibraryClientConfig {
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/fake_library_mcp.py"
    );
    let mut args = vec![script.to_string(), mode.to_string()];
    args.extend(extra.iter().cloned());
    LibraryClientConfig {
        command: "python3".into(),
        args,
        request_timeout: Duration::from_secs(5),
        restart_backoff_initial: Duration::from_millis(20),
        restart_backoff_max: Duration::from_millis(100),
        ..LibraryClientConfig::default()
    }
}

#[tokio::test]
async fn happy_path_round_trips_each_tool() {
    let c = LibraryMcpClient::spawn(cfg("echo", &[]));
    for name in [
        "tantra_library_search",
        "tantra_library_describe",
        "tantra_indicator_compute",
        "tantra_signal_study",
    ] {
        let v = c.call_tool(name, json!({"a": 1})).await.expect("ok");
        assert_eq!(v["tool"], name);
        assert_eq!(v["arguments"]["a"], 1);
    }
    c.ping().await.expect("ping");
    assert!(c.is_healthy());
}

#[tokio::test]
async fn times_out_when_server_never_answers() {
    let mut config = cfg("sleep", &[]);
    config.request_timeout = Duration::from_millis(150);
    let c = LibraryMcpClient::spawn(config);
    let r = c.call_tool("tantra_signal_study", json!({})).await;
    assert!(matches!(r, Err(LibraryError::Timeout)), "{r:?}");
}

#[tokio::test]
async fn tool_error_is_mapped() {
    let c = LibraryMcpClient::spawn(cfg("tool_error", &[]));
    let r = c.call_tool("tantra_library_describe", json!({})).await;
    match r {
        Err(LibraryError::Tool { code, message }) => {
            assert_eq!(code, "invalid_param");
            assert_eq!(message, "x");
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn crash_then_restart_succeeds() {
    let dir = tempfile::tempdir().expect("tmp");
    let marker = dir.path().join("crashed").to_string_lossy().to_string();
    let c = LibraryMcpClient::spawn(cfg("crash", &[marker]));
    let r = c.call_tool("tantra_library_search", json!({})).await;
    assert!(matches!(r, Err(LibraryError::Unavailable(_))), "{r:?}");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match c.call_tool("tantra_library_search", json!({"q": 1})).await {
            Ok(v) => {
                assert_eq!(v["arguments"]["q"], 1);
                break;
            }
            Err(LibraryError::Unavailable(_)) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("unexpected {e:?}"),
        }
    }
}

#[tokio::test]
async fn busy_when_queue_full() {
    let mut config = cfg("sleep", &[]);
    config.queue_capacity = 1;
    config.request_timeout = Duration::from_secs(5);
    let c = LibraryMcpClient::spawn(config);
    let c1 = c.clone();
    let c2 = c.clone();
    let h1 = tokio::spawn(async move { c1.call_tool("tantra_signal_study", json!({})).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let h2 = tokio::spawn(async move { c2.call_tool("tantra_signal_study", json!({})).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = c.call_tool("tantra_signal_study", json!({})).await;
    assert!(matches!(r, Err(LibraryError::Busy)), "{r:?}");
    h1.abort();
    h2.abort();
}

#[tokio::test]
async fn oversized_line_is_payload_too_large() {
    let mut config = cfg("huge", &[]);
    config.max_line_bytes = 1024;
    let c = LibraryMcpClient::spawn(config);
    let r = c.call_tool("tantra_library_search", json!({})).await;
    assert!(matches!(r, Err(LibraryError::PayloadTooLarge)), "{r:?}");
}

#[test]
fn from_env_sets_lakehouse_root() {
    let c = LibraryClientConfig::from_env(Some(Path::new("/tmp/lake")));
    assert!(
        c.env
            .iter()
            .any(|(k, v)| k == "TANTRA_LIBRARY_LAKEHOUSE_ROOT" && v == "/tmp/lake")
    );
    let none = LibraryClientConfig::from_env(None);
    assert!(
        !none
            .env
            .iter()
            .any(|(k, _)| k == "TANTRA_LIBRARY_LAKEHOUSE_ROOT")
    );
}

#[tokio::test]
async fn init_timeout_restarts_child() {
    let mut config = cfg("no_init", &[]);
    config.request_timeout = Duration::from_millis(200);
    let c = LibraryMcpClient::spawn(config);
    // Call after init is underway so the init deadline expires before the call's own timeout.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let r = c.call_tool("tantra_library_search", json!({})).await;
    assert!(matches!(r, Err(LibraryError::Unavailable(_))), "{r:?}");
    assert!(!c.is_healthy());
}

#[tokio::test]
async fn blocked_stdin_write_does_not_wedge() {
    let mut config = cfg("stop_reading", &[]);
    config.request_timeout = Duration::from_millis(500);
    let c = LibraryMcpClient::spawn(config);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let big = "x".repeat(4 * 1024 * 1024);
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        c.call_tool("tantra_library_search", json!({"big": big})),
    )
    .await
    .expect("must not wedge");
    assert!(
        matches!(r, Err(LibraryError::Unavailable(_) | LibraryError::Timeout)),
        "{r:?}"
    );
}

#[tokio::test]
async fn failed_ping_clears_health() {
    let mut config = cfg("sleep_ping", &[]);
    config.request_timeout = Duration::from_millis(200);
    let c = LibraryMcpClient::spawn(config);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(c.is_healthy());
    assert!(c.ping().await.is_err());
    assert!(!c.is_healthy());
}
