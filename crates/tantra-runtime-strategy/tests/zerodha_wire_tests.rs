//! Wire-transport tests for `ZerodhaKiteAdapter` (GAP-0003).
//!
//! Verifies that when Kite Connect credentials are configured the adapter
//! delegates `submit_order` / `cancel_order` to the real `tantra-exec-zerodha`
//! wire transport, and that broker errors normalize into `DispatchError`.
//!
//! The mock Kite Connect server is a blocking std HTTP/1.1 server. The
//! executor is injected as `Arc<dyn BrokerExecutor>` together with a handle to
//! a test-owned multi-thread runtime (gap 0028); the adapter bridges async to
//! sync via `Handle::block_on` / `block_in_place`.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tantra_core::{Exchange, InstrumentId, Quantity};
use tantra_risk::RiskEngine;
use tantra_runtime_strategy::{
    AngelOneAdapter, BrokerAdapter, DispatchError, Dispatcher, OrderIntent, ZerodhaAdapter,
};

fn fixed_now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-01-15T09:15:00Z")
        .expect("valid fixed timestamp")
        .with_timezone(&Utc)
}

#[derive(Clone)]
struct MockResponse {
    status: u16,
    body: String,
}

struct MockState {
    routes: HashMap<String, MockResponse>,
    default: MockResponse,
}

/// Spawn a blocking mock Kite Connect HTTP/1.1 server on a random port and
/// return that port. Each connection is read once, routed by request path,
/// answered, and closed.
fn spawn_mock(state: Arc<Mutex<MockState>>) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("mock server binds");
    let port = listener.local_addr().expect("mock local addr").port();
    let state = state.clone();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.expect("mock accepts connection");
            let state = state.clone();
            thread::spawn(move || {
                let mut buf = [0u8; 8192];
                let _ = stream.read(&mut buf);
                let req = String::from_utf8_lossy(&buf[..]);
                let MockResponse { status, body } = {
                    let st = state.lock().expect("mock state not poisoned");
                    let first_line = req.lines().next().unwrap_or("");
                    let mut parts = first_line.split_whitespace();
                    let _method = parts.next();
                    let path = parts.next().unwrap_or("/");
                    st.routes
                        .get(path)
                        .cloned()
                        .unwrap_or_else(|| st.default.clone())
                };
                let resp = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
            });
        }
    });
    port
}

fn success_state() -> Arc<Mutex<MockState>> {
    Arc::new(Mutex::new(MockState {
        routes: HashMap::from([(
            "/orders/regular".to_string(),
            MockResponse {
                status: 200,
                body: r#"{"status":"success","data":{"order_id":"ZERODHA_ORD_1001"}}"#.to_string(),
            },
        )]),
        default: MockResponse {
            status: 200,
            body: r#"{"status":"success","data":{"order_id":"ZERODHA_ORD_DEFAULT"}}"#.to_string(),
        },
    }))
}

fn live_adapter_at(base_url: &str) -> (tokio::runtime::Runtime, ZerodhaAdapter) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("test runtime");
    let exec: Arc<dyn tantra_exec_core::BrokerExecutor> =
        Arc::new(tantra_exec_zerodha::ZerodhaExecutor::with_base_url(
            "test_api_key",
            "test_access_token",
            base_url,
        ));
    let adapter =
        ZerodhaAdapter::with_executor(exec, rt.handle().clone(), Decimal::from(1_000_000));
    (rt, adapter)
}

fn live_adapter(port: u16) -> (tokio::runtime::Runtime, ZerodhaAdapter) {
    live_adapter_at(&format!("http://127.0.0.1:{port}"))
}

fn market_buy_intent() -> OrderIntent {
    let infy = InstrumentId::try_new(Exchange::Nse, "INFY").unwrap();
    OrderIntent::market_buy(infy, Quantity::from_u64(10))
}

#[test]
fn debug_output_redacts_credentials() {
    let a =
        AngelOneAdapter::with_credentials("SECRET_KEY", "CLIENT1", "SECRET_JWT", Decimal::from(1));
    let d = format!("{a:?}");
    assert!(d.contains("[REDACTED]"));
    assert!(!d.contains("SECRET_KEY"));
    assert!(!d.contains("SECRET_JWT"));
    assert!(!d.contains("CLIENT1"));
    let port = spawn_mock(success_state());
    let (_rt, z) = live_adapter(port);
    assert!(!format!("{z:?}").contains("test_access_token"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submit_from_async_multi_thread_context_does_not_panic() {
    let port = spawn_mock(success_state());
    let exec: Arc<dyn tantra_exec_core::BrokerExecutor> =
        Arc::new(tantra_exec_zerodha::ZerodhaExecutor::with_base_url(
            "test_api_key",
            "test_access_token",
            format!("http://127.0.0.1:{port}"),
        ));
    let adapter = ZerodhaAdapter::with_executor(
        exec,
        tokio::runtime::Handle::current(),
        Decimal::from(1_000_000),
    );
    let mut dispatcher = Dispatcher::new(RiskEngine::default(), Box::new(adapter));
    let id = dispatcher
        .dispatch_intent_with_id(
            &market_buy_intent(),
            tantra_core::uuid::Uuid::from_u128(0x000C_10EF_0002),
            fixed_now(),
        )
        .expect("submit from multi-thread context");
    assert_eq!(id, "ZERODHA_ORD_1001");
}

#[tokio::test(flavor = "current_thread")]
async fn submit_from_current_thread_runtime_errors_instead_of_panicking() {
    let port = spawn_mock(success_state());
    let (rt, adapter) = live_adapter(port);
    let mut dispatcher = Dispatcher::new(RiskEngine::default(), Box::new(adapter));
    let res = dispatcher.dispatch_intent_with_id(
        &market_buy_intent(),
        tantra_core::uuid::Uuid::from_u128(0x000C_10EF_0003),
        fixed_now(),
    );
    // The guard's own error: the executor was never driven, so no request left.
    let err = res.expect_err("current-thread runtime must be refused");
    assert!(
        err.to_string().contains("current-thread"),
        "unexpected error: {err}"
    );
    thread::spawn(move || drop(rt))
        .join()
        .expect("drop runtime");
}

#[test]
fn test_zerodha_wire_submit_order_delegates_to_kite_transport() {
    let state = success_state();
    let port = spawn_mock(state.clone());

    let (_rt, adapter) = live_adapter(port);
    assert_eq!(adapter.broker_name(), "zerodha");

    let nse = Exchange::Nse;
    let infy = InstrumentId::try_new(nse, "INFY").unwrap();
    let intent = OrderIntent::market_buy(infy, Quantity::from_u64(10));

    let mut dispatcher = Dispatcher::new(RiskEngine::default(), Box::new(adapter));
    let order_id = dispatcher
        .dispatch_intent_with_id(
            &intent,
            tantra_core::uuid::Uuid::from_u128(0x000C_10EF_0001),
            fixed_now(),
        )
        .expect("wire submit_order should succeed");

    assert_eq!(order_id, "ZERODHA_ORD_1001");
}

#[test]
fn test_zerodha_wire_auth_error_maps_to_session_expired() {
    let state = Arc::new(Mutex::new(MockState {
        routes: HashMap::from([(
            "/orders/regular".to_string(),
            MockResponse {
                status: 401,
                body: r#"{"status":"error","error_type":"TokenException","message":"invalid api key"}"#
                    .to_string(),
            },
        )]),
        default: MockResponse {
            status: 500,
            body: r#"{"status":"error","message":"internal"}"#.to_string(),
        },
    }));
    let port = spawn_mock(state.clone());

    let (_rt, adapter) = live_adapter(port);
    let nse = Exchange::Nse;
    let infy = InstrumentId::try_new(nse, "INFY").unwrap();
    let intent = OrderIntent::market_buy(infy, Quantity::from_u64(10));

    let mut dispatcher = Dispatcher::new(RiskEngine::default(), Box::new(adapter));
    let err = dispatcher
        .dispatch_intent_with_id(
            &intent,
            tantra_core::uuid::Uuid::from_u128(0x000C_10EF_0002),
            fixed_now(),
        )
        .expect_err("wire auth failure must be rejected");

    match err {
        DispatchError::BrokerRejection(msg) => {
            assert!(msg.contains("session expired"), "unexpected message: {msg}");
        }
        other => panic!("expected BrokerRejection(session expired), got {other:?}"),
    }
}

#[test]
fn test_zerodha_wire_rate_limit_error_maps_to_rate_limited() {
    let state = Arc::new(Mutex::new(MockState {
        routes: HashMap::from([(
            "/orders/regular".to_string(),
            MockResponse {
                status: 429,
                body: r#"{"status":"error","error_type":"TooManyRequests","message":"rate limit exceeded"}"#
                    .to_string(),
            },
        )]),
        default: MockResponse {
            status: 500,
            body: r#"{"status":"error","message":"internal"}"#.to_string(),
        },
    }));
    let port = spawn_mock(state.clone());

    let (_rt, adapter) = live_adapter(port);
    let nse = Exchange::Nse;
    let infy = InstrumentId::try_new(nse, "INFY").unwrap();
    let intent = OrderIntent::market_buy(infy, Quantity::from_u64(10));

    let mut dispatcher = Dispatcher::new(RiskEngine::default(), Box::new(adapter));
    let err = dispatcher
        .dispatch_intent_with_id(
            &intent,
            tantra_core::uuid::Uuid::from_u128(0x000C_10EF_0003),
            fixed_now(),
        )
        .expect_err("wire rate limit must be rejected");

    match err {
        DispatchError::BrokerRejection(msg) => {
            assert!(msg.contains("rate limited"), "unexpected message: {msg}");
        }
        other => panic!("expected BrokerRejection(rate limited), got {other:?}"),
    }
}

#[test]
fn test_zerodha_wire_network_error_maps_to_network() {
    // Point at a port with no listener: connection refused -> transport error.
    let (_rt, adapter) = live_adapter_at("http://127.0.0.1:19999");
    let nse = Exchange::Nse;
    let infy = InstrumentId::try_new(nse, "INFY").unwrap();
    let intent = OrderIntent::market_buy(infy, Quantity::from_u64(10));

    let mut dispatcher = Dispatcher::new(RiskEngine::default(), Box::new(adapter));
    let err = dispatcher
        .dispatch_intent_with_id(
            &intent,
            tantra_core::uuid::Uuid::from_u128(0x000C_10EF_0004),
            fixed_now(),
        )
        .expect_err("wire network failure must be rejected");

    match err {
        DispatchError::BrokerRejection(msg) => {
            assert!(msg.contains("network error"), "unexpected message: {msg}");
        }
        other => panic!("expected BrokerRejection(network), got {other:?}"),
    }
}

#[test]
fn test_zerodha_wire_cancel_order_delegates_to_kite_transport() {
    let state = Arc::new(Mutex::new(MockState {
        routes: HashMap::from([
            (
                "/orders/regular".to_string(),
                MockResponse {
                    status: 200,
                    body: r#"{"status":"success","data":{"order_id":"ZERODHA_ORD_2001"}}"#
                        .to_string(),
                },
            ),
            (
                "/orders/regular/ZERODHA_ORD_2001".to_string(),
                MockResponse {
                    status: 200,
                    body: "{}".to_string(),
                },
            ),
        ]),
        default: MockResponse {
            status: 500,
            body: r#"{"status":"error","message":"internal"}"#.to_string(),
        },
    }));
    let port = spawn_mock(state.clone());

    let (_rt, adapter) = live_adapter(port);
    let nse = Exchange::Nse;
    let infy = InstrumentId::try_new(nse, "INFY").unwrap();
    let intent = OrderIntent::market_buy(infy, Quantity::from_u64(10));

    let mut dispatcher = Dispatcher::new(RiskEngine::default(), Box::new(adapter));
    let order_id = dispatcher
        .dispatch_intent_with_id(
            &intent,
            tantra_core::uuid::Uuid::from_u128(0x000C_10EF_0005),
            fixed_now(),
        )
        .expect("place order");

    dispatcher
        .cancel_order(&order_id, fixed_now())
        .expect("wire cancel_order should succeed");
    assert_eq!(order_id, "ZERODHA_ORD_2001");
}
