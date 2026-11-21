//! In-process strategy-loop port: unbound behaviour and agent -> runtime DTO compatibility.

use std::sync::{Arc, OnceLock};

use tantra_agent_core::strategy_loop::{
    StrategyHedgeRequest, StrategyLoopBounds, StrategyLoopEvalRequest, StrategyLoopPort,
    StrategyLoopPortErrorKind, StrategyLoopSweepRequest,
};
use tantra_runtime_api::strategy_loop::{
    ExpiryHedgeRequest, StrategyLoopRequest, StrategySweepRequest,
};
use tantra_runtime_api_rest::RuntimeStrategyLoopPort;

fn eval() -> StrategyLoopEvalRequest {
    serde_json::from_value(serde_json::json!({
        "strategy_id": "orb15", "instrument_id": "NSE:NIFTY", "asset_class": "index",
        "start_date": "2024-01-01", "end_date": "2024-02-01"
    }))
    .expect("agent eval request")
}

fn roundtrip<A: serde::Serialize, R: serde::de::DeserializeOwned>(a: &A) -> R {
    serde_json::from_value(serde_json::to_value(a).expect("ser"))
        .expect("runtime DTO accepts agent JSON")
}

#[tokio::test]
async fn unbound_port_is_unavailable() {
    let port = RuntimeStrategyLoopPort::new(Arc::new(OnceLock::new()));
    let err = port.evaluate(eval()).await.expect_err("unbound");
    assert_eq!(err.kind, StrategyLoopPortErrorKind::Unavailable);
}

#[test]
fn eval_converts_to_runtime_dto() {
    let _: StrategyLoopRequest = roundtrip(&eval());
}

#[test]
fn sweep_converts_to_runtime_dto() {
    let req = StrategyLoopSweepRequest {
        base: eval(),
        bounds: StrategyLoopBounds::default(),
    };
    let _: StrategySweepRequest = roundtrip(&req);
}

#[test]
fn hedge_converts_to_runtime_dto() {
    let req: StrategyHedgeRequest = serde_json::from_value(serde_json::json!({
        "instrument_id": "NSE:NIFTY", "asset_class": "index"
    }))
    .expect("agent hedge request");
    let _: ExpiryHedgeRequest = roundtrip(&req);
}
