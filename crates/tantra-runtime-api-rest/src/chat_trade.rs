//! Trade-intent sink adapter for the chat tool catalog's `propose_trade` tool
//! (agent plan 0007 Track 5 Phase 12, NoFX approval-queue pattern).

use async_trait::async_trait;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::trade_intent::{
    TradeIntent, TradeIntentSink, TradeIntentStatus,
};

/// Trade-intent adapter answering `propose_trade` with an explicit gap citation
/// until the runtime wires a `FileJobRegistry`-backed approval queue or the
/// paper-trading sandbox (see `docs/gaps/0007` item 17).
pub struct FeedTradeIntentSink;

impl FeedTradeIntentSink {
    /// Builds the trade-intent adapter — every call answers its explicit gap citation.
    pub fn new() -> Self {
        Self
    }
}

impl Default for FeedTradeIntentSink {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TradeIntentSink for FeedTradeIntentSink {
    async fn propose_trade(&self, _intent: &TradeIntent) -> Result<TradeIntentStatus, PortError> {
        // No approval-queue sink or paper sandbox is wired into the chat agent yet —
        // see tantra-runtime/docs/gaps/0007 item 17. An explicit error, never an
        // autonomous live dispatch (untrusted-AI principle).
        Err(PortError::unavailable(
            "trade-intent sink not wired in the runtime; see tantra-runtime/docs/gaps/0007 item 17",
        ))
    }
}
