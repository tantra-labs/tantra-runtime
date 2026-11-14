//! Strategy-store adapter for the chat tool catalog's `strategy_manage` tool
//! (agent plan 0007 Track 5 Phase 13, Jesse-MCP strategy CRUD).

use async_trait::async_trait;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::strategy_store::{StrategySpec, StrategyStore};

/// Strategy-store adapter answering `strategy_manage` with an explicit gap
/// citation until the runtime wires a real strategy-manifest store (see
/// `docs/gaps/0007` item 18).
pub struct FeedStrategyStore;

impl FeedStrategyStore {
    /// Builds the strategy-store adapter — every call answers its explicit gap citation.
    pub fn new() -> Self {
        Self
    }
}

impl Default for FeedStrategyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StrategyStore for FeedStrategyStore {
    async fn save_strategy(&self, _spec: &StrategySpec) -> Result<StrategySpec, PortError> {
        // No strategy-manifest store is wired into the chat agent yet — see
        // tantra-runtime/docs/gaps/0007 item 18. An explicit error, never a
        // fabricated success.
        Err(PortError::unavailable(
            "strategy store not wired in the runtime; see tantra-runtime/docs/gaps/0007 item 18",
        ))
    }

    async fn list_strategies(&self) -> Result<Vec<StrategySpec>, PortError> {
        Err(PortError::unavailable(
            "strategy store not wired in the runtime; see tantra-runtime/docs/gaps/0007 item 18",
        ))
    }

    async fn get_strategy(&self, _strategy_id: &str) -> Result<StrategySpec, PortError> {
        Err(PortError::unavailable(
            "strategy store not wired in the runtime; see tantra-runtime/docs/gaps/0007 item 18",
        ))
    }

    async fn delete_strategy(&self, _strategy_id: &str) -> Result<bool, PortError> {
        Err(PortError::unavailable(
            "strategy store not wired in the runtime; see tantra-runtime/docs/gaps/0007 item 18",
        ))
    }
}
