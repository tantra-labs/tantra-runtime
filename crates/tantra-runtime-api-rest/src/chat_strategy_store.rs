//! Strategy-store adapter for the chat tool catalog's `strategy_manage` tool
//! (agent plan 0007 Track 5 Phase 13, Jesse-MCP strategy CRUD; docs/gaps/0007 item 18).

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use chrono::Utc;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::strategy_store::{StrategySpec, StrategyStore};

/// Strategy-manifest store (docs/gaps/0007 item 18): an in-memory CRUD store over the chat
/// agent's `strategy_manage` tool, keyed by `strategy_id`. `save_strategy` stamps
/// `updated_at`; the first save also stamps `created_at`. A restart empties it — this is the
/// declarative, session-scoped v1 store (persistence is a follow-up, tracked the same gap).
#[derive(Default)]
pub struct FeedStrategyStore {
    inner: Mutex<HashMap<String, StrategySpec>>,
}

impl FeedStrategyStore {
    /// Builds an empty in-memory strategy-manifest store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StrategyStore for FeedStrategyStore {
    async fn save_strategy(&self, spec: &StrategySpec) -> Result<StrategySpec, PortError> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Utc::now();
        let mut stored = spec.clone();
        stored.updated_at = now;
        // Preserve the original created timestamp on update.
        if let Some(existing) = inner.get(&spec.strategy_id) {
            stored.created_at = existing.created_at;
        } else {
            stored.created_at = now;
        }
        inner.insert(stored.strategy_id.clone(), stored.clone());
        Ok(stored)
    }

    async fn list_strategies(&self) -> Result<Vec<StrategySpec>, PortError> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let mut all: Vec<StrategySpec> = inner.values().cloned().collect();
        all.sort_by(|a, b| a.strategy_id.cmp(&b.strategy_id));
        Ok(all)
    }

    async fn get_strategy(&self, strategy_id: &str) -> Result<StrategySpec, PortError> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner
            .get(strategy_id)
            .cloned()
            .ok_or_else(|| PortError::not_found(format!("strategy '{strategy_id}' not found")))
    }

    async fn delete_strategy(&self, strategy_id: &str) -> Result<bool, PortError> {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(inner.remove(strategy_id).is_some())
    }
}
