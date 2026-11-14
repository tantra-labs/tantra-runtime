//! Trade-intent sink adapter for the chat tool catalog's `propose_trade` tool
//! (agent plan 0007 Track 5 Phase 12, NoFX approval-queue pattern; docs/gaps/0007 item 17).

use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::trade_intent::{
    TradeIntent, TradeIntentSink, TradeIntentStatus,
};
use tantra_runtime_jobs::{ApprovalPolicy, CronSchedule, FileJobRegistry, JobSpec, JobStatus};

/// Cron that never fires (31 Feb) — intent jobs are not scheduled; an operator executes them.
const NEVER_CRON: &str = "0 0 31 2 *";

/// Trade-intent adapter (docs/gaps/0007 item 17): a validated intent enqueues into the
/// `FileJobRegistry`-backed approval queue as a `PendingApproval` job — the NoFX human gate,
/// visible to operators through the jobs registry/CLI. `paper_trading` intents additionally
/// carry the `x-papertrading` metadata flag that marks them for the paper sandbox when an
/// operator executes the job. Never an autonomous live dispatch (untrusted-AI principle); a
/// missing registry answers the explicit gap citation rather than a fabricated confirmation.
pub struct FeedTradeIntentSink {
    registry: Option<Arc<Mutex<FileJobRegistry>>>,
}

impl FeedTradeIntentSink {
    /// Builds the trade-intent adapter over the runtime's jobs registry.
    pub fn new(registry: Option<Arc<Mutex<FileJobRegistry>>>) -> Self {
        Self { registry }
    }

    /// Builds a trade-intent adapter with no registry — every call answers its explicit gap
    /// citation (defensive; the runtime wires the real registry when one is configured).
    pub fn stub() -> Self {
        Self { registry: None }
    }
}

impl Default for FeedTradeIntentSink {
    fn default() -> Self {
        Self::stub()
    }
}

#[async_trait]
impl TradeIntentSink for FeedTradeIntentSink {
    async fn propose_trade(&self, intent: &TradeIntent) -> Result<TradeIntentStatus, PortError> {
        let Some(registry) = &self.registry else {
            return Err(PortError::unavailable(
                "trade-intent approval queue not wired; see tantra-runtime/docs/gaps/0007 item 17",
            ));
        };
        let schedule = CronSchedule::new(NEVER_CRON)
            .map_err(|e| PortError::internal(format!("invalid never-cron: {e}")))?;
        let mut spec = JobSpec::new(
            format!("trade-intent-{}-{}", intent.instrument.symbol, intent.side),
            schedule,
        );
        // Unique name (the registry refuses duplicate names): stamp the minted job id in.
        spec.name = format!(
            "trade-intent-{}-{}-{}",
            intent.instrument.symbol, intent.side, spec.id
        );
        spec.goal = format!(
            "{} {} {} {} of {} (algo {})",
            if intent.paper_trading {
                "paper"
            } else {
                "live"
            },
            intent.side,
            intent.order_type,
            intent.quantity.0,
            intent.instrument,
            intent.algo_id,
        );
        spec.status = JobStatus::PendingApproval;
        spec.approval_policy = ApprovalPolicy::Manual;
        spec.enabled = false;
        spec.metadata
            .insert("kind".to_string(), "trade-intent".to_string());
        spec.metadata
            .insert("instrument".to_string(), intent.instrument.to_string());
        spec.metadata
            .insert("side".to_string(), intent.side.clone());
        spec.metadata
            .insert("order_type".to_string(), intent.order_type.clone());
        spec.metadata
            .insert("quantity".to_string(), intent.quantity.0.to_string());
        if let Some(price) = intent.price {
            spec.metadata
                .insert("price".to_string(), price.0.to_string());
        }
        if intent.paper_trading {
            spec.metadata
                .insert("x-papertrading".to_string(), "true".to_string());
        }
        if let Some(rationale) = &intent.rationale {
            spec.metadata
                .insert("rationale".to_string(), rationale.clone());
        }

        let mut registry = registry.lock().unwrap_or_else(PoisonError::into_inner);
        let id = registry
            .add_job(spec)
            .map_err(|e| PortError::internal(e.to_string()))?;
        Ok(TradeIntentStatus {
            intent_id: id.to_string(),
            status: "pending_approval".to_string(),
            message: if intent.paper_trading {
                "trade intent queued for paper execution (x-papertrading); operator execution required"
                    .to_string()
            } else {
                "trade intent held for human approval (PendingApproval)".to_string()
            },
        })
    }
}
