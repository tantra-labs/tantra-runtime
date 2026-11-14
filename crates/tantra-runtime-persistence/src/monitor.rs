//! Runtime↔agent monitor scheduler bridge (plan 0009 Phase D / agent gap 0006).
//!
//! Inverts through the agent-side ports: the agent `JobScheduler` (cron, budgets,
//! approval gate) stays in `tantra-agent-core`; this runtime-side bridge supplies
//! the persistence (a [`JobStore`]) and the monitor dispatch ([`MonitorRunner`]).

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tantra_agent_core::jobs::{
    InMemoryJobStore, JobId, JobResult, JobScheduler, JobSpec, JobStore, JobStoreError,
    JobTickAction, ManualApprovalGate,
};
use thiserror::Error;

/// Errors produced by the monitor scheduler bridge.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MonitorBridgeError {
    /// The backing [`JobStore`] rejected an operation.
    #[error("job store error: {0}")]
    Store(String),
    /// The [`MonitorRunner`] could not be reached or failed unexpectedly.
    #[error("monitor runner error: {0}")]
    Runner(String),
    /// The agent-side [`JobScheduler`] rejected the tick/recording.
    #[error("scheduler error: {0}")]
    Scheduler(String),
}

impl From<JobStoreError> for MonitorBridgeError {
    fn from(value: JobStoreError) -> Self {
        Self::Store(value.to_string())
    }
}

/// A monitor handler invoked when a scheduled monitor job is due and cleared.
#[async_trait]
pub trait MonitorRunner: Send + Sync + 'static {
    /// Run the monitor job. Return `Ok(summary)` on success or `Err(message)`.
    async fn run_monitor(&self, job_id: &str, goal: &str) -> Result<String, String>;
}

/// Outcome of dispatching one due monitor job during a tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorRun {
    /// The job that was dispatched.
    pub job_id: JobId,
    /// How the dispatch resolved.
    pub outcome: MonitorRunOutcome,
}

/// Dispatch outcome for a due job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonitorRunOutcome {
    /// The monitor ran and produced a summary.
    Success { summary: String },
    /// The monitor runner returned an error.
    Failed { message: String },
    /// The job requires human approval and was held, not executed.
    PendingApproval { action_id: String, details: String },
    /// The job was skipped (e.g. its turn budget is exhausted).
    Skipped { reason: String },
}

/// Bridge: evaluates agent-side cron schedules, dispatches due read-only monitor
/// jobs to a [`MonitorRunner`], and records results back through the [`JobStore`].
#[derive(Clone)]
pub struct MonitorSchedulerBridge {
    scheduler: Arc<JobScheduler<ManualApprovalGate>>,
    store: Arc<dyn JobStore>,
    runner: Arc<dyn MonitorRunner>,
}

impl MonitorSchedulerBridge {
    /// Build a bridge over the given store and runner.
    ///
    /// The scheduler is wired with the production-safe [`ManualApprovalGate`]:
    /// monitor jobs are read-only and therefore auto-run; any non-read-only job
    /// would require explicit approval and is never auto-executed.
    pub fn new(store: Arc<dyn JobStore>, runner: Arc<dyn MonitorRunner>) -> Self {
        let scheduler = JobScheduler::with_clock(ManualApprovalGate, Arc::new(Utc::now));
        Self {
            scheduler: Arc::new(scheduler),
            store,
            runner,
        }
    }

    /// Build a bridge over a fresh [`InMemoryJobStore`] (tests, demos, CLI).
    pub fn in_memory(runner: Arc<dyn MonitorRunner>) -> Self {
        Self::new(Arc::new(InMemoryJobStore::new()), runner)
    }

    /// Access the backing job store.
    pub fn store(&self) -> Arc<dyn JobStore> {
        self.store.clone()
    }

    /// Persist a monitor [`JobSpec`] and return its id.
    pub async fn register_monitor(&self, spec: &JobSpec) -> Result<JobId, MonitorBridgeError> {
        self.store.create_job(spec).await.map_err(Into::into)
    }

    /// Evaluate every scheduled job against `now`, dispatch due read-only
    /// monitors to the runner, and record each outcome in the store.
    ///
    /// Runs are returned in the same order as the scheduler produced them.
    pub async fn tick(&self, now: DateTime<Utc>) -> Result<Vec<MonitorRun>, MonitorBridgeError> {
        let actions = self
            .scheduler
            .tick(now, self.store.as_ref())
            .await
            .map_err(|e| MonitorBridgeError::Scheduler(e.to_string()))?;
        let mut runs = Vec::with_capacity(actions.len());
        for action in actions {
            match action {
                JobTickAction::Execute { id } => {
                    let outcome = match self.store.get_job(&id).await? {
                        Some(job) => match self.runner.run_monitor(&id, &job.spec.goal).await {
                            Ok(summary) => {
                                let outcome = MonitorRunOutcome::Success { summary };
                                self.scheduler
                                    .record_execution(self.store.as_ref(), &id, &JobResult::Success)
                                    .await
                                    .map_err(|e| MonitorBridgeError::Scheduler(e.to_string()))?;
                                outcome
                            }
                            Err(message) => {
                                let outcome = MonitorRunOutcome::Failed {
                                    message: message.clone(),
                                };
                                self.scheduler
                                    .record_execution(
                                        self.store.as_ref(),
                                        &id,
                                        &JobResult::Failed(message),
                                    )
                                    .await
                                    .map_err(|e| MonitorBridgeError::Scheduler(e.to_string()))?;
                                outcome
                            }
                        },
                        None => MonitorRunOutcome::Failed {
                            message: format!("monitor job not found: {id}"),
                        },
                    };
                    runs.push(MonitorRun {
                        job_id: id,
                        outcome,
                    });
                }
                JobTickAction::PendingApproval {
                    id,
                    action_id,
                    details,
                } => {
                    self.scheduler
                        .record_execution(self.store.as_ref(), &id, &JobResult::PendingApproval)
                        .await
                        .map_err(|e| MonitorBridgeError::Scheduler(e.to_string()))?;
                    runs.push(MonitorRun {
                        job_id: id,
                        outcome: MonitorRunOutcome::PendingApproval { action_id, details },
                    });
                }
                JobTickAction::Skipped { id, reason } => {
                    self.scheduler
                        .record_execution(self.store.as_ref(), &id, &JobResult::BudgetExhausted)
                        .await
                        .map_err(|e| MonitorBridgeError::Scheduler(e.to_string()))?;
                    runs.push(MonitorRun {
                        job_id: id,
                        outcome: MonitorRunOutcome::Skipped { reason },
                    });
                }
            }
        }
        Ok(runs)
    }
}
