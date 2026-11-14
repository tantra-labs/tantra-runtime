//! Integration tests for the runtime↔agent monitor scheduler bridge
//! (plan 0009 Phase D / `tantra-agent` gap 0006).
//!
//! Exercises the inverted ports: the agent-side `JobScheduler` drives cron
//! dispatch and budget/approval gating, while this runtime-side bridge supplies
//! the `JobStore` (in-memory) and the `MonitorRunner` dispatch.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tantra_agent_core::jobs::{JobResult, JobSpec, JobStatus};
use tantra_runtime_persistence::monitor::{
    MonitorRunOutcome, MonitorRunner, MonitorSchedulerBridge,
};
use tokio::sync::Mutex;

/// A test [`MonitorRunner`] recording `(job_id, goal)` invocations and
/// returning a configurable result.
#[derive(Clone)]
struct FakeRunner {
    calls: Arc<Mutex<Vec<(String, String)>>>,
    result: Result<String, String>,
}

impl FakeRunner {
    fn new(result: Result<String, String>) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            result,
        }
    }

    async fn called(&self) -> Vec<(String, String)> {
        self.calls.lock().await.clone()
    }
}

#[async_trait]
impl MonitorRunner for FakeRunner {
    async fn run_monitor(&self, job_id: &str, goal: &str) -> Result<String, String> {
        self.calls
            .lock()
            .await
            .push((job_id.to_string(), goal.to_string()));
        self.result.clone()
    }
}

/// Parse an RFC3339 timestamp as UTC (test helper).
fn ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .expect("valid RFC3339 timestamp")
        .with_timezone(&Utc)
}

/// A due, read-only monitor spec: scheduled on weekdays at 09:00 with a past
/// `next_run` so a `tick` at `2020-01-01T00:00:01+00:00` dispatches it.
fn due_monitor_spec(id: &str, goal: &str) -> JobSpec {
    let mut spec = JobSpec::new(id, goal).with_schedule("0 9 * * 1-5");
    spec.next_run = Some("2020-01-01T00:00:00+00:00".to_string());
    spec
}

const NOW: &str = "2020-01-01T00:00:01+00:00";

#[tokio::test]
async fn due_read_only_monitor_dispatched_once() {
    let runner = Arc::new(FakeRunner::new(Ok("all good".to_string())));
    let bridge = MonitorSchedulerBridge::in_memory(runner.clone());

    let id = bridge
        .register_monitor(&due_monitor_spec("m1", "monitor RELIANCE 200-SMA"))
        .await
        .unwrap();
    assert_eq!(id, "m1");

    let runs = bridge.tick(ts(NOW)).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].job_id, "m1");
    assert_eq!(
        runs[0].outcome,
        MonitorRunOutcome::Success {
            summary: "all good".to_string()
        }
    );

    // Runner invoked exactly once with the job id and goal.
    let called = runner.called().await;
    assert_eq!(
        called,
        vec![("m1".to_string(), "monitor RELIANCE 200-SMA".to_string())]
    );

    // Execution recorded through the store.
    let job = bridge
        .store()
        .get_job(&"m1".to_string())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.runs, 1);
    assert_eq!(job.last_result, Some(JobResult::Success));
}

#[tokio::test]
async fn future_monitor_not_dispatched() {
    let runner = Arc::new(FakeRunner::new(Ok("ok".to_string())));
    let bridge = MonitorSchedulerBridge::in_memory(runner.clone());

    let mut spec = due_monitor_spec("m2", "future goal");
    spec.next_run = Some("2030-01-01T00:00:00+00:00".to_string());
    bridge.register_monitor(&spec).await.unwrap();

    let runs = bridge.tick(ts(NOW)).await.unwrap();
    assert!(runs.is_empty());
    assert!(runner.called().await.is_empty());
}

#[tokio::test]
async fn runner_failure_recorded() {
    let runner = Arc::new(FakeRunner::new(Err("boom".to_string())));
    let bridge = MonitorSchedulerBridge::in_memory(runner.clone());

    bridge
        .register_monitor(&due_monitor_spec("m3", "failing goal"))
        .await
        .unwrap();

    let runs = bridge.tick(ts(NOW)).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(
        runs[0].outcome,
        MonitorRunOutcome::Failed {
            message: "boom".to_string()
        }
    );

    let job = bridge
        .store()
        .get_job(&"m3".to_string())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(job.last_result, Some(JobResult::Failed(m)) if m == "boom"));
}

#[tokio::test]
async fn non_read_only_monitor_held_for_approval() {
    let runner = Arc::new(FakeRunner::new(Ok("ok".to_string())));
    let bridge = MonitorSchedulerBridge::in_memory(runner.clone());

    let mut spec = due_monitor_spec("m4", "writes goal");
    spec.read_only = false;
    bridge.register_monitor(&spec).await.unwrap();

    let runs = bridge.tick(ts(NOW)).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert!(matches!(
        runs[0].outcome,
        MonitorRunOutcome::PendingApproval { ref action_id, .. }
            if action_id == "approve-m4"
    ));

    // Never executed: the runner must not have been called.
    assert!(runner.called().await.is_empty());

    let job = bridge
        .store()
        .get_job(&"m4".to_string())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(job.spec.status, JobStatus::PendingApproval { .. }));
}

#[tokio::test]
async fn budget_exhausted_monitor_skipped() {
    let runner = Arc::new(FakeRunner::new(Ok("ok".to_string())));
    let bridge = MonitorSchedulerBridge::in_memory(runner.clone());

    let mut spec = due_monitor_spec("m5", "budget goal");
    spec.budget.max_turns = 0;
    bridge.register_monitor(&spec).await.unwrap();

    let runs = bridge.tick(ts(NOW)).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert!(matches!(
        runs[0].outcome,
        MonitorRunOutcome::Skipped { ref reason } if reason.contains("max_turns")
    ));
    assert!(runner.called().await.is_empty());
}

#[tokio::test]
async fn register_monitor_persists() {
    let runner = Arc::new(FakeRunner::new(Ok("ok".to_string())));
    let bridge = MonitorSchedulerBridge::in_memory(runner.clone());

    let id = bridge
        .register_monitor(&due_monitor_spec("m6", "persist goal"))
        .await
        .unwrap();
    assert_eq!(id, "m6");

    let job = bridge.store().get_job(&id).await.unwrap().unwrap();
    assert_eq!(job.spec.id, "m6");
    assert_eq!(job.spec.goal, "persist goal");
    assert!(job.spec.read_only);
}
