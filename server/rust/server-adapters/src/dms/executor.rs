use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::json;
use thiserror::Error;
use tokio::sync::watch;
use tracing::{info, info_span, warn};

use super::{
    poller::{CompletionError, HeartbeatError, HeartbeatResult, PollError, PollResult, Poller},
    session::SessionSnapshot,
};
use server_core::{self, Services};

/// Configuration parameters required to materialize a job on disk and run the
/// Python pipeline.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub data_dir: PathBuf,
    pub reconstruction_url: String,
    pub cpu_workers: usize,
}

#[derive(Debug, Error)]
pub enum TaskExecutorError {
    #[error(transparent)]
    Poll(#[from] PollError),
    #[error(transparent)]
    Completion(#[from] CompletionError),
    #[error(transparent)]
    Failure(#[from] super::poller::FailureError),
    #[error(transparent)]
    SerializeMeta(#[from] serde_json::Error),
    #[error(transparent)]
    JobSetup(#[from] server_core::DomainError),
}

/// Drives the lease lifecycle by translating DMS tasks into local job
/// executions, invoking the Python runner, and reporting completion/failure
/// back to the orchestrator.
pub struct TaskExecutor<C, R> {
    poller: Poller<C, R>,
    services: Arc<Services>,
    config: ExecutorConfig,
}

impl<C, R> TaskExecutor<C, R>
where
    C: super::poller::DmsApi,
    R: rand::Rng,
{
    pub fn new(poller: Poller<C, R>, services: Arc<Services>, config: ExecutorConfig) -> Self {
        Self {
            poller,
            services,
            config,
        }
    }

    /// Attempts to lease a task once. Returns the suggested idle delay when no
    /// work is available.
    pub async fn step(&mut self) -> Result<Option<Duration>, TaskExecutorError> {
        let start = Instant::now();
        let result = match self.poller.poll_once(start).await? {
            PollResult::Idle { schedule } => Ok(Some(schedule.delay)),
            PollResult::AlreadyRunning => Ok(None),
            PollResult::Leased(snapshot) => {
                self.handle_lease(snapshot).await?;
                Ok(None)
            }
        };
        #[cfg(feature = "metrics")]
        metrics::histogram!("dms.poll.latency_ms").record(start.elapsed().as_secs_f64() * 1000.0);
        result
    }

    async fn handle_lease(&mut self, snapshot: SessionSnapshot) -> Result<(), TaskExecutorError> {
        let capability = snapshot.capability().to_string();
        // Unwrap legacy payload: task.meta.legacy becomes the job request body
        let request_json = serde_json::to_string(&snapshot.meta()["legacy"])?;
        let mut job = match server_core::create_job_metadata(
            &self.config.data_dir,
            &request_json,
            &self.config.reconstruction_url,
            None,
        ) {
            Ok(job) => job,
            Err(err) => {
                let details = json!({
                    "task_id": snapshot.task_id(),
                    "error": err.to_string(),
                });
                self.poller
                    .fail_task("JobSetupFailed", Some(details))
                    .await?;
                return Err(TaskExecutorError::JobSetup(err));
            }
        };

        // For legacy payloads, always use the access token provided inside
        // meta.legacy (already captured in job metadata). Do not override it
        // with the short-lived DMS lease token.
        if snapshot.access_token().is_none() {
            warn!(
                task_id = snapshot.task_id(),
                "Lease missing access token; using legacy payload token"
            );
        }

        let span = info_span!(
            "executor.run",
            task_id = %snapshot.task_id(),
            job_id = %job.meta.id,
            capability = %capability
        );
        let _guard = span.enter();
        info!(
            domain_id = %job.meta.domain_id,
            domain_url = %job.meta.domain_server_url,
            data_ids = job.meta.data_ids.len(),
            skip_manifest_upload = job.meta.skip_manifest_upload,
            "Starting Python pipeline"
        );
        #[cfg(feature = "metrics")]
        metrics::gauge!("dms.active_task").set(1.0);

        let run_result = server_core::execute_job(
            self.services.as_ref(),
            &mut job,
            &capability,
            self.config.cpu_workers,
        )
        .await;

        #[cfg(feature = "metrics")]
        metrics::gauge!("dms.active_task").set(0.0);

        match run_result {
            Ok(outputs) => {
                let completion_meta = json!({
                    "job_id": job.meta.id,
                    "domain_id": job.meta.domain_id,
                    "status": job.status,
                    "capability": capability,
                });
                self.poller
                    .complete_task(outputs, Some(completion_meta))
                    .await?;
                info!(task_id = snapshot.task_id(), "Job completed successfully");
            }
            Err(err) => {
                warn!(
                    task_id = snapshot.task_id(),
                    job_id = %job.meta.id,
                    error = %err,
                    "Python pipeline failed"
                );
                let failure_meta = json!({
                    "job_id": job.meta.id,
                    "domain_id": job.meta.domain_id,
                    "capability": capability,
                    "error": err.to_string(),
                });
                self.poller
                    .fail_task("JobFailed", Some(failure_meta))
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn shutdown(&mut self) {
        if let Some(snapshot) = self.poller.session_snapshot().await {
            let span = info_span!(
                "dms.shutdown_heartbeat",
                task_id = %snapshot.task_id(),
                capability = %snapshot.capability()
            );
            let _guard = span.enter();
            match self
                .poller
                .send_heartbeat(snapshot.task_id(), None, Instant::now())
                .await
            {
                Ok(HeartbeatResult::Scheduled(_)) => {
                    info!("Sent shutdown heartbeat");
                }
                Ok(HeartbeatResult::Canceled) => {
                    info!("Lease canceled during shutdown heartbeat");
                }
                Ok(HeartbeatResult::LostLease) => {
                    info!("Lease lost during shutdown heartbeat");
                }
                Err(HeartbeatError::NoActiveSession) => {}
                Err(err) => {
                    warn!(error = %err, "Failed to send shutdown heartbeat");
                }
            }
            self.poller.clear_session().await;
        }
        #[cfg(feature = "metrics")]
        metrics::gauge!("dms.active_task").set(0.0);
    }
}

/// Convenience helper that runs the executor in a loop, respecting idle delays.
pub async fn run_executor_loop<C, R>(
    mut executor: TaskExecutor<C, R>,
    idle_floor: Duration,
    mut shutdown: watch::Receiver<bool>,
) where
    C: super::poller::DmsApi + Send + 'static,
    R: rand::Rng + Send + 'static,
{
    loop {
        if *shutdown.borrow() {
            executor.shutdown().await;
            break;
        }

        let next_delay = match executor.step().await {
            Ok(delay) => delay.unwrap_or(idle_floor),
            Err(err) => {
                warn!(error = %err, "Task executor step failed");
                idle_floor
            }
        };

        tokio::select! {
            _ = shutdown.changed() => {
                executor.shutdown().await;
                break;
            }
            _ = tokio::time::sleep(next_delay) => {}
        }
    }
    info!("Task executor loop exiting");
}
