use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::{json, Value};
use thiserror::Error;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, info_span, warn};

use super::{
    poller::{CompletionError, HeartbeatError, HeartbeatResult, PollError, PollResult, Poller},
    session::SessionSnapshot,
};
use server_core::types::JobRequestData;
use server_core::{self, AccessTokenSink, Services};

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
                let immediate_repoll = self.handle_lease(*snapshot).await?;
                if immediate_repoll {
                    Ok(Some(Duration::from_millis(0)))
                } else {
                    Ok(None)
                }
            }
        };
        #[cfg(feature = "metrics")]
        metrics::histogram!("dms.poll.latency_ms").record(start.elapsed().as_secs_f64() * 1000.0);
        result
    }

    async fn handle_lease(&mut self, snapshot: SessionSnapshot) -> Result<bool, TaskExecutorError> {
        let capability = snapshot.capability().to_string();
        let request_json = match build_job_request_json(&snapshot) {
            Ok(json) => json,
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

        // Prefer the short‑lived session token from the lease/heartbeat when available.
        if let Some(token) = snapshot.access_token() {
            job.set_access_token(token);
        } else {
            warn!(
                task_id = snapshot.task_id(),
                "Lease missing access token; falling back to legacy payload token"
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

        // Drive heartbeats while the Python pipeline runs to keep the lease alive
        let cancel_token = CancellationToken::new();
        let (run_result, lease_interrupted) = {
            // Clone the job root path for progress sampling without borrowing `job`
            let job_root = job.job_path.clone();
            let run_future = server_core::execute_job(
                self.services.as_ref(),
                &mut job,
                &capability,
                self.config.cpu_workers,
                cancel_token.clone(),
            );
            tokio::pin!(run_future);

            // Progress heartbeat controls
            const PROGRESS_TRIGGER_STEP_PCT: i32 = 5; // significant change threshold
            const PROGRESS_MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

            let mut last_reported_pct: Option<i32> = None;
            let mut last_reported_status: Option<String> = None;
            let mut last_progress_hb_at: Option<Instant> = None; // only for progress-driven heartbeats
            let mut last_progress_poll_at: Instant = Instant::now();
            let mut lease_interrupted_flag = false;

            loop {
                if lease_interrupted_flag {
                    let res = (&mut run_future).await;
                    break (res, true);
                }

                let now = Instant::now();
                let snapshot_opt = self.poller.session_snapshot().await;

                if snapshot_opt.is_none() {
                    let maybe_result = tokio::select! {
                        res = &mut run_future => Some(res),
                        _ = tokio::time::sleep(Duration::from_millis(50)) => None,
                    };
                    if let Some(res) = maybe_result {
                        break (res, lease_interrupted_flag);
                    }
                    continue;
                }

                let snapshot = snapshot_opt.expect("checked some");

                // 1) TTL-based heartbeat: if due, send immediately
                if let Some(due) = snapshot.next_heartbeat_due() {
                    if due <= now {
                        let progress = progress_payload(&job_root);
                        match self
                            .poller
                            .send_heartbeat(snapshot.task_id(), progress.clone(), now)
                            .await
                        {
                            Ok(HeartbeatResult::Scheduled(_)) => {
                                // Update last reported to the values we just sent (if any)
                                if let Some(p) = progress.as_ref() {
                                    if let (Some(pct), Some(status)) = (
                                        p.get("pct").and_then(|x| x.as_i64()).map(|x| x as i32),
                                        p.get("status")
                                            .and_then(|x| x.as_str())
                                            .map(|s| s.to_string()),
                                    ) {
                                        last_reported_pct = Some(pct);
                                        last_reported_status = Some(status);
                                    }
                                }
                            }
                            Ok(HeartbeatResult::Canceled) => {
                                lease_interrupted_flag = true;
                                cancel_token.cancel();
                                warn!(
                                    task_id = snapshot.task_id(),
                                    capability = %snapshot.capability(),
                                    "Lease canceled during execution; stopping heartbeats"
                                );
                                continue;
                            }
                            Ok(HeartbeatResult::LostLease) => {
                                lease_interrupted_flag = true;
                                cancel_token.cancel();
                                warn!(
                                    task_id = snapshot.task_id(),
                                    capability = %snapshot.capability(),
                                    "Lease lost during execution; stopping heartbeats"
                                );
                                continue;
                            }
                            Err(HeartbeatError::NoActiveSession) => { /* ignore */ }
                            Err(err) => {
                                warn!(error = %err, "Heartbeat attempt failed");
                            }
                        }
                        // Jump to next loop iteration to recompute schedule
                        continue;
                    }
                }

                // 2) Progress-based heartbeat: poll manifest/fs periodically
                let progress_poll_interval = self
                    .services
                    .as_ref()
                    .manifest_interval
                    .min(Duration::from_secs(2));
                if now.duration_since(last_progress_poll_at) >= progress_poll_interval {
                    last_progress_poll_at = now;
                    let (pct, status) = progress_from_manifest(&job_root)
                        .unwrap_or_else(|| compute_progress_from_fs(&job_root));

                    let significant = match (last_reported_pct, last_reported_status.as_deref()) {
                        (Some(prev_pct), Some(prev_status)) => {
                            (pct - prev_pct).abs() >= PROGRESS_TRIGGER_STEP_PCT
                                || status.as_str() != prev_status
                        }
                        _ => true, // first report
                    };
                    let past_min_interval = last_progress_hb_at
                        .map(|t| now.duration_since(t) >= PROGRESS_MIN_HEARTBEAT_INTERVAL)
                        .unwrap_or(true);

                    if significant && past_min_interval {
                        let progress = Some(json!({ "pct": pct, "status": status }));
                        match self
                            .poller
                            .send_heartbeat(snapshot.task_id(), progress.clone(), now)
                            .await
                        {
                            Ok(HeartbeatResult::Scheduled(_)) => {
                                last_progress_hb_at = Some(now);
                                last_reported_pct = Some(pct);
                                last_reported_status = Some(status);
                            }
                            Ok(HeartbeatResult::Canceled) => {
                                lease_interrupted_flag = true;
                                cancel_token.cancel();
                                warn!(
                                    task_id = snapshot.task_id(),
                                    capability = %snapshot.capability(),
                                    "Lease canceled during execution; stopping heartbeats"
                                );
                                continue;
                            }
                            Ok(HeartbeatResult::LostLease) => {
                                lease_interrupted_flag = true;
                                cancel_token.cancel();
                                warn!(
                                    task_id = snapshot.task_id(),
                                    capability = %snapshot.capability(),
                                    "Lease lost during execution; stopping heartbeats"
                                );
                                continue;
                            }
                            Err(HeartbeatError::NoActiveSession) => { /* ignore */ }
                            Err(err) => {
                                warn!(error = %err, "Progress heartbeat attempt failed");
                            }
                        }
                        // After emitting progress heartbeat, recompute schedule next loop
                        continue;
                    }
                }

                // 3) Sleep until the nearer of next TTL or next progress poll
                let ttl_sleep = snapshot
                    .next_heartbeat_due()
                    .and_then(|due| due.checked_duration_since(now))
                    .unwrap_or(Duration::from_millis(250));
                let progress_sleep = progress_poll_interval
                    .saturating_sub(now.duration_since(last_progress_poll_at));
                let sleep_duration = ttl_sleep.min(progress_sleep).max(Duration::from_millis(50));

                let maybe_result = tokio::select! {
                    res = &mut run_future => { Some(res) },
                    _ = tokio::time::sleep(sleep_duration) => None,
                };
                if let Some(res) = maybe_result {
                    break (res, lease_interrupted_flag);
                }
            }
        };

        #[cfg(feature = "metrics")]
        metrics::gauge!("dms.active_task").set(0.0);

        if lease_interrupted {
            match run_result {
                Ok(_) => info!(
                    task_id = snapshot.task_id(),
                    capability = %capability,
                    "Job canceled due to lease loss"
                ),
                Err(err) => warn!(
                    task_id = snapshot.task_id(),
                    capability = %capability,
                    error = %err,
                    "Job aborted after lease cancellation"
                ),
            }
            self.poller.clear_session().await;
            info!(
                task_id = snapshot.task_id(),
                capability = %capability,
                "Cleared canceled lease; resuming polling immediately"
            );
            return Ok(true);
        }

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

        Ok(false)
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

fn build_job_request_json(snapshot: &SessionSnapshot) -> server_core::Result<String> {
    if let Some(legacy) = snapshot
        .meta()
        .get("legacy")
        .filter(|value| !value.is_null())
    {
        return serde_json::to_string(legacy).map_err(server_core::DomainError::from);
    }

    let access_token = snapshot.access_token().ok_or_else(|| {
        server_core::DomainError::BadRequest("task lease missing access token".into())
    })?;
    let domain_id = snapshot.domain_id().ok_or_else(|| {
        server_core::DomainError::BadRequest("task lease missing domain_id".into())
    })?;
    let processing_type = processing_type_from_capability(snapshot.capability())?;
    let inputs_cids = snapshot.inputs_cids().to_vec();
    if inputs_cids.is_empty() {
        return Err(server_core::DomainError::BadRequest(
            "task lease missing inputs_cids".into(),
        ));
    }

    let domain_server_url = snapshot
        .domain_server_url()
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| {
            server_core::DomainError::BadRequest("task lease missing domain_server_url".into())
        })?
        .to_string();

    let request = JobRequestData {
        data_ids: Vec::new(),
        domain_id: domain_id.to_string(),
        access_token: access_token.to_string(),
        processing_type,
        inputs_cids,
        domain_server_url: Some(domain_server_url),
        skip_manifest_upload: None,
        override_job_name: None,
        override_manifest_id: None,
    };

    serde_json::to_string(&request).map_err(server_core::DomainError::from)
}

fn processing_type_from_capability(capability: &str) -> server_core::Result<String> {
    let processing = match capability {
        "/reconstruction/local-refinement/v1" => Some("local_refinement"),
        "/reconstruction/global-refinement/v1" => Some("global_refinement"),
        "/reconstruction/local-and-global-refinement/v1" => Some("local_and_global_refinement"),
        _ => None,
    };

    if let Some(p) = processing {
        return Ok(p.to_string());
    }

    let lower = capability.to_ascii_lowercase();
    if lower.contains("local-and-global") || lower.contains("local_and_global") {
        return Ok("local_and_global_refinement".into());
    }
    if lower.contains("global") && !lower.contains("local") {
        return Ok("global_refinement".into());
    }
    if lower.contains("local") {
        return Ok("local_refinement".into());
    }
    if lower.contains("refinement") {
        return Ok("local_and_global_refinement".into());
    }

    Err(server_core::DomainError::BadRequest(format!(
        "unsupported reconstruction capability `{capability}`"
    )))
}

// --- progress helpers ---

fn progress_payload(job_root: &std::path::Path) -> Option<Value> {
    if let Some((pct, status)) = progress_from_manifest(job_root) {
        return Some(json!({ "pct": pct, "status": status }));
    }
    let (pct, status) = compute_progress_from_fs(job_root);
    Some(json!({ "pct": pct, "status": status }))
}

fn progress_from_manifest(job_root: &std::path::Path) -> Option<(i32, String)> {
    let path = job_root.join("job_manifest.json");
    let bytes = std::fs::read(path).ok()?;
    let v: Value = serde_json::from_slice(&bytes).ok()?;
    let pct = v
        .get("jobProgress")
        .and_then(|x| x.as_i64())
        .map(|x| x as i32)?;
    let status = v
        .get("jobStatusDetails")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    Some((pct, status))
}

fn compute_progress_from_fs(job_root: &std::path::Path) -> (i32, String) {
    use std::fs;

    let datasets_root = job_root.join("datasets");
    let refined_root = job_root.join("refined").join("local");

    let total = fs::read_dir(&datasets_root)
        .ok()
        .map(|it| {
            it.filter_map(|e| e.ok())
                .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                .count()
        })
        .unwrap_or(0) as i32;

    let mut refined = 0i32;
    if let Ok(entries) = fs::read_dir(&datasets_root) {
        for e in entries.flatten() {
            if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let scan_id = e.file_name().to_string_lossy().to_string();
            let refined_scan_path = refined_root.join(&scan_id);
            if refined_scan_path.exists() {
                refined += 1;
            }
        }
    }
    let refined_adj = (refined - 1).max(0);
    let progress = if total > 0 {
        (refined_adj as f64 / total as f64 * 100.0).round() as i32
    } else {
        0
    };
    let status_text = format!("Processed {} of {} scans", refined_adj, total);
    (progress, status_text)
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
