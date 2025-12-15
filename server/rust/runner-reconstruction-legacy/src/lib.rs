//! runner-reconstruction-legacy: skeleton runner for legacy reconstruction.

use std::{
    env,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use compute_runner_api::ArtifactSink;
use compute_runner_api::{Runner, TaskCtx};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::{
    fs,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::{interval, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

mod domain_lookup;
pub mod input;
pub mod manifest;
pub mod output;
pub mod python;
pub mod refined;
pub mod strategy;
pub mod summary;
pub mod workspace;

use crate::strategy::unzip_refined_scan;
use manifest::{ManifestState, ProgressListener};
use posemesh_domain_http::domain_data::download_by_id;
use uuid::Uuid;
use workspace::Workspace;

const LEGACY_INITIAL_STATUS: &str = "Request received by reconstruction server";
const LEGACY_PROGRESS_INTERVAL_SECS: u64 = 30;

#[derive(Serialize)]
struct JobMetadataRecord {
    // Fields expected by the Python manifest writer (utils/data_utils.py::save_manifest_json)
    id: String,
    name: String,
    domain_id: String,
    processing_type: String,
    created_at: String,
    domain_server_url: String,
    reconstruction_server_url: Option<String>,
    data_ids: Vec<String>,
    override_job_name: Option<String>,
    override_manifest_id: Option<String>,
}

struct JobContext {
    metadata: JobMetadataRecord,
    #[allow(dead_code)]
    skip_manifest_upload: bool,
    job_request_json: Option<String>,
}

impl JobContext {
    fn from_lease(lease: &compute_runner_api::LeaseEnvelope) -> Result<Self> {
        let legacy = lease
            .task
            .meta
            .get("legacy")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let legacy_obj = legacy.as_object();

        let skip_manifest_upload = legacy_obj
            .and_then(|map| map.get("skip_manifest_upload"))
            .and_then(|value| value.as_bool())
            .unwrap_or(false);

        let override_job_name = legacy_obj
            .and_then(|map| map.get("override_job_name"))
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        let override_manifest_id = legacy_obj
            .and_then(|map| map.get("override_manifest_id"))
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        let job_id = lease
            .task
            .job_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| lease.task.id.to_string());
        let job_name = override_job_name
            .clone()
            .unwrap_or_else(|| format!("job_{}", job_id));

        let processing_type = legacy_obj
            .and_then(|map| map.get("processing_type"))
            .and_then(|value| value.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| infer_processing_type(&lease.task.capability));

        let inputs_cids = lease.task.inputs_cids.clone();
        let domain_server_url = lease
            .domain_server_url
            .as_ref()
            .map(|url| url.to_string())
            .or_else(|| {
                legacy_obj
                    .and_then(|map| map.get("domain_server_url"))
                    .and_then(|value| value.as_str())
                    .map(|s| s.to_string())
            })
            .unwrap_or_default();
        let domain_server_url = domain_server_url.trim_end_matches('/').to_string();

        // Attempt to capture the externally advertised node URL so Python can include
        // it in the final manifest (reconstructionServerURL). This mirrors legacy Go.
        let reconstruction_server_url = env::var("NODE_URL").ok();

        // Extract data IDs from input CIDs (last path segment), falling back to the full CID if parsing fails.
        let data_ids: Vec<String> = inputs_cids
            .iter()
            .map(|cid| extract_last_segment(cid))
            .collect();

        let metadata = JobMetadataRecord {
            // Primary keys expected by Python
            id: job_id.clone(),
            name: job_name.clone(),
            domain_id: lease.domain_id.map(|id| id.to_string()).unwrap_or_default(),
            processing_type: processing_type.clone(),
            created_at: Utc::now().to_rfc3339(),
            domain_server_url: domain_server_url.clone(),
            reconstruction_server_url,
            data_ids,
            override_job_name,
            override_manifest_id,
        };

        let job_request_json = sanitized_job_request_json(&legacy, &metadata.domain_id)?;

        Ok(Self {
            metadata,
            skip_manifest_upload,
            job_request_json,
        })
    }

    fn processing_type(&self) -> &str {
        &self.metadata.processing_type
    }

    fn domain_data_name_suffix(&self) -> String {
        if let Some(override_name) = self.metadata.override_job_name.as_ref() {
            if !override_name.trim().is_empty() {
                return override_name.clone();
            }
        }
        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(&self.metadata.created_at) {
            return parsed.format("%Y-%m-%d_%H-%M-%S").to_string();
        }
        if !self.metadata.name.trim().is_empty() {
            return self.metadata.name.clone();
        }
        "legacy_job".to_string()
    }

    fn should_generate_scan_summary(&self) -> bool {
        matches!(
            self.processing_type(),
            "local_refinement" | "local_and_global_refinement"
        )
    }

    async fn persist_metadata(&self, path: &std::path::Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create metadata directory {}", parent.display()))?;
        }
        let bytes = serde_json::to_vec_pretty(&self.metadata)?;
        fs::write(path, bytes)
            .await
            .with_context(|| format!("write job metadata to {}", path.display()))?;
        Ok(())
    }

    async fn persist_job_request(&self, path: &std::path::Path) -> Result<()> {
        let Some(contents) = self.job_request_json.as_ref() else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create job request directory {}", parent.display()))?;
        }
        fs::write(path, contents)
            .await
            .with_context(|| format!("write job request to {}", path.display()))?;
        Ok(())
    }
}

fn sanitized_job_request_json(legacy: &Value, fallback_domain_id: &str) -> Result<Option<String>> {
    let map = match legacy.as_object() {
        Some(map) if !map.is_empty() => map,
        _ => return Ok(None),
    };
    let mut cloned = map.clone();
    cloned.remove("access_token");
    cloned
        .entry("domain_id".to_string())
        .or_insert_with(|| Value::String(fallback_domain_id.to_string()));
    let sanitized = Value::Object(cloned);
    let json = serde_json::to_string_pretty(&sanitized)?;
    Ok(Some(json))
}

#[cfg(test)]
mod job_request_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn sanitized_job_request_removes_access_token_and_inserts_domain_id() {
        let legacy = json!({
            "access_token": "secret",
            "data_ids": ["abc"],
            "processing_type": "local_refinement"
        });
        let json = sanitized_job_request_json(&legacy, "domain-123")
            .unwrap()
            .expect("expected sanitized json");
        let parsed: Value = serde_json::from_str(&json).unwrap();
        assert!(parsed.get("access_token").is_none());
        assert_eq!(parsed["domain_id"], "domain-123");
        assert_eq!(parsed["processing_type"], "local_refinement");
    }

    #[tokio::test]
    async fn persist_job_request_writes_file() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("job_request.json");
        let metadata = JobMetadataRecord {
            id: "id".into(),
            name: "job_id".into(),
            domain_id: "domain".into(),
            processing_type: "local_refinement".into(),
            created_at: chrono::Utc::now().to_rfc3339(),
            domain_server_url: "https://example.com".into(),
            reconstruction_server_url: None,
            data_ids: vec!["abc".into()],
            override_job_name: None,
            override_manifest_id: None,
        };
        let job_ctx = JobContext {
            metadata,
            skip_manifest_upload: false,
            job_request_json: Some("{\"foo\":1}".into()),
        };
        job_ctx.persist_job_request(&path).await.unwrap();
        let contents = std::fs::read_to_string(path).unwrap();
        assert!(contents.contains("\"foo\""));
    }
}

fn infer_processing_type(capability: &str) -> String {
    let lower = capability.to_ascii_lowercase();
    if lower.contains("local-and-global") || lower.contains("local_and_global") {
        "local_and_global_refinement".into()
    } else if lower.contains("global") && !lower.contains("local") {
        "global_refinement".into()
    } else if lower.contains("local") {
        "local_refinement".into()
    } else {
        "local_and_global_refinement".into()
    }
}

/// Configuration for the legacy reconstruction runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerConfig {
    /// Optional base directory for job workspaces.
    pub workspace_root: Option<PathBuf>,
    /// Python executable used to launch the refinement pipeline.
    pub python_bin: PathBuf,
    /// Python entrypoint script.
    pub python_script: PathBuf,
    /// Additional arguments passed to the python script.
    pub python_args: Vec<String>,
    /// Number of CPU workers granted to the pipeline.
    pub cpu_workers: usize,
}

impl RunnerConfig {
    pub const ENV_WORKSPACE_ROOT: &'static str = "LEGACY_RUNNER_WORKSPACE_ROOT";
    pub const ENV_PYTHON_BIN: &'static str = "LEGACY_RUNNER_PYTHON_BIN";
    pub const ENV_PYTHON_SCRIPT: &'static str = "LEGACY_RUNNER_PYTHON_SCRIPT";
    pub const ENV_PYTHON_ARGS: &'static str = "LEGACY_RUNNER_PYTHON_ARGS";
    pub const ENV_CPU_WORKERS: &'static str = "LEGACY_RUNNER_CPU_WORKERS";

    pub const DEFAULT_PYTHON_BIN: &'static str = "python3";
    pub const DEFAULT_PYTHON_SCRIPT: &'static str = "main.py";
    pub const DEFAULT_CPU_WORKERS: usize = 2;

    /// Build a config from environment variables.
    pub fn from_env() -> Result<Self> {
        let workspace_root = match env::var(Self::ENV_WORKSPACE_ROOT) {
            Ok(v) if !v.trim().is_empty() => Some(PathBuf::from(v)),
            _ => None,
        };

        let python_bin = env::var(Self::ENV_PYTHON_BIN)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(Self::DEFAULT_PYTHON_BIN));

        let python_script = env::var(Self::ENV_PYTHON_SCRIPT)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(Self::DEFAULT_PYTHON_SCRIPT));

        let python_args = env::var(Self::ENV_PYTHON_ARGS)
            .map(|raw| raw.split_whitespace().map(|s| s.to_string()).collect())
            .unwrap_or_else(|_| Vec::new());

        let cpu_workers = match env::var(Self::ENV_CPU_WORKERS) {
            Ok(val) if !val.trim().is_empty() => val
                .parse::<usize>()
                .with_context(|| format!("invalid {} value", Self::ENV_CPU_WORKERS))?,
            _ => Self::DEFAULT_CPU_WORKERS,
        };

        Ok(Self {
            workspace_root,
            python_bin,
            python_script,
            python_args,
            cpu_workers,
        })
    }
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            workspace_root: None,
            python_bin: PathBuf::from(Self::DEFAULT_PYTHON_BIN),
            python_script: PathBuf::from(Self::DEFAULT_PYTHON_SCRIPT),
            python_args: Vec::new(),
            cpu_workers: Self::DEFAULT_CPU_WORKERS,
        }
    }
}

/// Public crate identifier used by workspace smoke tests.
pub const CRATE_NAME: &str = "runner-reconstruction-legacy";

/// Skeleton reconstruction runner.
pub struct RunnerReconstructionLegacy {
    config: RunnerConfig,
    capability: &'static str,
}

impl RunnerReconstructionLegacy {
    pub const CAPABILITY_LOCAL_AND_GLOBAL: &'static str =
        "/reconstruction/local-and-global-refinement/v1";
    pub const CAPABILITY_GLOBAL_ONLY: &'static str = "/reconstruction/global-refinement/v1";
    pub const CAPABILITY_LOCAL_ONLY: &'static str = "/reconstruction/local-refinement/v1";
    pub const SUPPORTED_CAPABILITIES: [&'static str; 3] = [
        Self::CAPABILITY_LOCAL_ONLY,
        Self::CAPABILITY_GLOBAL_ONLY,
        Self::CAPABILITY_LOCAL_AND_GLOBAL,
    ];

    pub fn new(config: RunnerConfig) -> Self {
        Self::with_capability(Self::CAPABILITY_LOCAL_AND_GLOBAL, config)
    }

    pub fn with_capability(capability: &'static str, config: RunnerConfig) -> Self {
        Self { config, capability }
    }

    pub fn for_all_capabilities(config: RunnerConfig) -> Vec<Self> {
        Self::SUPPORTED_CAPABILITIES
            .iter()
            .map(|cap| Self::with_capability(cap, config.clone()))
            .collect()
    }

    /// Access the runner configuration.
    pub fn config(&self) -> &RunnerConfig {
        &self.config
    }

    /// Create a workspace for the given domain/job identifiers using the runner configuration.
    pub fn create_workspace(
        &self,
        domain_id: &str,
        job_id: Option<&str>,
        task_id: &str,
    ) -> Result<workspace::Workspace> {
        workspace::Workspace::create(
            self.config.workspace_root.as_deref(),
            domain_id,
            job_id,
            task_id,
        )
    }
}

impl Default for RunnerReconstructionLegacy {
    fn default() -> Self {
        Self::new(RunnerConfig::default())
    }
}

#[async_trait::async_trait]
impl Runner for RunnerReconstructionLegacy {
    fn capability(&self) -> &'static str {
        self.capability
    }

    async fn run(&self, ctx: TaskCtx<'_>) -> anyhow::Result<()> {
        let lease = ctx.lease;
        let domain_id = lease
            .domain_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "domain".into());
        let job_id = lease.task.job_id.map(|id| id.to_string());
        let task_id = lease.task.id.to_string();

        let workspace = self.create_workspace(&domain_id, job_id.as_deref(), &task_id)?;
        // Ensure stateless behavior: schedule workspace cleanup on function exit (success or error).
        struct WorkspaceCleanup(std::path::PathBuf);
        impl Drop for WorkspaceCleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _workspace_cleanup = WorkspaceCleanup(workspace.root().to_path_buf());
        let job_ctx = JobContext::from_lease(lease)?;
        job_ctx
            .persist_metadata(workspace.job_metadata_path())
            .await?;
        job_ctx
            .persist_job_request(workspace.job_request_path())
            .await?;

        // Decide upload policy per-stage; global-only stages won't upload local zips.

        info!(
            capability = self.capability,
            domain_id = %job_ctx.metadata.domain_id,
            job_id = %job_ctx.metadata.name,
            task_id = %task_id,
            workspace = %workspace.root().display(),
            configured_workspace_root = %self
                .config
                .workspace_root
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<temp>".into()),
            "workspace prepared"
        );

        let name_suffix = job_ctx.domain_data_name_suffix();
        let override_manifest_id = job_ctx.metadata.override_manifest_id.as_deref();

        let (progress_tx, mut progress_rx) = unbounded_channel::<(i32, String)>();
        let state = ManifestState::default();
        update_progress(&state, &progress_tx, 0, LEGACY_INITIAL_STATUS);

        // Write initial manifest snapshot and publish it.
        manifest::write_processing_manifest_python(
            workspace.job_manifest_path(),
            workspace.root(),
            &self.config.python_bin,
            0,
            LEGACY_INITIAL_STATUS,
        )
        .await?;
        upload_manifest_artifact(
            ctx.output,
            "job_manifest.json",
            workspace.job_manifest_path(),
            &name_suffix,
            override_manifest_id,
        )
        .await?;
        if let Err(err) = input::materialize_datasets(&ctx, &workspace).await {
            let _ = manifest::write_failed_manifest_python(
                workspace.job_manifest_path(),
                workspace.root(),
                &self.config.python_bin,
                &format!("materialize datasets error: {}", err),
            )
            .await;
            let _ = upload_manifest_artifact(
                ctx.output,
                "job_manifest.json",
                workspace.job_manifest_path(),
                &name_suffix,
                override_manifest_id,
            )
            .await;
            return Err(err);
        }

        // Stage any pre-existing refined scan zip(s) so global can reuse them without re-running local.
        if let Err(err) = stage_existing_refined_outputs(&workspace).await {
            tracing::warn!(
                target: "runner_reconstruction_legacy",
                error = %err,
                "failed to stage existing refined outputs"
            );
        }

        // Try to stage refined outputs using Domain metadata (fallback silently to local-only if this fails)
        if let Err(err) = stage_from_domain(&workspace, &job_ctx, &ctx).await {
            tracing::info!(
                target: "runner_reconstruction_legacy",
                error = %err,
                "domain-driven staging failed; continuing with local-only classification"
            );
        }

        // mock mode removed; rely on runner-reconstruction-legacy-noop for smoke tests.

        // Generate scan data summary before Python starts so it can be embedded in final manifest (parity with Go).
        if job_ctx.should_generate_scan_summary() {
            if let Err(err) = summary::write_scan_data_summary(
                workspace.datasets(),
                workspace.scan_data_summary_path(),
            )
            .await
            {
                tracing::warn!(
                    target: "runner_reconstruction_legacy",
                    error = %err,
                    "failed to write scan data summary (pre-run)"
                );
            }
        }

        let cancel_token = CancellationToken::new();
        let manifest_task = manifest::spawn_python_processing_writer(
            workspace.job_manifest_path().to_path_buf(),
            workspace.root().to_path_buf(),
            self.config.python_bin.clone(),
            Duration::from_secs(LEGACY_PROGRESS_INTERVAL_SECS),
            state.clone(),
            Arc::new(ProgressForwarder {
                tx: progress_tx.clone(),
            }),
            cancel_token.clone(),
        );

        let mut refined_uploader = refined::RefinedUploader::new();
        let mut refined_interval = interval(Duration::from_secs(LEGACY_PROGRESS_INTERVAL_SECS));
        refined_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Compute pending vs refined scans based on presence of refined/local/<scan>/sfm core files.
        let all_scan_names =
            collect_scan_names(workspace.datasets()).context("collect scan names")?;
        let (pending_scans, refined_scans) = classify_scans(&workspace, &all_scan_names)?;

        // Progress parity (revised): treat each local scan as 1 task and add 1 task for global when applicable.
        let total_scans = all_scan_names.len();
        let baseline_done = refined_scans.len();
        let initial_pending = pending_scans.len();
        let includes_global = matches!(
            job_ctx.processing_type(),
            "global_refinement" | "local_and_global_refinement"
        );
        let denom_tasks = total_scans + if includes_global { 1 } else { 0 };
        let pre_progress = if denom_tasks > 0 {
            ((100.0 * (baseline_done as f64) / (denom_tasks as f64)).round()) as i32
        } else {
            100
        };

        // Update manifest before starting local: credit already-done scans in status.
        let pre_status = format!(
            "Local refinement {}/{} | total {}/{}",
            0, initial_pending, baseline_done, total_scans
        );
        update_progress(
            &state,
            &progress_tx,
            pre_progress.clamp(0, 100),
            pre_status.clone(),
        );
        // Write first so uploaded snapshot reflects the latest status.
        let _ = manifest::write_processing_manifest_python(
            workspace.job_manifest_path(),
            workspace.root(),
            &self.config.python_bin,
            pre_progress.clamp(0, 100),
            &pre_status,
        )
        .await;
        // Upload the snapshot immediately so users see pre-credited status before local starts.
        let _ = upload_manifest_artifact(
            ctx.output,
            "job_manifest.json",
            workspace.job_manifest_path(),
            &name_suffix,
            override_manifest_id,
        )
        .await;

        // Execute stages according to processing type
        match job_ctx.processing_type() {
            "local_refinement" => {
                if pending_scans.is_empty() {
                    info!("no pending scans for local_refinement; skipping local stage");
                } else if let Err(err) = execute_python_stage(
                    &self.config,
                    &job_ctx,
                    &workspace,
                    &ctx,
                    &cancel_token,
                    &mut progress_rx,
                    &mut refined_uploader,
                    &state,
                    &progress_tx,
                    &name_suffix,
                    override_manifest_id,
                    "local_refinement",
                    &pending_scans,
                    &refined_scans,
                    baseline_done as i32,
                    initial_pending as i32,
                    total_scans as i32,
                    includes_global,
                )
                .await
                {
                    cancel_token.cancel();
                    manifest_task.stop().await;
                    return Err(err);
                }
            }
            "global_refinement" => {
                // Validate all scans have refined outputs
                let missing: Vec<_> = all_scan_names
                    .iter()
                    .filter(|s| !is_refined_complete(&workspace, s))
                    .cloned()
                    .collect();
                if !missing.is_empty() {
                    manifest::write_failed_manifest_python(
                        workspace.job_manifest_path(),
                        workspace.root(),
                        &self.config.python_bin,
                        &format!("missing refined outputs for scans: {}", missing.join(", ")),
                    )
                    .await?;
                    upload_manifest_artifact(
                        ctx.output,
                        "job_manifest.json",
                        workspace.job_manifest_path(),
                        &name_suffix,
                        override_manifest_id,
                    )
                    .await?;
                    return Err(anyhow!("missing refined outputs for some scans"));
                }
                // No local stage: revised semantics (N/(total+1)) and announce global running.
                let status_global_running =
                    format!("Tasks {}/{} — global running", baseline_done, denom_tasks);
                update_progress(
                    &state,
                    &progress_tx,
                    pre_progress.clamp(0, 100),
                    status_global_running.clone(),
                );
                let _ = manifest::write_processing_manifest_python(
                    workspace.job_manifest_path(),
                    workspace.root(),
                    &self.config.python_bin,
                    pre_progress.clamp(0, 100),
                    &status_global_running,
                )
                .await;
                let _ = upload_manifest_artifact(
                    ctx.output,
                    "job_manifest.json",
                    workspace.job_manifest_path(),
                    &name_suffix,
                    override_manifest_id,
                )
                .await;
                if let Err(err) = execute_python_stage(
                    &self.config,
                    &job_ctx,
                    &workspace,
                    &ctx,
                    &cancel_token,
                    &mut progress_rx,
                    &mut refined_uploader,
                    &state,
                    &progress_tx,
                    &name_suffix,
                    override_manifest_id,
                    "global_refinement",
                    &[],
                    &[],
                    baseline_done as i32,
                    initial_pending as i32,
                    total_scans as i32,
                    includes_global,
                )
                .await
                {
                    cancel_token.cancel();
                    manifest_task.stop().await;
                    return Err(err);
                }
            }
            _ => {
                // local_and_global_refinement
                if pending_scans.is_empty() {
                    info!("no pending scans; running only global refinement");
                    let status_global_running =
                        format!("Tasks {}/{} — global running", baseline_done, denom_tasks);
                    update_progress(
                        &state,
                        &progress_tx,
                        pre_progress.clamp(0, 100),
                        status_global_running.clone(),
                    );
                    let _ = manifest::write_processing_manifest_python(
                        workspace.job_manifest_path(),
                        workspace.root(),
                        &self.config.python_bin,
                        pre_progress.clamp(0, 100),
                        &status_global_running,
                    )
                    .await;
                    let _ = upload_manifest_artifact(
                        ctx.output,
                        "job_manifest.json",
                        workspace.job_manifest_path(),
                        &name_suffix,
                        override_manifest_id,
                    )
                    .await;
                } else if let Err(err) = execute_python_stage(
                    &self.config,
                    &job_ctx,
                    &workspace,
                    &ctx,
                    &cancel_token,
                    &mut progress_rx,
                    &mut refined_uploader,
                    &state,
                    &progress_tx,
                    &name_suffix,
                    override_manifest_id,
                    "local_refinement",
                    &pending_scans,
                    &refined_scans,
                    baseline_done as i32,
                    initial_pending as i32,
                    total_scans as i32,
                    includes_global,
                )
                .await
                {
                    cancel_token.cancel();
                    manifest_task.stop().await;
                    return Err(err);
                }
                // After local completion, show progress as total_scans / (total_scans + 1) when global will run.
                let post_local_progress = if includes_global && total_scans > 0 {
                    ((100.0 * (total_scans as f64) / ((total_scans + 1) as f64)).round()) as i32
                } else {
                    100
                };
                // Announce global pending, then running, with explicit write+upload
                let status_global_pending =
                    format!("Tasks {}/{} — global pending", total_scans, denom_tasks);
                update_progress(
                    &state,
                    &progress_tx,
                    post_local_progress.clamp(0, 100),
                    status_global_pending.clone(),
                );
                let _ = manifest::write_processing_manifest_python(
                    workspace.job_manifest_path(),
                    workspace.root(),
                    &self.config.python_bin,
                    post_local_progress.clamp(0, 100),
                    &status_global_pending,
                )
                .await;
                let _ = upload_manifest_artifact(
                    ctx.output,
                    "job_manifest.json",
                    workspace.job_manifest_path(),
                    &name_suffix,
                    override_manifest_id,
                )
                .await;
                let status_global_running =
                    format!("Tasks {}/{} — global running", total_scans, denom_tasks);
                update_progress(
                    &state,
                    &progress_tx,
                    post_local_progress.clamp(0, 100),
                    status_global_running.clone(),
                );
                let _ = manifest::write_processing_manifest_python(
                    workspace.job_manifest_path(),
                    workspace.root(),
                    &self.config.python_bin,
                    post_local_progress.clamp(0, 100),
                    &status_global_running,
                )
                .await;
                let _ = upload_manifest_artifact(
                    ctx.output,
                    "job_manifest.json",
                    workspace.job_manifest_path(),
                    &name_suffix,
                    override_manifest_id,
                )
                .await;
                if let Err(err) = execute_python_stage(
                    &self.config,
                    &job_ctx,
                    &workspace,
                    &ctx,
                    &cancel_token,
                    &mut progress_rx,
                    &mut refined_uploader,
                    &state,
                    &progress_tx,
                    &name_suffix,
                    override_manifest_id,
                    "global_refinement",
                    &[],
                    &[],
                    baseline_done as i32,
                    initial_pending as i32,
                    total_scans as i32,
                    includes_global,
                )
                .await
                {
                    cancel_token.cancel();
                    manifest_task.stop().await;
                    return Err(err);
                }
            }
        }

        // Stop background tasks
        drop(progress_tx);
        cancel_token.cancel();
        manifest_task.stop().await;

        // Upload final global outputs only when a global stage ran
        if matches!(
            job_ctx.processing_type(),
            "global_refinement" | "local_and_global_refinement"
        ) {
            if let Err(err) = output::upload_final_outputs(
                &workspace,
                ctx.output,
                &name_suffix,
                override_manifest_id,
            )
            .await
            {
                let _ = manifest::write_failed_manifest_python(
                    workspace.job_manifest_path(),
                    workspace.root(),
                    &self.config.python_bin,
                    &format!("upload refined outputs error: {}", err),
                )
                .await;
                let _ = upload_manifest_artifact(
                    ctx.output,
                    "job_manifest.json",
                    workspace.job_manifest_path(),
                    &name_suffix,
                    override_manifest_id,
                )
                .await;
                return Err(err);
            }
            if !workspace.job_manifest_path().exists() {
                warn!(
                    capability = self.capability,
                    domain_id = %job_ctx.metadata.domain_id,
                    job_name = %job_ctx.metadata.name,
                    manifest = %workspace.job_manifest_path().display(),
                    "python pipeline did not produce job manifest; skipping upload"
                );
            }
            // Do not upload job_manifest.json here; refined_manifest.json carries final state.
        } else {
            // Final sweep for local-only runs
            let _ = refined_uploader.process(&workspace, ctx.output, true).await;
        }

        ctx.ctrl
            .progress(json!({"progress": 100, "status": "succeeded"}))
            .await?;

        Ok(())
    }
}

struct ProgressForwarder {
    tx: UnboundedSender<(i32, String)>,
}

#[async_trait]
impl ProgressListener for ProgressForwarder {
    async fn report_progress(&self, progress: i32, status: String) -> Result<()> {
        let _ = self.tx.send((progress, status));
        Ok(())
    }
}

fn update_progress(
    state: &ManifestState,
    tx: &UnboundedSender<(i32, String)>,
    progress: i32,
    status: impl Into<String>,
) {
    let status = status.into();
    state.update(progress, status.clone());
    let _ = tx.send((progress, status));
}

// mock outputs helper removed; use runner-reconstruction-legacy-noop for smoke tests.

async fn upload_manifest_artifact(
    sink: &dyn ArtifactSink,
    rel_path: &str,
    file_path: &Path,
    name_suffix: &str,
    existing_id: Option<&str>,
) -> anyhow::Result<()> {
    use compute_runner_api::runner::{DomainArtifactContent, DomainArtifactRequest};
    if !file_path.exists() {
        return Ok(());
    }
    sink.put_domain_artifact(DomainArtifactRequest {
        rel_path,
        name: &format!("refined_manifest_{}", name_suffix),
        data_type: "refined_manifest_json",
        existing_id,
        content: DomainArtifactContent::File(file_path),
    })
    .await?;
    Ok(())
}

fn build_python_args(
    config: &RunnerConfig,
    job_ctx: &JobContext,
    workspace: &Workspace,
    mode: &str,
    scan_names: &[String],
) -> Vec<String> {
    let mut args = config.python_args.clone();
    args.push("--mode".to_string());
    args.push(mode.to_string());
    args.push("--job_root_path".to_string());
    args.push(workspace.root().display().to_string());
    args.push("--output_path".to_string());
    args.push(workspace.root().join("refined").display().to_string());
    args.push("--domain_id".to_string());
    args.push(job_ctx.metadata.domain_id.clone());
    args.push("--job_id".to_string());
    args.push(job_ctx.metadata.name.clone());
    args.push("--local_refinement_workers".to_string());
    args.push(config.cpu_workers.to_string());

    if !scan_names.is_empty() {
        args.push("--scans".to_string());
        args.extend(scan_names.iter().cloned());
    }

    args
}

fn collect_scan_names(datasets_path: &Path) -> Result<Vec<String>> {
    let mut scans = Vec::new();
    let entries = match std::fs::read_dir(datasets_path) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(scans),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("read_dir datasets path {}", datasets_path.display()))
        }
    };

    for entry in entries.flatten() {
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        if let Some(name_str) = name.to_str() {
            if !name_str.is_empty() {
                scans.push(name_str.to_string());
            }
        }
    }

    scans.sort();
    scans.dedup();
    Ok(scans)
}

fn scan_completion(workspace: &Workspace, scan_names: &[String]) -> Result<(usize, usize)> {
    let mut done = 0usize;
    for scan in scan_names {
        let sfm = workspace.refined_local().join(scan).join("sfm");
        if !sfm.exists() {
            continue;
        }
        // Require the core files like the Go server did.
        let required = ["images.bin", "cameras.bin", "points3D.bin", "portals.csv"];
        let mut complete = true;
        for f in required.iter() {
            if !sfm.join(f).exists() {
                complete = false;
                break;
            }
        }
        if complete {
            done += 1;
        }
    }
    Ok((done, scan_names.len()))
}

/// Extract the last non-empty segment from a CID/URL-like string.
/// Examples:
/// - "https://domain/.../data/abc123" -> "abc123"
/// - "abc123" -> "abc123"
/// - "https://domain/.../data/" -> "data" (best-effort)
fn extract_last_segment(input: &str) -> String {
    let trimmed = input.trim_end_matches('/');
    match trimmed.rsplit('/').next() {
        Some(seg) if !seg.is_empty() => seg.to_string(),
        _ => input.to_string(),
    }
}

// --- Scheduler-parity helpers: staging + conditional local/global orchestration ---

#[allow(clippy::too_many_arguments)]
async fn execute_python_stage(
    config: &RunnerConfig,
    job_ctx: &JobContext,
    workspace: &Workspace,
    ctx: &TaskCtx<'_>,
    cancel_token: &CancellationToken,
    progress_rx: &mut tokio::sync::mpsc::UnboundedReceiver<(i32, String)>,
    refined_uploader: &mut refined::RefinedUploader,
    state: &ManifestState,
    progress_tx: &UnboundedSender<(i32, String)>,
    name_suffix: &str,
    override_manifest_id: Option<&str>,
    mode: &'static str,
    progress_scans: &[String],
    hide: &[String],
    baseline_done: i32,
    _initial_pending: i32,
    total_scans: i32,
    has_global: bool,
) -> Result<()> {
    // Temporarily hide selected dataset folders to prevent Python local from processing them.
    let mut hidden = false;
    if !hide.is_empty() {
        hide_scans(workspace, hide)?;
        hidden = true;
    }

    let python_args = build_python_args(config, job_ctx, workspace, mode, progress_scans);
    info!(
        domain_id = %job_ctx.metadata.domain_id,
        job_name = %job_ctx.metadata.name,
        python_bin = %config.python_bin.display(),
        python_script = %config.python_script.display(),
        workspace = %workspace.root().display(),
        mode = mode,
        args = ?python_args,
        "launching legacy python stage"
    );

    let python_bin = config.python_bin.clone();
    let python_script = config.python_script.clone();
    let cancel = cancel_token.clone();
    let mut python_future: Pin<
        Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send>,
    > = Box::pin(async move {
        python::run_script(&python_bin, &python_script, &python_args, &cancel).await
    });
    let mut python_result: Option<Result<(), anyhow::Error>> = None;
    let mut refined_interval = interval(Duration::from_secs(LEGACY_PROGRESS_INTERVAL_SECS));
    refined_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            res = &mut python_future, if python_result.is_none() => {
                python_result = Some(res);
            }
            Some((progress, status)) = progress_rx.recv() => {
                let payload = json!({"progress": progress, "status": status});
                let _ = ctx.ctrl.progress(payload).await;
                let _ = upload_manifest_artifact(
                    ctx.output,
                    "job_manifest.json",
                    workspace.job_manifest_path(),
                    name_suffix,
                    override_manifest_id,
                ).await;
            }
            _ = refined_interval.tick(), if python_result.is_none() => {
                if let Err(err) = refined_uploader
                    .process(
                        workspace,
                        ctx.output,
                        // Upload local zips only during stages that include local refinement
                        mode != "global_refinement",
                    )
                    .await
                {
                    cancel_token.cancel();
                    if hidden { let _ = restore_scans(workspace, hide); }
                    // Best-effort: write and upload a failed manifest before returning
                    let _ = manifest::write_failed_manifest_python(
                        workspace.job_manifest_path(),
                        workspace.root(),
                        &config.python_bin,
                        &format!("refined outputs upload error: {}", err),
                    ).await;
                    let _ = upload_manifest_artifact(
                        ctx.output,
                        "job_manifest.json",
                        workspace.job_manifest_path(),
                        name_suffix,
                        override_manifest_id,
                    ).await;
                    return Err(err);
                }

                // Revised progress: (baseline + local_done) / (total_scans + (has_global?1:0))
                if mode != "global_refinement" {
                    let (done_local, _total_local) = scan_completion(workspace, progress_scans)?;
                    let denom = (total_scans + if has_global { 1 } else { 0 }).max(1);
                    let done_total = (baseline_done + done_local as i32).clamp(0, total_scans);
                    let pct = (((100.0 * (done_total as f64) / (denom as f64)).round()) as i32).clamp(0, 100);
                    let status_text = format!(
                        "Tasks {}/{} — refining local",
                        done_total,
                        denom
                    );
                    update_progress(state, progress_tx, pct, status_text);
                }
            }
            cancelled_now = ctx.ctrl.is_cancelled() => {
                if cancelled_now {
                    cancel_token.cancel();
                }
            }
        }

        if python_result.is_some() {
            break;
        }
    }

    if hidden {
        let _ = restore_scans(workspace, hide);
    }

    let python_result = python_result.unwrap_or(Ok(()));
    if let Err(err) = python_result {
        cancel_token.cancel();
        // Best-effort: write and upload a failed manifest before returning
        let _ = manifest::write_failed_manifest_python(
            workspace.job_manifest_path(),
            workspace.root(),
            &config.python_bin,
            &format!("python stage error: {}", err),
        )
        .await;
        let _ = upload_manifest_artifact(
            ctx.output,
            "job_manifest.json",
            workspace.job_manifest_path(),
            name_suffix,
            override_manifest_id,
        )
        .await;
        return Err(err);
    }
    Ok(())
}

async fn stage_existing_refined_outputs(workspace: &Workspace) -> Result<()> {
    let datasets_path = workspace.datasets();
    let entries = match std::fs::read_dir(datasets_path) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(err).with_context(|| format!("read_dir {}", datasets_path.display()))
        }
    };

    for entry in entries.flatten() {
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let scan = entry.file_name().to_string_lossy().to_string();
        let zip_path = entry.path().join("RefinedScan.zip");
        if !zip_path.exists() {
            continue;
        }
        let unzip_root = workspace.refined_local().join(&scan).join("sfm");
        let bytes = tokio::fs::read(&zip_path)
            .await
            .with_context(|| format!("read refined zip {}", zip_path.display()))?;
        let _ = unzip_refined_scan(bytes, &unzip_root)
            .await
            .with_context(|| {
                format!(
                    "unzip refined scan {} into {}",
                    zip_path.display(),
                    unzip_root.display()
                )
            })?;
    }
    Ok(())
}

fn classify_scans(
    workspace: &Workspace,
    scan_names: &[String],
) -> Result<(Vec<String>, Vec<String>)> {
    let mut pending = Vec::new();
    let mut refined = Vec::new();
    for scan in scan_names {
        if is_refined_complete(workspace, scan) {
            refined.push(scan.clone());
        } else {
            pending.push(scan.clone());
        }
    }
    Ok((pending, refined))
}

fn is_refined_complete(workspace: &Workspace, scan: &str) -> bool {
    let sfm = workspace.refined_local().join(scan).join("sfm");
    if !sfm.exists() {
        return false;
    }
    let required = ["images.bin", "cameras.bin", "points3D.bin", "portals.csv"];
    required.iter().all(|f| sfm.join(f).exists())
}

fn hide_scans(workspace: &Workspace, scans: &[String]) -> Result<()> {
    if scans.is_empty() {
        return Ok(());
    }
    let hidden_root = workspace.root().join("datasets_hidden");
    std::fs::create_dir_all(&hidden_root)
        .with_context(|| format!("create {}", hidden_root.display()))?;
    for scan in scans {
        let src = workspace.datasets().join(scan);
        let dst = hidden_root.join(scan);
        if src.exists() {
            std::fs::rename(&src, &dst)
                .with_context(|| format!("hide dataset {} -> {}", src.display(), dst.display()))?;
        }
    }
    Ok(())
}

fn restore_scans(workspace: &Workspace, scans: &[String]) -> Result<()> {
    let hidden_root = workspace.root().join("datasets_hidden");
    for scan in scans {
        let src = hidden_root.join(scan);
        let dst = workspace.datasets().join(scan);
        if src.exists() {
            std::fs::rename(&src, &dst).with_context(|| {
                format!("restore dataset {} -> {}", src.display(), dst.display())
            })?;
        }
    }
    Ok(())
}

async fn stage_from_domain(
    workspace: &Workspace,
    job_ctx: &JobContext,
    ctx: &TaskCtx<'_>,
) -> Result<()> {
    let domain_url = job_ctx
        .metadata
        .domain_server_url
        .trim()
        .trim_end_matches('/')
        .to_string();
    let domain_id = job_ctx.metadata.domain_id.trim().to_string();
    if domain_url.is_empty() || domain_id.is_empty() {
        return Ok(());
    }

    let client_id = get_client_id();
    let token = ctx.access_token.get();

    // Scheduler-style skip: for each local dataset scan that isn't already refined locally,
    // check if a refined zip exists in Domain by name (refined_scan_{scan}). If present,
    // download and unzip so local refinement can be skipped for that scan.
    let local_scans = collect_scan_names(workspace.datasets())?;
    for scan in local_scans {
        if is_refined_complete(workspace, &scan) {
            continue;
        }

        let refined_name = format!("refined_scan_{}", scan);
        match crate::domain_lookup::resolve_domain_data_id(
            &domain_url,
            &client_id,
            &token,
            &domain_id,
            &refined_name,
            "refined_scan_zip",
        )
        .await
        {
            Ok(Some(existing_id)) => {
                // Ensure dataset directory exists
                let ds_dir = workspace.datasets().join(&scan);
                if !ds_dir.exists() {
                    let _ = std::fs::create_dir_all(&ds_dir);
                }
                if let Ok(bytes) =
                    download_by_id(&domain_url, &client_id, &token, &domain_id, &existing_id).await
                {
                    let unzip_root = workspace.refined_local().join(&scan).join("sfm");
                    let _ = unzip_refined_scan(bytes, &unzip_root).await?;
                    tracing::info!(
                        target = "runner_reconstruction_legacy",
                        scan = %scan,
                        refined_id = %existing_id,
                        "staged refined zip from domain; will skip local refinement for this scan"
                    );
                }
            }
            Ok(None) => {}
            Err(err) => {
                tracing::info!(
                    target = "runner_reconstruction_legacy",
                    scan = %scan,
                    error = %err,
                    "refined zip lookup by name failed; continuing without domain-side skip"
                );
            }
        }
    }

    Ok(())
}

fn get_client_id() -> String {
    if let Ok(id) = std::env::var("CLIENT_ID") {
        if !id.trim().is_empty() {
            return id;
        }
    }
    format!("posemesh-compute-node/{}", Uuid::new_v4())
}

#[cfg(test)]
mod conditional_tests {
    use super::*;
    use std::io::Write as _;
    use zip::{write::FileOptions, CompressionMethod, ZipWriter};

    fn make_zip_bytes(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        {
            let mut writer = ZipWriter::new(&mut cursor);
            let opts = FileOptions::default().compression_method(CompressionMethod::Stored);
            for (name, data) in entries.iter() {
                writer.start_file(*name, opts).unwrap();
                writer.write_all(data).unwrap();
            }
            writer.finish().unwrap();
        }
        cursor.into_inner()
    }

    #[tokio::test]
    async fn staging_and_classification_respects_preexisting_refined_zip() {
        let ws = Workspace::create(None, "dom", Some("job"), "task").unwrap();

        // Create dataset with a pre-existing refined zip
        let scan = "scan_old".to_string();
        let ds_dir = ws.datasets().join(&scan);
        std::fs::create_dir_all(&ds_dir).unwrap();

        let zip_bytes = make_zip_bytes(&[
            ("images.bin", b"a" as &[u8]),
            ("cameras.bin", b"b"),
            ("points3D.bin", b"c"),
            ("portals.csv", b"d"),
            ("notes.txt", b"e"),
        ]);
        std::fs::write(ds_dir.join("RefinedScan.zip"), &zip_bytes).unwrap();

        // Stage refined zip into refined/local/<scan>/sfm
        stage_existing_refined_outputs(&ws).await.unwrap();

        for req in ["images.bin", "cameras.bin", "points3D.bin", "portals.csv"].iter() {
            assert!(ws
                .refined_local()
                .join(&scan)
                .join("sfm")
                .join(req)
                .exists());
        }

        // Classification should mark this scan as refined
        let all = collect_scan_names(ws.datasets()).unwrap();
        let (pending, refined) = classify_scans(&ws, &all).unwrap();
        assert!(refined.contains(&scan));
        assert!(!pending.contains(&scan));
    }

    #[test]
    fn hide_and_restore_moves_dataset_folders_temporarily() {
        let ws = Workspace::create(None, "dom", Some("job"), "task").unwrap();
        let scans = vec!["scan_x".to_string(), "scan_y".to_string()];
        for s in scans.iter() {
            std::fs::create_dir_all(ws.datasets().join(s)).unwrap();
            assert!(ws.datasets().join(s).exists());
        }

        hide_scans(&ws, &scans).unwrap();
        for s in scans.iter() {
            assert!(!ws.datasets().join(s).exists());
            assert!(ws.root().join("datasets_hidden").join(s).exists());
        }

        restore_scans(&ws, &scans).unwrap();
        for s in scans.iter() {
            assert!(ws.datasets().join(s).exists());
            assert!(!ws.root().join("datasets_hidden").join(s).exists());
        }
    }
}
