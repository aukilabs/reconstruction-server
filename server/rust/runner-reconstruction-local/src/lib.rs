//! runner-reconstruction-local: minimal runner for local refinement.

use std::{
    env,
    path::{Path, PathBuf},
    pin::Pin,
};

use anyhow::{Context, Result};
use chrono::Utc;
use compute_runner_api::runner::{DomainArtifactContent, DomainArtifactRequest};
use compute_runner_api::{ArtifactSink, Runner, TaskCtx};
use serde::Serialize;
use serde_json::json;
use tokio::fs;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

mod input;
mod python;
mod refined;
mod workspace;

/// Public crate identifier used by workspace smoke tests.
pub const CRATE_NAME: &str = "runner-reconstruction-local";

/// Capability handled by this runner (local refinement).
pub const CAPABILITY: &str = "/reconstruction/local-refinement/v1";

/// Convenience slice for wiring all supported capabilities.
pub const CAPABILITIES: [&str; 1] = [CAPABILITY];

/// Scaffold runner for local refinement.
pub struct RunnerReconstructionLocal {
    config: RunnerConfig,
    capability: &'static str,
}

impl RunnerReconstructionLocal {
    /// Create a new local refinement runner.
    pub fn new() -> Self {
        Self::with_capability(CAPABILITY, load_config())
    }

    pub fn with_capability(capability: &'static str, config: RunnerConfig) -> Self {
        Self { config, capability }
    }

    pub fn for_all_capabilities() -> Vec<Self> {
        let config = load_config();
        CAPABILITIES
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

impl Default for RunnerReconstructionLocal {
    fn default() -> Self {
        Self::new()
    }
}

fn load_config() -> RunnerConfig {
    RunnerConfig::from_env().unwrap_or_else(|err| {
        warn!(error = %err, "failed to read local runner config; using defaults");
        RunnerConfig::default()
    })
}

#[async_trait::async_trait]
impl Runner for RunnerReconstructionLocal {
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

        if ctx.ctrl.is_cancelled().await {
            anyhow::bail!("task cancelled before execution");
        }

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

        input::materialize_datasets(&ctx, &workspace).await?;

        let scan_names = collect_scan_names(workspace.datasets())?;
        let python_args = build_python_args(&self.config, &job_ctx, &workspace, &scan_names);

        let cancel_token = CancellationToken::new();
        let python_bin = self.config.python_bin.clone();
        let python_script = self.config.python_script.clone();
        let python_args_clone = python_args.clone();
        let cancel = cancel_token.clone();
        let job_root = workspace.root().to_path_buf();
        let mut python_future: Pin<
            Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send>,
        > = Box::pin(async move {
            python::run_script(
                &python_bin,
                &python_script,
                &python_args_clone,
                &cancel,
                Some(&job_root),
            )
            .await
        });

        let python_result = loop {
            tokio::select! {
                res = &mut python_future => break res,
                cancelled = ctx.ctrl.is_cancelled() => {
                    if cancelled {
                        cancel_token.cancel();
                    }
                }
            }
        };
        if let Err(err) = upload_task_log(ctx.output, &workspace, &task_id).await {
            warn!(error = %err, task_id = %task_id, "failed to upload task log");
        }
        python_result?;

        let mut refined_uploader = refined::RefinedUploader::new();
        refined_uploader
            .process(&workspace, ctx.output, true)
            .await?;

        ctx.ctrl
            .progress(json!({"progress": 100, "status": "succeeded"}))
            .await?;

        Ok(())
    }
}

async fn upload_task_log(
    sink: &dyn ArtifactSink,
    workspace: &workspace::Workspace,
    task_id: &str,
) -> Result<()> {
    let log_path = workspace.root().join("log.txt");
    if !log_path.exists() {
        warn!(task_id = %task_id, path = %log_path.display(), "task log missing");
        return Ok(());
    }

    let rel_path = format!("logs/{task_id}.txt");
    let name = format!("task_log_{task_id}");
    sink.put_domain_artifact(DomainArtifactRequest {
        rel_path: &rel_path,
        name: &name,
        data_type: "task_log_txt",
        existing_id: None,
        content: DomainArtifactContent::File(&log_path),
    })
    .await
    .with_context(|| format!("upload task log {}", log_path.display()))?;
    Ok(())
}

#[derive(Serialize)]
struct JobMetadataRecord {
    id: String,
    name: String,
    domain_id: String,
    processing_type: String,
    created_at: String,
    domain_server_url: String,
    reconstruction_server_url: Option<String>,
    data_ids: Vec<String>,
}

struct JobContext {
    metadata: JobMetadataRecord,
}

impl JobContext {
    fn from_lease(lease: &compute_runner_api::LeaseEnvelope) -> Result<Self> {
        let job_id = lease
            .task
            .job_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| lease.task.id.to_string());
        let job_name = format!("job_{}", job_id);

        let domain_server_url = lease
            .domain_server_url
            .as_ref()
            .map(|url| url.to_string())
            .unwrap_or_default();
        let domain_server_url = domain_server_url.trim_end_matches('/').to_string();

        let reconstruction_server_url = env::var("NODE_URL").ok();

        let data_ids: Vec<String> = lease
            .task
            .inputs_cids
            .iter()
            .map(|cid| extract_last_segment(cid))
            .collect();

        let metadata = JobMetadataRecord {
            id: job_id.clone(),
            name: job_name,
            domain_id: lease.domain_id.map(|id| id.to_string()).unwrap_or_default(),
            processing_type: "local_refinement".to_string(),
            created_at: Utc::now().to_rfc3339(),
            domain_server_url,
            reconstruction_server_url,
            data_ids,
        };

        Ok(Self { metadata })
    }

    async fn persist_metadata(&self, path: &Path) -> Result<()> {
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
}

/// Configuration for the local reconstruction runner.
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
    pub const ENV_WORKSPACE_ROOT: &'static str = "LOCAL_RUNNER_WORKSPACE_ROOT";
    pub const ENV_PYTHON_BIN: &'static str = "LOCAL_RUNNER_PYTHON_BIN";
    pub const ENV_PYTHON_SCRIPT: &'static str = "LOCAL_RUNNER_PYTHON_SCRIPT";
    pub const ENV_PYTHON_ARGS: &'static str = "LOCAL_RUNNER_PYTHON_ARGS";
    pub const ENV_CPU_WORKERS: &'static str = "LOCAL_RUNNER_CPU_WORKERS";

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

fn build_python_args(
    config: &RunnerConfig,
    job_ctx: &JobContext,
    workspace: &workspace::Workspace,
    scan_names: &[String],
) -> Vec<String> {
    let mut args = config.python_args.clone();
    args.push("--mode".to_string());
    args.push("local_refinement".to_string());
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

/// Extract the last non-empty segment from a CID/URL-like string.
fn extract_last_segment(input: &str) -> String {
    let trimmed = input.trim_end_matches('/');
    match trimmed.rsplit('/').next() {
        Some(seg) if !seg.is_empty() => seg.to_string(),
        _ => input.to_string(),
    }
}
