//! runner-reconstruction-legacy: skeleton runner for legacy reconstruction.

use std::{env, path::PathBuf, pin::Pin, sync::Arc, time::Duration};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use compute_runner_api::{ArtifactSink, Runner, TaskCtx};
use serde::Serialize;
use serde_json::json;
use tokio::{
    fs,
    sync::mpsc::{unbounded_channel, UnboundedSender},
    time::{interval, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

pub mod input;
pub mod manifest;
pub mod output;
pub mod python;
pub mod refined;
pub mod summary;
pub mod workspace;

use manifest::{ManifestState, ProgressListener};
use workspace::Workspace;

#[derive(Serialize)]
struct JobMetadataRecord {
    created_at: String,
    job_id: String,
    task_id: String,
    job_name: String,
    domain_id: String,
    capability: String,
    processing_type: String,
    inputs_cids: Vec<String>,
    outputs_prefix: Option<String>,
    domain_server_url: String,
    skip_manifest_upload: bool,
    override_job_name: Option<String>,
    override_manifest_id: Option<String>,
}

struct JobContext {
    metadata: JobMetadataRecord,
    skip_manifest_upload: bool,
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
        let outputs_prefix = lease.task.outputs_prefix.clone();
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

        let metadata = JobMetadataRecord {
            created_at: Utc::now().to_rfc3339(),
            job_id: job_id.clone(),
            task_id: lease.task.id.to_string(),
            job_name,
            domain_id: lease.domain_id.map(|id| id.to_string()).unwrap_or_default(),
            capability: lease.task.capability.clone(),
            processing_type,
            inputs_cids,
            outputs_prefix,
            domain_server_url,
            skip_manifest_upload,
            override_job_name,
            override_manifest_id,
        };

        Ok(Self {
            metadata,
            skip_manifest_upload,
        })
    }

    fn skip_manifest_upload(&self) -> bool {
        self.skip_manifest_upload
    }

    fn processing_type(&self) -> &str {
        &self.metadata.processing_type
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

async fn upload_manifest_if_needed(
    sink: &dyn ArtifactSink,
    workspace: &Workspace,
    skip_manifest_upload: bool,
) -> anyhow::Result<()> {
    if skip_manifest_upload {
        return Ok(());
    }
    let path = workspace.job_manifest_path();
    if !path.exists() {
        return Ok(());
    }
    let bytes = fs::read(path)
        .await
        .with_context(|| format!("read manifest {}", path.display()))?;
    sink.put_bytes("job_manifest.json", &bytes).await?;
    Ok(())
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
    /// If true, execute in mock mode (skip heavy pipeline).
    pub mock_mode: bool,
}

impl RunnerConfig {
    pub const ENV_WORKSPACE_ROOT: &'static str = "LEGACY_RUNNER_WORKSPACE_ROOT";
    pub const ENV_PYTHON_BIN: &'static str = "LEGACY_RUNNER_PYTHON_BIN";
    pub const ENV_PYTHON_SCRIPT: &'static str = "LEGACY_RUNNER_PYTHON_SCRIPT";
    pub const ENV_PYTHON_ARGS: &'static str = "LEGACY_RUNNER_PYTHON_ARGS";
    pub const ENV_CPU_WORKERS: &'static str = "LEGACY_RUNNER_CPU_WORKERS";
    pub const ENV_MOCK_MODE: &'static str = "LEGACY_RUNNER_MOCK";

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

        let mock_mode = env::var(Self::ENV_MOCK_MODE)
            .ok()
            .map(|v| parse_bool(&v))
            .unwrap_or(false);

        Ok(Self {
            workspace_root,
            python_bin,
            python_script,
            python_args,
            cpu_workers,
            mock_mode,
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
            mock_mode: false,
        }
    }
}

/// Parse a string into a boolean. Accepts common truthy values.
fn parse_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
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

        let job_ctx = JobContext::from_lease(lease)?;
        job_ctx
            .persist_metadata(workspace.job_metadata_path())
            .await?;

        let (progress_tx, mut progress_rx) = unbounded_channel::<(i32, String)>();
        let state = ManifestState::default();
        update_progress(&state, &progress_tx, 0, "initializing");

        manifest::write_processing_manifest(workspace.job_manifest_path(), 0, "initializing")
            .await?;
        upload_manifest_if_needed(ctx.output, &workspace, job_ctx.skip_manifest_upload()).await?;

        let materialized = input::materialize_datasets(&ctx, &workspace).await?;
        update_progress(
            &state,
            &progress_tx,
            20,
            format!("materialized {} inputs", materialized.len()),
        );

        if self.config.mock_mode {
            create_mock_outputs(&workspace).context("create mock outputs")?;
        }

        let cancel_token = CancellationToken::new();
        let manifest_task = manifest::spawn_processing_writer(
            workspace.job_manifest_path().to_path_buf(),
            Duration::from_secs(2),
            state.clone(),
            Arc::new(ProgressForwarder {
                tx: progress_tx.clone(),
            }),
            cancel_token.clone(),
        );

        let mut refined_uploader = refined::RefinedUploader::new();
        let mut refined_interval = interval(Duration::from_secs(5));
        refined_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut python_future: Pin<
            Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send>,
        > = if self.config.mock_mode {
            Box::pin(async { Ok(()) })
        } else {
            let args = self.config.python_args.clone();
            let python_bin = self.config.python_bin.clone();
            let python_script = self.config.python_script.clone();
            let log_path = workspace.root().join("python.log");
            let cancel = cancel_token.clone();
            Box::pin(async move {
                python::run_script(&python_bin, &python_script, &args, &log_path, &cancel).await
            })
        };
        let mut python_result: Option<Result<(), anyhow::Error>> = None;
        let mut cancelled = false;

        loop {
            tokio::select! {
                res = &mut python_future, if python_result.is_none() => {
                    python_result = Some(res);
                }
                Some((progress, status)) = progress_rx.recv() => {
                    let payload = json!({"progress": progress, "status": status});
                    let _ = ctx.ctrl.progress(payload).await;
                }
                _ = refined_interval.tick(), if python_result.is_none() => {
                    if let Err(err) = refined_uploader.process(&workspace, ctx.output).await {
                        cancel_token.cancel();
                        manifest_task.stop().await;
                        return Err(err);
                    }
                }
                cancelled_now = ctx.ctrl.is_cancelled(), if !cancelled => {
                    if cancelled_now {
                        cancelled = true;
                        cancel_token.cancel();
                    }
                }
            }

            if python_result.is_some() {
                break;
            }
        }

        progress_rx.close();
        cancel_token.cancel();
        manifest_task.stop().await;

        if cancelled {
            manifest::write_failed_manifest(workspace.job_manifest_path(), "task cancelled")
                .await?;
            upload_manifest_if_needed(ctx.output, &workspace, job_ctx.skip_manifest_upload())
                .await?;
            return Err(anyhow!("task cancelled"));
        }

        let python_result = python_result.unwrap_or(Ok(()));
        if let Err(err) = python_result {
            manifest::write_failed_manifest(workspace.job_manifest_path(), "python failed").await?;
            upload_manifest_if_needed(ctx.output, &workspace, job_ctx.skip_manifest_upload())
                .await?;
            return Err(err);
        }

        refined_uploader
            .process(&workspace, ctx.output)
            .await
            .context("final refined upload pass")?;

        if job_ctx.should_generate_scan_summary() {
            if let Err(err) = summary::write_scan_data_summary(
                workspace.datasets(),
                workspace.scan_data_summary_path(),
            )
            .await
            {
                eprintln!("failed to write scan data summary: {err}");
            }
        }

        update_progress(&state, &progress_tx, 90, "uploading outputs");
        drop(progress_tx);

        output::upload_final_outputs(&workspace, ctx.output)
            .await
            .context("upload final outputs")?;
        manifest::write_processing_manifest(workspace.job_manifest_path(), 100, "succeeded")
            .await?;
        upload_manifest_if_needed(ctx.output, &workspace, job_ctx.skip_manifest_upload()).await?;

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

fn create_mock_outputs(workspace: &Workspace) -> Result<()> {
    let global_root = workspace.root().join("refined/global");
    let topology_root = global_root.join("topology");
    std::fs::create_dir_all(&topology_root)
        .with_context(|| format!("create directory {}", topology_root.display()))?;
    std::fs::write(
        global_root.join("refined_manifest.json"),
        b"{\"mock\":true}\n",
    )
    .with_context(|| "write mock refined_manifest".to_string())?;
    std::fs::write(global_root.join("RefinedPointCloudReduced.ply"), b"ply\n")
        .with_context(|| "write mock reduced pointcloud".to_string())?;
    std::fs::write(global_root.join("RefinedPointCloud.ply.drc"), b"drc")
        .with_context(|| "write mock draco pointcloud".to_string())?;
    std::fs::write(topology_root.join("topology_downsampled_0.111.obj"), b"obj")
        .with_context(|| "write mock topology obj".to_string())?;

    std::fs::write(
        workspace.root().join("result.json"),
        b"{\"status\":\"mock\"}\n",
    )
    .with_context(|| "write mock result".to_string())?;
    std::fs::write(workspace.root().join("outputs_index.json"), b"{}\n")
        .with_context(|| "write mock outputs index".to_string())?;
    let sfm_dir = workspace.refined_local().join("mock_scan").join("sfm");
    std::fs::create_dir_all(&sfm_dir)
        .with_context(|| format!("create directory {}", sfm_dir.display()))?;
    std::fs::write(sfm_dir.join("Manifest.json"), b"{}\n")
        .with_context(|| "write mock local manifest".to_string())?;
    std::fs::write(sfm_dir.join("points.bin"), b"123")
        .with_context(|| "write mock points".to_string())?;

    Ok(())
}
