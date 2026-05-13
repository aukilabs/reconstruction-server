//! runner-reconstruction-localize: long-running pose tracking server (SLAM-compatible HTTP API).

mod input;
mod python;
mod strategy;
mod workspace;

use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use chrono::Utc;
use compute_runner_api::{Runner, TaskCtx};
use serde::Serialize;
use serde_json::json;
use tokio::fs;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub const CRATE_NAME: &str = "runner-reconstruction-localize";
pub const CAPABILITY: &str = "/reconstruction/localize-images/v1";
pub const CAPABILITIES: [&str; 1] = [CAPABILITY];

pub struct RunnerReconstructionLocalize {
    config: RunnerConfig,
    capability: &'static str,
}

impl RunnerReconstructionLocalize {
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

impl Default for RunnerReconstructionLocalize {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Runner for RunnerReconstructionLocalize {
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
            bail!("task cancelled before execution");
        }

        let workspace = self.create_workspace(&domain_id, job_id.as_deref(), &task_id)?;
        let workspace_root = workspace.root().to_path_buf();
        struct Cleanup(Option<PathBuf>);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                if let Some(p) = self.0.take() {
                    let _ = std::fs::remove_dir_all(&p);
                }
            }
        }
        let _cleanup = Cleanup(Some(workspace_root.clone()));
        let job_ctx = JobContext::from_lease(lease)?;
        job_ctx
            .persist_metadata(workspace.job_metadata_path())
            .await?;

        info!(
            capability = self.capability,
            domain_id = %job_ctx.metadata.domain_id,
            task_id = %task_id,
            workspace = %workspace.root().display(),
            "localize-images workspace prepared"
        );

        let _ = ctx
            .ctrl
            .progress(json!({"pct": 10, "stage": "inputs", "status": "resolving"}))
            .await;

        let recon_root = input::resolve_reconstruction_root(&ctx, &workspace).await?;

        let _ = ctx
            .ctrl
            .progress(json!({"pct": 20, "stage": "pose_server", "status": "starting"}))
            .await;

        let script = default_pose_server_script();
        let port = env::var("LOCALIZE_POSE_SERVER_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(8080);

        let mut args = vec![
            "--reconstruction_root".to_string(),
            recon_root.display().to_string(),
            "--job_root".to_string(),
            workspace.root().display().to_string(),
            "--host".to_string(),
            env::var("LOCALIZE_POSE_SERVER_HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            "--port".to_string(),
            port.to_string(),
            "--max-queue".to_string(),
            env::var("LOCALIZE_POSE_SERVER_MAX_QUEUE")
                .unwrap_or_else(|_| "10".into()),
        ];
        if let Ok(h5) = env::var("LOCALIZE_FEATURES_H5_PATH") {
            if !h5.trim().is_empty() {
                args.push("--features_h5".into());
                args.push(h5);
            }
        }
        args.push("--voxel_size".to_string());
        args.push(
            env::var("LOCALIZE_VOXEL_SIZE").unwrap_or_else(|_| "0.2".into()),
        );

        let cancel = CancellationToken::new();
        let python_bin = self.config.python_bin.clone();
        let script_clone = script.clone();
        let args_clone = args.clone();
        let job_root = workspace.root().to_path_buf();
        let cancel_c = cancel.clone();

        let mut fut: std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> =
            Box::pin(async move {
                python::run_script(
                    &python_bin,
                    &script_clone,
                    &args_clone,
                    &cancel_c,
                    Some(&job_root),
                )
                .await
            });

        loop {
            tokio::select! {
                res = &mut fut => {
                    res?;
                    break;
                }
                cancelled = ctx.ctrl.is_cancelled() => {
                    if cancelled {
                        cancel.cancel();
                    }
                }
            }
        }

        let _ = ctx
            .ctrl
            .progress(json!({"pct": 100, "status": "succeeded"}))
            .await;

        Ok(())
    }
}

fn default_pose_server_script() -> PathBuf {
    if let Ok(p) = env::var("POSE_TRACKING_SERVER_SCRIPT") {
        let pb = PathBuf::from(p.trim());
        if !pb.as_os_str().is_empty() {
            return pb;
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .join("pose_tracking_server.py")
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
            processing_type: "localize_images".to_string(),
            created_at: Utc::now().to_rfc3339(),
            domain_server_url,
            reconstruction_server_url: None,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerConfig {
    pub workspace_root: Option<PathBuf>,
    pub python_bin: PathBuf,
}

impl RunnerConfig {
    pub const ENV_WORKSPACE_ROOT: &'static str = "LOCALIZE_RUNNER_WORKSPACE_ROOT";
    pub const ENV_PYTHON_BIN: &'static str = "LOCALIZE_RUNNER_PYTHON_BIN";
    pub const DEFAULT_PYTHON_BIN: &'static str = "python3";

    pub fn from_env() -> Result<Self> {
        let workspace_root = match env::var(Self::ENV_WORKSPACE_ROOT) {
            Ok(v) if !v.trim().is_empty() => Some(PathBuf::from(v)),
            _ => None,
        };
        let python_bin = env::var(Self::ENV_PYTHON_BIN)
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(Self::DEFAULT_PYTHON_BIN));
        Ok(Self {
            workspace_root,
            python_bin,
        })
    }
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            workspace_root: None,
            python_bin: PathBuf::from(Self::DEFAULT_PYTHON_BIN),
        }
    }
}

fn load_config() -> RunnerConfig {
    RunnerConfig::from_env().unwrap_or_else(|err| {
        warn!(error = %err, "localize runner config; using defaults");
        RunnerConfig::default()
    })
}

fn extract_last_segment(input: &str) -> String {
    let trimmed = input.trim_end_matches('/');
    match trimmed.rsplit('/').next() {
        Some(seg) if !seg.is_empty() => seg.to_string(),
        _ => input.to_string(),
    }
}
