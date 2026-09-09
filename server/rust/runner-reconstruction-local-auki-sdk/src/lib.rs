//! runner-reconstruction-local-auki-sdk: local refinement for Auki SDK capture sessions.
//!
//! Same contract as `/reconstruction/local-refinement/v1` on both ends -- one workspace
//! per job, one Python pipeline invocation, refined `sfm/` folders zipped and uploaded as
//! `refined_scan_<scan>` artifacts -- but the input is a small Domain reference to one
//! Robot-hosted capture ZIP. The ZIP is fetched through the authenticated P2P dataset
//! handle and expanded here rather than materialized from Domain Server; the pipeline
//! entrypoint remains `local_auki_main.py` instead of `main.py --mode local_refinement`.

use std::{
    env,
    path::{Path, PathBuf},
    pin::Pin,
};

use anyhow::{Context, Result};
use chrono::Utc;
use compute_runner_api::runner::{DomainArtifactContent, DomainArtifactRequest};
use compute_runner_api::{ArtifactSink, Runner, TaskCtx};
use posemesh_compute_node::engine::AukiProtocolsHandle;
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
pub const CRATE_NAME: &str = "runner-reconstruction-local-auki-sdk";

/// Capability handled by this runner (local refinement of an Auki SDK capture session).
pub const CAPABILITY: &str = "/reconstruction/local-refinement-auki-sdk/v0";

/// Convenience slice for wiring all supported capabilities.
pub const CAPABILITIES: [&str; 1] = [CAPABILITY];

/// Runner for local refinement of Auki SDK capture sessions.
pub struct RunnerReconstructionLocalAukiSdk {
    config: RunnerConfig,
    capability: &'static str,
    /// The authenticated peer surface this runner fetches the session ZIP over.
    ///
    /// Held, not resolved: on Compute the context behind it belongs to one
    /// task's peer, so it is looked up at the point of use and never cached.
    protocols: AukiProtocolsHandle,
}

impl RunnerReconstructionLocalAukiSdk {
    /// Create a new Auki SDK local refinement runner.
    pub fn new(protocols: AukiProtocolsHandle) -> Self {
        Self::with_capability(CAPABILITY, load_config(), protocols)
    }

    pub fn with_capability(
        capability: &'static str,
        config: RunnerConfig,
        protocols: AukiProtocolsHandle,
    ) -> Self {
        Self {
            config,
            capability,
            protocols,
        }
    }

    pub fn for_all_capabilities(protocols: AukiProtocolsHandle) -> Vec<Self> {
        let config = load_config();
        CAPABILITIES
            .iter()
            .map(|cap| Self::with_capability(cap, config.clone(), protocols.clone()))
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

fn load_config() -> RunnerConfig {
    RunnerConfig::from_env().unwrap_or_else(|err| {
        warn!(error = %err, "failed to read local auki runner config; using defaults");
        RunnerConfig::default()
    })
}

#[async_trait::async_trait]
impl Runner for RunnerReconstructionLocalAukiSdk {
    fn capability(&self) -> &'static str {
        self.capability
    }

    async fn run(&self, ctx: TaskCtx<'_>) -> anyhow::Result<()> {
        // The engine reports failures as `err.to_string()`, and anyhow's Display prints
        // only the outermost context -- so a chain like
        // "extract session zip X into Y" <- "write Z" <- "No space left on device"
        // reaches DMS as just the first line with the real cause dropped. Flatten the
        // whole chain into the message before handing the error back.
        self.run_task(ctx).await.map_err(flatten_error_chain)
    }
}

/// Collapse an anyhow chain into a single `outer: middle: inner` message.
fn flatten_error_chain(err: anyhow::Error) -> anyhow::Error {
    anyhow::anyhow!("{:#}", err)
}

impl RunnerReconstructionLocalAukiSdk {
    async fn run_task(&self, ctx: TaskCtx<'_>) -> anyhow::Result<()> {
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
        let _ = ctx
            .ctrl
            .progress(json!({"pct": 5, "stage": "workspace", "status": "prepared"}))
            .await;
        let _ = ctx
            .ctrl
            .log_event(json!({
                "level": "info",
                "stage": "workspace",
                "message": "workspace prepared",
                "task_id": task_id,
                "job_id": job_ctx.metadata.name,
                "timestamp": Utc::now().to_rfc3339(),
            }))
            .await;

        let session = input::materialize_session_zip(&ctx, &workspace, &self.protocols).await?;
        if ctx.ctrl.is_cancelled().await {
            anyhow::bail!("task cancelled during input materialization");
        }
        let session_id = resolve_single_session(&session)?;
        let _ = ctx
            .ctrl
            .progress(json!({
                "pct": 15,
                "stage": "inputs",
                "scan": session.scan_name,
                "session_id": session_id,
                "extracted_files": session.extracted_files,
            }))
            .await;
        let _ = ctx
            .ctrl
            .log_event(json!({
                "level": "info",
                "stage": "inputs",
                "message": "session zip expanded",
                "scan": session.scan_name,
                "session_id": session_id,
                "extracted_files": session.extracted_files,
                "task_id": task_id,
                "job_id": job_ctx.metadata.name,
                "timestamp": Utc::now().to_rfc3339(),
            }))
            .await;

        let python_args = build_python_args(
            &self.config,
            &job_ctx,
            &workspace,
            &session.app_root,
            &session_id,
            &session.scan_name,
        );
        let _ = ctx
            .ctrl
            .progress(json!({"pct": 20, "stage": "python", "status": "starting"}))
            .await;
        let _ = ctx
            .ctrl
            .log_event(json!({
                "level": "info",
                "stage": "python",
                "message": "python pipeline starting",
                "task_id": task_id,
                "job_id": job_ctx.metadata.name,
                "timestamp": Utc::now().to_rfc3339(),
            }))
            .await;

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
        match &python_result {
            Ok(()) => {
                let _ = ctx
                    .ctrl
                    .progress(json!({"pct": 85, "stage": "python", "status": "completed"}))
                    .await;
                let _ = ctx
                    .ctrl
                    .log_event(json!({
                        "level": "info",
                        "stage": "python",
                        "message": "python pipeline completed",
                        "task_id": task_id,
                        "job_id": job_ctx.metadata.name,
                        "timestamp": Utc::now().to_rfc3339(),
                    }))
                    .await;
            }
            Err(err) => {
                let _ = ctx
                    .ctrl
                    .progress(json!({"pct": 20, "stage": "python", "status": "failed"}))
                    .await;
                let _ = ctx
                    .ctrl
                    .log_event(json!({
                        "level": "error",
                        "stage": "python",
                        "message": err.to_string(),
                        "task_id": task_id,
                        "job_id": job_ctx.metadata.name,
                        "timestamp": Utc::now().to_rfc3339(),
                    }))
                    .await;
            }
        }
        if let Err(err) = upload_task_log(ctx.output, &workspace, &task_id).await {
            warn!(error = %err, task_id = %task_id, "failed to upload task log");
        }
        python_result?;

        if ctx.ctrl.is_cancelled().await {
            anyhow::bail!("task cancelled before upload");
        }

        let mut refined_uploader = refined::RefinedUploader::new();
        let uploaded = refined_uploader
            .process(&workspace, ctx.output, true)
            .await?;
        let _ = ctx
            .ctrl
            .progress(json!({"pct": 95, "stage": "upload", "uploaded_scans": uploaded.len()}))
            .await;
        let _ = ctx
            .ctrl
            .log_event(json!({
                "level": "info",
                "stage": "upload",
                "message": "outputs uploaded",
                "uploaded_scans": uploaded.len(),
                "task_id": task_id,
                "job_id": job_ctx.metadata.name,
                "timestamp": Utc::now().to_rfc3339(),
            }))
            .await;

        ctx.ctrl
            .progress(json!({"progress": 100, "status": "succeeded"}))
            .await?;

        Ok(())
    }
}

/// One task refines one session. A capture folder holding several sessions is rejected
/// here rather than in Python so the error names them: the pipeline's own
/// `resolve_auki_session_id` cannot disambiguate them either, and picking one silently
/// would refine an arbitrary session under the requested scan name.
fn resolve_single_session(session: &input::MaterializedSession) -> Result<String> {
    match session.session_ids.as_slice() {
        [only] => Ok(only.clone()),
        [] => anyhow::bail!(
            "no Auki session found under capture folder {}",
            session.app_root.display()
        ),
        many => anyhow::bail!(
            "capture folder {} holds {} sessions ({}); this capability refines a single-session \
             capture",
            session.app_root.display(),
            many.len(),
            many.join(", ")
        ),
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

        let reconstruction_server_url = None;

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
            processing_type: "local_refinement_auki_sdk".to_string(),
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

/// Configuration for the Auki SDK local reconstruction runner.
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
    /// Process every nth image per sensor (1 = every image).
    pub every_nth_image: usize,
}

impl RunnerConfig {
    pub const ENV_WORKSPACE_ROOT: &'static str = "LOCAL_AUKI_RUNNER_WORKSPACE_ROOT";
    pub const ENV_PYTHON_BIN: &'static str = "LOCAL_AUKI_RUNNER_PYTHON_BIN";
    pub const ENV_PYTHON_SCRIPT: &'static str = "LOCAL_AUKI_RUNNER_PYTHON_SCRIPT";
    pub const ENV_PYTHON_ARGS: &'static str = "LOCAL_AUKI_RUNNER_PYTHON_ARGS";
    pub const ENV_EVERY_NTH_IMAGE: &'static str = "LOCAL_AUKI_RUNNER_EVERY_NTH_IMAGE";

    pub const DEFAULT_PYTHON_BIN: &'static str = "python3";
    pub const DEFAULT_PYTHON_SCRIPT: &'static str = "local_auki_main.py";
    pub const DEFAULT_EVERY_NTH_IMAGE: usize = 1;

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

        let every_nth_image = match env::var(Self::ENV_EVERY_NTH_IMAGE) {
            Ok(val) if !val.trim().is_empty() => {
                let parsed = val
                    .parse::<usize>()
                    .with_context(|| format!("invalid {} value", Self::ENV_EVERY_NTH_IMAGE))?;
                parsed.max(1)
            }
            _ => Self::DEFAULT_EVERY_NTH_IMAGE,
        };

        Ok(Self {
            workspace_root,
            python_bin,
            python_script,
            python_args,
            every_nth_image,
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
            every_nth_image: Self::DEFAULT_EVERY_NTH_IMAGE,
        }
    }
}

fn build_python_args(
    config: &RunnerConfig,
    job_ctx: &JobContext,
    workspace: &workspace::Workspace,
    app_root: &Path,
    session_id: &str,
    scan_name: &str,
) -> Vec<String> {
    let mut args = config.python_args.clone();
    args.push("--job_root_path".to_string());
    args.push(workspace.root().display().to_string());
    args.push("--dataset_path".to_string());
    args.push(app_root.display().to_string());
    // `refined/local/<scan>` -- the same layout local refinement produces, so
    // refined::RefinedUploader picks the result up unchanged.
    args.push("--output_path".to_string());
    args.push(workspace.refined_local().display().to_string());
    args.push("--session_id".to_string());
    args.push(session_id.to_string());
    args.push("--output_name".to_string());
    args.push(scan_name.to_string());
    args.push("--every_nth_image".to_string());
    args.push(config.every_nth_image.to_string());
    args.push("--domain_id".to_string());
    args.push(job_ctx.metadata.domain_id.clone());
    args.push("--job_id".to_string());
    args.push(job_ctx.metadata.name.clone());

    args
}

/// Extract the last non-empty segment from a CID/URL-like string.
fn extract_last_segment(input: &str) -> String {
    let trimmed = input.trim_end_matches('/');
    match trimmed.rsplit('/').next() {
        Some(seg) if !seg.is_empty() => seg.to_string(),
        _ => input.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_matches_dms_registration() {
        assert_eq!(CAPABILITY, "/reconstruction/local-refinement-auki-sdk/v0");
        assert_eq!(CAPABILITIES.len(), 1);
        assert_eq!(
            RunnerReconstructionLocalAukiSdk::new(AukiProtocolsHandle::default()).capability(),
            CAPABILITY
        );
    }

    #[test]
    fn defaults_target_the_auki_entrypoint() {
        let config = RunnerConfig::default();
        assert_eq!(config.python_script, PathBuf::from("local_auki_main.py"));
        assert_eq!(config.python_bin, PathBuf::from("python3"));
        assert_eq!(config.every_nth_image, 1);
    }

    #[test]
    fn python_args_point_at_capture_folder_and_refined_local() {
        let temp = tempfile::TempDir::new().unwrap();
        let workspace =
            workspace::Workspace::create(Some(temp.path()), "domain-1", Some("job-1"), "task-1")
                .unwrap();
        let lease: compute_runner_api::LeaseEnvelope = serde_json::from_value(json!({
            "domain_id": "0f1d2d1a-0000-4000-8000-000000000001",
            "task": {
                "id": "0f1d2d1a-0000-4000-8000-000000000002",
                "capability": CAPABILITY,
                "inputs_cids": ["cid-1"],
            }
        }))
        .unwrap();
        let job_ctx = JobContext::from_lease(&lease).unwrap();
        let app_root = temp.path().join("capture");

        let args = build_python_args(
            &RunnerConfig::default(),
            &job_ctx,
            &workspace,
            &app_root,
            "R001-abc",
            "2026-08-05_09-33-06",
        );

        let index_of = |flag: &str| args.iter().position(|a| a == flag).expect(flag);
        assert_eq!(
            args[index_of("--dataset_path") + 1],
            app_root.display().to_string()
        );
        assert_eq!(
            args[index_of("--output_path") + 1],
            workspace.refined_local().display().to_string()
        );
        assert_eq!(
            args[index_of("--job_root_path") + 1],
            workspace.root().display().to_string()
        );
        assert_eq!(args[index_of("--session_id") + 1], "R001-abc");
        assert_eq!(args[index_of("--output_name") + 1], "2026-08-05_09-33-06");
        assert_eq!(args[index_of("--every_nth_image") + 1], "1");
    }

    #[test]
    fn flattened_error_keeps_the_root_cause_in_the_message() {
        // What the engine actually reports is `err.to_string()`, which for a plain
        // anyhow chain shows only the outermost context. Everything below it -- the
        // reason the task failed -- has to be in that one string or it is lost.
        let io_err = std::io::Error::other("No space left on device");
        let chained = anyhow::Error::new(io_err)
            .context("write /jobs/datasets/scan/frame.jpg")
            .context("extract session zip /tmp/in.zip into /jobs/datasets/scan");

        assert_eq!(
            chained.to_string(),
            "extract session zip /tmp/in.zip into /jobs/datasets/scan"
        );

        let flattened = flatten_error_chain(chained).to_string();
        assert!(flattened.contains("extract session zip"), "{flattened}");
        assert!(flattened.contains("frame.jpg"), "{flattened}");
        assert!(flattened.contains("No space left on device"), "{flattened}");
    }

    struct NoInput;
    #[async_trait::async_trait]
    impl compute_runner_api::InputSource for NoInput {
        async fn get_bytes_by_cid(&self, _cid: &str) -> Result<Vec<u8>> {
            anyhow::bail!("not used")
        }
        async fn materialize_cid_to_temp(&self, _cid: &str) -> Result<PathBuf> {
            anyhow::bail!("not used")
        }
    }

    struct NoSink;
    #[async_trait::async_trait]
    impl ArtifactSink for NoSink {
        async fn put_bytes(&self, _rel_path: &str, _bytes: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn put_file(&self, _rel_path: &str, _file_path: &Path) -> Result<()> {
            Ok(())
        }
    }

    struct QuietCtrl;
    #[async_trait::async_trait]
    impl compute_runner_api::ControlPlane for QuietCtrl {
        async fn is_cancelled(&self) -> bool {
            false
        }
        async fn progress(&self, _value: serde_json::Value) -> Result<()> {
            Ok(())
        }
        async fn log_event(&self, _fields: serde_json::Value) -> Result<()> {
            Ok(())
        }
    }

    struct NoToken;
    impl compute_runner_api::runner::AccessTokenProvider for NoToken {
        fn get(&self) -> String {
            String::new()
        }
    }

    /// Drives the real `Runner::run` entry point (not the inner helper) to confirm the
    /// failure a caller sees carries the whole cause chain -- the wiring that turned a
    /// production ENOSPC into an unexplained "extract session zip X into Y".
    #[tokio::test]
    async fn run_reports_failures_with_the_full_cause_chain() {
        let lease: compute_runner_api::LeaseEnvelope = serde_json::from_value(json!({
            "domain_id": "0f1d2d1a-0000-4000-8000-000000000001",
            "task": {
                "id": "0f1d2d1a-0000-4000-8000-000000000002",
                "capability": CAPABILITY,
                // Two inputs: rejected inside materialize_session_zip, so the error
                // travels the same path a disk failure would.
                "inputs_cids": ["cid-a", "cid-b"],
            }
        }))
        .unwrap();

        // The handle is never activated: this rejects on the input count long
        // before anything would resolve a protocol context.
        let runner = RunnerReconstructionLocalAukiSdk::new(AukiProtocolsHandle::default());
        let err = runner
            .run(TaskCtx {
                lease: &lease,
                input: &NoInput,
                output: &NoSink,
                ctrl: &QuietCtrl,
                access_token: &NoToken,
            })
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("recording reference input"), "{message}");
        assert!(message.contains("cid-a, cid-b"), "{message}");
    }

    #[test]
    fn multi_session_capture_is_rejected_with_names() {
        let session = input::MaterializedSession {
            cid: "cid".into(),
            data_id: None,
            name: None,
            data_type: None,
            domain_id: None,
            scan_name: "scan".into(),
            dataset_dir: PathBuf::from("/tmp/datasets/scan"),
            app_root: PathBuf::from("/tmp/datasets/scan/capture"),
            session_ids: vec!["R001-a".into(), "R001-b".into()],
            extracted_files: 2,
        };

        let err = resolve_single_session(&session).unwrap_err().to_string();
        assert!(err.contains("R001-a"), "{err}");
        assert!(err.contains("R001-b"), "{err}");
    }
}
