//! runner-reconstruction-local-auki-session: local refinement of an Auki SDK capture session.
//!
//! The input is one zip of a whole capture folder (registries plus one session's sensor and
//! pose logs). The runner expands it, starts `python3 -m reconstruction local-auki-session`,
//! and uploads the refined scan the way local refinement does (`refined_scan_<scan>`), plus the
//! task log.

use std::{env, path::PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use node_host::{NodeRunner, TaskContext, TaskIo};
use serde_json::json;
use tracing::{info, warn};

mod input;

use runner_reconstruction_common::job::{
    tasks_cleanup_disabled, upload_task_log, JobContext, WorkspaceCleanup,
};
use runner_reconstruction_common::{python, refined, workspace};

/// Public crate identifier used by workspace smoke tests.
pub const CRATE_NAME: &str = "runner-reconstruction-local-auki-session";

/// Capability handled by this runner: a capture zip stored in the Domain.
pub const CAPABILITY: &str = "/reconstruction/local-refinement-auki-session/v1";

/// Convenience slice for wiring all supported capabilities.
pub const CAPABILITIES: [&str; 1] = [CAPABILITY];

pub struct RunnerReconstructionLocalAukiSession {
    config: RunnerConfig,
    capability: &'static str,
}

impl RunnerReconstructionLocalAukiSession {
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

    fn create_workspace(
        &self,
        domain_id: &str,
        job_id: Option<&str>,
        task_id: &str,
    ) -> Result<workspace::Workspace> {
        workspace::Workspace::create(
            workspace::Stage::Local,
            self.config.workspace_root.as_deref(),
            domain_id,
            job_id,
            task_id,
        )
    }
}

impl Default for RunnerReconstructionLocalAukiSession {
    fn default() -> Self {
        Self::new()
    }
}

fn load_config() -> RunnerConfig {
    RunnerConfig::from_env().unwrap_or_else(|err| {
        warn!(error = %err, "failed to read auki session runner config; using defaults");
        RunnerConfig::default()
    })
}

#[async_trait::async_trait]
impl NodeRunner for RunnerReconstructionLocalAukiSession {
    fn capability(&self) -> &'static str {
        self.capability
    }

    async fn run(&self, task: &TaskContext, io: &TaskIo) -> anyhow::Result<()> {
        let domain_id = io.domain_id().to_string();
        let job_id = task.task.job_id.map(|id| id.to_string());
        let task_id = task.task.id.to_string();

        if task.is_cancelled() {
            anyhow::bail!("task cancelled before execution");
        }

        let mut workspace = self.create_workspace(&domain_id, job_id.as_deref(), &task_id)?;
        let retain_workspace_after_run = tasks_cleanup_disabled();
        if retain_workspace_after_run {
            workspace.persist_temp_base();
        }
        // By default remove the workspace on exit; set DISABLE_TASKS_CLEANUP to retain it for debugging.
        let _workspace_cleanup = if retain_workspace_after_run {
            None
        } else {
            Some(WorkspaceCleanup(workspace.root().to_path_buf()))
        };

        let job_ctx = JobContext::from_task(task, "local_refinement_auki_session")?;
        job_ctx
            .persist_metadata(workspace.job_metadata_path())
            .await?;
        info!(
            capability = self.capability,
            domain_id = %job_ctx.metadata.domain_id,
            job_id = %job_ctx.metadata.name,
            task_id = %task_id,
            workspace = %workspace.root().display(),
            retain_workspace_after_run = retain_workspace_after_run,
            "workspace prepared"
        );
        let _ = task.progress(json!({"pct": 5, "stage": "workspace", "status": "prepared"}));

        let session = input::materialize_session_zip(task, io, &workspace).await?;
        if task.is_cancelled() {
            anyhow::bail!("task cancelled during input materialization");
        }
        let session_id = resolve_single_session(&session)?;
        let _ = task.progress(json!({"pct": 15, "stage": "inputs", "scan": session.scan_name}));
        let _ = task.log_event(json!({
            "level": "info",
            "stage": "inputs",
            "message": "capture zip expanded",
            "scan": session.scan_name,
            "session_id": session_id,
            "files": session.extracted_files,
            "task_id": task_id,
            "job_id": job_ctx.metadata.name,
            "timestamp": Utc::now().to_rfc3339(),
        }));

        let python_args =
            build_python_args(&self.config, &job_ctx, &workspace, &session, &session_id);
        let _ = task.progress(json!({"pct": 20, "stage": "python", "status": "starting"}));
        // The task's own token: DMS cancel, lost lease or forced shutdown stops Python.
        let python_result = python::run_python(
            &self.config.python_bin,
            &self.config.python_entry(),
            &python_args,
            &task.cancellation(),
            Some(workspace.root()),
        )
        .await;
        match &python_result {
            Ok(()) => {
                let _ = task.progress(json!({"pct": 85, "stage": "python", "status": "completed"}));
            }
            Err(err) => {
                let _ = task.progress(json!({"pct": 20, "stage": "python", "status": "failed"}));
                let _ = task.log_event(json!({
                    "level": "error",
                    "stage": "python",
                    "message": err.to_string(),
                    "task_id": task_id,
                    "job_id": job_ctx.metadata.name,
                    "timestamp": Utc::now().to_rfc3339(),
                }));
            }
        }
        if let Err(err) = upload_task_log(io, &workspace, &task_id).await {
            warn!(error = %err, task_id = %task_id, "failed to upload task log");
        }
        python_result?;

        if task.is_cancelled() {
            anyhow::bail!("task cancelled before upload");
        }

        let uploaded = refined::RefinedUploader::new()
            .process(&workspace, io, true)
            .await?;
        if uploaded.is_empty() {
            anyhow::bail!(
                "no refined scan to upload for {}: the pipeline wrote no complete sfm folder",
                session.scan_name
            );
        }
        let _ =
            task.progress(json!({"pct": 95, "stage": "upload", "uploaded_scans": uploaded.len()}));

        task.progress(json!({"progress": 100, "status": "succeeded"}))?;
        Ok(())
    }
}

/// The one session in the capture; this capability refines single-session captures.
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

/// Configuration for the Auki session runner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerConfig {
    /// Optional base directory for job workspaces.
    pub workspace_root: Option<PathBuf>,
    /// Python executable used to launch the refinement pipeline.
    pub python_bin: PathBuf,
    /// Module run as `python -m <module> local-auki-session`.
    pub python_module: String,
    /// Script run instead of the module, when `LOCAL_AUKI_SESSION_RUNNER_PYTHON_SCRIPT` is set.
    pub python_script: Option<PathBuf>,
    /// Additional arguments passed to python, after the module or script.
    pub python_args: Vec<String>,
    /// Keep every nth frame of each sensor.
    pub every_nth_image: usize,
}

impl RunnerConfig {
    pub const ENV_WORKSPACE_ROOT: &'static str = "LOCAL_AUKI_SESSION_RUNNER_WORKSPACE_ROOT";
    pub const ENV_PYTHON_BIN: &'static str = "LOCAL_AUKI_SESSION_RUNNER_PYTHON_BIN";
    pub const ENV_PYTHON_MODULE: &'static str = "LOCAL_AUKI_SESSION_RUNNER_PYTHON_MODULE";
    pub const ENV_PYTHON_SCRIPT: &'static str = "LOCAL_AUKI_SESSION_RUNNER_PYTHON_SCRIPT";
    pub const ENV_PYTHON_ARGS: &'static str = "LOCAL_AUKI_SESSION_RUNNER_PYTHON_ARGS";
    pub const ENV_EVERY_NTH_IMAGE: &'static str = "LOCAL_AUKI_SESSION_RUNNER_EVERY_NTH_IMAGE";

    pub const DEFAULT_PYTHON_BIN: &'static str = "python3";
    pub const DEFAULT_PYTHON_MODULE: &'static str = "reconstruction";
    pub const PYTHON_SUBCOMMAND: &'static str = "local-auki-session";
    pub const DEFAULT_EVERY_NTH_IMAGE: usize = 1;

    /// Build a config from environment variables.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| env::var(key).ok())
    }

    /// Build a config from a variable lookup (the environment, in `from_env`).
    pub fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let non_empty = |key| var(key).filter(|v: &String| !v.trim().is_empty());

        let every_nth_image = match non_empty(Self::ENV_EVERY_NTH_IMAGE) {
            Some(val) => val
                .parse::<usize>()
                .ok()
                .filter(|n| *n >= 1)
                .with_context(|| {
                    format!(
                        "invalid {} value {val:?}: expected an integer >= 1",
                        Self::ENV_EVERY_NTH_IMAGE
                    )
                })?,
            None => Self::DEFAULT_EVERY_NTH_IMAGE,
        };

        Ok(Self {
            workspace_root: non_empty(Self::ENV_WORKSPACE_ROOT).map(PathBuf::from),
            python_bin: var(Self::ENV_PYTHON_BIN)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(Self::DEFAULT_PYTHON_BIN)),
            python_module: non_empty(Self::ENV_PYTHON_MODULE)
                .unwrap_or_else(|| Self::DEFAULT_PYTHON_MODULE.to_string()),
            python_script: non_empty(Self::ENV_PYTHON_SCRIPT).map(PathBuf::from),
            python_args: var(Self::ENV_PYTHON_ARGS)
                .map(|raw| raw.split_whitespace().map(|s| s.to_string()).collect())
                .unwrap_or_default(),
            every_nth_image,
        })
    }

    /// What python runs: the script override if set, else the module's subcommand.
    pub(crate) fn python_entry(&self) -> python::PythonEntry {
        match &self.python_script {
            Some(script) => python::PythonEntry::Script(script.clone()),
            None => python::PythonEntry::Module {
                module: self.python_module.clone(),
                subcommand: Self::PYTHON_SUBCOMMAND,
            },
        }
    }
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            workspace_root: None,
            python_bin: PathBuf::from(Self::DEFAULT_PYTHON_BIN),
            python_module: Self::DEFAULT_PYTHON_MODULE.to_string(),
            python_script: None,
            python_args: Vec::new(),
            every_nth_image: Self::DEFAULT_EVERY_NTH_IMAGE,
        }
    }
}

fn build_python_args(
    config: &RunnerConfig,
    job_ctx: &JobContext,
    workspace: &workspace::Workspace,
    session: &input::MaterializedSession,
    session_id: &str,
) -> Vec<String> {
    let mut args = config.python_args.clone();
    args.extend([
        "--job_root_path".to_string(),
        workspace.root().display().to_string(),
        // Outputs go to refined/local/<scan>, as for a DMT scan, so RefinedUploader finds them.
        "--output_path".to_string(),
        workspace.root().join("refined").display().to_string(),
        "--dataset_path".to_string(),
        session.app_root.display().to_string(),
        "--session_id".to_string(),
        session_id.to_string(),
        "--output_name".to_string(),
        session.scan_name.clone(),
        "--every_nth_image".to_string(),
        config.every_nth_image.to_string(),
        "--domain_id".to_string(),
        job_ctx.metadata.domain_id.clone(),
        "--job_id".to_string(),
        job_ctx.metadata.name.clone(),
    ]);
    args
}

#[cfg(test)]
mod tests {
    use super::*;
    use runner_reconstruction_common::job::JobMetadataRecord;
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::path::Path;

    fn config(vars: &[(&str, &str)]) -> RunnerConfig {
        let vars: HashMap<&str, &str> = vars.iter().copied().collect();
        RunnerConfig::from_lookup(|key| vars.get(key).map(|v| v.to_string())).unwrap()
    }

    fn session(session_ids: &[&str]) -> input::MaterializedSession {
        input::MaterializedSession {
            scan_name: "2026-08-05_09-33-06".into(),
            app_root: PathBuf::from("/job/datasets/2026-08-05_09-33-06/capture"),
            session_ids: session_ids.iter().map(|s| s.to_string()).collect(),
            extracted_files: 0,
        }
    }

    #[test]
    fn runs_the_reconstruction_module_by_default() {
        let config = config(&[("LOCAL_AUKI_SESSION_RUNNER_PYTHON_ARGS", "--log_level DEBUG")]);
        assert_eq!(
            python::command_args(&config.python_entry(), &config.python_args),
            [
                "-m",
                "reconstruction",
                "local-auki-session",
                "--log_level",
                "DEBUG"
            ]
            .map(OsString::from)
        );
    }

    #[test]
    fn empty_environment_matches_default() {
        assert_eq!(config(&[]), RunnerConfig::default());
    }

    #[test]
    fn every_nth_image_must_be_positive() {
        assert_eq!(
            config(&[("LOCAL_AUKI_SESSION_RUNNER_EVERY_NTH_IMAGE", "3")]).every_nth_image,
            3
        );
        for bad in ["0", "-1", "x"] {
            let vars = HashMap::from([("LOCAL_AUKI_SESSION_RUNNER_EVERY_NTH_IMAGE", bad)]);
            assert!(RunnerConfig::from_lookup(|key| vars.get(key).map(|v| v.to_string())).is_err());
        }
    }

    #[test]
    fn python_args_name_the_session_and_scan() {
        let temp = tempfile::TempDir::new().unwrap();
        let workspace = workspace::Workspace::create(
            workspace::Stage::Local,
            Some(temp.path()),
            "domain",
            Some("job"),
            "task",
        )
        .unwrap();
        let job_ctx = JobContext {
            metadata: JobMetadataRecord {
                id: "1".into(),
                name: "job_1".into(),
                domain_id: "d1".into(),
                processing_type: "local_refinement_auki_session".into(),
                created_at: String::new(),
                domain_server_url: String::new(),
                reconstruction_server_url: None,
                data_ids: Vec::new(),
            },
        };
        let args = build_python_args(
            &config(&[]),
            &job_ctx,
            &workspace,
            &session(&["R001"]),
            "R001",
        );
        let value = |flag: &str| {
            let i = args.iter().position(|a| a == flag).unwrap();
            args[i + 1].clone()
        };
        assert_eq!(
            Path::new(&value("--output_path")),
            workspace.root().join("refined")
        );
        assert_eq!(
            value("--dataset_path"),
            "/job/datasets/2026-08-05_09-33-06/capture"
        );
        assert_eq!(value("--session_id"), "R001");
        assert_eq!(value("--output_name"), "2026-08-05_09-33-06");
        assert_eq!(value("--every_nth_image"), "1");
        assert_eq!(value("--job_id"), "job_1");
    }

    #[test]
    fn exactly_one_session_is_refined() {
        assert_eq!(resolve_single_session(&session(&["R001"])).unwrap(), "R001");
        assert!(resolve_single_session(&session(&[])).is_err());
        let err = resolve_single_session(&session(&["R001", "R002"]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("2 sessions"), "{err}");
    }
}
