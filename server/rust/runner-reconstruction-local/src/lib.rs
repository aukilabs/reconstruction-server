//! runner-reconstruction-local: minimal runner for local refinement.

use std::{
    env,
    path::{Path, PathBuf},
};

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
            workspace::Stage::Local,
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
impl NodeRunner for RunnerReconstructionLocal {
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

        let job_ctx = JobContext::from_task(task, "local_refinement")?;
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
            retain_workspace_after_run = retain_workspace_after_run,
            "workspace prepared"
        );
        let _ = task.progress(json!({"pct": 5, "stage": "workspace", "status": "prepared"}));
        let _ = task.log_event(json!({
            "level": "info",
            "stage": "workspace",
            "message": "workspace prepared",
            "task_id": task_id,
            "job_id": job_ctx.metadata.name,
            "timestamp": Utc::now().to_rfc3339(),
        }));

        let datasets = input::materialize_datasets(task, io, &workspace).await?;
        if task.is_cancelled() {
            anyhow::bail!("task cancelled during input materialization");
        }
        let _ = task.progress(json!({"pct": 15, "stage": "inputs", "datasets": datasets.len()}));
        let _ = task.log_event(json!({
            "level": "info",
            "stage": "inputs",
            "message": "inputs materialized",
            "count": datasets.len(),
            "task_id": task_id,
            "job_id": job_ctx.metadata.name,
            "timestamp": Utc::now().to_rfc3339(),
        }));

        let scan_names = collect_scan_names(workspace.datasets())?;
        let python_args = build_python_args(&self.config, &job_ctx, &workspace, &scan_names);
        let _ = task.progress(json!({"pct": 20, "stage": "python", "status": "starting"}));
        let _ = task.log_event(json!({
            "level": "info",
            "stage": "python",
            "message": "python pipeline starting",
            "task_id": task_id,
            "job_id": job_ctx.metadata.name,
            "timestamp": Utc::now().to_rfc3339(),
        }));

        let python_bin = self.config.python_bin.clone();
        let python_entry = self.config.python_entry();
        let job_root = workspace.root().to_path_buf();
        // The task's own token: DMS cancel, lost lease or forced shutdown stops Python.
        let python_result = python::run_python(
            &python_bin,
            &python_entry,
            &python_args,
            &task.cancellation(),
            Some(&job_root),
        )
        .await;
        match &python_result {
            Ok(()) => {
                let _ = task.progress(json!({"pct": 85, "stage": "python", "status": "completed"}));
                let _ = task.log_event(json!({
                    "level": "info",
                    "stage": "python",
                    "message": "python pipeline completed",
                    "task_id": task_id,
                    "job_id": job_ctx.metadata.name,
                    "timestamp": Utc::now().to_rfc3339(),
                }));
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

        let mut refined_uploader = refined::RefinedUploader::new();
        let uploaded = refined_uploader.process(&workspace, io, true).await?;
        let _ =
            task.progress(json!({"pct": 95, "stage": "upload", "uploaded_scans": uploaded.len()}));
        let _ = task.log_event(json!({
            "level": "info",
            "stage": "upload",
            "message": "outputs uploaded",
            "uploaded_scans": uploaded.len(),
            "task_id": task_id,
            "job_id": job_ctx.metadata.name,
            "timestamp": Utc::now().to_rfc3339(),
        }));

        task.progress(json!({"progress": 100, "status": "succeeded"}))?;

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
    /// Module run as `python -m <module> local`.
    pub python_module: String,
    /// Script run instead of the module, when `LOCAL_RUNNER_PYTHON_SCRIPT` is set.
    pub python_script: Option<PathBuf>,
    /// Additional arguments passed to python, after the module or script.
    pub python_args: Vec<String>,
    /// Number of CPU workers granted to the pipeline.
    pub cpu_workers: usize,
}

impl RunnerConfig {
    pub const ENV_WORKSPACE_ROOT: &'static str = "LOCAL_RUNNER_WORKSPACE_ROOT";
    pub const ENV_PYTHON_BIN: &'static str = "LOCAL_RUNNER_PYTHON_BIN";
    pub const ENV_PYTHON_MODULE: &'static str = "LOCAL_RUNNER_PYTHON_MODULE";
    pub const ENV_PYTHON_SCRIPT: &'static str = "LOCAL_RUNNER_PYTHON_SCRIPT";
    pub const ENV_PYTHON_ARGS: &'static str = "LOCAL_RUNNER_PYTHON_ARGS";
    pub const ENV_CPU_WORKERS: &'static str = "LOCAL_RUNNER_CPU_WORKERS";

    pub const DEFAULT_PYTHON_BIN: &'static str = "python3";
    pub const DEFAULT_PYTHON_MODULE: &'static str = "reconstruction";
    pub const PYTHON_SUBCOMMAND: &'static str = "local";
    pub const DEFAULT_CPU_WORKERS: usize = 2;

    /// Build a config from environment variables.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| env::var(key).ok())
    }

    /// Build a config from a variable lookup (the environment, in `from_env`).
    pub fn from_lookup(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let workspace_root = match var(Self::ENV_WORKSPACE_ROOT) {
            Some(v) if !v.trim().is_empty() => Some(PathBuf::from(v)),
            _ => None,
        };

        let python_bin = var(Self::ENV_PYTHON_BIN)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(Self::DEFAULT_PYTHON_BIN));

        let python_module = match var(Self::ENV_PYTHON_MODULE) {
            Some(v) if !v.trim().is_empty() => v,
            _ => Self::DEFAULT_PYTHON_MODULE.to_string(),
        };

        let python_script = match var(Self::ENV_PYTHON_SCRIPT) {
            Some(v) if !v.trim().is_empty() => Some(PathBuf::from(v)),
            _ => None,
        };

        let python_args = var(Self::ENV_PYTHON_ARGS)
            .map(|raw| raw.split_whitespace().map(|s| s.to_string()).collect())
            .unwrap_or_default();

        let cpu_workers = match var(Self::ENV_CPU_WORKERS) {
            Some(val) if !val.trim().is_empty() => val
                .parse::<usize>()
                .with_context(|| format!("invalid {} value", Self::ENV_CPU_WORKERS))?,
            _ => Self::DEFAULT_CPU_WORKERS,
        };

        Ok(Self {
            workspace_root,
            python_bin,
            python_module,
            python_script,
            python_args,
            cpu_workers,
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

#[cfg(test)]
mod config_tests {
    use super::*;
    use std::collections::HashMap;
    use std::ffi::OsString;

    fn config(vars: &[(&str, &str)]) -> RunnerConfig {
        let vars: HashMap<&str, &str> = vars.iter().copied().collect();
        RunnerConfig::from_lookup(|key| vars.get(key).map(|v| v.to_string())).unwrap()
    }

    fn command_line(config: &RunnerConfig) -> Vec<OsString> {
        python::command_args(&config.python_entry(), &config.python_args)
    }

    fn os(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    #[test]
    fn runs_the_reconstruction_module_by_default() {
        let config = config(&[("LOCAL_RUNNER_PYTHON_ARGS", "--log_level DEBUG")]);
        assert_eq!(config.python_bin, PathBuf::from("python3"));
        assert_eq!(
            command_line(&config),
            os(&["-m", "reconstruction", "local", "--log_level", "DEBUG"])
        );
    }

    #[test]
    fn script_override_runs_the_script() {
        let config = config(&[
            ("LOCAL_RUNNER_PYTHON_SCRIPT", "legacy.py"),
            ("LOCAL_RUNNER_PYTHON_ARGS", "--log_level DEBUG"),
        ]);
        assert_eq!(
            command_line(&config),
            os(&["legacy.py", "--log_level", "DEBUG"])
        );
    }

    #[test]
    fn module_override() {
        let config = config(&[("LOCAL_RUNNER_PYTHON_MODULE", "other_pipeline")]);
        assert_eq!(
            command_line(&config),
            os(&["-m", "other_pipeline", "local"])
        );
    }

    #[test]
    fn empty_overrides_are_ignored() {
        let config = config(&[
            ("LOCAL_RUNNER_PYTHON_SCRIPT", ""),
            ("LOCAL_RUNNER_PYTHON_MODULE", " "),
        ]);
        assert_eq!(config, RunnerConfig::default());
    }

    #[test]
    fn empty_environment_matches_default() {
        assert_eq!(config(&[]), RunnerConfig::default());
    }
}
