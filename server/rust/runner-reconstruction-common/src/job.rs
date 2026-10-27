//! Per-job plumbing every runner shares: the job metadata file, the task log upload and
//! workspace cleanup.

use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::Utc;
use node_host::{ArtifactContent, ArtifactRequest, TaskContext, TaskIo};
use serde::Serialize;
use tokio::fs;
use tracing::warn;

use crate::workspace::Workspace;

/// When true, the job workspace directory is left on disk after the task finishes (same env semantics as splatter-server).
pub fn tasks_cleanup_disabled() -> bool {
    match env::var("DISABLE_TASKS_CLEANUP") {
        Ok(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => false,
    }
}

/// Removes a directory when dropped.
pub struct WorkspaceCleanup(pub PathBuf);

impl Drop for WorkspaceCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Upload `<job root>/log.txt` as the task log, if Python wrote one.
pub async fn upload_task_log(sink: &TaskIo, workspace: &Workspace, task_id: &str) -> Result<()> {
    let log_path = workspace.root().join("log.txt");
    if !log_path.exists() {
        warn!(task_id = %task_id, path = %log_path.display(), "task log missing");
        return Ok(());
    }

    let rel_path = format!("logs/{task_id}.txt");
    let name = format!("task_log_{task_id}");
    sink.put_domain_artifact(ArtifactRequest {
        rel_path: &rel_path,
        name: &name,
        data_type: "task_log_txt",
        existing_id: None,
        content: ArtifactContent::File(&log_path),
    })
    .await
    .with_context(|| format!("upload task log {}", log_path.display()))?;
    Ok(())
}

/// `job_metadata.json`, written into the job root before Python starts.
#[derive(Serialize)]
pub struct JobMetadataRecord {
    pub id: String,
    pub name: String,
    pub domain_id: String,
    pub processing_type: String,
    pub created_at: String,
    pub domain_server_url: String,
    pub reconstruction_server_url: Option<String>,
    pub data_ids: Vec<String>,
}

pub struct JobContext {
    pub metadata: JobMetadataRecord,
}

impl JobContext {
    /// `processing_type`: e.g. "local_refinement", recorded in the metadata file.
    pub fn from_task(task: &TaskContext, processing_type: &str) -> Result<Self> {
        let lease = task.credential.lease_snapshot()?;
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
            processing_type: processing_type.to_string(),
            created_at: Utc::now().to_rfc3339(),
            domain_server_url,
            reconstruction_server_url,
            data_ids,
        };

        Ok(Self { metadata })
    }

    pub async fn persist_metadata(&self, path: &Path) -> Result<()> {
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

    /// Suffix for the names of Domain data this job uploads: its creation time, else the job
    /// name, else `fallback`.
    pub fn domain_data_name_suffix(&self, fallback: &str) -> String {
        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(&self.metadata.created_at) {
            return parsed.format("%Y-%m-%d_%H-%M-%S").to_string();
        }
        if !self.metadata.name.trim().is_empty() {
            return self.metadata.name.clone();
        }
        fallback.to_string()
    }
}

/// Extract the last non-empty segment from a CID/URL-like string.
pub fn extract_last_segment(input: &str) -> String {
    let trimmed = input.trim_end_matches('/');
    match trimmed.rsplit('/').next() {
        Some(seg) if !seg.is_empty() => seg.to_string(),
        _ => input.to_string(),
    }
}
