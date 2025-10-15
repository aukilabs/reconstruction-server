use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use tempfile::TempDir;

/// Represents the on-disk layout for a reconstruction job.
pub struct Workspace {
    root: PathBuf,
    datasets: PathBuf,
    refined_local: PathBuf,
    refined_global: PathBuf,
    manifest: PathBuf,
    summary: PathBuf,
    metadata: PathBuf,
    _temp_guard: Option<TempDir>,
}

impl Workspace {
    /// Create a workspace using the optional base directory.
    pub fn create(
        base_root: Option<&Path>,
        domain_id: &str,
        job_id: Option<&str>,
        task_id: &str,
    ) -> Result<Self> {
        let domain_segment = sanitize_segment(domain_id);
        let job_segment = job_id
            .filter(|s| !s.trim().is_empty())
            .map(|val| format!("job_{}", sanitize_segment(val)))
            .unwrap_or_else(|| format!("task_{}", sanitize_segment(task_id)));

        let (temp_guard, base_dir) = match base_root {
            Some(base) => (None, base.to_path_buf()),
            None => {
                let temp = TempDir::new().context("create temporary workspace base dir")?;
                let base_path = temp.path().to_path_buf();
                (Some(temp), base_path)
            }
        };

        let root = base_dir.join("jobs").join(domain_segment).join(job_segment);

        let datasets = root.join("datasets");
        let refined_local = root.join("refined").join("local");
        let refined_global = root.join("refined").join("global");
        let manifest = root.join("job_manifest.json");
        let summary = root.join("scan_data_summary.json");
        let metadata = root.join("job_metadata.json");

        create_dir(&datasets)?;
        create_dir(&refined_local)?;
        create_dir(&refined_global)?;

        Ok(Self {
            root,
            datasets,
            refined_local,
            refined_global,
            manifest,
            summary,
            metadata,
            _temp_guard: temp_guard,
        })
    }

    /// Root directory for the job workspace.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Path where datasets are materialized.
    pub fn datasets(&self) -> &Path {
        &self.datasets
    }

    /// Path containing local refinement outputs.
    pub fn refined_local(&self) -> &Path {
        &self.refined_local
    }

    /// Path containing global refinement outputs.
    pub fn refined_global(&self) -> &Path {
        &self.refined_global
    }

    /// Path to the job manifest JSON file.
    pub fn job_manifest_path(&self) -> &Path {
        &self.manifest
    }

    /// Path to the scan data summary JSON file.
    pub fn scan_data_summary_path(&self) -> &Path {
        &self.summary
    }

    /// Path to the job metadata JSON file.
    pub fn job_metadata_path(&self) -> &Path {
        &self.metadata
    }
}

fn create_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create directory {}", parent.display()))?;
    }
    if path.metadata().map(|m| m.is_dir()).unwrap_or(false) {
        return Ok(());
    }
    fs::create_dir_all(path).with_context(|| format!("create directory {}", path.display()))?;
    Ok(())
}

fn sanitize_segment(input: &str) -> String {
    if input.is_empty() {
        return "unnamed".into();
    }
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_segment_replaces_invalid_chars() {
        assert_eq!(sanitize_segment("a:b/c"), "a_b_c");
        assert_eq!(sanitize_segment(""), "unnamed");
    }
}
