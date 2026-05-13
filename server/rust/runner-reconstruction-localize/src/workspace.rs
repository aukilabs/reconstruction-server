use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use tempfile::TempDir;

pub struct Workspace {
    root: PathBuf,
    refined_global: PathBuf,
    metadata: PathBuf,
    _temp_guard: Option<TempDir>,
}

impl Workspace {
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
        let refined_global = root.join("refined").join("global");
        let metadata = root.join("job_metadata.json");
        create_dir(&refined_global)?;
        Ok(Self {
            root,
            refined_global,
            metadata,
            _temp_guard: temp_guard,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn refined_global(&self) -> &Path {
        &self.refined_global
    }

    pub fn job_metadata_path(&self) -> &Path {
        &self.metadata
    }
}

fn create_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create directory {}", parent.display()))?;
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
