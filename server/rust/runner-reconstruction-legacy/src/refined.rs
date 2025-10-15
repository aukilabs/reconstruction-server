use std::collections::HashSet;
use std::io::{Cursor, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use compute_runner_api::ArtifactSink;
use tokio::task;
use walkdir::WalkDir;
use zip::{write::FileOptions, CompressionMethod, ZipWriter};

use crate::workspace::Workspace;

/// Tracks which scans have already been uploaded from the local refinement folder.
#[derive(Default)]
pub struct RefinedUploader {
    completed: HashSet<String>,
}

impl RefinedUploader {
    pub fn new() -> Self {
        Self {
            completed: HashSet::new(),
        }
    }

    /// Scan `workspace.refined_local()` for completed scans, zipping any new `sfm`
    /// folders and uploading them through the artifact sink. Returns the list of
    /// scan identifiers that were uploaded during this call.
    pub async fn process(
        &mut self,
        workspace: &Workspace,
        sink: &dyn ArtifactSink,
    ) -> Result<Vec<String>> {
        let mut uploaded = Vec::new();
        let refined_root = workspace.refined_local();
        let entries = match std::fs::read_dir(refined_root) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(uploaded),
            Err(err) => {
                return Err(err).with_context(|| format!("read_dir {}", refined_root.display()))
            }
        };

        for entry in entries.flatten() {
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let scan_id = entry.file_name().to_string_lossy().to_string();
            if self.completed.contains(&scan_id) {
                continue;
            }
            let sfm_path = entry.path().join("sfm");
            if !sfm_path.exists() {
                continue;
            }

            let zip_bytes = zip_directory(&sfm_path)
                .await
                .with_context(|| format!("zip scan directory {}", sfm_path.display()))?;
            if zip_bytes.is_empty() {
                continue;
            }

            let artifact_path = format!("refined/local/{}/RefinedScan.zip", scan_id);
            sink.put_bytes(&artifact_path, &zip_bytes)
                .await
                .with_context(|| format!("upload refined scan {}", scan_id))?;

            self.completed.insert(scan_id.clone());
            uploaded.push(scan_id);
        }

        Ok(uploaded)
    }
}

async fn zip_directory(dir: &Path) -> Result<Vec<u8>> {
    let dir = dir.to_path_buf();
    let bytes = task::spawn_blocking(move || zip_directory_blocking(&dir)).await??;
    Ok(bytes)
}

fn zip_directory_blocking(dir: &Path) -> Result<Vec<u8>> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = FileOptions::default().compression_method(CompressionMethod::Stored);
    let mut has_files = false;

    for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path == dir {
            continue;
        }
        if entry.file_type().is_dir() {
            continue;
        }
        let relative = path
            .strip_prefix(dir)
            .map_err(|_| anyhow!("failed to strip prefix"))?
            .to_string_lossy()
            .replace('\\', "/");
        writer.start_file(relative, options)?;
        let bytes = std::fs::read(path)?;
        writer.write_all(&bytes)?;
        has_files = true;
    }

    if !has_files {
        return Ok(Vec::new());
    }

    let cursor = writer.finish()?;
    Ok(cursor.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zip_directory_blocking_returns_empty_when_no_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("sfm");
        std::fs::create_dir_all(&dir).unwrap();
        let bytes = zip_directory_blocking(&dir).unwrap();
        assert!(bytes.is_empty());
    }
}
