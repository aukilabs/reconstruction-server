use std::collections::HashSet;
use std::io::{Cursor, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use node_host::capture_identity::{read_capture_manifest, CAPTURE_METADATA_FILE};
use node_host::{ArtifactContent, ArtifactRequest, TaskIo};
use tokio::task;
use tracing::info;
use walkdir::WalkDir;
use zip::{write::FileOptions, CompressionMethod, ZipWriter};

use crate::workspace::Workspace;

const REQUIRED_SFM_FILES: &[&str] = &["images.bin", "cameras.bin", "points3D.bin", "portals.csv"];
const ZIP_ALLOWED_EXTENSIONS: &[&str] = &[".bin", ".csv", ".txt"];

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
        sink: &TaskIo,
        upload_local_zips: bool,
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

            if !upload_local_zips || !has_required_sfm_files(&sfm_path) {
                continue;
            }

            let metadata = read_capture_manifest(
                &scan_id,
                &workspace.datasets().join(&scan_id).join("Manifest.json"),
            )?;
            // The uploader can run while Python is still finishing other scans.
            // Write the metadata before this scan's ZIP is ever published.
            std::fs::write(
                sfm_path.join(CAPTURE_METADATA_FILE),
                serde_json::to_vec(&metadata)?,
            )?;
            let zip_bytes = zip_directory(&sfm_path, ZIP_ALLOWED_EXTENSIONS)
                .await
                .with_context(|| format!("zip scan directory {}", sfm_path.display()))?;
            if zip_bytes.is_empty() {
                continue;
            }

            let artifact_path = format!("refined/local/{}/RefinedScan.zip", scan_id);
            let req = ArtifactRequest {
                rel_path: &artifact_path,
                name: &format!("refined_scan_{}", scan_id),
                data_type: "refined_scan_zip",
                existing_id: None,
                content: ArtifactContent::Bytes(&zip_bytes),
            };

            match sink.put_domain_artifact(req).await {
                Ok(_) => {}
                Err(err) => {
                    if is_conflict_err(&err) {
                        info!(
                            scan = %scan_id,
                            "refined scan already exists in domain (409); skipping upload"
                        );
                    } else {
                        return Err(err)
                            .with_context(|| format!("upload refined scan {}", scan_id));
                    }
                }
            }

            self.completed.insert(scan_id.clone());
            uploaded.push(scan_id);
        }

        Ok(uploaded)
    }
}

async fn zip_directory(dir: &Path, allowed_extensions: &[&str]) -> Result<Vec<u8>> {
    let dir = dir.to_path_buf();
    let allowed = allowed_extensions
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let bytes = task::spawn_blocking(move || zip_directory_blocking(&dir, &allowed)).await??;
    Ok(bytes)
}

fn zip_directory_blocking(dir: &Path, allowed_extensions: &[String]) -> Result<Vec<u8>> {
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
        if path != dir.join(CAPTURE_METADATA_FILE) && !should_include_file(path, allowed_extensions)
        {
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

fn should_include_file(path: &Path, allowed_extensions: &[String]) -> bool {
    if allowed_extensions.is_empty() {
        return true;
    }
    let ext = path.extension().and_then(|os| os.to_str()).map(|s| {
        let mut lower = String::with_capacity(s.len() + 1);
        lower.push('.');
        lower.push_str(&s.to_ascii_lowercase());
        lower
    });
    match ext {
        Some(ext) => allowed_extensions.iter().any(|allowed| allowed == &ext),
        None => false,
    }
}

fn has_required_sfm_files(sfm: &Path) -> bool {
    REQUIRED_SFM_FILES
        .iter()
        .all(|name| sfm.join(name).exists())
}

fn is_conflict_err(err: &anyhow::Error) -> bool {
    let needle1 = "409";
    let needle2 = "conflict";
    for cause in err.chain() {
        let s = cause.to_string();
        if s.contains(needle1) || s.to_ascii_lowercase().contains(needle2) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_zip_preserves_identity_time_and_only_the_metadata_json() {
        let tmp = tempfile::tempdir().unwrap();
        let id = "8c4869c0-99c5-4515-8545-e3674d383338";
        let timestamp = "2026-10-13T12:34:56.1234567Z";
        let manifest_path = tmp.path().join("Manifest.json");
        std::fs::write(
            &manifest_path,
            serde_json::json!({
                "id":id,"scanStartTime":timestamp,"userId":"not-for-cache"
            })
            .to_string(),
        )
        .unwrap();
        let metadata = read_capture_manifest(id, &manifest_path).unwrap();
        std::fs::write(
            tmp.path().join(CAPTURE_METADATA_FILE),
            serde_json::to_vec(&metadata).unwrap(),
        )
        .unwrap();
        for name in REQUIRED_SFM_FILES {
            std::fs::write(tmp.path().join(name), name).unwrap();
        }
        std::fs::create_dir(tmp.path().join("nested")).unwrap();
        std::fs::write(
            tmp.path().join("nested").join(CAPTURE_METADATA_FILE),
            "exclude",
        )
        .unwrap();
        let allowed: Vec<String> = ZIP_ALLOWED_EXTENSIONS
            .iter()
            .map(|s| s.to_string())
            .collect();
        let bytes = zip_directory_blocking(tmp.path(), &allowed).unwrap();
        let mut zip = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
        assert_eq!(zip.len(), REQUIRED_SFM_FILES.len() + 1);
        assert!(zip.by_name("Manifest.json").is_err());
        assert!(zip.by_name("nested/capture_metadata.json").is_err());
        let file = zip.by_name(CAPTURE_METADATA_FILE).unwrap();
        let data: serde_json::Value = serde_json::from_reader(file).unwrap();
        assert_eq!(
            data,
            serde_json::json!({"captureId":id,"scanStartTime":timestamp})
        );
    }
}
