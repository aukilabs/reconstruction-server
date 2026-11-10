use std::collections::HashSet;
use std::io::{Cursor, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use node_host::{ArtifactContent, ArtifactRequest, TaskIo};
use serde_json::json;
use tokio::task;
use tracing::info;
use walkdir::WalkDir;
use zip::{write::FileOptions, CompressionMethod, ZipWriter};

use crate::workspace::Workspace;

// Mirrored in reconstruction/output/uploads.py (UPLOAD_TABLE), checked by
// tests/test_upload_contract.py; keep both in sync.
const REQUIRED_SFM_FILES: &[&str] = &["images.bin", "cameras.bin", "points3D.bin", "portals.csv"];
const ZIP_ALLOWED_EXTENSIONS: &[&str] = &[".bin", ".csv", ".txt"];

// Global/update Python reads this to order UUID-named scans; only the time is copied
// because the full manifest carries user/device fields.
const CAPTURE_METADATA_FILE: &str = "capture_metadata.json";

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

            let manifest_path = workspace.datasets().join(&scan_id).join("Manifest.json");
            write_sfm_capture_metadata_from_manifest(&manifest_path, &sfm_path)
                .with_context(|| format!("capture metadata for scan {}", scan_id))?;

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
        if !should_include_file(path, dir, allowed_extensions) {
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

/// Writes `sfm/capture_metadata.json` when `manifest_path` contains `scanStartTime`.
fn write_sfm_capture_metadata_from_manifest(manifest_path: &Path, sfm_dir: &Path) -> Result<()> {
    if !manifest_path.is_file() {
        return Ok(());
    }
    let raw = std::fs::read_to_string(manifest_path).with_context(|| {
        format!(
            "read manifest for capture metadata {}",
            manifest_path.display()
        )
    })?;
    let value: serde_json::Value = serde_json::from_str(&raw).with_context(|| {
        format!(
            "parse manifest JSON for capture metadata {}",
            manifest_path.display()
        )
    })?;
    let Some(scan_start) = value.get("scanStartTime").and_then(|v| v.as_str()) else {
        return Ok(());
    };
    let metadata_path = sfm_dir.join(CAPTURE_METADATA_FILE);
    let payload = json!({ "scanStartTime": scan_start });
    std::fs::write(
        &metadata_path,
        serde_json::to_string(&payload).context("serialize capture metadata")?,
    )
    .with_context(|| format!("write capture metadata {}", metadata_path.display()))?;
    Ok(())
}

fn should_include_file(path: &Path, zip_root: &Path, allowed_extensions: &[String]) -> bool {
    if path == zip_root.join(CAPTURE_METADATA_FILE) {
        return true;
    }
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
    use std::fs;
    use zip::ZipArchive;

    #[test]
    fn capture_metadata_helper_copies_scan_start_time_only() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest = tmp.path().join("Manifest.json");
        fs::write(
            &manifest,
            r#"{"scanStartTime":"2025-03-19T10:57:00Z","userId":"u1"}"#,
        )
        .unwrap();
        let sfm = tmp.path().join("sfm");
        fs::create_dir_all(&sfm).unwrap();

        write_sfm_capture_metadata_from_manifest(&manifest, &sfm).unwrap();

        let written = fs::read_to_string(sfm.join(CAPTURE_METADATA_FILE)).unwrap();
        assert_eq!(written, r#"{"scanStartTime":"2025-03-19T10:57:00Z"}"#);
    }

    #[test]
    fn capture_metadata_helper_fails_on_invalid_manifest_json() {
        let tmp = tempfile::tempdir().unwrap();
        let manifest = tmp.path().join("Manifest.json");
        fs::write(&manifest, "{not json").unwrap();
        let sfm = tmp.path().join("sfm");
        fs::create_dir_all(&sfm).unwrap();

        let err = write_sfm_capture_metadata_from_manifest(&manifest, &sfm).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("parse manifest JSON") && msg.contains(&manifest.display().to_string()),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn capture_metadata_helper_skips_missing_manifest_or_field() {
        let tmp = tempfile::tempdir().unwrap();
        let sfm = tmp.path().join("sfm");
        fs::create_dir_all(&sfm).unwrap();
        let missing = tmp.path().join("Manifest.json");

        write_sfm_capture_metadata_from_manifest(&missing, &sfm).unwrap();
        assert!(!sfm.join(CAPTURE_METADATA_FILE).exists());

        fs::write(&missing, r#"{"userId":"u1"}"#).unwrap();
        write_sfm_capture_metadata_from_manifest(&missing, &sfm).unwrap();
        assert!(!sfm.join(CAPTURE_METADATA_FILE).exists());
    }

    #[test]
    fn zip_includes_top_level_capture_metadata_only() {
        let tmp = tempfile::tempdir().unwrap();
        let sfm = tmp.path().join("sfm");
        fs::create_dir_all(sfm.join("x")).unwrap();
        for name in REQUIRED_SFM_FILES {
            fs::write(sfm.join(name), b"x").unwrap();
        }
        fs::write(sfm.join(CAPTURE_METADATA_FILE), r#"{"scanStartTime":"t"}"#).unwrap();
        fs::write(sfm.join("x").join(CAPTURE_METADATA_FILE), b"nested").unwrap();
        fs::write(sfm.join("Manifest.json"), b"{}").unwrap();

        let allowed: Vec<String> = ZIP_ALLOWED_EXTENSIONS
            .iter()
            .map(|s| s.to_string())
            .collect();
        let zip_bytes = zip_directory_blocking(&sfm, &allowed).unwrap();
        let mut archive = ZipArchive::new(Cursor::new(zip_bytes)).unwrap();
        let names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().to_string())
            .collect();
        assert!(names.iter().any(|n| n == CAPTURE_METADATA_FILE));
        assert!(!names.iter().any(|n| n.contains("x/capture_metadata")));
        assert!(!names.iter().any(|n| n.contains("Manifest.json")));
    }
}
