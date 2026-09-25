use std::collections::HashSet;
use std::io::{Cursor, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use compute_runner_api::runner::{DomainArtifactContent, DomainArtifactRequest};
use compute_runner_api::ArtifactSink;
use tokio::task;
use tracing::{debug, info, warn};
use walkdir::WalkDir;
use zip::{write::FileOptions, CompressionMethod, ZipWriter};

use crate::workspace::Workspace;

const REQUIRED_SFM_FILES: &[&str] = &["images.bin", "cameras.bin", "points3D.bin", "portals.csv"];
/// SfM + mono-depth debug intermediates (depths/mesh) shipped inside RefinedScan.zip for now.
const ZIP_ALLOWED_EXTENSIONS: &[&str] = &[".bin", ".csv", ".txt", ".png", ".ply", ".json", ".npy"];
/// Top-level dirs under `refined/local/<scan>/` included in the zip (paths keep the dir prefix).
const ZIP_SCAN_SUBDIRS: &[&str] = &["sfm", "infer", "fit", "carve", "mesh"];

/// Optional mesh outputs under `refined/local/<scan>/mesh/` (file name, artifact name suffix, data_type).
const OPTIONAL_MESH_ARTIFACTS: &[(&str, &str, &str)] = &[
    ("tsdf_mesh.ply", "mesh", "tsdf_mesh_ply"),
    ("tsdf_points.ply", "mesh_points", "tsdf_points_ply"),
    ("meta.json", "mesh_meta", "tsdf_mesh_meta_json"),
];

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

    /// Scan `workspace.refined_local()` for completed scans, zipping `sfm` plus
    /// optional mono-depth intermediates (`infer`/`fit`/`carve`/`mesh`) and uploading
    /// through the artifact sink. Returns the list of scan identifiers uploaded.
    pub async fn process(
        &mut self,
        workspace: &Workspace,
        sink: &dyn ArtifactSink,
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

            // Zip scan root so entries are `sfm/…`, `infer/…`, `fit/…`, `carve/…`, `mesh/…`.
            // Global unpacks into `refined/local/<scan>/` and reads carve/fit depths from there.
            let zip_bytes = zip_scan_root(&entry.path(), ZIP_SCAN_SUBDIRS, ZIP_ALLOWED_EXTENSIONS)
                .await
                .with_context(|| format!("zip scan directory {}", entry.path().display()))?;
            if zip_bytes.is_empty() {
                continue;
            }

            let artifact_path = format!("refined/local/{}/RefinedScan.zip", scan_id);
            let req = DomainArtifactRequest {
                rel_path: &artifact_path,
                name: &format!("refined_scan_{}", scan_id),
                data_type: "refined_scan_zip",
                existing_id: None,
                content: DomainArtifactContent::Bytes(&zip_bytes),
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

            upload_optional_mesh_artifacts(&entry.path(), &scan_id, sink).await?;

            self.completed.insert(scan_id.clone());
            uploaded.push(scan_id);
        }

        Ok(uploaded)
    }
}

async fn zip_scan_root(
    scan_root: &Path,
    subdirs: &[&str],
    allowed_extensions: &[&str],
) -> Result<Vec<u8>> {
    let scan_root = scan_root.to_path_buf();
    let subdirs: Vec<String> = subdirs.iter().map(|s| s.to_string()).collect();
    let allowed = allowed_extensions
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let bytes =
        task::spawn_blocking(move || zip_scan_root_blocking(&scan_root, &subdirs, &allowed))
            .await??;
    Ok(bytes)
}

fn zip_scan_root_blocking(
    scan_root: &Path,
    subdirs: &[String],
    allowed_extensions: &[String],
) -> Result<Vec<u8>> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = FileOptions::default().compression_method(CompressionMethod::Stored);
    let mut has_files = false;

    for sub in subdirs {
        let dir = scan_root.join(sub);
        if !dir.is_dir() {
            continue;
        }
        for entry in WalkDir::new(&dir).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            if path == dir {
                continue;
            }
            if entry.file_type().is_dir() {
                continue;
            }
            if !should_include_file(path, allowed_extensions) {
                continue;
            }
            let relative = path
                .strip_prefix(scan_root)
                .map_err(|_| anyhow!("failed to strip prefix"))?
                .to_string_lossy()
                .replace('\\', "/");
            writer.start_file(relative, options)?;
            let bytes = std::fs::read(path)?;
            writer.write_all(&bytes)?;
            has_files = true;
        }
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

async fn upload_optional_mesh_artifacts(
    scan_root: &Path,
    scan_id: &str,
    sink: &dyn ArtifactSink,
) -> Result<()> {
    let mesh_dir = scan_root.join("mesh");
    if !mesh_dir.is_dir() {
        debug!(scan = %scan_id, "mesh directory missing; skipping mesh artifact upload");
        return Ok(());
    }

    for (file_name, name_suffix, data_type) in OPTIONAL_MESH_ARTIFACTS {
        let path = mesh_dir.join(file_name);
        if !path.is_file() {
            debug!(
                scan = %scan_id,
                file = file_name,
                "optional mesh artifact missing; skipping"
            );
            continue;
        }

        let rel_path = mesh_artifact_rel_path(scan_id, file_name);
        let artifact_name = format!("refined_scan_{}_{}", name_suffix, scan_id);
        let req = DomainArtifactRequest {
            rel_path: &rel_path,
            name: &artifact_name,
            data_type,
            existing_id: None,
            content: DomainArtifactContent::File(&path),
        };

        match sink.put_domain_artifact(req).await {
            Ok(_) => {
                info!(
                    scan = %scan_id,
                    rel_path = %rel_path,
                    "uploaded optional mesh artifact"
                );
            }
            Err(err) if is_conflict_err(&err) => {
                info!(
                    scan = %scan_id,
                    rel_path = %rel_path,
                    "mesh artifact already exists in domain (409); skipping upload"
                );
            }
            Err(err) => {
                warn!(
                    scan = %scan_id,
                    rel_path = %rel_path,
                    error = %err,
                    "optional mesh artifact upload failed; continuing"
                );
            }
        }
    }

    Ok(())
}

fn mesh_artifact_rel_path(scan_id: &str, file_name: &str) -> String {
    format!("refined/local/{}/mesh/{}", scan_id, file_name)
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
    use super::mesh_artifact_rel_path;

    #[test]
    fn mesh_artifact_rel_path_matches_domain_layout() {
        assert_eq!(
            mesh_artifact_rel_path("scan_a", "tsdf_mesh.ply"),
            "refined/local/scan_a/mesh/tsdf_mesh.ply"
        );
    }
}
