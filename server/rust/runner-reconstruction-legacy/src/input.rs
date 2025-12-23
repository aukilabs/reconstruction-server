use std::path::{Path, PathBuf};
use std::{env, fs, io};

use anyhow::{anyhow, Context, Result};
use compute_runner_api::{MaterializedInput, TaskCtx};
use posemesh_domain_http::domain_data::{download_by_id, download_metadata_v1, DownloadQuery};
use uuid::Uuid;

use crate::workspace::Workspace;

/// Data captured for each materialized dataset.
#[derive(Debug, Clone)]
pub struct MaterializedDataset {
    pub cid: String,
    pub data_id: Option<String>,
    pub name: Option<String>,
    pub data_type: Option<String>,
    pub domain_id: Option<String>,
    pub dataset_dir: PathBuf,
    pub manifest_path: Option<PathBuf>,
}

/// Materialize each CID into the workspace datasets directory, copying the
/// downloaded content onto disk and returning metadata for later processing.
///
/// CIDs can be either:
/// - Full URLs (e.g., `https://domain-server/api/v1/domains/{domain_id}/data/{data_id}`)
/// - Artifact names (e.g., `refined_scan_2025-12-12_14-56-20`) which will be resolved via Domain API
pub async fn materialize_datasets(
    ctx: &TaskCtx<'_>,
    workspace: &Workspace,
) -> Result<Vec<MaterializedDataset>> {
    let mut datasets = Vec::new();
    for cid in &ctx.lease.task.inputs_cids {
        // Check if this is a URL or a name-based reference
        let materialized = if is_url(cid) {
            // Standard URL-based CID materialization
            ctx.input
                .materialize_cid_with_meta(cid)
                .await
                .with_context(|| format!("materialize input CID {}", cid))?
        } else {
            // Name-based lookup: resolve via Domain API
            materialize_by_name(ctx, workspace, cid)
                .await
                .with_context(|| format!("materialize input by name {}", cid))?
        };

        let copied_paths = copy_materialized_to_workspace(&materialized, workspace)
            .with_context(|| format!("copy dataset for CID {}", cid))?;

        // Best-effort cleanup of the temporary materialization directory.
        if !workspace.root().starts_with(&materialized.root_dir)
            && !materialized.root_dir.starts_with(workspace.root())
        {
            fs::remove_dir_all(&materialized.root_dir).ok();
        }

        datasets.push(MaterializedDataset {
            cid: materialized.cid.clone(),
            data_id: materialized.data_id.clone(),
            name: materialized.name.clone(),
            data_type: materialized.data_type.clone(),
            domain_id: materialized.domain_id.clone(),
            dataset_dir: copied_paths.dataset_root,
            manifest_path: copied_paths.manifest_path,
        });
    }
    Ok(datasets)
}

/// Check if a CID string looks like a URL
fn is_url(cid: &str) -> bool {
    cid.starts_with("http://") || cid.starts_with("https://")
}

/// Generate a client ID for Domain API calls
fn get_client_id() -> String {
    if let Ok(id) = env::var("CLIENT_ID") {
        if !id.trim().is_empty() {
            return id;
        }
    }
    format!("posemesh-compute-node/{}", Uuid::new_v4())
}

/// Materialize an artifact by looking it up by name in the Domain API.
///
/// This supports the workflow where `inputs_cids` contains artifact names like
/// `refined_scan_2025-12-12_14-56-20` instead of full URLs. The function will:
/// 1. Query the Domain API to find the data ID for the given name
/// 2. Download the artifact bytes
/// 3. Extract the contents to a temporary directory structured like a standard materialization
async fn materialize_by_name(
    ctx: &TaskCtx<'_>,
    workspace: &Workspace,
    name: &str,
) -> Result<MaterializedInput> {
    let domain_url = ctx
        .lease
        .domain_server_url
        .as_ref()
        .map(|u| u.to_string())
        .unwrap_or_default();
    let domain_url = domain_url.trim().trim_end_matches('/');

    let domain_id = ctx
        .lease
        .domain_id
        .map(|id| id.to_string())
        .unwrap_or_default();

    if domain_url.is_empty() {
        return Err(anyhow!(
            "cannot resolve artifact by name '{}': no domain_server_url in lease",
            name
        ));
    }
    if domain_id.is_empty() {
        return Err(anyhow!(
            "cannot resolve artifact by name '{}': no domain_id in lease",
            name
        ));
    }

    let client_id = get_client_id();
    let token = ctx.access_token.get();

    // Query Domain API to find the data by name
    // Try with refined_scan_zip data_type first (most common for global refinement inputs)
    let metas = download_metadata_v1(
        domain_url,
        &client_id,
        &token,
        &domain_id,
        &DownloadQuery {
            ids: Vec::new(),
            name: Some(name.to_string()),
            data_type: None, // Don't filter by type to be more flexible
        },
    )
    .await
    .map_err(|e| anyhow!("failed to query Domain for artifact '{}': {}", name, e))?;

    let meta = metas
        .into_iter()
        .find(|m| m.name == name)
        .ok_or_else(|| anyhow!("artifact '{}' not found in domain {}", name, domain_id))?;

    tracing::info!(
        target: "runner_reconstruction_legacy",
        name = %name,
        data_id = %meta.id,
        data_type = %meta.data_type,
        "resolved artifact name to domain data ID"
    );

    // Download the artifact bytes
    let bytes = download_by_id(domain_url, &client_id, &token, &domain_id, &meta.id)
        .await
        .map_err(|e| anyhow!("failed to download artifact '{}' (id={}): {}", name, meta.id, e))?;

    // Create a temporary directory structure that matches what materialize_cid_with_meta produces
    let temp_root = workspace.root().join("_materialize_temp").join(&meta.id);
    let datasets_dir = temp_root.join("datasets");

    // Extract scan name from the artifact name (e.g., "refined_scan_2025-12-12_14-56-20" -> "2025-12-12_14-56-20")
    let scan_name = name
        .strip_prefix("refined_scan_")
        .unwrap_or(name)
        .to_string();
    let scan_dir = datasets_dir.join(&scan_name);
    fs::create_dir_all(&scan_dir)
        .with_context(|| format!("create dataset directory {}", scan_dir.display()))?;

    // Write the artifact - for refined_scan_zip, write as RefinedScan.zip
    let artifact_path = if meta.data_type == "refined_scan_zip" {
        let path = scan_dir.join("RefinedScan.zip");
        fs::write(&path, &bytes)
            .with_context(|| format!("write artifact to {}", path.display()))?;
        path
    } else {
        // For other types, use the original name or a generic filename
        let filename = format!("{}.{}", name, meta.data_type.replace('_', "."));
        let path = scan_dir.join(&filename);
        fs::write(&path, &bytes)
            .with_context(|| format!("write artifact to {}", path.display()))?;
        path
    };

    Ok(MaterializedInput {
        cid: name.to_string(),
        path: artifact_path,
        data_id: Some(meta.id),
        name: Some(name.to_string()),
        data_type: Some(meta.data_type),
        domain_id: Some(domain_id),
        root_dir: temp_root,
        related_files: vec![],
        extracted_paths: vec![],
    })
}

struct CopyResult {
    dataset_root: PathBuf,
    manifest_path: Option<PathBuf>,
}

fn copy_materialized_to_workspace(
    materialized: &MaterializedInput,
    workspace: &Workspace,
) -> Result<CopyResult> {
    let source_datasets = materialized.root_dir.join("datasets");
    if !source_datasets.exists() {
        return Err(anyhow!(
            "materialized input missing datasets folder: {}",
            source_datasets.display()
        ));
    }

    let mut manifest_path = None;
    let mut dataset_root = None;

    for entry in fs::read_dir(&source_datasets)
        .with_context(|| format!("read_dir {}", source_datasets.display()))?
    {
        let entry = entry?;
        let file_name = entry.file_name();
        let dest = workspace.datasets().join(&file_name);

        if entry.file_type()?.is_dir() {
            copy_dir_recursively(entry.path(), &dest)?;
            if dataset_root.is_none() {
                dataset_root = Some(dest.clone());
            }
        } else {
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create directory {}", parent.display()))?;
            }
            fs::copy(entry.path(), &dest).with_context(|| {
                format!("copy file {} -> {}", entry.path().display(), dest.display())
            })?;
        }
    }

    if let Some(root) = dataset_root.as_ref() {
        // 1) Normalize manifest file naming to Manifest.json
        let candidate = root.join("Manifest.json");
        if candidate.exists() {
            manifest_path = Some(candidate);
        } else {
            // Fallback: if compute-node saved DMT manifest without renaming,
            // normalize it to Manifest.json for downstream expectations.
            let mut found = None;
            if let Ok(entries) = std::fs::read_dir(root) {
                for entry in entries.flatten() {
                    if let Some(fname) = entry.file_name().to_str() {
                        if fname.ends_with(".dmt_manifest_json")
                            || fname == "manifest.dmt_manifest_json"
                        {
                            found = Some(entry.path());
                            break;
                        }
                    }
                }
            }
            if let Some(src) = found {
                let dest = root.join("Manifest.json");
                std::fs::rename(&src, &dest)
                    .or_else(|_| std::fs::copy(&src, &dest).map(|_| ()))
                    .with_context(|| {
                        format!("normalize manifest {} -> {}", src.display(), dest.display())
                    })?;
                manifest_path = Some(dest);
            }
        }

        // 2) Normalize refined scan zip filename if present under generic naming from compute-node.
        let refined_zip_target = root.join("RefinedScan.zip");
        if !refined_zip_target.exists() {
            if let Ok(entries) = std::fs::read_dir(root) {
                for entry in entries.flatten() {
                    if let Some(fname) = entry.file_name().to_str() {
                        if fname.ends_with(".refined_scan_zip") {
                            // Best-effort rename; fallback to copy on cross-device moves.
                            std::fs::rename(entry.path(), &refined_zip_target)
                                .or_else(|_| {
                                    std::fs::copy(entry.path(), &refined_zip_target).map(|_| ())
                                })
                                .ok();
                            break;
                        }
                    }
                }
            }
        }

        // 3) Normalize DMT input artifacts to legacy-friendly filenames expected by Python
        //    This matches legacy Go helper.go mapping.
        let renames: &[(&str, &str)] = &[
            (".dmt_arposes_csv", "ARposes.csv"),
            (".dmt_portal_detections_csv", "PortalDetections.csv"),
            (".dmt_observations_csv", "PortalDetections.csv"),
            (".dmt_intrinsics_csv", "CameraIntrinsics.csv"),
            (".dmt_cameraintrinsics_csv", "CameraIntrinsics.csv"),
            (".dmt_frames_csv", "Frames.csv"),
            (".dmt_recording_mp4", "Frames.mp4"),
            (".dmt_featurepoints_ply", "FeaturePoints.ply"),
            (".dmt_pointcloud_ply", "FeaturePoints.ply"),
            (".dmt_gyro_csv", "Gyro.csv"),
            (".dmt_accel_csv", "Accel.csv"),
            (".dmt_gyroaccel_csv", "gyro_accel.csv"),
        ];
        if let Ok(entries) = std::fs::read_dir(root) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let fname = match path.file_name().and_then(|s| s.to_str()) {
                    Some(f) => f,
                    None => continue,
                };
                for (suffix, target_name) in renames.iter() {
                    if fname.ends_with(suffix) {
                        let dest = root.join(target_name);
                        if dest.exists() {
                            // Avoid overwriting if multiple matches; prefer first.
                            break;
                        }
                        std::fs::rename(&path, &dest)
                            .or_else(|_| std::fs::copy(&path, &dest).map(|_| ()))
                            .with_context(|| {
                                format!("normalize {} -> {}", path.display(), dest.display())
                            })?;
                        break;
                    }
                }
            }
        }
    }

    Ok(CopyResult {
        dataset_root: dataset_root.unwrap_or_else(|| workspace.datasets().to_path_buf()),
        manifest_path,
    })
}

fn copy_dir_recursively(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    let src = src.as_ref();
    let dst = dst.as_ref();
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let dest_path = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_recursively(entry.path(), dest_path)?;
        } else {
            fs::copy(entry.path(), dest_path)?;
        }
    }
    Ok(())
}
