use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Error, Result};
use node_host::auki_sdk::DataMetadata;
use node_host::{TaskContext, TaskIo};
use tokio::fs;
use tracing::info;

use crate::strategy::unzip_refined_scan;
use crate::workspace::Workspace;

const REQUIRED_SFM_FILES: &[&str] = &["images.bin", "cameras.bin", "points3D.bin", "portals.csv"];
const REQUIRED_GLOBAL_SFM_FILES: &[&str] = &[
    "images.bin",
    "cameras.bin",
    "points3D.bin",
    "frames.bin",
    "rigs.bin",
];

/// Data captured for each materialized refined scan.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MaterializedRefinedScan {
    pub name: String,
    pub data_id: String,
    pub scan_name: String,
    pub dataset_dir: PathBuf,
    pub zip_path: PathBuf,
    pub refined_sfm_dir: PathBuf,
}

/// Materialize each refined scan name into the workspace, downloading from Domain,
/// keeping the zip under datasets, and extracting into refined/local/<scan>/sfm.
pub async fn materialize_refined_scans(
    task: &TaskContext,
    io: &TaskIo,
    workspace: &Workspace,
) -> Result<Vec<MaterializedRefinedScan>> {
    let domain_id = io.domain_id().to_string();

    let mut scans = Vec::new();
    for name in &task.task.inputs_cids {
        if is_url(name) {
            return Err(anyhow!(
                "update refinement expects refined scan names, got URL: {}",
                name
            ));
        }

        let meta = match resolve_by_name_and_type(io, &domain_id, name, "refined_scan_zip")
            .await
            .with_context(|| format!("resolve refined scan name {}", name))
        {
            Ok(meta) => meta,
            Err(_) => continue,
        };

        info!(
            target: "runner_reconstruction_update",
            name = %name,
            data_id = %meta.id,
            "resolved refined scan name to domain data ID"
        );

        let scan_name = strip_refined_prefix(name);
        let dataset_dir = workspace.datasets().join(&scan_name);
        fs::create_dir_all(&dataset_dir)
            .await
            .with_context(|| format!("create dataset directory {}", dataset_dir.display()))?;

        let zip_path = dataset_dir.join("RefinedScan.zip");
        io.download_to(meta.id, meta.size, &zip_path)
            .await
            .map_err(|e| anyhow!("failed to download refined scan '{}': {}", name, e))?;
        let bytes = fs::read(&zip_path)
            .await
            .with_context(|| format!("read refined scan zip {}", zip_path.display()))?;

        let refined_sfm_dir = workspace.refined_local().join(&scan_name).join("sfm");
        let _ = unzip_refined_scan(bytes, &refined_sfm_dir)
            .await
            .with_context(|| {
                format!(
                    "unzip refined scan {} into {}",
                    zip_path.display(),
                    refined_sfm_dir.display()
                )
            })?;

        if !has_required_sfm_files(&refined_sfm_dir) {
            return Err(anyhow!(
                "refined scan '{}' missing required sfm files under {}",
                scan_name,
                refined_sfm_dir.display()
            ));
        }

        scans.push(MaterializedRefinedScan {
            name: name.to_string(),
            data_id: meta.id.to_string(),
            scan_name,
            dataset_dir,
            zip_path,
            refined_sfm_dir,
        });
    }

    Ok(scans)
}

#[allow(dead_code)]
pub struct MaterializedRefinedGlobal {
    pub name: String,
    pub data_id: String,
    pub scan_name: String,
    pub dataset_dir: PathBuf,
    pub refined_sfm_dir: PathBuf,
}

pub async fn materialize_global_colmap(
    task: &TaskContext,
    io: &TaskIo,
    workspace: &Workspace,
) -> Result<MaterializedRefinedGlobal> {
    let domain_id = io.domain_id().to_string();

    let expected_colmap = [
        ("colmap_images_bin", "images.bin"),
        ("colmap_cameras_bin", "cameras.bin"),
        ("colmap_points3d_bin", "points3D.bin"),
        ("colmap_frames_bin", "frames.bin"),
        ("colmap_rigs_bin", "rigs.bin"),
    ];

    let mut global_refinement_name = "";

    fs::create_dir_all(&workspace.refined_global().join("refined_sfm_combined"))
        .await
        .with_context(|| {
            format!(
                "create dataset directory {}",
                workspace
                    .refined_global()
                    .join("refined_sfm_combined")
                    .display()
            )
        })?;

    for (display_name, file_name) in &expected_colmap {
        for name in &task.task.inputs_cids {
            if is_url(name) {
                return Err(anyhow!(
                    "update refinement expects refined scan names, got URL: {}",
                    name
                ));
            }

            let meta = match resolve_by_name_and_type(io, &domain_id, name, display_name)
                .await
                .with_context(|| format!("resolve colmap file name {}", name))
            {
                Ok(meta) => meta,
                Err(_) => continue,
            };

            info!(
                target: "runner_reconstruction_update",
                name = %name,
                data_id = %meta.id,
                "resolved refined scan name to domain data ID"
            );

            let file_path = &workspace
                .root()
                .join("refined")
                .join("global")
                .join("refined_sfm_combined")
                .join(file_name);
            io.download_to(meta.id, meta.size, file_path)
                .await
                .map_err(|e| anyhow!("failed to download colmap file '{}': {}", name, e))?;

            if global_refinement_name.is_empty() {
                let prefix = format!("{}{}", display_name, "_");
                global_refinement_name = name.as_str().strip_prefix(&prefix).unwrap_or(name);
            }
        }
    }

    let sfm_dir = workspace
        .root()
        .join("refined")
        .join("global")
        .join("refined_sfm_combined");

    if !has_required_global_sfm_files(&sfm_dir) {
        return Err(anyhow!(
            "global colmap files' missing required sfm files under {}",
            workspace
                .root()
                .join("refined")
                .join("global")
                .join("refined_sfm_combined")
                .display()
        ));
    }

    let result = Some(MaterializedRefinedGlobal {
        name: global_refinement_name.to_string(),
        data_id: "".to_string(),
        scan_name: global_refinement_name.to_string(),
        dataset_dir: PathBuf::new(),
        refined_sfm_dir: sfm_dir,
    });

    result.ok_or_else(|| anyhow!("could not materialize any global colmap refined scan"))
}

pub async fn materialize_refine_manifest(
    task: &TaskContext,
    io: &TaskIo,
    workspace: &Workspace,
) -> Result<(), Error> {
    let domain_id = io.domain_id().to_string();

    for name in &task.task.inputs_cids {
        if is_url(name) {
            return Err(anyhow!(
                "update refinement expects refined scan names, got URL: {}",
                name
            ));
        }

        let meta = match resolve_by_name_and_type(io, &domain_id, name, "refined_manifest_json")
            .await
            .with_context(|| format!("resolve refined_manifest name {}", name))
        {
            Ok(meta) => meta,
            Err(_) => continue,
        };

        info!(
            target: "runner_reconstruction_update",
            name = %name,
            data_id = %meta.id,
            "resolved refined_manifest to domain data ID"
        );

        let file_path = workspace
            .root()
            .join("refined")
            .join("global")
            .join("refined_manifest.json");
        io.download_to(meta.id, meta.size, &file_path)
            .await
            .map_err(|e| anyhow!("failed to download refined_manifest '{}': {}", name, e))?;
    }
    Ok(())
}

async fn resolve_by_name_and_type(
    io: &TaskIo,
    domain_id: &str,
    name: &str,
    data_type: &str,
) -> Result<DataMetadata> {
    let metas = io
        .find(name, Some(data_type))
        .await
        .map_err(|e| anyhow!("failed to query Domain for artifact '{}': {}", name, e))?;

    metas
        .into_iter()
        .find(|m| m.name == name && m.data_type == data_type)
        .ok_or_else(|| anyhow!("artifact '{}' not found in domain {}", name, domain_id))
}

fn strip_refined_prefix(name: &str) -> String {
    name.strip_prefix("refined_scan_")
        .unwrap_or(name)
        .to_string()
}

fn has_required_sfm_files(sfm: &Path) -> bool {
    REQUIRED_SFM_FILES
        .iter()
        .all(|name| sfm.join(name).exists())
}

fn has_required_global_sfm_files(sfm: &Path) -> bool {
    REQUIRED_GLOBAL_SFM_FILES
        .iter()
        .all(|name| sfm.join(name).exists())
}

fn is_url(cid: &str) -> bool {
    cid.starts_with("http://") || cid.starts_with("https://")
}
