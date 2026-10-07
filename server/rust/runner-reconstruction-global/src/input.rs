use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use node_host::auki_sdk::DataMetadata;
use node_host::{TaskContext, TaskIo};
use tokio::fs;
use tracing::info;

use crate::strategy::unzip_refined_scan;
use crate::workspace::Workspace;

const REQUIRED_SFM_FILES: &[&str] = &["images.bin", "cameras.bin", "points3D.bin", "portals.csv"];

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
                "global refinement expects refined scan names, got URL: {}",
                name
            ));
        }

        let meta = resolve_by_name(io, &domain_id, name)
            .await
            .with_context(|| format!("resolve refined scan name {}", name))?;

        info!(
            target: "runner_reconstruction_global",
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

async fn resolve_by_name(io: &TaskIo, domain_id: &str, name: &str) -> Result<DataMetadata> {
    let metas = io
        .find(name, Some("refined_scan_zip"))
        .await
        .map_err(|e| anyhow!("failed to query Domain for artifact '{}': {}", name, e))?;

    metas
        .into_iter()
        .find(|m| m.name == name && m.data_type == "refined_scan_zip")
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

fn is_url(cid: &str) -> bool {
    cid.starts_with("http://") || cid.starts_with("https://")
}
