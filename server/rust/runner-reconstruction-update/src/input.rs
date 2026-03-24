use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Error, Result};
use compute_runner_api::TaskCtx;
use posemesh_domain_http::domain_data::{download_by_id, download_metadata_v1, DownloadQuery};
use tokio::fs;
use tracing::info;
use uuid::Uuid;

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
    ctx: &TaskCtx<'_>,
    workspace: &Workspace,
) -> Result<Vec<MaterializedRefinedScan>> {
    let domain_url = ctx
        .lease
        .domain_server_url
        .as_ref()
        .map(|u| u.to_string())
        .unwrap_or_default();
    let domain_url = domain_url.trim().trim_end_matches('/').to_string();

    let domain_id = ctx
        .lease
        .domain_id
        .map(|id| id.to_string())
        .unwrap_or_default();

    if domain_url.is_empty() {
        return Err(anyhow!(
            "cannot resolve refined scan by name: no domain_server_url in lease"
        ));
    }
    if domain_id.is_empty() {
        return Err(anyhow!(
            "cannot resolve refined scan by name: no domain_id in lease"
        ));
    }

    let client_id = get_client_id();
    let token = ctx.access_token.get();

    let mut scans = Vec::new();
    for name in &ctx.lease.task.inputs_cids {
        if is_url(name) {
            return Err(anyhow!(
                "update refinement expects refined scan names, got URL: {}",
                name
            ));
        }

        let meta = match resolve_by_name(&domain_url, &client_id, &token, &domain_id, name)
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

        let bytes = download_by_id(&domain_url, &client_id, &token, &domain_id, &meta.id)
            .await
            .map_err(|e| anyhow!("failed to download refined scan '{}': {}", name, e))?;

        let scan_name = strip_refined_prefix(name);
        let dataset_dir = workspace.datasets().join(&scan_name);
        fs::create_dir_all(&dataset_dir)
            .await
            .with_context(|| format!("create dataset directory {}", dataset_dir.display()))?;

        let zip_path = dataset_dir.join("RefinedScan.zip");
        fs::write(&zip_path, &bytes)
            .await
            .with_context(|| format!("write refined scan zip {}", zip_path.display()))?;

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
            data_id: meta.id,
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
    ctx: &TaskCtx<'_>,
    workspace: &Workspace,
) -> Result<MaterializedRefinedGlobal> {
    let domain_url = ctx
        .lease
        .domain_server_url
        .as_ref()
        .map(|u| u.to_string())
        .unwrap_or_default();
    let domain_url = domain_url.trim().trim_end_matches('/').to_string();

    let domain_id = ctx
        .lease
        .domain_id
        .map(|id| id.to_string())
        .unwrap_or_default();

    if domain_url.is_empty() {
        return Err(anyhow!(
            "cannot resolve refined scan by name: no domain_server_url in lease"
        ));
    }
    if domain_id.is_empty() {
        return Err(anyhow!(
            "cannot resolve refined scan by name: no domain_id in lease"
        ));
    }

    let client_id = get_client_id();
    let token = ctx.access_token.get();

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
                &workspace
                    .refined_global()
                    .join("refined_sfm_combined")
                    .display()
            )
        })?;

    for (display_name, file_name) in &expected_colmap {
        for name in &ctx.lease.task.inputs_cids {
            if is_url(name) {
                return Err(anyhow!(
                    "update refinement expects refined scan names, got URL: {}",
                    name
                ));
            }

            let meta = match resolve_by_name_and_type(
                &domain_url,
                &client_id,
                &token,
                &domain_id,
                name,
                display_name,
            )
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

            let bytes = download_by_id(&domain_url, &client_id, &token, &domain_id, &meta.id)
                .await
                .map_err(|e| anyhow!("failed to download colmap file '{}': {}", name, e))?;

            if global_refinement_name.is_empty() {
                let prefix = format!("{}{}", display_name, "_");
                global_refinement_name = name.as_str().strip_prefix(&prefix).unwrap_or(name);
            }

            let file_path = &workspace
                .root()
                .join("refined")
                .join("global")
                .join("refined_sfm_combined")
                .join(file_name);
            fs::write(&file_path, &bytes)
                .await
                .with_context(|| format!("write file {}", file_path.display()))?;
        }
    }

    let sfm_dir = workspace
        .root()
        .join("refined")
        .join("global")
        .join("refined_sfm_combined");

    println!("Listing files under {}", sfm_dir.display());
    for entry in std::fs::read_dir(&sfm_dir)? {
        let entry = entry?;
        let path = entry.path();
        println!("{}", path.display());
    }

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
    ctx: &TaskCtx<'_>,
    workspace: &Workspace,
) -> Result<(), Error> {
    let domain_url = ctx
        .lease
        .domain_server_url
        .as_ref()
        .map(|u| u.to_string())
        .unwrap_or_default();
    let domain_url = domain_url.trim().trim_end_matches('/').to_string();

    let domain_id = ctx
        .lease
        .domain_id
        .map(|id| id.to_string())
        .unwrap_or_default();

    if domain_url.is_empty() {
        return Err(anyhow!(
            "cannot resolve refined scan by name: no domain_server_url in lease"
        ));
    }
    if domain_id.is_empty() {
        return Err(anyhow!(
            "cannot resolve refined scan by name: no domain_id in lease"
        ));
    }

    let client_id = get_client_id();
    let token = ctx.access_token.get();

    for name in &ctx.lease.task.inputs_cids {
        if is_url(name) {
            return Err(anyhow!(
                "update refinement expects refined scan names, got URL: {}",
                name
            ));
        }

        let meta = match resolve_by_name_and_type(
            &domain_url,
            &client_id,
            &token,
            &domain_id,
            name,
            "refined_manifest_json",
        )
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

        let bytes = download_by_id(&domain_url, &client_id, &token, &domain_id, &meta.id)
            .await
            .map_err(|e| anyhow!("failed to download refined_manifest '{}': {}", name, e))?;

        let file_path = workspace
            .root()
            .join("refined")
            .join("global")
            .join("refined_manifest.json");
        fs::write(&file_path, &bytes)
            .await
            .with_context(|| format!("write file {}", file_path.display()))?;
    }
    Ok(())
}

async fn resolve_by_name(
    domain_url: &str,
    client_id: &str,
    token: &str,
    domain_id: &str,
    name: &str,
) -> Result<posemesh_domain_http::domain_data::DomainDataMetadata> {
    let metas = download_metadata_v1(
        domain_url,
        client_id,
        token,
        domain_id,
        &DownloadQuery {
            ids: Vec::new(),
            name: Some(name.to_string()),
            data_type: Some("refined_scan_zip".to_string()),
        },
    )
    .await
    .map_err(|e| anyhow!("failed to query Domain for artifact '{}': {}", name, e))?;

    metas
        .into_iter()
        .find(|m| m.name == name && m.data_type == "refined_scan_zip")
        .ok_or_else(|| anyhow!("artifact '{}' not found in domain {}", name, domain_id))
}

async fn resolve_by_name_and_type(
    domain_url: &str,
    client_id: &str,
    token: &str,
    domain_id: &str,
    name: &str,
    data_type: &str,
) -> Result<posemesh_domain_http::domain_data::DomainDataMetadata> {
    let metas = download_metadata_v1(
        domain_url,
        client_id,
        token,
        domain_id,
        &DownloadQuery {
            ids: Vec::new(),
            name: Some(name.to_string()),
            data_type: Some(data_type.to_string()),
        },
    )
    .await
    .map_err(|e| anyhow!("failed to query Domain for artifact '{}': {}", name, e))?;

    println!("matching file name {} datatype {}", name, data_type);
    for meta in &metas {
        println!(
            "found artifact with name {} and data_type {}",
            meta.name, meta.data_type
        );
    }

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

fn get_client_id() -> String {
    if let Ok(id) = std::env::var("CLIENT_ID") {
        if !id.trim().is_empty() {
            return id;
        }
    }
    format!("posemesh-compute-node/{}", Uuid::new_v4())
}
