use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use compute_runner_api::TaskCtx;
use posemesh_domain_http::domain_data::{download_by_id, download_metadata_v1, DownloadQuery};
use tokio::fs;
use tracing::info;
use uuid::Uuid;

use crate::strategy::unzip_bytes_to_dir;
use crate::workspace::Workspace;

/// Resolve directory passed to `pose_tracking_server.py --reconstruction_root`.
pub async fn resolve_reconstruction_root(
    ctx: &TaskCtx<'_>,
    workspace: &Workspace,
) -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("LOCALIZE_RUNNER_BUNDLE_DIR") {
        let p = PathBuf::from(dir.trim());
        if p.is_dir() {
            return Ok(p);
        }
        return Err(anyhow!(
            "LOCALIZE_RUNNER_BUNDLE_DIR is not a directory: {}",
            p.display()
        ));
    }

    if !ctx.lease.task.inputs_cids.is_empty() {
        materialize_inputs_to_refined_global(ctx, workspace).await?;
    }

    let rg = workspace.refined_global();
    if rg.join("refined_manifest.json").is_file()
        || rg.join("refined_sfm_combined").join("cameras.bin").is_file()
    {
        return Ok(rg.to_path_buf());
    }

    Err(anyhow!(
        "No reconstruction bundle under workspace refined/global: need refined_sfm_combined/cameras.bin (and features.h5). \
         Set LOCALIZE_RUNNER_BUNDLE_DIR to that refined/global directory, or provide task inputs_cids to download artifacts."
    ))
}

async fn materialize_inputs_to_refined_global(
    ctx: &TaskCtx<'_>,
    workspace: &Workspace,
) -> Result<()> {
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
    if domain_url.is_empty() || domain_id.is_empty() {
        return Err(anyhow!(
            "inputs_cids set but domain_server_url or domain_id missing from lease"
        ));
    }
    let client_id = client_id();
    let token = ctx.access_token.get();
    let dest = workspace.refined_global();
    fs::create_dir_all(dest)
        .await
        .with_context(|| format!("create {}", dest.display()))?;

    for (idx, cid) in ctx.lease.task.inputs_cids.iter().enumerate() {
        if is_url(cid) {
            return Err(anyhow!(
                "localize runner expects domain data IDs, got URL: {}",
                cid
            ));
        }
        let meta = resolve_by_id(&domain_url, &client_id, &token, &domain_id, cid)
            .await
            .with_context(|| format!("resolve domain data {}", cid))?;
        info!(id = %meta.id, name = %meta.name, "downloading localization input");
        let bytes = download_by_id(&domain_url, &client_id, &token, &domain_id, &meta.id)
            .await
            .map_err(|e| anyhow!("download {}: {}", cid, e))?;

        if is_zip(&bytes) {
            unzip_bytes_to_dir(&bytes, dest)
                .await
                .with_context(|| format!("unzip input {}", idx))?;
        } else if looks_like_json(&bytes) && idx == 0 {
            let path = dest.join("refined_manifest.json");
            fs::write(&path, &bytes)
                .await
                .with_context(|| format!("write {}", path.display()))?;
        } else {
            let name = sanitize_filename(&meta.name);
            let path = dest.join(name);
            fs::write(&path, &bytes)
                .await
                .with_context(|| format!("write {}", path.display()))?;
        }
    }
    Ok(())
}

async fn resolve_by_id(
    domain_url: &str,
    client_id: &str,
    token: &str,
    domain_id: &str,
    id: &str,
) -> Result<posemesh_domain_http::domain_data::DomainDataMetadata> {
    let metas = download_metadata_v1(
        domain_url,
        client_id,
        token,
        domain_id,
        &DownloadQuery {
            ids: vec![id.to_string()],
            name: None,
            data_type: None,
        },
    )
    .await
    .map_err(|e| anyhow!("metadata query: {}", e))?;
    metas
        .into_iter()
        .find(|m| m.id == id)
        .ok_or_else(|| anyhow!("artifact id '{}' not found", id))
}

fn is_url(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

fn is_zip(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[0] == 0x50 && bytes[1] == 0x4b
}

fn looks_like_json(bytes: &[u8]) -> bool {
    let t = bytes.iter().take(256).copied().filter(|b| !b.is_ascii_whitespace()).next();
    matches!(t, Some(b'{'))
}

fn sanitize_filename(name: &str) -> String {
    let base = name.rsplit('/').next().unwrap_or(name);
    base
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') { c } else { '_' })
        .collect()
}

fn client_id() -> String {
    if let Ok(id) = std::env::var("CLIENT_ID") {
        if !id.trim().is_empty() {
            return id;
        }
    }
    format!("posemesh-compute-node/{}", Uuid::new_v4())
}
