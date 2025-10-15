use std::path::{Path, PathBuf};
use std::{fs, io};

use anyhow::{anyhow, Context, Result};
use compute_runner_api::{MaterializedInput, TaskCtx};

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
pub async fn materialize_datasets(
    ctx: &TaskCtx<'_>,
    workspace: &Workspace,
) -> Result<Vec<MaterializedDataset>> {
    let mut datasets = Vec::new();
    for cid in &ctx.lease.task.inputs_cids {
        let materialized = ctx
            .input
            .materialize_cid_with_meta(cid)
            .await
            .with_context(|| format!("materialize input CID {}", cid))?;

        let copied_paths = copy_materialized_to_workspace(&materialized, workspace)
            .with_context(|| format!("copy dataset for CID {}", cid))?;

        // Best-effort cleanup of the temporary materialization directory.
        if !workspace.root().starts_with(&materialized.root_dir)
            && !materialized.root_dir.starts_with(workspace.root())
        {
            let _ = fs::remove_dir_all(&materialized.root_dir);
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
        let candidate = root.join("Manifest.json");
        if candidate.exists() {
            manifest_path = Some(candidate);
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
