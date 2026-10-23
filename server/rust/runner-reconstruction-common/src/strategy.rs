//! Reconstruction-specific strategies for artifact naming and input handling.

use anyhow::Result;
use std::path::{Path, PathBuf};
use tokio::task;
use zip::ZipArchive;

/// Map a well-known reconstruction output path to a preferred (name, data_type)
/// pair used when uploading artifacts to Domain.
pub fn describe_known_output(rel_path: &str, suffix: &str) -> Option<(String, String)> {
    let name = |base: &str| format!("{}_{}", base, suffix);
    match rel_path.trim_start_matches('/') {
        "refined/global/refined_manifest.json" => {
            Some((name("refined_manifest"), "refined_manifest_json".into()))
        }
        "refined/global/RefinedPointCloudReduced.ply" => Some((
            name("refined_pointcloud_reduced"),
            "refined_pointcloud_ply".into(),
        )),
        "refined/global/RefinedPointCloud.ply.drc" => Some((
            name("refined_pointcloud_full_draco"),
            "refined_pointcloud_ply_draco".into(),
        )),
        "refined/global/topology/topology_downsampled_0.111.obj" => {
            Some((name("topologymesh_v1_lowpoly_obj"), "obj".into()))
        }
        "refined/global/topology/topology_downsampled_0.111.glb" => {
            Some((name("topologymesh_v1_lowpoly_glb"), "glb".into()))
        }
        "refined/global/topology/topology_downsampled_0.333.obj" => {
            Some((name("topologymesh_v1_midpoly_obj"), "obj".into()))
        }
        "refined/global/topology/topology_downsampled_0.333.glb" => {
            Some((name("topologymesh_v1_midpoly_glb"), "glb".into()))
        }
        "refined/global/topology/topology.obj" => {
            Some((name("topologymesh_v1_highpoly_obj"), "obj".into()))
        }
        "refined/global/topology/topology.glb" => {
            Some((name("topologymesh_v1_highpoly_glb"), "glb".into()))
        }
        "outputs_index.json" => Some((name("outputs_index"), "json".into())),
        "result.json" => Some((name("result"), "json".into())),
        "scan_data_summary.json" => Some((name("scan_data_summary"), "json".into())),
        _ => None,
    }
}

/// Unzip a refined scan zip (bytes) into `unzip_root`, returning the list of
/// extracted file paths.
pub async fn unzip_refined_scan(zip_bytes: Vec<u8>, unzip_root: &Path) -> Result<Vec<PathBuf>> {
    let unzip_root = unzip_root.to_path_buf();
    let result = task::spawn_blocking(move || {
        std::fs::create_dir_all(&unzip_root)?;
        let cursor = std::io::Cursor::new(zip_bytes);
        let mut archive = ZipArchive::new(cursor)?;
        let mut extracted = Vec::new();
        for idx in 0..archive.len() {
            let mut file = archive.by_index(idx)?;
            if file.is_dir() {
                continue;
            }
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut file, &mut buf)?;
            let out_path = unzip_root.join(file.name());
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&out_path, &buf)?;
            extracted.push(out_path);
        }
        Ok::<_, anyhow::Error>(extracted)
    })
    .await?;
    result
}
