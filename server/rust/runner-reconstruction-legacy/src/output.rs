use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use compute_runner_api::runner::{DomainArtifactContent, DomainArtifactRequest};
use compute_runner_api::ArtifactSink;
use tracing::{debug, info};

use crate::workspace::Workspace;

#[derive(Debug, Clone)]
pub struct OutputSpec {
    pub relative_path: &'static str,
    pub display_name: &'static str,
    pub mandatory: bool,
}

const GLOBAL_OUTPUTS: &[OutputSpec] = &[
    OutputSpec {
        relative_path: "refined/global/refined_manifest.json",
        display_name: "refined_manifest",
        mandatory: true,
    },
    OutputSpec {
        relative_path: "refined/global/RefinedPointCloudReduced.ply",
        display_name: "refined_pointcloud",
        mandatory: true,
    },
    OutputSpec {
        relative_path: "refined/global/RefinedPointCloud.ply.drc",
        display_name: "refined_pointcloud_full_draco",
        mandatory: false,
    },
    OutputSpec {
        relative_path: "refined/global/topology/topology_downsampled_0.111.obj",
        display_name: "topologymesh_v1_lowpoly_obj",
        mandatory: false,
    },
    OutputSpec {
        relative_path: "refined/global/topology/topology_downsampled_0.111.glb",
        display_name: "topologymesh_v1_lowpoly_glb",
        mandatory: false,
    },
    OutputSpec {
        relative_path: "refined/global/topology/topology_downsampled_0.333.obj",
        display_name: "topologymesh_v1_midpoly_obj",
        mandatory: false,
    },
    OutputSpec {
        relative_path: "refined/global/topology/topology_downsampled_0.333.glb",
        display_name: "topologymesh_v1_midpoly_glb",
        mandatory: false,
    },
    OutputSpec {
        relative_path: "refined/global/topology/topology.obj",
        display_name: "topologymesh_v1_highpoly_obj",
        mandatory: false,
    },
    OutputSpec {
        relative_path: "refined/global/topology/topology.glb",
        display_name: "topologymesh_v1_highpoly_glb",
        mandatory: false,
    },
];

/// Upload the final global outputs expected by downstream systems.
/// Returns a map from display name to the artifact path used for upload.
pub async fn upload_final_outputs(
    workspace: &Workspace,
    sink: &dyn ArtifactSink,
    name_suffix: &str,
    override_manifest_id: Option<&str>,
) -> Result<HashMap<String, String>> {
    let mut uploaded = HashMap::new();

    for spec in GLOBAL_OUTPUTS {
        let path = workspace.root().join(spec.relative_path);
        if !path.exists() {
            if spec.mandatory {
                return Err(anyhow::anyhow!(
                    "missing mandatory output '{}' at {}",
                    spec.display_name,
                    path.display()
                ));
            }
            debug!(
                display = spec.display_name,
                rel_path = spec.relative_path,
                abs_path = %path.display(),
                "optional output missing; skipping upload"
            );
            continue;
        }
        let size = std::fs::metadata(&path)
            .map(|m| m.len())
            .unwrap_or_default();
        info!(
            display = spec.display_name,
            rel_path = spec.relative_path,
            size_bytes = size,
            "uploading output"
        );
        let name = format!("{}_{}", spec.display_name, name_suffix);
        let existing_id = if spec.display_name == "refined_manifest" {
            override_manifest_id
        } else {
            None
        };
        sink.put_domain_artifact(DomainArtifactRequest {
            rel_path: spec.relative_path,
            name: &name,
            data_type: data_type_for_display(spec.display_name),
            existing_id,
            content: DomainArtifactContent::File(&path),
        })
        .await
        .with_context(|| format!("upload output {}", spec.display_name))?;
        uploaded.insert(
            spec.display_name.to_string(),
            spec.relative_path.to_string(),
        );
    }

    upload_json_if_exists(sink, "outputs_index.json", workspace.root()).await?;
    upload_json_if_exists(sink, "result.json", workspace.root()).await?;
    upload_json_if_exists(sink, "scan_data_summary.json", workspace.root()).await?;

    Ok(uploaded)
}

async fn upload_json_if_exists(
    sink: &dyn ArtifactSink,
    file_name: &str,
    root: &Path,
) -> Result<()> {
    let path = root.join(file_name);
    if !path.exists() {
        debug!(file = file_name, path = %path.display(), "json output missing; skipping upload");
        return Ok(());
    }
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("read output {}", path.display()))?;
    info!(
        file = file_name,
        size_bytes = bytes.len(),
        "uploading json output"
    );
    sink.put_bytes(file_name, &bytes)
        .await
        .with_context(|| format!("upload output {}", file_name))?;
    Ok(())
}

fn data_type_for_display(display: &str) -> &str {
    match display {
        "refined_manifest" => "refined_manifest_json",
        "refined_pointcloud" => "refined_pointcloud_ply",
        "refined_pointcloud_full_draco" => "refined_pointcloud_ply_draco",
        "topologymesh_v1_lowpoly_obj" => "obj",
        "topologymesh_v1_lowpoly_glb" => "glb",
        "topologymesh_v1_midpoly_obj" => "obj",
        "topologymesh_v1_midpoly_glb" => "glb",
        "topologymesh_v1_highpoly_obj" => "obj",
        "topologymesh_v1_highpoly_glb" => "glb",
        _ => "binary",
    }
}
