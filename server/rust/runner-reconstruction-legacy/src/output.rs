use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use compute_runner_api::ArtifactSink;

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
) -> Result<HashMap<String, String>> {
    let mut uploaded = HashMap::new();

    for spec in GLOBAL_OUTPUTS {
        let path = workspace.root().join(spec.relative_path);
        if !path.exists() {
            if spec.mandatory {
                return Err(anyhow::anyhow!(
                    "missing mandatory output {} at {}",
                    spec.display_name,
                    path.display()
                ));
            }
            continue;
        }
        let bytes = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read output {}", path.display()))?;
        sink.put_bytes(spec.relative_path, &bytes)
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
        return Ok(());
    }
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("read output {}", path.display()))?;
    sink.put_bytes(file_name, &bytes)
        .await
        .with_context(|| format!("upload output {}", file_name))?;
    Ok(())
}
