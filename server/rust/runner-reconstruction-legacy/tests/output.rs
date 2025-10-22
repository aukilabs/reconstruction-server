use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use compute_runner_api::runner::{DomainArtifactContent, DomainArtifactRequest};
use compute_runner_api::ArtifactSink;
use runner_reconstruction_legacy::{output::upload_final_outputs, workspace::Workspace};
use tempfile::tempdir;
use tokio::runtime::Runtime;

#[derive(Default)]
struct RecordingSink {
    uploads: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

#[async_trait]
impl ArtifactSink for RecordingSink {
    async fn put_bytes(&self, rel_path: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.uploads
            .lock()
            .unwrap()
            .insert(rel_path.to_string(), bytes.to_vec());
        Ok(())
    }

    async fn put_file(&self, rel_path: &str, file_path: &std::path::Path) -> anyhow::Result<()> {
        let bytes = std::fs::read(file_path)?;
        self.put_bytes(rel_path, &bytes).await
    }

    async fn put_domain_artifact(
        &self,
        request: DomainArtifactRequest<'_>,
    ) -> anyhow::Result<Option<String>> {
        match request.content {
            DomainArtifactContent::Bytes(bytes) => {
                self.put_bytes(request.rel_path, bytes).await?;
            }
            DomainArtifactContent::File(path) => {
                self.put_file(request.rel_path, path).await?;
            }
        }
        Ok(None)
    }
}

struct TempWorkspace {
    workspace: Workspace,
    _guard: tempfile::TempDir,
}

fn create_populated_workspace() -> TempWorkspace {
    let root = tempdir().unwrap();
    let ws = Workspace::create(Some(root.path()), "dom", Some("job"), "task").unwrap();

    // Required outputs
    std::fs::create_dir_all(ws.root().join("refined/global/topology")).unwrap();
    std::fs::write(
        ws.root().join("refined/global/refined_manifest.json"),
        b"{\"manifest\":true}",
    )
    .unwrap();
    std::fs::write(
        ws.root()
            .join("refined/global/RefinedPointCloudReduced.ply"),
        b"ply data",
    )
    .unwrap();
    std::fs::write(ws.root().join("result.json"), b"{\"status\":\"ok\"}").unwrap();
    std::fs::write(ws.root().join("outputs_index.json"), b"{}").unwrap();
    std::fs::write(ws.root().join("scan_data_summary.json"), b"{}").unwrap();

    // Optional ones
    std::fs::write(
        ws.root().join("refined/global/RefinedPointCloud.ply.drc"),
        b"drc",
    )
    .unwrap();
    std::fs::write(
        ws.root()
            .join("refined/global/topology/topology_downsampled_0.111.obj"),
        b"obj",
    )
    .unwrap();

    TempWorkspace {
        workspace: ws,
        _guard: root,
    }
}

#[test]
fn upload_final_outputs_sends_expected_files() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp = create_populated_workspace();
        let workspace = temp.workspace;
        let sink = RecordingSink::default();

        let uploaded = upload_final_outputs(&workspace, &sink, "suffix", None)
            .await
            .expect("upload outputs");

        assert!(uploaded.contains_key("refined_manifest"));
        assert!(uploaded.contains_key("refined_pointcloud"));
        assert!(uploaded.contains_key("refined_pointcloud_full_draco"));

        let uploads = sink.uploads.lock().unwrap();
        assert!(uploads.contains_key("refined/global/refined_manifest.json"));
        assert!(uploads.contains_key("refined/global/topology/topology_downsampled_0.111.obj"));
        assert!(uploads.contains_key("result.json"));
        assert!(uploads.contains_key("outputs_index.json"));
    });
}

#[test]
fn missing_optional_outputs_are_ignored() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let root = tempdir().unwrap();
        let ws = Workspace::create(Some(root.path()), "dom", Some("job"), "task").unwrap();
        std::fs::create_dir_all(ws.root().join("refined/global")).unwrap();
        std::fs::write(
            ws.root().join("refined/global/refined_manifest.json"),
            b"{}",
        )
        .unwrap();
        std::fs::write(
            ws.root()
                .join("refined/global/RefinedPointCloudReduced.ply"),
            b"ply",
        )
        .unwrap();

        let sink = RecordingSink::default();
        let uploaded = upload_final_outputs(&ws, &sink, "suffix", None)
            .await
            .expect("upload outputs");
        assert!(uploaded.contains_key("refined_manifest"));
        assert!(uploaded.contains_key("refined_pointcloud"));
        assert!(!uploaded.contains_key("refined_pointcloud_full_draco"));
    });
}

#[test]
fn missing_mandatory_outputs_return_error() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp = tempdir().unwrap();
        let ws = Workspace::create(Some(temp.path()), "dom", Some("job"), "task").unwrap();
        let sink = RecordingSink::default();
        let err = upload_final_outputs(&ws, &sink, "suffix", None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("missing mandatory output"));
    });
}
