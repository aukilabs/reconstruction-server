use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use chrono::Utc;
use compute_runner_api::{
    ArtifactSink, ControlPlane, InputSource, LeaseEnvelope, MaterializedInput, TaskCtx, TaskSpec,
};
use runner_reconstruction_legacy::{input, workspace::Workspace};
use serde_json::json;
use uuid::Uuid;

struct StubInput {
    responses: Arc<Mutex<Vec<MaterializedInput>>>,
}

#[async_trait::async_trait]
impl InputSource for StubInput {
    async fn get_bytes_by_cid(&self, _cid: &str) -> anyhow::Result<Vec<u8>> {
        Ok(vec![])
    }

    async fn materialize_cid_to_temp(&self, _cid: &str) -> anyhow::Result<PathBuf> {
        Ok(std::env::temp_dir())
    }

    async fn materialize_cid_with_meta(&self, _cid: &str) -> anyhow::Result<MaterializedInput> {
        let mut guard = self.responses.lock().unwrap();
        guard
            .pop()
            .ok_or_else(|| anyhow::anyhow!("no materialized input prepared"))
    }
}

struct NullSink;
#[async_trait::async_trait]
impl ArtifactSink for NullSink {
    async fn put_bytes(&self, _rel_path: &str, _bytes: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }
    async fn put_file(&self, _rel_path: &str, _file_path: &std::path::Path) -> anyhow::Result<()> {
        Ok(())
    }
}

struct NullCtrl;
#[async_trait::async_trait]
impl ControlPlane for NullCtrl {
    async fn is_cancelled(&self) -> bool {
        false
    }
    async fn progress(&self, _value: serde_json::Value) -> anyhow::Result<()> {
        Ok(())
    }
    async fn log_event(&self, _fields: serde_json::Value) -> anyhow::Result<()> {
        Ok(())
    }
}

fn make_materialized_input(temp_root: &Path, folder: &str) -> MaterializedInput {
    let dataset_root = temp_root.join("datasets").join(folder);
    std::fs::create_dir_all(&dataset_root).unwrap();
    std::fs::write(dataset_root.join("dummy.bin"), b"data").unwrap();
    std::fs::write(dataset_root.join("Manifest.json"), b"{}\n").unwrap();

    MaterializedInput {
        cid: folder.to_string(),
        path: dataset_root.join("dummy.bin"),
        data_id: Some(format!("data-{}", folder)),
        name: Some(folder.to_string()),
        data_type: Some("refined_scan_zip".into()),
        domain_id: Some("dom1".into()),
        root_dir: temp_root.to_path_buf(),
        related_files: vec![],
        extracted_paths: vec![],
    }
}

#[tokio::test]
async fn materialize_copies_into_workspace() {
    let temp = tempfile::tempdir().unwrap();
    let mat1 = make_materialized_input(temp.path(), "scan_a");

    let input = StubInput {
        responses: Arc::new(Mutex::new(vec![mat1])),
    };
    let workspace = Workspace::create(None, "dom-123", Some("job-abc"), "task-xyz").unwrap();

    // Use a URL-formatted CID to trigger standard materialization (not name-based lookup)
    let lease = lease_with_inputs(vec!["https://domain.example/api/v1/data/cid1"]);
    let ctrl = NullCtrl;
    let sink = NullSink;

    let datasets = input::materialize_datasets(
        &TaskCtx {
            lease: &lease,
            input: &input,
            output: &sink,
            ctrl: &ctrl,
            access_token: &TokenStub,
        },
        &workspace,
    )
    .await
    .unwrap();

    assert_eq!(datasets.len(), 1);
    let ds = &datasets[0];
    assert_eq!(ds.data_id.as_deref(), Some("data-scan_a"));
    assert!(ds.dataset_dir.exists());
    assert!(ds
        .manifest_path
        .as_ref()
        .is_some_and(|p| p.ends_with("Manifest.json")));
    assert!(ds.dataset_dir.starts_with(workspace.datasets()));
    assert!(ds.manifest_path.as_ref().unwrap().exists());
}

struct TokenStub;
impl compute_runner_api::runner::AccessTokenProvider for TokenStub {
    fn get(&self) -> String {
        "token".into()
    }
}

fn lease_with_inputs(inputs: Vec<&str>) -> LeaseEnvelope {
    LeaseEnvelope {
        access_token: Some("token".into()),
        access_token_expires_at: Some(Utc::now()),
        lease_expires_at: Some(Utc::now()),
        cancel: false,
        status: Some("leased".into()),
        domain_id: Some(Uuid::new_v4()),
        domain_server_url: Some("https://domain.example".parse().unwrap()),
        task: TaskSpec {
            id: Uuid::new_v4(),
            job_id: Some(Uuid::new_v4()),
            capability: "/reconstruction/legacy/v1".into(),
            capability_filters: json!({}),
            inputs_cids: inputs.into_iter().map(|s| s.to_string()).collect(),
            outputs_prefix: None,
            label: None,
            stage: None,
            meta: json!({}),
            priority: None,
            attempts: None,
            max_attempts: None,
            deps_remaining: None,
            status: Some("leased".into()),
            mode: None,
            organization_filter: None,
            billing_units: None,
            estimated_credit_cost: None,
            debited_amount: None,
            debited_at: None,
            lease_expires_at: None,
        },
    }
}
