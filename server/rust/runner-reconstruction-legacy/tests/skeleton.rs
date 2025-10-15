use compute_runner_api::{
    ArtifactSink, ControlPlane, InputSource, MaterializedInput, Runner, TaskCtx,
};
use runner_reconstruction_legacy::{RunnerConfig, RunnerReconstructionLegacy};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

struct MemInput {
    calls: Arc<Mutex<Vec<String>>>,
    responses: Arc<Mutex<Vec<MaterializedInput>>>,
}
#[async_trait::async_trait]
impl InputSource for MemInput {
    async fn get_bytes_by_cid(&self, cid: &str) -> anyhow::Result<Vec<u8>> {
        self.calls.lock().unwrap().push(cid.to_string());
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

#[derive(Default)]
struct MemSink {
    paths: Arc<Mutex<Vec<String>>>,
}
#[async_trait::async_trait]
impl ArtifactSink for MemSink {
    async fn put_bytes(&self, rel_path: &str, _bytes: &[u8]) -> anyhow::Result<()> {
        self.paths.lock().unwrap().push(rel_path.to_string());
        Ok(())
    }
    async fn put_file(&self, rel_path: &str, _file_path: &std::path::Path) -> anyhow::Result<()> {
        self.paths.lock().unwrap().push(rel_path.to_string());
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

fn lease_with_inputs(inputs: Vec<&str>) -> compute_runner_api::LeaseEnvelope {
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;
    compute_runner_api::LeaseEnvelope {
        access_token: Some("t".into()),
        access_token_expires_at: Some(Utc::now()),
        lease_expires_at: Some(Utc::now()),
        cancel: false,
        status: Some("leased".into()),
        domain_id: Some(Uuid::new_v4()),
        domain_server_url: Some("https://domain.example".parse().unwrap()),
        task: compute_runner_api::TaskSpec {
            id: Uuid::new_v4(),
            job_id: Some(Uuid::new_v4()),
            capability: "/reconstruction/legacy/v1".into(),
            capability_filters: json!({}),
            inputs_cids: inputs.into_iter().map(|s| s.to_string()).collect(),
            outputs_prefix: Some("out".into()),
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

#[test]
fn capability_string_matches() {
    assert_eq!(
        RunnerReconstructionLegacy::default().capability(),
        RunnerReconstructionLegacy::CAPABILITY_LOCAL_AND_GLOBAL
    );

    let local_runner = RunnerReconstructionLegacy::with_capability(
        RunnerReconstructionLegacy::CAPABILITY_LOCAL_ONLY,
        RunnerConfig::default(),
    );
    assert_eq!(
        local_runner.capability(),
        RunnerReconstructionLegacy::CAPABILITY_LOCAL_ONLY
    );

    let global_runner = RunnerReconstructionLegacy::with_capability(
        RunnerReconstructionLegacy::CAPABILITY_GLOBAL_ONLY,
        RunnerConfig::default(),
    );
    assert_eq!(
        global_runner.capability(),
        RunnerReconstructionLegacy::CAPABILITY_GLOBAL_ONLY
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runner_reads_inputs_and_writes_placeholders() {
    let lease = lease_with_inputs(vec!["c1", "c2"]);
    let mat_a = make_materialized_input("scan_a");
    let mat_b = make_materialized_input("scan_b");
    let input = MemInput {
        calls: Arc::new(Mutex::new(Vec::new())),
        responses: Arc::new(Mutex::new(vec![mat_b, mat_a])),
    };
    let sink = MemSink::default();
    let ctrl = NullCtrl;

    let runner = RunnerReconstructionLegacy::new(RunnerConfig {
        workspace_root: None,
        python_bin: PathBuf::from("python3"),
        python_script: PathBuf::from("python3"),
        python_args: vec![],
        cpu_workers: 2,
        mock_mode: true,
    });

    runner
        .run(TaskCtx {
            lease: &lease,
            input: &input,
            output: &sink,
            ctrl: &ctrl,
        })
        .await
        .unwrap();

    let paths = sink.paths.lock().unwrap().clone();
    assert!(paths
        .iter()
        .any(|p| p == "refined/global/refined_manifest.json"));
    assert!(paths.iter().any(|p| p.ends_with("RefinedScan.zip")));
}

fn make_materialized_input(folder: &str) -> MaterializedInput {
    let root = std::env::temp_dir().join(format!("runner-mat-{}", uuid::Uuid::new_v4()));
    let dataset_root = root.join("datasets").join(folder);
    std::fs::create_dir_all(&dataset_root).unwrap();
    std::fs::write(dataset_root.join("Manifest.json"), b"{}\n").unwrap();

    MaterializedInput {
        cid: folder.to_string(),
        path: dataset_root.join("dummy.bin"),
        data_id: Some(format!("data-{}", folder)),
        name: Some(folder.to_string()),
        data_type: Some("refined_scan_zip".into()),
        domain_id: Some("dom1".into()),
        root_dir: root,
        related_files: vec![],
        extracted_paths: vec![],
    }
}
