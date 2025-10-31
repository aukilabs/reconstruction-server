use compute_runner_api::{ArtifactSink, ControlPlane, InputSource, Runner, TaskCtx};
use runner_reconstruction_legacy_noop::RunnerReconstructionLegacyNoop;
use std::sync::{Arc, Mutex};
use tokio::time::{Duration, Instant};

struct MemInput;
#[async_trait::async_trait]
impl InputSource for MemInput {
    async fn get_bytes_by_cid(&self, _cid: &str) -> anyhow::Result<Vec<u8>> {
        Ok(vec![])
    }
    async fn materialize_cid_to_temp(&self, _cid: &str) -> anyhow::Result<std::path::PathBuf> {
        Ok(std::env::temp_dir())
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
            capability: runner_reconstruction_legacy_noop::CAPABILITY.into(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_writes_ack_and_sleeps() {
    let lease = lease_with_inputs(vec!["cid1"]);
    let input = MemInput;
    let sink = MemSink::default();
    let ctrl = NullCtrl;
    let runner = RunnerReconstructionLegacyNoop::new(1);

    let start = Instant::now();
    runner
        .run(TaskCtx {
            lease: &lease,
            input: &input,
            output: &sink,
            ctrl: &ctrl,
            access_token: &TokenStub,
        })
        .await
        .unwrap();
    let elapsed = start.elapsed();

    let paths = sink.paths.lock().unwrap().clone();
    let actual: std::collections::HashSet<_> = paths.into_iter().collect();
    let expected: std::collections::HashSet<_> = [
        "job_manifest.json",
        "refined/global/refined_manifest.json",
        "refined/global/RefinedPointCloudReduced.ply",
        "outputs_index.json",
        "result.json",
        "scan_data_summary.json",
    ]
    .into_iter()
    .map(|s| s.to_string())
    .collect();
    assert_eq!(actual, expected);
    assert!(
        elapsed >= Duration::from_millis(900),
        "elapsed: {:?}",
        elapsed
    );
}

struct TokenStub;
impl compute_runner_api::runner::AccessTokenProvider for TokenStub {
    fn get(&self) -> String {
        "t".into()
    }
}
