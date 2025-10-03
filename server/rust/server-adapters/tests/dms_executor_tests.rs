use std::{
    collections::VecDeque,
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};

use rand::{rngs::StdRng, SeedableRng};
use serde_json::{json, Value};
use server_adapters::dms::{
    client::DmsClientError,
    executor::{ExecutorConfig, TaskExecutor},
    models::{LeaseRequest, LeaseResponse, TaskSummary},
    poller::{DmsApi, PollController, Poller},
    session::{CapabilitySelector, HeartbeatPolicy, SessionManager},
};
use server_core::{DomainError, DomainPort, ExpectedOutput, Job, Result as CoreResult, Services};
use tempfile::TempDir;

#[derive(Clone)]
struct MockClient {
    leases: Arc<Mutex<VecDeque<LeaseResponse>>>,
    complete_calls: CompleteCallLog,
    fail_calls: FailCallLog,
}

type CompleteCall = (String, Vec<String>, Option<Value>);
type CompleteCallLog = Arc<Mutex<Vec<CompleteCall>>>;
type FailCall = (String, String, Option<Value>);
type FailCallLog = Arc<Mutex<Vec<FailCall>>>;

impl MockClient {
    fn new(leases: Vec<LeaseResponse>) -> Self {
        Self {
            leases: Arc::new(Mutex::new(leases.into_iter().collect())),
            complete_calls: Arc::new(Mutex::new(Vec::new())),
            fail_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn complete_calls(&self) -> CompleteCallLog {
        Arc::clone(&self.complete_calls)
    }

    fn fail_calls(&self) -> FailCallLog {
        Arc::clone(&self.fail_calls)
    }
}

#[async_trait::async_trait]
impl DmsApi for MockClient {
    async fn lease_task(&self, _request: &LeaseRequest) -> Result<LeaseResponse, DmsClientError> {
        let mut leases = self.leases.lock().expect("leases mutex poisoned");
        Ok(leases.pop_front().unwrap_or_default())
    }

    async fn send_heartbeat(
        &self,
        _task_id: &str,
        _progress: Option<&Value>,
    ) -> Result<LeaseResponse, DmsClientError> {
        Ok(LeaseResponse::default())
    }

    async fn complete_task(
        &self,
        task_id: &str,
        outputs: &[String],
        meta: Option<&Value>,
    ) -> Result<(), DmsClientError> {
        self.complete_calls
            .lock()
            .expect("complete mutex poisoned")
            .push((task_id.to_string(), outputs.to_vec(), meta.cloned()));
        Ok(())
    }

    async fn fail_task(
        &self,
        task_id: &str,
        reason: &str,
        details: Option<&Value>,
    ) -> Result<(), DmsClientError> {
        self.fail_calls.lock().expect("fail mutex poisoned").push((
            task_id.to_string(),
            reason.to_string(),
            details.cloned(),
        ));
        Ok(())
    }
}

struct NoopDomain;

#[async_trait::async_trait]
impl DomainPort for NoopDomain {
    async fn upload_manifest(
        &self,
        _job: &mut Job,
        _manifest_path: &std::path::Path,
    ) -> CoreResult<()> {
        Ok(())
    }

    async fn upload_output(&self, _job: &Job, _output: &ExpectedOutput) -> CoreResult<()> {
        Ok(())
    }

    async fn download_data_batch(
        &self,
        _job: &Job,
        _ids: &[String],
        _datasets_root: &std::path::Path,
    ) -> CoreResult<()> {
        Ok(())
    }

    async fn upload_refined_scan_zip(
        &self,
        _job: &Job,
        _scan_id: &str,
        _zip_bytes: Vec<u8>,
    ) -> CoreResult<()> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct RunnerCall {
    capability: String,
    access_token: String,
}

struct RecordingRunner {
    calls: Arc<Mutex<Vec<RunnerCall>>>,
    fail: bool,
}

impl RecordingRunner {
    fn new(fail: bool) -> (Self, Arc<Mutex<Vec<RunnerCall>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                calls: Arc::clone(&calls),
                fail,
            },
            calls,
        )
    }
}

#[async_trait::async_trait]
impl server_core::JobRunner for RecordingRunner {
    async fn run_python(&self, job: &Job, capability: &str, _cpu_workers: usize) -> CoreResult<()> {
        self.calls
            .lock()
            .expect("runner mutex poisoned")
            .push(RunnerCall {
                capability: capability.to_string(),
                access_token: job.meta.access_token.clone(),
            });
        if self.fail {
            Err(DomainError::Internal("boom".into()))
        } else {
            let global_dir = job.job_path.join("refined").join("global");
            let topology_dir = global_dir.join("topology");
            fs::create_dir_all(&topology_dir).map_err(DomainError::Io)?;
            fs::write(global_dir.join("refined_manifest.json"), b"{}\n")
                .map_err(DomainError::Io)?;
            fs::write(global_dir.join("RefinedPointCloudReduced.ply"), b"ply")
                .map_err(DomainError::Io)?;
            fs::write(topology_dir.join("topology_downsampled_0.111.obj"), b"obj")
                .map_err(DomainError::Io)?;
            Ok(())
        }
    }
}

fn lease_response(capability: &str) -> LeaseResponse {
    LeaseResponse {
        task: Some(TaskSummary {
            id: "task-1".into(),
            capability: capability.into(),
            meta: json!({
                "legacy": {
                    "data_ids": ["scan-1"],
                    "domain_id": "domain-42",
                    "access_token": "placeholder",
                    "processing_type": "local_and_global_refinement",
                    "domain_server_url": "https://domain.example",
                    "skip_manifest_upload": false,
                    "override_job_name": "",
                    "override_manifest_id": "",
                }
            }),
        }),
        access_token: Some("lease-token".into()),
        ..LeaseResponse::default()
    }
}

struct Harness {
    executor: TaskExecutor<MockClient, StdRng>,
    client: MockClient,
    runner_calls: Arc<Mutex<Vec<RunnerCall>>>,
    _tempdir: TempDir,
}

fn build_harness(fail_runner: bool) -> Harness {
    let lease = lease_response("cap/refinement");
    let client = MockClient::new(vec![lease]);
    let client_handle = client.clone();

    let selector = CapabilitySelector::new(vec!["cap/refinement".to_string()]);
    let session = SessionManager::new(selector);
    let rng = StdRng::seed_from_u64(42);
    let controller = PollController::new(Duration::from_millis(10), Duration::from_millis(10), rng);
    let heartbeat = HeartbeatPolicy::default_policy();
    let poller = Poller::new(client, session, controller, heartbeat);

    let domain: Arc<dyn DomainPort + Send + Sync> = Arc::new(NoopDomain);
    let (runner_instance, runner_calls) = RecordingRunner::new(fail_runner);
    let runner: Arc<dyn server_core::JobRunner + Send + Sync> = Arc::new(runner_instance);
    let services = Arc::new(Services {
        domain,
        runner,
        manifest_interval: Duration::from_millis(25),
    });

    let tempdir = TempDir::new().expect("tempdir");
    let config = ExecutorConfig {
        data_dir: tempdir.path().to_path_buf(),
        reconstruction_url: "http://node".into(),
        cpu_workers: 2,
    };

    let executor = TaskExecutor::new(poller, services, config);

    Harness {
        executor,
        client: client_handle,
        runner_calls,
        _tempdir: tempdir,
    }
}

#[tokio::test]
async fn successful_job_triggers_completion() {
    let mut harness = build_harness(false);
    let outcome = harness.executor.step().await.expect("step ok");
    assert!(outcome.is_none());

    let completes = harness.client.complete_calls();
    let complete_guard = completes.lock().expect("complete guard");
    assert_eq!(complete_guard.len(), 1);
    let (task_id, outputs, meta) = &complete_guard[0];
    assert_eq!(task_id, "task-1");
    assert!(!outputs.is_empty());
    assert!(outputs.iter().any(|o| o == "refined_manifest"));
    let meta = meta.as_ref().expect("completion meta");
    assert_eq!(
        meta.get("status").and_then(|v| v.as_str()),
        Some("succeeded")
    );
    assert_eq!(
        meta.get("capability").and_then(|v| v.as_str()),
        Some("cap/refinement")
    );

    let runner_calls = harness.runner_calls.lock().expect("runner guard");
    assert_eq!(runner_calls.len(), 1);
    assert_eq!(runner_calls[0].capability, "cap/refinement");
    assert_eq!(runner_calls[0].access_token, "lease-token");

    assert!(harness
        .client
        .fail_calls()
        .lock()
        .expect("fail guard")
        .is_empty());
}

#[tokio::test]
async fn failing_runner_reports_failure() {
    let mut harness = build_harness(true);
    let outcome = harness.executor.step().await.expect("step ok");
    assert!(outcome.is_none());

    let fails = harness.client.fail_calls();
    let fail_guard = fails.lock().expect("fail guard");
    assert_eq!(fail_guard.len(), 1);
    let (task_id, reason, details) = &fail_guard[0];
    assert_eq!(task_id, "task-1");
    assert_eq!(reason, "JobFailed");
    let details = details.as_ref().expect("details");
    let error_msg = details
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert!(error_msg.contains("boom"));

    assert!(harness
        .client
        .complete_calls()
        .lock()
        .expect("complete guard")
        .is_empty());

    let runner_calls = harness.runner_calls.lock().expect("runner guard");
    assert_eq!(runner_calls.len(), 1);
    assert_eq!(runner_calls[0].access_token, "lease-token");
}
