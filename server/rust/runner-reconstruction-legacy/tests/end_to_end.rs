use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::Utc;
use compute_runner_api::{
    ArtifactSink, ControlPlane, InputSource, LeaseEnvelope, MaterializedInput, Runner, TaskCtx,
    TaskSpec,
};
use runner_reconstruction_legacy::{RunnerConfig, RunnerReconstructionLegacy};
use serde_json::json;
use tempfile::tempdir;
use tokio::runtime::Runtime;
use uuid::Uuid;

struct StubInput {
    responses: Arc<Mutex<Vec<MaterializedInput>>>,
}

#[async_trait]
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

#[derive(Clone, Debug)]
struct UploadRecord {
    path: String,
}

#[derive(Default, Clone)]
struct RecordingSink {
    uploads: Arc<Mutex<Vec<UploadRecord>>>,
}

impl RecordingSink {
    fn paths(&self) -> Vec<String> {
        self.uploads
            .lock()
            .unwrap()
            .iter()
            .map(|record| record.path.clone())
            .collect()
    }
}

#[async_trait]
impl ArtifactSink for RecordingSink {
    async fn put_bytes(&self, rel_path: &str, _bytes: &[u8]) -> anyhow::Result<()> {
        self.uploads.lock().unwrap().push(UploadRecord {
            path: rel_path.to_string(),
        });
        Ok(())
    }

    async fn put_file(&self, rel_path: &str, file_path: &std::path::Path) -> anyhow::Result<()> {
        let bytes = std::fs::read(file_path)?;
        self.put_bytes(rel_path, &bytes).await
    }
}

#[derive(Default, Clone)]
struct RecordingCtrl {
    events: Arc<Mutex<Vec<serde_json::Value>>>,
    cancel_after: Option<usize>,
    calls: Arc<Mutex<usize>>,
}

#[async_trait]
impl ControlPlane for RecordingCtrl {
    async fn is_cancelled(&self) -> bool {
        let mut calls = self.calls.lock().unwrap();
        *calls += 1;
        if let Some(limit) = self.cancel_after {
            *calls >= limit
        } else {
            false
        }
    }

    async fn progress(&self, value: serde_json::Value) -> anyhow::Result<()> {
        self.events.lock().unwrap().push(value);
        Ok(())
    }

    async fn log_event(&self, _fields: serde_json::Value) -> anyhow::Result<()> {
        Ok(())
    }
}

fn lease_with_inputs(inputs: Vec<&str>) -> LeaseEnvelope {
    lease_with_inputs_and_meta(inputs, json!({}))
}

fn lease_with_inputs_and_meta(inputs: Vec<&str>, meta: serde_json::Value) -> LeaseEnvelope {
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
            meta,
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

fn make_materialized_input(root: &Path, folder: &str) -> MaterializedInput {
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
        root_dir: root.to_path_buf(),
        related_files: vec![],
        extracted_paths: vec![],
    }
}

fn make_materialized_input_with_manifest(
    root: &Path,
    folder: &str,
    manifest: serde_json::Value,
) -> MaterializedInput {
    let materialized = make_materialized_input(root, folder);
    let manifest_path = root.join("datasets").join(folder).join("Manifest.json");
    std::fs::write(manifest_path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    materialized
}

#[test]
fn run_success_path_mock_mode() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp = tempdir().unwrap();
        let materialized = make_materialized_input(temp.path(), "scan_a");
        let input = StubInput {
            responses: Arc::new(Mutex::new(vec![materialized])),
        };
        let sink = RecordingSink::default();
        let ctrl = RecordingCtrl::default();
        let lease = lease_with_inputs(vec!["cid1"]);

        let config = RunnerConfig {
            workspace_root: None,
            python_bin: PathBuf::from("python3"),
            python_script: PathBuf::from("python3"),
            python_args: vec![],
            cpu_workers: 2,
            mock_mode: true,
        };
        let runner = RunnerReconstructionLegacy::new(config);

        runner
            .run(TaskCtx {
                lease: &lease,
                input: &input,
                output: &sink,
                ctrl: &ctrl,
            })
            .await
            .expect("runner success");

        let uploads = sink.paths();
        assert!(uploads.contains(&"refined/global/refined_manifest.json".into()));
        assert!(uploads.contains(&"result.json".into()));
        assert!(uploads.iter().any(|u| u.contains("RefinedScan.zip")));

        let events = ctrl.events.lock().unwrap();
        assert!(events.iter().any(|e| e["progress"].as_i64() == Some(100)));
    });
}

#[test]
fn manifest_uploaded_when_not_skipped() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp = tempdir().unwrap();
        let materialized = make_materialized_input(temp.path(), "scan_manifest");
        let input = StubInput {
            responses: Arc::new(Mutex::new(vec![materialized])),
        };
        let sink = RecordingSink::default();
        let ctrl = RecordingCtrl::default();

        let meta = json!({
            "legacy": {
                "domain_server_url": "https://domain.example",
                "skip_manifest_upload": false,
                "override_job_name": "custom-job",
                "override_manifest_id": "existing-id",
                "processing_type": "local_and_global_refinement",
                "inputs_cids": ["cid1"]
            }
        });
        let mut lease = lease_with_inputs_and_meta(vec!["cid1"], meta);
        let domain_id = Uuid::new_v4();
        lease.domain_id = Some(domain_id);
        let job_id = Uuid::new_v4();
        lease.task.job_id = Some(job_id);

        let config = RunnerConfig {
            workspace_root: Some(temp.path().to_path_buf()),
            python_bin: PathBuf::from("python3"),
            python_script: PathBuf::from("python3"),
            python_args: vec![],
            cpu_workers: 2,
            mock_mode: true,
        };
        let runner = RunnerReconstructionLegacy::new(config);

        runner
            .run(TaskCtx {
                lease: &lease,
                input: &input,
                output: &sink,
                ctrl: &ctrl,
            })
            .await
            .expect("runner success");

        let uploads = sink.paths();
        assert!(uploads.contains(&"job_manifest.json".into()));

        let metadata_path = temp
            .path()
            .join("jobs")
            .join(domain_id.to_string())
            .join(format!("job_{}", job_id))
            .join("job_metadata.json");
        match std::fs::read_dir(temp.path().join("jobs")) {
            Ok(entries) => eprintln!(
                "domain entries: {:?}",
                entries
                    .map(|entry| entry.unwrap().path())
                    .collect::<Vec<_>>()
            ),
            Err(err) => eprintln!("domain entries error: {err}"),
        }
        match std::fs::read_dir(temp.path().join("jobs").join(domain_id.to_string())) {
            Ok(entries) => eprintln!(
                "job entries: {:?}",
                entries
                    .map(|entry| entry.unwrap().path())
                    .collect::<Vec<_>>()
            ),
            Err(err) => eprintln!("job entries error: {err}"),
        }
        assert!(
            metadata_path.exists(),
            "metadata missing at {:?}",
            metadata_path
        );
        let metadata_bytes = std::fs::read(&metadata_path).unwrap();
        let metadata_json: serde_json::Value = serde_json::from_slice(&metadata_bytes).unwrap();
        assert_eq!(metadata_json["skip_manifest_upload"], false);
        assert_eq!(metadata_json["override_job_name"], "custom-job");
        assert_eq!(metadata_json["override_manifest_id"], "existing-id");
        assert_eq!(metadata_json["job_id"], job_id.to_string());
    });
}

#[test]
fn manifest_upload_skipped_when_requested() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp = tempdir().unwrap();
        let materialized = make_materialized_input(temp.path(), "scan_skip");
        let input = StubInput {
            responses: Arc::new(Mutex::new(vec![materialized])),
        };
        let sink = RecordingSink::default();
        let ctrl = RecordingCtrl::default();

        let meta = json!({
            "legacy": {
                "domain_server_url": "https://domain.example",
                "skip_manifest_upload": true,
                "processing_type": "local_and_global_refinement",
                "inputs_cids": ["cid1"]
            }
        });
        let mut lease = lease_with_inputs_and_meta(vec!["cid1"], meta);
        let domain_id = Uuid::new_v4();
        lease.domain_id = Some(domain_id);
        let job_id = Uuid::new_v4();
        lease.task.job_id = Some(job_id);

        let config = RunnerConfig {
            workspace_root: Some(temp.path().to_path_buf()),
            python_bin: PathBuf::from("python3"),
            python_script: PathBuf::from("python3"),
            python_args: vec![],
            cpu_workers: 2,
            mock_mode: true,
        };
        let runner = RunnerReconstructionLegacy::new(config);

        runner
            .run(TaskCtx {
                lease: &lease,
                input: &input,
                output: &sink,
                ctrl: &ctrl,
            })
            .await
            .expect("runner success");

        let uploads = sink.paths();
        assert!(!uploads.contains(&"job_manifest.json".into()));

        let metadata_path = temp
            .path()
            .join("jobs")
            .join(domain_id.to_string())
            .join(format!("job_{}", job_id))
            .join("job_metadata.json");
        match std::fs::read_dir(temp.path().join("jobs")) {
            Ok(entries) => eprintln!(
                "domain entries: {:?}",
                entries
                    .map(|entry| entry.unwrap().path())
                    .collect::<Vec<_>>()
            ),
            Err(err) => eprintln!("domain entries error: {err}"),
        }
        match std::fs::read_dir(temp.path().join("jobs").join(domain_id.to_string())) {
            Ok(entries) => eprintln!(
                "job entries: {:?}",
                entries
                    .map(|entry| entry.unwrap().path())
                    .collect::<Vec<_>>()
            ),
            Err(err) => eprintln!("job entries error: {err}"),
        }
        assert!(
            metadata_path.exists(),
            "metadata missing at {:?}",
            metadata_path
        );
        let metadata_bytes = std::fs::read(&metadata_path).unwrap();
        let metadata_json: serde_json::Value = serde_json::from_slice(&metadata_bytes).unwrap();
        assert_eq!(metadata_json["skip_manifest_upload"], true);
    });
}

#[test]
fn scan_summary_generated_and_uploaded_for_local_processing() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp = tempdir().unwrap();
        let manifest = json!({
            "frameCount": 30,
            "duration": 3.0,
            "portals": [
                {"shortId": "portal_a", "physicalSize": 1.5},
                {"shortId": "portal_b", "physicalSize": 2.5},
                {"shortId": "portal_a", "physicalSize": 1.5}
            ],
            "brand": "Apple",
            "model": "iPhone",
            "systemName": "iOS",
            "systemVersion": "16.0",
            "appVersion": "1.2.3",
            "buildId": "42"
        });
        let materialized =
            make_materialized_input_with_manifest(temp.path(), "scan_summary", manifest);
        let input = StubInput {
            responses: Arc::new(Mutex::new(vec![materialized])),
        };
        let sink = RecordingSink::default();
        let ctrl = RecordingCtrl::default();

        let meta = json!({
            "legacy": {
                "domain_server_url": "https://domain.example",
                "processing_type": "local_refinement",
                "inputs_cids": ["cid1"]
            }
        });
        let mut lease = lease_with_inputs_and_meta(vec!["cid1"], meta);
        let domain_id = Uuid::new_v4();
        lease.domain_id = Some(domain_id);
        let job_id = Uuid::new_v4();
        lease.task.job_id = Some(job_id);

        let config = RunnerConfig {
            workspace_root: Some(temp.path().to_path_buf()),
            python_bin: PathBuf::from("python3"),
            python_script: PathBuf::from("python3"),
            python_args: vec![],
            cpu_workers: 2,
            mock_mode: true,
        };
        let runner = RunnerReconstructionLegacy::new(config);

        runner
            .run(TaskCtx {
                lease: &lease,
                input: &input,
                output: &sink,
                ctrl: &ctrl,
            })
            .await
            .expect("runner success");

        let uploads = sink.paths();
        assert!(
            uploads.contains(&"scan_data_summary.json".into()),
            "scan summary should be uploaded"
        );

        let summary_path = temp
            .path()
            .join("jobs")
            .join(domain_id.to_string())
            .join(format!("job_{}", job_id))
            .join("scan_data_summary.json");
        assert!(summary_path.exists(), "scan summary file missing");
        let bytes = fs::read(&summary_path).unwrap();
        let summary: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(summary["scanCount"].as_i64(), Some(1));
        assert_eq!(summary["totalFrameCount"].as_i64(), Some(30));
        assert!((summary["totalScanDuration"].as_f64().unwrap() - 3.0).abs() < 1e-6);
        assert_eq!(summary["portalCount"].as_i64(), Some(2));
        let devices = summary["deviceVersionsUsed"].as_array().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0], json!("Apple iPhone iOS 16.0"));
        let app_versions = summary["appVersionsUsed"].as_array().unwrap();
        assert_eq!(app_versions.len(), 1);
        assert_eq!(app_versions[0], json!("1.2.3 (build 42)"));
        let portal_ids = summary["portalIDs"].as_array().unwrap();
        assert_eq!(portal_ids, &vec![json!("portal_a"), json!("portal_b")]);
    });
}

#[test]
fn scan_summary_skipped_for_global_processing() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp = tempdir().unwrap();
        let manifest = json!({
            "frameCount": 15,
            "duration": 1.5
        });
        let materialized =
            make_materialized_input_with_manifest(temp.path(), "scan_global", manifest);
        let input = StubInput {
            responses: Arc::new(Mutex::new(vec![materialized])),
        };
        let sink = RecordingSink::default();
        let ctrl = RecordingCtrl::default();

        let meta = json!({
            "legacy": {
                "domain_server_url": "https://domain.example",
                "processing_type": "global_refinement",
                "inputs_cids": ["cid1"]
            }
        });
        let mut lease = lease_with_inputs_and_meta(vec!["cid1"], meta);
        let domain_id = Uuid::new_v4();
        lease.domain_id = Some(domain_id);
        let job_id = Uuid::new_v4();
        lease.task.job_id = Some(job_id);

        let config = RunnerConfig {
            workspace_root: Some(temp.path().to_path_buf()),
            python_bin: PathBuf::from("python3"),
            python_script: PathBuf::from("python3"),
            python_args: vec![],
            cpu_workers: 2,
            mock_mode: true,
        };
        let runner = RunnerReconstructionLegacy::new(config);

        runner
            .run(TaskCtx {
                lease: &lease,
                input: &input,
                output: &sink,
                ctrl: &ctrl,
            })
            .await
            .expect("runner success");

        let uploads = sink.paths();
        assert!(
            !uploads.contains(&"scan_data_summary.json".into()),
            "scan summary should not be uploaded for global refinement"
        );

        let summary_path = temp
            .path()
            .join("jobs")
            .join(domain_id.to_string())
            .join(format!("job_{}", job_id))
            .join("scan_data_summary.json");
        assert!(
            !summary_path.exists(),
            "scan summary file should not be created"
        );
    });
}

#[test]
fn run_handles_cancellation() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp = tempdir().unwrap();
        let materialized = make_materialized_input(temp.path(), "scan_a");
        let input = StubInput {
            responses: Arc::new(Mutex::new(vec![materialized])),
        };
        let sink = RecordingSink::default();
        let ctrl = RecordingCtrl {
            cancel_after: Some(1),
            ..Default::default()
        };
        let lease = lease_with_inputs(vec!["cid1"]);

        let config = RunnerConfig {
            workspace_root: None,
            python_bin: PathBuf::from("python3"),
            python_script: PathBuf::from("python3"),
            python_args: vec![],
            cpu_workers: 2,
            mock_mode: true,
        };
        let runner = RunnerReconstructionLegacy::new(config);

        let err = runner
            .run(TaskCtx {
                lease: &lease,
                input: &input,
                output: &sink,
                ctrl: &ctrl,
            })
            .await
            .expect_err("cancellation should surface");
        assert!(err.to_string().contains("task cancelled"));
    });
}

#[test]
fn run_surfaces_python_failure() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let temp = tempdir().unwrap();
        let materialized = make_materialized_input(temp.path(), "scan_a");
        let input = StubInput {
            responses: Arc::new(Mutex::new(vec![materialized])),
        };
        let sink = RecordingSink::default();
        let ctrl = RecordingCtrl::default();
        let lease = lease_with_inputs(vec!["cid1"]);

        let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../scripts/mock_py_runner.py");
        let config = RunnerConfig {
            workspace_root: None,
            python_bin: PathBuf::from("python3"),
            python_script: script,
            python_args: vec!["exit".into(), "1".into()],
            cpu_workers: 2,
            mock_mode: false,
        };
        let runner = RunnerReconstructionLegacy::new(config);

        let err = runner
            .run(TaskCtx {
                lease: &lease,
                input: &input,
                output: &sink,
                ctrl: &ctrl,
            })
            .await
            .expect_err("python failure should surface");
        let msg = err.to_string();
        assert!(
            msg.contains("python execution failed") || msg.contains("python failed"),
            "unexpected error message: {}",
            msg
        );
    });
}
