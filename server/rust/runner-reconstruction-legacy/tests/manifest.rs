use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use runner_reconstruction_legacy::manifest::{
    spawn_python_processing_writer, write_failed_manifest, ManifestState, ProgressListener,
};
use std::path::PathBuf;
use tempfile::tempdir;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

#[derive(Default, Clone)]
struct RecordingListener {
    events: Arc<Mutex<Vec<(i32, String)>>>,
}

#[async_trait::async_trait]
impl ProgressListener for RecordingListener {
    async fn report_progress(&self, progress: i32, status: String) -> anyhow::Result<()> {
        self.events.lock().unwrap().push((progress, status));
        Ok(())
    }
}

#[tokio::test]
async fn manifest_writer_updates_file_and_progress() {
    let tmp = tempdir().unwrap();
    let manifest_path = tmp.path().join("job_manifest.json");
    let state = ManifestState::new(0, "starting");
    let listener = RecordingListener::default();
    let cancel = CancellationToken::new();

    // Use the python-based writer (falls back to minimal JSON if python import fails)
    let task = spawn_python_processing_writer(
        manifest_path.clone(),
        tmp.path().to_path_buf(),
        PathBuf::from("python3"),
        Duration::from_millis(50),
        state.clone(),
        Arc::new(listener.clone()),
        cancel.clone(),
    );

    // Allow initial snapshot, then update progress and wait for propagation.
    sleep(Duration::from_millis(60)).await;
    state.update(42, "halfway");

    // Wait up to ~1s for the writer tick to apply the update (robust to CI jitter).
    let mut attempts = 0;
    loop {
        // Check listener first (should be reported prior to write).
        let events = listener.events.lock().unwrap().clone();
        if events.iter().any(|(p, _)| *p == 42) {
            break;
        }
        if attempts > 50 {
            break;
        }
        attempts += 1;
        sleep(Duration::from_millis(20)).await;
    }

    // Give the writer a bit more time to flush the manifest file, then stop.
    sleep(Duration::from_millis(60)).await;
    task.stop().await;

    // Validate the latest manifest reflects the updated progress and status.
    let contents = std::fs::read_to_string(&manifest_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&contents).unwrap();
    assert_eq!(json["jobStatus"], "processing");
    assert_eq!(json["jobStatusDetails"], "halfway");

    // Accept either string or number representation for robustness, but require 42.
    match &json["jobProgress"] {
        serde_json::Value::Number(n) => assert_eq!(n.as_i64().unwrap_or_default(), 42),
        serde_json::Value::String(s) => assert_eq!(s, "42"),
        other => panic!("unexpected jobProgress type: {other}"),
    }

    let events = listener.events.lock().unwrap().clone();
    assert!(
        events.iter().any(|(p, _)| *p == 42),
        "progress events should include 42"
    );
    assert!(!events.is_empty(), "expected at least one progress event");
}

#[tokio::test]
async fn write_failed_manifest_outputs_expected_json() {
    let tmp = tempdir().unwrap();
    let manifest_path = tmp.path().join("failed_manifest.json");

    write_failed_manifest(&manifest_path, "boom").await.unwrap();

    let contents = std::fs::read_to_string(&manifest_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&contents).unwrap();
    assert_eq!(json["jobStatus"], "failed");
    assert_eq!(json["jobStatusDetails"], "boom");
}
