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

    sleep(Duration::from_millis(80)).await;
    state.update(42, "halfway");
    sleep(Duration::from_millis(80)).await;

    task.stop().await;

    let contents = std::fs::read_to_string(&manifest_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&contents).unwrap();
    assert_eq!(json["jobStatus"], "processing");
    assert_eq!(json["jobProgress"], 42);
    assert_eq!(json["jobStatusDetails"], "halfway");

    let events = listener.events.lock().unwrap().clone();
    assert!(events.iter().any(|(p, _)| *p == 42));
    assert!(events.len() >= 2, "expected multiple progress events");
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
