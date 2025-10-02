use std::{fs, path::PathBuf, process::Command};

use serde_json::json;
use tempfile::TempDir;
use walkdir::WalkDir;

fn make_job_request(data_dir: &PathBuf) -> PathBuf {
    let request = json!({
        "data_ids": [],
        "domain_id": "dom-123",
        "access_token": "token",
        "processing_type": "local_refinement",
        "domain_server_url": "http://example",
        "skip_manifest_upload": true,
        "override_job_name": "",
        "override_manifest_id": "",
    });
    fs::create_dir_all(data_dir).unwrap();
    let request_path = data_dir.join("job_request.json");
    fs::write(&request_path, request.to_string()).unwrap();
    request_path
}

#[test]
fn cli_job_request_path_produces_artifacts() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let request_path = make_job_request(&data_dir);

    let status = Command::new(env!("CARGO_BIN_EXE_server-bin"))
        .arg("--job-request")
        .arg(&request_path)
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--mock-python")
        .arg("--job-manifest-interval-ms")
        .arg("10")
        .status()
        .expect("failed to execute server-bin binary");

    assert!(status.success(), "CLI exited with non-zero status");

    let dom_path = data_dir.join("dom-123");
    let job_dir = fs::read_dir(&dom_path)
        .expect("domain directory")
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|entry| entry.path())
        .expect("job directory not created");

    let manifest_path = job_dir.join("job_manifest.json");
    assert!(manifest_path.exists(), "expected job manifest to exist");

    let has_files = WalkDir::new(&job_dir)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .any(|entry| entry.file_type().is_file());
    assert!(has_files, "expected artifacts to be written");
}
