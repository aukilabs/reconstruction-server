use posemesh_domain::{datastore::common::DomainError, protobuf::task::{Status, Task}};
use posemesh_networking::{client::TClient, libp2p::NetworkError};
use quick_protobuf::serialize_into_vec;
use tokio::{sync::watch, task::JoinHandle, time::interval};
use std::{collections::HashSet, error::Error, fs, path::Path, process::Stdio, time::Duration};
use serde_json::{json, Value};
use tokio::{io::{AsyncBufReadExt, BufReader as TokioBufReader}, process::Command};

pub fn write_scan_data_summary(
    scan_folder:&Path,
    summary_json_path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut scan_count = 0;
    let mut total_frame_count = 0;
    let mut total_scan_duration = 0.0;
    let mut scan_durations = Vec::new();
    let mut unique_portal_ids = HashSet::new();
    let mut portal_sizes = Vec::new();
    let mut devices_used = HashSet::new();
    let mut app_versions_used = HashSet::new();
    
    let manifest_path = scan_folder.join("Manifest.json");
    if !manifest_path.exists() {
        return Err("Manifest.json not found".into());
    }

    let manifest_data = fs::read_to_string(&manifest_path)?;
    let manifest: Value = serde_json::from_str(&manifest_data)?;

    scan_count += 1;

    let frame_count = manifest["frameCount"].as_f64().unwrap_or(0.0) as i32;
    let duration = manifest["duration"].as_f64().unwrap_or(0.0);
    total_frame_count += frame_count;
    total_scan_duration += duration;
    scan_durations.push(duration);

    if let Some(portals) = manifest.get("portals").and_then(|p| p.as_array()) {
        for portal in portals {
            if let Some(portal_map) = portal.as_object() {
                if let Some(portal_id) = portal_map.get("shortId").and_then(|id| id.as_str()) {
                    if unique_portal_ids.insert(portal_id.to_string()) {
                        if let Some(size) = portal_map.get("physicalSize").and_then(|s| s.as_f64()) {
                            portal_sizes.push(size);
                        }
                    }
                }
            }
        }
    }

    let device = if let (Some(brand), Some(model), Some(system_name), Some(system_version)) = (
        manifest.get("brand").and_then(|b| b.as_str()),
        manifest.get("model").and_then(|m| m.as_str()),
        manifest.get("systemName").and_then(|s| s.as_str()),
        manifest.get("systemVersion").and_then(|v| v.as_str()),
    ) {
        format!("{} {} {} {}", brand, model, system_name, system_version)
    } else {
        "unknown".to_string()
    };
    devices_used.insert(device);

    let app_version = if let (Some(version), Some(build_id)) = (
        manifest.get("appVersion").and_then(|v| v.as_str()),
        manifest.get("buildId").and_then(|b| b.as_str()),
    ) {
        format!("{} (build {})", version, build_id)
    } else {
        "unknown".to_string()
    };
    app_versions_used.insert(app_version);
    

    scan_durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let shortest_scan_duration = scan_durations.first().unwrap_or(&0.0);
    let longest_scan_duration = scan_durations.last().unwrap_or(&0.0);
    let median_scan_duration = scan_durations.get(scan_durations.len() / 2).unwrap_or(&0.0);

    let average_scan_duration = total_scan_duration / scan_count as f64;
    let average_scan_frame_count = total_frame_count as f64 / scan_count as f64;
    let average_scan_frame_rate = total_frame_count as f64 / total_scan_duration;

    let summary = json!({
        "scanCount": scan_count,
        "totalFrameCount": total_frame_count,
        "totalScanDuration": total_scan_duration,
        "averageScanDuration": average_scan_duration,
        "averageScanFrameCount": average_scan_frame_count,
        "averageFrameRate": average_scan_frame_rate,
        "shortestScanDuration": shortest_scan_duration,
        "longestScanDuration": longest_scan_duration,
        "medianScanDuration": median_scan_duration,
        "portalCount": unique_portal_ids.len(),
        "portalIDs": unique_portal_ids,
        "portalSizes": portal_sizes,
        "deviceVersionsUsed": devices_used,
        "appVersionsUsed": app_versions_used,
    });

    fs::write(
        summary_json_path,
        serde_json::to_string_pretty(&summary)?,
    )?;

    Ok(())
} 

pub async fn health<C: TClient + Send + Sync + 'static>(task: Task, mut c: C, job_id: &str, rx: watch::Receiver<bool>) -> JoinHandle<()> {
    let job_id_clone = job_id.to_string();
    tokio::spawn(async move {
        let mut rx = rx.clone();
        let mut interval = interval(Duration::from_secs(30)); // Send heartbeat every 30 seconds
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut progress_task = task.clone();
                    progress_task.status = Status::PROCESSING;
                    if let Ok(message) = serialize_into_vec(&progress_task) {
                        if let Err(e) = c.publish(job_id_clone.clone(), message).await {
                            tracing::error!("Error publishing heartbeat: {}", e);
                            continue;
                        }
                    } else {
                        tracing::error!("Failed to serialize heartbeat");
                    }
                }
                // Check if we should stop
                Ok(_) = rx.changed() => {
                    break;
                }
            }
        }
    })
}

pub async fn execute_python(params: Vec<&str>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut child = Command::new("python3")
        .args(params)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Read stdout in real-time
    if let Some(stdout) = child.stdout.take() {
        let stdout_reader = TokioBufReader::new(stdout);
        tokio::spawn(async move {
            let mut lines = stdout_reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                println!("stdout: {}", line);
            }
        });
    }

    // Read stderr in real-time
    if let Some(stderr) = child.stderr.take() {
        let stderr_reader = TokioBufReader::new(stderr);
        tokio::spawn(async move {
            let mut lines = stderr_reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                println!("stderr: {}", line);
            }
        });
    }

    // Wait for the process to complete
    let status = child.wait().await?;
    if !status.success() {
        tracing::info!("Python process failed");
        return Err(format!("Python process failed with status {}", status).into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use posemesh_networking::client::TClient;
    use posemesh_networking::libp2p::NetworkError;
    use tokio::sync::watch;
    use tokio::time::{sleep, Duration};
    use std::sync::{Arc, Mutex};

    // Mock Client to capture published messages
    #[derive(Clone)]
    struct MockClient {
        published_messages: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl MockClient {
        fn new() -> Self {
            MockClient {
                published_messages: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_published_messages(&self) -> Vec<Vec<u8>> {
            self.published_messages.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl TClient for MockClient {
        async fn publish(&mut self, _job_id: String, message: Vec<u8>) -> Result<(), NetworkError> {
            self.published_messages.lock().unwrap().push(message);
            Ok(())
        }
        async fn subscribe(&mut self, _job_id: String) -> Result<(), NetworkError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_health_sends_heartbeat() {
        let task = Task {
            status: Status::PROCESSING,
            ..Default::default()
        };
        let client = MockClient::new();
        let (tx, rx) = watch::channel(false);
        // Start the health task
        let heartbeat_handle = health(task, client.clone(), "job_id", rx).await;

        // Allow some time for heartbeats to be sent
        sleep(Duration::from_secs(32)).await;

        // Check that heartbeats were sent
        let messages = client.get_published_messages();
        assert!(messages.len() >= 1, "Expected at least 1 heartbeat, got {}", messages.len());

        // Stop the heartbeat task
        tx.send(true).unwrap();
        let _ = heartbeat_handle.await;
    }

    #[tokio::test]
    async fn test_execute_python_should_fail_when_python_script_does_not_exist() {
        async fn run() -> Result<(), Box<dyn core::error::Error + Send + Sync>> {
            let params = vec!["non_existent_script.py"];
            execute_python(params).await?;
            Ok(())
        }

        let res = run().await;
        assert!(res.is_err(), "Expected error when script does not exist");
    }
}
