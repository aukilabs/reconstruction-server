use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde_json::Value as JsonValue;
use tokio::task;

/// Generate a scan data summary JSON from dataset manifests.
/// Returns `Ok(true)` when a summary file was written, `Ok(false)` otherwise.
pub async fn write_scan_data_summary(datasets_root: &Path, out_path: &Path) -> Result<bool> {
    let datasets_root = datasets_root.to_path_buf();
    let out_path = out_path.to_path_buf();
    task::spawn_blocking(move || summary_sync(&datasets_root, &out_path))
        .await
        .map_err(|e| anyhow::anyhow!("scan summary task join failure: {e}"))?
}

fn summary_sync(datasets_root: &PathBuf, out_path: &PathBuf) -> Result<bool> {
    let entries = match fs::read_dir(datasets_root) {
        Ok(entries) => entries,
        Err(_) => return Ok(false),
    };

    let mut scan_count = 0_i32;
    let mut total_frame_count = 0_i32;
    let mut total_scan_duration = 0.0_f64;
    let mut scan_durations = Vec::new();
    let mut portal_ids: Vec<String> = Vec::new();
    let mut portal_sizes = Vec::new();
    let mut devices_used = Vec::new();
    let mut app_versions_used = Vec::new();

    for entry in entries.flatten() {
        if !is_directory(&entry) {
            continue;
        }
        let manifest_path = entry.path().join("Manifest.json");
        let manifest_bytes = match fs::read(&manifest_path) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let manifest: JsonValue = match serde_json::from_slice(&manifest_bytes) {
            Ok(value) => value,
            Err(_) => continue,
        };

        scan_count += 1;
        let frame_count = manifest
            .get("frameCount")
            .and_then(JsonValue::as_f64)
            .unwrap_or(0.0) as i32;
        let duration = manifest
            .get("duration")
            .and_then(JsonValue::as_f64)
            .unwrap_or(0.0);
        total_frame_count += frame_count;
        total_scan_duration += duration;
        scan_durations.push(duration);

        if let Some(portals) = manifest.get("portals").and_then(JsonValue::as_array) {
            for portal in portals {
                if let (Some(id), Some(size)) = (
                    portal.get("shortId").and_then(JsonValue::as_str),
                    portal.get("physicalSize").and_then(JsonValue::as_f64),
                ) {
                    if !portal_ids.iter().any(|existing| existing == id) {
                        portal_ids.push(id.to_string());
                        portal_sizes.push(size);
                    }
                }
            }
        }

        let device = format!(
            "{} {} {} {}",
            manifest
                .get("brand")
                .and_then(JsonValue::as_str)
                .unwrap_or_default(),
            manifest
                .get("model")
                .and_then(JsonValue::as_str)
                .unwrap_or_default(),
            manifest
                .get("systemName")
                .and_then(JsonValue::as_str)
                .unwrap_or_default(),
            manifest
                .get("systemVersion")
                .and_then(JsonValue::as_str)
                .unwrap_or_default()
        )
        .trim()
        .to_string();
        let device = if device.is_empty() {
            "unknown".to_string()
        } else {
            device
        };
        if !devices_used.iter().any(|d| d == &device) {
            devices_used.push(device);
        }

        let app_version = match (
            manifest.get("appVersion").and_then(JsonValue::as_str),
            manifest.get("buildId").and_then(JsonValue::as_str),
        ) {
            (Some(version), Some(build)) => format!("{version} (build {build})"),
            _ => "unknown".to_string(),
        };
        if !app_versions_used.iter().any(|v| v == &app_version) {
            app_versions_used.push(app_version);
        }
    }

    if scan_durations.is_empty() {
        return Ok(false);
    }

    scan_durations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let shortest = *scan_durations.first().unwrap_or(&0.0);
    let longest = *scan_durations.last().unwrap_or(&0.0);
    let median = scan_durations[scan_durations.len() / 2];
    let avg_duration = total_scan_duration / (scan_durations.len() as f64);
    let avg_frame_count = (total_frame_count as f64) / (scan_durations.len() as f64);
    let avg_fps = if total_scan_duration > 0.0 {
        (total_frame_count as f64) / total_scan_duration
    } else {
        0.0
    };

    let summary = serde_json::json!({
        "scanCount": scan_count,
        "totalFrameCount": total_frame_count,
        "totalScanDuration": total_scan_duration,
        "averageScanDuration": avg_duration,
        "averageScanFrameCount": avg_frame_count,
        "averageFrameRate": avg_fps,
        "shortestScanDuration": shortest,
        "longestScanDuration": longest,
        "medianScanDuration": median,
        "portalCount": portal_ids.len(),
        "portalIDs": portal_ids,
        "portalSizes": portal_sizes,
        "deviceVersionsUsed": devices_used,
        "appVersionsUsed": app_versions_used,
    });

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create summary directory {}", parent.display()))?;
    }
    fs::write(out_path, serde_json::to_vec_pretty(&summary)?)
        .with_context(|| format!("write scan summary {}", out_path.display()))?;
    Ok(true)
}

fn is_directory(entry: &fs::DirEntry) -> bool {
    entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn summary_written_when_manifest_present() {
        let tmp = tempfile::tempdir().unwrap();
        let dataset = tmp.path().join("datasets").join("scan_a");
        fs::create_dir_all(&dataset).unwrap();
        fs::write(
            dataset.join("Manifest.json"),
            serde_json::json!({
                "frameCount": 30,
                "duration": 3.0,
                "portals": [
                    {"shortId": "p1", "physicalSize": 1.2},
                    {"shortId": "p1", "physicalSize": 1.2}
                ],
                "brand": "Brand",
                "model": "Model",
                "systemName": "OS",
                "systemVersion": "1.0",
                "appVersion": "2.0",
                "buildId": "42"
            })
            .to_string(),
        )
        .unwrap();

        let out = tmp.path().join("scan_data_summary.json");
        let wrote = write_scan_data_summary(&tmp.path().join("datasets"), &out)
            .await
            .unwrap();
        assert!(wrote);
        assert!(out.exists());
    }

    #[tokio::test]
    async fn summary_skipped_when_no_manifests() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("scan_data_summary.json");
        let wrote = write_scan_data_summary(&tmp.path().join("datasets"), &out)
            .await
            .unwrap();
        assert!(!wrote);
        assert!(!out.exists());
    }
}
