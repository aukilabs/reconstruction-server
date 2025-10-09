use crate::{errors::*, types::*};
use base64::Engine;
use chrono::Utc;
use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};
use tokio::time::{sleep, Duration};
use tracing::{debug, info, warn};
use uuid::Uuid;

#[async_trait::async_trait]
pub trait DomainPort: Send + Sync {
    async fn upload_manifest(&self, job: &mut Job, manifest_path: &std::path::Path) -> Result<()>;
    async fn upload_output(&self, job: &Job, output: &ExpectedOutput) -> Result<()>;
    async fn upload_outputs(&self, job: &Job, outputs: &[ExpectedOutput]) -> Result<()> {
        let mut first_err: Option<DomainError> = None;
        for out in outputs {
            if let Err(e) = self.upload_output(job, out).await {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        if let Some(e) = first_err {
            Err(e)
        } else {
            Ok(())
        }
    }
    async fn download_data_batch(
        &self,
        job: &Job,
        ids: &[String],
        datasets_root: &std::path::Path,
    ) -> Result<()>;
    async fn download_data_by_uris(
        &self,
        job: &Job,
        uris: &[String],
        datasets_root: &std::path::Path,
    ) -> Result<()>;
    async fn upload_refined_scan_zip(
        &self,
        job: &Job,
        scan_id: &str,
        zip_bytes: Vec<u8>,
    ) -> Result<()>;
}

#[async_trait::async_trait]
pub trait JobRunner: Send + Sync {
    async fn run_python(&self, job: &Job, capability: &str, cpu_workers: usize) -> Result<()>;
}

pub struct Services {
    pub domain: Arc<dyn DomainPort + Send + Sync>,
    pub runner: Arc<dyn JobRunner + Send + Sync>,
    /// Interval for periodic manifest writer
    pub manifest_interval: std::time::Duration,
}

pub fn read_job_request_json(s: &str) -> Result<JobRequestData> {
    let req: JobRequestData = serde_json::from_str(s)?;
    Ok(req)
}

pub fn create_job_metadata(
    dir_path: &PathBuf,
    request_json: &str,
    reconstruction_server_url: &str,
    retrigger_job_id: Option<&str>,
) -> Result<Job> {
    fs::create_dir_all(dir_path).map_err(DomainError::Io)?;

    let mut req = read_job_request_json(request_json)?;
    // Fallback domain_server_url from JWT `iss` if not set
    if req.domain_server_url.as_deref().unwrap_or("").is_empty() {
        if let Some(iss) = decode_jwt_iss(&req.access_token) {
            req.domain_server_url = Some(iss);
        }
    }

    let start_time = Utc::now();
    let job_id = retrigger_job_id
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let job_name = format!("job_{}", job_id);

    let data_ids = if retrigger_job_id.is_some() {
        vec![job_id.clone()]
    } else {
        req.data_ids.clone()
    };

    let job_path = dir_path.join(&req.domain_id).join(&job_name);
    if retrigger_job_id.is_none() {
        fs::create_dir_all(&job_path).map_err(DomainError::Io)?;
        fs::write(job_path.join("job_request.json"), request_json).map_err(DomainError::Io)?;
        let meta = JobMetadata {
            created_at: start_time,
            id: job_id.clone(),
            name: job_name.clone(),
            domain_id: req.domain_id.clone(),
            data_ids: data_ids.clone(),
            processing_type: req.processing_type.clone(),
            domain_server_url: req.domain_server_url.clone().unwrap_or_default(),
            reconstruction_server_url: reconstruction_server_url.to_string(),
            access_token: req.access_token.clone(),
            skip_manifest_upload: req.skip_manifest_upload.unwrap_or(false),
            override_job_name: req.override_job_name.clone().unwrap_or_default(),
            override_manifest_id: req.override_manifest_id.clone().unwrap_or_default(),
            inputs_cids: req.inputs_cids.clone(),
        };
        let meta_json = serde_json::to_vec(&meta)?;
        fs::write(job_path.join("job_metadata.json"), meta_json).map_err(DomainError::Io)?;
    }

    let meta = JobMetadata {
        created_at: start_time,
        id: job_id.clone(),
        name: job_name.clone(),
        domain_id: req.domain_id.clone(),
        data_ids: data_ids.clone(),
        processing_type: req.processing_type.clone(),
        domain_server_url: req.domain_server_url.clone().unwrap_or_default(),
        reconstruction_server_url: reconstruction_server_url.to_string(),
        access_token: req.access_token.clone(),
        skip_manifest_upload: req.skip_manifest_upload.unwrap_or(false),
        override_job_name: req.override_job_name.clone().unwrap_or_default(),
        override_manifest_id: req.override_manifest_id.clone().unwrap_or_default(),
        inputs_cids: req.inputs_cids.clone(),
    };

    let job = Job {
        meta,
        job_path,
        status: "started".to_string(),
        uploaded_data_ids: HashMap::new(),
        completed_scans: HashMap::new(),
    };

    Ok(job)
}

pub fn write_job_manifest_processing(job: &Job) -> Result<PathBuf> {
    let path = job.job_path.join("job_manifest.json");
    if let Err(_e) = crate::manifest::try_write_python_processing_manifest(
        job,
        &path,
        0,
        "Request received by reconstruction server",
    ) {
        let manifest = serde_json::json!({
            "jobStatus": "processing",
            "jobProgress": 0,
            "jobStatusDetails": "Request received by reconstruction server",
        });
        fs::write(&path, serde_json::to_vec_pretty(&manifest)?).map_err(DomainError::Io)?;
    }
    Ok(path)
}

pub async fn execute_job(
    svcs: &Services,
    job: &mut Job,
    capability: &str,
    cpu_workers: usize,
) -> Result<Vec<String>> {
    if !job.meta.override_job_name.is_empty() && !job.meta.override_manifest_id.is_empty() {
        let key = format!(
            "{}.{}",
            REFINED_MANIFEST_DATA_NAME, REFINED_MANIFEST_DATA_TYPE
        );
        job.uploaded_data_ids
            .insert(key, job.meta.override_manifest_id.clone());
    }

    let manifest_path = write_job_manifest_processing(job)?;
    let mut uploaded_artifacts: Vec<String> = Vec::new();
    if !job.meta.skip_manifest_upload {
        svcs.domain.upload_manifest(job, &manifest_path).await?;
    }

    let datasets_root = job.job_path.join("datasets");
    fs::create_dir_all(&datasets_root).map_err(DomainError::Io)?;
    const BATCH: usize = 20;
    let mut use_data_ids = job.meta.inputs_cids.is_empty();
    let mut uri_error: Option<DomainError> = None;
    if !job.meta.inputs_cids.is_empty() {
        for chunk in job.meta.inputs_cids.chunks(BATCH) {
            if let Err(err) = svcs
                .domain
                .download_data_by_uris(job, chunk, &datasets_root)
                .await
            {
                warn!(
                    error = %err,
                    "Failed to download inputs via URIs; falling back to data_ids"
                );
                uri_error = Some(err);
                use_data_ids = true;
                break;
            }
        }
        if uri_error.is_none() {
            use_data_ids = false;
        }
    }

    if use_data_ids {
        for chunk in job.meta.data_ids.chunks(BATCH) {
            if let Err(err) = svcs
                .domain
                .download_data_batch(job, chunk, &datasets_root)
                .await
            {
                return Err(DomainError::Internal(format!(
                    "Invalid reconstruction inputs: unable to fetch provided URIs or data IDs ({})",
                    err
                )));
            }
        }
    } else if let Some(err) = uri_error {
        return Err(DomainError::Internal(format!(
            "Invalid reconstruction inputs: unable to fetch provided URIs ({})",
            err
        )));
    }

    if job.meta.processing_type == "local_and_global_refinement"
        || job.meta.processing_type == "local_refinement"
    {
        let _ =
            write_scan_data_summary(&datasets_root, &job.job_path.join("scan_data_summary.json"));
    }

    // Start periodic manifest writer (atomic, configurable interval; local file only)
    let writer = crate::manifest::PeriodicManifestWriter::spawn(
        job.clone(),
        job.job_path.join("job_manifest.json"),
        svcs.manifest_interval,
    );

    let uploader = crate::manifest::PeriodicManifestUploader::spawn(
        job.clone(),
        job.job_path.join("job_manifest.json"),
        Duration::from_secs(30),
        Arc::clone(&svcs.domain),
    );

    let progressing = monitor_and_upload_locals(job.clone(), Arc::clone(&svcs.domain)).await;

    let run_res = svcs.runner.run_python(job, capability, cpu_workers).await;
    if let Some(done_tx) = progressing {
        let _ = done_tx.send(true);
    }
    // Stop manifest writer/uploader cleanly
    writer.stop().await;
    uploader.stop().await;
    if !job.meta.skip_manifest_upload {
        svcs.domain
            .upload_manifest(job, &job.job_path.join("job_manifest.json"))
            .await?;
    }
    if let Err(e) = run_res {
        job.status = "failed".into();
        write_failed_manifest(job, "Reconstruction job script failed")?;
        if !job.meta.skip_manifest_upload {
            svcs.domain
                .upload_manifest(job, &job.job_path.join("job_manifest.json"))
                .await?;
        }
        return Err(e);
    }

    sweep_and_upload_locals(job.clone(), Arc::clone(&svcs.domain)).await;

    if job.meta.processing_type != "local_refinement" {
        let refined_output = job.job_path.join("refined").join("global");
        let outputs = vec![
            ExpectedOutput {
                file_path: refined_output.join("refined_manifest.json"),
                name: REFINED_MANIFEST_DATA_NAME.into(),
                data_type: REFINED_MANIFEST_DATA_TYPE.into(),
                optional: false,
            },
            ExpectedOutput {
                file_path: refined_output.join("RefinedPointCloudReduced.ply"),
                name: "refined_pointcloud".into(),
                data_type: "refined_pointcloud_ply".into(),
                optional: false,
            },
            ExpectedOutput {
                file_path: refined_output.join("RefinedPointCloud.ply.drc"),
                name: "refined_pointcloud_full_draco".into(),
                data_type: "refined_pointcloud_ply_draco".into(),
                optional: true,
            },
            ExpectedOutput {
                file_path: refined_output
                    .join("topology")
                    .join("topology_downsampled_0.111.obj"),
                name: "topologymesh_v1_lowpoly_obj".into(),
                data_type: "obj".into(),
                optional: true,
            },
            ExpectedOutput {
                file_path: refined_output
                    .join("topology")
                    .join("topology_downsampled_0.111.glb"),
                name: "topologymesh_v1_lowpoly_glb".into(),
                data_type: "glb".into(),
                optional: true,
            },
            ExpectedOutput {
                file_path: refined_output
                    .join("topology")
                    .join("topology_downsampled_0.333.obj"),
                name: "topologymesh_v1_midpoly_obj".into(),
                data_type: "obj".into(),
                optional: true,
            },
            ExpectedOutput {
                file_path: refined_output
                    .join("topology")
                    .join("topology_downsampled_0.333.glb"),
                name: "topologymesh_v1_midpoly_glb".into(),
                data_type: "glb".into(),
                optional: true,
            },
            ExpectedOutput {
                file_path: refined_output.join("topology").join("topology.obj"),
                name: "topologymesh_v1_highpoly_obj".into(),
                data_type: "obj".into(),
                optional: true,
            },
            ExpectedOutput {
                file_path: refined_output.join("topology").join("topology.glb"),
                name: "topologymesh_v1_highpoly_glb".into(),
                data_type: "glb".into(),
                optional: true,
            },
        ];

        if !job.meta.skip_manifest_upload {
            let manifest_output = &outputs[0];
            if manifest_output.file_path.exists() || !manifest_output.optional {
                svcs.domain.upload_output(job, manifest_output).await?;
                let key = format!("{}.{}", manifest_output.name, manifest_output.data_type);
                uploaded_artifacts.push(
                    job.uploaded_data_ids
                        .get(&key)
                        .cloned()
                        .unwrap_or_else(|| manifest_output.name.clone()),
                );
            }
        }
        for output in outputs.iter().skip(1) {
            if !output.file_path.exists() && output.optional {
                continue;
            }
            svcs.domain.upload_output(job, output).await?;
            let key = format!("{}.{}", output.name, output.data_type);
            uploaded_artifacts.push(
                job.uploaded_data_ids
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| output.name.clone()),
            );
        }
    }

    info!(job_id = %job.meta.id, domain_id = %job.meta.domain_id, "job succeeded");
    job.status = "succeeded".into();
    Ok(uploaded_artifacts)
}

// --- helpers ---

fn decode_jwt_iss(token: &str) -> Option<String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() < 2 {
        return None;
    }
    let payload_b64 = parts[1];
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .ok()?;
    let v: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    v.get("iss").and_then(|s| s.as_str()).map(|s| s.to_string())
}

fn required_local_outputs_exist(sfm_path: &std::path::Path) -> bool {
    let required = ["images.bin", "cameras.bin", "points3D.bin", "portals.csv"];
    required.iter().all(|f| sfm_path.join(f).exists())
}

fn write_failed_manifest(job: &Job, msg: &str) -> Result<()> {
    let path = job.job_path.join("job_manifest.json");
    if let Err(_e) = crate::manifest::try_write_python_failed_manifest(job, &path, msg) {
        let manifest = serde_json::json!({
            "jobStatus": "failed",
            "jobProgress": 0,
            "jobStatusDetails": msg,
        });
        fs::write(&path, serde_json::to_vec_pretty(&manifest)?).map_err(DomainError::Io)?;
    }
    Ok(())
}

pub(crate) async fn monitor_and_upload_locals(
    mut job: Job,
    domain: Arc<dyn DomainPort + Send + Sync>,
) -> Option<tokio::sync::watch::Sender<bool>> {
    if job.meta.processing_type != "local_refinement"
        && job.meta.processing_type != "local_and_global_refinement"
    {
        return None;
    }
    let (tx, rx) = tokio::sync::watch::channel(false);
    let datasets_root = job.job_path.join("datasets");
    let refined_root = job.job_path.join("refined").join("local");
    let _manifest_path = job.job_path.join("job_manifest.json");

    tokio::spawn(async move {
        loop {
            if *rx.borrow() {
                break;
            }
            let _total = std::fs::read_dir(&datasets_root)
                .ok()
                .map(|it| {
                    it.filter_map(|e| e.ok())
                        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                        .count()
                })
                .unwrap_or(0) as i32;
            let mut _refined = 0i32;

            if let Ok(entries) = std::fs::read_dir(&datasets_root) {
                for e in entries.flatten() {
                    if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        continue;
                    }
                    let scan_id = e.file_name().to_string_lossy().to_string();
                    let sfm = refined_root.join(&scan_id).join("sfm");
                    if sfm.exists() && required_local_outputs_exist(&sfm) {
                        _refined += 1;
                        if !job.completed_scans.get(&scan_id).cloned().unwrap_or(false) {
                            if let Err(err) =
                                zip_and_upload_scan(&job, &scan_id, &sfm, Arc::clone(&domain)).await
                            {
                                warn!("failed to upload scan zip {}: {}", scan_id, err);
                            } else {
                                job.completed_scans.insert(scan_id.clone(), true);
                            }
                        }
                    }
                }
            }

            // Compute progress to drive logs; manifest writing handled by PeriodicManifestWriter
            let (progress, status_text) = compute_progress_status(&job);
            debug!(job_id = %job.meta.id, %progress, %status_text, "progress tick");

            sleep(Duration::from_secs(30)).await;
        }
    });

    Some(tx)
}

/// Compute current job progress and status text based on folders on disk.
/// Mirrors the Go checkProgress logic, including the "-1" refined adjustment.
pub(crate) fn compute_progress_status(job: &Job) -> (i32, String) {
    let datasets_root = job.job_path.join("datasets");
    let refined_root = job.job_path.join("refined").join("local");

    let total = std::fs::read_dir(&datasets_root)
        .ok()
        .map(|it| {
            it.filter_map(|e| e.ok())
                .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                .count()
        })
        .unwrap_or(0) as i32;
    let mut refined = 0i32;
    if let Ok(entries) = std::fs::read_dir(&datasets_root) {
        for e in entries.flatten() {
            if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let scan_id = e.file_name().to_string_lossy().to_string();
            let refined_scan_path = refined_root.join(&scan_id);
            if refined_scan_path.exists() {
                refined += 1;
            }
        }
    }
    let refined_adj = (refined - 1).max(0);
    let progress = if total > 0 {
        (refined_adj as f64 / total as f64 * 100.0).round() as i32
    } else {
        0
    };
    let status_text = format!("Processed {} of {} scans", refined_adj, total);
    (progress, status_text)
}

async fn sweep_and_upload_locals(mut job: Job, domain: Arc<dyn DomainPort + Send + Sync>) {
    let datasets_root = job.job_path.join("datasets");
    let refined_root = job.job_path.join("refined").join("local");
    if let Ok(entries) = std::fs::read_dir(&datasets_root) {
        for e in entries.flatten() {
            if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let scan_id = e.file_name().to_string_lossy().to_string();
            if job.completed_scans.get(&scan_id).cloned().unwrap_or(false) {
                continue;
            }
            let sfm = refined_root.join(&scan_id).join("sfm");
            if sfm.exists() && required_local_outputs_exist(&sfm) {
                let _ = zip_and_upload_scan(&job, &scan_id, &sfm, Arc::clone(&domain)).await;
                job.completed_scans.insert(scan_id.clone(), true);
            }
        }
    }
}

async fn zip_and_upload_scan(
    job: &Job,
    scan_id: &str,
    sfm: &std::path::Path,
    domain: Arc<dyn DomainPort + Send + Sync>,
) -> Result<()> {
    use std::io::Write as _;
    let mut buffer = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buffer));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for entry in walkdir::WalkDir::new(sfm)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_dir() {
                continue;
            }
            if let Some(ext) = entry.path().extension().and_then(|s| s.to_str()) {
                if ext.eq_ignore_ascii_case("txt")
                    || ext.eq_ignore_ascii_case("bin")
                    || ext.eq_ignore_ascii_case("csv")
                {
                    let rel = entry.path().strip_prefix(sfm).unwrap_or(entry.path());
                    let rel_str = rel.to_string_lossy();
                    zip.start_file(rel_str, options)
                        .map_err(|e| DomainError::Internal(e.to_string()))?;
                    let bytes = std::fs::read(entry.path()).map_err(DomainError::Io)?;
                    zip.write_all(&bytes)
                        .map_err(|e| DomainError::Internal(e.to_string()))?;
                }
            }
        }
        zip.finish()
            .map_err(|e| DomainError::Internal(e.to_string()))?;
    }

    domain.upload_refined_scan_zip(job, scan_id, buffer).await?;
    Ok(())
}

fn write_scan_data_summary(datasets_root: &PathBuf, out_path: &PathBuf) -> Result<()> {
    use serde_json::Value as V;
    let mut scan_count = 0i32;
    let mut total_frame_count = 0i32;
    let mut total_scan_duration = 0.0f64;
    let mut scan_durations: Vec<f64> = Vec::new();
    let mut portal_ids: Vec<String> = Vec::new();
    let mut portal_sizes: Vec<f64> = Vec::new();
    let mut devices_used: Vec<String> = Vec::new();
    let mut app_versions_used: Vec<String> = Vec::new();

    let entries = match fs::read_dir(datasets_root) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };
    for e in entries.flatten() {
        if !e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let manifest_path = e.path().join("Manifest.json");
        if !manifest_path.exists() {
            continue;
        }
        let bytes = match fs::read(&manifest_path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let v: V = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(_) => continue,
        };
        scan_count += 1;

        let frame_count = v.get("frameCount").and_then(|x| x.as_f64()).unwrap_or(0.0) as i32;
        let duration = v.get("duration").and_then(|x| x.as_f64()).unwrap_or(0.0);
        total_frame_count += frame_count;
        total_scan_duration += duration;
        scan_durations.push(duration);

        if let Some(portals) = v.get("portals").and_then(|x| x.as_array()) {
            for p in portals {
                if let (Some(id), Some(size)) = (
                    p.get("shortId").and_then(|x| x.as_str()),
                    p.get("physicalSize").and_then(|x| x.as_f64()),
                ) {
                    if !portal_ids.iter().any(|s| s == id) {
                        portal_ids.push(id.to_string());
                        portal_sizes.push(size);
                    }
                }
            }
        }

        let device = format!(
            "{} {} {} {}",
            v.get("brand").and_then(|x| x.as_str()).unwrap_or(""),
            v.get("model").and_then(|x| x.as_str()).unwrap_or(""),
            v.get("systemName").and_then(|x| x.as_str()).unwrap_or(""),
            v.get("systemVersion")
                .and_then(|x| x.as_str())
                .unwrap_or("")
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
            v.get("appVersion").and_then(|x| x.as_str()),
            v.get("buildId").and_then(|x| x.as_str()),
        ) {
            (Some(av), Some(b)) => format!("{} (build {})", av, b),
            _ => "unknown".to_string(),
        };
        if !app_versions_used.iter().any(|d| d == &app_version) {
            app_versions_used.push(app_version);
        }
    }

    if scan_durations.is_empty() {
        return Ok(());
    }
    scan_durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let shortest = *scan_durations.first().unwrap();
    let longest = *scan_durations.last().unwrap();
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
    fs::write(out_path, serde_json::to_vec_pretty(&summary)?).map_err(DomainError::Io)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    fn write_dummy_outputs(job: &Job) {
        let global_dir = job.job_path.join("refined").join("global");
        let topology_dir = global_dir.join("topology");
        fs::create_dir_all(&topology_dir).unwrap();
        fs::write(global_dir.join("refined_manifest.json"), b"{}\n").unwrap();
        fs::write(global_dir.join("RefinedPointCloudReduced.ply"), b"ply").unwrap();
    }

    struct NoopDomain;
    #[async_trait::async_trait]
    impl DomainPort for NoopDomain {
        async fn upload_manifest(
            &self,
            _job: &mut Job,
            _manifest_path: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }
        async fn upload_output(&self, _job: &Job, _output: &ExpectedOutput) -> Result<()> {
            Ok(())
        }
        async fn download_data_batch(
            &self,
            _job: &Job,
            _ids: &[String],
            _datasets_root: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }
        async fn download_data_by_uris(
            &self,
            _job: &Job,
            _uris: &[String],
            _datasets_root: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }
        async fn upload_refined_scan_zip(
            &self,
            _job: &Job,
            _scan_id: &str,
            _zip_bytes: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }
    }
    struct NoopRunner;
    #[async_trait::async_trait]
    impl JobRunner for NoopRunner {
        async fn run_python(
            &self,
            _job: &Job,
            _capability: &str,
            _cpu_workers: usize,
        ) -> Result<()> {
            write_dummy_outputs(_job);
            Ok(())
        }
    }

    #[tokio::test]
    async fn create_job_and_execute_smoke() {
        let dir = tempfile::tempdir().unwrap();
        let req = serde_json::json!({
            "data_ids": ["a","b"],
            "domain_id": "dom1",
            "access_token": "token",
            "processing_type": "local_and_global_refinement",
            "domain_server_url": "http://example",
            "skip_manifest_upload": false,
            "override_job_name": "",
            "override_manifest_id": "",
        });
        let mut job = create_job_metadata(
            &dir.path().to_path_buf(),
            &req.to_string(),
            "localhost",
            None,
        )
        .unwrap();
        let domain: Arc<dyn DomainPort + Send + Sync> = Arc::new(NoopDomain);
        let runner: Arc<dyn JobRunner + Send + Sync> = Arc::new(NoopRunner);
        let svcs = Services {
            domain,
            runner,
            manifest_interval: std::time::Duration::from_millis(50),
        };
        let outputs = execute_job(&svcs, &mut job, "capability/test", 2)
            .await
            .unwrap();
        assert_eq!(job.status, "succeeded");
        assert!(!outputs.is_empty());
    }

    struct CountingDomain(Arc<AtomicUsize>);
    #[async_trait::async_trait]
    impl DomainPort for CountingDomain {
        async fn upload_manifest(
            &self,
            _job: &mut Job,
            _manifest_path: &std::path::Path,
        ) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn upload_output(&self, _job: &Job, _output: &ExpectedOutput) -> Result<()> {
            Ok(())
        }
        async fn download_data_batch(
            &self,
            _job: &Job,
            _ids: &[String],
            _datasets_root: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }
        async fn download_data_by_uris(
            &self,
            _job: &Job,
            _uris: &[String],
            _datasets_root: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }
        async fn upload_refined_scan_zip(
            &self,
            _job: &Job,
            _scan_id: &str,
            _zip_bytes: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }
    }
    struct InstantRunner;
    #[async_trait::async_trait]
    impl JobRunner for InstantRunner {
        async fn run_python(
            &self,
            _job: &Job,
            _capability: &str,
            _cpu_workers: usize,
        ) -> Result<()> {
            write_dummy_outputs(_job);
            Ok(())
        }
    }

    #[tokio::test]
    async fn execute_job_calls_manifest_initial_and_final() {
        let dir = tempfile::tempdir().unwrap();
        let req = serde_json::json!({
            "data_ids": [],
            "domain_id": "domx",
            "access_token": "token",
            "processing_type": "local_and_global_refinement",
            "domain_server_url": "http://example",
            "skip_manifest_upload": false,
            "override_job_name": "",
            "override_manifest_id": "",
        });
        let mut job = create_job_metadata(
            &dir.path().to_path_buf(),
            &req.to_string(),
            "localhost",
            None,
        )
        .unwrap();

        let counter = Arc::new(AtomicUsize::new(0));
        let domain: Arc<dyn DomainPort + Send + Sync> = Arc::new(CountingDomain(counter.clone()));
        let runner: Arc<dyn JobRunner + Send + Sync> = Arc::new(InstantRunner);
        let svcs = Services {
            domain,
            runner,
            manifest_interval: std::time::Duration::from_millis(50),
        };
        let outputs = execute_job(&svcs, &mut job, "capability/test", 1)
            .await
            .unwrap();
        // At least 2 calls: initial POST and final PUT after stop
        assert!(
            counter.load(Ordering::SeqCst) >= 2,
            "expected at least 2 manifest uploads"
        );
        assert!(!outputs.is_empty());
    }

    struct FailingManifestDomain;
    #[async_trait::async_trait]
    impl DomainPort for FailingManifestDomain {
        async fn upload_manifest(
            &self,
            _job: &mut Job,
            _manifest_path: &std::path::Path,
        ) -> Result<()> {
            Err(DomainError::Internal("manifest boom".into()))
        }

        async fn upload_output(&self, _job: &Job, _output: &ExpectedOutput) -> Result<()> {
            Ok(())
        }

        async fn download_data_batch(
            &self,
            _job: &Job,
            _ids: &[String],
            _datasets_root: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }
        async fn download_data_by_uris(
            &self,
            _job: &Job,
            _uris: &[String],
            _datasets_root: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }

        async fn upload_refined_scan_zip(
            &self,
            _job: &Job,
            _scan_id: &str,
            _zip_bytes: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn execute_job_propagates_manifest_upload_failure() {
        let dir = tempfile::tempdir().unwrap();
        let req = serde_json::json!({
            "data_ids": [],
            "domain_id": "dom_fail",
            "access_token": "token",
            "processing_type": "local_and_global_refinement",
            "domain_server_url": "http://example",
            "skip_manifest_upload": false,
            "override_job_name": "",
            "override_manifest_id": "",
        });

        let mut job = create_job_metadata(
            &dir.path().to_path_buf(),
            &req.to_string(),
            "localhost",
            None,
        )
        .unwrap();

        let domain: Arc<dyn DomainPort + Send + Sync> = Arc::new(FailingManifestDomain);
        let runner: Arc<dyn JobRunner + Send + Sync> = Arc::new(NoopRunner);
        let svcs = Services {
            domain,
            runner,
            manifest_interval: std::time::Duration::from_millis(50),
        };

        let res = execute_job(&svcs, &mut job, "capability/test", 1).await;
        match res {
            Err(DomainError::Internal(msg)) => assert!(msg.contains("manifest boom")),
            other => panic!("expected manifest failure, got {:?}", other),
        }
    }

    struct OutputFailDomain;
    #[async_trait::async_trait]
    impl DomainPort for OutputFailDomain {
        async fn upload_manifest(
            &self,
            _job: &mut Job,
            _manifest_path: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }

        async fn upload_output(&self, _job: &Job, _output: &ExpectedOutput) -> Result<()> {
            Err(DomainError::Internal("output boom".into()))
        }

        async fn download_data_batch(
            &self,
            _job: &Job,
            _ids: &[String],
            _datasets_root: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }
        async fn download_data_by_uris(
            &self,
            _job: &Job,
            _uris: &[String],
            _datasets_root: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }

        async fn upload_refined_scan_zip(
            &self,
            _job: &Job,
            _scan_id: &str,
            _zip_bytes: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn execute_job_propagates_output_upload_failure() {
        let dir = tempfile::tempdir().unwrap();
        let req = serde_json::json!({
            "data_ids": [],
            "domain_id": "dom_fail_output",
            "access_token": "token",
            "processing_type": "local_and_global_refinement",
            "domain_server_url": "http://example",
            "skip_manifest_upload": false,
            "override_job_name": "",
            "override_manifest_id": "",
        });

        let mut job = create_job_metadata(
            &dir.path().to_path_buf(),
            &req.to_string(),
            "localhost",
            None,
        )
        .unwrap();

        let domain: Arc<dyn DomainPort + Send + Sync> = Arc::new(OutputFailDomain);
        let runner: Arc<dyn JobRunner + Send + Sync> = Arc::new(InstantRunner);
        let svcs = Services {
            domain,
            runner,
            manifest_interval: std::time::Duration::from_millis(50),
        };

        let res = execute_job(&svcs, &mut job, "capability/test", 1).await;
        match res {
            Err(DomainError::Internal(msg)) => assert!(msg.contains("output boom")),
            other => panic!("expected output failure, got {:?}", other),
        }
    }

    #[test]
    fn progress_matches_go_logic() {
        let tmp = tempfile::tempdir().unwrap();
        let job_root = tmp.path().join("jobs").join("dom").join("job_1");
        std::fs::create_dir_all(job_root.join("datasets").join("scanA")).unwrap();
        // refined/local/scanA exists, but no sfm files
        std::fs::create_dir_all(job_root.join("refined/local/scanA")).unwrap();
        let job = Job {
            meta: JobMetadata {
                id: "1".into(),
                name: "job_1".into(),
                domain_id: "dom".into(),
                processing_type: "local_and_global_refinement".into(),
                created_at: chrono::Utc::now(),
                domain_server_url: "http://example".into(),
                reconstruction_server_url: "localhost".into(),
                access_token: "token".into(),
                data_ids: vec![],
                inputs_cids: vec![],
                skip_manifest_upload: true,
                override_job_name: String::new(),
                override_manifest_id: String::new(),
            },
            job_path: job_root.clone(),
            status: "started".into(),
            uploaded_data_ids: Default::default(),
            completed_scans: Default::default(),
        };
        let (p, s) = compute_progress_status(&job);
        assert_eq!(p, 0);
        assert_eq!(s, "Processed 0 of 1 scans");

        // Add another dataset and refined dir; refined_adj = (2-1)=1 of total 2 => 50%
        std::fs::create_dir_all(job_root.join("datasets").join("scanB")).unwrap();
        std::fs::create_dir_all(job_root.join("refined/local/scanB")).unwrap();
        let (p2, s2) = compute_progress_status(&job);
        assert_eq!(p2, 50);
        assert_eq!(s2, "Processed 1 of 2 scans");
    }

    struct DropTrackingDomain(Arc<AtomicBool>);
    impl Drop for DropTrackingDomain {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl DomainPort for DropTrackingDomain {
        async fn upload_manifest(
            &self,
            _job: &mut Job,
            _manifest_path: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }

        async fn upload_output(&self, _job: &Job, _output: &ExpectedOutput) -> Result<()> {
            Ok(())
        }

        async fn download_data_batch(
            &self,
            _job: &Job,
            _ids: &[String],
            _datasets_root: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }
        async fn download_data_by_uris(
            &self,
            _job: &Job,
            _uris: &[String],
            _datasets_root: &std::path::Path,
        ) -> Result<()> {
            Ok(())
        }

        async fn upload_refined_scan_zip(
            &self,
            _job: &Job,
            _scan_id: &str,
            _zip_bytes: Vec<u8>,
        ) -> Result<()> {
            Ok(())
        }
    }

    struct DropTrackingRunner(Arc<AtomicBool>);

    impl Drop for DropTrackingRunner {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl JobRunner for DropTrackingRunner {
        async fn run_python(
            &self,
            _job: &Job,
            _capability: &str,
            _cpu_workers: usize,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn services_drop_non_static_dependencies() {
        let domain_dropped = Arc::new(AtomicBool::new(false));
        let runner_dropped = Arc::new(AtomicBool::new(false));

        {
            let domain: Arc<dyn DomainPort + Send + Sync> =
                Arc::new(DropTrackingDomain(domain_dropped.clone()));
            let runner: Arc<dyn JobRunner + Send + Sync> =
                Arc::new(DropTrackingRunner(runner_dropped.clone()));
            let services = Services {
                domain,
                runner,
                manifest_interval: Duration::from_millis(10),
            };
            assert!(!domain_dropped.load(Ordering::SeqCst));
            assert!(!runner_dropped.load(Ordering::SeqCst));
            drop(services);
        }

        assert!(domain_dropped.load(Ordering::SeqCst));
        assert!(runner_dropped.load(Ordering::SeqCst));
    }
}
