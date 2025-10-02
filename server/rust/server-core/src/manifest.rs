use crate::{types::Job, usecases::compute_progress_status, DomainPort};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::interval};
use tracing::{debug, error, info};

/// Atomically write bytes to `path` by writing to a temp file in the same
/// directory and then renaming.
fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file_name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "manifest.json".to_string());
    // Use deterministic RNG seeded from time to avoid bringing in thread_rng
    let mut rng = StdRng::from_entropy();
    let tmp_name = format!("{}.tmp.{}", file_name, rng.gen::<u64>());
    let tmp_path = path.with_file_name(tmp_name);
    fs::write(&tmp_path, bytes)?;
    // On POSIX this is atomic; on Windows it's best-effort replacement
    fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Attempt to write a rich manifest using the Python helper (Go-level detail).
/// On success returns Ok(()); on failure returns the IO error so caller can fallback.
pub(crate) fn try_write_python_processing_manifest(
    job: &Job,
    manifest_path: &Path,
    progress: i32,
    status_text: &str,
) -> std::io::Result<()> {
    use std::process::{Command, Stdio};
    let code = "from utils.data_utils import save_manifest_json; import sys; "
        .to_string()
        + "save_manifest_json({}, sys.argv[1], sys.argv[2], job_status='processing', job_progress=int(sys.argv[3]), job_status_details=sys.argv[4])";
    let mut cmd = Command::new("python3");
    cmd.arg("-c")
        .arg(code)
        .arg(manifest_path.to_string_lossy().to_string())
        .arg(job.job_path.to_string_lossy().to_string())
        .arg(progress.to_string())
        .arg(status_text)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = cmd.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(
            "python save_manifest_json returned non-zero",
        ))
    }
}

/// Attempt to write a rich failed manifest using Python helper; on failure return Err for fallback.
pub(crate) fn try_write_python_failed_manifest(
    job: &Job,
    manifest_path: &Path,
    error_message: &str,
) -> std::io::Result<()> {
    use std::process::{Command, Stdio};
    let code = "from utils.data_utils import save_failed_manifest_json; import sys; ".to_string()
        + "save_failed_manifest_json(sys.argv[1], sys.argv[2], sys.argv[3])";
    let mut cmd = Command::new("python3");
    cmd.arg("-c")
        .arg(code)
        .arg(manifest_path.to_string_lossy().to_string())
        .arg(job.job_path.to_string_lossy().to_string())
        .arg(error_message)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = cmd.status()?;
    if status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(
            "python save_failed_manifest_json returned non-zero",
        ))
    }
}

pub struct PeriodicManifestWriter {
    stop_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl PeriodicManifestWriter {
    /// Spawn a background task that, at `interval`, serializes a manifest for `job`
    /// and writes it atomically to `manifest_path`.
    pub fn spawn(job: Job, manifest_path: PathBuf, interval_dur: Duration) -> Self {
        let (tx, mut rx) = watch::channel(false);
        let join = tokio::spawn(async move {
            let mut ticker = interval(interval_dur);
            let mut first = true;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {},
                    _ = rx.changed() => {
                        if *rx.borrow() { break; }
                        continue;
                    }
                }

                // If Python wrote a final manifest (succeeded/failed), do not overwrite.
                let should_skip = match std::fs::read(&manifest_path) {
                    Ok(bytes) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
                        Ok(v) => matches!(
                            v.get("jobStatus").and_then(|s| s.as_str()),
                            Some("succeeded") | Some("failed")
                        ),
                        Err(_) => false,
                    },
                    Err(_) => false,
                };
                if should_skip {
                    debug!(path = %manifest_path.display(), "final manifest present; skipping writer tick");
                    continue;
                }

                let (progress, status_text) = compute_progress_status(&job);
                // Prefer Go-level manifest via Python; fallback to minimal JSON
                let py_res = try_write_python_processing_manifest(
                    &job,
                    &manifest_path,
                    progress,
                    &status_text,
                );
                if py_res.is_err() {
                    let manifest = serde_json::json!({
                        "jobStatus": "processing",
                        "jobProgress": progress,
                        "jobStatusDetails": status_text,
                    });
                    let bytes = match serde_json::to_vec_pretty(&manifest) {
                        Ok(b) => b,
                        Err(e) => {
                            error!(error = %e, "failed to serialize manifest json");
                            continue;
                        }
                    };
                    if let Err(e) = atomic_write_bytes(&manifest_path, &bytes) {
                        error!(path = %manifest_path.display(), error = %e, "failed to write manifest atomically");
                        continue;
                    }
                }
                if first {
                    first = false;
                    info!(path = %manifest_path.display(), "periodic manifest writer started and wrote first tick");
                } else {
                    debug!(path = %manifest_path.display(), "periodic manifest writer tick wrote manifest");
                }
            }
        });
        Self { stop_tx: tx, join }
    }

    /// Signal the background task to stop and wait for it to finish.
    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.join.await;
    }
}

/// Mirrors Go's checkProgress upload behavior: every `interval_dur`,
/// compute progress (for logging), then upload the current manifest via
/// POST (first time) or PUT (subsequent).
pub struct PeriodicManifestUploader {
    stop_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl PeriodicManifestUploader {
    pub fn spawn(
        mut job: Job,
        manifest_path: PathBuf,
        interval_dur: Duration,
        domain: Arc<dyn DomainPort + Send + Sync>,
    ) -> Self {
        let (tx, mut rx) = watch::channel(false);
        let join = tokio::spawn(async move {
            let mut ticker = interval(interval_dur);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {},
                    _ = rx.changed() => {
                        if *rx.borrow() { break; }
                        continue;
                    }
                }
                if job.meta.skip_manifest_upload {
                    continue;
                }
                // Log current progress like Go
                let (progress, status_text) = compute_progress_status(&job);
                info!(job_id = %job.meta.id, domain_id = %job.meta.domain_id, progress = %progress, %status_text, "manifest progress tick");
                // Upload manifest: POST first, then PUT updates (domain impl handles logic based on job.uploaded_data_ids)
                if let Err(e) = domain.upload_manifest(&mut job, &manifest_path).await {
                    error!(job_id = %job.meta.id, domain_id = %job.meta.domain_id, error = %format!("{}", e), "failed to upload job manifest");
                }
            }
        });
        Self { stop_tx: tx, join }
    }

    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.join.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{advance, pause};

    #[tokio::test]
    async fn periodic_writer_updates_and_is_atomic() {
        pause();
        let tmp = tempfile::tempdir().unwrap();
        let job_root = tmp.path().join("jobs").join("dom").join("job_1");
        fs::create_dir_all(job_root.join("datasets")).unwrap();
        fs::create_dir_all(job_root.join("refined/local")).unwrap();

        // Minimal Job
        let job = Job {
            meta: crate::types::JobMetadata {
                id: "1".into(),
                name: "job_1".into(),
                domain_id: "dom".into(),
                processing_type: "local_and_global_refinement".into(),
                created_at: chrono::Utc::now(),
                domain_server_url: "http://example".into(),
                reconstruction_server_url: "localhost".into(),
                access_token: "token".into(),
                data_ids: vec![],
                skip_manifest_upload: true,
                override_job_name: String::new(),
                override_manifest_id: String::new(),
            },
            job_path: job_root.clone(),
            status: "started".into(),
            uploaded_data_ids: Default::default(),
            completed_scans: Default::default(),
        };

        let manifest_path = job_root.join("job_manifest.json");
        let writer = PeriodicManifestWriter::spawn(
            job.clone(),
            manifest_path.clone(),
            Duration::from_millis(100),
        );

        // Advance time and wait until file appears
        for _ in 0..5 {
            advance(Duration::from_millis(100)).await;
            if manifest_path.exists() {
                break;
            }
        }
        assert!(
            manifest_path.exists(),
            "manifest should be written by writer"
        );
        let bytes = fs::read(&manifest_path).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(v.get("jobStatus").is_some());
        assert!(v.get("jobProgress").is_some());
        assert!(v.get("jobStatusDetails").is_some());

        // Simulate racing reads while writer may write: ensure always parses
        for _ in 0..20 {
            advance(Duration::from_millis(100)).await;
            let bytes = fs::read(&manifest_path).unwrap();
            let _v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        }

        writer.stop().await;

        // After stop, advancing time should not change the file
        let before = fs::metadata(&manifest_path).unwrap().modified().unwrap();
        advance(Duration::from_secs(1)).await;
        let after = fs::metadata(&manifest_path).unwrap().modified().unwrap();
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn writer_skips_when_final_manifest_present() {
        pause();
        let tmp = tempfile::tempdir().unwrap();
        let job_root = tmp.path().join("jobs").join("dom").join("job_1");
        fs::create_dir_all(job_root.join("datasets")).unwrap();
        fs::create_dir_all(job_root.join("refined/local")).unwrap();

        let job = Job {
            meta: crate::types::JobMetadata {
                id: "1".into(),
                name: "job_1".into(),
                domain_id: "dom".into(),
                processing_type: "local_and_global_refinement".into(),
                created_at: chrono::Utc::now(),
                domain_server_url: "http://example".into(),
                reconstruction_server_url: "localhost".into(),
                access_token: "token".into(),
                data_ids: vec![],
                skip_manifest_upload: true,
                override_job_name: String::new(),
                override_manifest_id: String::new(),
            },
            job_path: job_root.clone(),
            status: "started".into(),
            uploaded_data_ids: Default::default(),
            completed_scans: Default::default(),
        };

        let manifest_path = job_root.join("job_manifest.json");
        // Write a final manifest
        let final_manifest = serde_json::json!({
            "jobStatus": "succeeded",
            "jobProgress": 100,
            "jobStatusDetails": "done"
        });
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&final_manifest).unwrap(),
        )
        .unwrap();
        let before = fs::metadata(&manifest_path).unwrap().modified().unwrap();

        let writer = PeriodicManifestWriter::spawn(
            job.clone(),
            manifest_path.clone(),
            Duration::from_millis(100),
        );
        for _ in 0..5 {
            advance(Duration::from_millis(100)).await;
        }
        writer.stop().await;

        let after = fs::metadata(&manifest_path).unwrap().modified().unwrap();
        assert_eq!(before, after, "writer should not overwrite final manifest");
    }
}
