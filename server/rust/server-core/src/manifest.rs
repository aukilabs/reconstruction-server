use crate::{types::Job, usecases::compute_progress_status};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{fs, path::PathBuf, time::Duration};
use tokio::{sync::watch, task::JoinHandle, time::interval};
use tracing::{debug, error, info};

/// Atomically write bytes to `path` by writing to a temp file in the same
/// directory and then renaming.
fn atomic_write_bytes(path: &PathBuf, bytes: &[u8]) -> std::io::Result<()> {
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

pub struct PeriodicManifestWriter {
    stop_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl PeriodicManifestWriter {
    /// Spawn a background task that, at `interval`, serializes a manifest for `job`
    /// and writes it atomically to `manifest_path`. If `upload` is Some, also
    /// uploads the manifest using the provided DomainPort implementation.
    pub fn spawn(
        job: Job,
        manifest_path: PathBuf,
        interval_dur: Duration,
        upload: Option<&'static dyn crate::usecases::DomainPort>,
    ) -> Self {
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

                let (progress, status_text) = compute_progress_status(&job);
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
                } else if first {
                    first = false;
                    info!(path = %manifest_path.display(), "periodic manifest writer started and wrote first tick");
                } else {
                    debug!(path = %manifest_path.display(), "periodic manifest writer tick wrote manifest");
                }

                if let Some(domain) = upload {
                    if !job.meta.skip_manifest_upload {
                        // best-effort; errors are logged in adapter layer when propagated, but ignore here
                        let _ = domain.upload_manifest(&job, &manifest_path).await;
                    }
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
            None,
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
}
