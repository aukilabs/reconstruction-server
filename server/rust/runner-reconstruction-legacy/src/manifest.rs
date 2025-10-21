use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Serialize;
use tokio::{process::Command, sync::watch, task::JoinHandle, time::interval};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct ManifestState {
    inner: Arc<std::sync::Mutex<State>>,
}

#[derive(Clone, Debug)]
struct State {
    progress: i32,
    status: String,
}

impl ManifestState {
    pub fn new(progress: i32, status: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(State {
                progress,
                status: status.into(),
            })),
        }
    }

    pub fn update(&self, progress: i32, status: impl Into<String>) {
        let mut guard = self.inner.lock().expect("manifest state mutex poisoned");
        guard.progress = progress;
        guard.status = status.into();
    }

    fn snapshot(&self) -> State {
        self.inner
            .lock()
            .expect("manifest state mutex poisoned")
            .clone()
    }
}

impl Default for ManifestState {
    fn default() -> Self {
        Self::new(0, "initializing")
    }
}

#[async_trait]
pub trait ProgressListener: Send + Sync {
    async fn report_progress(&self, progress: i32, status: String) -> Result<()>;
}

pub struct ManifestTask {
    stop_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl ManifestTask {
    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.join.await;
    }
}

pub fn spawn_processing_writer(
    manifest_path: PathBuf,
    interval_dur: Duration,
    state: ManifestState,
    listener: Arc<dyn ProgressListener>,
    cancel: CancellationToken,
) -> ManifestTask {
    let (tx, mut rx) = watch::channel(false);
    let join = tokio::spawn(async move {
        if let Err(err) = write_snapshot(&manifest_path, &state, listener.as_ref()).await {
            eprintln!("manifest: failed to write initial snapshot: {err}");
        }

        let mut ticker = interval(interval_dur);
        let token = cancel.child_token();
        loop {
            tokio::select! {
                _ = ticker.tick() => {},
                _ = token.cancelled() => break,
                _ = rx.changed() => {
                    if *rx.borrow() {
                        break;
                    }
                    continue;
                }
            }

            if let Err(err) = write_snapshot(&manifest_path, &state, listener.as_ref()).await {
                eprintln!(
                    "manifest: failed to write snapshot for {}: {err}",
                    manifest_path.display()
                );
            }
        }
    });
    ManifestTask { stop_tx: tx, join }
}

pub async fn write_failed_manifest(path: &Path, message: &str) -> Result<()> {
    let snapshot = ManifestJson {
        job_status: "failed",
        job_progress: 0,
        job_status_details: message.to_string(),
    };
    let bytes = serde_json::to_vec_pretty(&snapshot).context("serialize failed manifest")?;
    atomic_write_bytes(path, &bytes).context("write failed manifest")
}

pub async fn write_processing_manifest(path: &Path, progress: i32, status: &str) -> Result<()> {
    let snapshot = ManifestJson {
        job_status: "processing",
        job_progress: progress,
        job_status_details: status.to_string(),
    };
    let bytes = serde_json::to_vec_pretty(&snapshot).context("serialize processing manifest")?;
    atomic_write_bytes(path, &bytes).context("write processing manifest")
}

/// Write a richer processing manifest using the Python helper `save_manifest_json`, mirroring
/// the legacy Go server behaviour. This includes job metadata, server details and, when present,
/// the scan data summary so downstream UIs see parity even mid-job.
pub async fn write_processing_manifest_python(
    manifest_path: &Path,
    job_root_path: &Path,
    python_bin: &Path,
    progress: i32,
    status: &str,
) -> Result<()> {
    let script = format!(
        "from utils.data_utils import save_manifest_json; save_manifest_json({{}}, r'{manifest}', r'{root}', job_status='processing', job_progress={progress}, job_status_details=r'{status}')",
        manifest = manifest_path.display(),
        root = job_root_path.display(),
        progress = progress,
        status = escape_py_single_quoted(status),
    );
    let out = Command::new(python_bin)
        .arg("-c")
        .arg(script)
        .output()
        .await;
    match out {
        Ok(o) if o.status.success() => return Ok(()),
        Ok(_) => {
            // fallthrough to fallback
        }
        Err(_spawn_err) => {
            // fallthrough to fallback
        }
    }
    {
        // Fallback to minimal JSON to avoid losing visibility.
        return write_processing_manifest(manifest_path, progress, status)
            .await
            .context("python manifest snapshot failed; wrote minimal manifest instead");
    }
}

/// Periodically write processing snapshots via Python helper; used by the runner for rich mid-job manifests.
pub fn spawn_python_processing_writer(
    manifest_path: PathBuf,
    job_root_path: PathBuf,
    python_bin: PathBuf,
    interval_dur: Duration,
    state: ManifestState,
    listener: Arc<dyn ProgressListener>,
    cancel: CancellationToken,
) -> ManifestTask {
    let (tx, mut rx) = watch::channel(false);
    let join = tokio::spawn(async move {
        // Initial snapshot
        if let Err(err) = write_processing_manifest_python(
            &manifest_path,
            &job_root_path,
            &python_bin,
            state.snapshot().progress,
            &state.snapshot().status,
        )
        .await
        {
            eprintln!("manifest(py): failed to write initial snapshot: {err}");
        }

        let mut ticker = interval(interval_dur);
        let token = cancel.child_token();
        loop {
            tokio::select! {
                _ = ticker.tick() => {},
                _ = token.cancelled() => break,
                _ = rx.changed() => {
                    if *rx.borrow() { break; }
                    continue;
                }
            }

            let snap = state.snapshot();
            if let Err(err) = listener
                .report_progress(snap.progress, snap.status.clone())
                .await
            {
                eprintln!("manifest(py): progress forward failed: {err}");
            }

            if let Err(err) = write_processing_manifest_python(
                &manifest_path,
                &job_root_path,
                &python_bin,
                snap.progress,
                &snap.status,
            )
            .await
            {
                eprintln!(
                    "manifest(py): failed to write snapshot for {}: {err}",
                    manifest_path.display()
                );
            }
        }
    });
    ManifestTask { stop_tx: tx, join }
}

fn escape_py_single_quoted(input: &str) -> String {
    input.replace('\\', "\\\\").replace('\'', "\\\'")
}

#[derive(Serialize)]
struct ManifestJson {
    #[serde(rename = "jobStatus")]
    job_status: &'static str,
    #[serde(rename = "jobProgress")]
    job_progress: i32,
    #[serde(rename = "jobStatusDetails")]
    job_status_details: String,
}

async fn write_snapshot(
    manifest_path: &Path,
    state: &ManifestState,
    listener: &dyn ProgressListener,
) -> Result<()> {
    let snapshot = state.snapshot();
    listener
        .report_progress(snapshot.progress, snapshot.status.clone())
        .await
        .context("report progress")?;

    let json = ManifestJson {
        job_status: "processing",
        job_progress: snapshot.progress,
        job_status_details: snapshot.status.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&json).context("serialize manifest json")?;
    atomic_write_bytes(manifest_path, &bytes).context("write manifest")
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::fs;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "manifest.json".into());
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp_name = format!("{}.tmp.{}", file_name, ts);
    let tmp_path = path.with_file_name(tmp_name);
    fs::write(&tmp_path, bytes).with_context(|| format!("write tmp {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} -> {}", tmp_path.display(), path.display()))?;
    Ok(())
}
