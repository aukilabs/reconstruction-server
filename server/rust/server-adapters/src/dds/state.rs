use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const STATE_PATH: &str = "data/registration_state.json";
pub const LOCK_PATH: &str = "data/registration.lock";

pub const STATUS_REGISTERING: &str = "registering";
pub const STATUS_REGISTERED: &str = "registered";
pub const STATUS_DISCONNECTED: &str = "disconnected";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationState {
    pub status: String,
    #[serde(default, with = "chrono::serde::ts_seconds_option")]
    pub last_healthcheck: Option<DateTime<Utc>>, // seconds precision is enough
}

impl Default for RegistrationState {
    fn default() -> Self {
        Self { status: STATUS_DISCONNECTED.to_string(), last_healthcheck: None }
    }
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut p = PathBuf::from(path);
    let base = p
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("registration_state.json");
    let pid = std::process::id();
    let nonce = uuid::Uuid::new_v4();
    let tmp = format!("{}.tmp.{}.{}", base, pid, nonce);
    p.set_file_name(tmp);
    p
}

pub fn read_state_from_path(path: &Path) -> Result<RegistrationState> {
    match fs::read_to_string(path) {
        Ok(s) => {
            let st: RegistrationState = serde_json::from_str(&s)
                .with_context(|| format!("decode {}", path.display()))?;
            Ok(st)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(RegistrationState::default()),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

pub fn write_state_to_path(path: &Path, st: &RegistrationState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create dir {}", parent.display()))?;
    }
    let tmp = tmp_path_for(path);
    let encoded = serde_json::to_vec_pretty(st).context("encode state json")?;
    let mut f = File::create(&tmp).with_context(|| format!("create tmp {}", tmp.display()))?;
    f.write_all(&encoded)
        .with_context(|| format!("write tmp {}", tmp.display()))?;
    f.sync_all().ok();
    drop(f);
    // Try atomic replace; if not supported (e.g., some platforms), remove then rename.
    match fs::rename(&tmp, path) {
        Ok(()) => {}
        Err(_e) => {
            let _ = fs::remove_file(path);
            fs::rename(&tmp, path)
                .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
        }
    }
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

pub fn read_state() -> Result<RegistrationState> {
    read_state_from_path(Path::new(STATE_PATH))
}

pub fn write_state(st: &RegistrationState) -> Result<()> {
    write_state_to_path(Path::new(STATE_PATH), st)
}

pub fn set_status(new_status: &str) -> Result<()> {
    let mut st = read_state()?;
    st.status = new_status.to_string();
    write_state(&st)
}

pub fn touch_healthcheck_now() -> Result<()> {
    let mut st = read_state()?;
    st.last_healthcheck = Some(Utc::now());
    write_state(&st)
}

// File-based advisory lock. The returned file must be kept alive to hold the lock.
pub struct LockGuard {
    path: std::path::PathBuf,
    _file: fs::File,
}

impl LockGuard {
    // Best-effort lock with stale recovery based on file mtime.
    // If lock exists and is older than `stale_after`, it is removed.
    pub fn try_acquire(stale_after: Duration) -> std::io::Result<Option<Self>> {
        let path = Path::new(LOCK_PATH);
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }

        // Attempt to create the lock file exclusively
        match fs::OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut f) => {
                // Write a timestamp to aid diagnostics and staleness checks
                let now = chrono::Utc::now().to_rfc3339();
                let _ = writeln!(f, "created_at={}, pid={}", now, std::process::id());
                Ok(Some(Self { path: path.to_path_buf(), _file: f }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Check age for staleness
                if let Ok(meta) = fs::metadata(path) {
                    if let Ok(modified) = meta.modified() {
                        if let Ok(age) = modified.elapsed() {
                            if age > stale_after {
                                // Try to remove stale lock and acquire again
                                let _ = fs::remove_file(path);
                                // Second attempt
                                if let Ok(mut f2) = fs::OpenOptions::new().write(true).create_new(true).open(path) {
                                    let now = chrono::Utc::now().to_rfc3339();
                                    let _ = writeln!(f2, "created_at={}, pid={}", now, std::process::id());
                                    return Ok(Some(Self { path: path.to_path_buf(), _file: f2 }));
                                }
                            }
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // Remove the lock file to signal release; ignore errors
        let _ = fs::remove_file(&self.path);
    }
}
