//! Capture identity is independent of its recording time and Domain data UUIDs.
use std::{path::Path, sync::OnceLock};

use anyhow::{bail, Context, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs;
use uuid::Uuid;

use crate::TaskIo;

pub const CAPTURE_METADATA_FILE: &str = "capture_metadata.json";

pub fn is_uuid_capture_id(id: &str) -> bool {
    id.len() == 36 && Uuid::parse_str(id).is_ok()
}

pub fn capture_id(id: &str) -> Option<&str> {
    static LEGACY: OnceLock<Regex> = OnceLock::new();
    let legacy =
        LEGACY.get_or_init(|| Regex::new(r"^\d{4}-\d{2}-\d{2}[_-]\d{2}-\d{2}-\d{2}$").unwrap());
    (is_uuid_capture_id(id) || legacy.is_match(id)).then_some(id)
}

pub fn manifest_capture_id(id: &str) -> Option<&str> {
    capture_id(id.strip_prefix("dmt_scan_").unwrap_or(id))
}

pub fn artifact_capture_id(name: &str) -> Option<&str> {
    static ARTIFACT: OnceLock<Regex> = OnceLock::new();
    let pattern = ARTIFACT.get_or_init(|| Regex::new(
        r"(?i)^(?:dmt_[a-z0-9_]+|refined_scan)_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\d{4}-\d{2}-\d{2}[_-]\d{2}-\d{2}-\d{2})$"
    ).unwrap());
    pattern
        .captures(name)
        .and_then(|m| m.get(1))
        .map(|m| m.as_str())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureMetadata {
    pub capture_id: String,
    pub scan_start_time: Option<String>,
}

impl CaptureMetadata {
    pub fn from_manifest(id: &str, manifest: &Value) -> Result<Self> {
        let manifest_id = manifest
            .get("id")
            .and_then(Value::as_str)
            .and_then(manifest_capture_id);
        if is_uuid_capture_id(id) && manifest_id != Some(id) {
            bail!("capture manifest identity does not match {id}");
        }
        let scan_start_time = manifest
            .get("scanStartTime")
            .and_then(Value::as_str)
            .filter(|time| chrono::DateTime::parse_from_rfc3339(time).is_ok())
            .map(str::to_owned);
        let metadata = Self {
            capture_id: id.to_owned(),
            scan_start_time,
        };
        metadata.validate(id)?;
        Ok(metadata)
    }

    pub fn validate(&self, id: &str) -> Result<()> {
        if capture_id(id).is_none() || self.capture_id != id {
            bail!("refined capture metadata identity does not match {id}");
        }
        if is_uuid_capture_id(id)
            && self
                .scan_start_time
                .as_deref()
                .is_none_or(|time| chrono::DateTime::parse_from_rfc3339(time).is_err())
        {
            bail!("capture {id} has no valid recording start time");
        }
        Ok(())
    }
}

/// Legacy inputs may have no manifest/time. UUID inputs require both.
pub fn read_capture_manifest(id: &str, path: &Path) -> Result<CaptureMetadata> {
    let manifest = match std::fs::read(path) {
        Ok(bytes) => {
            serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !is_uuid_capture_id(id) => {
            Value::Null
        }
        Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
    };
    CaptureMetadata::from_manifest(id, &manifest)
}

/// Old refined ZIPs did not retain time metadata. Recover the original manifest
/// when available; a genuinely absent legacy manifest keeps its time unknown.
pub async fn ensure_refined_capture_metadata(io: &TaskIo, id: &str, sfm_dir: &Path) -> Result<()> {
    let metadata_path = sfm_dir.join(CAPTURE_METADATA_FILE);
    match fs::read(&metadata_path).await {
        Ok(bytes) => {
            let metadata: CaptureMetadata = serde_json::from_slice(&bytes)?;
            return metadata.validate(id);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let name = format!("dmt_manifest_{id}");
    let manifests = io.find(&name, Some("dmt_manifest_json")).await?;
    let matches: Vec<_> = manifests
        .into_iter()
        .filter(|item| item.name == name && item.data_type == "dmt_manifest_json")
        .collect();
    if matches.len() > 1 {
        bail!("ambiguous capture manifest for {id}");
    }
    let metadata = if let Some(manifest) = matches.first() {
        let source_path = sfm_dir.join(".source_capture_manifest.json");
        io.download_to(manifest.id, manifest.size, &source_path)
            .await?;
        let result = read_capture_manifest(id, &source_path);
        fs::remove_file(&source_path).await?;
        result?
    } else {
        CaptureMetadata::from_manifest(id, &Value::Null)?
    };
    fs::write(metadata_path, serde_json::to_vec(&metadata)?).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const A: &str = "8c4869c0-99c5-4515-8545-e3674d383338";
    const B: &str = "7d909fbe-20fa-4a13-8f88-aa5d8b91ceef";

    #[test]
    fn uuid_and_legacy_artifacts_keep_their_identity() {
        for id in [A, B, "2025-03-19_10-57-00", "2025-03-19-10-57-00"] {
            for prefix in [
                "dmt_manifest",
                "dmt_recording",
                "dmt_human_occlusion",
                "refined_scan",
            ] {
                assert_eq!(artifact_capture_id(&format!("{prefix}_{id}")), Some(id));
            }
            assert_eq!(manifest_capture_id(&format!("dmt_scan_{id}")), Some(id));
        }
        assert_eq!(artifact_capture_id(&format!("dmt_manifest_../{A}")), None);
        assert_eq!(
            artifact_capture_id(&format!("dmt_manifest_{A}/suffix")),
            None
        );
    }

    #[test]
    fn identical_times_do_not_identify_a_capture() {
        let time = "2026-10-13T12:34:56.1234567Z";
        for id in [A, B] {
            let metadata =
                CaptureMetadata::from_manifest(id, &json!({"id":id,"scanStartTime":time})).unwrap();
            assert_eq!(metadata.capture_id, id);
            assert_eq!(metadata.scan_start_time.as_deref(), Some(time));
        }
        assert!(CaptureMetadata::from_manifest(A, &json!({"id":B,"scanStartTime":time})).is_err());
        assert!(CaptureMetadata::from_manifest(A, &json!({"id":A})).is_err());
        assert!(CaptureMetadata::from_manifest("2025-03-19_10-57-00", &Value::Null).is_ok());
    }
}
