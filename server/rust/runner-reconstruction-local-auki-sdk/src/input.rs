//! Input materialization for the Auki SDK local refinement capability.
//!
//! Unlike `/reconstruction/local-refinement/v1` -- which receives one Domain artifact
//! per DMT scan file (ARposes.csv, Frames.mp4, ...) -- this capability receives one
//! small Domain artifact describing a Robot-hosted Auki capture ZIP. The ZIP is fetched
//! through `TaskCtx`'s authenticated P2P dataset handle and expanded here, on the Rust
//! side, so the Python entrypoint still sees the same plain on-disk capture folder.

use std::collections::VecDeque;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use compute_runner_api::{MaterializedInput, P2pDatasetReference, TaskCtx, P2P_DATASET_SCHEMA};
use libp2p_identity::PeerId;
use multiaddr::{Multiaddr, Protocol};
use serde::Deserialize;
use tokio::{fs as async_fs, task};
use tracing::{info, warn};
use uuid::Uuid;
use zip::ZipArchive;

use crate::workspace::{sanitize_segment, Workspace};

/// Directory that marks an Auki *session* folder. `registries/` and `scan_report.json`
/// sit next to session folders, never inside them, so this is the one unambiguous
/// marker -- the same rule `utils/auki_refinement_util.py::resolve_auki_session_id`
/// uses on the Python side.
const SESSION_MARKER_DIR: &str = "sensorlogs";

/// How many directory levels below the extraction root we search for the capture
/// folder. Covers a zip rooted at the capture folder itself and the common case of a
/// single wrapper directory inside the archive.
const APP_ROOT_SEARCH_DEPTH: usize = 3;

const RECORDING_REFERENCE_DATA_TYPE: &str = "scan_path_recording_reference_json";
const MAX_REFERENCE_BYTES: u64 = 64 * 1024;

/// A single Auki capture session ZIP, fetched over P2P and expanded into the workspace.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MaterializedSession {
    pub cid: String,
    pub data_id: Option<String>,
    pub name: Option<String>,
    pub data_type: Option<String>,
    pub domain_id: Option<String>,
    /// Scan identifier derived from the input artifact. Reused as the refinement output
    /// folder name so uploads keep local refinement's `refined_scan_<scan>` convention.
    pub scan_name: String,
    /// `datasets/<scan_name>` -- where the zip was expanded.
    pub dataset_dir: PathBuf,
    /// The Auki capture folder ("app root") inside `dataset_dir`: the folder holding
    /// `registries/` alongside one subfolder per captured session. This is what gets
    /// passed to the Python entrypoint as `--dataset_path`.
    pub app_root: PathBuf,
    /// Session ids discovered under `app_root`.
    pub session_ids: Vec<String>,
    /// Number of files written by the extraction.
    pub extracted_files: usize,
}

/// Materialize the task's single Domain reference, fetch its ZIP from the Robot over
/// authenticated P2P, and expand it into the existing workspace datasets directory.
pub async fn materialize_session_zip(
    ctx: &TaskCtx<'_>,
    workspace: &Workspace,
) -> Result<MaterializedSession> {
    let lease_domain = ctx
        .lease
        .domain_id
        .context("local Auki refinement lease is missing the Domain required for P2P input")?;
    let cids = &ctx.lease.task.inputs_cids;
    let cid = match cids.len() {
        1 => &cids[0],
        0 => {
            return Err(anyhow!(
                "local Auki refinement expects one recording reference input, got none"
            ))
        }
        n => {
            return Err(anyhow!(
                "local Auki refinement expects one recording reference input, got {} ({})",
                n,
                cids.join(", ")
            ))
        }
    };
    let p2p_dataset = ctx
        .p2p_dataset
        .context("authenticated P2P dataset handle is unavailable for Auki session input")?;

    let materialized = ctx
        .input
        .materialize_cid_with_meta(cid)
        .await
        .with_context(|| format!("materialize input CID {}", cid))?;

    // Domain Server supplies only the small JSON reference. Keep the existing cleanup
    // guard so both valid and rejected references are removed on every exit path.
    let _input_cleanup = TempInputCleanup::for_materialized(&materialized, workspace);

    let reference = read_and_validate_reference(&materialized, lease_domain, Utc::now())
        .await
        .with_context(|| format!("validate P2P recording reference for CID {cid}"))?;
    let scan_name = derive_scan_name(&materialized, &materialized.path);

    let input_dir = workspace.root().join("inputs");
    async_fs::create_dir_all(&input_dir)
        .await
        .with_context(|| format!("create P2P input directory {}", input_dir.display()))?;
    let zip_path = input_dir.join(format!("{}.zip", reference.dataset_id));
    info!(
        cid = %cid,
        dataset_id = %reference.dataset_id,
        peer_id = %reference.peer_id,
        scan = %scan_name,
        destination = %zip_path.display(),
        "fetching Auki session ZIP over authenticated P2P"
    );
    p2p_dataset
        .fetch(&reference, &zip_path)
        .await
        .with_context(|| {
            format!(
                "fetch P2P dataset {} from Robot {}",
                reference.dataset_id, reference.peer_id
            )
        })?;

    let dataset_dir = workspace.datasets().join(&scan_name);
    fs::create_dir_all(&dataset_dir)
        .with_context(|| format!("create dataset directory {}", dataset_dir.display()))?;

    info!(
        cid = %cid,
        artifact_name = materialized.name.as_deref().unwrap_or("<unnamed>"),
        zip = %zip_path.display(),
        scan = %scan_name,
        dest = %dataset_dir.display(),
        "expanding auki session zip"
    );

    // The Compute Node adapter has already streamed this ZIP into the task workspace and
    // verified its size and SHA-256 against the Domain reference. Keep the established
    // safe extraction path and Python-facing directory layout unchanged.
    let extracted_files = extract_zip(&zip_path, &dataset_dir)
        .await
        .with_context(|| {
            format!(
                "extract session zip {} into {}",
                zip_path.display(),
                dataset_dir.display()
            )
        })?;

    let app_root = resolve_app_root(&dataset_dir)?;
    let session_ids = collect_session_ids(&app_root)?;
    info!(
        scan = %scan_name,
        files = extracted_files,
        app_root = %app_root.display(),
        sessions = ?session_ids,
        "auki session zip expanded"
    );

    Ok(MaterializedSession {
        cid: materialized.cid.clone(),
        data_id: materialized.data_id.clone(),
        name: materialized.name.clone(),
        data_type: materialized.data_type.clone(),
        domain_id: materialized.domain_id.clone(),
        scan_name,
        dataset_dir,
        app_root,
        session_ids,
        extracted_files,
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetReferenceDocument {
    schema: String,
    dataset_id: String,
    domain_id: Uuid,
    name: String,
    peer_id: String,
    multiaddrs: Vec<String>,
    size_bytes: u64,
    sha256: String,
    available_until: DateTime<Utc>,
}

async fn read_and_validate_reference(
    materialized: &MaterializedInput,
    lease_domain: Uuid,
    now: DateTime<Utc>,
) -> Result<P2pDatasetReference> {
    if materialized.data_type.as_deref() != Some(RECORDING_REFERENCE_DATA_TYPE) {
        anyhow::bail!(
            "Auki session input must have Domain data type {RECORDING_REFERENCE_DATA_TYPE}, got {}",
            materialized.data_type.as_deref().unwrap_or("<missing>")
        );
    }
    let source_domain = materialized
        .domain_id
        .as_deref()
        .context("recording reference Domain metadata is missing")?;
    let source_domain = Uuid::parse_str(source_domain)
        .context("recording reference Domain metadata is not a UUID")?;
    if source_domain != lease_domain {
        anyhow::bail!("recording reference Domain metadata does not match the task lease");
    }

    let metadata = async_fs::metadata(&materialized.path)
        .await
        .with_context(|| {
            format!(
                "inspect materialized recording reference {}",
                materialized.path.display()
            )
        })?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_REFERENCE_BYTES {
        anyhow::bail!(
            "materialized recording reference must be a non-empty file no larger than {} bytes",
            MAX_REFERENCE_BYTES
        );
    }
    let bytes = async_fs::read(&materialized.path).await.with_context(|| {
        format!(
            "read materialized recording reference {}",
            materialized.path.display()
        )
    })?;
    let document: DatasetReferenceDocument =
        serde_json::from_slice(&bytes).context("parse strict auki-p2p-dataset reference JSON")?;
    validate_reference_document(document, materialized, lease_domain, now)
}

fn validate_reference_document(
    document: DatasetReferenceDocument,
    materialized: &MaterializedInput,
    lease_domain: Uuid,
    now: DateTime<Utc>,
) -> Result<P2pDatasetReference> {
    if document.schema != P2P_DATASET_SCHEMA {
        anyhow::bail!(
            "unsupported recording reference schema: {}",
            document.schema
        );
    }
    let dataset_id = Uuid::parse_str(&document.dataset_id)
        .context("recording reference dataset_id must be a scan task UUID")?;
    if dataset_id.to_string() != document.dataset_id {
        anyhow::bail!("recording reference dataset_id must use canonical UUID form");
    }
    if document.domain_id != lease_domain {
        anyhow::bail!("recording reference Domain does not match the task lease");
    }
    if document.name.trim().is_empty() {
        anyhow::bail!("recording reference name is missing");
    }
    if !document.name.starts_with("scan_path_recording_") {
        anyhow::bail!("recording reference name does not preserve the scan timestamp prefix");
    }
    let artifact_name = materialized
        .name
        .as_deref()
        .context("recording reference Domain artifact name is missing")?;
    if document.name != artifact_name {
        anyhow::bail!("recording reference name does not match its Domain artifact name");
    }
    PeerId::from_str(&document.peer_id).context("recording reference Peer ID is invalid")?;
    validate_multiaddrs(&document.multiaddrs)?;
    if document.size_bytes == 0 {
        anyhow::bail!("recording reference size_bytes must be greater than zero");
    }
    let digest =
        hex::decode(&document.sha256).context("recording reference sha256 must be hexadecimal")?;
    if digest.len() != 32 {
        anyhow::bail!("recording reference sha256 must contain exactly 32 bytes");
    }
    if document.available_until <= now {
        anyhow::bail!("recording reference has expired");
    }

    Ok(P2pDatasetReference {
        schema: document.schema,
        dataset_id: document.dataset_id,
        domain_id: document.domain_id,
        name: document.name,
        peer_id: document.peer_id,
        multiaddrs: document.multiaddrs,
        size_bytes: document.size_bytes,
        sha256: document.sha256,
        available_until: document.available_until,
    })
}

fn validate_multiaddrs(addresses: &[String]) -> Result<()> {
    if addresses.is_empty() {
        anyhow::bail!("recording reference has no explicit advertised multiaddr");
    }
    for address in addresses {
        let parsed = Multiaddr::from_str(address)
            .with_context(|| format!("recording reference multiaddr is invalid: {address}"))?;
        let mut has_tcp = false;
        for protocol in parsed.iter() {
            match protocol {
                Protocol::Tcp(0) => {
                    anyhow::bail!("recording reference multiaddr uses ephemeral tcp/0: {address}")
                }
                Protocol::Tcp(_) => has_tcp = true,
                Protocol::Ip4(ip) if ip.is_unspecified() => anyhow::bail!(
                    "recording reference multiaddr uses an unspecified address: {address}"
                ),
                Protocol::Ip6(ip) if ip.is_unspecified() => anyhow::bail!(
                    "recording reference multiaddr uses an unspecified address: {address}"
                ),
                _ => {}
            }
        }
        if !has_tcp {
            anyhow::bail!("recording reference multiaddr is not TCP: {address}");
        }
    }
    Ok(())
}

/// Removes the Domain Server reference's temporary download directory on every exit
/// path from `materialize_session_zip`, success or failure.
struct TempInputCleanup(Option<PathBuf>);

impl TempInputCleanup {
    /// Only takes ownership of the directory when it is genuinely a separate temp dir --
    /// never when it overlaps the job workspace, which would delete the extracted capture.
    fn for_materialized(materialized: &MaterializedInput, workspace: &Workspace) -> Self {
        let root = &materialized.root_dir;
        let overlaps = workspace.root().starts_with(root) || root.starts_with(workspace.root());
        Self(if overlaps { None } else { Some(root.clone()) })
    }
}

impl Drop for TempInputCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            if let Err(err) = fs::remove_dir_all(&path) {
                warn!(error = %err, path = %path.display(), "failed to remove temporary input directory");
            }
        }
    }
}

/// Derive the scan identifier for this input.
///
/// Preferred source is the `datasets/<scan>/` folder the compute node's storage layer
/// already derived from the Domain reference artifact name (for example,
/// `scan_path_recording_2026-08-05_09-33-06` -> `2026-08-05_09-33-06`).
/// Reusing it means this capability names its refined scans exactly the way
/// `/reconstruction/local-refinement/v1` does, instead of inventing a second rule.
fn derive_scan_name(materialized: &MaterializedInput, source_path: &Path) -> String {
    if let Ok(relative) = source_path.strip_prefix(materialized.root_dir.join("datasets")) {
        let mut components = relative.components();
        if let Some(first) = components.next() {
            // Only a directory component qualifies; a zip sitting directly in
            // `datasets/` has no folder name to borrow.
            if components.next().is_some() {
                return sanitize_segment(&first.as_os_str().to_string_lossy());
            }
        }
    }

    if let Some(name) = materialized.name.as_deref() {
        let stem = name.strip_suffix(".zip").unwrap_or(name);
        if !stem.trim().is_empty() {
            let scan_name = stem.strip_prefix("scan_path_recording_").unwrap_or(stem);
            return sanitize_segment(scan_name);
        }
    }

    let stem = source_path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    sanitize_segment(&stem)
}

async fn extract_zip(zip_path: &Path, dest: &Path) -> Result<usize> {
    let zip_path = zip_path.to_path_buf();
    let dest = dest.to_path_buf();
    task::spawn_blocking(move || extract_zip_blocking(&zip_path, &dest)).await?
}

fn extract_zip_blocking(zip_path: &Path, dest: &Path) -> Result<usize> {
    let file = fs::File::open(zip_path)
        .with_context(|| format!("open session zip {}", zip_path.display()))?;
    let mut archive = ZipArchive::new(io::BufReader::new(file))
        .with_context(|| format!("read session zip {}", zip_path.display()))?;

    let mut written = 0usize;
    let mut rewritten = 0usize;
    let mut skipped = 0usize;
    for idx in 0..archive.len() {
        let mut entry = archive.by_index(idx)?;
        let raw_name = entry.name().to_string();
        let relative = match sanitize_entry_path(&raw_name) {
            Some(sanitized) => {
                if sanitized.as_os_str() != Path::new(&raw_name).as_os_str() {
                    rewritten += 1;
                }
                sanitized
            }
            None => {
                skipped += 1;
                continue;
            }
        };
        let out_path = dest.join(relative);

        if entry.is_dir() {
            fs::create_dir_all(&out_path)
                .with_context(|| format!("create directory {}", out_path.display()))?;
            continue;
        }
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create directory {}", parent.display()))?;
        }
        let out_file = fs::File::create(&out_path)
            .with_context(|| format!("create {}", out_path.display()))?;
        let mut out = io::BufWriter::new(out_file);
        io::copy(&mut entry, &mut out).with_context(|| format!("write {}", out_path.display()))?;
        out.flush()
            .with_context(|| format!("flush {}", out_path.display()))?;
        written += 1;
    }

    if rewritten > 0 || skipped > 0 {
        warn!(
            rewritten,
            skipped,
            zip = %zip_path.display(),
            "normalized zip entry paths that pointed outside the archive root"
        );
    }

    Ok(written)
}

/// Reduce a zip entry name to a path that cannot escape the extraction root, keeping
/// only ordinary path components. Absolute prefixes and `..` are dropped rather than
/// rejecting the entry: real archives get produced from a parent-relative working
/// directory (every name prefixed `../`), and dropping those entries would extract an
/// empty tree and fail later with a far less obvious error. Returns `None` when nothing
/// usable is left.
fn sanitize_entry_path(name: &str) -> Option<PathBuf> {
    let sanitized: PathBuf = Path::new(name)
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(part) => Some(part),
            _ => None,
        })
        .collect();

    if sanitized.as_os_str().is_empty() {
        None
    } else {
        Some(sanitized)
    }
}

/// Locate the Auki capture folder ("app root") below `extract_root`: the directory that
/// owns at least one session subfolder. Handles both an archive rooted at the capture
/// folder itself and one that wraps it in a single directory.
fn resolve_app_root(extract_root: &Path) -> Result<PathBuf> {
    let mut queue = VecDeque::new();
    queue.push_back((extract_root.to_path_buf(), 0usize));

    while let Some((dir, depth)) = queue.pop_front() {
        if !collect_session_ids(&dir)?.is_empty() {
            return Ok(dir);
        }
        if depth >= APP_ROOT_SEARCH_DEPTH {
            continue;
        }
        for entry in child_dirs(&dir)? {
            queue.push_back((entry, depth + 1));
        }
    }

    Err(anyhow!(
        "could not locate an Auki capture folder under {}: no directory within {} level(s) owns a \
         session subfolder containing '{}/'. Top-level entries: {}",
        extract_root.display(),
        APP_ROOT_SEARCH_DEPTH,
        SESSION_MARKER_DIR,
        list_entries(extract_root)
    ))
}

/// Session ids directly under `dir`: subfolders owning a `sensorlogs/` directory.
fn collect_session_ids(dir: &Path) -> Result<Vec<String>> {
    let mut sessions: Vec<String> = child_dirs(dir)?
        .into_iter()
        .filter(|child| child.join(SESSION_MARKER_DIR).is_dir())
        .filter_map(|child| {
            child
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
        })
        .collect();
    sessions.sort();
    Ok(sessions)
}

fn child_dirs(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err).with_context(|| format!("read_dir {}", dir.display())),
    };

    let mut dirs = Vec::new();
    for entry in entries.flatten() {
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            dirs.push(entry.path());
        }
    }
    dirs.sort();
    Ok(dirs)
}

fn list_entries(dir: &Path) -> String {
    match fs::read_dir(dir) {
        Ok(entries) => {
            let names: Vec<String> = entries
                .flatten()
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect();
            if names.is_empty() {
                "<empty>".to_string()
            } else {
                names.join(", ")
            }
        }
        Err(err) => format!("<unreadable: {}>", err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write_zip(zip_path: &Path, entries: &[&str]) {
        let file = fs::File::create(zip_path).unwrap();
        let mut writer = zip::ZipWriter::new(file);
        let options =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        for entry in entries {
            writer.start_file(*entry, options).unwrap();
            writer.write_all(b"{}").unwrap();
        }
        writer.finish().unwrap();
    }

    fn make_session(app_root: &Path, session_id: &str) {
        fs::create_dir_all(app_root.join(session_id).join(SESSION_MARKER_DIR)).unwrap();
        fs::create_dir_all(app_root.join(session_id).join("poselogs")).unwrap();
        fs::create_dir_all(app_root.join("registries")).unwrap();
    }

    #[test]
    fn temp_input_cleanup_removes_download_dir_on_drop() {
        let temp = TempDir::new().unwrap();
        let download_dir = temp.path().join("domain-input-1");
        let zip = download_dir.join("datasets/scan/payload");
        fs::create_dir_all(zip.parent().unwrap()).unwrap();
        fs::write(&zip, b"PK\x03\x04").unwrap();

        let workspace =
            Workspace::create(Some(&temp.path().join("ws")), "domain", Some("job"), "task")
                .unwrap();
        let mut materialized = MaterializedInput::new("cid", zip);
        materialized.root_dir = download_dir.clone();

        {
            let _guard = TempInputCleanup::for_materialized(&materialized, &workspace);
            assert!(download_dir.is_dir());
        }
        // Freed even though nothing called a cleanup function -- this is the path that
        // used to leak one full-size archive per failed attempt.
        assert!(!download_dir.exists());
    }

    #[test]
    fn temp_input_cleanup_never_deletes_the_workspace() {
        let temp = TempDir::new().unwrap();
        let workspace =
            Workspace::create(Some(temp.path()), "domain", Some("job"), "task").unwrap();
        // Pathological case: the download landed inside the job workspace itself.
        let inside = workspace.root().join("domain-input-1");
        fs::create_dir_all(&inside).unwrap();
        let mut materialized = MaterializedInput::new("cid", inside.join("payload"));
        materialized.root_dir = inside.clone();

        {
            let _guard = TempInputCleanup::for_materialized(&materialized, &workspace);
        }
        assert!(inside.is_dir(), "workspace content must survive the guard");
    }

    #[test]
    fn resolves_app_root_at_extraction_root() {
        let temp = TempDir::new().unwrap();
        make_session(temp.path(), "R001-abc");

        assert_eq!(resolve_app_root(temp.path()).unwrap(), temp.path());
        assert_eq!(collect_session_ids(temp.path()).unwrap(), vec!["R001-abc"]);
    }

    #[test]
    fn resolves_app_root_inside_wrapper_directory() {
        let temp = TempDir::new().unwrap();
        let wrapper = temp.path().join("scan_path_recording_2026-08-05_09-33-06");
        make_session(&wrapper, "R001-abc");

        assert_eq!(resolve_app_root(temp.path()).unwrap(), wrapper);
    }

    #[test]
    fn reports_missing_app_root() {
        let temp = TempDir::new().unwrap();
        fs::create_dir_all(temp.path().join("not-a-capture")).unwrap();

        let err = resolve_app_root(temp.path()).unwrap_err().to_string();
        assert!(err.contains("not-a-capture"), "{err}");
    }

    #[test]
    fn collects_every_session_sorted() {
        let temp = TempDir::new().unwrap();
        make_session(temp.path(), "R002-b");
        make_session(temp.path(), "R001-a");

        assert_eq!(
            collect_session_ids(temp.path()).unwrap(),
            vec!["R001-a", "R002-b"]
        );
    }

    #[test]
    fn scan_name_prefers_compute_node_dataset_folder() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().to_path_buf();
        let zip = root
            .join("datasets")
            .join("2026-08-05_09-33-06")
            .join("scan_path_recording_2026-08-05_09-33-06_zip.auki_session_zip");
        fs::create_dir_all(zip.parent().unwrap()).unwrap();
        fs::write(&zip, b"PK\x03\x04").unwrap();

        let mut materialized = MaterializedInput::new("cid", zip.clone());
        materialized.root_dir = root;
        materialized.name = Some("scan_path_recording_2026-08-05_09-33-06.zip".into());

        assert_eq!(derive_scan_name(&materialized, &zip), "2026-08-05_09-33-06");
    }

    #[test]
    fn scan_name_falls_back_to_artifact_name() {
        let temp = TempDir::new().unwrap();
        let zip = temp.path().join("payload.bin");
        fs::write(&zip, b"PK\x03\x04").unwrap();

        let mut materialized = MaterializedInput::new("cid", zip.clone());
        materialized.root_dir = temp.path().to_path_buf();
        materialized.name = Some("scan_path_recording_2026-08-05_09-33-06.zip".into());

        assert_eq!(derive_scan_name(&materialized, &zip), "2026-08-05_09-33-06");
    }

    #[test]
    fn extracts_archive_with_wrapper_directory() {
        let temp = TempDir::new().unwrap();
        let zip_path = temp.path().join("session.zip");
        write_zip(
            &zip_path,
            &[
                "capture/registries/sensors/galbot/entry.json",
                "capture/R001-abc/sensorlogs/head_left_rgb/log_manifest.json",
                "capture/R001-abc/poselogs/world__base_link/log_manifest.json",
            ],
        );

        let dest = temp.path().join("out");
        let written = extract_zip_blocking(&zip_path, &dest).unwrap();

        assert_eq!(written, 3);
        assert_eq!(resolve_app_root(&dest).unwrap(), dest.join("capture"));
    }

    #[test]
    fn contains_traversal_entries_inside_destination() {
        let temp = TempDir::new().unwrap();
        let zip_path = temp.path().join("session.zip");
        // An archive zipped from a parent-relative working directory: every entry is
        // prefixed `../` (real shape, see /data/zip/teleop-scan.zip). The prefix is
        // stripped rather than treated as a reason to drop the entry.
        write_zip(
            &zip_path,
            &[
                "../capture/R001-abc/sensorlogs/head_left_rgb/log_manifest.json",
                "/absolute/capture/R001-abc/sensorlogs/right_arm_rgb/log_manifest.json",
            ],
        );

        let dest = temp.path().join("out");
        let written = extract_zip_blocking(&zip_path, &dest).unwrap();

        assert_eq!(written, 2);
        assert!(dest
            .join("capture/R001-abc/sensorlogs/head_left_rgb/log_manifest.json")
            .is_file());
        assert!(dest
            .join("absolute/capture/R001-abc/sensorlogs/right_arm_rgb/log_manifest.json")
            .is_file());
        // Nothing escaped the destination.
        assert!(!temp.path().join("capture").exists());
        assert_eq!(resolve_app_root(&dest).unwrap(), dest.join("capture"));
    }

    #[test]
    fn sanitizes_entry_paths() {
        assert_eq!(
            sanitize_entry_path("../capture/a.json").unwrap(),
            Path::new("capture/a.json")
        );
        assert_eq!(
            sanitize_entry_path("/capture/a.json").unwrap(),
            Path::new("capture/a.json")
        );
        assert_eq!(
            sanitize_entry_path("capture/./a.json").unwrap(),
            Path::new("capture/a.json")
        );
        assert!(sanitize_entry_path("..").is_none());
        assert!(sanitize_entry_path("/").is_none());
        assert!(sanitize_entry_path("").is_none());
    }
}

#[cfg(test)]
#[path = "input_p2p_tests.rs"]
mod p2p_tests;
