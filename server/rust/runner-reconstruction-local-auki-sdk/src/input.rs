//! Input materialization for the Auki SDK local refinement capability.
//!
//! Unlike `/reconstruction/local-refinement/v1` -- which receives one Domain artifact
//! per DMT scan file (ARposes.csv, Frames.mp4, ...) and only has to normalize their
//! file names -- this capability receives a *single* zip holding a whole Auki capture
//! folder (e.g. `scan_path_recording_2026-08-05_09-33-06.zip`). The zip is expanded
//! here, on the Rust side, so the Python entrypoint sees a plain on-disk capture
//! folder exactly like a manually unpacked session.

use std::collections::VecDeque;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use compute_runner_api::{MaterializedInput, TaskCtx};
use tokio::task;
use tracing::{info, warn};
use walkdir::WalkDir;
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

/// First four bytes of every local zip file header.
const ZIP_MAGIC: [u8; 4] = [0x50, 0x4b, 0x03, 0x04];

/// A single Auki capture session zip, downloaded and expanded into the workspace.
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

/// Download the task's single session zip and expand it into the workspace datasets
/// directory, returning where the capture folder landed.
pub async fn materialize_session_zip(
    ctx: &TaskCtx<'_>,
    workspace: &Workspace,
) -> Result<MaterializedSession> {
    let cids = &ctx.lease.task.inputs_cids;
    let cid = match cids.len() {
        1 => &cids[0],
        0 => {
            return Err(anyhow!(
                "local auki refinement expects a single session zip input, got none"
            ))
        }
        n => {
            return Err(anyhow!(
                "local auki refinement expects a single session zip input, got {} ({})",
                n,
                cids.join(", ")
            ))
        }
    };

    let materialized = ctx
        .input
        .materialize_cid_with_meta(cid)
        .await
        .with_context(|| format!("materialize input CID {}", cid))?;

    // Drop guard rather than a cleanup call at the end of the happy path: the downloaded
    // archive is as large as the capture itself, and leaking one copy per failed attempt
    // is enough to fill the disk and turn a single recoverable failure into a run of
    // unrecoverable ones (observed: three retries, 522 MB leaked each).
    let _input_cleanup = TempInputCleanup::for_materialized(&materialized, workspace);

    let zip_path = find_session_zip(&materialized.root_dir)
        .with_context(|| format!("locate session zip for CID {}", cid))?;
    let scan_name = derive_scan_name(&materialized, &zip_path);
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

    // Expanded straight from the downloaded zip into the workspace (rather than copying
    // the archive in first, as local refinement does with its dataset files) -- these
    // captures run to multiple GB and a second on-disk copy is not worth the space.
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

/// Removes the compute node's temporary download directory on every exit path from
/// `materialize_session_zip`, success or failure.
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

/// Find the downloaded zip below `root`. Identified by content (local file header
/// magic) rather than extension: the compute node renames every downloaded artifact to
/// `<name>.<data_type>`, so the on-disk file rarely ends in `.zip`.
fn find_session_zip(root: &Path) -> Result<PathBuf> {
    let mut candidates = Vec::new();
    for entry in WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        if has_zip_magic(entry.path()) {
            candidates.push(entry.path().to_path_buf());
        }
    }

    match candidates.len() {
        1 => Ok(candidates.remove(0)),
        0 => Err(anyhow!(
            "no zip archive found under materialized input {}",
            root.display()
        )),
        _ => Err(anyhow!(
            "expected exactly one zip archive under materialized input {}, found {}: {}",
            root.display(),
            candidates.len(),
            candidates
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn has_zip_magic(path: &Path) -> bool {
    let mut header = [0u8; 4];
    match fs::File::open(path).and_then(|mut f| f.read_exact(&mut header)) {
        Ok(()) => header == ZIP_MAGIC,
        Err(_) => false,
    }
}

/// Derive the scan identifier for this input.
///
/// Preferred source is the `datasets/<scan>/` folder the compute node's storage layer
/// already derived from the artifact name (the capture timestamp when the name carries
/// one, e.g. `scan_path_recording_2026-08-05_09-33-06.zip` -> `2026-08-05_09-33-06`).
/// Reusing it means this capability names its refined scans exactly the way
/// `/reconstruction/local-refinement/v1` does, instead of inventing a second rule.
fn derive_scan_name(materialized: &MaterializedInput, zip_path: &Path) -> String {
    if let Ok(relative) = zip_path.strip_prefix(materialized.root_dir.join("datasets")) {
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
            return sanitize_segment(stem);
        }
    }

    let stem = zip_path
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

        assert_eq!(
            derive_scan_name(&materialized, &zip),
            "scan_path_recording_2026-08-05_09-33-06"
        );
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

    #[test]
    fn finds_zip_by_magic_bytes() {
        let temp = TempDir::new().unwrap();
        let nested = temp.path().join("datasets").join("scan");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("notes.txt"), b"plain text").unwrap();
        let zip = nested.join("payload.auki_session_zip");
        fs::write(&zip, b"PK\x03\x04rest").unwrap();

        assert_eq!(find_session_zip(temp.path()).unwrap(), zip);
    }
}
