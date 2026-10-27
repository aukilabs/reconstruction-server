//! The capture zip: download, safe extraction, and finding the capture folder in it.
//!
//! Local refinement of a DMT scan receives one Domain artifact per scan file; an Auki session
//! arrives as a single zip of the whole capture folder (e.g.
//! `scan_path_recording_2026-08-05_09-33-06.zip`). It is expanded here so Python sees a plain
//! capture folder on disk.

use std::collections::VecDeque;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use node_host::{Materialized, TaskContext, TaskIo};
use runner_reconstruction_common::workspace::{sanitize_segment, Workspace};
use tokio::task;
use tracing::{info, warn};
use zip::ZipArchive;

/// Marks a session folder; `registries/` and `scan_report.json` sit next to sessions, never in
/// them. reconstruction/io/auki_session.py's resolve_auki_session_id uses the same rule.
const SESSION_MARKER_DIR: &str = "sensorlogs";

/// How deep below the extraction root to look for the capture folder: the archive's root, or a
/// wrapper directory or two inside it.
const APP_ROOT_SEARCH_DEPTH: usize = 3;

/// First four bytes of every local zip file header.
const ZIP_MAGIC: [u8; 4] = [0x50, 0x4b, 0x03, 0x04];

/// A capture zip expanded into the workspace.
#[derive(Debug, Clone)]
pub struct MaterializedSession {
    /// Scan identifier derived from the input artifact; the refined scan is named after it.
    pub scan_name: String,
    /// The capture folder (the SDK's app root) inside `datasets/<scan_name>`, where the zip was
    /// expanded: `registries/` and one folder per session. Passed to Python as `--dataset_path`.
    pub app_root: PathBuf,
    /// Session ids found under `app_root`.
    pub session_ids: Vec<String>,
    /// Number of files the extraction wrote.
    pub extracted_files: usize,
}

/// Download the task's single input, the capture zip, from the Domain and expand it.
pub async fn materialize_session_zip(
    task: &TaskContext,
    io: &TaskIo,
    workspace: &Workspace,
) -> Result<MaterializedSession> {
    let cids = &task.task.inputs_cids;
    let cid = match cids.as_slice() {
        [cid] => cid,
        [] => return Err(anyhow!("expected one capture zip input, got none")),
        many => {
            return Err(anyhow!(
                "expected one capture zip input, got {} ({})",
                many.len(),
                many.join(", ")
            ))
        }
    };

    let materialized = io
        .materialize(cid)
        .await
        .with_context(|| format!("materialize input CID {}", cid))?;
    // A guard, not a call on the happy path: the download is as large as the capture, and
    // leaking one per failed attempt fills the disk (observed: 522 MB per retry).
    let _input_cleanup = TempInputCleanup::for_materialized(&materialized, workspace);

    if !has_zip_magic(&materialized.path) {
        return Err(anyhow!(
            "input {} ({}) is not a zip archive",
            cid,
            materialized.name.as_deref().unwrap_or("<unnamed>")
        ));
    }
    let scan_name = derive_scan_name(&materialized);
    expand_session_zip(&materialized.path, scan_name, workspace).await
}

/// Expand a capture zip into `datasets/<scan_name>` and find the capture folder in it.
pub async fn expand_session_zip(
    zip_path: &Path,
    scan_name: String,
    workspace: &Workspace,
) -> Result<MaterializedSession> {
    let dataset_dir = workspace.datasets().join(&scan_name);
    fs::create_dir_all(&dataset_dir)
        .with_context(|| format!("create dataset directory {}", dataset_dir.display()))?;
    info!(zip = %zip_path.display(), scan = %scan_name, dest = %dataset_dir.display(), "expanding capture zip");

    // Straight from the download into the workspace: captures run to several GB, so no
    // second copy of the archive.
    let extracted_files = extract_zip(zip_path, &dataset_dir).await.with_context(|| {
        format!(
            "extract capture zip {} into {}",
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
        "capture zip expanded"
    );

    Ok(MaterializedSession {
        scan_name,
        app_root,
        session_ids,
        extracted_files,
    })
}

/// Removes the download's temporary directory when dropped, success or failure.
struct TempInputCleanup(Option<PathBuf>);

impl TempInputCleanup {
    /// Takes the directory only when it is separate from the job workspace; never one that
    /// overlaps it, which would delete the extracted capture.
    fn for_materialized(materialized: &Materialized, workspace: &Workspace) -> Self {
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

/// By content, not extension: node-host names every download `<name>.<data_type>`.
fn has_zip_magic(path: &Path) -> bool {
    let mut header = [0u8; 4];
    match fs::File::open(path).and_then(|mut f| f.read_exact(&mut header)) {
        Ok(()) => header == ZIP_MAGIC,
        Err(_) => false,
    }
}

/// The scan identifier for this input: the `datasets/<scan>/` folder node-host derived from the
/// artifact name (the capture timestamp when the name has one,
/// `scan_path_recording_2026-08-05_09-33-06.zip` -> `2026-08-05_09-33-06`), so refined scans
/// are named the way `/reconstruction/local-refinement/v1` names them. Falls back to the
/// artifact name, then the file name.
fn derive_scan_name(materialized: &Materialized) -> String {
    if let Ok(relative) = materialized
        .path
        .strip_prefix(materialized.root_dir.join("datasets"))
    {
        let mut components = relative.components();
        if let Some(first) = components.next() {
            // Only a directory qualifies; a file directly in datasets/ has no folder name.
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

    let stem = materialized
        .path
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
        .with_context(|| format!("open capture zip {}", zip_path.display()))?;
    let mut archive = ZipArchive::new(io::BufReader::new(file))
        .with_context(|| format!("read capture zip {}", zip_path.display()))?;

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

/// A zip entry name reduced to ordinary components, so it cannot escape the extraction root.
/// Absolute prefixes and `..` are dropped rather than the entry: real archives are made from a
/// parent-relative directory (every name starting `../`), and dropping those would extract an
/// empty tree and fail later with a far less obvious error. None if nothing is left.
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

/// The capture folder below `extract_root`: the first directory, breadth first, that owns a
/// session folder.
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
    use runner_reconstruction_common::workspace::Stage;
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

    fn materialized(root_dir: &Path, path: PathBuf, name: Option<&str>) -> Materialized {
        Materialized {
            cid: "cid".into(),
            path,
            data_id: None,
            name: name.map(str::to_string),
            data_type: None,
            domain_id: None,
            root_dir: root_dir.to_path_buf(),
            related_files: Vec::new(),
            extracted_paths: Vec::new(),
        }
    }

    fn workspace(base: &Path) -> Workspace {
        Workspace::create(Stage::Local, Some(base), "domain", Some("job"), "task").unwrap()
    }

    #[test]
    fn temp_input_cleanup_removes_download_dir_on_drop() {
        let temp = TempDir::new().unwrap();
        let download_dir = temp.path().join("domain-input-1");
        let zip = download_dir.join("datasets/scan/payload");
        fs::create_dir_all(zip.parent().unwrap()).unwrap();
        fs::write(&zip, b"PK\x03\x04").unwrap();

        let workspace = workspace(&temp.path().join("ws"));
        let materialized = materialized(&download_dir, zip, None);
        {
            let _guard = TempInputCleanup::for_materialized(&materialized, &workspace);
            assert!(download_dir.is_dir());
        }
        assert!(!download_dir.exists());
    }

    #[test]
    fn temp_input_cleanup_never_deletes_the_workspace() {
        let temp = TempDir::new().unwrap();
        let workspace = workspace(temp.path());
        // The download landed inside the job workspace itself.
        let inside = workspace.root().join("domain-input-1");
        fs::create_dir_all(&inside).unwrap();
        let materialized = materialized(&inside, inside.join("payload"), None);
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
    fn scan_name_prefers_the_dataset_folder() {
        let temp = TempDir::new().unwrap();
        let zip = temp
            .path()
            .join("datasets/2026-08-05_09-33-06/scan_path_recording_2026-08-05_09-33-06.zip.auki_capture_zip");
        let materialized = materialized(
            temp.path(),
            zip,
            Some("scan_path_recording_2026-08-05_09-33-06.zip"),
        );

        assert_eq!(derive_scan_name(&materialized), "2026-08-05_09-33-06");
    }

    #[test]
    fn scan_name_falls_back_to_artifact_name() {
        let temp = TempDir::new().unwrap();
        let materialized = materialized(
            temp.path(),
            temp.path().join("payload.bin"),
            Some("scan_path_recording_2026-08-05_09-33-06.zip"),
        );

        assert_eq!(
            derive_scan_name(&materialized),
            "scan_path_recording_2026-08-05_09-33-06"
        );
    }

    #[test]
    fn detects_zip_by_magic_bytes() {
        let temp = TempDir::new().unwrap();
        let zip = temp.path().join("payload.auki_capture_zip");
        fs::write(&zip, b"PK\x03\x04rest").unwrap();
        let text = temp.path().join("notes.txt");
        fs::write(&text, b"plain text").unwrap();

        assert!(has_zip_magic(&zip));
        assert!(!has_zip_magic(&text));
        assert!(!has_zip_magic(&temp.path().join("missing")));
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
        assert_eq!(extract_zip_blocking(&zip_path, &dest).unwrap(), 3);
        assert_eq!(resolve_app_root(&dest).unwrap(), dest.join("capture"));
    }

    #[test]
    fn contains_traversal_entries_inside_destination() {
        let temp = TempDir::new().unwrap();
        let zip_path = temp.path().join("session.zip");
        // Zipped from a parent-relative directory: every entry starts `../`. The prefix is
        // stripped rather than the entry dropped.
        write_zip(
            &zip_path,
            &[
                "../capture/R001-abc/sensorlogs/head_left_rgb/log_manifest.json",
                "/absolute/capture/R001-abc/sensorlogs/right_arm_rgb/log_manifest.json",
            ],
        );

        let dest = temp.path().join("out");
        assert_eq!(extract_zip_blocking(&zip_path, &dest).unwrap(), 2);
        assert!(dest
            .join("capture/R001-abc/sensorlogs/head_left_rgb/log_manifest.json")
            .is_file());
        assert!(dest
            .join("absolute/capture/R001-abc/sensorlogs/right_arm_rgb/log_manifest.json")
            .is_file());
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

    #[tokio::test]
    async fn expands_a_capture_zip_into_the_workspace() {
        let temp = TempDir::new().unwrap();
        let zip_path = temp.path().join("capture.zip");
        write_zip(
            &zip_path,
            &[
                "capture/registries/sensors/galbot/entry.json",
                "capture/R001-abc/sensorlogs/head_left_rgb/log_manifest.json",
            ],
        );
        let workspace = workspace(&temp.path().join("ws"));

        let session = expand_session_zip(&zip_path, "2026-08-05_09-33-06".into(), &workspace)
            .await
            .unwrap();

        assert_eq!(
            session.app_root,
            workspace.datasets().join("2026-08-05_09-33-06/capture")
        );
        assert_eq!(session.session_ids, vec!["R001-abc"]);
        assert_eq!(session.extracted_files, 2);
    }
}
