//! Task-scoped Domain input/output on the SDK data client.
//!
//! Ported from `posemesh-compute-node` storage. File layout, artifact naming,
//! upsert lookup and error text match the 0.3.2 host that produced the
//! artifacts downstream consumers (DMT, peyote, later pipeline stages) read.
use anyhow::{anyhow, Context, Result};
use auki_sdk::{
    AukiDomainData, AuthError, DataError, DataLimits, DataListQuery, DataMetadata, DataWrite,
    DomainDataClient, TaskContext, TransferOptions,
};
use parking_lot::Mutex;
use regex::Regex;
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

/// Largest payload sent as one buffered request; larger files stream as multipart.
pub const BUFFER_LIMIT: usize = 64 * 1024 * 1024;

/// Storage failures. Display strings are those of the 0.3.2 host because
/// runners match on them (e.g. a 409 conflict on re-upload).
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("bad request (400)")]
    BadRequest,
    #[error("unauthorized (401)")]
    Unauthorized,
    #[error("not found (404)")]
    NotFound,
    #[error("conflict (409)")]
    Conflict,
    #[error("server error ({0})")]
    Server(u16),
    #[error("network error: {0}")]
    Network(String),
    #[error("other storage error: {0}")]
    Other(String),
}

/// One materialized input: the primary file plus any further matches.
#[derive(Debug, Clone)]
pub struct Materialized {
    pub cid: String,
    pub path: PathBuf,
    pub data_id: Option<String>,
    pub name: Option<String>,
    pub data_type: Option<String>,
    pub domain_id: Option<String>,
    pub root_dir: PathBuf,
    pub related_files: Vec<PathBuf>,
    pub extracted_paths: Vec<PathBuf>,
}

pub enum ArtifactContent<'a> {
    Bytes(&'a [u8]),
    File(&'a Path),
}

/// A named Domain artifact upload; `existing_id` forces an update of that item.
pub struct ArtifactRequest<'a> {
    pub rel_path: &'a str,
    pub name: &'a str,
    pub data_type: &'a str,
    pub existing_id: Option<&'a str>,
    pub content: ArtifactContent<'a>,
}

/// One uploaded artifact as reported in the DMS receipt.
/// Exactly these four keys: the 0.3.2 receipt shape.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct UploadedArtifact {
    pub logical_path: String,
    pub name: String,
    pub data_type: String,
    pub id: Option<String>,
}

/// Domain access for one task lease.
pub struct TaskIo {
    data: DomainDataClient,
    cancellation: CancellationToken,
    base: Option<Url>,
    outputs_prefix: Option<String>,
    task_id: Uuid,
    uploads: Mutex<HashMap<String, UploadedArtifact>>,
}

impl TaskIo {
    pub fn new(task: &TaskContext) -> Result<Self> {
        let lease = task.credential.lease_snapshot()?;
        // The 0.3.2 host refused leases without a Domain Server (node_setup_failed).
        let base = lease
            .domain_server_url
            .ok_or_else(|| anyhow!("lease missing domain_server_url"))?;
        let data = AukiDomainData::with_limits(
            task.credential.clone(),
            DataLimits {
                max_data_bytes: BUFFER_LIMIT,
                ..DataLimits::default()
            },
        )?
        .in_domain(task.credential.domain_id());
        Ok(Self::from_parts(
            data,
            task.cancellation(),
            Some(base),
            task.task.outputs_prefix.clone(),
            task.task.id,
        ))
    }

    pub fn from_parts(
        data: DomainDataClient,
        cancellation: CancellationToken,
        base: Option<Url>,
        outputs_prefix: Option<String>,
        task_id: Uuid,
    ) -> Self {
        Self {
            data,
            cancellation,
            base,
            outputs_prefix,
            task_id,
            uploads: Mutex::new(HashMap::new()),
        }
    }

    pub fn domain_id(&self) -> Uuid {
        self.data.domain_id()
    }

    pub async fn close(&self) {
        self.data.close().await;
    }

    /// Download the data a CID names into a fresh temp root.
    ///
    /// Accepts a data UUID or an absolute/relative Domain data URL
    /// (`…/domains/{d}/data/{id}` or `…/domains/{d}/data?ids=&name=&data_type=`).
    /// Each item lands at `<root>/datasets/<timestamp-or-name>/<name>.<data_type>`.
    pub async fn materialize(&self, cid: &str) -> Result<Materialized> {
        let query = self.parse_cid(cid).map_err(|e| anyhow!(e))?;
        let mut parts = self.download(query).await.map_err(|e| anyhow!(e))?;
        if parts.is_empty() {
            return Err(anyhow!("domain response missing data for {}", cid));
        }
        let primary = parts.remove(0);
        Ok(Materialized {
            cid: cid.to_string(),
            data_id: Some(primary.meta.id.to_string()),
            name: Some(primary.meta.name),
            data_type: Some(primary.meta.data_type),
            domain_id: Some(primary.meta.domain_id.to_string()),
            path: primary.path,
            root_dir: primary.root,
            related_files: parts.into_iter().map(|p| p.path).collect(),
            extracted_paths: Vec::new(),
        })
    }

    /// Metadata matching the query in this task's Domain.
    pub async fn list(&self, query: &DataListQuery) -> Result<Vec<DataMetadata>, StorageError> {
        self.data
            .list_with_cancellation(query, &self.cancellation)
            .await
            .map_err(map_error)
    }

    /// Metadata by exact name, optionally filtered by data type.
    pub async fn find(
        &self,
        name: &str,
        data_type: Option<&str>,
    ) -> Result<Vec<DataMetadata>, StorageError> {
        let query = DataListQuery {
            name: Some(name.to_string()),
            data_type: data_type.map(str::to_string),
            ..Default::default()
        };
        match self.list(&query).await {
            Err(StorageError::NotFound) => Ok(Vec::new()),
            other => other,
        }
    }

    /// Stream one data item into `dest`, replacing any existing file.
    pub async fn download_to(&self, id: Uuid, size: u64, dest: &Path) -> Result<u64> {
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let file = fs::File::create(dest)
            .await
            .with_context(|| format!("create {}", dest.display()))?;
        let written = self
            .read_into(id, size, file)
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(written)
    }

    /// Upload or replace a named artifact and record it for the receipt.
    ///
    /// An existing item is replaced by id: explicit `existing_id`, else the id
    /// already written for this `logical_path` in the task, else an exact
    /// `(name, data_type)` match in the Domain.
    pub async fn put_domain_artifact(
        &self,
        request: ArtifactRequest<'_>,
    ) -> Result<Option<String>> {
        let logical_path = self.apply_outputs_prefix(request.rel_path);
        let mut existing_id = request.existing_id.map(str::to_string);
        if existing_id.is_none() {
            existing_id = self
                .uploads
                .lock()
                .get(&logical_path)
                .and_then(|record| record.id.clone());
        }
        if existing_id.is_none() {
            existing_id = self
                .find_artifact_id(request.name, request.data_type)
                .await
                .map_err(|e| anyhow!(e))?;
        }
        let target = target(existing_id.as_deref(), request.name, request.data_type)
            .map_err(|e| anyhow!(e))?;
        let saved = match request.content {
            ArtifactContent::Bytes(bytes) => self.upload_bytes(target, bytes).await,
            ArtifactContent::File(path) => self.upload_file(target, path).await,
        }
        .map_err(|e| anyhow!(e))?;
        let final_id = saved.or(existing_id);
        self.uploads.lock().insert(
            logical_path.clone(),
            UploadedArtifact {
                logical_path,
                name: request.name.to_string(),
                data_type: request.data_type.to_string(),
                id: final_id.clone(),
            },
        );
        Ok(final_id)
    }

    /// Upload bytes under a name derived from the path and task id,
    /// with the data type inferred from the extension.
    pub async fn put_bytes(&self, rel_path: &str, bytes: &[u8]) -> Result<()> {
        let logical_path = self.apply_outputs_prefix(rel_path);
        let name = format!(
            "{}_{}",
            sanitize_artifact(&logical_path.replace('/', "_")),
            self.task_id
        );
        let data_type = infer_data_type(rel_path);
        self.put_domain_artifact(ArtifactRequest {
            rel_path,
            name: &name,
            data_type: &data_type,
            existing_id: None,
            content: ArtifactContent::Bytes(bytes),
        })
        .await
        .map(|_| ())
    }

    /// Recorded uploads, ordered by logical path.
    pub fn uploaded_artifacts(&self) -> Vec<UploadedArtifact> {
        let mut artifacts: Vec<_> = self.uploads.lock().values().cloned().collect();
        artifacts.sort_by(|a, b| a.logical_path.cmp(&b.logical_path));
        artifacts
    }

    fn apply_outputs_prefix(&self, rel_path: &str) -> String {
        let trimmed_rel = rel_path.trim_start_matches('/');
        match self
            .outputs_prefix
            .as_deref()
            .map(|p| p.trim_matches('/'))
            .filter(|p| !p.is_empty())
        {
            Some(prefix) if trimmed_rel.is_empty() => prefix.to_string(),
            Some(prefix) => format!("{prefix}/{trimmed_rel}"),
            None => trimmed_rel.to_string(),
        }
    }

    fn parse_cid(&self, cid: &str) -> Result<DataListQuery, StorageError> {
        let cid = cid.trim();
        if cid.contains("://") || cid.starts_with('/') {
            let url = if cid.contains("://") {
                Url::parse(cid)
                    .map_err(|e| StorageError::Other(format!("parse domain url: {e}")))?
            } else {
                self.base
                    .as_ref()
                    .ok_or_else(|| StorageError::Other("lease missing domain_server_url".into()))?
                    .join(cid)
                    .map_err(|e| StorageError::Other(format!("join domain url: {e}")))?
            };
            let (domain, query) = parse_download_target(&url)?;
            if let Some(domain) = domain {
                if Uuid::parse_str(&domain).ok() != Some(self.data.domain_id()) {
                    return Err(StorageError::Unauthorized);
                }
            }
            return Ok(query);
        }
        Ok(DataListQuery {
            ids: vec![Uuid::parse_str(cid).map_err(|_| StorageError::BadRequest)?],
            ..Default::default()
        })
    }

    async fn download(&self, query: DataListQuery) -> Result<Vec<DownloadedPart>, StorageError> {
        let items = self.list(&query).await?;
        if items.is_empty() {
            return Err(StorageError::NotFound);
        }
        let root = std::env::temp_dir().join(format!("domain-input-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).await.map_err(io_error)?;
        let result = async {
            let mut parts = Vec::new();
            for item in items {
                let path = download_path(&root, &item.name, &item.data_type);
                fs::create_dir_all(path.parent().expect("path under download root"))
                    .await
                    .map_err(io_error)?;
                let file = fs::File::create(&path).await.map_err(io_error)?;
                self.read_into(item.id, item.size, file).await?;
                parts.push(DownloadedPart {
                    meta: item,
                    path,
                    root: root.clone(),
                });
            }
            Ok(parts)
        }
        .await;
        if result.is_err() {
            let _ = fs::remove_dir_all(&root).await;
        }
        result
    }

    async fn read_into(&self, id: Uuid, size: u64, file: fs::File) -> Result<u64, StorageError> {
        let file = Arc::new(tokio::sync::Mutex::new(file));
        self.data
            .read_to(
                id,
                transfer_options(size.max(1)),
                &self.cancellation,
                move |bytes| {
                    let file = file.clone();
                    async move {
                        let mut file = file.lock().await;
                        file.write_all(&bytes)
                            .await
                            .map_err(|_| DataError::Callback)?;
                        // Flush inside the awaited callback so the file is
                        // complete before its path is handed to the runner.
                        file.flush().await.map_err(|_| DataError::Callback)
                    }
                },
            )
            .await
            .map_err(map_error)
    }

    async fn find_artifact_id(
        &self,
        name: &str,
        data_type: &str,
    ) -> Result<Option<String>, StorageError> {
        Ok(self
            .find(name, Some(data_type))
            .await?
            .into_iter()
            .find(|v| v.name == name && v.data_type == data_type)
            .map(|v| v.id.to_string()))
    }

    async fn upload_bytes(
        &self,
        target: DataWrite<'_>,
        bytes: &[u8],
    ) -> Result<Option<String>, StorageError> {
        let saved = match self
            .data
            .write_with_cancellation(target, bytes, &self.cancellation)
            .await
        {
            Ok(saved) => saved,
            Err(DataError::TooLarge { .. }) if !bytes.is_empty() => {
                let mut offset = 0;
                self.data
                    .write_stream(
                        target,
                        bytes.len() as u64,
                        transfer_options(bytes.len() as u64),
                        &self.cancellation,
                        |maximum| {
                            let end = (offset + maximum).min(bytes.len());
                            let chunk = bytes[offset..end].to_vec();
                            offset = end;
                            std::future::ready(Ok(chunk))
                        },
                    )
                    .await
                    .map_err(map_error)?
            }
            Err(error) => return Err(map_error(error)),
        };
        Ok(Some(saved.id.to_string()))
    }

    async fn upload_file(
        &self,
        target: DataWrite<'_>,
        path: &Path,
    ) -> Result<Option<String>, StorageError> {
        let mut file = fs::File::open(path).await.map_err(io_error)?;
        let size = file.metadata().await.map_err(io_error)?.len();
        if size == 0 {
            return Err(StorageError::BadRequest);
        }
        if size <= BUFFER_LIMIT as u64 {
            let mut bytes = Vec::with_capacity(size as usize);
            (&mut file)
                .take(BUFFER_LIMIT as u64 + 1)
                .read_to_end(&mut bytes)
                .await
                .map_err(io_error)?;
            if bytes.len() > BUFFER_LIMIT {
                return Err(StorageError::Other(
                    "upload file grew beyond buffered limit".into(),
                ));
            }
            return self.upload_bytes(target, &bytes).await;
        }
        let file = Arc::new(tokio::sync::Mutex::new(file));
        let saved = self
            .data
            .write_stream(
                target,
                size,
                transfer_options(size),
                &self.cancellation,
                move |maximum| {
                    let file = file.clone();
                    async move {
                        let mut bytes = vec![0; maximum];
                        let mut read = 0;
                        let mut file = file.lock().await;
                        while read < maximum {
                            let n = file
                                .read(&mut bytes[read..])
                                .await
                                .map_err(|_| DataError::Callback)?;
                            if n == 0 {
                                break;
                            }
                            read += n;
                        }
                        bytes.truncate(read);
                        Ok(bytes)
                    }
                },
            )
            .await
            .map_err(map_error)?;
        Ok(Some(saved.id.to_string()))
    }
}

struct DownloadedPart {
    meta: DataMetadata,
    path: PathBuf,
    root: PathBuf,
}

/// `<root>/datasets/<timestamp-or-name>/<name>.<data_type>`, as in the 0.3.2 host.
pub fn download_path(root: &Path, name: &str, data_type: &str) -> PathBuf {
    let scan = extract_timestamp(name).unwrap_or_else(|| name.to_string());
    root.join("datasets")
        .join(sanitize_component(&scan))
        .join(format!(
            "{}.{}",
            sanitize_component(name),
            sanitize_component(data_type)
        ))
}

/// Parse `…/domains/{domain}/data[/{id}][?ids=a,b&name=&data_type=]`.
/// Returns the domain from the path, if any, and the list query.
fn parse_download_target(url: &Url) -> Result<(Option<String>, DataListQuery), StorageError> {
    let segments: Vec<&str> = url
        .path_segments()
        .map(|segments| segments.filter(|seg| !seg.is_empty()).collect())
        .unwrap_or_default();
    let mut domain = None;
    let mut data_id = None;
    for idx in 0..segments.len() {
        if segments[idx] == "domains" && idx + 2 < segments.len() && segments[idx + 2] == "data" {
            domain = Some(segments[idx + 1].to_string());
            data_id = segments.get(idx + 3).copied();
            break;
        }
    }
    let mut ids: Vec<String> = Vec::new();
    let mut name = None;
    let mut data_type = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "ids" => ids.extend(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string),
            ),
            "name" => {
                name.get_or_insert_with(|| value.to_string());
            }
            "data_type" => {
                data_type.get_or_insert_with(|| value.to_string());
            }
            _ => {}
        }
    }
    if let Some(id) = data_id {
        ids = vec![id.to_string()];
    }
    let ids = ids
        .iter()
        .map(|id| Uuid::parse_str(id).map_err(|_| StorageError::BadRequest))
        .collect::<Result<_, _>>()?;
    Ok((
        domain,
        DataListQuery {
            ids,
            name,
            data_type,
        },
    ))
}

fn target<'a>(
    id: Option<&str>,
    name: &'a str,
    data_type: &'a str,
) -> Result<DataWrite<'a>, StorageError> {
    Ok(match id {
        Some(id) => DataWrite::ById(Uuid::parse_str(id).map_err(|_| StorageError::BadRequest)?),
        None => DataWrite::Named { name, data_type },
    })
}

fn transfer_options(size: u64) -> TransferOptions {
    TransferOptions {
        max_bytes: size,
        ..TransferOptions::default()
    }
}

fn extract_timestamp(name: &str) -> Option<String> {
    Regex::new(r"\d{4}-\d{2}-\d{2}[_-]\d{2}-\d{2}-\d{2}")
        .ok()
        .and_then(|re| re.find(name).map(|m| m.as_str().to_string()))
}

fn sanitize_with(value: &str, empty: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        empty.into()
    } else {
        sanitized
    }
}

fn sanitize_component(value: &str) -> String {
    sanitize_with(value, "part")
}

fn sanitize_artifact(value: &str) -> String {
    sanitize_with(value, "artifact")
}

/// Data type for `put_bytes`, from the path extension.
pub fn infer_data_type(rel_path: &str) -> String {
    let ext = Path::new(rel_path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.trim().to_ascii_lowercase());
    match ext.as_deref() {
        Some("json") => "json".into(),
        Some("ply") => "ply".into(),
        Some("drc") => "ply_draco".into(),
        Some("glb") => "glb".into(),
        Some("obj") => "obj".into(),
        Some("csv") => "csv".into(),
        Some("mp4") => "mp4".into(),
        Some(other) => format!("{}_data", sanitize_artifact(other)),
        None => "binary".into(),
    }
}

fn io_error(error: std::io::Error) -> StorageError {
    StorageError::Other(error.to_string())
}

fn map_error(error: DataError) -> StorageError {
    if matches!(error, DataError::Auth(AuthError::AuthenticationRequired)) {
        return StorageError::Unauthorized;
    }
    if matches!(error, DataError::Transport) {
        return StorageError::Network(error.to_string());
    }
    match error.status() {
        Some(400) => StorageError::BadRequest,
        Some(401) => StorageError::Unauthorized,
        Some(404) => StorageError::NotFound,
        Some(409) => StorageError::Conflict,
        Some(status @ 500..=599) => StorageError::Server(status),
        _ => StorageError::Other(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_layout_matches_previous_host() {
        let root = Path::new("/r");
        assert_eq!(
            download_path(root, "dmt_manifest_2024-01-02_03-04-05", "dmt_manifest_json"),
            PathBuf::from(
                "/r/datasets/2024-01-02_03-04-05/dmt_manifest_2024-01-02_03-04-05.dmt_manifest_json"
            )
        );
        assert_eq!(
            download_path(root, "refined_manifest_x.y", "refined_manifest_json"),
            PathBuf::from(
                "/r/datasets/refined_manifest_x_y/refined_manifest_x_y.refined_manifest_json"
            )
        );
        assert_eq!(
            download_path(root, "", ""),
            PathBuf::from("/r/datasets/part/part.part")
        );
    }

    #[test]
    fn download_target_forms() {
        let d = "11111111-1111-1111-1111-111111111111";
        let a = "22222222-2222-2222-2222-222222222222";
        let b = "33333333-3333-3333-3333-333333333333";
        let url = Url::parse(&format!("https://ds/api/v1/domains/{d}/data/{a}")).unwrap();
        let (domain, q) = parse_download_target(&url).unwrap();
        assert_eq!(domain.as_deref(), Some(d));
        assert_eq!(q.ids, vec![Uuid::parse_str(a).unwrap()]);

        let url = Url::parse(&format!(
            "https://ds/api/v1/domains/{d}/data?ids={a},{b}&name=n&data_type=t&name=ignored"
        ))
        .unwrap();
        let (_, q) = parse_download_target(&url).unwrap();
        assert_eq!(q.ids.len(), 2);
        assert_eq!(q.name.as_deref(), Some("n"));
        assert_eq!(q.data_type.as_deref(), Some("t"));

        let url = Url::parse(&format!("https://ds/api/v1/domains/{d}/data/not-a-uuid")).unwrap();
        assert!(matches!(
            parse_download_target(&url),
            Err(StorageError::BadRequest)
        ));
    }

    #[test]
    fn put_bytes_data_types() {
        for (path, expected) in [
            ("a/outputs_index.json", "json"),
            ("x.PLY", "ply"),
            ("x.drc", "ply_draco"),
            ("x.zip", "zip_data"),
            ("x", "binary"),
        ] {
            assert_eq!(infer_data_type(path), expected);
        }
    }

    #[test]
    fn error_text_is_stable() {
        assert_eq!(StorageError::Conflict.to_string(), "conflict (409)");
        assert_eq!(StorageError::NotFound.to_string(), "not found (404)");
    }
}
