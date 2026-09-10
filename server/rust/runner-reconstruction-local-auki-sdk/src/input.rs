//! Input materialization for the Auki SDK local refinement capability.
//!
//! Unlike `/reconstruction/local-refinement/v1` -- which receives one Domain artifact
//! per DMT scan file (ARposes.csv, Frames.mp4, ...) -- this capability receives one
//! small Domain artifact describing a Robot-hosted Auki capture ZIP. The ZIP is
//! fetched over authenticated Blob v1 and expanded here, on the Rust side, so the
//! Python entrypoint still sees the same plain on-disk capture folder.
//!
//! The Robot used to publish that ZIP through the P2P dataset protocol and we
//! fetched it through `TaskCtx::p2p_dataset`. Posemesh retired that protocol;
//! the Robot now stores the ZIP as one content-addressed blob and publishes a
//! manifest saying where to fetch it. What arrives here is the same
//! `scan_path_recording_reference_json` artifact with a different body.
//!
//! One consequence worth knowing: **a blob has no expiry.** The dataset
//! reference carried an `available_until` the publisher enforced; the manifest
//! carries one that is advisory. We treat a passed deadline as a warning and
//! still attempt the fetch, because the bytes are very likely still there and
//! refusing outright would be a worse failure than trying.

use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use auki_protocols::blob::BlobClient;
use chrono::{DateTime, Utc};
use compute_runner_api::{MaterializedInput, TaskCtx};
use libp2p_identity::PeerId;
use multiaddr::{Multiaddr, Protocol};
use posemesh_compute_node::engine::AukiProtocolsHandle;
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
/// Manifest schema the Robot-side publisher writes.
const BLOB_MANIFEST_SCHEMA: &str = "auki.blob.manifest/1";
const MAX_REFERENCE_BYTES: u64 = 64 * 1024;
const MAX_REFERENCE_ROUTES: usize = 16;
const MAX_CIRCUIT_ROUTES: usize = 3;
const MAX_ROUTE_TEXT_BYTES: usize = 1024;

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
    protocols: &AukiProtocolsHandle,
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
    let materialized = ctx
        .input
        .materialize_cid_with_meta(cid)
        .await
        .with_context(|| format!("materialize input CID {}", cid))?;

    // Domain Server supplies only the small JSON reference. Keep the existing cleanup
    // guard so both valid and rejected references are removed on every exit path.
    let _input_cleanup = TempInputCleanup::for_materialized(&materialized, workspace);

    let manifest = read_and_validate_manifest(&materialized, lease_domain, Utc::now())
        .await
        .with_context(|| format!("validate the recording manifest for CID {cid}"))?;
    let scan_name = derive_scan_name(&materialized, &materialized.path);

    let input_dir = workspace.root().join("inputs");
    async_fs::create_dir_all(&input_dir)
        .await
        .with_context(|| format!("create P2P input directory {}", input_dir.display()))?;
    let zip_path = input_dir.join(format!("{}.zip", manifest.blob.sha256));
    info!(
        cid = %cid,
        sha256 = %manifest.blob.sha256,
        peer_id = %manifest.peer_id,
        routes = manifest.routes.len(),
        scan = %scan_name,
        destination = %zip_path.display(),
        "fetching the Auki session ZIP over authenticated Blob v1"
    );
    // Resolved at the point of use, never cached: on Compute the protocol
    // context belongs to this task's peer.
    let protocol_context = protocols
        .get()
        .context("the authenticated P2P protocol surface is unavailable for Auki session input")?;
    let bytes = fetch_blob(&BlobClient::new(protocol_context.protocols()), &manifest).await?;
    async_fs::write(&zip_path, &bytes)
        .await
        .with_context(|| format!("write the fetched session ZIP to {}", zip_path.display()))?;

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

    // Blob v1 verifies the SHA-256 of every byte it returns before handing it
    // over, so the archive on disk is already content-verified. Keep the
    // established safe extraction path and Python-facing directory layout.
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

/// The Robot's blob manifest, as written by the robot-side publisher.
///
/// `deny_unknown_fields` on purpose: a publisher that grows a field we do not
/// understand should stop us loudly here rather than have us guess. It is also
/// what makes the dataset-reference/manifest transition fail cleanly rather
/// than silently while the two sides are out of step.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlobManifestDocument {
    schema: String,
    peer_id: String,
    routes: Vec<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    available_until: Option<DateTime<Utc>>,
    blobs: Vec<BlobManifestEntry>,
    /// Deprecated alias of `blobs`, emitted by the publisher for one release.
    /// Accepted and ignored; `blobs` is authoritative.
    #[serde(default)]
    #[allow(dead_code)]
    images: Option<Vec<BlobManifestEntry>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlobManifestEntry {
    #[allow(dead_code)]
    id: String,
    sha256: String,
    #[serde(default)]
    size_bytes: Option<u64>,
}

/// A validated manifest for exactly one recording ZIP.
#[derive(Debug)]
struct RecordingManifest {
    peer_id: PeerId,
    routes: Vec<String>,
    blob: BlobManifestEntry,
}

async fn read_and_validate_manifest(
    materialized: &MaterializedInput,
    lease_domain: Uuid,
    now: DateTime<Utc>,
) -> Result<RecordingManifest> {
    if materialized.data_type.as_deref() != Some(RECORDING_REFERENCE_DATA_TYPE) {
        anyhow::bail!(
            "Auki session input must have Domain data type {RECORDING_REFERENCE_DATA_TYPE}, got {}",
            materialized.data_type.as_deref().unwrap_or("<missing>")
        );
    }
    // The Domain binding is checked here, against the artifact's own metadata.
    // The manifest body no longer repeats it -- this was always the stronger of
    // the two checks, since it is Domain Server's word rather than the
    // publisher's.
    let source_domain = materialized
        .domain_id
        .as_deref()
        .context("recording manifest Domain metadata is missing")?;
    let source_domain = Uuid::parse_str(source_domain)
        .context("recording manifest Domain metadata is not a UUID")?;
    if source_domain != lease_domain {
        anyhow::bail!("recording manifest Domain metadata does not match the task lease");
    }

    let metadata = async_fs::metadata(&materialized.path)
        .await
        .with_context(|| {
            format!(
                "inspect the materialized recording manifest {}",
                materialized.path.display()
            )
        })?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_REFERENCE_BYTES {
        anyhow::bail!(
            "a materialized recording manifest must be a non-empty file no larger than {} bytes",
            MAX_REFERENCE_BYTES
        );
    }
    let bytes = async_fs::read(&materialized.path).await.with_context(|| {
        format!(
            "read the materialized recording manifest {}",
            materialized.path.display()
        )
    })?;
    let document: BlobManifestDocument = serde_json::from_slice(&bytes).context(
        "parse the strict Blob v1 recording manifest JSON (a publisher still emitting an \
         auki-p2p-dataset reference will fail here, which is intended while the two sides \
         are out of step)",
    )?;
    validate_manifest_document(document, now)
}

fn validate_manifest_document(
    document: BlobManifestDocument,
    now: DateTime<Utc>,
) -> Result<RecordingManifest> {
    if document.schema != BLOB_MANIFEST_SCHEMA {
        anyhow::bail!(
            "unsupported recording manifest schema: {} (expected {BLOB_MANIFEST_SCHEMA})",
            document.schema
        );
    }
    // One recording is one blob. More than one means the publisher used a verb
    // that splits its output, which this capability cannot reassemble.
    let blob = match document.blobs.len() {
        1 => document.blobs.into_iter().next().expect("checked above"),
        0 => anyhow::bail!("recording manifest lists no blobs"),
        n => anyhow::bail!(
            "recording manifest lists {n} blobs; this capability expects exactly one ZIP"
        ),
    };

    if let Some(content_type) = document.content_type.as_deref() {
        if content_type != "application/zip" {
            anyhow::bail!(
                "recording manifest declares content type {content_type}; expected application/zip"
            );
        }
    }

    let digest =
        hex::decode(&blob.sha256).context("recording manifest sha256 must be hexadecimal")?;
    if digest.len() != 32 {
        anyhow::bail!("recording manifest sha256 must contain exactly 32 bytes");
    }
    if blob.sha256 != blob.sha256.to_ascii_lowercase() {
        anyhow::bail!("recording manifest sha256 must be lowercase");
    }
    if matches!(blob.size_bytes, Some(0)) {
        anyhow::bail!("recording manifest size_bytes must be greater than zero");
    }

    let peer_id =
        PeerId::from_str(&document.peer_id).context("recording manifest Peer ID is invalid")?;
    let routes = validate_multiaddrs(&document.routes, peer_id)?;

    // ADVISORY, not enforced. A blob has no expiry and nothing prunes the
    // publisher's blob root, so a passed deadline says "this manifest is old",
    // not "the bytes are gone". Refusing would turn a probably-fine fetch into
    // a certain failure.
    if let Some(available_until) = document.available_until {
        if available_until <= now {
            warn!(
                %available_until,
                sha256 = %blob.sha256,
                "recording manifest is past its advisory availability deadline; \
                 attempting the fetch anyway"
            );
        }
    }

    Ok(RecordingManifest {
        peer_id,
        routes,
        blob,
    })
}

/// Try each validated route in turn, returning the first verified fetch.
///
/// Routes are ordered by the publisher and already canonicalised by
/// `validate_multiaddrs`; a robot behind a relay commonly advertises several,
/// only some of which this node can reach.
async fn fetch_blob(client: &BlobClient, manifest: &RecordingManifest) -> Result<Vec<u8>> {
    let mut last_error = None;
    for route in &manifest.routes {
        let parsed: Multiaddr = match route.parse() {
            Ok(parsed) => parsed,
            Err(error) => {
                last_error = Some(anyhow!("route {route} is not a multiaddr: {error}"));
                continue;
            }
        };
        match client
            .fetch_exact(manifest.peer_id, parsed, manifest.blob.sha256.clone())
            .await
        {
            Ok(receipt) => {
                info!(
                    route = %route,
                    relayed = receipt.relayed,
                    bytes = receipt.bytes.len(),
                    "fetched the Auki session ZIP"
                );
                if let Some(expected) = manifest.blob.size_bytes {
                    if receipt.bytes.len() as u64 != expected {
                        anyhow::bail!(
                            "fetched blob is {} bytes but the manifest declared {expected}",
                            receipt.bytes.len()
                        );
                    }
                }
                return Ok(receipt.into_bytes());
            }
            Err(error) => {
                warn!(route = %route, %error, "route failed; trying the next one");
                last_error = Some(anyhow!("route {route}: {error}"));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        anyhow!(
            "recording manifest for {} carried no usable route",
            manifest.blob.sha256
        )
    }))
    .with_context(|| {
        format!(
            "fetch blob {} from Robot {} over Blob v1",
            manifest.blob.sha256, manifest.peer_id
        )
    })
}

fn validate_multiaddrs(addresses: &[String], expected_peer_id: PeerId) -> Result<Vec<String>> {
    if addresses.is_empty() {
        anyhow::bail!("recording reference has no explicit advertised multiaddr");
    }
    if addresses.len() > MAX_REFERENCE_ROUTES {
        anyhow::bail!(
            "recording reference has {} route candidates; maximum is {MAX_REFERENCE_ROUTES}",
            addresses.len()
        );
    }

    let mut parsed = Vec::with_capacity(addresses.len());
    for (route_index, address) in addresses.iter().enumerate() {
        if address.len() > MAX_ROUTE_TEXT_BYTES {
            warn!(
                route_index,
                route_bytes = address.len(),
                maximum = MAX_ROUTE_TEXT_BYTES,
                "skipping oversized recording reference route candidate"
            );
            continue;
        }
        match Multiaddr::from_str(address) {
            Ok(route) => parsed.push((route_index, address.as_str(), route)),
            Err(error) => warn!(
                route_index,
                error = %error,
                "skipping malformed recording reference route candidate"
            ),
        }
    }

    let circuit_count = parsed
        .iter()
        .filter(|(_, _, route)| {
            route
                .iter()
                .any(|protocol| matches!(protocol, Protocol::P2pCircuit))
        })
        .count();
    if circuit_count > MAX_CIRCUIT_ROUTES {
        anyhow::bail!(
            "recording reference has {circuit_count} circuit route candidates; maximum is {MAX_CIRCUIT_ROUTES}"
        );
    }

    let mut direct = Vec::new();
    let mut circuit = Vec::new();
    let mut direct_routes = HashSet::new();
    let mut relay_peer_ids = HashSet::new();
    let mut relay_endpoints = HashSet::new();
    for (route_index, address, route) in parsed {
        let is_circuit = route
            .iter()
            .any(|protocol| matches!(protocol, Protocol::P2pCircuit));
        if is_circuit {
            let canonical = match canonicalize_circuit_route(&route, expected_peer_id) {
                Ok(canonical) => canonical,
                Err(error) => {
                    warn!(
                        route_index,
                        route = %address,
                        error = %error,
                        "skipping unsafe recording reference circuit candidate"
                    );
                    continue;
                }
            };
            if relay_peer_ids.contains(&canonical.relay_peer_id)
                || relay_endpoints.contains(&canonical.endpoint)
            {
                warn!(
                    route_index,
                    route = %address,
                    "skipping duplicate recording reference circuit candidate"
                );
                continue;
            }
            relay_peer_ids.insert(canonical.relay_peer_id);
            relay_endpoints.insert(canonical.endpoint);
            circuit.push(canonical.route.to_string());
        } else {
            let canonical = match canonicalize_direct_route(&route, expected_peer_id) {
                Ok(canonical) => canonical,
                Err(error) => {
                    warn!(
                        route_index,
                        route = %address,
                        error = %error,
                        "skipping unsafe recording reference direct candidate"
                    );
                    continue;
                }
            };
            let canonical = canonical.to_string();
            if !direct_routes.insert(canonical.clone()) {
                warn!(
                    route_index,
                    route = %address,
                    "skipping duplicate recording reference direct candidate"
                );
                continue;
            }
            direct.push(canonical);
        }
    }

    direct.extend(circuit);
    if direct.is_empty() {
        anyhow::bail!("recording reference contains no safe direct or circuit route candidate");
    }
    Ok(direct)
}

fn canonicalize_direct_route(route: &Multiaddr, expected_peer_id: PeerId) -> Result<Multiaddr> {
    let protocols = route.iter().collect::<Vec<_>>();
    let (network, port, suffix) = match protocols.as_slice() {
        [network, Protocol::Tcp(port)] => (network, *port, None),
        [network, Protocol::Tcp(port), Protocol::P2p(peer_id)] => (network, *port, Some(*peer_id)),
        _ => anyhow::bail!("expected exact address/tcp[/p2p] grammar"),
    };
    if !matches!(
        network,
        Protocol::Ip4(_)
            | Protocol::Ip6(_)
            | Protocol::Dns(_)
            | Protocol::Dns4(_)
            | Protocol::Dns6(_)
    ) {
        anyhow::bail!("direct route address must be ip4, ip6, dns, dns4, or dns6");
    }
    if port == 0 {
        anyhow::bail!("direct route TCP port must be non-zero");
    }
    if suffix.is_some_and(|peer_id| peer_id != expected_peer_id) {
        anyhow::bail!("direct route terminal Peer ID does not match the reference");
    }

    let mut canonical = route.clone();
    if suffix.is_some() {
        canonical.pop();
    }
    Ok(canonical)
}

struct CanonicalCircuitRoute {
    route: Multiaddr,
    relay_peer_id: PeerId,
    endpoint: String,
}

fn canonicalize_circuit_route(
    route: &Multiaddr,
    expected_target_peer_id: PeerId,
) -> Result<CanonicalCircuitRoute> {
    let mut protocols = route.iter();
    let (host, port, relay_peer_id, target_peer_id) = match (
        protocols.next(),
        protocols.next(),
        protocols.next(),
        protocols.next(),
        protocols.next(),
        protocols.next(),
    ) {
        (
            Some(Protocol::Dns4(host)),
            Some(Protocol::Tcp(port)),
            Some(Protocol::P2p(relay_peer_id)),
            Some(Protocol::P2pCircuit),
            Some(Protocol::P2p(target_peer_id)),
            None,
        ) => (host, port, relay_peer_id, target_peer_id),
        _ => anyhow::bail!("expected exact dns4/tcp/p2p/p2p-circuit/p2p grammar"),
    };
    if port == 0 {
        anyhow::bail!("circuit route TCP port must be non-zero");
    }
    if target_peer_id != expected_target_peer_id {
        anyhow::bail!("circuit route target Peer ID does not match the reference");
    }

    let host = canonicalize_public_fqdn(&host)
        .context("circuit route host is not an allowed public FQDN")?;
    let endpoint = format!("/dns4/{host}/tcp/{port}");
    let canonical =
        format!("{endpoint}/p2p/{relay_peer_id}/p2p-circuit/p2p/{expected_target_peer_id}")
            .parse()
            .context("construct canonical circuit multiaddr")?;
    Ok(CanonicalCircuitRoute {
        route: canonical,
        relay_peer_id,
        endpoint,
    })
}

fn canonicalize_public_fqdn(raw: &str) -> Option<String> {
    let host = raw.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() || host.len() > 253 || !host.contains('.') {
        return None;
    }
    if host.parse::<std::net::IpAddr>().is_ok() {
        return None;
    }
    const FORBIDDEN_SUFFIXES: &[&str] = &[
        ".localhost",
        ".local",
        ".internal",
        ".invalid",
        ".test",
        ".example",
        ".home.arpa",
    ];
    if host == "localhost"
        || FORBIDDEN_SUFFIXES
            .iter()
            .any(|suffix| host.ends_with(suffix))
    {
        return None;
    }
    if host.split('.').any(|label| {
        label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    }) {
        return None;
    }
    Some(host)
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
#[path = "input_blob_tests.rs"]
mod blob_tests;
