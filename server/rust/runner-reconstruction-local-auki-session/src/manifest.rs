//! The P2P input: a recording manifest in the Domain that names the capture zip's blobs on the
//! robot's peer, fetched over Blob v1.
//!
//! The robot stores the capture as content-addressed blobs and publishes a small
//! `scan_path_recording_reference_json` artifact saying where to fetch them:
//!
//! ```json
//! {"schema": "auki.blob.manifest/1", "peer_id": "12D3KooW…",
//!  "routes": ["/ip4/10.0.0.4/tcp/4001/p2p/12D3KooW…"], "content_type": "application/zip",
//!  "available_until": "2026-09-09T12:00:00Z",
//!  "blobs": [{"id": "scan_path_recording_2026-09-09_10-00-00.zip", "sha256": "…", "size_bytes": 41231}]}
//! ```
//!
//! Blob v1 caps one blob at 64 MiB and holds it in memory, and a capture is hundreds of MB to
//! several GB, so a manifest may list the zip as several blobs: parts, concatenated in order.
//! Each is verified by Blob v1 (SHA-256, and the remote peer's identity) and appended to disk
//! as it arrives. A single-blob manifest is the same thing with one part.

use std::fs;
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};
use auki_p2p::{canonicalize_circuit_route, validate_direct_route, Multiaddr, PeerId, Protocol};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tracing::{info, warn};

/// Domain data type of the recording manifest.
pub const RECORDING_REFERENCE_DATA_TYPE: &str = "scan_path_recording_reference_json";
const BLOB_MANIFEST_SCHEMA: &str = "auki.blob.manifest/1";
/// Upper bound on the manifest file itself.
pub const MAX_MANIFEST_BYTES: u64 = 64 * 1024;
const MAX_ROUTES: usize = 16;
const MAX_CIRCUIT_ROUTES: usize = 3;
const MAX_ROUTE_TEXT_BYTES: usize = 1024;
/// 512 parts of at most 64 MiB: 32 GiB.
const MAX_BLOBS: usize = 512;

/// The manifest as the robot-side publisher writes it. `deny_unknown_fields` on purpose: a field
/// this runner does not understand should stop it loudly rather than be guessed at.
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
    /// Deprecated alias of `blobs` the publisher emitted for one release; `blobs` wins.
    #[serde(default, rename = "images")]
    _images: Option<Vec<BlobManifestEntry>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobManifestEntry {
    pub id: String,
    pub sha256: String,
    #[serde(default)]
    pub size_bytes: Option<u64>,
}

/// A validated manifest: the robot's peer, the routes to try (direct first), and the parts.
#[derive(Debug)]
pub struct RecordingManifest {
    pub peer_id: PeerId,
    pub routes: Vec<Multiaddr>,
    pub blobs: Vec<BlobManifestEntry>,
}

/// Parse and validate a manifest file's contents.
pub fn parse_manifest(bytes: &[u8], now: DateTime<Utc>) -> Result<RecordingManifest> {
    if bytes.is_empty() || bytes.len() as u64 > MAX_MANIFEST_BYTES {
        anyhow::bail!(
            "a recording manifest must be a non-empty file no larger than {MAX_MANIFEST_BYTES} bytes"
        );
    }
    let document: BlobManifestDocument = serde_json::from_slice(bytes).context(
        "parse the Blob v1 recording manifest (an older auki-p2p-dataset reference fails here)",
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
    if document.blobs.is_empty() {
        anyhow::bail!("recording manifest lists no blobs");
    }
    if document.blobs.len() > MAX_BLOBS {
        anyhow::bail!(
            "recording manifest lists {} blobs; maximum is {MAX_BLOBS}",
            document.blobs.len()
        );
    }
    if let Some(content_type) = document.content_type.as_deref() {
        if content_type != "application/zip" {
            anyhow::bail!(
                "recording manifest declares content type {content_type}; expected application/zip"
            );
        }
    }
    for blob in &document.blobs {
        let is_hex_digest =
            blob.sha256.len() == 64 && blob.sha256.bytes().all(|b| b.is_ascii_hexdigit());
        if !is_hex_digest {
            anyhow::bail!(
                "recording manifest sha256 of {} must be 64 hexadecimal characters",
                blob.id
            );
        }
        if blob.sha256 != blob.sha256.to_ascii_lowercase() {
            anyhow::bail!("recording manifest sha256 of {} must be lowercase", blob.id);
        }
        if blob.size_bytes == Some(0) {
            anyhow::bail!(
                "recording manifest size_bytes of {} must be greater than zero",
                blob.id
            );
        }
    }

    let peer_id =
        PeerId::from_str(&document.peer_id).context("recording manifest Peer ID is invalid")?;
    let routes = validate_routes(&document.routes, peer_id)?;

    // Advisory: a blob has no expiry and nothing prunes the publisher's blob store, so a passed
    // deadline means the manifest is old, not that the bytes are gone. Refusing would turn a
    // probably-fine fetch into a certain failure.
    if let Some(available_until) = document.available_until {
        if available_until <= now {
            warn!(%available_until, "recording manifest is past its advisory availability deadline; fetching anyway");
        }
    }

    Ok(RecordingManifest {
        peer_id,
        routes,
        blobs: document.blobs,
    })
}

/// The publisher's routes, canonicalised by the SDK and deduplicated: direct ones first, then
/// at most three relay circuits on distinct relays. Unusable candidates are skipped with a
/// warning; none usable is an error.
fn validate_routes(addresses: &[String], expected_peer_id: PeerId) -> Result<Vec<Multiaddr>> {
    if addresses.is_empty() {
        anyhow::bail!("recording manifest has no route");
    }
    if addresses.len() > MAX_ROUTES {
        anyhow::bail!(
            "recording manifest has {} route candidates; maximum is {MAX_ROUTES}",
            addresses.len()
        );
    }

    let mut direct: Vec<Multiaddr> = Vec::new();
    let mut circuit: Vec<Multiaddr> = Vec::new();
    let mut relay_peer_ids = Vec::new();
    let mut relay_endpoints = Vec::new();
    let mut circuit_candidates = 0;
    for (route_index, address) in addresses.iter().enumerate() {
        if address.len() > MAX_ROUTE_TEXT_BYTES {
            warn!(
                route_index,
                route_bytes = address.len(),
                "skipping an oversized route candidate"
            );
            continue;
        }
        let route = match Multiaddr::from_str(address) {
            Ok(route) => route,
            Err(error) => {
                warn!(route_index, %error, "skipping a malformed route candidate");
                continue;
            }
        };
        if route.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
            circuit_candidates += 1;
            if circuit_candidates > MAX_CIRCUIT_ROUTES {
                anyhow::bail!(
                    "recording manifest has more than {MAX_CIRCUIT_ROUTES} circuit route candidates"
                );
            }
            match canonicalize_circuit_route(&route, expected_peer_id) {
                Ok(canonical)
                    if !relay_peer_ids.contains(&canonical.relay_peer_id)
                        && !relay_endpoints.contains(&canonical.endpoint_key) =>
                {
                    relay_peer_ids.push(canonical.relay_peer_id);
                    relay_endpoints.push(canonical.endpoint_key);
                    circuit.push(canonical.route);
                }
                Ok(_) => warn!(route_index, route = %address, "skipping a duplicate circuit route"),
                Err(error) => {
                    warn!(route_index, route = %address, %error, "skipping an unsafe circuit route")
                }
            }
        } else {
            match validate_direct_route(&route, expected_peer_id) {
                Ok(canonical) if !direct.contains(&canonical) => direct.push(canonical),
                Ok(_) => warn!(route_index, route = %address, "skipping a duplicate direct route"),
                Err(error) => {
                    warn!(route_index, route = %address, %error, "skipping an unsafe direct route")
                }
            }
        }
    }

    direct.extend(circuit);
    if direct.is_empty() {
        anyhow::bail!("recording manifest has no safe direct or circuit route");
    }
    Ok(direct)
}

/// Fetch every blob in order into `dest`, trying each route in turn for each blob. `fetch`
/// returns one blob's verified bytes (`BlobClient::fetch_exact` in production). Returns the
/// number of bytes written. On error `dest` may hold a partial file; the caller's workspace
/// cleanup removes it.
pub async fn fetch_blobs<F, Fut>(manifest: &RecordingManifest, dest: &Path, fetch: F) -> Result<u64>
where
    F: Fn(PeerId, Multiaddr, String) -> Fut,
    Fut: Future<Output = Result<Vec<u8>>>,
{
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let mut file = fs::File::create(dest).with_context(|| format!("create {}", dest.display()))?;
    let mut written = 0u64;
    for (index, blob) in manifest.blobs.iter().enumerate() {
        let bytes = fetch_one(manifest, blob, &fetch).await.with_context(|| {
            format!(
                "fetch blob {} of {} ({}) from robot {}",
                index + 1,
                manifest.blobs.len(),
                blob.sha256,
                manifest.peer_id
            )
        })?;
        if let Some(expected) = blob.size_bytes {
            if bytes.len() as u64 != expected {
                anyhow::bail!(
                    "blob {} is {} bytes but the manifest declares {expected}",
                    blob.id,
                    bytes.len()
                );
            }
        }
        file.write_all(&bytes)
            .with_context(|| format!("write {}", dest.display()))?;
        written += bytes.len() as u64;
        info!(
            part = index + 1,
            parts = manifest.blobs.len(),
            bytes = bytes.len(),
            total = written,
            "fetched blob"
        );
    }
    file.flush()
        .with_context(|| format!("flush {}", dest.display()))?;
    Ok(written)
}

async fn fetch_one<F, Fut>(
    manifest: &RecordingManifest,
    blob: &BlobManifestEntry,
    fetch: &F,
) -> Result<Vec<u8>>
where
    F: Fn(PeerId, Multiaddr, String) -> Fut,
    Fut: Future<Output = Result<Vec<u8>>>,
{
    let mut last_error = None;
    for route in &manifest.routes {
        match fetch(manifest.peer_id, route.clone(), blob.sha256.clone()).await {
            Ok(bytes) => return Ok(bytes),
            Err(error) => {
                warn!(route = %route, error = %format!("{error:#}"), "route failed; trying the next one");
                last_error = Some(error.context(format!("route {route}")));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("no route to try")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::cell::RefCell;

    const SHA_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SHA_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn peer_id() -> PeerId {
        auki_p2p::Identity::generate().peer_id()
    }

    fn document(peer_id: PeerId, routes: Vec<String>, overrides: Value) -> Vec<u8> {
        let mut doc = json!({
            "schema": BLOB_MANIFEST_SCHEMA,
            "peer_id": peer_id.to_string(),
            "routes": routes,
            "content_type": "application/zip",
            "blobs": [{"id": "scan.zip", "sha256": SHA_A, "size_bytes": 3}],
        });
        for (key, value) in overrides.as_object().unwrap() {
            doc[key] = value.clone();
        }
        serde_json::to_vec(&doc).unwrap()
    }

    fn direct(peer_id: PeerId, port: u16) -> String {
        format!("/ip4/10.0.0.4/tcp/{port}/p2p/{peer_id}")
    }

    fn parse(peer_id: PeerId, routes: Vec<String>, overrides: Value) -> Result<RecordingManifest> {
        parse_manifest(&document(peer_id, routes, overrides), Utc::now())
    }

    #[test]
    fn a_well_formed_manifest_is_accepted() {
        let id = peer_id();
        let manifest = parse(id, vec![direct(id, 4001)], json!({})).unwrap();
        assert_eq!(manifest.peer_id, id);
        // The terminal /p2p/<id> is dropped by canonicalisation.
        assert_eq!(
            manifest.routes,
            vec!["/ip4/10.0.0.4/tcp/4001".parse::<Multiaddr>().unwrap()]
        );
        assert_eq!(manifest.blobs.len(), 1);
    }

    #[test]
    fn a_dataset_reference_from_an_old_publisher_is_rejected() {
        let id = peer_id();
        let old = json!({
            "schema": "auki-p2p-dataset/1", "dataset_id": "x", "domain_id": "y", "name": "n",
            "peer_id": id.to_string(), "multiaddrs": [direct(id, 4001)], "size_bytes": 1,
            "sha256": SHA_A, "available_until": "2026-09-09T12:00:00Z",
        });
        assert!(parse_manifest(&serde_json::to_vec(&old).unwrap(), Utc::now()).is_err());
    }

    #[test]
    fn unknown_fields_and_schemas_are_rejected() {
        let id = peer_id();
        assert!(parse(id, vec![direct(id, 4001)], json!({"surprise": 1})).is_err());
        let err = parse(
            id,
            vec![direct(id, 4001)],
            json!({"schema": "auki.blob.manifest/2"}),
        )
        .unwrap_err();
        assert!(err.to_string().contains("unsupported"), "{err}");
    }

    #[test]
    fn the_deprecated_images_alias_is_ignored() {
        let id = peer_id();
        let manifest = parse(
            id,
            vec![direct(id, 4001)],
            json!({"images": [{"id": "other.zip", "sha256": SHA_B}]}),
        )
        .unwrap();
        assert_eq!(manifest.blobs[0].sha256, SHA_A);
    }

    #[test]
    fn blobs_are_one_to_many_parts() {
        let id = peer_id();
        assert!(parse(id, vec![direct(id, 4001)], json!({"blobs": []})).is_err());
        let manifest = parse(
            id,
            vec![direct(id, 4001)],
            json!({"blobs": [{"id": "part-1", "sha256": SHA_A}, {"id": "part-2", "sha256": SHA_B}]}),
        )
        .unwrap();
        assert_eq!(manifest.blobs.len(), 2);
    }

    #[test]
    fn content_digest_and_size_are_checked() {
        let id = peer_id();
        let routes = vec![direct(id, 4001)];
        assert!(parse(
            id,
            routes.clone(),
            json!({"content_type": "application/json"})
        )
        .is_err());
        assert!(parse(id, routes.clone(), json!({"content_type": null})).is_ok());
        for sha in ["abc", &SHA_A.to_ascii_uppercase(), &"z".repeat(64)] {
            let blobs = json!({"blobs": [{"id": "scan.zip", "sha256": sha}]});
            assert!(parse(id, routes.clone(), blobs).is_err(), "{sha}");
        }
        let zero = json!({"blobs": [{"id": "scan.zip", "sha256": SHA_A, "size_bytes": 0}]});
        assert!(parse(id, routes, zero).is_err());
    }

    #[test]
    fn an_invalid_peer_id_is_rejected() {
        let id = peer_id();
        assert!(parse(id, vec![direct(id, 4001)], json!({"peer_id": "not-a-peer"})).is_err());
    }

    #[test]
    fn an_expired_manifest_still_fetches() {
        let id = peer_id();
        let past = json!({"available_until": "2020-01-01T00:00:00Z"});
        assert!(parse(id, vec![direct(id, 4001)], past).is_ok());
    }

    #[test]
    fn oversized_manifests_are_rejected() {
        assert!(parse_manifest(b"", Utc::now()).is_err());
        let big = vec![b' '; MAX_MANIFEST_BYTES as usize + 1];
        assert!(parse_manifest(&big, Utc::now()).is_err());
    }

    #[test]
    fn routes_must_name_the_robot_and_be_tcp() {
        let id = peer_id();
        assert!(parse(id, vec![], json!({})).is_err());
        // Another peer's route, a QUIC route and garbage: nothing safe is left.
        let others = vec![
            direct(peer_id(), 4001),
            format!("/ip4/10.0.0.4/udp/4001/quic-v1/p2p/{id}"),
            "not a multiaddr".to_string(),
        ];
        assert!(parse(id, others.clone(), json!({})).is_err());
        // One good route among them is enough, and duplicates collapse.
        let mut mixed = others;
        mixed.push(direct(id, 4001));
        mixed.push("/ip4/10.0.0.4/tcp/4001".into());
        assert_eq!(parse(id, mixed, json!({})).unwrap().routes.len(), 1);
    }

    #[test]
    fn route_count_is_bounded() {
        let id = peer_id();
        let many: Vec<String> = (0..17).map(|i| direct(id, 4001 + i)).collect();
        assert!(parse(id, many, json!({})).is_err());
    }

    #[test]
    fn circuit_routes_follow_direct_ones() {
        let id = peer_id();
        let relay = peer_id();
        let circuit = format!("/dns4/relay.auki.network/tcp/443/p2p/{relay}/p2p-circuit/p2p/{id}");
        let manifest = parse(id, vec![circuit.clone(), direct(id, 4001)], json!({})).unwrap();
        assert_eq!(manifest.routes.len(), 2);
        assert!(!manifest.routes[0]
            .iter()
            .any(|p| matches!(p, Protocol::P2pCircuit)));
        assert!(manifest.routes[1]
            .iter()
            .any(|p| matches!(p, Protocol::P2pCircuit)));

        // A circuit to another peer, or through a private host, is unsafe.
        let wrong_target = format!(
            "/dns4/relay.auki.network/tcp/443/p2p/{relay}/p2p-circuit/p2p/{}",
            peer_id()
        );
        let private = format!("/dns4/relay.internal/tcp/443/p2p/{relay}/p2p-circuit/p2p/{id}");
        assert!(parse(id, vec![wrong_target, private], json!({})).is_err());

        let too_many: Vec<String> = (0..4)
            .map(|i| {
                format!(
                    "/dns4/relay{i}.auki.network/tcp/443/p2p/{}/p2p-circuit/p2p/{id}",
                    peer_id()
                )
            })
            .collect();
        assert!(parse(id, too_many, json!({})).is_err());
    }

    fn manifest_with(
        blobs: Vec<(&str, &str, Option<u64>)>,
        routes: Vec<&str>,
    ) -> RecordingManifest {
        RecordingManifest {
            peer_id: peer_id(),
            routes: routes.into_iter().map(|r| r.parse().unwrap()).collect(),
            blobs: blobs
                .into_iter()
                .map(|(id, sha, size)| BlobManifestEntry {
                    id: id.into(),
                    sha256: sha.into(),
                    size_bytes: size,
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn parts_are_concatenated_in_order() {
        let temp = tempfile::TempDir::new().unwrap();
        let dest = temp.path().join("inputs/capture.zip");
        let manifest = manifest_with(
            vec![("part-1", SHA_A, Some(3)), ("part-2", SHA_B, None)],
            vec!["/ip4/10.0.0.4/tcp/4001"],
        );
        let written = fetch_blobs(&manifest, &dest, |_, _, sha| async move {
            Ok(if sha == SHA_A {
                b"PK\x03".to_vec()
            } else {
                b"\x04rest".to_vec()
            })
        })
        .await
        .unwrap();
        assert_eq!(written, 8);
        assert_eq!(fs::read(&dest).unwrap(), b"PK\x03\x04rest");
    }

    #[tokio::test]
    async fn a_failing_route_falls_back_to_the_next() {
        let temp = tempfile::TempDir::new().unwrap();
        let manifest = manifest_with(
            vec![("scan.zip", SHA_A, None)],
            vec!["/ip4/10.0.0.4/tcp/4001", "/ip4/10.0.0.5/tcp/4001"],
        );
        let tried = RefCell::new(Vec::new());
        fetch_blobs(&manifest, &temp.path().join("z"), |_, route, _| {
            tried.borrow_mut().push(route.to_string());
            let first = tried.borrow().len() == 1;
            async move {
                if first {
                    Err(anyhow!("unreachable"))
                } else {
                    Ok(b"PK".to_vec())
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(tried.borrow().len(), 2);
    }

    #[tokio::test]
    async fn a_size_that_disagrees_with_the_manifest_fails() {
        let temp = tempfile::TempDir::new().unwrap();
        let manifest = manifest_with(
            vec![("scan.zip", SHA_A, Some(999_999))],
            vec!["/ip4/10.0.0.4/tcp/4001"],
        );
        let err = fetch_blobs(&manifest, &temp.path().join("z"), |_, _, _| async {
            Ok(b"short".to_vec())
        })
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("999999"), "{err:#}");
    }

    #[tokio::test]
    async fn every_route_failing_names_the_blob() {
        let temp = tempfile::TempDir::new().unwrap();
        let manifest = manifest_with(
            vec![("scan.zip", SHA_A, None)],
            vec!["/ip4/10.0.0.4/tcp/4001"],
        );
        let err = fetch_blobs(&manifest, &temp.path().join("z"), |_, _, _| async {
            Err(anyhow!("connection refused"))
        })
        .await
        .unwrap_err();
        let message = format!("{err:#}");
        assert!(
            message.contains(SHA_A) && message.contains("connection refused"),
            "{message}"
        );
    }

    /// Two real peers on loopback: a robot serving blobs from its store over Blob v1, and this
    /// runner fetching them with nothing but the manifest. DDS is faked with a key pair made
    /// for the test.
    mod round_trip {
        use super::*;
        use auki_p2p::{
            Identity, P2PAccessClaims, P2P_TOKEN_AUDIENCE, P2P_TOKEN_ISSUER, P2P_TOKEN_SCOPE,
            P2P_TOKEN_TTL, P2P_TOKEN_TYPE,
        };
        use auki_protocols::blob::{BlobClient, BlobEndpoint, FsBlobProvider};
        use auki_sdk::{
            AukiPeer, AukiPeerConfig, DdsVerificationKeys, ExternalAuthorityUpdate,
            SignedP2pCredential,
        };
        use chrono::TimeZone;
        use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
        use p256::pkcs8::{EncodePrivateKey, EncodePublicKey, LineEnding};
        use std::time::{SystemTime, UNIX_EPOCH};
        use uuid::Uuid;

        struct Dds {
            private_pem: String,
            public_pem: String,
        }

        impl Dds {
            fn new() -> Self {
                let key = p256::SecretKey::random(&mut p256::elliptic_curve::rand_core::OsRng);
                Self {
                    private_pem: key.to_pkcs8_pem(LineEnding::LF).unwrap().to_string(),
                    public_pem: key.public_key().to_public_key_pem(LineEnding::LF).unwrap(),
                }
            }

            async fn peer(&self, domain_id: Uuid, route: Option<Multiaddr>) -> AukiPeer {
                let identity = Identity::generate();
                let issued_at = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let expiration = issued_at + P2P_TOKEN_TTL.as_secs();
                let claims = P2PAccessClaims {
                    token_type: P2P_TOKEN_TYPE.into(),
                    iss: P2P_TOKEN_ISSUER.into(),
                    aud: vec![P2P_TOKEN_AUDIENCE.into()],
                    sub: Uuid::new_v4().to_string(),
                    organization_id: None,
                    peer_type: Some("compute".into()),
                    peer_id: identity.peer_id().to_string(),
                    domain_ids: vec![domain_id.to_string()],
                    scopes: vec![P2P_TOKEN_SCOPE.into()],
                    application: None,
                    iat: issued_at,
                    nbf: None,
                    exp: expiration,
                };
                let token = encode(
                    &Header::new(Algorithm::ES256),
                    &claims,
                    &EncodingKey::from_ec_pem(self.private_pem.as_bytes()).unwrap(),
                )
                .unwrap();
                let update = ExternalAuthorityUpdate::new(
                    domain_id,
                    identity.peer_id(),
                    DdsVerificationKeys::new(0, self.public_pem.as_bytes().to_vec(), None),
                    SignedP2pCredential::new(token).unwrap(),
                    Utc.timestamp_opt(expiration as i64, 0).unwrap(),
                );
                let mut config = AukiPeerConfig::new("http://127.0.0.1:9")
                    .unwrap()
                    .direct_only();
                if let Some(route) = route {
                    config = config
                        .with_listen_addresses([route.clone()])
                        .unwrap()
                        .with_advertised_direct_routes([route])
                        .unwrap();
                }
                AukiPeer::start_external(identity, update, config)
                    .await
                    .unwrap()
                    .0
            }
        }

        fn loopback_route() -> Multiaddr {
            let port = std::net::TcpListener::bind(("127.0.0.1", 0))
                .unwrap()
                .local_addr()
                .unwrap()
                .port();
            format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap()
        }

        async fn fetch_parts(parts: &[&[u8]]) -> Vec<u8> {
            let dds = Dds::new();
            let domain_id = Uuid::new_v4();
            let store = tempfile::tempdir().unwrap();
            let blobs: Vec<Value> = parts
                .iter()
                .enumerate()
                .map(|(i, part)| {
                    let sha = auki_registry::put_blob(store.path(), part).unwrap();
                    json!({"id": format!("part-{i}"), "sha256": sha, "size_bytes": part.len()})
                })
                .collect();

            let robot = dds.peer(domain_id, Some(loopback_route())).await;
            let _endpoint = BlobEndpoint::mount(
                robot.protocol_context().protocols(),
                FsBlobProvider::new(store.path()),
            )
            .unwrap();
            let advertised = robot
                .protocol_context()
                .routes()
                .snapshot()
                .unwrap()
                .direct_routes[0]
                .to_string();
            let robot_id = robot.protocol_context().peer_id();
            let manifest = parse(robot_id, vec![advertised], json!({"blobs": blobs})).unwrap();

            let runner = dds.peer(domain_id, None).await;
            let client = BlobClient::new(runner.protocol_context().protocols());
            let dest = store.path().join("fetched.zip");
            fetch_blobs(&manifest, &dest, |peer_id, route, sha| {
                let client = client.clone();
                async move {
                    Ok(client
                        .fetch_exact(peer_id, route, sha)
                        .await
                        .map_err(|e| anyhow!("{e}"))?
                        .into_bytes())
                }
            })
            .await
            .unwrap();

            runner.shutdown().await.unwrap();
            robot.shutdown().await.unwrap();
            fs::read(dest).unwrap()
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_published_zip_round_trips_from_the_robot() {
            let zip = b"PK\x03\x04 pretend this is a capture archive";
            assert_eq!(fetch_parts(&[zip]).await, zip);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_zip_in_parts_round_trips_in_order() {
            let fetched =
                fetch_parts(&[b"PK\x03\x04 first part, ", b"second part, ", b"last"]).await;
            assert_eq!(fetched, b"PK\x03\x04 first part, second part, last");
        }
    }
}
