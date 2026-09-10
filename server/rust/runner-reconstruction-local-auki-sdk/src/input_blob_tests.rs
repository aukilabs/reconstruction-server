//! Validation and fetch for the Blob v1 recording manifest.
//!
//! Replaces `input_p2p_tests.rs`, which was built on `P2pDatasetAdapter` /
//! `P2pDatasetServer` / `P2pCredentialStore` — all deleted upstream with the
//! dataset protocol.
//!
//! Three of that file's nine cases are deliberately not carried over, because
//! they now test the SDK rather than us:
//!
//! * *hash and size mismatches leave no partial zip* — `BlobClient` verifies
//!   the SHA-256 of every byte before returning, and we write to disk only
//!   after a verified fetch. There is no window in which a partial or wrong
//!   archive exists, so there is nothing left to assert.
//! * *interrupted transfer retries from zero and cleans partial files* — same
//!   reason: the bytes arrive verified and in memory, then get written once.
//! * *wrong Robot peer id fails before bytes are accepted* — `fetch_exact`
//!   mutually authenticates the remote peer as part of opening the stream.
//!
//! What remains ours, and is covered here: everything that has to be decided
//! *before* a fetch is attempted, and the route handling.

use std::time::{SystemTime, UNIX_EPOCH};

use auki_p2p::{
    Identity, Multiaddr, P2PAccessClaims, P2P_TOKEN_AUDIENCE, P2P_TOKEN_ISSUER, P2P_TOKEN_SCOPE,
    P2P_TOKEN_TTL, P2P_TOKEN_TYPE,
};
use auki_protocols::blob::{BlobClient, BlobEndpoint, FsBlobProvider};
use auki_sdk::{
    AukiPeer, AukiPeerConfig, DdsVerificationKeys, ExternalAuthorityUpdate, SignedP2pCredential,
};
use chrono::{Duration, TimeZone, Utc};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::{json, Value};
use uuid::Uuid;

use super::*;

const TEST_DDS_PRIVATE_KEY: &[u8] = br#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQggm4twpf4y/yNNw/k
fqecEEl4zBTwZdRDFUFp/fSxV8qhRANCAARUxrDWJ0AtEGTAYZ4412VPHqMCKoPw
UphDkcOIk7SODsKwUvTIiUr11NbXBJmbBRfhERczsuK4PVha5eg0fVqo
-----END PRIVATE KEY-----"#;

const TEST_DDS_PUBLIC_KEY: &[u8] = br#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEVMaw1idALRBkwGGeONdlTx6jAiqD
8FKYQ5HDiJO0jg7CsFL0yIlK9dTW1wSZmwUX4REXM7LiuD1YWuXoNH1aqA==
-----END PUBLIC KEY-----"#;

const SHA_ZEROS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn peer_id_text() -> String {
    Identity::generate().peer_id().to_string()
}

fn manifest(peer_id: &str, routes: Vec<&str>, overrides: Value) -> BlobManifestDocument {
    let mut value = json!({
        "schema": "auki.blob.manifest/1",
        "peer_id": peer_id,
        "routes": routes,
        "content_type": "application/zip",
        "available_until": (Utc::now() + Duration::hours(1)).to_rfc3339(),
        "blobs": [{"id": "scan.zip", "sha256": SHA_ZEROS, "size_bytes": 1024}],
    });
    if let (Some(base), Some(extra)) = (value.as_object_mut(), overrides.as_object()) {
        for (key, patch) in extra {
            if patch.is_null() {
                base.remove(key);
            } else {
                base.insert(key.clone(), patch.clone());
            }
        }
    }
    serde_json::from_value(value).expect("fixture must deserialize")
}

fn route_for(peer_id: &str, port: u16) -> String {
    format!("/ip4/127.0.0.1/tcp/{port}/p2p/{peer_id}")
}

// ------------------------------------------------------------------ schema

#[test]
fn a_well_formed_manifest_is_accepted() {
    let peer = peer_id_text();
    let document = manifest(&peer, vec![&route_for(&peer, 4001)], json!({}));
    let validated = validate_manifest_document(document, Utc::now()).unwrap();
    assert_eq!(validated.blob.sha256, SHA_ZEROS);
    assert_eq!(validated.routes.len(), 1);
}

#[test]
fn a_dataset_reference_from_an_old_publisher_is_rejected_loudly() {
    // The transition case. `deny_unknown_fields` means an old publisher's body
    // fails at parse rather than being half-understood.
    let body = json!({
        "schema": "auki-p2p-dataset/1",
        "dataset_id": Uuid::new_v4().to_string(),
        "domain_id": Uuid::new_v4().to_string(),
        "name": "scan_path_recording_2026-09-09",
        "peer_id": peer_id_text(),
        "multiaddrs": ["/ip4/127.0.0.1/tcp/4001"],
        "size_bytes": 10,
        "sha256": SHA_ZEROS,
        "available_until": Utc::now().to_rfc3339(),
    });
    let error = serde_json::from_value::<BlobManifestDocument>(body)
        .expect_err("an auki-p2p-dataset reference must not parse as a manifest");
    assert!(error.to_string().contains("unknown field"), "{error}");
}

#[test]
fn an_unknown_schema_is_rejected() {
    let peer = peer_id_text();
    let document = manifest(
        &peer,
        vec![&route_for(&peer, 4001)],
        json!({"schema": "auki.blob.manifest/99"}),
    );
    let error = validate_manifest_document(document, Utc::now()).unwrap_err();
    assert!(error.to_string().contains("unsupported"), "{error}");
}

#[test]
fn a_manifest_must_carry_exactly_one_blob() {
    // A recording is one ZIP. Several entries means the publisher used a verb
    // that splits its output, which this capability cannot reassemble.
    let peer = peer_id_text();
    for blobs in [
        json!([]),
        json!([
            {"id": "a.zip", "sha256": SHA_ZEROS, "size_bytes": 1},
            {"id": "b.zip", "sha256": SHA_ZEROS, "size_bytes": 1},
        ]),
    ] {
        let document = manifest(
            &peer,
            vec![&route_for(&peer, 4001)],
            json!({"blobs": blobs}),
        );
        assert!(validate_manifest_document(document, Utc::now()).is_err());
    }
}

#[test]
fn a_non_zip_content_type_is_rejected() {
    let peer = peer_id_text();
    let document = manifest(
        &peer,
        vec![&route_for(&peer, 4001)],
        json!({"content_type": "image/jpeg"}),
    );
    let error = validate_manifest_document(document, Utc::now()).unwrap_err();
    assert!(error.to_string().contains("application/zip"), "{error}");
}

#[test]
fn malformed_digests_are_rejected() {
    let peer = peer_id_text();
    // A digest of all zeros uppercases to itself, so the lowercase rule needs a
    // value with letters in it to be tested at all.
    let mixed_case = "ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    for sha in ["not-hex", "abcd", mixed_case] {
        let document = manifest(
            &peer,
            vec![&route_for(&peer, 4001)],
            json!({"blobs": [{"id": "s.zip", "sha256": sha, "size_bytes": 1}]}),
        );
        assert!(
            validate_manifest_document(document, Utc::now()).is_err(),
            "sha256 {sha} should be rejected"
        );
    }
}

#[test]
fn a_zero_size_blob_is_rejected() {
    let peer = peer_id_text();
    let document = manifest(
        &peer,
        vec![&route_for(&peer, 4001)],
        json!({"blobs": [{"id": "s.zip", "sha256": SHA_ZEROS, "size_bytes": 0}]}),
    );
    assert!(validate_manifest_document(document, Utc::now()).is_err());
}

#[test]
fn an_invalid_peer_id_is_rejected() {
    let document = manifest("not-a-peer-id", vec!["/ip4/127.0.0.1/tcp/4001"], json!({}));
    assert!(validate_manifest_document(document, Utc::now()).is_err());
}

// ------------------------------------------------------------------ expiry

#[test]
fn an_expired_manifest_is_a_warning_not_a_rejection() {
    // A blob has no expiry and nothing prunes the publisher's blob root, so a
    // passed deadline means "this manifest is old", not "the bytes are gone".
    // Refusing would turn a probably-fine fetch into a certain failure.
    let peer = peer_id_text();
    let document = manifest(
        &peer,
        vec![&route_for(&peer, 4001)],
        json!({"available_until": (Utc::now() - Duration::hours(2)).to_rfc3339()}),
    );
    let validated = validate_manifest_document(document, Utc::now())
        .expect("an advisory deadline must not block the fetch");
    assert_eq!(validated.blob.sha256, SHA_ZEROS);
}

#[test]
fn a_manifest_without_a_deadline_is_accepted() {
    let peer = peer_id_text();
    let document = manifest(
        &peer,
        vec![&route_for(&peer, 4001)],
        json!({"available_until": null}),
    );
    assert!(validate_manifest_document(document, Utc::now()).is_ok());
}

// ------------------------------------------------------------------ routes

#[test]
fn a_manifest_with_no_routes_is_rejected() {
    let document = manifest(&peer_id_text(), vec![], json!({}));
    assert!(validate_manifest_document(document, Utc::now()).is_err());
}

#[test]
fn a_route_naming_a_different_peer_is_rejected() {
    let document = manifest(
        &peer_id_text(),
        vec![&route_for(&peer_id_text(), 4001)],
        json!({}),
    );
    assert!(validate_manifest_document(document, Utc::now()).is_err());
}

#[test]
fn a_non_tcp_route_is_rejected() {
    let peer = peer_id_text();
    let document = manifest(
        &peer,
        vec![&format!("/ip4/127.0.0.1/udp/4001/quic-v1/p2p/{peer}")],
        json!({}),
    );
    assert!(validate_manifest_document(document, Utc::now()).is_err());
}

#[test]
fn too_many_route_candidates_are_rejected() {
    let peer = peer_id_text();
    let routes: Vec<String> = (0..MAX_REFERENCE_ROUTES + 1)
        .map(|index| route_for(&peer, 4001 + index as u16))
        .collect();
    let document = manifest(
        &peer,
        routes.iter().map(String::as_str).collect(),
        json!({}),
    );
    assert!(validate_manifest_document(document, Utc::now()).is_err());
}

// ------------------------------------------------------------- round trip

async fn test_peer(domain_id: Uuid, route: Option<Multiaddr>) -> AukiPeer {
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
        &EncodingKey::from_ec_pem(TEST_DDS_PRIVATE_KEY).unwrap(),
    )
    .unwrap();
    let update = ExternalAuthorityUpdate::new(
        domain_id,
        identity.peer_id(),
        DdsVerificationKeys::new(0, TEST_DDS_PUBLIC_KEY.to_vec(), None),
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
    let (peer, _authority) = AukiPeer::start_external(identity, update, config)
        .await
        .unwrap();
    peer
}

fn available_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_published_zip_round_trips_from_the_robot_to_this_runner() {
    // The end-to-end proof: a Robot stores a ZIP as a blob and serves it over
    // Blob v1; this runner, an unrelated peer, fetches it using nothing but
    // {peer_id, route, sha256} from the manifest.
    let domain_id = Uuid::new_v4();
    let zip_bytes = b"PK\x03\x04 pretend this is a capture archive".to_vec();

    let blob_root = tempfile::tempdir().unwrap();
    let sha256 = auki_registry::put_blob(blob_root.path(), &zip_bytes).unwrap();

    let route: Multiaddr = format!("/ip4/127.0.0.1/tcp/{}", available_port())
        .parse()
        .unwrap();
    let robot = test_peer(domain_id, Some(route.clone())).await;
    let _endpoint = BlobEndpoint::mount(
        robot.protocol_context().protocols(),
        FsBlobProvider::new(blob_root.path()),
    )
    .unwrap();

    let robot_peer_id = robot.protocol_context().peer_id().to_string();
    let advertised = robot
        .protocol_context()
        .routes()
        .snapshot()
        .unwrap()
        .direct_routes
        .first()
        .expect("the robot must advertise a direct route")
        .to_string();

    let document = manifest(
        &robot_peer_id,
        vec![&advertised],
        json!({"blobs": [{"id": "scan.zip", "sha256": sha256,
                          "size_bytes": zip_bytes.len()}]}),
    );
    let validated = validate_manifest_document(document, Utc::now()).unwrap();

    let receiver = test_peer(domain_id, None).await;
    let client = BlobClient::new(receiver.protocol_context().protocols());
    let fetched = fetch_blob(&client, &validated).await.unwrap();

    assert_eq!(fetched, zip_bytes);

    receiver.shutdown().await.unwrap();
    robot.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_size_that_disagrees_with_the_manifest_fails_the_fetch() {
    let domain_id = Uuid::new_v4();
    let zip_bytes = b"a short archive".to_vec();
    let blob_root = tempfile::tempdir().unwrap();
    let sha256 = auki_registry::put_blob(blob_root.path(), &zip_bytes).unwrap();

    let route: Multiaddr = format!("/ip4/127.0.0.1/tcp/{}", available_port())
        .parse()
        .unwrap();
    let robot = test_peer(domain_id, Some(route)).await;
    let _endpoint = BlobEndpoint::mount(
        robot.protocol_context().protocols(),
        FsBlobProvider::new(blob_root.path()),
    )
    .unwrap();
    let advertised = robot
        .protocol_context()
        .routes()
        .snapshot()
        .unwrap()
        .direct_routes
        .first()
        .unwrap()
        .to_string();

    // The content hash still matches -- only the declared size is a lie, which
    // Blob v1 has no opinion about. Catching it is ours.
    let document = manifest(
        &robot.protocol_context().peer_id().to_string(),
        vec![&advertised],
        json!({"blobs": [{"id": "scan.zip", "sha256": sha256, "size_bytes": 999_999}]}),
    );
    let validated = validate_manifest_document(document, Utc::now()).unwrap();

    let receiver = test_peer(domain_id, None).await;
    let client = BlobClient::new(receiver.protocol_context().protocols());
    let error = fetch_blob(&client, &validated).await.unwrap_err();
    assert!(format!("{error:#}").contains("999999"), "{error:#}");

    receiver.shutdown().await.unwrap();
    robot.shutdown().await.unwrap();
}
