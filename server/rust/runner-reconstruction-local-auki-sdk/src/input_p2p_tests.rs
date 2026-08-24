use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use auki_p2p::{
    DdsTokenVerifier, Identity, Multiaddr as P2pMultiaddr, Node, P2PAccessClaims, PeerRole,
    P2P_TOKEN_AUDIENCE, P2P_TOKEN_ISSUER, P2P_TOKEN_SCOPE, P2P_TOKEN_TTL, P2P_TOKEN_TYPE,
};
use compute_runner_api::runner::AccessTokenProvider;
use compute_runner_api::runner::{DomainArtifactContent, DomainArtifactRequest};
use compute_runner_api::{
    ArtifactSink, ControlPlane, InputSource, LeaseEnvelope, P2pDataset, P2pDatasetRegistration,
    TaskCtx,
};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use posemesh_compute_node::dds::p2p::P2pCredentialStore;
use posemesh_compute_node::p2p_dataset::{P2pDatasetAdapter, P2pDatasetServer};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

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

const REFERENCE_NAME: &str = "scan_path_recording_2026-08-19_12-00-00";
const SCAN_NAME: &str = "2026-08-19_12-00-00";

#[derive(Clone)]
struct FakeReferenceInput {
    cid: String,
    materialized: MaterializedInput,
}

#[async_trait]
impl InputSource for FakeReferenceInput {
    async fn get_bytes_by_cid(&self, cid: &str) -> Result<Vec<u8>> {
        if cid != self.cid {
            anyhow::bail!("unexpected CID {cid}");
        }
        Ok(fs::read(&self.materialized.path)?)
    }

    async fn materialize_cid_to_temp(&self, cid: &str) -> Result<PathBuf> {
        if cid != self.cid {
            anyhow::bail!("unexpected CID {cid}");
        }
        Ok(self.materialized.path.clone())
    }

    async fn materialize_cid_with_meta(&self, cid: &str) -> Result<MaterializedInput> {
        if cid != self.cid {
            anyhow::bail!("unexpected CID {cid}");
        }
        Ok(self.materialized.clone())
    }
}

struct NoSink;

#[async_trait]
impl ArtifactSink for NoSink {
    async fn put_bytes(&self, _rel_path: &str, _bytes: &[u8]) -> Result<()> {
        anyhow::bail!("input materialization must not upload bytes")
    }

    async fn put_file(&self, _rel_path: &str, _file_path: &Path) -> Result<()> {
        anyhow::bail!("input materialization must not upload files")
    }
}

struct QuietControl;

#[async_trait]
impl ControlPlane for QuietControl {
    async fn is_cancelled(&self) -> bool {
        false
    }

    async fn progress(&self, _value: Value) -> Result<()> {
        Ok(())
    }

    async fn log_event(&self, _fields: Value) -> Result<()> {
        Ok(())
    }
}

struct NoToken;

impl AccessTokenProvider for NoToken {
    fn get(&self) -> String {
        "synthetic-domain-token".into()
    }
}

static NO_SINK: NoSink = NoSink;
static QUIET_CONTROL: QuietControl = QuietControl;
static NO_TOKEN: NoToken = NoToken;

#[derive(Debug)]
struct RecordedArtifact {
    rel_path: String,
    name: String,
    data_type: String,
    bytes: Vec<u8>,
}

#[derive(Default)]
struct RecordingOutput {
    artifacts: Mutex<Vec<RecordedArtifact>>,
}

#[async_trait]
impl ArtifactSink for RecordingOutput {
    async fn put_bytes(&self, _rel_path: &str, _bytes: &[u8]) -> Result<()> {
        anyhow::bail!("output must retain explicit Domain artifact metadata")
    }

    async fn put_file(&self, _rel_path: &str, _file_path: &Path) -> Result<()> {
        anyhow::bail!("output must retain explicit Domain artifact metadata")
    }

    async fn put_domain_artifact(
        &self,
        request: DomainArtifactRequest<'_>,
    ) -> Result<Option<String>> {
        let bytes = match request.content {
            DomainArtifactContent::Bytes(bytes) => bytes.to_vec(),
            DomainArtifactContent::File(path) => fs::read(path)?,
        };
        self.artifacts.lock().unwrap().push(RecordedArtifact {
            rel_path: request.rel_path.into(),
            name: request.name.into(),
            data_type: request.data_type.into(),
            bytes,
        });
        Ok(Some(format!("artifact-{}", request.name)))
    }
}

struct UnexpectedP2p;

#[async_trait]
impl P2pDataset for UnexpectedP2p {
    async fn register(&self, _registration: P2pDatasetRegistration) -> Result<P2pDatasetReference> {
        anyhow::bail!("unexpected registration")
    }

    async fn fetch(&self, _reference: &P2pDatasetReference, _destination: &Path) -> Result<()> {
        anyhow::bail!("validation should fail before P2P fetch")
    }
}

struct CapturingP2p {
    zip: Vec<u8>,
    references: Mutex<Vec<P2pDatasetReference>>,
}

impl CapturingP2p {
    fn new(zip: Vec<u8>) -> Self {
        Self {
            zip,
            references: Mutex::new(Vec::new()),
        }
    }

    fn references(&self) -> Vec<P2pDatasetReference> {
        self.references.lock().unwrap().clone()
    }
}

#[async_trait]
impl P2pDataset for CapturingP2p {
    async fn register(&self, _registration: P2pDatasetRegistration) -> Result<P2pDatasetReference> {
        anyhow::bail!("unexpected registration")
    }

    async fn fetch(&self, reference: &P2pDatasetReference, destination: &Path) -> Result<()> {
        self.references.lock().unwrap().push(reference.clone());
        fs::write(destination, &self.zip)?;
        Ok(())
    }
}

struct DatasetFixture {
    robot: Node,
    compute: Node,
    robot_adapter: P2pDatasetAdapter,
    compute_adapter: P2pDatasetAdapter,
    server: P2pDatasetServer,
}

impl DatasetFixture {
    async fn start(domain_id: Uuid) -> Self {
        let robot = Node::start(
            Identity::generate(),
            verifier(),
            ["/ip4/127.0.0.1/tcp/0".parse::<P2pMultiaddr>().unwrap()],
        )
        .unwrap();
        let robot_address =
            tokio::time::timeout(StdDuration::from_secs(5), robot.first_listen_address())
                .await
                .expect("Robot listener timed out")
                .expect("Robot listen address");
        let robot_credentials = P2pCredentialStore::new(robot.clone());
        install_current_token(&robot_credentials, &robot, PeerRole::Robot, domain_id).await;
        let robot_adapter =
            P2pDatasetAdapter::new(robot.clone(), robot_credentials, vec![robot_address]);
        let shutdown = CancellationToken::new();
        let server = robot_adapter
            .start_serving(domain_id, &shutdown)
            .await
            .unwrap();

        let compute = Node::start(
            Identity::generate(),
            verifier(),
            std::iter::empty::<P2pMultiaddr>(),
        )
        .unwrap();
        let compute_credentials = P2pCredentialStore::new(compute.clone());
        install_current_token(&compute_credentials, &compute, PeerRole::Compute, domain_id).await;
        let compute_adapter =
            P2pDatasetAdapter::new(compute.clone(), compute_credentials, Vec::new());

        Self {
            robot,
            compute,
            robot_adapter,
            compute_adapter,
            server,
        }
    }

    async fn register(&self, domain_id: Uuid, source: PathBuf) -> P2pDatasetReference {
        self.robot_adapter
            .register_dataset(P2pDatasetRegistration {
                dataset_id: Uuid::new_v4().to_string(),
                domain_id,
                name: REFERENCE_NAME.into(),
                path: source,
                available_until: Utc::now() + chrono::Duration::minutes(10),
            })
            .await
            .unwrap()
    }

    async fn shutdown(self) {
        self.server.shutdown().await.unwrap();
        self.robot.shutdown().await.unwrap();
        self.compute.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn verified_p2p_zip_reaches_the_existing_safe_extraction_path() {
    let temp = TempDir::new().unwrap();
    let domain_id = Uuid::new_v4();
    let fixture = DatasetFixture::start(domain_id).await;
    let source = temp.path().join("robot/session.zip");
    write_capture_zip(&source);
    let reference = fixture.register(domain_id, source).await;

    let run = run_materialization(
        temp.path(),
        "success",
        domain_id,
        serde_json::to_vec(&reference).unwrap(),
        REFERENCE_NAME,
        RECORDING_REFERENCE_DATA_TYPE,
        &fixture.compute_adapter,
    )
    .await;
    let session = run.result.unwrap();

    assert_eq!(session.scan_name, SCAN_NAME);
    assert_eq!(session.session_ids, vec!["R001-synthetic"]);
    assert_eq!(session.extracted_files, 3);
    assert!(session
        .app_root
        .join("R001-synthetic/sensorlogs/head_left_rgb/log_manifest.json")
        .is_file());
    assert!(run
        .workspace
        .root()
        .join(format!("inputs/{}.zip", reference.dataset_id))
        .is_file());
    assert!(!run.domain_input_root.exists());

    let output = RecordingOutput::default();
    fs::write(run.workspace.root().join("log.txt"), b"synthetic task log").unwrap();
    let sfm = run.workspace.refined_local().join(SCAN_NAME).join("sfm");
    fs::create_dir_all(&sfm).unwrap();
    for (name, bytes) in [
        ("images.bin", b"images".as_slice()),
        ("cameras.bin", b"cameras".as_slice()),
        ("points3D.bin", b"points".as_slice()),
        ("portals.csv", b"portal".as_slice()),
    ] {
        fs::write(sfm.join(name), bytes).unwrap();
    }

    crate::upload_task_log(&output, &run.workspace, "task-123")
        .await
        .unwrap();
    let uploaded = crate::refined::RefinedUploader::new()
        .process(&run.workspace, &output, true)
        .await
        .unwrap();
    assert_eq!(uploaded, vec![SCAN_NAME]);

    {
        let artifacts = output.artifacts.lock().unwrap();
        let task_log = artifacts
            .iter()
            .find(|artifact| artifact.data_type == "task_log_txt")
            .expect("task log upload");
        assert_eq!(task_log.rel_path, "logs/task-123.txt");
        assert_eq!(task_log.name, "task_log_task-123");
        assert_eq!(task_log.bytes, b"synthetic task log");

        let refined = artifacts
            .iter()
            .find(|artifact| artifact.data_type == "refined_scan_zip")
            .expect("refined scan upload");
        assert_eq!(
            refined.rel_path,
            format!("refined/local/{SCAN_NAME}/RefinedScan.zip")
        );
        assert_eq!(refined.name, format!("refined_scan_{SCAN_NAME}"));
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&refined.bytes)).unwrap();
        for expected in ["images.bin", "cameras.bin", "points3D.bin", "portals.csv"] {
            assert!(archive.by_name(expected).is_ok(), "missing {expected}");
        }
    }

    fixture.shutdown().await;
}

#[tokio::test]
async fn malformed_schema_and_authorization_fields_are_rejected_before_fetch() {
    let temp = TempDir::new().unwrap();
    let domain_id = Uuid::new_v4();
    let reference = synthetic_reference(domain_id);

    let mut wrong_schema = serde_json::to_value(&reference).unwrap();
    wrong_schema["schema"] = json!("auki-p2p-dataset/v99");
    let run = run_materialization(
        temp.path(),
        "schema",
        domain_id,
        serde_json::to_vec(&wrong_schema).unwrap(),
        REFERENCE_NAME,
        RECORDING_REFERENCE_DATA_TYPE,
        &UnexpectedP2p,
    )
    .await;
    assert!(format_error(run.result).contains("unsupported recording reference schema"));

    let mut with_token = serde_json::to_value(&reference).unwrap();
    with_token["p2p_access_token"] = json!("synthetic-secret-must-not-be-accepted");
    let run = run_materialization(
        temp.path(),
        "unknown-auth",
        domain_id,
        serde_json::to_vec(&with_token).unwrap(),
        REFERENCE_NAME,
        RECORDING_REFERENCE_DATA_TYPE,
        &UnexpectedP2p,
    )
    .await;
    assert!(format_error(run.result).contains("unknown field `p2p_access_token`"));
}

#[tokio::test]
async fn wrong_domain_expired_and_non_tcp_references_are_rejected_before_fetch() {
    let temp = TempDir::new().unwrap();
    let domain_id = Uuid::new_v4();

    let mut wrong_domain = synthetic_reference(domain_id);
    wrong_domain.domain_id = Uuid::new_v4();
    let run = run_reference(
        temp.path(),
        "wrong-domain",
        domain_id,
        &wrong_domain,
        &UnexpectedP2p,
    )
    .await;
    assert!(format_error(run.result).contains("Domain does not match the task lease"));

    let mut expired = synthetic_reference(domain_id);
    expired.available_until = Utc::now() - chrono::Duration::seconds(1);
    let run = run_reference(temp.path(), "expired", domain_id, &expired, &UnexpectedP2p).await;
    assert!(format_error(run.result).contains("reference has expired"));

    let mut non_tcp = synthetic_reference(domain_id);
    non_tcp.multiaddrs = vec!["/ip4/127.0.0.1/udp/41001".into()];
    let run = run_reference(temp.path(), "non-tcp", domain_id, &non_tcp, &UnexpectedP2p).await;
    assert!(format_error(run.result).contains("no safe direct or circuit route"));

    let valid = synthetic_reference(domain_id);
    let run = run_materialization(
        temp.path(),
        "wrong-type",
        domain_id,
        serde_json::to_vec(&valid).unwrap(),
        REFERENCE_NAME,
        "scan_path_recording_zip",
        &UnexpectedP2p,
    )
    .await;
    assert!(format_error(run.result).contains(RECORDING_REFERENCE_DATA_TYPE));
}

#[tokio::test]
async fn one_to_three_circuit_routes_reach_the_unchanged_materialization_path() {
    let temp = TempDir::new().unwrap();
    let domain_id = Uuid::new_v4();
    let source = temp.path().join("circuit-fixture.zip");
    write_capture_zip(&source);
    let zip = fs::read(source).unwrap();

    for count in 1..=3 {
        let mut reference = synthetic_reference(domain_id);
        reference.multiaddrs = circuit_routes(&reference.peer_id, count);
        let p2p = CapturingP2p::new(zip.clone());
        let run = run_reference(
            temp.path(),
            &format!("circuit-{count}"),
            domain_id,
            &reference,
            &p2p,
        )
        .await;
        let session = run.result.unwrap();

        assert_eq!(session.session_ids, vec!["R001-synthetic"]);
        assert_eq!(session.extracted_files, 3);
        assert!(session
            .app_root
            .join("R001-synthetic/sensorlogs/head_left_rgb/log_manifest.json")
            .is_file());
        let captured = p2p.references();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].multiaddrs, reference.multiaddrs);
    }
}

#[tokio::test]
async fn malformed_route_is_filtered_before_a_valid_circuit_reaches_fetch() {
    let temp = TempDir::new().unwrap();
    let domain_id = Uuid::new_v4();
    let source = temp.path().join("filtered-fixture.zip");
    write_capture_zip(&source);
    let p2p = CapturingP2p::new(fs::read(source).unwrap());
    let mut reference = synthetic_reference(domain_id);
    let valid = circuit_routes(&reference.peer_id, 1).remove(0);
    let relay = Identity::generate().peer_id();
    let wrong_target = Identity::generate().peer_id();
    let malformed = format!(
        "/dns4/relay-invalid.dev.aukiverse.com/tcp/443/p2p/{relay}/p2p-circuit/p2p/{wrong_target}"
    );
    reference.multiaddrs = vec![malformed, valid.clone()];

    let run = run_reference(temp.path(), "filtered-circuit", domain_id, &reference, &p2p).await;
    assert!(run.result.is_ok());
    assert_eq!(p2p.references()[0].multiaddrs, vec![valid]);
}

#[test]
fn route_candidate_and_circuit_bounds_match_posemesh() {
    let target = Identity::generate().peer_id();
    let sixteen = (1..=16)
        .map(|index| format!("/ip4/192.0.2.{index}/tcp/41001"))
        .collect::<Vec<_>>();
    assert_eq!(
        validate_multiaddrs(&sixteen, target).unwrap().len(),
        sixteen.len()
    );

    let mut seventeen = sixteen;
    seventeen.push("/dns4/robot.dev.aukiverse.com/tcp/41001".into());
    assert!(validate_multiaddrs(&seventeen, target)
        .unwrap_err()
        .to_string()
        .contains("maximum is 16"));

    let four_circuits = circuit_routes(&target.to_string(), 4);
    assert!(validate_multiaddrs(&four_circuits, target)
        .unwrap_err()
        .to_string()
        .contains("maximum is 3"));
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_robot_peer_id_fails_before_dataset_bytes_are_accepted() {
    let temp = TempDir::new().unwrap();
    let domain_id = Uuid::new_v4();
    let fixture = DatasetFixture::start(domain_id).await;
    let source = temp.path().join("robot/session.zip");
    write_capture_zip(&source);
    let mut reference = fixture.register(domain_id, source).await;
    reference.peer_id = Identity::generate().peer_id().to_string();

    let run = tokio::time::timeout(
        StdDuration::from_secs(5),
        run_reference(
            temp.path(),
            "wrong-peer",
            domain_id,
            &reference,
            &fixture.compute_adapter,
        ),
    )
    .await
    .expect("wrong Peer ID check timed out");
    assert!(run.result.is_err());
    assert_no_completed_or_partial_zip(&run.workspace, &reference.dataset_id);

    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn hash_and_size_mismatches_leave_no_completed_or_partial_zip() {
    let temp = TempDir::new().unwrap();
    let domain_id = Uuid::new_v4();
    let fixture = DatasetFixture::start(domain_id).await;
    let source = temp.path().join("robot/session.zip");
    write_capture_zip(&source);
    let reference = fixture.register(domain_id, source).await;

    let mut wrong_hash = reference.clone();
    wrong_hash.sha256 = "00".repeat(32);
    let run = run_reference(
        temp.path(),
        "wrong-hash",
        domain_id,
        &wrong_hash,
        &fixture.compute_adapter,
    )
    .await;
    assert!(run.result.is_err());
    assert_no_completed_or_partial_zip(&run.workspace, &wrong_hash.dataset_id);

    let mut wrong_size = reference;
    wrong_size.size_bytes += 1;
    let run = run_reference(
        temp.path(),
        "wrong-size",
        domain_id,
        &wrong_size,
        &fixture.compute_adapter,
    )
    .await;
    assert!(run.result.is_err());
    assert_no_completed_or_partial_zip(&run.workspace, &wrong_size.dataset_id);

    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn interrupted_transfer_retries_from_zero_and_cleans_partial_files() {
    let temp = TempDir::new().unwrap();
    let domain_id = Uuid::new_v4();
    let fixture = DatasetFixture::start(domain_id).await;
    let source = temp.path().join("robot/session.zip");
    write_capture_zip(&source);
    let reference = fixture.register(domain_id, source.clone()).await;

    // Registration captured the original size/hash. Truncating the owned source makes
    // both real adapter attempts end before that declared size.
    fs::write(&source, b"PK\x03\x04truncated").unwrap();
    let run = run_reference(
        temp.path(),
        "interrupted",
        domain_id,
        &reference,
        &fixture.compute_adapter,
    )
    .await;
    assert!(run.result.is_err());
    assert_no_completed_or_partial_zip(&run.workspace, &reference.dataset_id);

    fixture.shutdown().await;
}

struct MaterializationRun {
    result: Result<MaterializedSession>,
    workspace: Workspace,
    domain_input_root: PathBuf,
}

async fn run_reference(
    root: &Path,
    case: &str,
    domain_id: Uuid,
    reference: &P2pDatasetReference,
    p2p: &dyn P2pDataset,
) -> MaterializationRun {
    run_materialization(
        root,
        case,
        domain_id,
        serde_json::to_vec(reference).unwrap(),
        REFERENCE_NAME,
        RECORDING_REFERENCE_DATA_TYPE,
        p2p,
    )
    .await
}

async fn run_materialization(
    root: &Path,
    case: &str,
    domain_id: Uuid,
    reference_bytes: Vec<u8>,
    artifact_name: &str,
    data_type: &str,
    p2p: &dyn P2pDataset,
) -> MaterializationRun {
    let cid = format!("https://domain.example/v1/data/{case}");
    let domain_input_root = root.join(format!("domain-input-{case}"));
    let path = domain_input_root
        .join("datasets")
        .join(SCAN_NAME)
        .join(format!("{case}.json"));
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, reference_bytes).unwrap();
    let mut materialized = MaterializedInput::new(cid.clone(), path);
    materialized.root_dir = domain_input_root.clone();
    materialized.name = Some(artifact_name.into());
    materialized.data_type = Some(data_type.into());
    materialized.domain_id = Some(domain_id.to_string());
    let input = FakeReferenceInput {
        cid: cid.clone(),
        materialized,
    };
    let workspace = Workspace::create(
        Some(&root.join(format!("workspace-{case}"))),
        &domain_id.to_string(),
        Some(case),
        &Uuid::new_v4().to_string(),
    )
    .unwrap();
    let lease = test_lease(domain_id, cid);
    let ctx = TaskCtx {
        lease: &lease,
        input: &input,
        output: &NO_SINK,
        ctrl: &QUIET_CONTROL,
        access_token: &NO_TOKEN,
        p2p_dataset: Some(p2p),
    };
    let result = materialize_session_zip(&ctx, &workspace).await;
    MaterializationRun {
        result,
        workspace,
        domain_input_root,
    }
}

fn test_lease(domain_id: Uuid, cid: String) -> LeaseEnvelope {
    serde_json::from_value(json!({
        "domain_id": domain_id,
        "domain_server_url": "https://domain.example",
        "task": {
            "id": Uuid::new_v4(),
            "job_id": Uuid::new_v4(),
            "capability": "/reconstruction/local-refinement-auki-sdk/v0",
            "inputs_cids": [cid]
        }
    }))
    .unwrap()
}

fn synthetic_reference(domain_id: Uuid) -> P2pDatasetReference {
    P2pDatasetReference {
        schema: P2P_DATASET_SCHEMA.into(),
        dataset_id: Uuid::new_v4().to_string(),
        domain_id,
        name: REFERENCE_NAME.into(),
        peer_id: Identity::generate().peer_id().to_string(),
        multiaddrs: vec!["/ip4/127.0.0.1/tcp/41001".into()],
        size_bytes: 1024,
        sha256: "11".repeat(32),
        available_until: Utc::now() + chrono::Duration::minutes(10),
    }
}

fn circuit_routes(target_peer_id: &str, count: usize) -> Vec<String> {
    (0..count)
        .map(|index| {
            let relay_peer_id = Identity::generate().peer_id();
            format!(
                "/dns4/relay-{index}.dev.aukiverse.com/tcp/{}/p2p/{relay_peer_id}/p2p-circuit/p2p/{target_peer_id}",
                4400 + index
            )
        })
        .collect()
}

fn write_capture_zip(path: &Path) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let file = fs::File::create(path).unwrap();
    let mut writer = zip::ZipWriter::new(file);
    let options =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (name, contents) in [
        (
            "registries/sensors/galbot/entry.json",
            b"registry".as_slice(),
        ),
        (
            "R001-synthetic/sensorlogs/head_left_rgb/log_manifest.json",
            b"sensorlog".as_slice(),
        ),
        (
            "R001-synthetic/poselogs/world__base_link/log_manifest.json",
            b"poselog".as_slice(),
        ),
    ] {
        writer.start_file(name, options).unwrap();
        writer.write_all(contents).unwrap();
    }
    writer.finish().unwrap();
}

fn assert_no_completed_or_partial_zip(workspace: &Workspace, dataset_id: &str) {
    let input_dir = workspace.root().join("inputs");
    assert!(!input_dir.join(format!("{dataset_id}.zip")).exists());
    if let Ok(entries) = fs::read_dir(input_dir) {
        for entry in entries.flatten() {
            assert!(
                !entry.file_name().to_string_lossy().ends_with(".part"),
                "partial P2P input was not cleaned up: {}",
                entry.path().display()
            );
        }
    }
}

fn format_error(result: Result<MaterializedSession>) -> String {
    format!("{:#}", result.unwrap_err())
}

fn verifier() -> DdsTokenVerifier {
    DdsTokenVerifier::from_es256_pem(TEST_DDS_PUBLIC_KEY).unwrap()
}

async fn install_current_token(
    credentials: &P2pCredentialStore,
    node: &Node,
    role: PeerRole,
    domain_id: Uuid,
) {
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let expires_at_unix = issued_at + P2P_TOKEN_TTL.as_secs();
    let claims = P2PAccessClaims {
        token_type: P2P_TOKEN_TYPE.into(),
        iss: P2P_TOKEN_ISSUER.into(),
        aud: vec![P2P_TOKEN_AUDIENCE.into()],
        sub: Uuid::new_v4().to_string(),
        peer_type: role,
        peer_id: node.peer_id().to_string(),
        domain_ids: vec![domain_id.to_string()],
        scopes: vec![P2P_TOKEN_SCOPE.into()],
        iat: issued_at,
        exp: expires_at_unix,
    };
    let token = encode(
        &Header::new(Algorithm::ES256),
        &claims,
        &EncodingKey::from_ec_pem(TEST_DDS_PRIVATE_KEY).unwrap(),
    )
    .unwrap();
    credentials
        .install(
            token,
            DateTime::from_timestamp(expires_at_unix as i64, 0).unwrap(),
        )
        .await
        .unwrap();
}
