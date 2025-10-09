use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use k256::{ecdsa::SigningKey, FieldBytes};
use rand::{rngs::StdRng, SeedableRng};
use serde_json::{json, Value};
use server_adapters::{
    auth::siwe::{self, AccessBundle},
    auth::token_manager::{
        AccessAuthenticator, Clock, TokenManager, TokenManagerConfig, TokenProvider,
    },
    dms::{
        client::DmsClient,
        models::{LeaseRequest, LeaseResponse, TaskSummary},
    },
};
use sha3::{Digest, Keccak256};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};
use tokio::task::JoinHandle;

const TEST_PRIV_HEX: &str = "4c0883a69102937d6231471b5dbb6204fe5129617082798ce3f4fdf2548b6f90";

#[tokio::test]
async fn siwe_e2e_retry_succeeds_after_refresh() {
    let clock = Arc::new(TokioTestClock::new());

    let dds_state = Arc::new(MockDdsState::new(
        clock.clone(),
        vec!["nonce-1".into(), "nonce-2".into()],
        vec!["dms-token-1".into(), "dms-token-2".into()],
    ));
    let (dds_addr, dds_handle) = spawn_axum(dds_router(dds_state.clone())).await;

    let dms_state = Arc::new(MockDmsState::new(
        vec![
            "Bearer dms-token-1".into(),
            "Bearer dms-token-1".into(),
            "Bearer dms-token-2".into(),
        ],
        vec![StatusCode::UNAUTHORIZED, StatusCode::OK],
    ));
    let (dms_addr, dms_handle) = spawn_axum(dms_router(dms_state.clone())).await;

    let authenticator = Arc::new(DdsAuthenticator::new(format!("http://{}", dds_addr)));
    let token_manager = Arc::new(TokenManager::with_rng(
        authenticator,
        clock.clone(),
        TokenManagerConfig {
            safety_ratio: 1.0,
            max_retries: 0,
            jitter: Duration::ZERO,
        },
        StdRng::seed_from_u64(42),
    ));
    let provider: Arc<dyn TokenProvider> = token_manager.clone() as Arc<dyn TokenProvider>;

    let http = reqwest::Client::builder()
        .no_proxy()
        .use_rustls_tls()
        .build()
        .expect("reqwest client");
    let dms_client =
        DmsClient::new(format!("http://{}", dms_addr), provider.clone(), http).unwrap();

    let lease = dms_client
        .lease_task(&LeaseRequest {
            capability: "cap/refinement".into(),
            job_id: None,
            domain_id: None,
        })
        .await
        .expect("lease");
    assert_eq!(lease.task.as_ref().map(|t| t.id.as_str()), Some("task-123"));

    let heartbeat = dms_client
        .send_heartbeat("task-123", Some(&json!({"message": "running"})))
        .await
        .expect("heartbeat");
    assert_eq!(
        heartbeat.lease_expires_at.as_deref(),
        Some("2025-01-01T00:05:00Z")
    );
    assert_eq!(dms_state.heartbeat_calls(), 2);

    token_manager.stop().await;
    dds_handle.abort();
    dms_handle.abort();
}

#[tokio::test]
async fn siwe_e2e_stops_after_second_unauthorized() {
    let clock = Arc::new(TokioTestClock::new());

    let dds_state = Arc::new(MockDdsState::new(
        clock.clone(),
        vec!["nonce-a".into(), "nonce-b".into()],
        vec!["dms-token-a".into(), "dms-token-b".into()],
    ));
    let (dds_addr, dds_handle) = spawn_axum(dds_router(dds_state.clone())).await;

    let dms_state = Arc::new(MockDmsState::new(
        vec![
            "Bearer dms-token-a".into(),
            "Bearer dms-token-a".into(),
            "Bearer dms-token-b".into(),
        ],
        vec![StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED],
    ));
    let (dms_addr, dms_handle) = spawn_axum(dms_router(dms_state.clone())).await;

    let authenticator = Arc::new(DdsAuthenticator::new(format!("http://{}", dds_addr)));
    let token_manager = Arc::new(TokenManager::with_rng(
        authenticator,
        clock.clone(),
        TokenManagerConfig {
            safety_ratio: 1.0,
            max_retries: 0,
            jitter: Duration::ZERO,
        },
        StdRng::seed_from_u64(7),
    ));
    let provider: Arc<dyn TokenProvider> = token_manager.clone() as Arc<dyn TokenProvider>;

    let http = reqwest::Client::builder()
        .no_proxy()
        .use_rustls_tls()
        .build()
        .expect("reqwest client");
    let dms_client =
        DmsClient::new(format!("http://{}", dms_addr), provider.clone(), http).unwrap();

    dms_client
        .lease_task(&LeaseRequest {
            capability: "cap/refinement".into(),
            job_id: None,
            domain_id: None,
        })
        .await
        .expect("lease");

    let err = dms_client
        .send_heartbeat("task-123", Some(&json!({"message": "running"})))
        .await
        .expect_err("should fail after second 401");
    assert!(matches!(
        err,
        server_adapters::dms::client::DmsClientError::UnexpectedStatus(StatusCode::UNAUTHORIZED)
    ));
    assert_eq!(dms_state.heartbeat_calls(), 2);

    token_manager.stop().await;
    dds_handle.abort();
    dms_handle.abort();
}

struct DdsAuthenticator {
    base_url: String,
    priv_hex: String,
    address: String,
}

impl DdsAuthenticator {
    fn new(base_url: String) -> Self {
        let address = derive_address(TEST_PRIV_HEX);
        Self {
            base_url,
            priv_hex: TEST_PRIV_HEX.to_string(),
            address,
        }
    }
}

#[async_trait::async_trait]
impl AccessAuthenticator for DdsAuthenticator {
    async fn login(&self) -> Result<AccessBundle, server_adapters::auth::siwe::SiweError> {
        let meta = siwe::request_nonce(&self.base_url, &self.address).await?;
        let message = siwe::compose_message(&meta, &self.address, None)?;
        let signature = siwe::sign_message(&self.priv_hex, &message)?;
        siwe::verify(&self.base_url, &self.address, &message, &signature).await
    }
}

struct MockDdsState {
    clock: Arc<TokioTestClock>,
    nonces: StdMutex<VecDeque<String>>,
    tokens: StdMutex<VecDeque<String>>,
    address: String,
}

impl MockDdsState {
    fn new(clock: Arc<TokioTestClock>, nonces: Vec<String>, tokens: Vec<String>) -> Self {
        Self {
            clock,
            nonces: StdMutex::new(VecDeque::from(nonces)),
            tokens: StdMutex::new(VecDeque::from(tokens)),
            address: derive_address(TEST_PRIV_HEX),
        }
    }
}

fn dds_router(state: Arc<MockDdsState>) -> Router {
    Router::new()
        .route("/api/v1/auth/siwe/request", post(dds_nonce))
        .route("/api/v1/auth/siwe/verify", post(dds_verify))
        .with_state(state)
}

async fn dds_nonce(State(state): State<Arc<MockDdsState>>) -> impl IntoResponse {
    let nonce = state
        .nonces
        .lock()
        .expect("nonce lock")
        .pop_front()
        .expect("nonce available");
    let issued_at = state.clock.now_utc().to_rfc3339();
    Json(json!({
        "nonce": nonce,
        "domain": "example.com",
        "uri": "https://example.com",
        "version": "1",
        "chainId": 1,
        "issuedAt": issued_at
    }))
}

async fn dds_verify(
    State(state): State<Arc<MockDdsState>>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let address = body
        .get("address")
        .and_then(Value::as_str)
        .expect("address field")
        .to_lowercase();
    assert_eq!(address, state.address);
    let token = state
        .tokens
        .lock()
        .expect("token lock")
        .pop_front()
        .expect("token available");
    let expires_at = (state.clock.now_utc() + chrono::Duration::seconds(300)).to_rfc3339();
    Json(json!({
        "access_token": token,
        "access_expires_at": expires_at
    }))
}

struct MockDmsState {
    expected_headers: StdMutex<VecDeque<String>>,
    heartbeat_statuses: StdMutex<VecDeque<StatusCode>>,
    heartbeat_calls: AtomicUsize,
}

impl MockDmsState {
    fn new(headers: Vec<String>, statuses: Vec<StatusCode>) -> Self {
        Self {
            expected_headers: StdMutex::new(VecDeque::from(headers)),
            heartbeat_statuses: StdMutex::new(VecDeque::from(statuses)),
            heartbeat_calls: AtomicUsize::new(0),
        }
    }

    fn heartbeat_calls(&self) -> usize {
        self.heartbeat_calls.load(Ordering::SeqCst)
    }

    fn expect_header(&self, headers: &HeaderMap) {
        let expected = self
            .expected_headers
            .lock()
            .expect("header lock")
            .pop_front()
            .expect("expected header");
        let actual = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .expect("authorization header");
        assert_eq!(actual, expected);
    }

    fn next_status(&self) -> StatusCode {
        self.heartbeat_statuses
            .lock()
            .expect("status lock")
            .pop_front()
            .unwrap_or(StatusCode::OK)
    }
}

fn dms_router(state: Arc<MockDmsState>) -> Router {
    Router::new()
        .route("/tasks", get(dms_lease))
        .route("/tasks/:task_id/heartbeat", post(dms_heartbeat))
        .with_state(state)
}

async fn dms_lease(
    State(state): State<Arc<MockDmsState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    state.expect_header(&headers);
    Json(LeaseResponse {
        task: Some(TaskSummary {
            id: "task-123".into(),
            job_id: None,
            capability: "cap/refinement".into(),
            inputs_cids: vec![
                "https://domains.dev/api/v1/domains/domain-123/data/cid-001".to_string()
            ],
            meta: json!({}),
        }),
        domain_id: Some("domain-123".into()),
        domain_server_url: Some("https://domains.dev".into()),
        ..LeaseResponse::default()
    })
}

async fn dms_heartbeat(
    State(state): State<Arc<MockDmsState>>,
    Path(_task_id): Path<String>,
    headers: HeaderMap,
    Json(_body): Json<Value>,
) -> Response {
    state.expect_header(&headers);
    state.heartbeat_calls.fetch_add(1, Ordering::SeqCst);
    let status = state.next_status();
    if status == StatusCode::UNAUTHORIZED {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let body = LeaseResponse {
        lease_expires_at: Some("2025-01-01T00:05:00Z".into()),
        domain_server_url: Some("https://domains.dev".into()),
        ..LeaseResponse::default()
    };
    (StatusCode::OK, Json(body)).into_response()
}

async fn spawn_axum(app: Router) -> (SocketAddr, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("axum server");
    });
    (addr, handle)
}

#[derive(Clone)]
struct TokioTestClock {
    start_instant: tokio::time::Instant,
    start_std: std::time::Instant,
    start_utc: chrono::DateTime<chrono::Utc>,
}

impl TokioTestClock {
    fn new() -> Self {
        let start_instant = tokio::time::Instant::now();
        Self {
            start_std: start_instant.into_std(),
            start_utc: chrono::Utc::now(),
            start_instant,
        }
    }

    fn current_instant(&self) -> std::time::Instant {
        let elapsed = tokio::time::Instant::now().duration_since(self.start_instant);
        self.start_std + elapsed
    }

    fn current_utc(&self) -> chrono::DateTime<chrono::Utc> {
        self.start_utc
            + chrono::Duration::from_std(
                tokio::time::Instant::now().duration_since(self.start_instant),
            )
            .unwrap()
    }
}

impl Clock for TokioTestClock {
    fn now_instant(&self) -> std::time::Instant {
        self.current_instant()
    }

    fn now_utc(&self) -> chrono::DateTime<chrono::Utc> {
        self.current_utc()
    }
}

fn derive_address(priv_hex: &str) -> String {
    let key_bytes = hex::decode(priv_hex).expect("hex priv");
    let field_bytes = FieldBytes::from_slice(&key_bytes);
    let signing_key = SigningKey::from_bytes(field_bytes).expect("signing key");
    let verifying_key = signing_key.verifying_key();
    let encoded = verifying_key.to_encoded_point(false);
    let pubkey = encoded.as_bytes();
    let mut hasher = Keccak256::new();
    hasher.update(&pubkey[1..]);
    let hashed = hasher.finalize();
    let address_bytes = &hashed[12..];
    format!("0x{}", hex::encode(address_bytes))
}
