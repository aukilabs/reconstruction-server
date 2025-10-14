use axum::serve;
use clap::Parser;
use rand::{rngs::StdRng, SeedableRng};
use server_adapters::{
    auth::siwe as siwe_auth,
    auth::token_manager::{
        AccessAuthenticator, SystemClock, TokenManager, TokenManagerConfig, TokenProvider,
        TokenProviderError,
    },
    dds::{http as dds_http, register},
    dms::{
        client::DmsClient,
        executor::{run_executor_loop, ExecutorConfig, TaskExecutor},
        poller::{PollController, Poller},
        session::{CapabilitySelector, HeartbeatPolicy, SessionManager},
    },
    storage::HttpDomainClient,
};
use server_core::Services;
use sha3::Digest;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const DDS_CAPABILITIES: &[&str] = &[
    "/reconstruction/global-refinement/v1",
    "/reconstruction/local-refinement/v1",
];

mod cli;
mod config;
use crate::cli::Cli;

struct PythonRunner;
struct NoopRunner;
#[async_trait::async_trait]
impl server_core::JobRunner for PythonRunner {
    async fn run_python(
        &self,
        job: &server_core::Job,
        _capability: &str,
        cpu_workers: usize,
        cancel: CancellationToken,
    ) -> server_core::Result<()> {
        use std::io::{BufRead, BufReader, Write};
        use std::process::{Command, Stdio};
        use std::thread;
        use std::time::Duration;
        use tokio::time::sleep;
        let refinement_python = "main.py";
        let output_path = job.job_path.join("refined");
        let datasets_root = job.job_path.join("datasets");
        let log_path = job.job_path.join("log.txt");
        let mut params = vec![
            refinement_python.to_string(),
            "--mode".into(),
            job.meta.processing_type.clone(),
            "--job_root_path".into(),
            job.job_path.to_string_lossy().into_owned(),
            "--output_path".into(),
            output_path.to_string_lossy().into_owned(),
            "--domain_id".into(),
            job.meta.domain_id.clone(),
            "--job_id".into(),
            job.meta.name.clone(),
            "--local_refinement_workers".into(),
            cpu_workers.to_string(),
            "--scans".into(),
        ];
        if let Ok(entries) = std::fs::read_dir(&datasets_root) {
            for e in entries.flatten() {
                if e.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    params.push(e.file_name().to_string_lossy().into_owned());
                }
            }
        }

        let mut cmd = Command::new("python3");
        cmd.args(params)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().map_err(server_core::DomainError::Io)?;
        let log_file = std::fs::File::create(&log_path).map_err(server_core::DomainError::Io)?;

        if let Some(stdout) = child.stdout.take() {
            let mut writer = log_file.try_clone().map_err(server_core::DomainError::Io)?;
            thread::spawn(move || {
                let reader = BufReader::new(stdout);
                for line in reader.lines().map_while(Result::ok) {
                    let _ = writeln!(writer, "{}", line);
                    println!("{}", line);
                }
            });
        }
        if let Some(stderr) = child.stderr.take() {
            let mut writer = log_file.try_clone().map_err(server_core::DomainError::Io)?;
            thread::spawn(move || {
                let reader = BufReader::new(stderr);
                for line in reader.lines().map_while(Result::ok) {
                    let _ = writeln!(writer, "{}", line);
                    eprintln!("{}", line);
                }
            });
        }

        let status = loop {
            if cancel.is_cancelled() {
                tracing::warn!("Cancellation requested; terminating python runner");
                let _ = child.kill();
                let _ = child.wait().map_err(server_core::DomainError::Io)?;
                return Err(server_core::DomainError::Internal("python canceled".into()));
            }
            if let Some(status) = child.try_wait().map_err(server_core::DomainError::Io)? {
                break status;
            }
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::warn!("Cancellation requested; terminating python runner");
                    let _ = child.kill();
                    let _ = child.wait().map_err(server_core::DomainError::Io)?;
                    return Err(server_core::DomainError::Internal("python canceled".into()));
                }
                _ = sleep(Duration::from_millis(200)) => {}
            }
        };
        if !status.success() {
            return Err(server_core::DomainError::Internal("python failed".into()));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct IdentityTokenProvider {
    token: Arc<String>,
}

impl IdentityTokenProvider {
    fn new(token: String) -> Self {
        Self {
            token: Arc::new(token),
        }
    }
}

#[async_trait::async_trait]
impl TokenProvider for IdentityTokenProvider {
    async fn bearer(&self) -> std::result::Result<String, TokenProviderError> {
        Ok((*self.token).clone())
    }

    async fn on_unauthorized(&self) {}
}

#[async_trait::async_trait]
impl server_core::JobRunner for NoopRunner {
    async fn run_python(
        &self,
        _job: &server_core::Job,
        _capability: &str,
        _cpu_workers: usize,
        _cancel: CancellationToken,
    ) -> server_core::Result<()> {
        // Intentionally do nothing; used for lightweight testing via MOCK_PYTHON=true
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let cli = Cli::parse();

    if let Some(path) = &cli.job_request {
        let req_bytes = std::fs::read_to_string(path)?;
        let retrigger_id = if cli.retrigger {
            path.parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .trim_start_matches("job_")
                .to_string()
        } else {
            String::new()
        };
        let mut job = server_core::create_job_metadata(
            &cli.data_dir,
            &req_bytes,
            "localhost",
            (!retrigger_id.is_empty()).then_some(retrigger_id.as_str()),
        )?;
        // Build a storage client that prefers session tokens when available.
        // In offline mode the session is empty, so it will fallback to job meta token.
        let storage_http = reqwest::Client::builder()
            .use_rustls_tls()
            .no_proxy()
            .build()?;
        let offline_session = SessionManager::new(CapabilitySelector::new(vec![]));
        let domain: Arc<dyn server_core::DomainPort + Send + Sync> = Arc::new(
            HttpDomainClient::with_provider(storage_http, Arc::new(offline_session)),
        );
        let runner: Arc<dyn server_core::JobRunner + Send + Sync> = if cli.mock_python {
            Arc::new(NoopRunner)
        } else {
            Arc::new(PythonRunner)
        };
        let services = Services {
            domain,
            runner,
            manifest_interval: std::time::Duration::from_millis(
                cli.job_manifest_interval_ms.max(10),
            ),
        };
        let capability = job.meta.processing_type.clone();
        let _outputs = server_core::execute_job(
            &services,
            &mut job,
            capability.as_str(),
            cli.cpu_workers,
            CancellationToken::new(),
        )
        .await?;
        return Ok(());
    }

    let node_config = config::NodeConfig::from_env()?;

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(cli.log_level.clone()));
    if cli.log_format == "json" {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt::layer())
            .init();
    }

    info!(
        dms_base = %node_config.dms_base_url,
        default_capability = %node_config.default_capability,
        capabilities = ?node_config.node_capabilities,
        "Loaded compute node configuration"
    );
    // Build DMS session manager first so we can wire it as a token provider
    let session = SessionManager::new(CapabilitySelector::new(
        node_config.node_capabilities.clone(),
    ));

    // Share HTTP timeout setting across clients
    let request_timeout = Duration::from_secs(cli.request_timeout_secs.max(1));

    // Domain storage client uses session lease tokens when available
    let storage_http = reqwest::Client::builder()
        .timeout(request_timeout)
        .build()?;
    let domain: Arc<dyn server_core::DomainPort + Send + Sync> = Arc::new(
        HttpDomainClient::with_provider(storage_http, Arc::new(session.clone())),
    );
    let runner: Arc<dyn server_core::JobRunner + Send + Sync> = if cli.mock_python {
        Arc::new(NoopRunner)
    } else {
        Arc::new(PythonRunner)
    };
    let services = Arc::new(Services {
        domain,
        runner,
        manifest_interval: std::time::Duration::from_millis(cli.job_manifest_interval_ms.max(10)),
    });

    // request_timeout already computed above
    let rng = StdRng::from_entropy();
    let controller = PollController::new(
        node_config.poll_backoff.min,
        node_config.poll_backoff.max,
        rng,
    );
    let heartbeat_policy = HeartbeatPolicy::default_policy();
    let dms_http = reqwest::Client::builder()
        .timeout(request_timeout)
        .build()?;
    // Prefer SIWE-based DDS authentication if DDS_BASE_URL and SECP256K1_PRIVHEX are available.
    let identity_provider: Arc<dyn TokenProvider> = if let (Some(dds_base_url), Some(privhex)) = (
        node_config.dds.base_url.clone(),
        node_config.dds.secp256k1_privhex.clone(),
    ) {
        // If registration loop is configured, gate SIWE until registration callback completes.
        let registration_configured =
            node_config.dds.node_url.is_some() && node_config.dds.reg_secret.is_some();
        if registration_configured {
            info!("DDS auth configured; delaying SIWE until registration completes");
            Arc::new(SiweAfterRegistrationProvider::new(
                dds_base_url,
                privhex,
                node_config.token_safety_ratio,
                node_config.token_reauth_max_retries,
                node_config.token_reauth_jitter,
            )) as Arc<dyn TokenProvider>
        } else {
            info!("DDS auth configured; using SIWE token manager");
            let authenticator = Arc::new(DdsAuthenticator::new(dds_base_url, privhex));
            let manager: Arc<TokenManager<DdsAuthenticator, SystemClock>> =
                Arc::new(TokenManager::new(
                    authenticator,
                    Arc::new(SystemClock),
                    TokenManagerConfig {
                        safety_ratio: node_config.token_safety_ratio,
                        max_retries: node_config.token_reauth_max_retries,
                        jitter: node_config.token_reauth_jitter,
                    },
                ));
            manager.start_bg().await;
            manager as Arc<dyn TokenProvider>
        }
    } else {
        info!("DDS auth not fully configured; using static node identity");
        Arc::new(IdentityTokenProvider::new(
            node_config.node_identity.clone(),
        )) as Arc<dyn TokenProvider>
    };
    let dms_client = DmsClient::new(
        node_config.dms_base_url.as_str(),
        identity_provider,
        dms_http,
    )?;
    let poller = Poller::new(dms_client, session, controller, heartbeat_policy);
    let reconstruction_url = cli
        .node_url
        .clone()
        .unwrap_or_else(|| node_config.dms_base_url.to_string());
    let executor_config = ExecutorConfig {
        data_dir: cli.data_dir.clone(),
        reconstruction_url,
        cpu_workers: cli.cpu_workers,
    };
    let task_executor = TaskExecutor::new(poller, Arc::clone(&services), executor_config);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let executor_handle = tokio::spawn(run_executor_loop(
        task_executor,
        node_config.poll_backoff.min,
        shutdown_rx.clone(),
    ));
    // Build DDS router state to expose /health and registration callbacks
    let dds_state = dds_http::DdsState;
    let app = dds_http::router_dds(dds_state);

    // Start HTTP server first so DDS callbacks are accepted before attempting registration
    let addr: SocketAddr = normalize_port(&cli.port).parse()?;
    info!("Server running on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    let mut http_shutdown = shutdown_rx.clone();
    let server_future =
        serve(listener, app.into_make_service()).with_graceful_shutdown(async move {
            let _ = http_shutdown.changed().await;
        });

    // If all DDS config present, prepare registration client and spawn background loop
    if let (Some(dds_base_url), Some(node_url), Some(reg_secret), Some(privhex)) = (
        cli.dds_base_url.clone(),
        cli.node_url.clone(),
        cli.reg_secret.clone(),
        cli.secp256k1_privhex.clone(),
    ) {
        let node_version = cli.node_version.clone();
        // Build reqwest client with timeout
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()?;
        // Log derived pubkey prefix for visibility
        if let Ok(sk) = server_adapters::dds::crypto::load_secp256k1_privhex(&privhex) {
            let pk_hex = server_adapters::dds::crypto::secp256k1_pubkey_uncompressed_hex(&sk);
            let pk_short = pk_hex.get(0..16).unwrap_or(&pk_hex);
            info!(public_key_prefix = pk_short, "Derived secp256k1 public key");
        } else {
            warn!("Invalid SECP256K1_PRIVHEX; DDS registration disabled");
        }
        // Spawn registration loop (now that HTTP server is accepting connections)
        tokio::spawn(register::run_registration_loop(
            register::RegistrationConfig {
                dds_base_url,
                node_url,
                node_version,
                reg_secret,
                secp256k1_privhex: privhex,
                client,
                register_interval_secs: cli.register_interval_secs,
                max_retry: cli.register_max_retry,
                capabilities: DDS_CAPABILITIES.iter().map(|cap| cap.to_string()).collect(),
            },
        ));
    } else {
        warn!("DDS config incomplete; skipping registration loop");
    }

    // Shutdown handling
    let shutdown_task = {
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            let _ = shutdown_tx.send(true);
        })
    };

    // Run HTTP server (will exit on shutdown)
    let server_result = server_future.await;
    let _ = shutdown_tx.send(true);

    if let Err(join_err) = shutdown_task.await {
        warn!(error = %join_err, "Shutdown listener task failed");
    }

    if let Err(join_err) = executor_handle.await {
        warn!(error = %join_err, "Task executor task failed to join");
    }

    server_result?;
    Ok(())
}

fn normalize_port(port: &str) -> String {
    if let Some(rest) = port.strip_prefix(':') {
        format!("0.0.0.0:{}", rest)
    } else {
        port.to_string()
    }
}

async fn shutdown_signal() {
    // Wait for CTRL+C
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        sigterm.recv().await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

// --- DDS SIWE authenticator used by the token manager ---
#[derive(Clone)]
struct DdsAuthenticator {
    base_url: Arc<String>,
    priv_hex: Arc<String>,
    address: Arc<String>,
}

impl DdsAuthenticator {
    fn new(base_url: String, priv_hex: String) -> Self {
        let address = derive_eth_address(&priv_hex);
        Self {
            base_url: Arc::new(base_url),
            priv_hex: Arc::new(priv_hex),
            address: Arc::new(address),
        }
    }
}

#[async_trait::async_trait]
impl AccessAuthenticator for DdsAuthenticator {
    async fn login(
        &self,
    ) -> Result<server_adapters::auth::siwe::AccessBundle, server_adapters::auth::siwe::SiweError>
    {
        let meta = siwe_auth::request_nonce(self.base_url.as_str(), self.address.as_str()).await?;
        let message = siwe_auth::compose_message(&meta, self.address.as_str(), None)?;
        let signature = siwe_auth::sign_message(self.priv_hex.as_str(), &message)?;
        siwe_auth::verify(
            self.base_url.as_str(),
            self.address.as_str(),
            &message,
            &signature,
        )
        .await
    }
}

fn derive_eth_address(priv_hex: &str) -> String {
    // Compute Ethereum address from secp256k1 private key
    use k256::{ecdsa::SigningKey, FieldBytes};
    let trimmed = priv_hex.trim_start_matches("0x");
    let key_bytes = hex::decode(trimmed).expect("valid secp256k1 hex");
    let signing_key =
        SigningKey::from_bytes(FieldBytes::from_slice(&key_bytes)).expect("signing key");
    let verifying_key = signing_key.verifying_key();
    let encoded = verifying_key.to_encoded_point(false);
    let pubkey = encoded.as_bytes();
    let mut hasher = sha3::Keccak256::new();
    hasher.update(&pubkey[1..]);
    let hashed = hasher.finalize();
    let address_bytes = &hashed[12..];
    format!("0x{}", hex::encode(address_bytes))
}

// A TokenProvider that only starts SIWE after the registration callback from DDS
// has completed (persisting the node secret in memory via posemesh-node-registration).
#[derive(Clone)]
struct SiweAfterRegistrationProvider {
    dds_base_url: Arc<String>,
    priv_hex: Arc<String>,
    config: TokenManagerConfig,
    inner: ManagerCell,
}

// Clippy appeasement: factor complex types into aliases
type TokenMgr = TokenManager<DdsAuthenticator, SystemClock>;
type SharedManager = Arc<TokenMgr>;
type ManagerCell = Arc<tokio::sync::Mutex<Option<SharedManager>>>;

impl SiweAfterRegistrationProvider {
    fn new(
        dds_base_url: String,
        priv_hex: String,
        safety_ratio: f64,
        max_retries: u32,
        jitter: std::time::Duration,
    ) -> Self {
        Self {
            dds_base_url: Arc::new(dds_base_url),
            priv_hex: Arc::new(priv_hex),
            config: TokenManagerConfig {
                safety_ratio,
                max_retries,
                jitter,
            },
            inner: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    async fn ensure_started(&self) -> SharedManager {
        {
            let guard = self.inner.lock().await;
            if let Some(m) = &*guard {
                return m.clone();
            }
        }

        // Wait until registration callback persisted the node secret.
        // This uses the re-exported persist API from posemesh-node-registration.
        loop {
            match server_adapters::dds::persist::read_node_secret() {
                Ok(Some(_)) => {
                    info!("registration complete; starting SIWE token manager");
                    break;
                }
                Ok(None) => {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(e) => {
                    warn!(error = %e, "registration status read failed; retrying");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }

        let authenticator = Arc::new(DdsAuthenticator::new(
            self.dds_base_url.as_ref().clone(),
            self.priv_hex.as_ref().clone(),
        ));
        let manager: SharedManager = Arc::new(TokenManager::new(
            authenticator,
            Arc::new(SystemClock),
            self.config.clone(),
        ));
        manager.start_bg().await;

        let mut guard = self.inner.lock().await;
        *guard = Some(manager.clone());
        manager
    }
}

#[async_trait::async_trait]
impl TokenProvider for SiweAfterRegistrationProvider {
    async fn bearer(&self) -> std::result::Result<String, TokenProviderError> {
        let manager = self.ensure_started().await;
        manager.bearer().await
    }

    async fn on_unauthorized(&self) {
        let guard = self.inner.lock().await;
        if let Some(m) = &*guard {
            m.on_unauthorized().await
        }
    }
}
