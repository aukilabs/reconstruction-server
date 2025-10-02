use axum::serve;
use clap::Parser;
use rand::{rngs::StdRng, SeedableRng};
use server_adapters::{
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
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::watch;
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
        capability: &str,
        cpu_workers: usize,
    ) -> server_core::Result<()> {
        use std::io::{BufRead, BufReader, Write};
        use std::process::{Command, Stdio};
        use std::thread;
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
            "--output".into(),
            output_path.to_string_lossy().into_owned(),
            "--domain_id".into(),
            job.meta.domain_id.clone(),
            "--job_id".into(),
            job.meta.name.clone(),
            "--local_refinement_workers".into(),
            cpu_workers.to_string(),
            "--capability".into(),
            capability.to_string(),
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

        let status = child.wait().map_err(server_core::DomainError::Io)?;
        if !status.success() {
            return Err(server_core::DomainError::Internal("python failed".into()));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl server_core::JobRunner for NoopRunner {
    async fn run_python(
        &self,
        _job: &server_core::Job,
        _capability: &str,
        _cpu_workers: usize,
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
        let domain: Arc<dyn server_core::DomainPort + Send + Sync> =
            Arc::new(HttpDomainClient::default());
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
        let _outputs =
            server_core::execute_job(&services, &mut job, capability.as_str(), cli.cpu_workers)
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
    let domain: Arc<dyn server_core::DomainPort + Send + Sync> =
        Arc::new(HttpDomainClient::default());
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

    let request_timeout = Duration::from_secs(cli.request_timeout_secs.max(1));
    let session = SessionManager::new(CapabilitySelector::new(
        node_config.node_capabilities.clone(),
    ));
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
    let dms_client = DmsClient::new(
        node_config.dms_base_url.as_str(),
        node_config.node_identity.clone(),
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
        // Spawn registration loop
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

    let addr: SocketAddr = normalize_port(&cli.port).parse()?;
    info!("Server running on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    let mut http_shutdown = shutdown_rx.clone();
    let server_future =
        serve(listener, app.into_make_service()).with_graceful_shutdown(async move {
            let _ = http_shutdown.changed().await;
        });

    let shutdown_task = {
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            let _ = shutdown_tx.send(true);
        })
    };

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
