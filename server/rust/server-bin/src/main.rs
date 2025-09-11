use axum::serve;
use clap::Parser;
use parking_lot::Mutex;
use server_adapters::{http, storage::HttpDomainClient};
use server_core::{JobList, Services};
use std::{net::SocketAddr, sync::Arc};
use tracing::info;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

mod cli;
use crate::cli::Cli;

struct PythonRunner;
#[async_trait::async_trait]
impl server_core::JobRunner for PythonRunner {
    async fn run_python(
        &self,
        job: &server_core::Job,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let cli = Cli::parse();

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
            &std::path::PathBuf::from("jobs"),
            &req_bytes,
            "localhost",
            (!retrigger_id.is_empty()).then_some(retrigger_id.as_str()),
        )?;
        let domain = Box::leak(Box::new(HttpDomainClient::default()));
        let runner = Box::leak(Box::new(PythonRunner));
        let services = Services { domain, runner };
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                server_core::execute_job(&services, &mut job, cli.cpu_workers).await
            })?;
        return Ok(());
    }

    if cli.api_key.as_deref().unwrap_or("").is_empty() {
        eprintln!("API key is required");
        std::process::exit(1);
    }

    let domain = Box::leak(Box::new(HttpDomainClient::default()));
    let runner = Box::leak(Box::new(PythonRunner));
    let services = Services { domain, runner };
    let state = http::AppState {
        api_key: cli.api_key.clone(),
        jobs: Arc::new(Mutex::new(JobList::default())),
        job_in_progress: Arc::new(Mutex::new(false)),
        services: Arc::new(services),
        cpu_workers: cli.cpu_workers,
    };
    let app = server_adapters::http::router(state);

    let addr: SocketAddr = normalize_port(&cli.port).parse()?;
    info!("Server running on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve(listener, app.into_make_service()).await?;
    Ok(())
}

fn normalize_port(port: &str) -> String {
    if let Some(rest) = port.strip_prefix(':') {
        format!("0.0.0.0:{}", rest)
    } else {
        port.to_string()
    }
}
