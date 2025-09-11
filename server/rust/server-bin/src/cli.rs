use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "reconstruction-server",
    version,
    about = "Rust rewrite of reconstruction server"
)]
pub struct Cli {
    #[arg(long = "api-key")]
    pub api_key: Option<String>,
    #[arg(long = "port", default_value = ":8080")]
    pub port: String,
    #[arg(long = "log-level", default_value = "info")]
    pub log_level: String,
    #[arg(long = "cpu-workers", default_value_t = 2)]
    pub cpu_workers: usize,
    #[arg(long = "job-request")]
    pub job_request: Option<std::path::PathBuf>,
    #[arg(long = "retrigger", default_value_t = false)]
    pub retrigger: bool,
    #[arg(long = "log-format", default_value = "json", hide = true)] // json|text
    pub log_format: String,
}
