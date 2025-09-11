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
    // Server bind address/port. Accepts values like ":8080" or "0.0.0.0:8080".
    // Also reads from env var PORT.
    #[arg(long = "port", env = "PORT", default_value = ":8080")]
    pub port: String,
    #[arg(long = "log-level", default_value = "info")]
    pub log_level: String,
    #[arg(long = "cpu-workers", default_value_t = 2)]
    pub cpu_workers: usize,
    #[arg(long = "job-request")]
    pub job_request: Option<std::path::PathBuf>,
    #[arg(long = "retrigger", default_value_t = false)]
    pub retrigger: bool,
    // json|text; reads from LOG_FORMAT if present
    #[arg(long = "log-format", env = "LOG_FORMAT", default_value = "json")] // json|text
    pub log_format: String,

    // --- DDS registration and node config ---
    // All of these can be set via env vars and overridden via CLI flags.
    #[arg(long = "dds-base-url", env = "DDS_BASE_URL")]
    pub dds_base_url: Option<String>,
    #[arg(long = "node-url", env = "NODE_URL")]
    pub node_url: Option<String>,
    #[arg(long = "node-version", env = "NODE_VERSION")]
    pub node_version: Option<String>,
    #[arg(long = "reg-secret", env = "REG_SECRET")]
    pub reg_secret: Option<String>,
    #[arg(long = "secp256k1-privhex", env = "SECP256K1_PRIVHEX")]
    pub secp256k1_privhex: Option<String>,

    // Optional tuning knobs
    #[arg(
        long = "register-interval-secs",
        env = "REGISTER_INTERVAL_SECS",
        default_value_t = 120
    )]
    pub register_interval_secs: u64,
    #[arg(
        long = "register-max-retry",
        env = "REGISTER_MAX_RETRY",
        default_value_t = -1
    )]
    pub register_max_retry: i32,
    #[arg(
        long = "request-timeout-secs",
        env = "REQUEST_TIMEOUT_SECS",
        default_value_t = 10
    )]
    pub request_timeout_secs: u64,

    // --- Testing / development helpers ---
    // If true, do not invoke the heavy Python pipeline; use a noop runner instead.
    // Can be toggled via env var MOCK_PYTHON=true.
    #[arg(long = "mock-python", env = "MOCK_PYTHON", default_value_t = false)]
    pub mock_python: bool,
}
