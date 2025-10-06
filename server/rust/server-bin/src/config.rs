use std::{env, time::Duration};

use reqwest::Url;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub struct NodeConfig {
    pub dms_base_url: Url,
    pub node_identity: String,
    pub node_capabilities: Vec<String>,
    pub default_capability: String,
    pub heartbeat_jitter: Duration,
    pub poll_backoff: PollBackoff,
    pub token_safety_ratio: f64,
    pub token_reauth_max_retries: u32,
    pub token_reauth_jitter: Duration,
    pub dds: DdsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollBackoff {
    pub min: Duration,
    pub max: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DdsConfig {
    pub base_url: Option<String>,
    pub node_url: Option<String>,
    pub reg_secret: Option<String>,
    pub secp256k1_privhex: Option<String>,
    pub register_interval_secs: Option<u64>,
    pub register_max_retry: Option<i32>,
    pub request_timeout_secs: Option<u64>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("missing environment variable {0}")]
    Missing(&'static str),
    #[error("invalid URL in {field}: {details}")]
    InvalidUrl {
        field: &'static str,
        details: String,
    },
    #[error("invalid node identity (expected 'siwe:<uuid>'): {0}")]
    InvalidNodeIdentity(String),
    #[error("NODE_CAPABILITIES must contain at least one capability")]
    MissingCapabilities,
    #[error("invalid number in {field}: {source}")]
    InvalidNumber {
        field: &'static str,
        source: std::num::ParseIntError,
    },
    #[error("invalid decimal in {field}: {details}")]
    InvalidDecimal {
        field: &'static str,
        details: String,
    },
    #[error("TOKEN_SAFETY_RATIO must be between 0.0 and 1.0, got {value}")]
    InvalidTokenSafetyRatio { value: String },
    #[error("POLL_BACKOFF_MS_MIN ({min}) must be <= POLL_BACKOFF_MS_MAX ({max})")]
    InvalidBackoff { min: u64, max: u64 },
}

impl NodeConfig {
    const HEARTBEAT_JITTER_DEFAULT_MS: u64 = 250;
    const POLL_BACKOFF_MIN_DEFAULT_MS: u64 = 1_000;
    const POLL_BACKOFF_MAX_DEFAULT_MS: u64 = 30_000;
    const TOKEN_SAFETY_RATIO_DEFAULT: f64 = 0.75;
    const TOKEN_REAUTH_MAX_RETRIES_DEFAULT: u32 = 3;
    const TOKEN_REAUTH_JITTER_DEFAULT_MS: u64 = 500;

    pub fn from_env() -> Result<Self, ConfigError> {
        let dms_base_raw = get_required_env("DMS_BASE_URL")?;
        let dms_base_url = Url::parse(&dms_base_raw).map_err(|source| ConfigError::InvalidUrl {
            field: "DMS_BASE_URL",
            details: source.to_string(),
        })?;

        let node_identity = get_required_env("NODE_IDENTITY")?;
        validate_node_identity(&node_identity)?;

        let capabilities_raw = get_required_env("NODE_CAPABILITIES")?;
        let node_capabilities = split_capabilities(&capabilities_raw)?;

        let default_capability = node_capabilities
            .first()
            .cloned()
            .ok_or(ConfigError::MissingCapabilities)?;

        let heartbeat_jitter_ms =
            get_optional_u64("HEARTBEAT_JITTER_MS", Self::HEARTBEAT_JITTER_DEFAULT_MS)?;
        let poll_backoff_min_ms =
            get_optional_u64("POLL_BACKOFF_MS_MIN", Self::POLL_BACKOFF_MIN_DEFAULT_MS)?;
        let poll_backoff_max_ms =
            get_optional_u64("POLL_BACKOFF_MS_MAX", Self::POLL_BACKOFF_MAX_DEFAULT_MS)?;
        let token_safety_ratio =
            get_optional_ratio("TOKEN_SAFETY_RATIO", Self::TOKEN_SAFETY_RATIO_DEFAULT)?;
        let token_reauth_max_retries = get_optional_u32(
            "TOKEN_REAUTH_MAX_RETRIES",
            Self::TOKEN_REAUTH_MAX_RETRIES_DEFAULT,
        )?;
        let token_reauth_jitter_ms = get_optional_u64(
            "TOKEN_REAUTH_JITTER_MS",
            Self::TOKEN_REAUTH_JITTER_DEFAULT_MS,
        )?;

        if poll_backoff_min_ms > poll_backoff_max_ms {
            return Err(ConfigError::InvalidBackoff {
                min: poll_backoff_min_ms,
                max: poll_backoff_max_ms,
            });
        }

        let dds = DdsConfig {
            base_url: env::var("DDS_BASE_URL").ok(),
            node_url: env::var("NODE_URL").ok(),
            reg_secret: env::var("REG_SECRET").ok(),
            secp256k1_privhex: env::var("SECP256K1_PRIVHEX").ok(),
            register_interval_secs: get_optional_u64_opt("REGISTER_INTERVAL_SECS")?,
            register_max_retry: get_optional_i32_opt("REGISTER_MAX_RETRY")?,
            request_timeout_secs: get_optional_u64_opt("REQUEST_TIMEOUT_SECS")?,
        };

        Ok(Self {
            dms_base_url,
            node_identity,
            node_capabilities,
            default_capability,
            heartbeat_jitter: Duration::from_millis(heartbeat_jitter_ms),
            poll_backoff: PollBackoff {
                min: Duration::from_millis(poll_backoff_min_ms),
                max: Duration::from_millis(poll_backoff_max_ms),
            },
            token_safety_ratio,
            token_reauth_max_retries,
            token_reauth_jitter: Duration::from_millis(token_reauth_jitter_ms),
            dds,
        })
    }
}

fn get_required_env(name: &'static str) -> Result<String, ConfigError> {
    env::var(name).map_err(|_| ConfigError::Missing(name))
}

fn get_optional_u64(name: &'static str, default: u64) -> Result<u64, ConfigError> {
    match env::var(name) {
        Ok(value) => value.parse().map_err(|source| ConfigError::InvalidNumber {
            field: name,
            source,
        }),
        Err(_) => Ok(default),
    }
}

fn get_optional_u32(name: &'static str, default: u32) -> Result<u32, ConfigError> {
    match env::var(name) {
        Ok(value) => value.parse().map_err(|source| ConfigError::InvalidNumber {
            field: name,
            source,
        }),
        Err(_) => Ok(default),
    }
}

fn get_optional_u64_opt(name: &'static str) -> Result<Option<u64>, ConfigError> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => {
            value
                .parse()
                .map(Some)
                .map_err(|source| ConfigError::InvalidNumber {
                    field: name,
                    source,
                })
        }
        Ok(_) => Ok(None),
        Err(_) => Ok(None),
    }
}

fn get_optional_ratio(name: &'static str, default: f64) -> Result<f64, ConfigError> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value
                .parse::<f64>()
                .map_err(|source| ConfigError::InvalidDecimal {
                    field: name,
                    details: source.to_string(),
                })?;

            if !(0.0..=1.0).contains(&parsed) {
                Err(ConfigError::InvalidTokenSafetyRatio { value })
            } else {
                Ok(parsed)
            }
        }
        Err(_) => Ok(default),
    }
}

fn get_optional_i32_opt(name: &'static str) -> Result<Option<i32>, ConfigError> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => {
            value
                .parse()
                .map(Some)
                .map_err(|source| ConfigError::InvalidNumber {
                    field: name,
                    source,
                })
        }
        Ok(_) => Ok(None),
        Err(_) => Ok(None),
    }
}

fn validate_node_identity(identity: &str) -> Result<(), ConfigError> {
    const PREFIX: &str = "siwe:";
    if let Some(uuid_part) = identity.strip_prefix(PREFIX) {
        Uuid::parse_str(uuid_part)
            .map(|_| ())
            .map_err(|_| ConfigError::InvalidNodeIdentity(identity.to_string()))
    } else {
        Err(ConfigError::InvalidNodeIdentity(identity.to_string()))
    }
}

fn split_capabilities(raw: &str) -> Result<Vec<String>, ConfigError> {
    let capabilities: Vec<String> = raw
        .split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_string())
        .collect();

    if capabilities.is_empty() {
        Err(ConfigError::MissingCapabilities)
    } else {
        Ok(capabilities)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[derive(Default)]
    struct TestEnv {
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl TestEnv {
        fn set(mut self, key: &'static str, value: &str) -> Self {
            if !self.saved.iter().any(|(k, _)| *k == key) {
                self.saved.push((key, env::var(key).ok()));
            }
            env::set_var(key, value);
            self
        }

        fn unset(mut self, key: &'static str) -> Self {
            if !self.saved.iter().any(|(k, _)| *k == key) {
                self.saved.push((key, env::var(key).ok()));
            }
            env::remove_var(key);
            self
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..) {
                if let Some(original) = value {
                    env::set_var(key, original);
                } else {
                    env::remove_var(key);
                }
            }
        }
    }

    fn base_env() -> TestEnv {
        TestEnv::default()
            .set("DMS_BASE_URL", "https://dms.example.com")
            .set("NODE_IDENTITY", "siwe:00000000-0000-0000-0000-000000000001")
            .set(
                "NODE_CAPABILITIES",
                "local-and-global-refinement, other-capability",
            )
            .unset("HEARTBEAT_JITTER_MS")
            .unset("POLL_BACKOFF_MS_MIN")
            .unset("POLL_BACKOFF_MS_MAX")
            .unset("TOKEN_SAFETY_RATIO")
            .unset("TOKEN_REAUTH_MAX_RETRIES")
            .unset("TOKEN_REAUTH_JITTER_MS")
            .unset("DDS_BASE_URL")
            .unset("NODE_URL")
            .unset("REG_SECRET")
            .unset("SECP256K1_PRIVHEX")
            .unset("REGISTER_INTERVAL_SECS")
            .unset("REGISTER_MAX_RETRY")
            .unset("REQUEST_TIMEOUT_SECS")
    }

    #[test]
    fn loads_config_with_defaults() {
        let _guard = env_lock();
        let _env = base_env();

        let cfg = NodeConfig::from_env().expect("config should load");

        assert_eq!(cfg.dms_base_url.as_str(), "https://dms.example.com/");
        assert_eq!(
            cfg.node_identity,
            "siwe:00000000-0000-0000-0000-000000000001"
        );
        assert_eq!(cfg.node_capabilities.len(), 2);
        assert_eq!(cfg.default_capability, "local-and-global-refinement");
        assert_eq!(cfg.heartbeat_jitter, Duration::from_millis(250));
        assert_eq!(cfg.poll_backoff.min, Duration::from_millis(1_000));
        assert_eq!(cfg.poll_backoff.max, Duration::from_millis(30_000));
        assert!((cfg.token_safety_ratio - 0.75).abs() < f64::EPSILON);
        assert_eq!(cfg.token_reauth_max_retries, 3);
        assert_eq!(cfg.token_reauth_jitter, Duration::from_millis(500));
        assert_eq!(cfg.dds, DdsConfig::default());
    }

    #[test]
    fn errors_when_dms_base_missing() {
        let _guard = env_lock();
        let _env = base_env().unset("DMS_BASE_URL");

        let err = NodeConfig::from_env().unwrap_err();
        assert_eq!(err, ConfigError::Missing("DMS_BASE_URL"));
    }

    #[test]
    fn errors_on_invalid_node_identity() {
        let _guard = env_lock();
        let _env = base_env().set("NODE_IDENTITY", "bad-identity");

        let err = NodeConfig::from_env().unwrap_err();
        assert_eq!(err, ConfigError::InvalidNodeIdentity("bad-identity".into()));
    }

    #[test]
    fn parses_optional_overrides_and_dds_env() {
        let _guard = env_lock();
        let _env = base_env()
            .set("HEARTBEAT_JITTER_MS", "400")
            .set("POLL_BACKOFF_MS_MIN", "500")
            .set("POLL_BACKOFF_MS_MAX", "2000")
            .set("TOKEN_SAFETY_RATIO", "0.6")
            .set("TOKEN_REAUTH_MAX_RETRIES", "5")
            .set("TOKEN_REAUTH_JITTER_MS", "1500")
            .set("DDS_BASE_URL", "https://dds.example.com")
            .set("NODE_URL", "https://node.example.com")
            .set("REG_SECRET", "supersecret")
            .set("SECP256K1_PRIVHEX", "deadbeef")
            .set("REGISTER_INTERVAL_SECS", "30")
            .set("REGISTER_MAX_RETRY", "5")
            .set("REQUEST_TIMEOUT_SECS", "20");

        let cfg = NodeConfig::from_env().expect("config should load");

        assert_eq!(cfg.heartbeat_jitter, Duration::from_millis(400));
        assert_eq!(cfg.poll_backoff.min, Duration::from_millis(500));
        assert_eq!(cfg.poll_backoff.max, Duration::from_millis(2_000));
        assert!((cfg.token_safety_ratio - 0.6).abs() < f64::EPSILON);
        assert_eq!(cfg.token_reauth_max_retries, 5);
        assert_eq!(cfg.token_reauth_jitter, Duration::from_millis(1_500));
        assert_eq!(cfg.dds.base_url.as_deref(), Some("https://dds.example.com"));
        assert_eq!(
            cfg.dds.node_url.as_deref(),
            Some("https://node.example.com")
        );
        assert_eq!(cfg.dds.reg_secret.as_deref(), Some("supersecret"));
        assert_eq!(cfg.dds.secp256k1_privhex.as_deref(), Some("deadbeef"));
        assert_eq!(cfg.dds.register_interval_secs, Some(30));
        assert_eq!(cfg.dds.register_max_retry, Some(5));
        assert_eq!(cfg.dds.request_timeout_secs, Some(20));
    }

    #[test]
    fn errors_when_backoff_min_exceeds_max() {
        let _guard = env_lock();
        let _env = base_env()
            .set("POLL_BACKOFF_MS_MIN", "4000")
            .set("POLL_BACKOFF_MS_MAX", "1000");

        let err = NodeConfig::from_env().unwrap_err();
        assert_eq!(
            err,
            ConfigError::InvalidBackoff {
                min: 4_000,
                max: 1_000
            }
        );
    }

    #[test]
    fn errors_when_token_safety_ratio_not_a_number() {
        let _guard = env_lock();
        let _env = base_env().set("TOKEN_SAFETY_RATIO", "not-a-number");

        let err = NodeConfig::from_env().unwrap_err();
        assert_eq!(
            err,
            ConfigError::InvalidDecimal {
                field: "TOKEN_SAFETY_RATIO",
                details: "invalid float literal".into()
            }
        );
    }

    #[test]
    fn errors_when_token_safety_ratio_out_of_range() {
        let _guard = env_lock();
        let _env = base_env().set("TOKEN_SAFETY_RATIO", "1.5");

        let err = NodeConfig::from_env().unwrap_err();
        assert_eq!(
            err,
            ConfigError::InvalidTokenSafetyRatio {
                value: "1.5".into()
            }
        );
    }

    #[test]
    fn errors_when_token_reauth_max_retries_invalid_number() {
        let _guard = env_lock();
        let _env = base_env().set("TOKEN_REAUTH_MAX_RETRIES", "nope");

        let err = NodeConfig::from_env().unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidNumber {
                field: "TOKEN_REAUTH_MAX_RETRIES",
                ..
            }
        ));
    }
}
