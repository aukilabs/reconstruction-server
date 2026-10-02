use anyhow::{bail, Context, Result};
use std::env;
use std::time::Duration;
use url::Url;
use uuid::Uuid;

const DEFAULT_DMS_BASE_URL: &str = "https://dms.auki.network/v1";
const DEFAULT_DDS_BASE_URL: &str = "https://dds.auki.network";

/// Host settings read from the process environment.
///
/// Variable names and defaults match the former `posemesh-compute-node`
/// `NodeConfig::from_env`, so existing deployments keep working. Variables the
/// SDK runtime no longer uses (`HEARTBEAT_*`, `TOKEN_*`, `REGISTER_MAX_RETRY`,
/// `MAX_CONCURRENCY`, `ENABLE_NOOP`, `NOOP_SLEEP_SECS`) are ignored.
#[derive(Clone)]
pub struct HostConfig {
    pub dms_base_url: Url,
    pub dds_base_url: Url,
    pub reg_secret: String,
    pub secp256k1_privhex: String,
    pub node_version: String,
    pub client_id: String,
    pub request_timeout: Duration,
    pub register_interval: Duration,
    pub poll_backoff_ms_min: u64,
    pub poll_backoff_ms_max: u64,
}

impl std::fmt::Debug for HostConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostConfig")
            .field("dms_base_url", &self.dms_base_url.as_str())
            .field("dds_base_url", &self.dds_base_url.as_str())
            .field("node_version", &self.node_version)
            .field("client_id", &self.client_id)
            .field("request_timeout", &self.request_timeout)
            .field("register_interval", &self.register_interval)
            .field("poll_backoff_ms_min", &self.poll_backoff_ms_min)
            .field("poll_backoff_ms_max", &self.poll_backoff_ms_max)
            .finish_non_exhaustive()
    }
}

impl HostConfig {
    /// `node_version` is supplied by the binary (build-time version).
    /// `client_prefix` names the default `CLIENT_ID` when it is unset.
    pub fn from_env(node_version: &str, client_prefix: &str) -> Result<Self> {
        Self::from_lookup(node_version, client_prefix, |key| env::var(key).ok())
    }

    pub fn from_lookup(
        node_version: &str,
        client_prefix: &str,
        get: impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let value = |key: &str| {
            get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let url = |key: &str, default: &str| -> Result<Url> {
            let raw = value(key).unwrap_or_else(|| default.to_string());
            Url::parse(&raw).with_context(|| format!("{key} is not a valid URL"))
        };
        let number = |key: &str, default: u64| -> Result<u64> {
            match value(key) {
                Some(raw) => raw
                    .parse()
                    .with_context(|| format!("{key} must be an unsigned integer")),
                None => Ok(default),
            }
        };
        let required = |key: &str| value(key).with_context(|| format!("{key} is required"));

        let request_timeout_secs = number("REQUEST_TIMEOUT_SECS", 60)?;
        if !(1..=300).contains(&request_timeout_secs) {
            bail!("REQUEST_TIMEOUT_SECS must be between 1 and 300");
        }
        Ok(Self {
            dms_base_url: url("DMS_BASE_URL", DEFAULT_DMS_BASE_URL)?,
            dds_base_url: url("DDS_BASE_URL", DEFAULT_DDS_BASE_URL)?,
            reg_secret: required("REG_SECRET")?,
            secp256k1_privhex: required("SECP256K1_PRIVHEX")?,
            node_version: node_version.to_string(),
            client_id: value("CLIENT_ID")
                .unwrap_or_else(|| format!("{client_prefix}/{}", Uuid::new_v4())),
            request_timeout: Duration::from_secs(request_timeout_secs),
            register_interval: Duration::from_secs(number("REGISTER_INTERVAL_SECS", 120)?),
            poll_backoff_ms_min: number("POLL_BACKOFF_MS_MIN", 1_000)?,
            poll_backoff_ms_max: number("POLL_BACKOFF_MS_MAX", 30_000)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key| vars.get(key).cloned()
    }

    #[test]
    fn defaults_match_the_previous_host() {
        let cfg = HostConfig::from_lookup(
            "1.2.3",
            "node",
            lookup(&[("REG_SECRET", "secret"), ("SECP256K1_PRIVHEX", "ab")]),
        )
        .unwrap();
        assert_eq!(cfg.dms_base_url.as_str(), "https://dms.auki.network/v1");
        assert_eq!(cfg.dds_base_url.as_str(), "https://dds.auki.network/");
        assert_eq!(cfg.request_timeout, Duration::from_secs(60));
        assert_eq!(cfg.register_interval, Duration::from_secs(120));
        assert_eq!(
            (cfg.poll_backoff_ms_min, cfg.poll_backoff_ms_max),
            (1_000, 30_000)
        );
        assert!(cfg.client_id.starts_with("node/"));
        assert_eq!(cfg.node_version, "1.2.3");
    }

    #[test]
    fn credentials_are_required() {
        let err =
            HostConfig::from_lookup("1", "n", lookup(&[("SECP256K1_PRIVHEX", "ab")])).unwrap_err();
        assert!(err.to_string().contains("REG_SECRET"));
        let err = HostConfig::from_lookup("1", "n", lookup(&[("REG_SECRET", "s")])).unwrap_err();
        assert!(err.to_string().contains("SECP256K1_PRIVHEX"));
    }

    #[test]
    fn explicit_client_id_and_timeout_bounds() {
        let base = [
            ("REG_SECRET", "s"),
            ("SECP256K1_PRIVHEX", "ab"),
            ("CLIENT_ID", " splatter-a "),
        ];
        let cfg = HostConfig::from_lookup("1", "n", lookup(&base)).unwrap();
        assert_eq!(cfg.client_id, "splatter-a");
        let mut vars = base.to_vec();
        vars.push(("REQUEST_TIMEOUT_SECS", "301"));
        assert!(HostConfig::from_lookup("1", "n", lookup(&vars)).is_err());
    }
}
