use crate::dds::crypto::{
    format_timestamp_nanos, load_secp256k1_privhex, registration_credentials_b64,
    secp256k1_pubkey_uncompressed_hex, sign_recoverable_keccak_hex,
};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use rand::Rng;
use reqwest::Client;
use secp256k1::SecretKey;
use serde::Serialize;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use super::state::{
    read_state, set_status, touch_healthcheck_now, LockGuard, RegistrationState, STATUS_DISCONNECTED,
    STATUS_REGISTERED, STATUS_REGISTERING,
};

// Capabilities advertised to DDS
pub const CAPABILITIES: [&str; 2] = [
    "/reconstruction/global-refinement/v1",
    "/reconstruction/local-refinement/v1",
];

#[derive(Debug, Serialize)]
pub struct NodeRegistrationRequest<'a> {
    pub url: &'a str,
    pub version: &'a str,
    pub registration_credentials: String,
    pub signature: String,
    pub timestamp: String,
    pub public_key: String,
    pub capabilities: Vec<&'a str>,
}

fn registration_endpoint(dds_base_url: &str) -> String {
    let base = dds_base_url.trim_end_matches('/');
    format!("{}/internal/v1/nodes/register", base)
}

fn build_registration_request<'a>(
    node_url: &'a str,
    node_version: &'a str,
    reg_secret: &str,
    sk: &SecretKey,
) -> NodeRegistrationRequest<'a> {
    let ts = format_timestamp_nanos(Utc::now());
    // The server verifies the signature over the exact byte concatenation of url + timestamp
    // (no delimiter). Align our signing to match that contract.
    let msg = format!("{}{}", node_url, ts);
    let signature = sign_recoverable_keccak_hex(sk, msg.as_bytes());
    let public_key = secp256k1_pubkey_uncompressed_hex(sk);
    let registration_credentials = registration_credentials_b64(reg_secret);
    NodeRegistrationRequest {
        url: node_url,
        version: node_version,
        registration_credentials,
        signature,
        timestamp: ts,
        public_key,
        capabilities: CAPABILITIES.to_vec(),
    }
}

pub async fn register_once(
    dds_base_url: &str,
    node_url: &str,
    node_version: &str,
    reg_secret: &str,
    sk: &SecretKey,
    client: &Client,
) -> Result<()> {
    let req = build_registration_request(node_url, node_version, reg_secret, sk);
    let endpoint = registration_endpoint(dds_base_url);

    // Redact sensitive details in logs
    let pk_short = req.public_key.get(0..16).unwrap_or(&req.public_key);
    info!(
        url = req.url,
        version = req.version,
        public_key_prefix = pk_short,
        capabilities = ?req.capabilities,
        "Registering node with DDS"
    );

    let res = client
        .post(&endpoint)
        .json(&req)
        .send()
        .await
        .with_context(|| format!("POST {} failed", endpoint))?;

    if res.status().is_success() {
        debug!(status = ?res.status(), "Registration ok");
        Ok(())
    } else {
        let status = res.status();
        // Include a short, sanitized snippet of the response body for diagnostics.
        let body_snippet = match res.text().await {
            Ok(mut text) => {
                if text.len() > 512 {
                    text.truncate(512);
                }
                // Newlines can be noisy in structured logs; collapse them.
                text.replace('\n', " ")
            }
            Err(_) => "<unavailable>".to_string(),
        };
        Err(anyhow!(
            "registration failed: status {}, endpoint {}, body_snippet: {}",
            status,
            endpoint,
            body_snippet
        ))
    }
}

pub async fn run_registration_loop(
    dds_base_url: String,
    node_url: String,
    node_version: String,
    reg_secret: String,
    secp256k1_privhex: String,
    client: Client,
    register_interval_secs: u64,
    max_retry: i32, // -1 means infinite retry
) {
    let sk = match load_secp256k1_privhex(&secp256k1_privhex) {
        Ok(k) => k,
        Err(e) => {
            warn!("Invalid secp256k1 private key (redacted): {}", e);
            return;
        }
    };

    // TTL for healthcheck and the base interval between successful registrations
    let healthcheck_ttl = Duration::from_secs(register_interval_secs.max(1));
    // Lock staleness threshold: if a previous attempt crashed, consider lock stale after 2x TTL (min 30s, max 10m)
    let lock_stale_after = {
        let base = healthcheck_ttl.saturating_mul(2);
        let min = Duration::from_secs(30);
        let max = Duration::from_secs(600);
        if base < min { min } else if base > max { max } else { base }
    };

    // Helper: exponential backoff like Go's timerInterval, capped at 60s
    fn timer_interval_secs(attempt: i32) -> u64 {
        if attempt <= 0 {
            return 0;
        }
        let p = 2_i64.saturating_pow(attempt as u32);
        p.clamp(0, 60) as u64
    }

    // Ensure state file exists
    let _ = set_status(read_state().map(|s| s.status).unwrap_or_default().as_str());

    let mut attempt: i32 = 0;
    let mut next_sleep = Duration::from_secs(1);

    info!(
        event = "registration.loop.start",
        healthcheck_ttl_sec = healthcheck_ttl.as_secs() as i64,
        node_url = %node_url,
        node_version = %node_version,
        "registration loop started"
    );

    loop {
        tokio::time::sleep(next_sleep).await;
        let RegistrationState { status, last_healthcheck } =
            match read_state() { Ok(s) => s, Err(_) => RegistrationState::default() };

        match status.as_str() {
            STATUS_DISCONNECTED | STATUS_REGISTERING => {
                // Try to acquire a cross-process file lock
                let lock_guard = match LockGuard::try_acquire(lock_stale_after) {
                    Ok(Some(g)) => {
                        info!(event = "lock.acquired", "registration lock acquired");
                        Some(g)
                    }
                    Ok(None) => {
                        debug!(event = "lock.busy", "another registrar is active");
                        next_sleep = healthcheck_ttl; // wait before checking again
                        continue;
                    }
                    Err(e) => {
                        warn!(event = "lock.error", error = %e, "could not acquire lock");
                        next_sleep = healthcheck_ttl;
                        continue;
                    }
                };

                // Transition to registering if we were disconnected
                if status.as_str() == STATUS_DISCONNECTED {
                    if let Err(e) = set_status(STATUS_REGISTERING) {
                        warn!(event = "status.transition.error", error = %e);
                    } else {
                        info!(event = "status.transition", from = STATUS_DISCONNECTED, to = STATUS_REGISTERING, "moved to registering");
                    }
                }

                attempt += 1;

                let start = Instant::now();
                let res = register_once(
                    &dds_base_url,
                    &node_url,
                    &node_version,
                    &reg_secret,
                    &sk,
                    &client,
                )
                .await;
                let elapsed_ms = start.elapsed().as_millis();

                match res {
                    Ok(()) => {
                        let _ = set_status(STATUS_REGISTERED);
                        let _ = touch_healthcheck_now();
                        info!(
                            event = "registration.success",
                            elapsed_ms = elapsed_ms as i64,
                            "successfully registered to DDS"
                        );
                        attempt = 0;
                        next_sleep = healthcheck_ttl;
                        drop(lock_guard);
                    }
                    Err(e) => {
                        warn!(
                            event = "registration.error",
                            elapsed_ms = elapsed_ms as i64,
                            error = %e,
                            error_debug = ?e,
                            attempt = attempt,
                            "registration to DDS failed; will back off"
                        );
                        // If we have a max_retry limit and we've reached it, pause for TTL and reset attempts
                        if max_retry >= 0 && attempt >= max_retry {
                            warn!(
                                event = "registration.max_retry_reached",
                                max_retry = max_retry,
                                "max retry reached; pausing until next TTL window"
                            );
                            attempt = 0;
                            next_sleep = healthcheck_ttl;
                            drop(lock_guard);
                            continue;
                        }
                        // Exponential backoff capped at 60s with jitter ±20%
                        let base = Duration::from_secs(timer_interval_secs(attempt));
                        let jitter_factor: f64 = rand::thread_rng().gen_range(0.8..=1.2);
                        next_sleep = Duration::from_secs_f64(base.as_secs_f64() * jitter_factor.max(0.1));
                        drop(lock_guard);
                    }
                }
            }
            STATUS_REGISTERED => {
                let elapsed = last_healthcheck
                    .map(|t| Utc::now() - t)
                    .map(|d| d.to_std().unwrap_or_default())
                    .unwrap_or_else(|| Duration::from_secs(u64::MAX / 2));

                if elapsed > healthcheck_ttl {
                    info!(
                        event = "healthcheck.expired",
                        elapsed_since_healthcheck_sec = elapsed.as_secs() as i64,
                        "healthcheck TTL exceeded; re-entering registering"
                    );
                    let _ = set_status(STATUS_REGISTERING);
                    next_sleep = Duration::from_secs(1);
                } else {
                    next_sleep = healthcheck_ttl;
                }
            }
            other => {
                warn!(event = "status.unknown", status = other, "unknown status; resetting to disconnected");
                let _ = set_status(STATUS_DISCONNECTED);
                next_sleep = Duration::from_secs(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dds::crypto::load_secp256k1_privhex;
    use parking_lot::Mutex as PLMutex;
    use std::io;
    use std::sync::Arc;
    use tracing::subscriber;
    use tracing_subscriber::layer::SubscriberExt;

    struct BufWriter(Arc<PLMutex<Vec<u8>>>);
    impl io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    struct MakeBufWriter(Arc<PLMutex<Vec<u8>>>);
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeBufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            BufWriter(self.0.clone())
        }
    }

    #[tokio::test]
    async fn logs_do_not_include_secret() {
        // Capture tracing output to a buffer
        let buf = Arc::new(PLMutex::new(Vec::<u8>::new()));
        let make = MakeBufWriter(buf.clone());
        let layer = tracing_subscriber::fmt::layer()
            .with_writer(make)
            .with_ansi(false)
            .without_time();
        let subscriber = tracing_subscriber::registry().with(layer);
        let _guard = subscriber::set_default(subscriber);

        // Prepare inputs
        let secret = "my-super-secret";
        let dds = "http://127.0.0.1:9"; // should fail fast
        let url = "https://node.example.com";
        let version = "1.2.3";
        let sk = load_secp256k1_privhex(
            "e331b6d69882b4ed5bb7f55b585d7d0f7dc3aeca4a3deee8d16bde3eca51aace",
        )
        .unwrap();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(200))
            .build()
            .unwrap();

        // Invoke once (will error due to unreachable endpoint), but it logs before send
        let _ = register_once(dds, url, version, secret, &sk, &client).await;

        let captured = String::from_utf8(buf.lock().clone()).unwrap_or_default();
        assert!(captured.contains("Registering node with DDS"));
        assert!(
            !captured.contains(secret),
            "logs leaked secret: {}",
            captured
        );
    }
}
