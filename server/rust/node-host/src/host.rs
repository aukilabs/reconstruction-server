use crate::config::HostConfig;
use crate::router::Router;
use anyhow::{Context, Result};
use auki_sdk::{
    AukiComputeCredential, AukiDmsTasks, ComputeConfig, SecretString, TaskError, TasksConfig,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Run the compute node until SIGINT/SIGTERM.
///
/// The first signal stops claiming and lets an active task finish; a second
/// signal interrupts it (no DMS receipt is sent for interrupted work).
pub async fn run(cfg: HostConfig, router: Router) -> Result<()> {
    let graceful = CancellationToken::new();
    let forced = CancellationToken::new();
    let signals = tokio::spawn(watch_signals(graceful.clone(), forced.clone()));
    let result = run_with_shutdown(cfg, router, graceful, forced).await;
    signals.abort();
    result
}

/// Run with host-owned shutdown tokens: `graceful` stops claiming, `forced`
/// also interrupts the active task.
pub async fn run_with_shutdown(
    cfg: HostConfig,
    router: Router,
    graceful: CancellationToken,
    forced: CancellationToken,
) -> Result<()> {
    let capabilities = router.capabilities();
    let mut machine = ComputeConfig::new(
        cfg.dds_base_url.as_str(),
        cfg.dms_base_url.as_str(),
        SecretString::new(cfg.reg_secret.clone()),
        SecretString::new(cfg.secp256k1_privhex.clone()),
        &cfg.node_version,
        &cfg.client_id,
    )
    .context("invalid compute node configuration")?;
    machine.request_timeout = cfg.request_timeout;
    machine.registration_interval = cfg.register_interval;
    let credential = AukiComputeCredential::new(machine)?;
    let tasks = AukiDmsTasks::new(credential.clone(), capabilities.clone(), task_config(&cfg))?;
    info!(?capabilities, version = %cfg.node_version, "compute node starting");
    let result = claim_loop(&tasks, &cfg, &router, &graceful, &forced).await;
    let cleanup = tasks.close().await;
    credential.close().await;
    cleanup?;
    result
}

fn task_config(cfg: &HostConfig) -> TasksConfig {
    TasksConfig {
        request_timeout: cfg.request_timeout,
        poll_interval: Duration::from_millis(cfg.poll_backoff_ms_min.clamp(10, 300_000)),
        ..TasksConfig::default()
    }
}

/// Claim across all capabilities (DMS chooses), run one lease at a time.
pub async fn claim_loop(
    tasks: &AukiDmsTasks,
    cfg: &HostConfig,
    router: &Router,
    graceful: &CancellationToken,
    forced: &CancellationToken,
) -> Result<()> {
    tokio::select! { biased;
        _ = graceful.cancelled() => return Ok(()),
        _ = forced.cancelled() => return Ok(()),
        result = tasks.start(graceful) => result?,
    }
    loop {
        let lease = tokio::select! { biased;
            _ = graceful.cancelled() => break,
            _ = forced.cancelled() => break,
            result = tasks.claim_any(graceful) => result,
        };
        match lease {
            Ok(Some(lease)) => match lease.execute(router, forced).await {
                // Failed and DMS-cancelled tasks already have their outcome; keep claiming.
                Ok(()) | Err(TaskError::Handler | TaskError::LeaseLost) => continue,
                Err(TaskError::Cancelled) if !forced.is_cancelled() => continue,
                Err(TaskError::Cancelled) => break,
                Err(
                    error
                    @ (TaskError::Closed | TaskError::Authentication | TaskError::PeerCleanup),
                ) => return Err(error.into()),
                Err(error) => warn!(%error, "Task ended; backing off before polling"),
            },
            Ok(None) => {}
            Err(
                error @ (TaskError::Closed
                | TaskError::Authentication
                | TaskError::Authority(_)
                | TaskError::Configuration(_)
                | TaskError::PeerCleanup),
            ) => return Err(error.into()),
            Err(error) => warn!(%error, "Task claim failed; backing off"),
        }
        let delay = jittered_delay_ms(cfg.poll_backoff_ms_min, cfg.poll_backoff_ms_max);
        tokio::select! {
            _ = graceful.cancelled() => break,
            _ = forced.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_millis(delay)) => {}
        }
    }
    Ok(())
}

/// Uniform delay in `[min, max]` from the clock's sub-second millis.
fn jittered_delay_ms(min: u64, max: u64) -> u64 {
    let (min, max) = (min.min(max), max.max(min));
    if min == max {
        return min;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    min + (now.subsec_millis() as u64) % (max - min + 1)
}

async fn watch_signals(graceful: CancellationToken, forced: CancellationToken) {
    for token in [graceful, forced] {
        if wait_for_signal().await.is_err() {
            return;
        }
        if token.is_cancelled() {
            continue;
        }
        info!("shutdown signal received");
        token.cancel();
    }
}

#[cfg(unix)]
async fn wait_for_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = term.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_in_bounds() {
        for _ in 0..100 {
            let d = jittered_delay_ms(1_000, 30_000);
            assert!((1_000..=30_000).contains(&d));
        }
        assert_eq!(jittered_delay_ms(5, 5), 5);
        assert!((5..=10).contains(&jittered_delay_ms(10, 5)));
    }
}
