use std::sync::Once;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

static INIT: Once = Once::new();

/// Install the global subscriber: `LOG_FORMAT=text` or JSON, filtered by `RUST_LOG` (default `info`).
/// Safe to call multiple times; only the first call installs a subscriber.
pub fn init_from_env() -> anyhow::Result<()> {
    let text = std::env::var("LOG_FORMAT").ok().as_deref() == Some("text");
    INIT.call_once(|| {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let fmt_layer = if text {
            fmt::layer().boxed()
        } else {
            fmt::layer().json().boxed()
        };
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
    });
    Ok(())
}

/// Span carrying the common task identifiers.
pub fn task_span(
    task_id: uuid::Uuid,
    job_id: uuid::Uuid,
    capability: &str,
    domain_id: uuid::Uuid,
) -> tracing::Span {
    tracing::info_span!(
        "task",
        task_id = %task_id,
        job_id = %job_id,
        capability = %capability,
        domain_id = %domain_id
    )
}
