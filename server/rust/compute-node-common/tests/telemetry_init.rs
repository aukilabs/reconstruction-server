use compute_node_common::config::LogFormat;

#[test]
fn telemetry_initializes() {
    compute_node_common::telemetry::init_with_format(LogFormat::Text).unwrap();
    tracing::info!(message = "telemetry initialized");
}

#[cfg(feature = "metrics")]
#[test]
fn metrics_feature_compiles() {
    // Ensure the metrics module exists and constants are accessible.
    let _ = compute_node_common::telemetry::metrics::DMS_POLL_LATENCY_MS;
    compute_node_common::telemetry::metrics::incr(
        compute_node_common::telemetry::metrics::TOKEN_ROTATE_COUNT,
        1,
    );
}
