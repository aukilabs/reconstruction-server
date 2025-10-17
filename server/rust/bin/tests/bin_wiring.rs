use posemesh_compute_node::{config::NodeConfig, dds::persist, engine::RunnerRegistry};
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn registry_contains_noop_when_enabled_and_run_node_ok() {
    // Build a minimal NodeConfig
    persist::clear_node_secret().unwrap();

    let cfg = NodeConfig {
        dms_base_url: "https://dms.example".parse().unwrap(),
        node_version: "1.0.0".into(),
        request_timeout_secs: 10,
        dds_base_url: Some("https://dds.example".parse().unwrap()),
        node_url: Some("https://node.example".parse().unwrap()),
        reg_secret: Some("secret".into()),
        secp256k1_privhex: Some(
            "4c0883a69102937d6231471b5dbb6204fe5129617082798ce3f4fdf2548b6f90".into(),
        ),
        heartbeat_jitter_ms: 250,
        poll_backoff_ms_min: 1000,
        poll_backoff_ms_max: 30000,
        token_safety_ratio: 0.75,
        token_reauth_max_retries: 3,
        token_reauth_jitter_ms: 500,
        register_interval_secs: None,
        register_max_retry: None,
        max_concurrency: 1,
        log_format: posemesh_compute_node::config::LogFormat::Json,
        enable_noop: true,
        noop_sleep_secs: 1,
    };

    let mut reg = RunnerRegistry::new();
    for runner in runner_reconstruction_legacy::RunnerReconstructionLegacy::for_all_capabilities(
        runner_reconstruction_legacy::RunnerConfig::default(),
    ) {
        reg = reg.register(runner);
    }
    if cfg.enable_noop {
        for runner in
            runner_reconstruction_legacy_noop::RunnerReconstructionLegacyNoop::for_all_capabilities(
                cfg.noop_sleep_secs,
            )
        {
            reg = reg.register(runner);
        }
    }

    for cap in runner_reconstruction_legacy::RunnerReconstructionLegacy::SUPPORTED_CAPABILITIES {
        assert!(reg.get(cap).is_some());
    }
    for cap in runner_reconstruction_legacy_noop::CAPABILITIES {
        assert!(reg.get(cap).is_some());
    }

    // Engine now waits for shutdown; ensure it stays pending.
    let result = timeout(
        Duration::from_millis(50),
        posemesh_compute_node::engine::run_node(cfg, reg),
    )
    .await;
    assert!(result.is_err(), "run_node unexpectedly completed");
}
