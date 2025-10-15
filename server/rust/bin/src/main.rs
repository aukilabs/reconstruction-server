#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize telemetry (LOG_FORMAT respected if set).
    compute_node_common::telemetry::init_from_env()?;

    let app = compute_node_common::http::router();
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    let addr = listener.local_addr()?;
    println!("http listening on {}", addr);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // Load config and wire runners
    let cfg = compute_node_common::config::NodeConfig::from_env()?;
    let legacy_runner_cfg = runner_reconstruction_legacy::RunnerConfig::from_env()?;
    let mut reg = compute_node_common::engine::RunnerRegistry::new();
    if cfg.enable_noop {
        for runner in
            runner_reconstruction_legacy_noop::RunnerReconstructionLegacyNoop::for_all_capabilities(
                cfg.noop_sleep_secs,
            )
        {
            reg = reg.register(runner);
        }
    } else {
        for runner in runner_reconstruction_legacy::RunnerReconstructionLegacy::for_all_capabilities(
            legacy_runner_cfg.clone(),
        ) {
            reg = reg.register(runner);
        }
    }

    let capabilities = reg.capabilities();
    compute_node_common::dds::register::spawn_registration_if_configured(&cfg, &capabilities)?;

    compute_node_common::engine::run_node(cfg, reg).await
}
