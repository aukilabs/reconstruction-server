#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize telemetry (LOG_FORMAT respected if set).
    posemesh_compute_node::telemetry::init_from_env()?;

    let app = posemesh_compute_node::http::router();
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    let addr = listener.local_addr()?;
    println!("http listening on {}", addr);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // Load config and wire runners
    let cfg = posemesh_compute_node::config::NodeConfig::from_env()?;
    let mut reg = posemesh_compute_node::engine::RunnerRegistry::new();
    for runner in runner_reconstruction_local::RunnerReconstructionLocal::for_all_capabilities() {
        reg = reg.register(runner);
    }
    for runner in runner_reconstruction_global::RunnerReconstructionGlobal::for_all_capabilities() {
        reg = reg.register(runner);
    }

    let capabilities = reg.capabilities();
    posemesh_compute_node::dds::register::spawn_registration_if_configured(&cfg, &capabilities)?;

    posemesh_compute_node::engine::run_node(cfg, reg).await
}
