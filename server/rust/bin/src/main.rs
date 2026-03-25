const RECONSTRUCTION_NODE_VERSION: &str = env!("RECONSTRUCTION_NODE_VERSION");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize telemetry (LOG_FORMAT respected if set).
    posemesh_compute_node::telemetry::init_from_env()?;

    // Load config and wire runners
    let mut cfg = posemesh_compute_node::config::NodeConfig::from_env()?;
    cfg.node_version = RECONSTRUCTION_NODE_VERSION.to_string();
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
