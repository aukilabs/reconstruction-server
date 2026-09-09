const RECONSTRUCTION_NODE_VERSION: &str = env!("RECONSTRUCTION_NODE_VERSION");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize telemetry (LOG_FORMAT respected if set).
    posemesh_compute_node::telemetry::init_from_env()?;

    // Load config and wire runners
    let mut cfg = posemesh_compute_node::config::NodeConfig::from_env()?;
    cfg.node_version = RECONSTRUCTION_NODE_VERSION.to_string();
    // The Auki-session capability fetches its input over Blob v1, so it needs
    // the authenticated peer surface. `with_protocols` hands every runner the
    // same handle; the engine activates it per task, from the peer it starts
    // for that lease.
    //
    // NOTE: this makes AUKI_P2P_ENABLED mandatory for this binary -- composition
    // fails outright without a peer identity. That was equally true of the
    // dataset protocol this replaces.
    let composition = posemesh_compute_node::engine::RunnerComposition::with_protocols(
        |protocols| {
            let mut reg = posemesh_compute_node::engine::RunnerRegistry::new();
            for runner in
                runner_reconstruction_local::RunnerReconstructionLocal::for_all_capabilities()
            {
                reg = reg.register(runner);
            }
            for runner in
                runner_reconstruction_local_auki_sdk::RunnerReconstructionLocalAukiSdk::for_all_capabilities(
                    protocols,
                )
            {
                reg = reg.register(runner);
            }
            for runner in
                runner_reconstruction_global::RunnerReconstructionGlobal::for_all_capabilities()
            {
                reg = reg.register(runner);
            }
            for runner in
                runner_reconstruction_update::RunnerReconstructionUpdate::for_all_capabilities()
            {
                reg = reg.register(runner);
            }
            reg
        },
    );

    // DDS registration needs the capability list up front, and the composition
    // closure has not run yet -- so ask the runners directly rather than the
    // registry they will eventually build.
    let capabilities = reconstruction_capabilities();
    posemesh_compute_node::dds::register::spawn_registration_if_configured(&cfg, &capabilities)?;

    posemesh_compute_node::engine::run_node(cfg, composition).await
}

/// Every capability this binary serves, in the order the registry would report.
///
/// Duplicated from the composition above because the capability list is needed
/// before the composition closure runs.
fn reconstruction_capabilities() -> Vec<String> {
    let mut capabilities: Vec<String> = runner_reconstruction_local::CAPABILITIES
        .iter()
        .chain(runner_reconstruction_local_auki_sdk::CAPABILITIES.iter())
        .chain(runner_reconstruction_global::CAPABILITIES.iter())
        .chain(runner_reconstruction_update::CAPABILITIES.iter())
        .map(|capability| capability.to_string())
        .collect();
    capabilities.sort();
    capabilities
}
