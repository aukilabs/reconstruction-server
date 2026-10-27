const RECONSTRUCTION_NODE_VERSION: &str = env!("RECONSTRUCTION_NODE_VERSION");

/// All reconstruction runners, one per capability. `p2p`: the node has a P2P peer, so it can
/// also serve the capabilities that fetch inputs from robots.
fn router(p2p: bool) -> node_host::Router {
    let mut router = node_host::Router::new();
    if p2p {
        for runner in
            runner_reconstruction_local_auki_session::RunnerReconstructionLocalAukiSession::for_p2p_capabilities()
        {
            router = router.register(runner);
        }
    }
    for runner in runner_reconstruction_local::RunnerReconstructionLocal::for_all_capabilities() {
        router = router.register(runner);
    }
    for runner in
        runner_reconstruction_local_auki_session::RunnerReconstructionLocalAukiSession::for_all_capabilities()
    {
        router = router.register(runner);
    }
    for runner in runner_reconstruction_global::RunnerReconstructionGlobal::for_all_capabilities() {
        router = router.register(runner);
    }
    for runner in runner_reconstruction_update::RunnerReconstructionUpdate::for_all_capabilities() {
        router = router.register(runner);
    }
    router
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize telemetry (LOG_FORMAT respected if set).
    node_host::telemetry::init_from_env()?;

    let cfg = node_host::HostConfig::from_env(RECONSTRUCTION_NODE_VERSION, "reconstruction-node")?;
    let p2p = cfg.peer_identity_file.is_some();
    node_host::run(cfg, router(p2p)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_serves_every_reconstruction_capability() {
        let router = router(false);
        let expected: Vec<&str> = runner_reconstruction_local::CAPABILITIES
            .iter()
            .chain(&runner_reconstruction_local_auki_session::CAPABILITIES)
            .chain(&runner_reconstruction_global::CAPABILITIES)
            .chain(&runner_reconstruction_update::CAPABILITIES)
            .copied()
            .collect();
        for cap in &expected {
            assert!(router.get(cap).is_some(), "missing runner for {cap}");
        }
        assert_eq!(router.capabilities().len(), expected.len());
        for cap in runner_reconstruction_local_auki_session::P2P_CAPABILITIES {
            assert!(router.get(cap).is_none(), "{cap} needs a P2P peer");
        }
    }

    #[test]
    fn a_node_with_a_peer_also_serves_p2p_capabilities() {
        let without = router(false).capabilities().len();
        let router = router(true);
        for cap in runner_reconstruction_local_auki_session::P2P_CAPABILITIES {
            assert!(router.get(cap).is_some(), "missing runner for {cap}");
        }
        assert_eq!(
            router.capabilities().len(),
            without + runner_reconstruction_local_auki_session::P2P_CAPABILITIES.len()
        );
    }
}
