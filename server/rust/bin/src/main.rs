const RECONSTRUCTION_NODE_VERSION: &str = env!("RECONSTRUCTION_NODE_VERSION");

/// All reconstruction runners, one per capability.
fn router() -> node_host::Router {
    let mut router = node_host::Router::new();
    for runner in runner_reconstruction_local::RunnerReconstructionLocal::for_all_capabilities() {
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
    node_host::run(cfg, router()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_serves_every_reconstruction_capability() {
        let router = router();
        let expected: Vec<&str> = runner_reconstruction_local::CAPABILITIES
            .iter()
            .chain(&runner_reconstruction_global::CAPABILITIES)
            .chain(&runner_reconstruction_update::CAPABILITIES)
            .copied()
            .collect();
        for cap in &expected {
            assert!(router.get(cap).is_some(), "missing runner for {cap}");
        }
        assert_eq!(router.capabilities().len(), expected.len());
    }
}
