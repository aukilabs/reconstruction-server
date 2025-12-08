#[test]
fn workspace_crates_link() {
    assert_eq!(
        compute_runner_api::CRATE_NAME,
        "posemesh-compute-node-runner-api"
    );
    assert_eq!(posemesh_compute_node::CRATE_NAME, "posemesh-compute-node");
    assert_eq!(
        runner_reconstruction_legacy_noop::CRATE_NAME,
        "runner-reconstruction-legacy-noop"
    );
    assert_eq!(
        runner_reconstruction_legacy::CRATE_NAME,
        "runner-reconstruction-legacy"
    );
}
