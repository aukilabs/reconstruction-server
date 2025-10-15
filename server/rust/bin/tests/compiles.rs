#[test]
fn workspace_crates_link() {
    assert_eq!(compute_runner_api::CRATE_NAME, "compute-runner-api");
    assert_eq!(compute_node_common::CRATE_NAME, "compute-node-common");
    assert_eq!(
        runner_reconstruction_legacy_noop::CRATE_NAME,
        "runner-reconstruction-legacy-noop"
    );
    assert_eq!(
        runner_reconstruction_legacy::CRATE_NAME,
        "runner-reconstruction-legacy"
    );
}
