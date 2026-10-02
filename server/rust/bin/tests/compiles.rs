#[test]
fn workspace_crates_link() {
    assert_eq!(
        runner_reconstruction_local::CRATE_NAME,
        "runner-reconstruction-local"
    );
    assert_eq!(
        runner_reconstruction_global::CRATE_NAME,
        "runner-reconstruction-global"
    );
    assert_eq!(
        runner_reconstruction_update::CRATE_NAME,
        "runner-reconstruction-update"
    );
}

#[test]
fn host_config_requires_node_credentials() {
    let err =
        node_host::HostConfig::from_lookup("1.0.0", "reconstruction-node", |_| None).unwrap_err();
    assert!(err.to_string().contains("REG_SECRET"));
}
