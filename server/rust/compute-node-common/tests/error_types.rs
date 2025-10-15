use compute_node_common::errors::*;
use runner_reconstruction_legacy_noop::CAPABILITY as NOOP_CAPABILITY;

#[test]
fn errors_are_constructible_and_display() {
    let e: DmsClientError = DmsClientError::Http("400 bad".into());
    let _t: &dyn std::error::Error = &e;
    assert!(format!("{}", e).contains("http error"));

    let e: ExecutorError = ExecutorError::NoRunner(NOOP_CAPABILITY.into());
    let _t: &dyn std::error::Error = &e;
    assert!(format!("{}", e).contains("No runner") || format!("{}", e).contains("no runner"));

    let e: TokenManagerError = TokenManagerError::Rotation("oops".into());
    let _t: &dyn std::error::Error = &e;
    assert!(format!("{}", e).contains("rotation"));

    let e: StorageError = StorageError::Unauthorized;
    let _t: &dyn std::error::Error = &e;
    assert!(format!("{}", e).contains("unauthorized"));
}
