use std::path::PathBuf;

use runner_reconstruction_legacy::python;
use tempfile::NamedTempFile;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

fn python_bin() -> PathBuf {
    std::env::var("PYTHON_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("python3"))
}

fn script_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../scripts/mock_py_runner.py")
}

#[tokio::test]
async fn run_script_logs_stdout_and_stderr() {
    let log_file = NamedTempFile::new().unwrap();
    let token = CancellationToken::new();

    python::run_script(
        &python_bin(),
        &script_path(),
        &["print".into(), "hello".into(), "world".into()],
        log_file.path(),
        &token,
    )
    .await
    .expect("python script should succeed");

    let log_contents = std::fs::read_to_string(log_file.path()).unwrap();
    assert!(log_contents.contains("OUT: hello"));
    assert!(log_contents.contains("ERR: world"));
}

#[tokio::test]
async fn run_script_reports_failure_on_exit_code() {
    let log_file = NamedTempFile::new().unwrap();
    let token = CancellationToken::new();

    let err = python::run_script(
        &python_bin(),
        &script_path(),
        &["exit".into(), "5".into()],
        log_file.path(),
        &token,
    )
    .await
    .expect_err("expected non-zero exit to bubble up");
    assert!(err.to_string().contains("failed"));
}

#[tokio::test]
async fn run_script_can_be_cancelled() {
    let log_file = NamedTempFile::new().unwrap();
    let token = CancellationToken::new();
    let cancel_token = token.clone();
    let log_path = log_file.path().to_path_buf();
    let args = vec!["sleep".into(), "5".into()];
    let py_bin = python_bin();
    let script = script_path();

    let task = tokio::spawn(async move {
        python::run_script(&py_bin, &script, &args, log_path.as_path(), &cancel_token).await
    });

    sleep(Duration::from_millis(100)).await;
    token.cancel();

    let err = task.await.unwrap().expect_err("cancellation should error");
    assert!(err.to_string().contains("canceled"));
}
