use std::{
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
};

use runner_reconstruction_legacy::python;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::MakeWriter;

fn python_bin() -> PathBuf {
    std::env::var("PYTHON_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("python3"))
}

fn script_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../scripts/mock_py_runner.py")
}

#[derive(Clone)]
struct BufferMakeWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for BufferMakeWriter {
    type Writer = BufferGuardWriter;

    fn make_writer(&'a self) -> Self::Writer {
        BufferGuardWriter(self.0.clone())
    }
}

struct BufferGuardWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for BufferGuardWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.0.lock().unwrap();
        guard.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

static LOG_BUFFER: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();

fn init_tracing_capture() -> Arc<Mutex<Vec<u8>>> {
    let buf = LOG_BUFFER.get_or_init(|| {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer = BufferMakeWriter(buf.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .json()
            .with_writer(writer)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("set global subscriber for tests");
        buf
    });
    let buf = buf.clone();
    buf.lock().unwrap().clear();
    buf
}

#[tokio::test]
async fn run_script_logs_stdout_and_stderr() {
    let buffer = init_tracing_capture();
    let token = CancellationToken::new();

    python::run_script(
        &python_bin(),
        &script_path(),
        &["print".into(), "hello".into(), "world".into()],
        &token,
    )
    .await
    .expect("python script should succeed");

    let log_contents = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    assert!(log_contents.contains(r#""stream":"stdout""#));
    assert!(log_contents.contains(r#""level":"INFO""#));
    assert!(log_contents.contains("hello"));
    assert!(log_contents.contains(r#""stream":"stderr""#));
    assert!(log_contents.contains(r#""level":"ERROR""#));
    assert!(log_contents.contains("world"));
}

#[tokio::test]
async fn run_script_reports_failure_on_exit_code() {
    let token = CancellationToken::new();

    let err = python::run_script(
        &python_bin(),
        &script_path(),
        &["exit".into(), "5".into()],
        &token,
    )
    .await
    .expect_err("expected non-zero exit to bubble up");
    assert!(err.to_string().contains("failed"));
}

#[tokio::test]
async fn run_script_can_be_cancelled() {
    let token = CancellationToken::new();
    let cancel_token = token.clone();
    let args = vec!["sleep".into(), "5".into()];
    let py_bin = python_bin();
    let script = script_path();

    let task =
        tokio::spawn(
            async move { python::run_script(&py_bin, &script, &args, &cancel_token).await },
        );

    sleep(Duration::from_millis(100)).await;
    token.cancel();

    let err = task.await.unwrap().expect_err("cancellation should error");
    assert!(err.to_string().contains("canceled"));
}
