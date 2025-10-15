use std::{path::Path, process::Stdio, sync::Arc};

use anyhow::{anyhow, Context, Result};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

/// Run the given python script with provided arguments, streaming stdout/stderr to the log file.
pub async fn run_script(
    python_bin: &Path,
    script_path: &Path,
    args: &[String],
    log_path: &Path,
    cancel: &CancellationToken,
) -> Result<()> {
    let mut cmd = Command::new(python_bin);
    cmd.arg(script_path)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().with_context(|| {
        format!(
            "spawn python process {} {}",
            python_bin.display(),
            script_path.display()
        )
    })?;

    let log_file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .await
        .with_context(|| format!("open log file {}", log_path.display()))?;
    let log = Arc::new(Mutex::new(log_file));

    let mut tasks = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        tasks.push(tokio::spawn(forward_stream(
            BufReader::new(stdout),
            log.clone(),
            false,
        )));
    }
    if let Some(stderr) = child.stderr.take() {
        tasks.push(tokio::spawn(forward_stream(
            BufReader::new(stderr),
            log.clone(),
            true,
        )));
    }

    let exit_status = tokio::select! {
        status = child.wait() => status,
        _ = cancel.cancelled() => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(anyhow!("python execution canceled"));
        }
    }?;

    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err.context("forwarding python output")),
            Err(join_err) => return Err(anyhow!("forward task panicked: {}", join_err)),
        }
    }

    if exit_status.success() {
        Ok(())
    } else {
        Err(anyhow!("python execution failed"))
    }
}

async fn forward_stream<R>(
    mut reader: BufReader<R>,
    log: Arc<Mutex<File>>,
    is_stderr: bool,
) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut line = String::new();
    while reader.read_line(&mut line).await? != 0 {
        let mut writer = log.lock().await;
        if is_stderr {
            writer
                .write_all(format!("ERR: {}", line).as_bytes())
                .await?;
        } else {
            writer
                .write_all(format!("OUT: {}", line).as_bytes())
                .await?;
        }
        writer.flush().await?;
        line.clear();
    }
    Ok(())
}
