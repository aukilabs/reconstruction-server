use std::{path::Path, process::Stdio};

use anyhow::{anyhow, Context, Result};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};
use tokio_util::sync::CancellationToken;
use tracing::Level;

/// Run the given python script with provided arguments, streaming stdout/stderr to the log file.
pub async fn run_script(
    python_bin: &Path,
    script_path: &Path,
    args: &[String],
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

    let mut tasks = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        tasks.push(tokio::spawn(forward_stream(BufReader::new(stdout), false)));
    }
    if let Some(stderr) = child.stderr.take() {
        tasks.push(tokio::spawn(forward_stream(BufReader::new(stderr), true)));
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

async fn forward_stream<R>(mut reader: BufReader<R>, is_stderr: bool) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut line = String::new();
    while reader.read_line(&mut line).await? != 0 {
        let trimmed = line.trim_end_matches(&['\r', '\n'][..]);
        if is_stderr {
            tracing::event!(
                Level::ERROR,
                stream = "stderr",
                message = %trimmed
            );
        } else {
            tracing::event!(
                Level::INFO,
                stream = "stdout",
                message = %trimmed
            );
        }
        line.clear();
    }
    Ok(())
}
