use std::{path::Path, process::Stdio};

use anyhow::{anyhow, Context, Result};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};
use tokio_util::sync::CancellationToken;
use tracing::Level;

/// Run the given python script with provided arguments, streaming stdout/stderr to the log file.
/// If `job_root` is provided, attempts to read `fail_reason.txt` from it on failure.
pub async fn run_script(
    python_bin: &Path,
    script_path: &Path,
    args: &[String],
    cancel: &CancellationToken,
    job_root: Option<&Path>,
) -> Result<()> {
    // Remove stale fail_reason.txt from previous runs
    if let Some(root) = job_root {
        let fail_reason_path = root.join("fail_reason.txt");
        if fail_reason_path.exists() {
            let _ = std::fs::remove_file(&fail_reason_path);
        }
    }

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
        let code = exit_status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".into());

        // Try to read fail_reason.txt from job_root for a detailed error message
        let fail_reason = job_root
            .map(|root| root.join("fail_reason.txt"))
            .filter(|p| p.exists())
            .and_then(|p| std::fs::read_to_string(&p).ok());

        match fail_reason {
            Some(reason) => Err(anyhow!("{}", reason.trim())),
            None => Err(anyhow!("python execution failed (exit code {})", code)),
        }
    }
}

struct ParsedLogInfo {
    level: Level,
    msg: String,
}

/// If the provided line is valid JSON, parses "level" and "message" fields.
/// This ensures we propagate log levels correctly from python side.
/// If not JSON or no "message" field, the whole string is logged, as INFO.
/// If JSON is valid but no log level, the "message" is logged as INFO.
fn parse_log_line(line: &str) -> ParsedLogInfo {
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
        let level_str = json
            .get("level")
            .and_then(|v| v.as_str())
            .unwrap_or("INFO");
        let level = match level_str.to_uppercase().as_str() {
            "ERROR" => Level::ERROR,
            "WARN" | "WARNING" => Level::WARN,
            "DEBUG" => Level::DEBUG,
            "TRACE" => Level::TRACE,
            _ => Level::INFO,
        };
        let msg = json
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or(line)
            .to_string();
        ParsedLogInfo { level, msg }
    } else {
        ParsedLogInfo {
            level: Level::INFO,
            msg: line.to_string(),
        }
    }
}

/// Emits a tracing event at the given level. Macro dispatch required since tracing::event! needs compile-time level.
macro_rules! emit_log {
    ($level:expr, $stream:expr, $msg:expr) => {
        match $level {
            Level::ERROR => tracing::error!(stream = $stream, message = %$msg),
            Level::WARN => tracing::warn!(stream = $stream, message = %$msg),
            Level::DEBUG => tracing::debug!(stream = $stream, message = %$msg),
            Level::TRACE => tracing::trace!(stream = $stream, message = %$msg),
            _ => tracing::info!(stream = $stream, message = %$msg),
        }
    };
}

async fn forward_stream<R>(mut reader: BufReader<R>, is_stderr: bool) -> Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut line = String::new();
    while reader.read_line(&mut line).await? != 0 {
        let trimmed = line.trim_end_matches(&['\r', '\n'][..]);
        let parsed = parse_log_line(trimmed);
        let stream_name = if is_stderr { "stderr" } else { "stdout" };
        emit_log!(parsed.level, stream_name, parsed.msg);
        line.clear();
    }
    Ok(())
}
