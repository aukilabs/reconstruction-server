//! Child-process lifetime for pipeline subprocesses.
//!
//! Pipelines fan out into grandchildren (e.g. `ns-train`, COLMAP) that keep
//! the GPU busy. Killing only the direct child orphans them, so the child runs
//! as the leader of its own process group and cancellation signals the group.
use std::time::Duration;
use tokio::process::{Child, Command};

/// How long the group gets to exit after SIGTERM before SIGKILL.
pub const TERMINATE_GRACE: Duration = Duration::from_secs(10);

/// Make `command` the leader of a new process group, killed if its handle drops.
pub fn isolate(command: &mut Command) -> &mut Command {
    #[cfg(unix)]
    command.process_group(0);
    command.kill_on_drop(true)
}

/// Stop the child's whole process group: SIGTERM, then SIGKILL after `grace`.
/// Waits for the direct child to exit.
pub async fn terminate_group(child: &mut Child, grace: Duration) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        signal_group(pid, libc::SIGTERM);
        if tokio::time::timeout(grace, child.wait()).await.is_err() {
            signal_group(pid, libc::SIGKILL);
            let _ = child.wait().await;
        } else {
            // The leader exited; make sure no stragglers outlive it.
            signal_group(pid, libc::SIGKILL);
        }
        return;
    }
    let _ = grace;
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[cfg(unix)]
fn signal_group(pid: u32, signal: libc::c_int) {
    // SAFETY: killpg only sends a signal; a stale or exited group yields ESRCH.
    unsafe {
        libc::killpg(pid as libc::pid_t, signal);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};

    fn alive(pid: i32) -> bool {
        // SAFETY: signal 0 only checks that the process exists.
        unsafe { libc::kill(pid, 0) == 0 }
    }

    #[tokio::test]
    async fn terminating_the_group_also_stops_grandchildren() {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 60 & echo $!; wait")
            .stdout(Stdio::piped());
        let mut child = isolate(&mut command).spawn().unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let grandchild: i32 = lines.next_line().await.unwrap().unwrap().parse().unwrap();
        assert!(alive(grandchild));

        terminate_group(&mut child, Duration::from_secs(5)).await;

        // The grandchild is reparented and reaped asynchronously; allow a moment.
        tokio::time::timeout(Duration::from_secs(5), async {
            while alive(grandchild) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("grandchild survived group termination");
    }

    #[tokio::test]
    async fn a_child_ignoring_sigterm_is_killed_after_the_grace_period() {
        let mut command = Command::new("sh");
        command.arg("-c").arg("trap '' TERM; sleep 60");
        let mut child = isolate(&mut command).spawn().unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = std::time::Instant::now();
        terminate_group(&mut child, Duration::from_millis(300)).await;
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(child.try_wait().unwrap().is_some());
    }
}
