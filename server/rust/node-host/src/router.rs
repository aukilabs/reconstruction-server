use crate::io::TaskIo;
use async_trait::async_trait;
use auki_sdk::{TaskContext, TaskError, TaskHandler, TaskResult};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

/// One capability's work. Progress, events and cancellation go through `task`;
/// Domain data goes through `io` so uploads land in the DMS receipt.
// async_trait marks the generated boxed-future method `#[must_use]`, which
// newer clippy (1.99+) reports as `double_must_use`.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait NodeRunner: Send + Sync {
    fn capability(&self) -> &'static str;
    async fn run(&self, task: &TaskContext, io: &TaskIo) -> anyhow::Result<()>;
}

/// Dispatches leases by capability and reports the 0.3.2 receipt shape:
/// complete `{output_cids, meta: {job, artifacts}}`, fail
/// `{reason: "runner failed: …", details: {job, artifacts}}`.
#[derive(Default, Clone)]
pub struct Router {
    runners: BTreeMap<&'static str, Arc<dyn NodeRunner>>,
}

/// DMS rejects longer failure reasons; truncate rather than lose the receipt.
const MAX_REASON_BYTES: usize = 4096;

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registering a capability again replaces the earlier runner.
    pub fn register(mut self, runner: impl NodeRunner + 'static) -> Self {
        self.runners.insert(runner.capability(), Arc::new(runner));
        self
    }

    pub fn capabilities(&self) -> Vec<String> {
        self.runners.keys().map(|cap| cap.to_string()).collect()
    }

    pub fn get(&self, capability: &str) -> Option<Arc<dyn NodeRunner>> {
        self.runners.get(capability).cloned()
    }
}

#[async_trait]
impl TaskHandler for Router {
    async fn run(&self, task: TaskContext) -> Result<TaskResult, TaskError> {
        let job = json!({
            "task_id": task.task.id,
            "job_id": task.task.job_id,
            "domain_id": task.credential.domain_id(),
            "capability": task.task.capability,
        });
        let io = match TaskIo::new(&task) {
            Ok(io) => io,
            Err(error) => {
                tracing::warn!(%error, "failed to build task storage");
                let details = json!({"stage": "build_ports", "error": error.to_string()});
                return fail(&task, "node_setup_failed".into(), details);
            }
        };
        let result = match self.get(&task.task.capability) {
            Some(runner) => runner
                .run(&task, &io)
                .await
                .map_err(|e| format!("runner failed: {e}")),
            None => Err(format!(
                "no runner registered for capability: {}",
                task.task.capability
            )),
        };
        io.close().await;
        let artifacts = io.uploaded_artifacts();
        let output_cids = artifacts.iter().filter_map(|a| a.id.clone()).collect();
        let receipt = json!({"job": job, "artifacts": artifacts});
        match result {
            Ok(()) => Ok(TaskResult {
                output_cids,
                meta: receipt,
            }),
            Err(reason) => {
                tracing::error!(task_id = %task.task.id, %reason, "Runner execution failed; reporting failure to DMS");
                fail(&task, reason, receipt)
            }
        }
    }
}

fn fail(task: &TaskContext, reason: String, details: Value) -> Result<TaskResult, TaskError> {
    let reason = truncate(reason);
    match task.set_failure(reason.clone(), details) {
        Ok(()) => {}
        // Oversized details: keep the reason, which DMT shows to users.
        Err(TaskError::Configuration(_)) => task.set_failure(reason, Value::Null)?,
        Err(error) => return Err(error),
    }
    Err(TaskError::Handler)
}

fn truncate(mut reason: String) -> String {
    if reason.len() > MAX_REASON_BYTES {
        let mut end = MAX_REASON_BYTES;
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        reason.truncate(end);
    }
    reason
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(&'static str);
    #[async_trait]
    impl NodeRunner for Fixed {
        fn capability(&self) -> &'static str {
            self.0
        }
        async fn run(&self, _: &TaskContext, _: &TaskIo) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn capabilities_are_sorted_and_deduplicated() {
        let router = Router::new()
            .register(Fixed("/b/v1"))
            .register(Fixed("/a/v1"))
            .register(Fixed("/b/v1"));
        assert_eq!(router.capabilities(), vec!["/a/v1", "/b/v1"]);
        assert!(router.get("/a/v1").is_some());
        assert!(router.get("/c/v1").is_none());
    }

    #[test]
    fn long_reasons_are_truncated_on_a_char_boundary() {
        let reason = truncate("é".repeat(3000));
        assert!(reason.len() <= MAX_REASON_BYTES);
        assert!(reason.chars().all(|c| c == 'é'));
        assert_eq!(truncate("short".into()), "short");
    }
}
