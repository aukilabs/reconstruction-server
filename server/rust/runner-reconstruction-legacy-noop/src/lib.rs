//! runner-reconstruction-legacy-noop: domain-aware noop runner that mirrors the
//! legacy reconstruction workflow without invoking the Python stack.
//!
//! The runner downloads all declared inputs, waits for a configurable amount of
//! time to mimic compute, and uploads placeholder artifacts (manifests and a
//! tiny point-cloud) so the surrounding pipeline can exercise success paths.

use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use compute_runner_api::{Runner, TaskCtx};
use serde_json::json;

/// Public crate identifier used by workspace smoke tests.
pub const CRATE_NAME: &str = "runner-reconstruction-legacy-noop";

/// Capability handled by this runner (legacy reconstruction noop).
pub const CAPABILITY: &str = "/reconstruction/local-and-global-refinement/v1";
pub const CAPABILITY_LOCAL_ONLY: &str = "/reconstruction/local-refinement/v1";
pub const CAPABILITY_GLOBAL_ONLY: &str = "/reconstruction/global-refinement/v1";
pub const CAPABILITIES: [&str; 3] = [CAPABILITY_LOCAL_ONLY, CAPABILITY_GLOBAL_ONLY, CAPABILITY];

/// No-op runner that emulates the legacy reconstruction flow without Python.
pub struct RunnerReconstructionLegacyNoop {
    sleep: Duration,
    capability: &'static str,
}

impl RunnerReconstructionLegacyNoop {
    /// Create a new noop runner with the desired sleep seconds.
    pub fn new(sleep_secs: u64) -> Self {
        Self::with_capability(CAPABILITY, sleep_secs)
    }

    pub fn with_capability(capability: &'static str, sleep_secs: u64) -> Self {
        Self {
            sleep: Duration::from_secs(sleep_secs),
            capability,
        }
    }

    pub fn for_all_capabilities(sleep_secs: u64) -> Vec<Self> {
        CAPABILITIES
            .iter()
            .map(|cap| Self::with_capability(cap, sleep_secs))
            .collect()
    }

    fn sleep_duration(&self) -> Duration {
        self.sleep
    }
}

impl Default for RunnerReconstructionLegacyNoop {
    fn default() -> Self {
        Self::new(5)
    }
}

#[async_trait::async_trait]
impl Runner for RunnerReconstructionLegacyNoop {
    fn capability(&self) -> &'static str {
        self.capability
    }

    async fn run(&self, ctx: TaskCtx<'_>) -> anyhow::Result<()> {
        let task = &ctx.lease.task;
        let job_id = task.job_id.map(|id| id.to_string());
        let task_id = task.id.to_string();

        if ctx.ctrl.is_cancelled().await {
            anyhow::bail!("task cancelled before noop execution");
        }

        // Materialize all input CIDs to ensure domain data is accessible before proceeding.
        let mut materialized: Vec<compute_runner_api::MaterializedInput> =
            Vec::with_capacity(task.inputs_cids.len());
        for cid in &task.inputs_cids {
            let input_meta = ctx
                .input
                .materialize_cid_with_meta(cid)
                .await
                .with_context(|| format!("materialize input CID {cid}"))?;
            ctx.ctrl
                .log_event(json!({
                    "event": "materialize_input",
                    "cid": cid,
                    "dataId": input_meta.data_id.clone(),
                    "dataType": input_meta.data_type.clone(),
                    "name": input_meta.name.clone(),
                    "domainId": input_meta.domain_id.clone(),
                }))
                .await
                .ok();
            materialized.push(input_meta);

            if ctx.ctrl.is_cancelled().await {
                anyhow::bail!("task cancelled while materializing inputs");
            }
        }

        ctx.ctrl
            .progress(json!({
                "stage": "inputs_materialized",
                "inputsCount": materialized.len(),
            }))
            .await
            .ok();

        // Wait for the configured duration to mimic compute time.
        let sleep = self.sleep_duration();
        if sleep.as_secs() > 0 || sleep.subsec_nanos() > 0 {
            tokio::time::sleep(sleep).await;
        }

        if ctx.ctrl.is_cancelled().await {
            anyhow::bail!("task cancelled during noop sleep");
        }

        let now = Utc::now().to_rfc3339();
        let inputs_summary: Vec<_> = materialized
            .iter()
            .map(|info| {
                json!({
                    "cid": info.cid,
                    "materializedPath": info.path.to_string_lossy().to_string(),
                    "dataId": info.data_id.as_deref(),
                    "dataType": info.data_type.as_deref(),
                    "name": info.name.as_deref(),
                    "domainId": info.domain_id.as_deref(),
                })
            })
            .collect();

        // Job manifest mirrors the final manifest produced by the legacy runner when successful.
        let job_manifest = json!({
            "jobStatus": "succeeded",
            "jobProgress": 100,
            "jobStatusDetails": "No-op legacy reconstruction completed",
            "generatedAt": now,
            "taskId": task_id,
            "jobId": job_id,
            "inputs": inputs_summary,
        });
        let job_manifest_bytes = serde_json::to_vec_pretty(&job_manifest)?;
        ctx.output
            .put_bytes("job_manifest.json", &job_manifest_bytes)
            .await?;

        // Refined manifest stub describing placeholder artifacts.
        let refined_manifest = json!({
            "version": 1,
            "generatedAt": now,
            "taskId": task_id,
            "jobId": job_id,
            "artifacts": [
                { "name": "refined_pointcloud_reduced", "path": "refined/global/RefinedPointCloudReduced.ply" }
            ],
            "notes": "Placeholder manifest generated by runner-reconstruction-legacy-noop",
        });
        let refined_manifest_bytes = serde_json::to_vec_pretty(&refined_manifest)?;
        ctx.output
            .put_bytes(
                "refined/global/refined_manifest.json",
                &refined_manifest_bytes,
            )
            .await?;

        // Minimal PLY payload representing an empty point cloud.
        const PLY_PLACEHOLDER: &str = concat!(
            "ply\n",
            "format ascii 1.0\n",
            "comment generated by runner-reconstruction-legacy-noop\n",
            "element vertex 0\n",
            "property float x\n",
            "property float y\n",
            "property float z\n",
            "end_header\n"
        );
        ctx.output
            .put_bytes(
                "refined/global/RefinedPointCloudReduced.ply",
                PLY_PLACEHOLDER.as_bytes(),
            )
            .await?;

        // High-level outputs index describing what was produced.
        let outputs_index = json!({
            "artifacts": [
                { "path": "job_manifest.json", "description": "Final job manifest" },
                { "path": "refined/global/refined_manifest.json", "description": "Placeholder refined manifest" },
                { "path": "refined/global/RefinedPointCloudReduced.ply", "description": "Placeholder point cloud" },
            ]
        });
        let outputs_index_bytes = serde_json::to_vec_pretty(&outputs_index)?;
        ctx.output
            .put_bytes("outputs_index.json", &outputs_index_bytes)
            .await?;

        // Result payload summarising the noop execution.
        let result_payload = json!({
            "status": "succeeded",
            "generatedAt": now,
            "runner": "runner-reconstruction-legacy-noop",
            "details": {
                "sleepSeconds": sleep.as_secs(),
                "inputsMaterialized": materialized.len(),
            }
        });
        let result_bytes = serde_json::to_vec_pretty(&result_payload)?;
        ctx.output.put_bytes("result.json", &result_bytes).await?;

        // Scan data summary placeholder mirrors legacy behavior.
        let scan_summary = json!({
            "generatedAt": now,
            "totalScans": materialized.len(),
            "notes": "Summary generated by noop runner; no real scan metrics available."
        });
        let summary_bytes = serde_json::to_vec_pretty(&scan_summary)?;
        ctx.output
            .put_bytes("scan_data_summary.json", &summary_bytes)
            .await?;

        ctx.ctrl
            .progress(json!({ "stage": "completed" }))
            .await
            .ok();

        Ok(())
    }
}
