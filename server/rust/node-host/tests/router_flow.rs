//! End-to-end host behaviour against a mock DMS + Domain server.
//!
//! These pin the wire contract of the former `posemesh-compute-node` 0.3.2
//! host: Domain input layout, upsert, and the exact complete/fail bodies.
use async_trait::async_trait;
use auki_auth::machine::token_manager::{TokenProvider, TokenProviderResult};
use auki_dms::client::DmsClient;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::{Duration as ChronoDuration, Utc};
use httpmock::prelude::*;
use node_host::auki_sdk::{AukiDmsTasks, TasksConfig};
use node_host::host::claim_loop;
use node_host::{
    ArtifactContent, ArtifactRequest, HostConfig, NodeRunner, Router, TaskContext, TaskIo,
};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const CAP: &str = "/test/pipeline/v1";
const SCAN: &str = "dmt_manifest_2024-01-02_03-04-05";

struct Auth;
#[async_trait]
impl TokenProvider for Auth {
    async fn bearer(&self) -> TokenProviderResult<String> {
        Ok("machine-fixture".into())
    }
    async fn on_unauthorized(&self) {}
}

struct Fixture {
    server: MockServer,
    task: Uuid,
    job: Uuid,
    domain: Uuid,
    input: Uuid,
    artifact: Uuid,
}

impl Fixture {
    fn new() -> Self {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/api/v1/info");
            then.header("content-type", "application/json").json_body(
                json!({"upload":{"domain_data_max_bytes":67108864,"request_max_bytes":134217728}}),
            );
        });
        Self {
            server,
            task: Uuid::new_v4(),
            job: Uuid::new_v4(),
            domain: Uuid::new_v4(),
            input: Uuid::new_v4(),
            artifact: Uuid::new_v4(),
        }
    }

    fn grant(&self) -> Value {
        let expires = Utc::now() + ChronoDuration::minutes(1);
        let claims = json!({"iss":"dds", "domain_id":self.domain, "aud":[self.server.base_url()],
            "exp":expires.timestamp()});
        let token = format!(
            "e30.{}.fixture",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        );
        json!({"task":{"id":self.task,"job_id":self.job,"capability":CAP,"inputs_cids":[self.input_url()]},
            "domain_id":self.domain,"domain_server_url":self.server.base_url(),
            "access_token":token,"access_token_expires_at":expires,"lease_expires_at":expires})
    }

    fn data_path(&self) -> String {
        format!("/api/v1/domains/{}/data", self.domain)
    }

    fn input_url(&self) -> String {
        format!(
            "{}{}/{}",
            self.server.base_url(),
            self.data_path(),
            self.input
        )
    }

    fn metadata(&self, id: Uuid, name: &str, data_type: &str, size: usize) -> Value {
        json!({"id":id,"domain_id":self.domain,"name":name,"data_type":data_type,"size":size,
            "created_at":"2026-09-01T00:00:00Z","updated_at":"2026-09-01T00:00:00Z"})
    }

    /// DMS lease + heartbeat, Domain input listing/download, upsert lookup and create.
    fn mock_lease_and_domain(&self) {
        self.mock_lease_and_domain_with(&[]);
    }

    /// `existing` is what the upsert lookup finds for the output name.
    fn mock_lease_and_domain_with(&self, existing: &[Value]) {
        let grant = self.grant();
        self.server.mock(|when, then| {
            when.method(GET).path("/tasks");
            then.json_body(grant.clone());
        });
        self.server.mock(|when, then| {
            when.method(POST)
                .path(format!("/tasks/{}/heartbeat", self.task));
            then.json_body(grant.clone());
        });
        let path = self.data_path();
        self.server.mock(|when, then| {
            when.method(GET)
                .path(&path)
                .query_param("ids", self.input.to_string());
            then.header("content-type", "application/json").json_body(
                json!({"data":[self.metadata(self.input, SCAN, "dmt_manifest_json", 5)]}),
            );
        });
        self.server.mock(|when, then| {
            when.method(GET)
                .path(format!("{path}/{}", self.input))
                .query_param("raw", "true");
            then.body("input");
        });
        self.server.mock(|when, then| {
            when.method(GET)
                .path(&path)
                .query_param("name", "refined_splat_2024-01-02_03-04-05")
                .query_param("data_type", "splat_data");
            then.header("content-type", "application/json")
                .json_body(json!({"data": existing}));
        });
        self.server.mock(|when, then| {
            when.method(POST).path(&path).body_includes("splat-bytes");
            then.header("content-type", "application/json").json_body(json!({"data":[
                self.metadata(self.artifact, "refined_splat_2024-01-02_03-04-05", "splat_data", 11)
            ]}));
        });
    }

    fn receipt(&self) -> Value {
        json!({
            "job": {"task_id":self.task,"job_id":self.job,"domain_id":self.domain,"capability":CAP},
            "artifacts": [{"logical_path":"refined_splat_2024-01-02_03-04-05",
                "name":"refined_splat_2024-01-02_03-04-05","data_type":"splat_data","id":self.artifact}],
        })
    }

    fn runtime(&self) -> AukiDmsTasks {
        let client = DmsClient::new(
            self.server.base_url().parse().unwrap(),
            Duration::from_secs(2),
            Arc::new(Auth),
        )
        .unwrap();
        AukiDmsTasks::from_client(
            client,
            "node-host-fixture".into(),
            vec![CAP.into()],
            TasksConfig {
                poll_interval: Duration::from_millis(10),
                heartbeat_interval: Duration::from_millis(20),
                ..TasksConfig::default()
            },
        )
        .unwrap()
    }
}

fn host_config() -> HostConfig {
    HostConfig::from_lookup("0.0.0-test", "test", |key| match key {
        "REG_SECRET" => Some("unused".into()),
        "SECP256K1_PRIVHEX" => Some("unused".into()),
        "POLL_BACKOFF_MS_MIN" | "POLL_BACKOFF_MS_MAX" => Some("10".into()),
        _ => None,
    })
    .unwrap()
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Succeed,
    Fail,
    WaitForCancel,
}

struct Scripted {
    mode: Mode,
    runs: Arc<AtomicUsize>,
}

#[async_trait]
impl NodeRunner for Scripted {
    fn capability(&self) -> &'static str {
        CAP
    }

    async fn run(&self, task: &TaskContext, io: &TaskIo) -> anyhow::Result<()> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        if self.mode == Mode::WaitForCancel {
            task.cancelled().await;
            anyhow::bail!("task cancelled: python execution");
        }
        let input = io.materialize(&task.task.inputs_cids[0]).await?;
        // Contract 2: the 0.3.2 download layout the local runner's renames rely on.
        assert_eq!(
            input.path.strip_prefix(&input.root_dir).unwrap(),
            std::path::Path::new(
                "datasets/2024-01-02_03-04-05/dmt_manifest_2024-01-02_03-04-05.dmt_manifest_json"
            )
        );
        assert_eq!(tokio::fs::read(&input.path).await?, b"input");
        tokio::fs::remove_dir_all(&input.root_dir).await?;
        task.progress(json!({"pct": 50, "stage": "python"}))?;
        io.put_domain_artifact(ArtifactRequest {
            rel_path: "refined_splat_2024-01-02_03-04-05",
            name: "refined_splat_2024-01-02_03-04-05",
            data_type: "splat_data",
            existing_id: None,
            content: ArtifactContent::Bytes(b"splat-bytes"),
        })
        .await?;
        if self.mode == Mode::Fail {
            anyhow::bail!("python job failed: status=Some(1)");
        }
        Ok(())
    }
}

/// A running claim loop over one scripted runner.
struct Host {
    runs: Arc<AtomicUsize>,
    tasks: AukiDmsTasks,
    graceful: CancellationToken,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Host {
    fn start(fixture: &Fixture, mode: Mode) -> Self {
        let runs = Arc::new(AtomicUsize::new(0));
        let router = Router::new().register(Scripted {
            mode,
            runs: runs.clone(),
        });
        let tasks = fixture.runtime();
        let graceful = CancellationToken::new();
        let handle = {
            let (tasks, graceful) = (tasks.clone(), graceful.clone());
            tokio::spawn(async move {
                let forced = CancellationToken::new();
                claim_loop(&tasks, &host_config(), &router, &graceful, &forced).await
            })
        };
        Self {
            runs,
            tasks,
            graceful,
            handle,
        }
    }

    async fn wait(&self, why: &str, done: impl Fn(usize) -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !done(self.runs.load(Ordering::SeqCst)) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting: {why}"));
    }

    /// Graceful stop: the loop must end cleanly.
    async fn stop(self) {
        self.graceful.cancel();
        self.handle.await.unwrap().unwrap();
        self.tasks.close().await.unwrap();
    }
}

#[tokio::test]
async fn success_reports_the_exact_0_3_2_complete_body() {
    let fixture = Fixture::new();
    fixture.mock_lease_and_domain();
    let complete = fixture.server.mock(|when, then| {
        when.method(POST)
            .path(format!("/tasks/{}/complete", fixture.task))
            .json_body(json!({"output_cids":[fixture.artifact], "meta": fixture.receipt()}));
        then.status(200);
    });
    let fail = fixture.server.mock(|when, then| {
        when.method(POST)
            .path(format!("/tasks/{}/fail", fixture.task));
        then.status(200);
    });
    let host = Host::start(&fixture, Mode::Succeed);
    host.wait("complete receipt", |_| complete.calls() >= 1)
        .await;
    host.stop().await;
    assert_eq!(fail.calls(), 0);
}

#[tokio::test]
async fn failure_reports_the_exact_0_3_2_fail_body_and_keeps_claiming() {
    let fixture = Fixture::new();
    fixture.mock_lease_and_domain();
    let fail = fixture.server.mock(|when, then| {
        when.method(POST)
            .path(format!("/tasks/{}/fail", fixture.task))
            .json_body(json!({
                "reason": "runner failed: python job failed: status=Some(1)",
                "details": fixture.receipt(),
            }));
        then.status(200);
    });
    // The mock DMS re-offers the same lease, so a second failure receipt proves
    // the loop claimed again after the first handler failure.
    let host = Host::start(&fixture, Mode::Fail);
    host.wait("two failure receipts", |_| fail.calls() >= 2)
        .await;
    host.stop().await;
}

#[tokio::test]
async fn dms_cancel_sends_no_receipt_and_keeps_claiming() {
    let fixture = Fixture::new();
    let grant = fixture.grant();
    let claims = fixture.server.mock(|when, then| {
        when.method(GET).path("/tasks");
        then.json_body(grant.clone());
    });
    let mut alive = fixture.server.mock(|when, then| {
        when.method(POST)
            .path(format!("/tasks/{}/heartbeat", fixture.task));
        then.json_body(grant.clone());
    });
    let receipts = fixture.server.mock(|when, then| {
        when.method(POST)
            .path_matches(regex::Regex::new(r"/tasks/.*/(complete|fail)$").unwrap());
        then.status(200);
    });
    let host = Host::start(&fixture, Mode::WaitForCancel);
    host.wait("runner started", |runs| runs >= 1).await;

    // DMS cancels the running task on its next heartbeat.
    alive.delete();
    let mut cancelled = grant.clone();
    cancelled["cancel"] = json!(true);
    fixture.server.mock(|when, then| {
        when.method(POST)
            .path(format!("/tasks/{}/heartbeat", fixture.task));
        then.json_body(cancelled);
    });
    let before = claims.calls();
    host.wait("claimed again after the cancel", |_| {
        claims.calls() > before
    })
    .await;
    host.stop().await;
    assert_eq!(receipts.calls(), 0);
}

#[tokio::test]
async fn an_existing_artifact_is_replaced_by_id() {
    let fixture = Fixture::new();
    let existing = fixture.metadata(
        fixture.artifact,
        "refined_splat_2024-01-02_03-04-05",
        "splat_data",
        3,
    );
    fixture.mock_lease_and_domain_with(std::slice::from_ref(&existing));
    let path = fixture.data_path();
    let replace = fixture.server.mock(|when, then| {
        when.method(PUT)
            .path(&path)
            .body_includes("splat-bytes")
            .body_includes(format!("id=\"{}\"", fixture.artifact));
        then.header("content-type", "application/json")
            .json_body(json!({"data":[existing]}));
    });
    let complete = fixture.server.mock(|when, then| {
        when.method(POST)
            .path(format!("/tasks/{}/complete", fixture.task))
            .json_body(json!({"output_cids":[fixture.artifact], "meta": fixture.receipt()}));
        then.status(200);
    });
    let host = Host::start(&fixture, Mode::Succeed);
    host.wait("complete receipt", |_| complete.calls() >= 1)
        .await;
    host.stop().await;
    assert!(replace.calls() >= 1);
}
