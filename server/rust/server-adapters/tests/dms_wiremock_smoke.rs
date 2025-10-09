use serde_json::json;
use server_adapters::{
    auth::token_manager::{TokenProvider, TokenProviderError},
    dms::{
        client::{DmsClient, LeaseState},
        models::{LeaseRequest, TaskSummary},
    },
};
use std::sync::Arc;
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Clone)]
struct StaticTokenProvider {
    token: String,
}

#[async_trait::async_trait]
impl TokenProvider for StaticTokenProvider {
    async fn bearer(&self) -> Result<String, TokenProviderError> {
        Ok(self.token.clone())
    }

    async fn on_unauthorized(&self) {}
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .use_rustls_tls()
        .no_proxy()
        .build()
        .expect("reqwest client")
}

#[tokio::test]
async fn lease_heartbeat_complete_smoke() {
    let server = MockServer::start().await;
    let token = "token-smoke";
    let provider = Arc::new(StaticTokenProvider {
        token: token.to_string(),
    });
    let client = DmsClient::new(
        server.uri(),
        provider.clone() as Arc<dyn TokenProvider>,
        http_client(),
    )
    .unwrap();

    let task_meta = json!({
        "legacy": {
            "data_ids": ["scan-1"],
            "domain_id": "domain-1",
            "access_token": "token-from-request",
            "processing_type": "local_and_global_refinement",
            "domain_server_url": "https://domain.example",
            "skip_manifest_upload": false,
            "override_job_name": "",
            "override_manifest_id": "",
        }
    });

    let lease_body = json!({
        "task": {
            "id": "task-123",
            "capability": "cap/refinement",
            "meta": task_meta,
        },
        "domain_id": "domain-1",
        "domain_server_url": "https://domain.example",
        "access_token": "lease-token-abc",
        "access_token_expires_at": "2025-01-01T00:01:00Z",
        "lease_expires_at": "2025-01-01T00:02:00Z"
    });

    Mock::given(method("GET"))
        .and(path("/tasks"))
        .and(query_param("capability", "cap/refinement"))
        .and(header("authorization", format!("Bearer {}", token)))
        .respond_with(ResponseTemplate::new(200).set_body_json(lease_body))
        .expect(1)
        .mount(&server)
        .await;

    let lease = client
        .lease_task(&LeaseRequest {
            capability: "cap/refinement".into(),
            job_id: None,
            domain_id: None,
        })
        .await
        .expect("lease");
    assert_eq!(
        lease.task.as_ref().map(|t| &t.id),
        Some(&"task-123".to_string())
    );
    assert_eq!(
        lease.domain_server_url.as_deref(),
        Some("https://domain.example")
    );
    assert_eq!(client.access_token().await, Some("lease-token-abc".into()));

    client
        .store_session(LeaseState {
            task_id: "task-123".into(),
            access_token: client.access_token().await,
            access_token_expires_at: lease.access_token_expires_at.clone(),
        })
        .await;

    Mock::given(method("POST"))
        .and(path("/tasks/task-123/heartbeat"))
        .and(header("authorization", format!("Bearer {}", token)))
        .and(body_json(json!({ "progress": { "message": "loading" } })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "task": TaskSummary {
                id: "task-123".into(),
                job_id: None,
                capability: "cap/refinement".into(),
                inputs_cids: vec![],
                meta: json!({}),
            },
            "lease_expires_at": "2025-01-01T00:03:00Z",
            "domain_server_url": "https://domain.example",
        })))
        .expect(1)
        .mount(&server)
        .await;

    let heartbeat = client
        .send_heartbeat("task-123", Some(&json!({"message": "loading"})))
        .await
        .expect("heartbeat");
    assert_eq!(
        heartbeat.lease_expires_at.as_deref(),
        Some("2025-01-01T00:03:00Z")
    );

    Mock::given(method("POST"))
        .and(path("/tasks/task-123/complete"))
        .and(header("authorization", format!("Bearer {}", token)))
        .and(body_json(json!({
            "output_cids": ["cid-1"],
            "meta": {"upload": true}
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    client
        .complete_task(
            "task-123",
            &["cid-1".into()],
            Some(&json!({"upload": true})),
        )
        .await
        .expect("complete");

    assert!(client.access_token().await.is_none());
}
