use httpmock::prelude::*;
use serde_json::json;
use server_adapters::dms::{
    client::{DmsClient, LeaseState},
    models::{LeaseRequest, LeaseResponse, TaskSummary},
};
use tokio::time::timeout;

fn test_config() -> (String, String, Vec<String>) {
    (
        "http://localhost".to_string(),
        "siwe:00000000-0000-0000-0000-000000000001".to_string(),
        vec!["local-and-global-refinement".to_string()],
    )
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .use_rustls_tls()
        .no_proxy()
        .build()
        .expect("reqwest client")
}

#[tokio::test]
async fn client_sends_authorization_header() {
    let server = MockServer::start();
    let (_, identity, _) = test_config();
    let base_url = server.base_url();
    let client = DmsClient::new(base_url, identity.clone(), http_client()).unwrap();

    let mock = server.mock(|when, then| {
        when.method(GET)
            .path("/tasks")
            .query_param("capability", "local-and-global-refinement")
            .header("authorization", format!("Bearer {}", identity));
        then.status(204);
    });

    let _ = client
        .lease_task(&LeaseRequest {
            capability: "local-and-global-refinement".into(),
            job_id: None,
            domain_id: None,
        })
        .await;

    mock.assert();
}

#[tokio::test]
async fn client_captures_access_token_but_does_not_echo_it() {
    let server = MockServer::start();
    let identity = "siwe:00000000-0000-0000-0000-000000000001".to_string();
    let client = DmsClient::new(server.base_url(), identity.clone(), http_client()).unwrap();

    let lease_response = LeaseResponse {
        task: Some(TaskSummary {
            id: "task-123".into(),
            capability: "local-and-global-refinement".into(),
            meta: serde_json::json!({"foo": "bar"}),
        }),
        access_token: Some("secret-token".into()),
        access_token_expires_at: Some("2025-01-01T00:00:00Z".into()),
        lease_expires_at: Some("2025-01-01T00:05:00Z".into()),
        ..LeaseResponse::default()
    };

    let heartbeat_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/tasks/task-123/heartbeat")
            .header("authorization", format!("Bearer {}", identity))
            .json_body(json!({ "progress": {"pct": 10}}));
        then.status(200).json_body_obj(&lease_response);
    });

    client
        .store_session(LeaseState {
            task_id: "task-123".into(),
            access_token: None,
            access_token_expires_at: None,
        })
        .await;

    let response = client
        .send_heartbeat("task-123", Some(&serde_json::json!({"pct": 10})))
        .await
        .unwrap();

    heartbeat_mock.assert();
    assert_eq!(response.access_token, Some("secret-token".into()));
    assert_eq!(client.access_token().await, Some("secret-token".into()));
}

#[tokio::test]
async fn client_complete_task_clears_session_and_sends_payload() {
    let server = MockServer::start();
    let identity = "siwe:00000000-0000-0000-0000-000000000001".to_string();
    let client = DmsClient::new(server.base_url(), identity.clone(), http_client()).unwrap();

    client
        .store_session(LeaseState {
            task_id: "task-123".into(),
            access_token: Some("secret-token".into()),
            access_token_expires_at: Some("2025-01-01T00:00:00Z".into()),
        })
        .await;

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/tasks/task-123/complete")
            .header("authorization", format!("Bearer {}", identity))
            .json_body(json!({
                "output_cids": ["cid-1", "cid-2"],
                "meta": {"foo": "bar"}
            }));
        then.status(200);
    });

    client
        .complete_task(
            "task-123",
            &["cid-1".into(), "cid-2".into()],
            Some(&json!({"foo": "bar"})),
        )
        .await
        .unwrap();

    mock.assert();
    assert!(client.access_token().await.is_none());
    assert!(client.access_token_expires_at().await.is_none());
}

#[tokio::test]
async fn client_fail_task_sends_reason_and_details() {
    let server = MockServer::start();
    let identity = "siwe:00000000-0000-0000-0000-000000000001".to_string();
    let client = DmsClient::new(server.base_url(), identity.clone(), http_client()).unwrap();

    client
        .store_session(LeaseState {
            task_id: "task-999".into(),
            access_token: Some("stale".into()),
            access_token_expires_at: None,
        })
        .await;

    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/tasks/task-999/fail")
            .header("authorization", format!("Bearer {}", identity))
            .json_body(json!({
                "reason": "pipeline error",
                "details": {"hint": "retry"}
            }));
        then.status(200);
    });

    client
        .fail_task(
            "task-999",
            "pipeline error",
            Some(&json!({"hint": "retry"})),
        )
        .await
        .unwrap();

    mock.assert();
    assert!(client.access_token().await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dms_client_handles_concurrent_session_updates() {
    let identity = "siwe:00000000-0000-0000-0000-000000000001".to_string();
    let client = DmsClient::new("http://localhost", identity, http_client()).unwrap();

    let handles: Vec<_> = (0..8)
        .map(|idx| {
            let client = client.clone();
            tokio::spawn(async move {
                for iter in 0..200 {
                    client
                        .store_session(LeaseState {
                            task_id: format!("task-{idx}"),
                            access_token: Some(format!("token-{idx}-{iter}")),
                            access_token_expires_at: Some("2025-01-01T00:00:00Z".into()),
                        })
                        .await;
                    let _ = client.access_token().await;
                    if iter % 3 == 0 {
                        client.clear_session().await;
                    }
                }
                Ok::<(), ()>(())
            })
        })
        .collect();

    for handle in handles {
        timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("concurrent task timed out")
            .expect("join success")
            .expect("task completed");
    }

    client.clear_session().await;
    assert!(client.access_token().await.is_none());
}
