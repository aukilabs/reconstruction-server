use axum::http::{Request, StatusCode};
use parking_lot::Mutex;
use server_adapters::http;
use server_core::{ExpectedOutput, Job, JobList, Services};
use std::sync::Arc;
use tower::util::ServiceExt;

struct NoopDomain;
#[async_trait::async_trait]
impl server_core::DomainPort for NoopDomain {
    async fn upload_manifest(
        &self,
        _job: &Job,
        _manifest_path: &std::path::Path,
    ) -> server_core::Result<()> {
        Ok(())
    }
    async fn upload_output(&self, _job: &Job, _output: &ExpectedOutput) -> server_core::Result<()> {
        Ok(())
    }
    async fn download_data_batch(
        &self,
        _job: &Job,
        _ids: &[String],
        _datasets_root: &std::path::Path,
    ) -> server_core::Result<()> {
        Ok(())
    }
    async fn upload_refined_scan_zip(
        &self,
        _job: &Job,
        _scan_id: &str,
        _zip_bytes: Vec<u8>,
    ) -> server_core::Result<()> {
        Ok(())
    }
}

struct SlowRunner;
#[async_trait::async_trait]
impl server_core::JobRunner for SlowRunner {
    async fn run_python(&self, _job: &Job, _cpu_workers: usize) -> server_core::Result<()> {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        Ok(())
    }
}

fn app_with_api_key() -> axum::Router {
    let domain = Box::leak(Box::new(NoopDomain));
    let runner = Box::leak(Box::new(SlowRunner));
    let services = Services { domain, runner };
    let state = http::AppState {
        api_key: Some("secret".into()),
        jobs: Arc::new(Mutex::new(JobList::default())),
        job_in_progress: Arc::new(Mutex::new(false)),
        services: Arc::new(services),
        cpu_workers: 1,
    };
    http::router(state)
}

async fn post_and_wait(app: axum::Router, body: serde_json::Value) {
    let req = Request::builder()
        .method("POST")
        .uri("/jobs")
        .header("X-API-Key", "secret")
        .body(axum::body::Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Poll until job reaches succeeded
    for _ in 0..50 {
        let req = Request::builder()
            .method("GET")
            .uri("/jobs")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if let Some(arr) = v.as_array() {
            if let Some(job) = arr.first() {
                if job.get("status").and_then(|s| s.as_str()) == Some("succeeded") {
                    return;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("job did not succeed in time");
}

fn base_body() -> serde_json::Value {
    serde_json::json!({
        "data_ids": ["a","b"],
        "domain_id": "dom_opt",
        "access_token": "token",
        "processing_type": "local_and_global_refinement"
    })
}

#[tokio::test]
async fn post_jobs_omitting_domain_server_url() {
    let app = app_with_api_key();
    let mut body = base_body();
    // Intentionally do not set domain_server_url
    body["skip_manifest_upload"] = serde_json::json!(true);
    body["override_job_name"] = serde_json::json!("");
    body["override_manifest_id"] = serde_json::json!("");
    post_and_wait(app, body).await;
}

#[tokio::test]
async fn post_jobs_omitting_skip_manifest_upload() {
    let app = app_with_api_key();
    let mut body = base_body();
    body["domain_server_url"] = serde_json::json!("http://example");
    // Intentionally omit skip_manifest_upload
    body["override_job_name"] = serde_json::json!("");
    body["override_manifest_id"] = serde_json::json!("");
    post_and_wait(app, body).await;
}

#[tokio::test]
async fn post_jobs_omitting_override_job_name() {
    let app = app_with_api_key();
    let mut body = base_body();
    body["domain_server_url"] = serde_json::json!("http://example");
    body["skip_manifest_upload"] = serde_json::json!(true);
    // Omit override_job_name
    body["override_manifest_id"] = serde_json::json!("");
    post_and_wait(app, body).await;
}

#[tokio::test]
async fn post_jobs_omitting_override_manifest_id() {
    let app = app_with_api_key();
    let mut body = base_body();
    body["domain_server_url"] = serde_json::json!("http://example");
    body["skip_manifest_upload"] = serde_json::json!(true);
    body["override_job_name"] = serde_json::json!("");
    // Omit override_manifest_id
    post_and_wait(app, body).await;
}
