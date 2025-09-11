use axum::http::StatusCode;
use parking_lot::Mutex;
use server_adapters::http;
use server_core::{JobList, Services};
use std::sync::Arc;
use tower::util::ServiceExt;

struct NoopDomain;
#[async_trait::async_trait]
impl server_core::DomainPort for NoopDomain {
    async fn upload_manifest(
        &self,
        _job: &server_core::Job,
        _manifest_path: &std::path::Path,
    ) -> server_core::Result<()> {
        Ok(())
    }
    async fn upload_output(
        &self,
        _job: &server_core::Job,
        _output: &server_core::ExpectedOutput,
    ) -> server_core::Result<()> {
        Ok(())
    }
    async fn download_data_batch(
        &self,
        _job: &server_core::Job,
        _ids: &[String],
        _datasets_root: &std::path::Path,
    ) -> server_core::Result<()> {
        Ok(())
    }
    async fn upload_refined_scan_zip(
        &self,
        _job: &server_core::Job,
        _scan_id: &str,
        _zip_bytes: Vec<u8>,
    ) -> server_core::Result<()> {
        Ok(())
    }
}
struct NoopRunner;
#[async_trait::async_trait]
impl server_core::JobRunner for NoopRunner {
    async fn run_python(
        &self,
        _job: &server_core::Job,
        _cpu_workers: usize,
    ) -> server_core::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn post_jobs_requires_api_key() {
    let domain = Box::leak(Box::new(NoopDomain));
    let runner = Box::leak(Box::new(NoopRunner));
    let services = Services {
        domain,
        runner,
        manifest_interval: std::time::Duration::from_millis(50),
    };
    let state = http::AppState {
        api_key: Some("secret".into()),
        jobs: Arc::new(Mutex::new(JobList::default())),
        job_in_progress: Arc::new(Mutex::new(false)),
        services: Arc::new(services),
        cpu_workers: 1,
        data_dir: std::path::PathBuf::from("jobs"),
    };
    let app = http::router(state);

    let req = axum::http::Request::builder()
        .method("POST")
        .uri("/jobs")
        .body(axum::body::Body::from("{}"))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
