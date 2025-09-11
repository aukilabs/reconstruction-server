use axum::body;
use axum::http::{Request, StatusCode};
use parking_lot::Mutex;
use server_adapters::http;
use server_core::{ExpectedOutput, Job, JobList, Services};
use std::fs;
use std::path::PathBuf;
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
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        Ok(())
    }
}

fn app_with(api_key: Option<&str>) -> axum::Router {
    let domain = Box::leak(Box::new(NoopDomain));
    let runner = Box::leak(Box::new(SlowRunner));
    let services = Services { domain, runner };
    let state = http::AppState {
        api_key: api_key.map(|s| s.to_string()),
        jobs: Arc::new(Mutex::new(JobList::default())),
        job_in_progress: Arc::new(Mutex::new(false)),
        services: Arc::new(services),
        cpu_workers: 1,
    };
    http::router(state)
}

fn job_request_body() -> String {
    serde_json::json!({
        "data_ids": ["a","b"],
        "domain_id": "dom1",
        "access_token": "token",
        "processing_type": "local_and_global_refinement",
        "domain_server_url": "http://example",
        "skip_manifest_upload": true,
        "override_job_name": "",
        "override_manifest_id": "",
    })
    .to_string()
}

#[tokio::test]
async fn unauthorized_without_api_key_header() {
    let app = app_with(Some("secret"));
    let req = Request::builder()
        .method("POST")
        .uri("/jobs")
        .body(axum::body::Body::from(job_request_body()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn busy_returns_503() {
    let app = app_with(Some("secret"));
    let req1 = Request::builder()
        .method("POST")
        .uri("/jobs")
        .header("X-API-Key", "secret")
        .body(axum::body::Body::from(job_request_body()))
        .unwrap();
    let _ = app.clone().oneshot(req1).await.unwrap();
    let req2 = Request::builder()
        .method("POST")
        .uri("/jobs")
        .header("X-API-Key", "secret")
        .body(axum::body::Body::from(job_request_body()))
        .unwrap();
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body::to_bytes(resp2.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        std::str::from_utf8(&body).unwrap(),
        "Reconstruction server is busy processing another job"
    );
}

#[tokio::test]
async fn invalid_body_yields_500() {
    let app = app_with(Some("secret"));
    let req = Request::builder()
        .method("POST")
        .uri("/jobs")
        .header("X-API-Key", "secret")
        .body(axum::body::Body::from("not json"))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = body::to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    assert!(!body.is_empty());
}

#[tokio::test]
async fn get_jobs_returns_json_array() {
    let app = app_with(Some("secret"));
    let req = Request::builder()
        .method("GET")
        .uri("/jobs")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert_eq!(ct, "application/json");
    let body = body::to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    assert_eq!(std::str::from_utf8(&body).unwrap(), "[]\n");
}

// --- Happy path and artifacts checks ---

struct TestDomain;
#[async_trait::async_trait]
impl server_core::DomainPort for TestDomain {
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
    async fn upload_refined_scan_zip(
        &self,
        _job: &Job,
        _scan_id: &str,
        _zip_bytes: Vec<u8>,
    ) -> server_core::Result<()> {
        Ok(())
    }
    async fn download_data_batch(
        &self,
        _job: &Job,
        _ids: &[String],
        datasets_root: &std::path::Path,
    ) -> server_core::Result<()> {
        fs::create_dir_all(datasets_root).unwrap();
        let scan = datasets_root.join("2024-09-27_01-13-50");
        fs::create_dir_all(&scan).unwrap();
        let manifest = serde_json::json!({
            "frameCount": 120.0,
            "duration": 12.0,
            "portals": [{"shortId": "p1", "physicalSize": 1.23}],
            "brand": "Auki",
            "model": "X",
            "systemName": "iOS",
            "systemVersion": "17",
            "appVersion": "1.0",
            "buildId": "42"
        });
        fs::write(
            scan.join("Manifest.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        Ok(())
    }
}

struct TestRunner;
#[async_trait::async_trait]
impl server_core::JobRunner for TestRunner {
    async fn run_python(&self, job: &Job, _cpu_workers: usize) -> server_core::Result<()> {
        let scan_id = "2024-09-27_01-13-50";
        let sfm = job
            .job_path
            .join("refined")
            .join("local")
            .join(scan_id)
            .join("sfm");
        fs::create_dir_all(&sfm).map_err(server_core::DomainError::Io)?;
        for name in ["images.bin", "cameras.bin", "points3D.bin", "portals.csv"] {
            fs::write(sfm.join(name), b"x").map_err(server_core::DomainError::Io)?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn post_jobs_happy_path_and_artifacts() {
    let domain = Box::leak(Box::new(TestDomain));
    let runner = Box::leak(Box::new(TestRunner));
    let services = Services { domain, runner };
    let state = http::AppState {
        api_key: Some("secret".into()),
        jobs: Arc::new(Mutex::new(JobList::default())),
        job_in_progress: Arc::new(Mutex::new(false)),
        services: Arc::new(services),
        cpu_workers: 1,
    };
    let app = http::router(state);

    let body = serde_json::json!({
        "data_ids": ["a","b"],
        "domain_id": "dom_test",
        "access_token": "token",
        "processing_type": "local_and_global_refinement",
        "domain_server_url": "http://example",
        "skip_manifest_upload": true,
        "override_job_name": "",
        "override_manifest_id": "",
    })
    .to_string();
    let req = Request::builder()
        .method("POST")
        .uri("/jobs")
        .header("X-API-Key", "secret")
        .body(axum::body::Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let mut job_dir: Option<PathBuf> = None;
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
                    let base = PathBuf::from("jobs").join("dom_test");
                    if base.exists() {
                        if let Ok(mut rd) = fs::read_dir(&base) {
                            if let Some(Ok(entry)) = rd.next() {
                                job_dir = Some(entry.path());
                            }
                        }
                    }
                    break;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let job_dir = job_dir.expect("job directory exists");

    let manifest_path = job_dir.join("job_manifest.json");
    assert!(manifest_path.exists(), "manifest file exists");
    let m: serde_json::Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    assert!(m.get("jobStatus").is_some());
    assert!(m.get("jobProgress").is_some());
    assert!(m.get("jobStatusDetails").is_some());

    let summary_path = job_dir.join("scan_data_summary.json");
    assert!(summary_path.exists(), "scan_data_summary.json exists");
    let s: serde_json::Value = serde_json::from_slice(&fs::read(&summary_path).unwrap()).unwrap();
    assert!(s.get("scanCount").is_some());
}
