use std::{collections::HashMap, fs, io::Write};

use async_trait::async_trait;
use httpmock::MockServer;
use reqwest::Client;
use server_adapters::storage::DomainTokenProvider;
use server_adapters::storage::HttpDomainClient;
use server_core::{DomainPort, Job, JobMetadata};
use std::io::Cursor;
use std::sync::{Arc, Mutex};

fn build_job(temp_dir: &tempfile::TempDir, domain_url: &str) -> Job {
    let job_path = temp_dir.path().join("jobs").join("dom1").join("job_123");
    fs::create_dir_all(job_path.join("datasets")).unwrap();
    Job {
        meta: JobMetadata {
            id: "job_123".into(),
            name: "job_123".into(),
            domain_id: "dom1".into(),
            processing_type: "local_and_global_refinement".into(),
            created_at: chrono::Utc::now(),
            domain_server_url: domain_url.into(),
            reconstruction_server_url: "http://localhost".into(),
            access_token: "token".into(),
            data_ids: vec![],
            skip_manifest_upload: false,
            override_job_name: String::new(),
            override_manifest_id: String::new(),
        },
        job_path,
        status: "started".into(),
        uploaded_data_ids: HashMap::new(),
        completed_scans: HashMap::new(),
    }
}

fn build_large_zip() -> Vec<u8> {
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let mut zip = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        let mut payload = vec![0u8; 1_048_576];
        for (idx, byte) in payload.iter_mut().enumerate() {
            *byte = (idx % 251) as u8;
        }
        zip.start_file("images.bin", options).unwrap();
        zip.write_all(&payload).unwrap();
        zip.finish().unwrap();
    }
    buffer
}

fn build_multipart_response(
    boundary: &str,
    parts: Vec<(String, String, String, Vec<u8>)>,
) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, data_type, domain_id, bytes) in parts {
        body.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{}\"; data-type=\"{}\"; domain-id=\"{}\"\r\n",
                name, data_type, domain_id
            )
            .as_bytes(),
        );
        body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
        body.extend_from_slice(&bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());
    body
}

#[tokio::test]
async fn download_data_batch_streams_large_zip_parts() {
    let server = MockServer::start_async().await;
    let boundary = "BOUNDARY-123456";
    let zip_bytes = build_large_zip();
    let manifest_bytes = br#"{ "example": true }"#.to_vec();
    let response_body = build_multipart_response(
        boundary,
        vec![
            (
                "scan_2024-01-02_03-04-05".into(),
                "refined_scan_zip".into(),
                "dom1".into(),
                zip_bytes.clone(),
            ),
            (
                "manifest".into(),
                "dmt_manifest_json".into(),
                "dom1".into(),
                manifest_bytes.clone(),
            ),
        ],
    );

    let _mock = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/api/v1/domains/dom1/data")
                .query_param("ids", "scan-1");
            then.status(200)
                .header(
                    "Content-Type",
                    format!("multipart/form-data; boundary={}", boundary),
                )
                .body(response_body.clone());
        })
        .await;

    let client = HttpDomainClient::new(
        Client::builder()
            .use_rustls_tls()
            .no_proxy()
            .build()
            .expect("reqwest client"),
    );
    let tempdir = tempfile::tempdir().unwrap();
    let job = build_job(&tempdir, &server.base_url());
    let datasets_root = job.job_path.join("datasets");

    client
        .download_data_batch(&job, &["scan-1".to_string()], &datasets_root)
        .await
        .expect("download");

    let scan_dir_name = "2024-01-02_03-04-05";
    let scan_folder = job
        .job_path
        .join("refined")
        .join("local")
        .join(scan_dir_name)
        .join("sfm")
        .join("images.bin");
    let extracted = fs::read(scan_folder).expect("extracted zip");
    assert_eq!(extracted.len(), 1_048_576);

    let manifest_path = datasets_root.join("manifest").join("Manifest.json");
    let manifest = fs::read(manifest_path).expect("manifest file");
    assert_eq!(manifest, manifest_bytes);

    let zip_path = job
        .job_path
        .join("datasets")
        .join(scan_dir_name)
        .join("RefinedScan.zip");
    assert_eq!(
        fs::metadata(zip_path).unwrap().len(),
        zip_bytes.len() as u64
    );
}

#[derive(Clone)]
struct StaticProvider(String);

#[async_trait]
impl DomainTokenProvider for StaticProvider {
    async fn bearer(&self) -> Option<String> {
        Some(self.0.clone())
    }
}

#[tokio::test]
async fn http_client_prefers_session_token_over_legacy_metadata() {
    let server = MockServer::start_async().await;

    // Expect Authorization header to carry the provider token
    let _mock = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/api/v1/domains/dom1/data")
                .header("authorization", "Bearer dms-lease-token");
            then.status(200).json_body(serde_json::json!({
                "data": [{
                    "id": "data-1",
                    "domain_id": "dom1",
                    "name": "refined_manifest",
                    "data_type": "refined_manifest_json"
                }]
            }));
        })
        .await;

    // Build client with provider that returns a DMS lease token
    let http = Client::builder()
        .use_rustls_tls()
        .no_proxy()
        .build()
        .expect("reqwest client");
    let provider = Arc::new(StaticProvider("dms-lease-token".into()));
    let client = HttpDomainClient::with_provider(http, provider);

    let tempdir = tempfile::tempdir().unwrap();
    let mut job = build_job(&tempdir, &server.base_url());
    // Set a legacy token in metadata; client should not use it
    job.meta.access_token = "legacy-placeholder".into();

    let manifest_path = job.job_path.join("job_manifest.json");
    std::fs::create_dir_all(job.job_path.clone()).unwrap();
    std::fs::write(&manifest_path, b"{}\n").unwrap();

    client
        .upload_manifest(&mut job, &manifest_path)
        .await
        .expect("manifest upload succeeds");
}

#[derive(Clone)]
struct SwitchingProvider(Arc<Mutex<String>>);

#[async_trait]
impl DomainTokenProvider for SwitchingProvider {
    async fn bearer(&self) -> Option<String> {
        Some(self.0.lock().expect("token mutex").clone())
    }
}

#[tokio::test]
async fn http_client_uses_updated_token_on_subsequent_requests() {
    let server = MockServer::start_async().await;

    // First request must use token-1 via POST
    let post_mock = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/api/v1/domains/dom1/data")
                .header("authorization", "Bearer token-1");
            then.status(200).json_body(serde_json::json!({
                "data": [{
                    "id": "data-1",
                    "domain_id": "dom1",
                    "name": "refined_manifest",
                    "data_type": "refined_manifest_json"
                }]
            }));
        })
        .await;

    // Second request (update) must use token-2 via PUT
    let put_mock = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::PUT)
                .path("/api/v1/domains/dom1/data")
                .header("authorization", "Bearer token-2");
            then.status(200).json_body(serde_json::json!({
                "data": [{
                    "id": "data-1",
                    "domain_id": "dom1",
                    "name": "refined_manifest",
                    "data_type": "refined_manifest_json"
                }]
            }));
        })
        .await;

    // Build client with switching provider
    let http = Client::builder()
        .use_rustls_tls()
        .no_proxy()
        .build()
        .expect("reqwest client");
    let token_cell = Arc::new(Mutex::new(String::from("token-1")));
    let provider = Arc::new(SwitchingProvider(token_cell.clone()));
    let client = HttpDomainClient::with_provider(http, provider);

    let tempdir = tempfile::tempdir().unwrap();
    let mut job = build_job(&tempdir, &server.base_url());
    job.meta.access_token = "legacy-unused".into();

    let manifest_path = job.job_path.join("job_manifest.json");
    std::fs::create_dir_all(job.job_path.clone()).unwrap();
    std::fs::write(&manifest_path, b"{}\n").unwrap();

    // First upload: should hit POST mock with token-1
    client
        .upload_manifest(&mut job, &manifest_path)
        .await
        .expect("first upload");

    // Switch token for subsequent request
    *token_cell.lock().expect("token mutex") = String::from("token-2");

    // Second upload: should hit PUT mock with token-2
    client
        .upload_manifest(&mut job, &manifest_path)
        .await
        .expect("second upload");

    // Optional: ensure both mocks were exercised at least once
    post_mock.assert();
    put_mock.assert();
}
