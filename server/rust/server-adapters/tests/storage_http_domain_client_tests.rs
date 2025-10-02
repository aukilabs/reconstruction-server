use std::{collections::HashMap, fs, io::Write};

use httpmock::MockServer;
use reqwest::Client;
use server_adapters::storage::HttpDomainClient;
use server_core::{DomainPort, Job, JobMetadata};
use std::io::Cursor;

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
