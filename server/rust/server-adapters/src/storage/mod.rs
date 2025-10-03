use multer::Multipart;
use regex::Regex;
#[cfg(test)]
use reqwest::header::HeaderValue;
use reqwest::Client;
use server_core::{
    DomainError, DomainPort, ExpectedOutput, Job, Result, REFINED_MANIFEST_DATA_NAME,
    REFINED_MANIFEST_DATA_TYPE,
};
use std::{
    fs,
    io::{Cursor, Read},
};
use zip::ZipArchive;

pub struct HttpDomainClient {
    client: Client,
}

impl Default for HttpDomainClient {
    fn default() -> Self {
        let client = Client::builder()
            .use_rustls_tls()
            .no_proxy()
            .build()
            .expect("http client");
        Self::new(client)
    }
}

impl HttpDomainClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl HttpDomainClient {
    async fn map_http_error(resp: reqwest::Response, context: &str) -> DomainError {
        let status = resp.status();
        let url = resp.url().to_string();
        // Try to read body for diagnostics; cap length to avoid noisy logs
        let body = resp
            .text()
            .await
            .unwrap_or_else(|e| format!("<body read error: {}>", e));
        let snippet = if body.len() > 500 {
            &body[..500]
        } else {
            &body
        };
        // If JSON, prefer common keys
        let detail = match serde_json::from_str::<serde_json::Value>(snippet) {
            Ok(v) => v
                .get("error")
                .or_else(|| v.get("message"))
                .and_then(|x| x.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| snippet.to_string()),
            Err(_) => snippet.to_string(),
        };
        let msg = format!("{}: status {} url={} body={}", context, status, url, detail);
        match status.as_u16() {
            400 => DomainError::BadRequest(msg),
            401 => DomainError::Unauthorized,
            404 => DomainError::NotFound(msg),
            409 => DomainError::Conflict(msg),
            _ => DomainError::Http(msg),
        }
    }

    fn auth(&self, job: &Job) -> String {
        format!("Bearer {}", job.meta.access_token)
    }
    fn build_multipart(
        name: &str,
        data_type: &str,
        domain_id: &str,
        id: Option<&str>,
        bytes: &[u8],
    ) -> (Vec<u8>, String) {
        let boundary = format!("------------------------{}", uuid::Uuid::new_v4().simple());
        let mut body: Vec<u8> = Vec::new();
        let disp = if let Some(id) = id {
            format!(
                "Content-Disposition: form-data; name=\"{}\"; data-type=\"{}\"; id=\"{}\"; domain-id=\"{}\"\r\n",
                name, data_type, id, domain_id
            )
        } else {
            format!(
                "Content-Disposition: form-data; name=\"{}\"; data-type=\"{}\"; domain-id=\"{}\"\r\n",
                name, data_type, domain_id
            )
        };
        let header = format!(
            "--{}\r\nContent-Type: application/octet-stream\r\n{}\r\n",
            boundary, disp
        );
        body.extend_from_slice(header.as_bytes());
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
        body.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());
        let ctype = format!("multipart/form-data; boundary={}", boundary);
        (body, ctype)
    }

    #[cfg(test)]
    fn content_disposition_value(
        name: &str,
        data_type: &str,
        domain_id: &str,
        id: Option<&str>,
    ) -> Result<HeaderValue> {
        let mut disposition = format!(
            "form-data; name=\"{}\"; data-type=\"{}\"; domain-id=\"{}\"",
            name, data_type, domain_id
        );
        if let Some(id) = id {
            disposition.push_str(&format!("; id=\"{}\"", id));
        }
        HeaderValue::from_str(&disposition)
            .map_err(|_| DomainError::Internal("invalid content disposition".into()))
    }
}

#[async_trait::async_trait]
impl DomainPort for HttpDomainClient {
    async fn upload_manifest(&self, job: &mut Job, manifest_path: &std::path::Path) -> Result<()> {
        use tracing::{debug, info, warn};
        let url = format!(
            "{}/api/v1/domains/{}/data",
            job.meta.domain_server_url, job.meta.domain_id
        );
        let bytes = fs::read(manifest_path).map_err(DomainError::Io)?;
        let name_suffix = if job.meta.override_job_name.is_empty() {
            job.meta.created_at.format("%Y-%m-%d_%H-%M-%S").to_string()
        } else {
            job.meta.override_job_name.clone()
        };
        let name = format!("{}_{}", REFINED_MANIFEST_DATA_NAME, name_suffix);
        // Use stored ID if present to drive PUT updates
        let key = format!(
            "{}.{}",
            REFINED_MANIFEST_DATA_NAME, REFINED_MANIFEST_DATA_TYPE
        );
        let existing_id = job.uploaded_data_ids.get(&key).cloned();
        let (body, ctype) = Self::build_multipart(
            &name,
            REFINED_MANIFEST_DATA_TYPE,
            &job.meta.domain_id,
            existing_id.as_deref(),
            &bytes,
        );
        let method = if existing_id.is_some() {
            reqwest::Method::PUT
        } else {
            reqwest::Method::POST
        };
        let resp = self
            .client
            .request(method, &url)
            .header("Authorization", self.auth(job))
            .header("Content-Type", ctype)
            .body(body)
            .send()
            .await
            .map_err(|e| DomainError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            if resp.status().as_u16() == 409 {
                warn!("manifest upload conflict (409); assuming already exists");
                return Ok(());
            }
            return Err(Self::map_http_error(resp, "upload_manifest").await);
        }
        let text = resp
            .text()
            .await
            .map_err(|e| DomainError::Http(e.to_string()))?;
        if let Ok(parsed) = serde_json::from_str::<server_core::PostDomainDataResponse>(&text) {
            if let Some(first) = parsed.data.first() {
                debug!("Uploaded manifest, received ID: {}", first.id);
                job.uploaded_data_ids.insert(key, first.id.clone());
                info!(job_id = %job.meta.id, domain_id = %job.meta.domain_id, data_id = %first.id, "Uploaded/updated job manifest in domain");
            }
        }
        Ok(())
    }

    async fn upload_output(&self, job: &Job, output: &ExpectedOutput) -> Result<()> {
        use tracing::debug;
        if !output.file_path.exists() {
            if output.optional {
                return Ok(());
            }
            return Err(DomainError::NotFound(format!(
                "missing output: {}",
                output.file_path.display()
            )));
        }
        let url = format!(
            "{}/api/v1/domains/{}/data",
            job.meta.domain_server_url, job.meta.domain_id
        );
        let bytes = fs::read(&output.file_path).map_err(DomainError::Io)?;
        let name_suffix = if job.meta.override_job_name.is_empty() {
            job.meta.created_at.format("%Y-%m-%d_%H-%M-%S").to_string()
        } else {
            job.meta.override_job_name.clone()
        };
        let name = format!("{}_{}", output.name, name_suffix);
        let existing = job
            .uploaded_data_ids
            .get(&format!("{}.{}", output.name, output.data_type))
            .map(|s| s.as_str());
        let (body, ctype) = Self::build_multipart(
            &name,
            &output.data_type,
            &job.meta.domain_id,
            existing,
            &bytes,
        );
        let method = if existing.is_some() {
            reqwest::Method::PUT
        } else {
            reqwest::Method::POST
        };
        debug!(
            url = %url,
            method = %method,
            name = %name,
            data_type = %output.data_type,
            has_existing_id = existing.is_some(),
            "Uploading output"
        );
        let resp = self
            .client
            .request(method, &url)
            .header("Authorization", self.auth(job))
            .header("Content-Type", ctype)
            .body(body)
            .send()
            .await
            .map_err(|e| DomainError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            if output.optional {
                return Ok(());
            }
            return Err(Self::map_http_error(resp, "upload_output").await);
        }
        Ok(())
    }

    async fn download_data_batch(
        &self,
        job: &Job,
        ids: &[String],
        datasets_root: &std::path::Path,
    ) -> Result<()> {
        use tracing::debug;
        fs::create_dir_all(datasets_root).map_err(DomainError::Io)?;
        if ids.is_empty() {
            return Ok(());
        }
        let ids_param = ids.join(",");
        let url = format!(
            "{}/api/v1/domains/{}/data?ids={}",
            job.meta.domain_server_url, job.meta.domain_id, ids_param
        );
        debug!(url = %url, ids_count = ids.len(), "Downloading data batch");
        let resp = self
            .client
            .get(&url)
            .header("Authorization", self.auth(job))
            .header("Accept", "multipart/form-data")
            .send()
            .await
            .map_err(|e| DomainError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Self::map_http_error(
                resp,
                &format!("download_data_batch(ids_count={})", ids.len()),
            )
            .await);
        }
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| DomainError::Internal("missing content-type header".into()))?;
        let boundary = multer::parse_boundary(content_type)
            .map_err(|e| DomainError::Internal(e.to_string()))?;
        let mut multipart = Multipart::new(resp.bytes_stream(), boundary);
        while let Some(mut field) = multipart
            .next_field()
            .await
            .map_err(|e| DomainError::Internal(e.to_string()))?
        {
            let disposition = field
                .headers()
                .get("content-disposition")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let params = parse_disposition_params(disposition);
            let name = params.get("name").cloned().unwrap_or_default();
            let data_type = params.get("data-type").cloned().unwrap_or_default();
            let mut body = Vec::new();
            while let Some(chunk) = field
                .chunk()
                .await
                .map_err(|e| DomainError::Internal(e.to_string()))?
            {
                body.extend_from_slice(&chunk);
            }
            let scan_folder = extract_timestamp(&name).unwrap_or_else(|| name.clone());
            let file_name = map_filename(&data_type, &name);
            let scan_dir = datasets_root.join(&scan_folder);
            fs::create_dir_all(&scan_dir).map_err(DomainError::Io)?;
            let file_path = scan_dir.join(&file_name);
            fs::write(&file_path, &body).map_err(DomainError::Io)?;
            if file_name == "RefinedScan.zip" {
                let unzip_path = job
                    .job_path
                    .join("refined")
                    .join("local")
                    .join(&scan_folder)
                    .join("sfm");
                fs::create_dir_all(&unzip_path).map_err(DomainError::Io)?;
                let mut ar = ZipArchive::new(Cursor::new(&body))
                    .map_err(|e| DomainError::Internal(e.to_string()))?;
                for i in 0..ar.len() {
                    let mut f = ar
                        .by_index(i)
                        .map_err(|e| DomainError::Internal(e.to_string()))?;
                    if f.is_dir() {
                        continue;
                    }
                    let mut buf = Vec::new();
                    f.read_to_end(&mut buf)
                        .map_err(|e| DomainError::Internal(e.to_string()))?;
                    let out = unzip_path.join(f.name());
                    if let Some(parent) = out.parent() {
                        fs::create_dir_all(parent).map_err(DomainError::Io)?;
                    }
                    fs::write(out, &buf).map_err(DomainError::Io)?;
                }
            }
        }
        Ok(())
    }

    async fn upload_refined_scan_zip(
        &self,
        job: &Job,
        scan_id: &str,
        zip_bytes: Vec<u8>,
    ) -> Result<()> {
        use tracing::debug;
        let url = format!(
            "{}/api/v1/domains/{}/data",
            job.meta.domain_server_url, job.meta.domain_id
        );
        let name = format!("refined_scan_{}", scan_id);
        let (body, ctype) = Self::build_multipart(
            &name,
            "refined_scan_zip",
            &job.meta.domain_id,
            None,
            &zip_bytes,
        );
        debug!(url = %url, scan_id = %scan_id, "Uploading refined scan ZIP");
        let resp = self
            .client
            .post(&url)
            .header("Authorization", self.auth(job))
            .header("Content-Type", ctype)
            .body(body)
            .send()
            .await
            .map_err(|e| DomainError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Self::map_http_error(
                resp,
                &format!("upload_refined_scan_zip(scan_id={})", scan_id),
            )
            .await);
        }
        Ok(())
    }
}

fn parse_disposition_params(disp: &str) -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    for seg in disp.split(';') {
        let s = seg.trim();
        if let Some((k, v)) = s.split_once('=') {
            let v = v.trim_matches('"');
            m.insert(k.to_ascii_lowercase(), v.to_string());
        }
    }
    m
}

fn extract_timestamp(name: &str) -> Option<String> {
    let re = Regex::new(r"\d{4}-\d{2}-\d{2}[_-]\d{2}-\d{2}-\d{2}").ok()?;
    re.find(name).map(|m| m.as_str().to_string())
}

fn map_filename(data_type: &str, name: &str) -> String {
    match data_type {
        "dmt_manifest_json" => "Manifest.json".into(),
        "dmt_featurepoints_ply" | "dmt_pointcloud_ply" => "FeaturePoints.ply".into(),
        "dmt_arposes_csv" => "ARposes.csv".into(),
        "dmt_portal_detections_csv" | "dmt_observations_csv" => "PortalDetections.csv".into(),
        "dmt_intrinsics_csv" | "dmt_cameraintrinsics_csv" => "CameraIntrinsics.csv".into(),
        "dmt_frames_csv" => "Frames.csv".into(),
        "dmt_gyro_csv" => "Gyro.csv".into(),
        "dmt_accel_csv" => "Accel.csv".into(),
        "dmt_gyroaccel_csv" => "gyro_accel.csv".into(),
        "dmt_recording_mp4" => "Frames.mp4".into(),
        "refined_scan_zip" => "RefinedScan.zip".into(),
        _ => format!("{}.{}", name, data_type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_disposition_without_id_matches_expected() {
        let header = HttpDomainClient::content_disposition_value(
            "manifest",
            "dmt_manifest_json",
            "domain-123",
            None,
        )
        .expect("header");
        assert_eq!(
            header.to_str().unwrap(),
            "form-data; name=\"manifest\"; data-type=\"dmt_manifest_json\"; domain-id=\"domain-123\""
        );
    }

    #[test]
    fn content_disposition_with_id_includes_identifier() {
        let header = HttpDomainClient::content_disposition_value(
            "refined_manifest",
            "dmt_manifest_json",
            "domain-456",
            Some("item-789"),
        )
        .expect("header");
        assert_eq!(
            header.to_str().unwrap(),
            "form-data; name=\"refined_manifest\"; data-type=\"dmt_manifest_json\"; domain-id=\"domain-456\"; id=\"item-789\""
        );
    }

    #[test]
    fn build_multipart_form_debug_contains_metadata() {
        let header = HttpDomainClient::content_disposition_value(
            "manifest",
            "dmt_manifest_json",
            "domain-123",
            None,
        )
        .expect("header");
        let s = header.to_str().unwrap();
        assert!(s.contains("manifest"));
        assert!(s.contains("dmt_manifest_json"));
    }
}
