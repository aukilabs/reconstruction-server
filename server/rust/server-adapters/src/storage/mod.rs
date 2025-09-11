use regex::Regex;
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
        Self {
            client: Client::builder().build().unwrap(),
        }
    }
}

impl HttpDomainClient {
    fn auth(&self, job: &Job) -> String {
        format!("Bearer {}", job.meta.access_token)
    }
    fn build_multipart(
        &self,
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
}

#[async_trait::async_trait]
impl DomainPort for HttpDomainClient {
    async fn upload_manifest(&self, job: &Job, manifest_path: &std::path::Path) -> Result<()> {
        let url = format!(
            "{}/api/v1/domains/{}/data",
            job.meta.domain_server_url, job.meta.domain_id
        );
        let bytes = fs::read(manifest_path).map_err(DomainError::Io)?;
        let name_suffix = job.meta.created_at.format("%Y-%m-%d_%H-%M-%S").to_string();
        let name = format!("{}_{}", REFINED_MANIFEST_DATA_NAME, name_suffix);
        let (body, ctype) = self.build_multipart(
            &name,
            REFINED_MANIFEST_DATA_TYPE,
            &job.meta.domain_id,
            None,
            &bytes,
        );
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
            if resp.status().as_u16() == 409 {
                return Ok(());
            }
            return Err(DomainError::Http(format!("status {}", resp.status())));
        }
        Ok(())
    }

    async fn upload_output(&self, job: &Job, output: &ExpectedOutput) -> Result<()> {
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
        let (body, ctype) = self.build_multipart(
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
            return Err(DomainError::Http(format!("status {}", resp.status())));
        }
        Ok(())
    }

    async fn download_data_batch(
        &self,
        job: &Job,
        ids: &[String],
        datasets_root: &std::path::Path,
    ) -> Result<()> {
        fs::create_dir_all(datasets_root).map_err(DomainError::Io)?;
        if ids.is_empty() {
            return Ok(());
        }
        let ids_param = ids.join(",");
        let url = format!(
            "{}/api/v1/domains/{}/data?ids={}",
            job.meta.domain_server_url, job.meta.domain_id, ids_param
        );
        let resp = self
            .client
            .get(&url)
            .header("Authorization", self.auth(job))
            .header("Accept", "multipart/form-data")
            .send()
            .await
            .map_err(|e| DomainError::Http(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(DomainError::Http(format!("status {}", resp.status())));
        }
        let ct = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let boundary = parse_boundary(&ct)
            .ok_or_else(|| DomainError::Internal("missing multipart boundary".into()))?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| DomainError::Http(e.to_string()))?
            .to_vec();
        let parts = parse_multipart_bytes(&bytes, &boundary)?;
        for p in parts {
            let disp = p
                .headers
                .get("content-disposition")
                .cloned()
                .unwrap_or_default();
            let params = parse_disposition_params(&disp);
            let name = params.get("name").cloned().unwrap_or_default();
            let data_type = params.get("data-type").cloned().unwrap_or_default();
            let scan_folder = extract_timestamp(&name).unwrap_or_else(|| name.clone());
            let file_name = map_filename(&data_type, &name);
            let scan_dir = datasets_root.join(&scan_folder);
            fs::create_dir_all(&scan_dir).map_err(DomainError::Io)?;
            let file_path = scan_dir.join(&file_name);
            fs::write(&file_path, &p.body).map_err(DomainError::Io)?;
            if file_name == "RefinedScan.zip" {
                let unzip_path = job
                    .job_path
                    .join("refined")
                    .join("local")
                    .join(&scan_folder)
                    .join("sfm");
                fs::create_dir_all(&unzip_path).map_err(DomainError::Io)?;
                let mut ar = ZipArchive::new(Cursor::new(&p.body))
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
        let url = format!(
            "{}/api/v1/domains/{}/data",
            job.meta.domain_server_url, job.meta.domain_id
        );
        let name = format!("refined_scan_{}", scan_id);
        let (body, ctype) = self.build_multipart(
            &name,
            "refined_scan_zip",
            &job.meta.domain_id,
            None,
            &zip_bytes,
        );
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
            return Err(DomainError::Http(format!("status {}", resp.status())));
        }
        Ok(())
    }
}

// ---- multipart helpers ----

fn parse_boundary(ct: &str) -> Option<String> {
    ct.split(';').find_map(|part| {
        let p = part.trim();
        p.strip_prefix("boundary=")
            .map(|b| b.trim_matches('"').to_string())
    })
}

struct PartData {
    headers: std::collections::HashMap<String, String>,
    body: Vec<u8>,
}

fn parse_multipart_bytes(body: &[u8], boundary: &str) -> Result<Vec<PartData>> {
    let mut parts = Vec::new();
    let marker = format!("--{}", boundary).into_bytes();
    let mut pos = 0usize;
    while pos + marker.len() <= body.len() && &body[pos..pos + marker.len()] != marker.as_slice() {
        pos += 1
    }
    while pos + marker.len() <= body.len() {
        if &body[pos..pos + marker.len()] != marker.as_slice() {
            break;
        }
        pos += marker.len();
        if body.get(pos..pos + 2) == Some(b"--") {
            break;
        }
        if body.get(pos..pos + 2) == Some(b"\r\n") {
            pos += 2;
        }
        let mut headers = std::collections::HashMap::new();
        loop {
            if body.get(pos..pos + 2) == Some(b"\r\n") {
                pos += 2;
                break;
            }
            let mut end = pos;
            while end + 1 < body.len() && &body[end..end + 2] != b"\r\n" {
                end += 1;
            }
            if end + 1 >= body.len() {
                break;
            }
            let line = &body[pos..end];
            if let Ok(line_str) = std::str::from_utf8(line) {
                if let Some((k, v)) = line_str.split_once(":") {
                    headers.insert(k.to_ascii_lowercase(), v.trim().to_string());
                }
            }
            pos = end + 2;
        }
        let mut end = pos;
        loop {
            if end + marker.len() <= body.len()
                && &body[end..end + marker.len()] == marker.as_slice()
            {
                break;
            }
            end += 1;
            if end >= body.len() {
                break;
            }
        }
        let mut part_end = end;
        if part_end >= 2 && &body[part_end - 2..part_end] == b"\r\n" {
            part_end -= 2;
        }
        let part = body[pos..part_end].to_vec();
        parts.push(PartData {
            headers,
            body: part,
        });
        pos = end;
    }
    Ok(parts)
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
