use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

pub const REFINED_MANIFEST_DATA_NAME: &str = "refined_manifest";
pub const REFINED_MANIFEST_DATA_TYPE: &str = "refined_manifest_json";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingType {
    LocalRefinement,
    GlobalRefinement,
    LocalAndGlobalRefinement,
}

impl Default for ProcessingType {
    fn default() -> Self {
        Self::LocalAndGlobalRefinement
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobMetadata {
    pub id: String,
    pub name: String,
    pub domain_id: String,
    pub processing_type: String,
    pub created_at: DateTime<Utc>,
    pub domain_server_url: String,
    pub reconstruction_server_url: String,
    #[serde(skip_serializing)]
    pub access_token: String,
    pub data_ids: Vec<String>,
    pub skip_manifest_upload: bool,
    pub override_job_name: String,
    pub override_manifest_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    #[serde(flatten)]
    pub meta: JobMetadata,
    pub job_path: PathBuf,
    pub status: String,
    pub uploaded_data_ids: HashMap<String, String>,
    pub completed_scans: HashMap<String, bool>,
}

/// Consumers that prepare reconstruction jobs may need to refresh the short-lived
/// bearer token issued with each DMS lease before invoking domain APIs.
///
/// The compute node should call `set_access_token` with the latest session token
/// so downstream domain uploads and downloads observe the correct credential.
pub trait AccessTokenSink {
    fn set_access_token(&mut self, token: impl Into<String>);
}

impl AccessTokenSink for Job {
    fn set_access_token(&mut self, token: impl Into<String>) {
        self.meta.access_token = token.into();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobRequestData {
    pub data_ids: Vec<String>,
    pub domain_id: String,
    pub access_token: String,
    pub processing_type: String,
    // Optional fields in POST /jobs
    pub domain_server_url: Option<String>,
    pub skip_manifest_upload: Option<bool>,
    pub override_job_name: Option<String>,
    pub override_manifest_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EditableDomainDataMetadata {
    pub name: String,
    pub data_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DomainDataMetadata {
    pub id: String,
    pub domain_id: String,
    #[serde(flatten)]
    pub inner: EditableDomainDataMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostDomainDataResponse {
    pub data: Vec<DomainDataMetadata>,
}

#[derive(Clone, Debug)]
pub struct ExpectedOutput {
    pub file_path: PathBuf,
    pub name: String,
    pub data_type: String,
    pub optional: bool,
}
