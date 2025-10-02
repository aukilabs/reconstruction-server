use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct LeaseRequest {
    pub capability: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct LeaseResponse {
    #[serde(default)]
    pub task: Option<TaskSummary>,
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub access_token_expires_at: Option<String>,
    #[serde(default)]
    pub lease_expires_at: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub cancel: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskSummary {
    pub id: String,
    pub capability: String,
    #[serde(default)]
    pub meta: Value,
}

#[derive(Debug, Serialize)]
pub struct HeartbeatRequest<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<&'a Value>,
}

#[derive(Debug, Serialize)]
pub struct CompletionRequest<'a> {
    pub output_cids: &'a [String],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<&'a Value>,
}

#[derive(Debug, Serialize)]
pub struct FailRequest<'a> {
    pub reason: &'a str,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<&'a Value>,
}
