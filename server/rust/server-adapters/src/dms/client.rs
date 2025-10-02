use std::sync::Arc;

use reqwest::{header, StatusCode, Url};
use serde_json::Value;
use tokio::sync::Mutex;

use super::models::{
    CompletionRequest, FailRequest, HeartbeatRequest, LeaseRequest, LeaseResponse,
};

#[derive(Debug, thiserror::Error)]
pub enum DmsClientError {
    #[error("invalid DMS base url `{raw}`: {details}")]
    InvalidBaseUrl { raw: String, details: String },
    #[error("invalid Authorization header: {details}")]
    InvalidIdentity { details: String },
    #[error("failed to build request url `{path}`: {details}")]
    InvalidRequestUrl { path: String, details: String },
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("unexpected status: {0}")]
    UnexpectedStatus(StatusCode),
}

pub type Result<T> = std::result::Result<T, DmsClientError>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LeaseState {
    pub task_id: String,
    pub access_token: Option<String>,
    pub access_token_expires_at: Option<String>,
}

#[derive(Clone)]
pub struct DmsClient {
    base_url: Url,
    node_identity: String,
    auth_header: header::HeaderValue,
    http: reqwest::Client,
    session: Arc<Mutex<Option<LeaseState>>>,
}

impl DmsClient {
    pub fn new(
        base_url: impl AsRef<str>,
        node_identity: String,
        http: reqwest::Client,
    ) -> Result<Self> {
        let base_raw = base_url.as_ref();
        let base_url = Url::parse(base_raw).map_err(|err| DmsClientError::InvalidBaseUrl {
            raw: base_raw.to_string(),
            details: err.to_string(),
        })?;

        let auth_value = format!("Bearer {}", node_identity);
        let mut auth_header = header::HeaderValue::from_str(&auth_value).map_err(|err| {
            DmsClientError::InvalidIdentity {
                details: err.to_string(),
            }
        })?;
        auth_header.set_sensitive(true);

        Ok(Self {
            base_url,
            node_identity,
            auth_header,
            http,
            session: Arc::new(Mutex::new(None)),
        })
    }

    pub fn node_identity(&self) -> &str {
        &self.node_identity
    }

    pub async fn access_token(&self) -> Option<String> {
        self.session
            .lock()
            .await
            .as_ref()
            .and_then(|s| s.access_token.clone())
    }

    pub async fn access_token_expires_at(&self) -> Option<String> {
        self.session
            .lock()
            .await
            .as_ref()
            .and_then(|s| s.access_token_expires_at.clone())
    }

    pub async fn store_session(&self, state: LeaseState) {
        *self.session.lock().await = Some(state);
    }

    pub async fn clear_session(&self) {
        *self.session.lock().await = None;
    }

    pub async fn lease_task(&self, request: &LeaseRequest) -> Result<LeaseResponse> {
        let url = self.join_path("tasks")?;
        let mut query: Vec<(&str, &str)> = Vec::new();
        query.push(("capability", request.capability.as_str()));
        if let Some(job_id) = request.job_id.as_deref() {
            query.push(("job_id", job_id));
        }
        if let Some(domain_id) = request.domain_id.as_deref() {
            query.push(("domain_id", domain_id));
        }

        let response = self
            .http
            .get(url)
            .header(header::AUTHORIZATION, self.auth_header.clone())
            .query(&query)
            .send()
            .await?;

        if response.status() == StatusCode::NO_CONTENT {
            return Ok(LeaseResponse::default());
        }
        if !response.status().is_success() {
            return Err(DmsClientError::UnexpectedStatus(response.status()));
        }

        let body: LeaseResponse = response.json().await?;
        self.update_session(&body).await;
        Ok(body)
    }

    pub async fn send_heartbeat(
        &self,
        task_id: &str,
        progress: Option<&Value>,
    ) -> Result<LeaseResponse> {
        let url = self.join_path(&format!("tasks/{}/heartbeat", task_id))?;
        let response = self
            .http
            .post(url)
            .header(header::AUTHORIZATION, self.auth_header.clone())
            .json(&HeartbeatRequest { progress })
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(DmsClientError::UnexpectedStatus(response.status()));
        }

        let body: LeaseResponse = response.json().await?;
        self.update_session(&body).await;
        Ok(body)
    }

    pub async fn complete_task(
        &self,
        task_id: &str,
        outputs: &[String],
        meta: Option<&Value>,
    ) -> Result<()> {
        let url = self.join_path(&format!("tasks/{}/complete", task_id))?;
        let response = self
            .http
            .post(url)
            .header(header::AUTHORIZATION, self.auth_header.clone())
            .json(&CompletionRequest {
                output_cids: outputs,
                meta,
            })
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(DmsClientError::UnexpectedStatus(response.status()));
        }

        self.clear_session().await;
        Ok(())
    }

    pub async fn fail_task(
        &self,
        task_id: &str,
        reason: &str,
        details: Option<&Value>,
    ) -> Result<()> {
        let url = self.join_path(&format!("tasks/{}/fail", task_id))?;
        let response = self
            .http
            .post(url)
            .header(header::AUTHORIZATION, self.auth_header.clone())
            .json(&FailRequest { reason, details })
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(DmsClientError::UnexpectedStatus(response.status()));
        }

        self.clear_session().await;
        Ok(())
    }

    fn join_path(&self, path: &str) -> Result<Url> {
        self.base_url
            .join(path)
            .map_err(|err| DmsClientError::InvalidRequestUrl {
                path: path.to_string(),
                details: err.to_string(),
            })
    }

    async fn update_session(&self, response: &LeaseResponse) {
        let mut guard = self.session.lock().await;
        match (&response.task, guard.as_mut()) {
            (Some(task), Some(state)) => {
                state.task_id = task.id.clone();
                state.access_token = response.access_token.clone();
                state.access_token_expires_at = response.access_token_expires_at.clone();
            }
            (Some(task), None) => {
                *guard = Some(LeaseState {
                    task_id: task.id.clone(),
                    access_token: response.access_token.clone(),
                    access_token_expires_at: response.access_token_expires_at.clone(),
                });
            }
            (None, Some(state)) => {
                if response.access_token.is_some() || response.access_token_expires_at.is_some() {
                    state.access_token = response.access_token.clone();
                    state.access_token_expires_at = response.access_token_expires_at.clone();
                }
            }
            (None, None) => {}
        }
    }
}

#[cfg(test)]
mod tests {}
