use crate::dds::persist::{read_node_secret_from_path, write_node_secret_to_path};
use axum::extract::State;
use axum::http::{header::USER_AGENT, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{debug, info, warn};

use super::state::touch_healthcheck_now;

#[derive(Clone)]
pub struct DdsState {
    pub secret_path: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct RegistrationCallbackRequest {
    pub id: String,
    pub secret: String,
    pub organization_id: Option<String>,
    pub lighthouses_in_domains: Option<serde_json::Value>,
    pub domains: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct OkResponse {
    ok: bool,
}

#[derive(Debug)]
enum CallbackError {
    Unprocessable(&'static str), // 422
    Forbidden(&'static str),     // 403
    Conflict(&'static str),      // 409
}

impl IntoResponse for CallbackError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match self {
            CallbackError::Unprocessable(m) => (StatusCode::UNPROCESSABLE_ENTITY, m),
            CallbackError::Forbidden(m) => (StatusCode::FORBIDDEN, m),
            CallbackError::Conflict(m) => (StatusCode::CONFLICT, m),
        };
        (status, msg).into_response()
    }
}

pub fn router_dds(state: DdsState) -> axum::Router {
    axum::Router::new()
        .route("/internal/v1/registrations", post(callback_registration))
        .route("/health", get(health))
        .with_state(state)
}

async fn health(State(_state): State<DdsState>, headers: HeaderMap) -> impl IntoResponse {
    let ua = headers
        .get(USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if ua.starts_with("DDS v") {
        match touch_healthcheck_now() {
            Ok(()) => debug!(event = "healthcheck.touch", user_agent = ua, "last_healthcheck updated via /health"),
            Err(e) => warn!(event = "healthcheck.touch.error", user_agent = ua, error = %e, "failed to update last_healthcheck"),
        }
    } else {
        debug!(event = "healthcheck.skip", user_agent = ua, "health check not from DDS; not updating last_healthcheck");
    }
    StatusCode::OK
}

async fn callback_registration(
    State(state): State<DdsState>,
    Json(payload): Json<RegistrationCallbackRequest>,
) -> Result<Json<OkResponse>, CallbackError> {
    // Basic shape validation
    if payload.id.trim().is_empty() {
        return Err(CallbackError::Unprocessable("missing id"));
    }
    if payload.secret.trim().is_empty() {
        return Err(CallbackError::Unprocessable("missing secret"));
    }

    // Optional: enforce some maximum size to avoid abuse
    if payload.secret.len() > 4096 {
        return Err(CallbackError::Forbidden("secret too large"));
    }

    // Log without exposing sensitive secret
    let secret_len = payload.secret.len();
    let org = payload.organization_id.as_deref().unwrap_or("");
    info!(id = %payload.id, org = %org, secret_len = secret_len, "Received registration callback");

    // Persist atomically
    write_node_secret_to_path(&state.secret_path, &payload.secret)
        .map_err(|_| CallbackError::Conflict("persist failed"))?;

    // Sanity read-back (optional; not exposing value)
    match read_node_secret_from_path(&state.secret_path) {
        Ok(Some(_)) => {}
        Ok(None) => {
            warn!("persisted secret missing after write");
            return Err(CallbackError::Conflict("persist verify failed"));
        }
        Err(_) => {
            return Err(CallbackError::Conflict("persist verify failed"));
        }
    }

    Ok(Json(OkResponse { ok: true }))
}
