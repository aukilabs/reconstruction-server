use axum::{
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use parking_lot::Mutex;
use server_core::{create_job_metadata, execute_job, Job, JobList, Services};
use std::{path::PathBuf, sync::Arc};
use tracing::{error, info};

#[derive(Clone)]
pub struct AppState {
    pub api_key: Option<String>,
    pub jobs: Arc<Mutex<JobList>>,
    pub job_in_progress: Arc<Mutex<bool>>,
    pub services: Arc<Services>,
    pub cpu_workers: usize,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/jobs", post(post_jobs))
        .route("/jobs", get(get_jobs))
        .with_state(state)
}

/// Returns the combined router containing existing job endpoints and DDS endpoints.
/// Keeps existing /jobs endpoints untouched and merges DDS router under `/internal/...` and `/health`.
pub fn router_with_dds(state: AppState, dds_state: crate::dds::http::DdsState) -> Router {
    // Note: axum requires the same state type to merge routers. Since these
    // routers use different state types, we build two routers and merge them.
    // This is allowed in axum 0.7 as each sub-router carries its own state.
    // If compilation errors arise about differing state types, we can instead
    // expose the merge to the caller (server-bin) as indicated in TODO.
    self::router(state).merge(crate::dds::http::router_dds(dds_state))
}

async fn post_jobs(State(state): State<AppState>, req: Request) -> Response {
    let headers = req.headers().clone();
    if let Some(expected) = &state.api_key {
        let incoming = headers.get("X-API-Key").and_then(|v| v.to_str().ok());
        if incoming != Some(expected.as_str()) {
            return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
        }
    }

    {
        let mut busy = state.job_in_progress.lock();
        if *busy {
            info!("Job already in progress; rejecting new job request");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Reconstruction server is busy processing another job",
            )
                .into_response();
        }
        *busy = true;
    }

    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost")
        .to_string();
    let body_bytes = match to_bytes(req.into_body(), 1024 * 1024 * 50).await {
        Ok(b) => b,
        Err(e) => {
            reset_busy(&state);
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };
    let req_str = match String::from_utf8(body_bytes.to_vec()) {
        Ok(s) => s,
        Err(e) => {
            reset_busy(&state);
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let job = match create_job_metadata(&PathBuf::from("jobs"), &req_str, &host, None) {
        Ok(j) => j,
        Err(e) => {
            reset_busy(&state);
            error!(?e, "job creation failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    {
        let mut jl = state.jobs.lock();
        jl.list.insert(job.meta.id.clone(), job.clone());
    }

    let state_clone = state.clone();
    tokio::spawn(async move {
        let mut job = job;
        let res = execute_job(&state_clone.services, &mut job, state_clone.cpu_workers).await;
        if let Err(e) = res {
            error!(?e, "job failed");
        }
        reset_busy(&state_clone);
        let mut jl = state_clone.jobs.lock();
        jl.list.insert(job.meta.id.clone(), job.clone());
    });

    StatusCode::OK.into_response()
}

fn reset_busy(state: &AppState) {
    *state.job_in_progress.lock() = false;
}

async fn get_jobs(State(state): State<AppState>) -> Response {
    let list = {
        let jl = state.jobs.lock();
        jl.list.values().cloned().collect::<Vec<Job>>()
    };
    let body = match serde_json::to_string_pretty(&list) {
        Ok(mut s) => {
            s.push('\n');
            s
        }
        Err(_) => "[]\n".into(),
    };
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}
