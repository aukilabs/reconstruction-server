use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rand::distributions::{Distribution, Uniform};
use rand::Rng;
use serde_json::Value;

use super::models::LeaseResponse;
use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    Pending,
    Running,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionState {
    pub task_id: String,
    pub capability: String,
    pub meta: Value,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub access_token: Option<String>,
    pub access_token_expires_at: Option<DateTime<Utc>>,
    pub last_progress: Option<Value>,
    pub next_heartbeat_due: Option<Instant>,
    pub status: SessionStatus,
}

impl SessionState {
    fn new(
        task_id: String,
        capability: String,
        meta: Value,
        lease_expires_at: Option<DateTime<Utc>>,
        access_token: Option<String>,
        access_token_expires_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            task_id,
            capability,
            meta,
            lease_expires_at,
            access_token,
            access_token_expires_at,
            last_progress: None,
            next_heartbeat_due: None,
            status: SessionStatus::Pending,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionSnapshot(SessionState);

impl SessionSnapshot {
    pub fn task_id(&self) -> &str {
        &self.0.task_id
    }

    pub fn capability(&self) -> &str {
        &self.0.capability
    }

    pub fn meta(&self) -> &Value {
        &self.0.meta
    }

    pub fn access_token(&self) -> Option<&str> {
        self.0.access_token.as_deref()
    }

    pub fn access_token_expires_at(&self) -> Option<DateTime<Utc>> {
        self.0.access_token_expires_at
    }

    pub fn lease_expires_at(&self) -> Option<DateTime<Utc>> {
        self.0.lease_expires_at
    }

    pub fn next_heartbeat_due(&self) -> Option<Instant> {
        self.0.next_heartbeat_due
    }

    pub fn status(&self) -> SessionStatus {
        self.0.status
    }
}

/// Provides access to short-lived credentials and identifiers exposed with
/// each lease/heartbeat cycle. Compute nodes should treat these values as
/// ephemeral and refresh them whenever the session is updated.
pub trait SessionCredentials {
    fn access_token(&self) -> Option<&str>;
    fn capability(&self) -> &str;
}

impl SessionCredentials for SessionSnapshot {
    fn access_token(&self) -> Option<&str> {
        self.access_token()
    }

    fn capability(&self) -> &str {
        self.capability()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilitySelector {
    capabilities: Vec<String>,
}

impl CapabilitySelector {
    pub fn new(capabilities: Vec<String>) -> Self {
        Self { capabilities }
    }

    pub fn choose(&self) -> Option<&str> {
        self.capabilities.first().map(|s| s.as_str())
    }

    pub fn accepts(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }

    pub fn all(&self) -> &[String] {
        &self.capabilities
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HeartbeatPolicy {
    pub min_ratio: f64,
    pub max_ratio: f64,
}

impl HeartbeatPolicy {
    pub const fn new(min_ratio: f64, max_ratio: f64) -> Self {
        Self {
            min_ratio,
            max_ratio,
        }
    }

    pub const fn default_policy() -> Self {
        Self {
            min_ratio: 0.55,
            max_ratio: 0.65,
        }
    }

    fn sample_ratio<R: Rng>(&self, rng: &mut R) -> f64 {
        let min = self.min_ratio.min(self.max_ratio).max(0.0);
        let max = self.max_ratio.max(self.min_ratio).max(min);
        let dist = Uniform::new_inclusive(min, max);
        dist.sample(rng)
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SessionError {
    #[error("lease did not include a task")]
    MissingTask,
    #[error("task capability `{got}` not in configured set {expected:?}")]
    CapabilityMismatch { expected: Vec<String>, got: String },
    #[error("no active session")]
    NoActiveSession,
}

#[derive(Clone)]
pub struct SessionManager {
    selector: CapabilitySelector,
    state: Arc<tokio::sync::Mutex<Option<SessionState>>>,
}

impl SessionManager {
    pub fn new(selector: CapabilitySelector) -> Self {
        Self {
            selector,
            state: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub fn selector(&self) -> &CapabilitySelector {
        &self.selector
    }

    pub async fn snapshot(&self) -> Option<SessionSnapshot> {
        let guard = self.state.lock().await;
        guard.as_ref().cloned().map(SessionSnapshot)
    }

    pub async fn clear(&self) {
        *self.state.lock().await = None;
    }

    pub async fn start_session<R: Rng>(
        &self,
        lease: &LeaseResponse,
        now: Instant,
        policy: &HeartbeatPolicy,
        rng: &mut R,
    ) -> Result<SessionSnapshot, SessionError> {
        let task = lease.task.clone().ok_or(SessionError::MissingTask)?;
        if !self.selector.accepts(&task.capability) {
            return Err(SessionError::CapabilityMismatch {
                expected: self.selector.all().to_vec(),
                got: task.capability,
            });
        }

        let access_token = lease.access_token.clone();
        let access_token_expires_at = parse_timestamp(lease.access_token_expires_at.as_deref());
        let lease_expires_at = parse_timestamp(lease.lease_expires_at.as_deref());

        let mut new_state = SessionState::new(
            task.id,
            task.capability,
            task.meta,
            lease_expires_at,
            access_token,
            access_token_expires_at,
        );
        new_state.next_heartbeat_due =
            compute_next_heartbeat(now, new_state.lease_expires_at, policy, rng);

        *self.state.lock().await = Some(new_state.clone());
        Ok(SessionSnapshot(new_state))
    }

    pub async fn apply_heartbeat<R: Rng>(
        &self,
        lease: &LeaseResponse,
        progress: Option<Value>,
        now: Instant,
        policy: &HeartbeatPolicy,
        rng: &mut R,
    ) -> Result<SessionSnapshot, SessionError> {
        let mut guard = self.state.lock().await;
        let state = guard.as_mut().ok_or(SessionError::NoActiveSession)?;

        if let Some(task) = lease.task.clone() {
            state.task_id = task.id;
            state.capability = task.capability;
            state.meta = task.meta;
        }

        if let Some(token) = lease.access_token.clone() {
            state.access_token = Some(token);
        }
        if let Some(expiry) = parse_timestamp(lease.access_token_expires_at.as_deref()) {
            state.access_token_expires_at = Some(expiry);
        }
        if let Some(lease_expiry) = parse_timestamp(lease.lease_expires_at.as_deref()) {
            state.lease_expires_at = Some(lease_expiry);
        }

        state.last_progress = progress;
        state.status = SessionStatus::Running;
        state.next_heartbeat_due = compute_next_heartbeat(now, state.lease_expires_at, policy, rng);

        Ok(SessionSnapshot(state.clone()))
    }
}

fn parse_timestamp(raw: Option<&str>) -> Option<DateTime<Utc>> {
    raw.and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

fn compute_next_heartbeat<R: Rng>(
    now: Instant,
    lease_expires_at: Option<DateTime<Utc>>,
    policy: &HeartbeatPolicy,
    rng: &mut R,
) -> Option<Instant> {
    let expires = lease_expires_at?;
    let ttl = expires.signed_duration_since(Utc::now());
    if ttl.num_milliseconds() <= 0 {
        return Some(now);
    }
    let ttl = ttl.to_std().ok()?;
    let ratio = policy.sample_ratio(rng).clamp(0.0, 1.0);
    let mut delay = ttl.mul_f64(ratio);
    if delay > ttl {
        delay = ttl;
    }
    if delay.is_zero() {
        delay = Duration::from_millis(100);
    }
    Some(now + delay.min(ttl))
}

// --- Domain token provider implementation ---

#[async_trait]
impl crate::storage::DomainTokenProvider for SessionManager {
    async fn bearer(&self) -> Option<String> {
        self.snapshot()
            .await
            .and_then(|s| s.access_token().map(|t| t.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::DomainTokenProvider;
    use chrono::{Duration as ChronoDuration, Utc};
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use serde_json::json;
    use tokio::time::timeout;

    fn selector() -> CapabilitySelector {
        CapabilitySelector::new(vec!["cap-a".to_string(), "cap-b".to_string()])
    }

    fn policy() -> HeartbeatPolicy {
        HeartbeatPolicy::default_policy()
    }

    fn timestamp_in(minutes: i64) -> String {
        (Utc::now() + ChronoDuration::minutes(minutes)).to_rfc3339()
    }

    #[test]
    fn choose_returns_first_capability() {
        let selector = selector();
        assert_eq!(selector.choose(), Some("cap-a"));
    }

    #[test]
    fn choose_none_when_empty() {
        let selector = CapabilitySelector::new(vec![]);
        assert_eq!(selector.choose(), None);
    }

    #[test]
    fn accepts_returns_true_for_matching_capability() {
        let selector = CapabilitySelector::new(vec!["cap-a".to_string()]);
        assert!(selector.accepts("cap-a"));
        assert!(!selector.accepts("other"));
    }

    #[tokio::test]
    async fn start_session_rejects_missing_task() {
        let manager = SessionManager::new(selector());
        let lease = LeaseResponse::default();
        let err = manager
            .start_session(
                &lease,
                Instant::now(),
                &policy(),
                &mut StdRng::seed_from_u64(1),
            )
            .await
            .unwrap_err();
        assert_eq!(err, SessionError::MissingTask);
    }

    #[tokio::test]
    async fn start_session_rejects_mismatched_capability() {
        let manager = SessionManager::new(selector());
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "123".into(),
                capability: "other".into(),
                meta: serde_json::json!({}),
            }),
            ..LeaseResponse::default()
        };

        let err = manager
            .start_session(
                &lease,
                Instant::now(),
                &policy(),
                &mut StdRng::seed_from_u64(2),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, SessionError::CapabilityMismatch { .. }));
    }

    #[tokio::test]
    async fn start_session_persists_state_and_next_heartbeat() {
        let manager = SessionManager::new(selector());
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "123".into(),
                capability: "cap-a".into(),
                meta: serde_json::json!({"foo": "bar"}),
            }),
            access_token: Some("token".into()),
            access_token_expires_at: Some(timestamp_in(60)),
            lease_expires_at: Some(timestamp_in(5)),
            ..LeaseResponse::default()
        };

        let now = Instant::now();
        let snapshot = manager
            .start_session(&lease, now, &policy(), &mut StdRng::seed_from_u64(3))
            .await
            .expect("session");

        assert_eq!(snapshot.task_id(), "123");
        assert_eq!(snapshot.capability(), "cap-a");
        assert_eq!(snapshot.access_token(), Some("token"));
        assert!(snapshot.access_token_expires_at().is_some());
        assert!(snapshot.lease_expires_at().is_some());
        assert!(snapshot.next_heartbeat_due().is_some());
        assert_eq!(snapshot.status(), SessionStatus::Pending);
    }

    #[tokio::test]
    async fn apply_heartbeat_marks_running_and_updates_schedule() {
        let manager = SessionManager::new(selector());
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "123".into(),
                capability: "cap-a".into(),
                meta: serde_json::json!({}),
            }),
            lease_expires_at: Some(timestamp_in(5)),
            ..LeaseResponse::default()
        };
        let now = Instant::now();
        let mut rng = StdRng::seed_from_u64(4);
        manager
            .start_session(&lease, now, &policy(), &mut rng)
            .await
            .expect("session");

        let mut rng = StdRng::seed_from_u64(5);
        let heartbeat = LeaseResponse {
            lease_expires_at: Some(timestamp_in(6)),
            access_token: Some("new-token".into()),
            ..LeaseResponse::default()
        };
        let snapshot = manager
            .apply_heartbeat(&heartbeat, None, now, &policy(), &mut rng)
            .await
            .expect("heartbeat");
        assert_eq!(snapshot.status(), SessionStatus::Running);
        assert_eq!(snapshot.access_token(), Some("new-token"));
        assert!(snapshot.next_heartbeat_due().is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn session_manager_handles_concurrent_updates() {
        let manager = SessionManager::new(selector());
        let base_lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "task-1".into(),
                capability: "cap-a".into(),
                meta: json!({}),
            }),
            access_token: Some("token".into()),
            access_token_expires_at: Some(timestamp_in(60)),
            lease_expires_at: Some(timestamp_in(5)),
            ..LeaseResponse::default()
        };
        let handles: Vec<_> = (0..8)
            .map(|idx| {
                let manager = manager.clone();
                let lease = base_lease.clone();
                tokio::spawn(async move {
                    let policy = policy();
                    let mut rng = StdRng::seed_from_u64(100 + idx as u64);
                    for iter in 0..200 {
                        manager
                            .start_session(&lease, Instant::now(), &policy, &mut rng)
                            .await
                            .unwrap();
                        let heartbeat = LeaseResponse {
                            lease_expires_at: Some(timestamp_in(5)),
                            access_token: Some(format!("token-{}-{}", idx, iter)),
                            ..LeaseResponse::default()
                        };
                        manager
                            .apply_heartbeat(&heartbeat, None, Instant::now(), &policy, &mut rng)
                            .await
                            .unwrap();
                        if iter % 25 == 0 {
                            let _ = manager.snapshot().await;
                        }
                    }
                    Ok::<(), SessionError>(())
                })
            })
            .collect();

        for handle in handles {
            timeout(std::time::Duration::from_secs(5), handle)
                .await
                .expect("manager task timed out")
                .expect("task join")
                .expect("manager operation");
        }

        manager.clear().await;
        assert!(manager.snapshot().await.is_none());
    }

    #[tokio::test]
    async fn session_manager_provides_and_updates_domain_token() {
        let manager = SessionManager::new(selector());
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "task-xyz".into(),
                capability: "cap-a".into(),
                meta: json!({}),
            }),
            access_token: Some("token".into()),
            access_token_expires_at: Some(timestamp_in(60)),
            lease_expires_at: Some(timestamp_in(5)),
            ..LeaseResponse::default()
        };
        let mut rng = StdRng::seed_from_u64(123);
        manager
            .start_session(&lease, Instant::now(), &policy(), &mut rng)
            .await
            .expect("session");

        // Provider returns initial lease token
        let t1 = manager.bearer().await;
        assert_eq!(t1.as_deref(), Some("token"));

        // Heartbeat updates token
        let heartbeat = LeaseResponse {
            access_token: Some("new-token".into()),
            lease_expires_at: Some(timestamp_in(6)),
            ..LeaseResponse::default()
        };
        let mut rng = StdRng::seed_from_u64(124);
        manager
            .apply_heartbeat(&heartbeat, None, Instant::now(), &policy(), &mut rng)
            .await
            .expect("heartbeat");

        let t2 = manager.bearer().await;
        assert_eq!(t2.as_deref(), Some("new-token"));
    }
}
