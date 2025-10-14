use rand::distributions::{Distribution, Uniform};
use rand::{rngs::StdRng, Rng, SeedableRng};
use reqwest::StatusCode;
use serde_json::Value;
use std::time::{Duration, Instant};

use super::{
    client::{DmsClient, DmsClientError, Result as ClientResult},
    models::{LeaseRequest, LeaseResponse},
    redact::{Redacted, RedactedOption},
    session::{CapabilitySelector, HeartbeatPolicy, SessionError, SessionManager, SessionSnapshot},
};

#[derive(Debug)]
pub struct IdleSchedule {
    pub delay: Duration,
    pub next_poll_at: Instant,
}

pub struct PollController<R> {
    min: Duration,
    max: Duration,
    current: Duration,
    rng: R,
}

impl PollController<StdRng> {
    pub fn from_defaults(min: Duration, max: Duration, seed: u64) -> Self {
        let rng = StdRng::seed_from_u64(seed);
        Self::new(min, max, rng)
    }
}

impl<R> PollController<R>
where
    R: Rng,
{
    pub fn new(min: Duration, max: Duration, rng: R) -> Self {
        assert!(min <= max, "min backoff must be <= max backoff");
        Self {
            min,
            max,
            current: Duration::ZERO,
            rng,
        }
    }

    pub fn reset(&mut self) {
        self.current = Duration::ZERO;
    }

    pub fn schedule_idle(&mut self, now: Instant) -> IdleSchedule {
        let delay = self.next_delay();
        IdleSchedule {
            delay,
            next_poll_at: now + delay,
        }
    }

    pub fn current_delay(&self) -> Duration {
        self.current
    }

    pub fn rng_mut(&mut self) -> &mut R {
        &mut self.rng
    }

    fn next_delay(&mut self) -> Duration {
        let base = if self.current.is_zero() {
            self.min
        } else {
            let doubled = self
                .current
                .checked_mul(2)
                .unwrap_or_else(|| Duration::from_secs(u64::MAX));
            if doubled > self.max {
                self.max
            } else {
                doubled
            }
        };
        self.current = base;

        let min_ms = self.min.as_millis().max(1).min(u64::MAX as u128) as u64;
        let base_ms = base.as_millis().max(1).min(u64::MAX as u128) as u64;
        if min_ms == base_ms {
            Duration::from_millis(base_ms)
        } else {
            let dist = Uniform::new_inclusive(min_ms, base_ms);
            let jitter_ms = dist.sample(&mut self.rng);
            Duration::from_millis(jitter_ms)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PollError {
    #[error(transparent)]
    Client(#[from] DmsClientError),
    #[error(transparent)]
    Session(#[from] SessionError),
    #[error("no capability configured")]
    NoCapability,
}

#[derive(Debug)]
pub enum PollResult {
    AlreadyRunning,
    Idle { schedule: IdleSchedule },
    Leased(Box<SessionSnapshot>),
}

#[derive(Debug, thiserror::Error)]
pub enum HeartbeatError {
    #[error(transparent)]
    Client(#[from] DmsClientError),
    #[error(transparent)]
    Session(#[from] SessionError),
    #[error("no active session for heartbeat")]
    NoActiveSession,
}

#[derive(Debug)]
pub enum HeartbeatResult {
    Scheduled(Box<SessionSnapshot>),
    Canceled,
    LostLease,
}

#[derive(Debug, thiserror::Error)]
pub enum CompletionError {
    #[error("no active session for completion")]
    NoActiveSession,
    #[error(transparent)]
    Client(#[from] DmsClientError),
}

#[derive(Debug, thiserror::Error)]
pub enum FailureError {
    #[error("no active session for failure report")]
    NoActiveSession,
    #[error("failed to report task failure after {attempts} attempts")]
    Transport {
        attempts: usize,
        #[source]
        source: DmsClientError,
    },
    #[error(transparent)]
    Client(#[from] DmsClientError),
}

#[async_trait::async_trait]
pub trait DmsApi: Send + Sync {
    async fn lease_task(&self, request: &LeaseRequest) -> ClientResult<LeaseResponse>;
    async fn send_heartbeat(
        &self,
        task_id: &str,
        progress: Option<&Value>,
    ) -> ClientResult<LeaseResponse>;
    async fn complete_task(
        &self,
        task_id: &str,
        outputs: &[String],
        meta: Option<&Value>,
    ) -> ClientResult<()>;
    async fn fail_task(
        &self,
        task_id: &str,
        reason: &str,
        details: Option<&Value>,
    ) -> ClientResult<()>;
}

#[async_trait::async_trait]
impl DmsApi for DmsClient {
    async fn lease_task(&self, request: &LeaseRequest) -> ClientResult<LeaseResponse> {
        DmsClient::lease_task(self, request).await
    }

    async fn send_heartbeat(
        &self,
        task_id: &str,
        progress: Option<&Value>,
    ) -> ClientResult<LeaseResponse> {
        DmsClient::send_heartbeat(self, task_id, progress).await
    }

    async fn complete_task(
        &self,
        task_id: &str,
        outputs: &[String],
        meta: Option<&Value>,
    ) -> ClientResult<()> {
        DmsClient::complete_task(self, task_id, outputs, meta).await
    }

    async fn fail_task(
        &self,
        task_id: &str,
        reason: &str,
        details: Option<&Value>,
    ) -> ClientResult<()> {
        DmsClient::fail_task(self, task_id, reason, details).await
    }
}

pub struct Poller<C, R> {
    client: C,
    session: SessionManager,
    controller: PollController<R>,
    heartbeat_policy: HeartbeatPolicy,
}

const MAX_FAIL_ATTEMPTS: usize = 3;

impl<C, R> Poller<C, R>
where
    C: DmsApi,
    R: Rng,
{
    pub fn new(
        client: C,
        session: SessionManager,
        controller: PollController<R>,
        heartbeat_policy: HeartbeatPolicy,
    ) -> Self {
        Self {
            client,
            session,
            controller,
            heartbeat_policy,
        }
    }

    pub fn capability_selector(&self) -> &CapabilitySelector {
        self.session.selector()
    }

    pub fn current_backoff(&self) -> Duration {
        self.controller.current_delay()
    }

    pub async fn session_snapshot(&self) -> Option<SessionSnapshot> {
        self.session.snapshot().await
    }

    pub async fn clear_session(&self) {
        self.session.clear().await;
    }

    pub async fn poll_once(&mut self, now: Instant) -> Result<PollResult, PollError> {
        if self.session.snapshot().await.is_some() {
            return Ok(PollResult::AlreadyRunning);
        }

        let capability = self
            .session
            .selector()
            .choose()
            .ok_or(PollError::NoCapability)?;

        let span = tracing::info_span!(
            "dms.lease",
            task_id = tracing::field::Empty,
            capability = %capability,
            access_token = tracing::field::Empty
        );
        let _guard = span.enter();
        let lease = self
            .client
            .lease_task(&LeaseRequest {
                capability: capability.to_string(),
                job_id: None,
                domain_id: None,
            })
            .await?;

        span.record(
            "task_id",
            tracing::field::display(lease.task.as_ref().map(|t| t.id.as_str()).unwrap_or("none")),
        );
        span.record(
            "access_token",
            tracing::field::display(RedactedOption::new(lease.access_token.as_deref())),
        );

        if lease.task.is_none() {
            let schedule = self.controller.schedule_idle(now);
            return Ok(PollResult::Idle { schedule });
        }

        {
            let rng = self.controller.rng_mut();
            self.session
                .start_session(&lease, now, &self.heartbeat_policy, rng)
                .await?;
        }
        self.controller.reset();
        let snapshot = self.session.snapshot().await.expect("session just started");
        Ok(PollResult::Leased(Box::new(snapshot)))
    }

    pub async fn send_heartbeat(
        &mut self,
        task_id: &str,
        progress: Option<Value>,
        now: Instant,
    ) -> Result<HeartbeatResult, HeartbeatError> {
        let snapshot = self
            .session
            .snapshot()
            .await
            .ok_or(HeartbeatError::NoActiveSession)?;
        let span = tracing::info_span!(
            "dms.heartbeat",
            task_id = %task_id,
            capability = %snapshot.capability(),
            access_token = tracing::field::Empty
        );
        span.record(
            "access_token",
            tracing::field::display(RedactedOption::new(snapshot.access_token())),
        );
        let _guard = span.enter();

        let response = match self.client.send_heartbeat(task_id, progress.as_ref()).await {
            Ok(resp) => resp,
            Err(DmsClientError::UnexpectedStatus(status))
                if status == StatusCode::CONFLICT
                    || status == StatusCode::NOT_FOUND
                    || status == StatusCode::GONE =>
            {
                self.session.clear().await;
                self.controller.reset();
                return Ok(HeartbeatResult::LostLease);
            }
            Err(err) => return Err(HeartbeatError::Client(err)),
        };

        if response.cancel.unwrap_or(false) {
            self.session.clear().await;
            self.controller.reset();
            return Ok(HeartbeatResult::Canceled);
        }

        let snapshot = {
            let rng = self.controller.rng_mut();
            self.session
                .apply_heartbeat(&response, progress, now, &self.heartbeat_policy, rng)
                .await?
        };
        Ok(HeartbeatResult::Scheduled(Box::new(snapshot)))
    }

    pub async fn complete_task(
        &mut self,
        outputs: Vec<String>,
        meta: Option<Value>,
    ) -> Result<(), CompletionError> {
        let snapshot = self
            .session
            .snapshot()
            .await
            .ok_or(CompletionError::NoActiveSession)?;
        let task_id = snapshot.task_id().to_string();
        let span = tracing::info_span!(
            "dms.complete",
            task_id = %task_id,
            capability = %snapshot.capability(),
            access_token = tracing::field::Empty
        );
        span.record(
            "access_token",
            tracing::field::display(RedactedOption::new(snapshot.access_token())),
        );
        let _guard = span.enter();

        let outputs_redacted = Redacted::new(&outputs);
        let meta_redacted = meta.as_ref().map(Redacted::new);
        tracing::info!(
            task_id = %task_id,
            outputs = ?outputs_redacted,
            meta = ?meta_redacted,
            "reporting task completion",
        );

        self.client
            .complete_task(&task_id, &outputs, meta.as_ref())
            .await?;

        self.session.clear().await;
        self.controller.reset();

        tracing::info!(task_id = %task_id, "task completion reported");
        Ok(())
    }

    pub async fn fail_task(
        &mut self,
        reason: &str,
        details: Option<Value>,
    ) -> Result<(), FailureError> {
        let snapshot = self
            .session
            .snapshot()
            .await
            .ok_or(FailureError::NoActiveSession)?;
        let task_id = snapshot.task_id().to_string();
        let span = tracing::info_span!(
            "dms.fail",
            task_id = %task_id,
            capability = %snapshot.capability(),
            reason = reason,
            access_token = tracing::field::Empty
        );
        span.record(
            "access_token",
            tracing::field::display(RedactedOption::new(snapshot.access_token())),
        );
        let _guard = span.enter();

        let details_ref = details.as_ref();
        let details_redacted = details_ref.map(Redacted::new);
        tracing::warn!(
            task_id = %task_id,
            reason,
            details = ?details_redacted,
            "reporting task failure",
        );

        let mut attempts = 0usize;
        loop {
            attempts += 1;
            match self.client.fail_task(&task_id, reason, details_ref).await {
                Ok(()) => break,
                Err(err @ DmsClientError::Http(_)) => {
                    tracing::warn!(
                        task_id = %task_id,
                        attempt = attempts,
                        error = %err,
                        "transport error while reporting failure",
                    );
                    if attempts < MAX_FAIL_ATTEMPTS {
                        continue;
                    }
                    return Err(FailureError::Transport {
                        attempts,
                        source: err,
                    });
                }
                Err(err) => return Err(FailureError::Client(err)),
            }
        }

        self.session.clear().await;
        self.controller.reset();

        tracing::info!(task_id = %task_id, "task failure reported");
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_backoff_for_test(&mut self, duration: Duration) {
        self.controller.current = duration;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dms::session::SessionStatus;
    use chrono::{Duration as ChronoDuration, Utc};
    use parking_lot::Mutex;
    use rand::SeedableRng;
    use serde_json::json;
    use std::io::Write;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;
    use tracing_subscriber::fmt;

    type LeaseResponses = Arc<Mutex<Vec<LeaseResponse>>>;
    type HeartbeatResponses = Arc<Mutex<Vec<Result<LeaseResponse, DmsClientError>>>>;
    type LeaseCallLog = Arc<Mutex<Vec<LeaseRequest>>>;
    type HeartbeatCallLog = Arc<Mutex<Vec<(String, Option<Value>)>>>;
    type CompletionResults = Arc<Mutex<Vec<Result<(), DmsClientError>>>>;
    type FailResults = Arc<Mutex<Vec<Result<(), DmsClientError>>>>;
    type CompletionCallLog = Arc<Mutex<Vec<(String, Vec<String>, Option<Value>)>>>;
    type FailCallLog = Arc<Mutex<Vec<(String, String, Option<Value>)>>>;

    #[derive(Clone)]
    struct MockClient {
        lease_responses: LeaseResponses,
        heartbeat_responses: HeartbeatResponses,
        lease_calls: LeaseCallLog,
        heartbeat_calls: HeartbeatCallLog,
        complete_results: CompletionResults,
        fail_results: FailResults,
        complete_calls: CompletionCallLog,
        fail_calls: FailCallLog,
    }

    impl MockClient {
        fn new(leases: Vec<LeaseResponse>) -> Self {
            Self {
                lease_responses: Arc::new(Mutex::new(leases)),
                heartbeat_responses: Arc::new(Mutex::new(Vec::new())),
                lease_calls: Arc::new(Mutex::new(Vec::new())),
                heartbeat_calls: Arc::new(Mutex::new(Vec::new())),
                complete_results: Arc::new(Mutex::new(Vec::new())),
                fail_results: Arc::new(Mutex::new(Vec::new())),
                complete_calls: Arc::new(Mutex::new(Vec::new())),
                fail_calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn push_heartbeat(&self, response: Result<LeaseResponse, DmsClientError>) {
            self.heartbeat_responses.lock().push(response);
        }

        fn push_complete(&self, result: Result<(), DmsClientError>) {
            self.complete_results.lock().push(result);
        }

        fn push_fail(&self, result: Result<(), DmsClientError>) {
            self.fail_results.lock().push(result);
        }
    }

    struct Writer {
        buffer: Arc<StdMutex<Vec<u8>>>,
    }

    impl Write for Writer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = self.buffer.lock().expect("log buffer poisoned");
            guard.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl DmsApi for MockClient {
        async fn lease_task(&self, request: &LeaseRequest) -> ClientResult<LeaseResponse> {
            self.lease_calls.lock().push(request.clone());
            let mut responses = self.lease_responses.lock();
            if responses.is_empty() {
                Ok(LeaseResponse::default())
            } else {
                Ok(responses.remove(0))
            }
        }

        async fn send_heartbeat(
            &self,
            task_id: &str,
            progress: Option<&Value>,
        ) -> ClientResult<LeaseResponse> {
            self.heartbeat_calls
                .lock()
                .push((task_id.to_string(), progress.cloned()));
            let mut responses = self.heartbeat_responses.lock();
            if responses.is_empty() {
                Ok(LeaseResponse::default())
            } else {
                responses.remove(0)
            }
        }

        async fn complete_task(
            &self,
            task_id: &str,
            outputs: &[String],
            meta: Option<&Value>,
        ) -> ClientResult<()> {
            self.complete_calls
                .lock()
                .push((task_id.to_string(), outputs.to_vec(), meta.cloned()));
            let mut results = self.complete_results.lock();
            if results.is_empty() {
                Ok(())
            } else {
                results.remove(0)
            }
        }

        async fn fail_task(
            &self,
            task_id: &str,
            reason: &str,
            details: Option<&Value>,
        ) -> ClientResult<()> {
            self.fail_calls.lock().push((
                task_id.to_string(),
                reason.to_string(),
                details.cloned(),
            ));
            let mut results = self.fail_results.lock();
            if results.is_empty() {
                Ok(())
            } else {
                results.remove(0)
            }
        }
    }

    fn selector() -> CapabilitySelector {
        CapabilitySelector::new(vec!["cap-a".to_string()])
    }

    fn manager() -> SessionManager {
        SessionManager::new(selector())
    }

    fn policy() -> HeartbeatPolicy {
        HeartbeatPolicy::default_policy()
    }

    fn timestamp_in(minutes: i64) -> String {
        (Utc::now() + ChronoDuration::minutes(minutes)).to_rfc3339()
    }

    async fn make_transport_error() -> DmsClientError {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(5))
            .build()
            .expect("reqwest client");
        let error = client
            .get("http://127.0.0.1:1")
            .send()
            .await
            .expect_err("transport error");
        DmsClientError::Http(error)
    }

    fn controller(rng_seed: u64) -> PollController<StdRng> {
        PollController::new(
            Duration::from_millis(500),
            Duration::from_millis(2_000),
            StdRng::seed_from_u64(rng_seed),
        )
    }

    #[tokio::test]
    async fn idle_backoff_increases_with_jitter() {
        let client = MockClient::new(vec![LeaseResponse::default()]);
        let session = manager();
        let mut poller = Poller::new(client.clone(), session, controller(42), policy());

        let now = Instant::now();
        let result = poller.poll_once(now).await.expect("poll ok");
        match result {
            PollResult::Idle { schedule } => {
                assert!(schedule.delay >= Duration::from_millis(500));
                assert!(schedule.delay <= Duration::from_millis(2_000));
            }
            _ => panic!("expected idle"),
        }

        let second = poller
            .poll_once(now + Duration::from_millis(1_000))
            .await
            .expect("poll ok");
        match second {
            PollResult::Idle { schedule } => {
                assert!(schedule.delay >= Duration::from_millis(500));
                assert!(schedule.delay <= Duration::from_millis(2_000));
            }
            _ => panic!("expected idle"),
        }

        let calls = client.lease_calls.lock();
        assert_eq!(calls.len(), 2);
        for call in calls.iter() {
            assert_eq!(call.capability, "cap-a");
        }
    }

    #[tokio::test]
    async fn poll_once_skips_when_session_active() {
        let client = MockClient::new(vec![]);
        let session = manager();
        let now = Instant::now();
        let mut rng = StdRng::seed_from_u64(7);
        session
            .start_session(
                &LeaseResponse {
                    task: Some(crate::dms::models::TaskSummary {
                        id: "123".into(),
                        job_id: None,
                        capability: "cap-a".into(),
                        inputs_cids: vec![],
                        meta: json!({}),
                    }),
                    ..LeaseResponse::default()
                },
                now,
                &policy(),
                &mut rng,
            )
            .await
            .unwrap();

        let mut poller = Poller::new(client.clone(), session.clone(), controller(1), policy());

        match poller.poll_once(Instant::now()).await.expect("poll") {
            PollResult::AlreadyRunning => {}
            other => panic!("unexpected result: {:?}", other),
        }

        assert!(client.lease_calls.lock().is_empty());
    }

    #[tokio::test]
    async fn lease_success_populates_session_fields() {
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "task-1".into(),
                job_id: None,
                capability: "cap-a".into(),
                inputs_cids: vec![],
                meta: json!({"foo": "bar"}),
            }),
            access_token: Some("token".into()),
            access_token_expires_at: Some("2025-01-01T00:00:00Z".into()),
            lease_expires_at: Some("2025-01-01T00:05:00Z".into()),
            ..LeaseResponse::default()
        };
        let client = MockClient::new(vec![lease]);
        let session = manager();
        let mut poller = Poller::new(client, session.clone(), controller(9), policy());

        match poller.poll_once(Instant::now()).await.expect("poll") {
            PollResult::Leased(snapshot) => {
                assert_eq!(snapshot.task_id(), "task-1");
                assert_eq!(snapshot.capability(), "cap-a");
                assert_eq!(snapshot.access_token(), Some("token"));
                assert_eq!(snapshot.meta(), &json!({"foo": "bar"}));
                assert!(snapshot.lease_expires_at().is_some());
                assert!(snapshot.next_heartbeat_due().is_some());
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn heartbeat_promotes_running_and_reschedules() {
        let lease_expiry = timestamp_in(2);
        let heartbeat_expiry = timestamp_in(2);
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "task-1".into(),
                job_id: None,
                capability: "cap-a".into(),
                inputs_cids: vec![],
                meta: json!({}),
            }),
            lease_expires_at: Some(lease_expiry),
            ..LeaseResponse::default()
        };
        let heartbeat = LeaseResponse {
            lease_expires_at: Some(heartbeat_expiry.clone()),
            ..LeaseResponse::default()
        };
        let client = MockClient::new(vec![lease]);
        client.push_heartbeat(Ok(heartbeat));
        let session = manager();
        let mut poller = Poller::new(client, session.clone(), controller(12), policy());

        let now = Instant::now();
        poller.poll_once(now).await.expect("poll");
        let heartbeat_now = Instant::now();
        let result = poller
            .send_heartbeat("task-1", Some(json!({"message": "loading"})), heartbeat_now)
            .await
            .expect("heartbeat");

        match result {
            HeartbeatResult::Scheduled(snapshot) => {
                assert_eq!(snapshot.status(), SessionStatus::Running);
                let due = snapshot.next_heartbeat_due().expect("due");
                let elapsed = due.duration_since(heartbeat_now);
                assert!(elapsed > Duration::ZERO);

                let lease_deadline = snapshot.lease_expires_at().expect("lease expiry");
                let ttl = lease_deadline
                    .signed_duration_since(Utc::now())
                    .to_std()
                    .unwrap_or(Duration::ZERO);
                assert!(ttl > Duration::ZERO);

                let policy = policy();
                let slack = 0.05;
                let min_ratio = (policy.min_ratio - slack).max(0.0);
                let max_ratio = (policy.max_ratio + slack).min(1.0);
                let min_expected = ttl.mul_f64(min_ratio);
                let max_expected = ttl.mul_f64(max_ratio);

                assert!(elapsed >= min_expected);
                assert!(elapsed <= max_expected + Duration::from_millis(500));
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[tokio::test]
    async fn heartbeat_cancel_clears_session() {
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "task-1".into(),
                job_id: None,
                capability: "cap-a".into(),
                inputs_cids: vec![],
                meta: json!({}),
            }),
            ..LeaseResponse::default()
        };
        let heartbeat = LeaseResponse {
            cancel: Some(true),
            ..LeaseResponse::default()
        };
        let client = MockClient::new(vec![lease]);
        client.push_heartbeat(Ok(heartbeat));
        let session = manager();
        let mut poller = Poller::new(client, session.clone(), controller(15), policy());

        poller.poll_once(Instant::now()).await.expect("poll");
        let res = poller
            .send_heartbeat("task-1", None, Instant::now())
            .await
            .expect("heartbeat");
        assert!(matches!(res, HeartbeatResult::Canceled));
        assert!(session.snapshot().await.is_none());
    }

    #[tokio::test]
    async fn heartbeat_conflict_returns_lost_lease() {
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "task-1".into(),
                job_id: None,
                capability: "cap-a".into(),
                inputs_cids: vec![],
                meta: json!({}),
            }),
            ..LeaseResponse::default()
        };
        let client = MockClient::new(vec![lease]);
        client.push_heartbeat(Err(DmsClientError::UnexpectedStatus(StatusCode::CONFLICT)));
        let session = manager();
        let mut poller = Poller::new(client, session.clone(), controller(20), policy());

        poller.poll_once(Instant::now()).await.expect("poll");
        let res = poller
            .send_heartbeat("task-1", None, Instant::now())
            .await
            .expect("heartbeat");
        assert!(matches!(res, HeartbeatResult::LostLease));
        assert!(session.snapshot().await.is_none());
        assert_eq!(poller.current_backoff(), Duration::ZERO);
    }

    #[tokio::test]
    async fn complete_task_clears_session_and_resets_backoff() {
        let lease_idle = LeaseResponse::default();
        let lease_active = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "task-1".into(),
                job_id: None,
                capability: "cap-a".into(),
                inputs_cids: vec![],
                meta: json!({"foo": "bar"}),
            }),
            ..LeaseResponse::default()
        };
        let client = MockClient::new(vec![lease_idle, lease_active]);
        client.push_complete(Ok(()));
        let session = manager();
        let mut poller = Poller::new(client.clone(), session.clone(), controller(30), policy());

        let now = Instant::now();
        let _ = poller.poll_once(now).await.expect("poll");
        match poller
            .poll_once(now + Duration::from_secs(1))
            .await
            .expect("poll")
        {
            PollResult::Leased(snapshot) => assert_eq!(snapshot.task_id(), "task-1"),
            other => panic!("unexpected poll result: {:?}", other),
        }

        poller.set_backoff_for_test(Duration::from_secs(5));
        assert_eq!(poller.current_backoff(), Duration::from_secs(5));

        let outputs = vec!["cid-1".to_string(), "cid-2".to_string()];
        let meta = Some(json!({"secret": "value"}));
        let log_buffer = Arc::new(StdMutex::new(Vec::new()));
        let make_writer = {
            let buffer = Arc::clone(&log_buffer);
            move || Writer {
                buffer: Arc::clone(&buffer),
            }
        };
        let subscriber = fmt()
            .with_writer(make_writer)
            .with_ansi(false)
            .without_time()
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);

        poller
            .complete_task(outputs.clone(), meta.clone())
            .await
            .expect("complete");

        drop(guard);

        {
            let calls = client.complete_calls.lock();
            assert_eq!(calls.len(), 1);
            let (task_id, recorded_outputs, recorded_meta) = &calls[0];
            assert_eq!(task_id, "task-1");
            assert_eq!(recorded_outputs, &outputs);
            assert_eq!(recorded_meta.as_ref(), meta.as_ref());
        }

        assert!(session.snapshot().await.is_none());
        assert_eq!(poller.current_backoff(), Duration::ZERO);

        let logs = {
            let data = log_buffer.lock().expect("log buffer").clone();
            String::from_utf8(data).expect("utf8")
        };
        assert!(logs.contains("[REDACTED]"));
        assert!(!logs.contains("cid-1"));
        assert!(!logs.contains("value"));
    }

    #[tokio::test]
    async fn fail_task_clears_session_and_resets_backoff() {
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "task-1".into(),
                job_id: None,
                capability: "cap-a".into(),
                inputs_cids: vec![],
                meta: json!({}),
            }),
            ..LeaseResponse::default()
        };
        let client = MockClient::new(vec![lease.clone()]);
        client.push_fail(Ok(()));
        let session = manager();
        let mut poller = Poller::new(client.clone(), session.clone(), controller(33), policy());

        poller.poll_once(Instant::now()).await.expect("poll");
        poller.set_backoff_for_test(Duration::from_secs(3));

        let details = Some(json!({"error": "sensitive"}));
        let log_buffer = Arc::new(StdMutex::new(Vec::new()));
        let make_writer = {
            let buffer = Arc::clone(&log_buffer);
            move || Writer {
                buffer: Arc::clone(&buffer),
            }
        };
        let subscriber = fmt()
            .with_writer(make_writer)
            .with_ansi(false)
            .without_time()
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);

        poller
            .fail_task("pipeline error", details.clone())
            .await
            .expect("fail");

        drop(guard);

        {
            let calls = client.fail_calls.lock();
            assert_eq!(calls.len(), 1);
            let (task_id, reason, recorded_details) = &calls[0];
            assert_eq!(task_id, "task-1");
            assert_eq!(reason, "pipeline error");
            assert_eq!(recorded_details.as_ref(), details.as_ref());
        }

        assert!(session.snapshot().await.is_none());
        assert_eq!(poller.current_backoff(), Duration::ZERO);

        let logs = {
            let data = log_buffer.lock().expect("log buffer").clone();
            String::from_utf8(data).expect("utf8")
        };
        assert!(logs.contains("[REDACTED]"));
        assert!(!logs.contains("sensitive"));
    }

    #[tokio::test]
    async fn fail_task_retries_transport_errors() {
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "task-1".into(),
                job_id: None,
                capability: "cap-a".into(),
                inputs_cids: vec![],
                meta: json!({}),
            }),
            ..LeaseResponse::default()
        };
        let client = MockClient::new(vec![lease]);
        for _ in 0..(MAX_FAIL_ATTEMPTS - 1) {
            client.push_fail(Err(make_transport_error().await));
        }
        client.push_fail(Ok(()));
        let session = manager();
        let mut poller = Poller::new(client.clone(), session.clone(), controller(45), policy());

        poller.poll_once(Instant::now()).await.expect("poll");

        poller.fail_task("runtime error", None).await.expect("fail");

        {
            let calls = client.fail_calls.lock();
            assert_eq!(calls.len(), MAX_FAIL_ATTEMPTS);
        }
        assert!(session.snapshot().await.is_none());
        assert_eq!(poller.current_backoff(), Duration::ZERO);
    }

    #[tokio::test]
    async fn fail_task_errors_after_max_transport_failures() {
        let lease = LeaseResponse {
            task: Some(crate::dms::models::TaskSummary {
                id: "task-1".into(),
                job_id: None,
                capability: "cap-a".into(),
                inputs_cids: vec![],
                meta: json!({}),
            }),
            ..LeaseResponse::default()
        };
        let client = MockClient::new(vec![lease]);
        for _ in 0..MAX_FAIL_ATTEMPTS {
            client.push_fail(Err(make_transport_error().await));
        }
        let session = manager();
        let mut poller = Poller::new(client.clone(), session.clone(), controller(50), policy());

        poller.poll_once(Instant::now()).await.expect("poll");

        let err = poller
            .fail_task("runtime error", None)
            .await
            .expect_err("fail should error");
        match err {
            FailureError::Transport { attempts, .. } => {
                assert_eq!(attempts, MAX_FAIL_ATTEMPTS);
            }
            other => panic!("unexpected error: {other:?}"),
        }

        assert!(session.snapshot().await.is_some());
        assert!(client.fail_calls.lock().len() >= MAX_FAIL_ATTEMPTS);
    }
}
