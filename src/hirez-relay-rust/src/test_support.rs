use std::{
    collections::VecDeque,
    sync::{
        Mutex as StdMutex,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;

use crate::upstream::{
    ApiCredential, ApiKeyState, Clock, HttpResponse, HttpTransport, RetrySleeper, SessionAudit,
    SessionAuditRecord, TransportError, UpstreamError,
};

pub struct FakeTransport {
    responses: StdMutex<VecDeque<Result<HttpResponse, TransportError>>>,
    pub calls: AtomicUsize,
    pub urls: StdMutex<Vec<String>>,
    pub timeouts: StdMutex<Vec<Duration>>,
}

impl FakeTransport {
    pub fn new(responses: Vec<Result<HttpResponse, TransportError>>) -> Self {
        Self {
            responses: StdMutex::new(responses.into()),
            calls: AtomicUsize::new(0),
            urls: StdMutex::new(Vec::new()),
            timeouts: StdMutex::new(Vec::new()),
        }
    }

    pub fn json(status: u16, reason: &str, body: &str) -> Result<HttpResponse, TransportError> {
        Ok(HttpResponse {
            status,
            reason: reason.to_owned(),
            body: body.to_owned(),
        })
    }
}

#[async_trait]
impl HttpTransport for FakeTransport {
    async fn get(&self, url: &str, timeout: Duration) -> Result<HttpResponse, TransportError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.urls.lock().expect("urls").push(url.to_owned());
        self.timeouts.lock().expect("timeouts").push(timeout);
        self.responses
            .lock()
            .expect("responses")
            .pop_front()
            .expect("fake transport response queue exhausted")
    }
}

pub struct FakeKeyState {
    pub keys: Vec<ApiCredential>,
    active: AtomicUsize,
    pub usage: AtomicUsize,
    pub logs: StdMutex<Vec<(String, String, String)>>,
    pub successes: StdMutex<Vec<String>>,
    pub failures: StdMutex<Vec<(String, bool)>>,
    pub syncs: StdMutex<Vec<String>>,
    rotate_on_sync: bool,
}

impl FakeKeyState {
    pub fn one() -> Self {
        Self::new(vec![ApiCredential {
            dev_id: "1234".to_owned(),
            auth_key: "secret".to_owned(),
        }])
    }

    pub fn new(keys: Vec<ApiCredential>) -> Self {
        Self {
            keys,
            active: AtomicUsize::new(0),
            usage: AtomicUsize::new(0),
            logs: StdMutex::new(Vec::new()),
            successes: StdMutex::new(Vec::new()),
            failures: StdMutex::new(Vec::new()),
            syncs: StdMutex::new(Vec::new()),
            rotate_on_sync: false,
        }
    }

    pub fn with_rotation(mut self) -> Self {
        self.rotate_on_sync = true;
        self
    }
}

#[async_trait]
impl ApiKeyState for FakeKeyState {
    async fn active_key(&self) -> Result<ApiCredential, UpstreamError> {
        self.keys
            .get(self.active.load(Ordering::Relaxed))
            .cloned()
            .ok_or_else(|| UpstreamError::Configuration("No active API key".to_owned()))
    }

    fn increment_usage(&self, _dev_id: &str) {
        self.usage.fetch_add(1, Ordering::Relaxed);
    }

    async fn log_endpoint(
        &self,
        dev_id: &str,
        endpoint: &str,
        _response_time_ms: u64,
        consumer: &str,
    ) {
        self.logs.lock().expect("logs").push((
            dev_id.to_owned(),
            endpoint.to_owned(),
            consumer.to_owned(),
        ));
    }

    async fn record_success(&self, dev_id: &str) {
        self.successes
            .lock()
            .expect("successes")
            .push(dev_id.to_owned());
    }

    async fn record_failure(&self, dev_id: &str, key_fault: bool) {
        self.failures
            .lock()
            .expect("failures")
            .push((dev_id.to_owned(), key_fault));
    }

    async fn sync_usage(&self, dev_id: &str) {
        self.syncs.lock().expect("syncs").push(dev_id.to_owned());
        if self.rotate_on_sync && self.active.load(Ordering::Relaxed) + 1 < self.keys.len() {
            self.active.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub struct FixedClock {
    millis: AtomicI64,
}

impl FixedClock {
    pub fn new(millis: i64) -> Self {
        Self {
            millis: AtomicI64::new(millis),
        }
    }
}

impl Clock for FixedClock {
    fn unix_millis(&self) -> i64 {
        self.millis.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct NoopRetrySleeper {
    pub calls: AtomicUsize,
}

#[async_trait]
impl RetrySleeper for NoopRetrySleeper {
    async fn sleep_before_retry(&self, _completed_attempt: usize) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct RecordingAudit {
    pub records: StdMutex<Vec<SessionAuditRecord>>,
}

#[async_trait]
impl SessionAudit for RecordingAudit {
    async fn save(&self, record: SessionAuditRecord) {
        self.records.lock().expect("audit records").push(record);
    }
}
