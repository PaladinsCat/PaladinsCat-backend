use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    key_crypto::{KeyCrypto, KeyCryptoError},
    upstream::{ApiCredential, ApiKeyState, SharedClock, UpstreamError},
};

pub const DEFAULT_API_KEY_RESERVE_CALLS: u64 = 100;
const FAILURE_THRESHOLD: u32 = 5;
const REVIVAL_COOLDOWN_MS: i64 = 30 * 60 * 1_000;
const ESTIMATE_REVIVAL_COOLDOWN_MS: i64 = 5 * 60 * 1_000;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum KeyStatus {
    Healthy,
    Limited,
    Unhealthy,
}

#[derive(Clone, Debug)]
pub struct EncryptedKeyRow {
    pub dev_id: String,
    pub encrypted_auth_key: String,
    pub status: String,
    pub total_24h: u64,
    pub daily_limit: Option<u64>,
    pub calls_total: u64,
    pub consecutive_failures: u32,
}

#[derive(Clone, Debug)]
pub struct BootstrapKey {
    pub dev_id: String,
    pub encrypted_auth_key: String,
    pub daily_limit: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageBatch {
    pub dev_id: String,
    pub calls: u64,
}

#[derive(Clone, Debug)]
pub struct AuthoritativeUsage {
    pub used: u64,
    pub reported_limit: Option<u64>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ApiKeyStatus {
    #[serde(rename = "devId")]
    pub dev_id: String,
    #[serde(rename = "authKey")]
    pub auth_key: String,
    pub status: KeyStatus,
    #[serde(rename = "daily_limit")]
    pub daily_limit: u64,
    #[serde(rename = "used_24h")]
    pub used_24h: u64,
    pub remaining: u64,
    #[serde(rename = "reserve_threshold")]
    pub reserve_threshold: u64,
    #[serde(rename = "calls_total")]
    pub calls_total: u64,
    #[serde(rename = "consecutive_failures")]
    pub consecutive_failures: u32,
    #[serde(rename = "last_used")]
    pub last_used: String,
}

#[derive(Debug, Error)]
pub enum KeyPoolError {
    #[error("{0}")]
    Repository(String),
    #[error("{0}")]
    Crypto(#[from] KeyCryptoError),
    #[error("{0}")]
    Configuration(String),
    #[error("{0}")]
    Exhausted(String),
    #[error("{0}")]
    Probe(String),
}

#[async_trait]
pub trait KeyPoolRepository: Send + Sync {
    async fn ensure_schema(&self) -> Result<(), KeyPoolError>;
    async fn load_keys(&self) -> Result<Vec<EncryptedKeyRow>, KeyPoolError>;
    async fn bootstrap_keys(&self, keys: &[BootstrapKey]) -> Result<usize, KeyPoolError>;
    async fn flush_usage(&self, batches: &[UsageBatch]) -> Result<(), KeyPoolError>;
    async fn update_status(
        &self,
        dev_id: &str,
        status: KeyStatus,
        consecutive_failures: u32,
    ) -> Result<(), KeyPoolError>;
    async fn log_endpoint(
        &self,
        dev_id: &str,
        endpoint: &str,
        response_time_ms: u64,
        consumer: &str,
    ) -> Result<(), KeyPoolError>;
    async fn save_authoritative_usage(
        &self,
        dev_id: &str,
        used: u64,
        limit: u64,
        status: KeyStatus,
    ) -> Result<(), KeyPoolError>;
    async fn local_usage_estimate(&self, dev_id: &str) -> Result<u64, KeyPoolError>;
    async fn record_sync_error(&self, dev_id: &str, error: &str) -> Result<(), KeyPoolError>;
    async fn cleanup_rolling_usage(&self) -> Result<(), KeyPoolError> {
        Ok(())
    }
}

#[async_trait]
pub trait UsageProbe: Send + Sync {
    async fn get_data_used(
        &self,
        key: &ApiCredential,
    ) -> Result<Option<AuthoritativeUsage>, KeyPoolError>;
}

#[derive(Clone, Debug)]
struct InMemoryKey {
    dev_id: String,
    auth_key: String,
    status: KeyStatus,
    daily_limit: u64,
    used_today: u64,
    pending_increments: u64,
    consecutive_failures: u32,
    calls_total: u64,
    is_backup: bool,
}

#[derive(Default)]
struct KeyPoolInner {
    keys: Vec<InMemoryKey>,
    active_dev_id: Option<String>,
    last_revival_attempt_ms: i64,
    last_estimate_revival_ms: HashMap<String, i64>,
}

pub struct KeyPool {
    reserve: u64,
    repository: Arc<dyn KeyPoolRepository>,
    usage_probe: RwLock<Option<Weak<dyn UsageProbe>>>,
    crypto: Arc<KeyCrypto>,
    clock: SharedClock,
    inner: RwLock<KeyPoolInner>,
    initialized: AtomicBool,
    init_lock: Mutex<()>,
    revival_lock: Mutex<()>,
}

impl KeyPool {
    pub fn new(
        reserve: u64,
        repository: Arc<dyn KeyPoolRepository>,
        usage_probe: Arc<dyn UsageProbe>,
        crypto: Arc<KeyCrypto>,
        clock: SharedClock,
    ) -> Self {
        Self {
            reserve,
            repository,
            usage_probe: RwLock::new(Some(Arc::downgrade(&usage_probe))),
            crypto,
            clock,
            inner: RwLock::new(KeyPoolInner::default()),
            initialized: AtomicBool::new(false),
            init_lock: Mutex::new(()),
            revival_lock: Mutex::new(()),
        }
    }

    pub fn set_usage_probe(&self, usage_probe: &Arc<dyn UsageProbe>) {
        *self.usage_probe.write().expect("usage probe write lock") =
            Some(Arc::downgrade(usage_probe));
    }

    pub async fn initialize(&self, key_file: Option<&Path>) -> Result<(), KeyPoolError> {
        let _guard = self.init_lock.lock().await;
        self.repository.ensure_schema().await?;
        if !self.crypto.smoke_test() {
            return Err(KeyPoolError::Configuration(
                "Encryption smoke test failed. MEK may be wrong.".to_owned(),
            ));
        }
        if let Some(path) = key_file {
            self.bootstrap_keys_from_file(path).await?;
        }
        self.reload_from_repository().await?;
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }

    pub async fn reload(&self, key_file: Option<&Path>) -> Result<(), KeyPoolError> {
        self.flush_usage().await?;
        self.initialize(key_file).await
    }

    async fn ensure_initialized(&self) -> Result<(), KeyPoolError> {
        if self.initialized.load(Ordering::Acquire) {
            return Ok(());
        }
        self.initialize(None).await
    }

    async fn reload_from_repository(&self) -> Result<(), KeyPoolError> {
        let rows = self.repository.load_keys().await?;
        let pending = self
            .inner
            .read()
            .expect("key pool read lock")
            .keys
            .iter()
            .map(|key| (key.dev_id.clone(), key.pending_increments))
            .collect::<HashMap<_, _>>();
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let Ok(auth_key) = self.crypto.decrypt(&row.encrypted_auth_key) else {
                continue;
            };
            keys.push(InMemoryKey {
                dev_id: row.dev_id.clone(),
                auth_key,
                status: normalize_status(&row.status),
                daily_limit: effective_daily_limit(&row.dev_id, row.daily_limit),
                used_today: row.total_24h,
                pending_increments: pending.get(&row.dev_id).copied().unwrap_or(0),
                consecutive_failures: row.consecutive_failures,
                calls_total: row.calls_total,
                is_backup: is_backup_dev_id(&row.dev_id),
            });
        }
        keys.sort_by(|left, right| left.dev_id.cmp(&right.dev_id));
        let mut inner = self.inner.write().expect("key pool write lock");
        inner.keys = keys;
        inner.active_dev_id = None;
        Ok(())
    }

    async fn bootstrap_keys_from_file(&self, path: &Path) -> Result<(), KeyPoolError> {
        if !path.exists() {
            return Ok(());
        }
        let raw = fs::read_to_string(path).map_err(|error| {
            KeyPoolError::Configuration(format!("Failed to read HIREZ_API_KEYS_FILE: {error}"))
        })?;
        let parsed: serde_json::Value = serde_json::from_str(&raw).map_err(|error| {
            KeyPoolError::Configuration(format!("Failed to read HIREZ_API_KEYS_FILE: {error}"))
        })?;
        let entries = if let Some(entries) = parsed.as_array() {
            entries
        } else {
            parsed
                .get("keys")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| {
                    KeyPoolError::Configuration(
                        "HIREZ_API_KEYS_FILE did not contain an array of Hi-Rez keys".to_owned(),
                    )
                })?
        };
        if entries.is_empty() {
            return Err(KeyPoolError::Configuration(
                "HIREZ_API_KEYS_FILE did not contain an array of Hi-Rez keys".to_owned(),
            ));
        }

        let mut encrypted = Vec::new();
        for entry in entries {
            let dev_id = json_scalar(entry.get("devId").or_else(|| entry.get("dev_id")));
            let auth_key = json_scalar(entry.get("authKey").or_else(|| entry.get("auth_key")));
            if dev_id.is_empty() || auth_key.is_empty() {
                continue;
            }
            encrypted.push(BootstrapKey {
                daily_limit: effective_daily_limit(&dev_id, None),
                dev_id,
                encrypted_auth_key: self.crypto.encrypt(&auth_key)?,
            });
        }
        self.repository.bootstrap_keys(&encrypted).await?;
        Ok(())
    }

    pub async fn active_key(&self) -> Result<ApiKeyStatus, KeyPoolError> {
        self.ensure_initialized().await?;
        if let Some(key) = self.select_available_key().await? {
            return Ok(key);
        }

        let _revival_guard = self.revival_lock.lock().await;
        if let Some(key) = self.select_available_key().await? {
            return Ok(key);
        }
        let now = self.clock.unix_millis();
        {
            let mut inner = self.inner.write().expect("key pool write lock");
            if now - inner.last_revival_attempt_ms < REVIVAL_COOLDOWN_MS {
                return Err(KeyPoolError::Exhausted(
                    "CRITICAL: All API keys exhausted or at/below reserve threshold. Revival on cooldown."
                        .to_owned(),
                ));
            }
            inner.last_revival_attempt_ms = now;
        }
        let dev_ids = self
            .inner
            .read()
            .expect("key pool read lock")
            .keys
            .iter()
            .map(|key| key.dev_id.clone())
            .collect::<Vec<_>>();
        for dev_id in dev_ids {
            self.sync_usage_internal(&dev_id).await;
        }
        self.select_available_key().await?.ok_or_else(|| {
            KeyPoolError::Exhausted(
                "CRITICAL: All API keys exhausted or at/below reserve threshold. No key available after sync attempt."
                    .to_owned(),
            )
        })
    }

    async fn select_available_key(&self) -> Result<Option<ApiKeyStatus>, KeyPoolError> {
        let status_update = {
            let mut inner = self.inner.write().expect("key pool write lock");
            if let Some(active_dev_id) = inner.active_dev_id.clone()
                && let Some(active) = inner
                    .keys
                    .iter_mut()
                    .find(|key| key.dev_id == active_dev_id)
            {
                if active.status == KeyStatus::Healthy
                    && has_usable_budget(active.used_today, active.daily_limit, self.reserve)
                {
                    return Ok(Some(format_key(active, self.reserve)));
                }
                if active.status != KeyStatus::Limited {
                    active.status = KeyStatus::Limited;
                    Some((
                        active.dev_id.clone(),
                        active.status,
                        active.consecutive_failures,
                    ))
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some((dev_id, status, failures)) = status_update {
            let _ = self
                .repository
                .update_status(&dev_id, status, failures)
                .await;
        }

        let mut inner = self.inner.write().expect("key pool write lock");
        // Pass 1: prefer primary (non-backup) keys; fall back to backup keys only
        // when no primary has usable budget. Returns to a primary the moment one
        // regains space (via sync_usage / estimate revival).
        let usable = |key: &InMemoryKey| {
            key.status == KeyStatus::Healthy
                && has_usable_budget(key.used_today, key.daily_limit, self.reserve)
        };
        let Some(index) = inner
            .keys
            .iter()
            .position(|key| !key.is_backup && usable(key))
            .or_else(|| {
                inner
                    .keys
                    .iter()
                    .position(|key| key.is_backup && usable(key))
            })
        else {
            return Ok(None);
        };
        let dev_id = inner.keys[index].dev_id.clone();
        inner.active_dev_id = Some(dev_id);
        Ok(Some(format_key(&inner.keys[index], self.reserve)))
    }

    pub fn increment_usage(&self, dev_id: &str) {
        let mut inner = self.inner.write().expect("key pool write lock");
        let mut clear_active = false;
        if let Some(key) = inner.keys.iter_mut().find(|key| key.dev_id == dev_id) {
            key.used_today = key.used_today.saturating_add(1);
            key.pending_increments = key.pending_increments.saturating_add(1);
            if !has_usable_budget(key.used_today, key.daily_limit, self.reserve) {
                key.status = KeyStatus::Limited;
                clear_active = true;
            }
        }
        if clear_active && inner.active_dev_id.as_deref() == Some(dev_id) {
            inner.active_dev_id = None;
        }
    }

    pub async fn flush_usage(&self) -> Result<(), KeyPoolError> {
        let batches = {
            let mut inner = self.inner.write().expect("key pool write lock");
            inner
                .keys
                .iter_mut()
                .filter_map(|key| {
                    if key.pending_increments == 0 {
                        return None;
                    }
                    let calls = key.pending_increments;
                    key.pending_increments = 0;
                    Some(UsageBatch {
                        dev_id: key.dev_id.clone(),
                        calls,
                    })
                })
                .collect::<Vec<_>>()
        };
        if batches.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.repository.flush_usage(&batches).await {
            let mut inner = self.inner.write().expect("key pool write lock");
            for batch in &batches {
                if let Some(key) = inner.keys.iter_mut().find(|key| key.dev_id == batch.dev_id) {
                    key.pending_increments = key.pending_increments.saturating_add(batch.calls);
                }
            }
            return Err(error);
        }
        let mut inner = self.inner.write().expect("key pool write lock");
        for batch in &batches {
            if let Some(key) = inner.keys.iter_mut().find(|key| key.dev_id == batch.dev_id) {
                key.calls_total = key.calls_total.saturating_add(batch.calls);
            }
        }
        Ok(())
    }

    pub async fn record_success(&self, dev_id: &str) {
        let update = {
            let mut inner = self.inner.write().expect("key pool write lock");
            inner
                .keys
                .iter_mut()
                .find(|key| key.dev_id == dev_id)
                .and_then(|key| {
                    let had_failures = key.consecutive_failures > 0;
                    let was_unhealthy = key.status == KeyStatus::Unhealthy;
                    key.consecutive_failures = 0;
                    if was_unhealthy {
                        key.status = KeyStatus::Healthy;
                    }
                    (had_failures || was_unhealthy)
                        .then_some((key.status, key.consecutive_failures))
                })
        };
        if let Some((status, failures)) = update {
            let _ = self
                .repository
                .update_status(dev_id, status, failures)
                .await;
        }
    }

    pub async fn record_failure(&self, dev_id: &str, key_fault: bool) {
        if !key_fault {
            return;
        }
        let update = {
            let mut inner = self.inner.write().expect("key pool write lock");
            let mut clear_active = false;
            let update = inner
                .keys
                .iter_mut()
                .find(|key| key.dev_id == dev_id)
                .map(|key| {
                    key.consecutive_failures = key.consecutive_failures.saturating_add(1);
                    if key.consecutive_failures >= FAILURE_THRESHOLD {
                        key.status = KeyStatus::Unhealthy;
                        clear_active = true;
                    }
                    (key.status, key.consecutive_failures)
                });
            if clear_active && inner.active_dev_id.as_deref() == Some(dev_id) {
                inner.active_dev_id = None;
            }
            update
        };
        if let Some((status, failures)) = update {
            let _ = self
                .repository
                .update_status(dev_id, status, failures)
                .await;
        }
    }

    pub async fn sync_usage(&self, dev_id: &str) {
        self.sync_usage_internal(dev_id).await;
    }

    pub async fn sync_all_usage(&self) -> Result<(), KeyPoolError> {
        self.flush_usage().await?;
        self.repository.cleanup_rolling_usage().await?;
        let dev_ids: Vec<_> = self
            .inner
            .read()
            .expect("key pool read lock")
            .keys
            .iter()
            .filter(|key| key.dev_id != "test")
            .map(|key| key.dev_id.clone())
            .collect();
        for dev_id in dev_ids {
            self.sync_usage(&dev_id).await;
        }
        self.repository.cleanup_rolling_usage().await
    }

    async fn sync_usage_internal(&self, dev_id: &str) {
        let Some(credential) = self.monitoring_credential(dev_id) else {
            return;
        };
        let Some(usage_probe) = self
            .usage_probe
            .read()
            .expect("usage probe read lock")
            .as_ref()
            .and_then(Weak::upgrade)
        else {
            let _ = self
                .repository
                .record_sync_error(dev_id, "Hi-Rez usage probe is unavailable")
                .await;
            return;
        };
        match usage_probe.get_data_used(&credential).await {
            Ok(Some(report)) => {
                let limit = effective_daily_limit(dev_id, report.reported_limit);
                let status = if has_usable_budget(report.used, limit, self.reserve) {
                    KeyStatus::Healthy
                } else {
                    KeyStatus::Limited
                };
                {
                    let mut inner = self.inner.write().expect("key pool write lock");
                    let mut clear_active = false;
                    if let Some(key) = inner.keys.iter_mut().find(|key| key.dev_id == dev_id) {
                        key.used_today = report.used;
                        key.daily_limit = limit;
                        key.status = status;
                        if status == KeyStatus::Healthy {
                            key.consecutive_failures = 0;
                        } else {
                            clear_active = true;
                        }
                    }
                    if clear_active && inner.active_dev_id.as_deref() == Some(dev_id) {
                        inner.active_dev_id = None;
                    }
                }
                let _ = self
                    .repository
                    .save_authoritative_usage(dev_id, report.used, limit, status)
                    .await;
            }
            Ok(None) => self.try_estimate_revival(dev_id).await,
            Err(error) => {
                let _ = self
                    .repository
                    .record_sync_error(dev_id, &error.to_string())
                    .await;
            }
        }
    }

    async fn try_estimate_revival(&self, dev_id: &str) {
        let now = self.clock.unix_millis();
        let eligible = {
            let inner = self.inner.read().expect("key pool read lock");
            let Some(key) = inner.keys.iter().find(|key| key.dev_id == dev_id) else {
                return;
            };
            matches!(key.status, KeyStatus::Limited | KeyStatus::Unhealthy)
                && now
                    - inner
                        .last_estimate_revival_ms
                        .get(dev_id)
                        .copied()
                        .unwrap_or(0)
                    >= ESTIMATE_REVIVAL_COOLDOWN_MS
        };
        if !eligible {
            return;
        }
        let Ok(estimated) = self.repository.local_usage_estimate(dev_id).await else {
            return;
        };
        let update = {
            let mut inner = self.inner.write().expect("key pool write lock");
            let Some(index) = inner.keys.iter().position(|key| key.dev_id == dev_id) else {
                return;
            };
            let limit = effective_daily_limit(dev_id, Some(inner.keys[index].daily_limit));
            if !has_usable_budget(estimated, limit, self.reserve) {
                return;
            }
            {
                let key = &mut inner.keys[index];
                key.status = KeyStatus::Healthy;
                key.consecutive_failures = 0;
                key.used_today = estimated;
                key.daily_limit = limit;
            }
            inner
                .last_estimate_revival_ms
                .insert(dev_id.to_owned(), now);
            (limit, KeyStatus::Healthy)
        };
        let _ = self
            .repository
            .save_authoritative_usage(dev_id, estimated, update.0, update.1)
            .await;
    }

    pub fn status(&self) -> Vec<ApiKeyStatus> {
        self.inner
            .read()
            .expect("key pool read lock")
            .keys
            .iter()
            .map(|key| format_key(key, self.reserve))
            .collect()
    }

    pub fn monitoring_credential(&self, dev_id: &str) -> Option<ApiCredential> {
        self.inner
            .read()
            .expect("key pool read lock")
            .keys
            .iter()
            .find(|key| key.dev_id == dev_id)
            .map(|key| ApiCredential {
                dev_id: key.dev_id.clone(),
                auth_key: key.auth_key.clone(),
            })
    }
}

#[async_trait]
impl ApiKeyState for KeyPool {
    async fn active_key(&self) -> Result<ApiCredential, UpstreamError> {
        let key = KeyPool::active_key(self)
            .await
            .map_err(|error| UpstreamError::Configuration(error.to_string()))?;
        Ok(ApiCredential {
            dev_id: key.dev_id,
            auth_key: key.auth_key,
        })
    }

    fn increment_usage(&self, dev_id: &str) {
        KeyPool::increment_usage(self, dev_id);
    }

    async fn log_endpoint(
        &self,
        dev_id: &str,
        endpoint: &str,
        response_time_ms: u64,
        consumer: &str,
    ) {
        let _ = self
            .repository
            .log_endpoint(dev_id, endpoint, response_time_ms, consumer)
            .await;
    }

    async fn record_success(&self, dev_id: &str) {
        KeyPool::record_success(self, dev_id).await;
    }

    async fn record_failure(&self, dev_id: &str, key_fault: bool) {
        KeyPool::record_failure(self, dev_id, key_fault).await;
    }

    async fn sync_usage(&self, dev_id: &str) {
        KeyPool::sync_usage(self, dev_id).await;
    }
}

fn configured_daily_limit(_dev_id: &str) -> u64 {
    std::env::var("HIREZ_DEFAULT_DAILY_LIMIT")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|limit| *limit > 0)
        .unwrap_or(7_500)
}

pub fn effective_daily_limit(_dev_id: &str, reported_limit: Option<u64>) -> u64 {
    reported_limit
        .filter(|limit| *limit > 0)
        .unwrap_or_else(|| configured_daily_limit(_dev_id))
}

pub fn is_backup_dev_id(dev_id: &str) -> bool {
    std::env::var("HIREZ_BACKUP_DEV_IDS")
        .map(|value| backup_list_contains(&value, dev_id))
        .unwrap_or(false)
}

fn backup_list_contains(list: &str, dev_id: &str) -> bool {
    list.split(',').any(|candidate| candidate.trim() == dev_id)
}

fn has_usable_budget(used: u64, limit: u64, reserve: u64) -> bool {
    limit.saturating_sub(used) > reserve
}

fn normalize_status(status: &str) -> KeyStatus {
    match status {
        "limited" => KeyStatus::Limited,
        "unhealthy" => KeyStatus::Unhealthy,
        _ => KeyStatus::Healthy,
    }
}

fn format_key(key: &InMemoryKey, reserve: u64) -> ApiKeyStatus {
    ApiKeyStatus {
        dev_id: key.dev_id.clone(),
        auth_key: key.auth_key.clone(),
        status: key.status,
        daily_limit: key.daily_limit,
        used_24h: key.used_today,
        remaining: key.daily_limit.saturating_sub(key.used_today),
        reserve_threshold: reserve,
        calls_total: key.calls_total,
        consecutive_failures: key.consecutive_failures,
        last_used: String::new(),
    }
}

fn json_scalar(value: Option<&serde_json::Value>) -> String {
    match value {
        Some(serde_json::Value::String(value)) => value.trim().to_owned(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use futures::future::join_all;
    use time::macros::datetime;

    use super::*;
    use crate::test_support::FixedClock;

    const TEST_MEK: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    #[derive(Default)]
    struct MemoryRepository {
        rows: StdMutex<Vec<EncryptedKeyRow>>,
        bootstrap: StdMutex<Vec<BootstrapKey>>,
        flushes: StdMutex<Vec<Vec<UsageBatch>>>,
        status_updates: StdMutex<Vec<(String, KeyStatus, u32)>>,
        endpoint_logs: StdMutex<Vec<(String, String, String)>>,
        estimates: StdMutex<HashMap<String, u64>>,
        fail_flush: AtomicBool,
    }

    #[async_trait]
    impl KeyPoolRepository for MemoryRepository {
        async fn ensure_schema(&self) -> Result<(), KeyPoolError> {
            Ok(())
        }

        async fn load_keys(&self) -> Result<Vec<EncryptedKeyRow>, KeyPoolError> {
            Ok(self.rows.lock().expect("rows").clone())
        }

        async fn bootstrap_keys(&self, keys: &[BootstrapKey]) -> Result<usize, KeyPoolError> {
            let mut inserted = 0;
            let mut rows = self.rows.lock().expect("rows");
            for key in keys {
                if rows.iter().any(|row| row.dev_id == key.dev_id) {
                    continue;
                }
                rows.push(EncryptedKeyRow {
                    dev_id: key.dev_id.clone(),
                    encrypted_auth_key: key.encrypted_auth_key.clone(),
                    status: "healthy".to_owned(),
                    total_24h: 0,
                    daily_limit: Some(key.daily_limit),
                    calls_total: 0,
                    consecutive_failures: 0,
                });
                self.bootstrap.lock().expect("bootstrap").push(key.clone());
                inserted += 1;
            }
            Ok(inserted)
        }

        async fn flush_usage(&self, batches: &[UsageBatch]) -> Result<(), KeyPoolError> {
            if self.fail_flush.load(Ordering::Relaxed) {
                return Err(KeyPoolError::Repository("flush failed".to_owned()));
            }
            self.flushes.lock().expect("flushes").push(batches.to_vec());
            let mut rows = self.rows.lock().expect("rows");
            for batch in batches {
                if let Some(row) = rows.iter_mut().find(|row| row.dev_id == batch.dev_id) {
                    row.total_24h += batch.calls;
                    row.calls_total += batch.calls;
                }
            }
            Ok(())
        }

        async fn update_status(
            &self,
            dev_id: &str,
            status: KeyStatus,
            consecutive_failures: u32,
        ) -> Result<(), KeyPoolError> {
            self.status_updates.lock().expect("updates").push((
                dev_id.to_owned(),
                status,
                consecutive_failures,
            ));
            if let Some(row) = self
                .rows
                .lock()
                .expect("rows")
                .iter_mut()
                .find(|row| row.dev_id == dev_id)
            {
                row.status = format!("{status:?}").to_lowercase();
                row.consecutive_failures = consecutive_failures;
            }
            Ok(())
        }

        async fn log_endpoint(
            &self,
            dev_id: &str,
            endpoint: &str,
            _response_time_ms: u64,
            consumer: &str,
        ) -> Result<(), KeyPoolError> {
            self.endpoint_logs.lock().expect("logs").push((
                dev_id.to_owned(),
                endpoint.to_owned(),
                consumer.to_owned(),
            ));
            Ok(())
        }

        async fn save_authoritative_usage(
            &self,
            dev_id: &str,
            used: u64,
            limit: u64,
            status: KeyStatus,
        ) -> Result<(), KeyPoolError> {
            if let Some(row) = self
                .rows
                .lock()
                .expect("rows")
                .iter_mut()
                .find(|row| row.dev_id == dev_id)
            {
                row.total_24h = used;
                row.daily_limit = Some(limit);
                row.status = format!("{status:?}").to_lowercase();
                if status == KeyStatus::Healthy {
                    row.consecutive_failures = 0;
                }
            }
            Ok(())
        }

        async fn local_usage_estimate(&self, dev_id: &str) -> Result<u64, KeyPoolError> {
            Ok(self
                .estimates
                .lock()
                .expect("estimates")
                .get(dev_id)
                .copied()
                .unwrap_or(0))
        }

        async fn record_sync_error(&self, _dev_id: &str, _error: &str) -> Result<(), KeyPoolError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct StaticProbe {
        reports: StdMutex<HashMap<String, Option<AuthoritativeUsage>>>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl UsageProbe for StaticProbe {
        async fn get_data_used(
            &self,
            key: &ApiCredential,
        ) -> Result<Option<AuthoritativeUsage>, KeyPoolError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .reports
                .lock()
                .expect("reports")
                .get(&key.dev_id)
                .cloned()
                .unwrap_or(None))
        }
    }

    fn crypto() -> Arc<KeyCrypto> {
        Arc::new(KeyCrypto::from_hex(TEST_MEK).expect("MEK"))
    }

    fn row(crypto: &KeyCrypto, dev_id: &str, used: u64, status: &str) -> EncryptedKeyRow {
        EncryptedKeyRow {
            dev_id: dev_id.to_owned(),
            encrypted_auth_key: crypto
                .encrypt(&format!("secret-{dev_id}"))
                .expect("encrypt"),
            status: status.to_owned(),
            total_24h: used,
            daily_limit: Some(configured_daily_limit(dev_id)),
            calls_total: 50,
            consecutive_failures: 0,
        }
    }

    async fn pool_with(
        rows: Vec<EncryptedKeyRow>,
        probe: Arc<StaticProbe>,
    ) -> (Arc<KeyPool>, Arc<MemoryRepository>) {
        let repository = Arc::new(MemoryRepository::default());
        *repository.rows.lock().expect("rows") = rows;
        let pool = Arc::new(KeyPool::new(
            DEFAULT_API_KEY_RESERVE_CALLS,
            repository.clone(),
            probe,
            crypto(),
            Arc::new(FixedClock::new(
                datetime!(2026-07-28 23:59:59 UTC).unix_timestamp() * 1_000,
            )),
        ));
        pool.initialize(None).await.expect("initialize");
        (pool, repository)
    }

    #[test]
    fn effective_limit_trusts_reported_and_env_default() {
        // Reported (authoritative) limits are trusted verbatim — no hard-coded clamp.
        assert_eq!(effective_daily_limit("2116", Some(300_000)), 300_000);
        assert_eq!(effective_daily_limit("2504", Some(300_000)), 300_000);
        assert_eq!(effective_daily_limit("2116", Some(10_000)), 10_000);
        assert_eq!(effective_daily_limit("4114", Some(15_000)), 15_000);
        assert_eq!(effective_daily_limit("4114", Some(7_000)), 7_000);
        // No report yet -> configurable default (7500 when env unset).
        assert_eq!(effective_daily_limit("2116", None), 7_500);
        assert_eq!(effective_daily_limit("4114", None), 7_500);
    }

    #[test]
    fn backup_dev_id_parses_env_list() {
        assert!(backup_list_contains("2504, 9999", "2504"));
        assert!(backup_list_contains("2504, 9999", "9999"));
        assert!(backup_list_contains(" 2504 ", "2504"));
        assert!(!backup_list_contains("2504, 9999", "4114"));
        assert!(!backup_list_contains("", "2504"));
    }

    #[tokio::test]
    async fn sticky_waterfall_rotates_at_inclusive_reserve() {
        let crypto = crypto();
        let probe = Arc::new(StaticProbe::default());
        let (pool, _) = pool_with(
            vec![
                row(&crypto, "1000", 7_399, "active"),
                row(&crypto, "2000", 0, "healthy"),
            ],
            probe,
        )
        .await;

        assert_eq!(pool.active_key().await.expect("first").dev_id, "1000");
        assert_eq!(pool.active_key().await.expect("sticky").dev_id, "1000");
        pool.increment_usage("1000");
        assert_eq!(pool.active_key().await.expect("waterfall").dev_id, "2000");
        assert_eq!(pool.status()[0].status, KeyStatus::Limited);
    }

    #[tokio::test]
    async fn concurrent_usage_is_exact_and_flush_is_atomic_per_batch() {
        let crypto = crypto();
        let (pool, repository) = pool_with(
            vec![row(&crypto, "1000", 0, "healthy")],
            Arc::new(StaticProbe::default()),
        )
        .await;
        let tasks = (0..100).map(|_| {
            let pool = pool.clone();
            tokio::spawn(async move {
                for _ in 0..10 {
                    pool.increment_usage("1000");
                }
            })
        });
        for task in join_all(tasks).await {
            task.expect("usage task");
        }
        pool.flush_usage().await.expect("flush");

        assert_eq!(pool.status()[0].used_24h, 1_000);
        assert_eq!(pool.status()[0].calls_total, 1_050);
        assert_eq!(
            repository.flushes.lock().expect("flushes")[0],
            [UsageBatch {
                dev_id: "1000".to_owned(),
                calls: 1_000
            }]
        );
    }

    #[tokio::test]
    async fn failed_flush_restores_pending_increments() {
        let crypto = crypto();
        let (pool, repository) = pool_with(
            vec![row(&crypto, "1000", 0, "healthy")],
            Arc::new(StaticProbe::default()),
        )
        .await;
        pool.increment_usage("1000");
        repository.fail_flush.store(true, Ordering::Relaxed);
        assert!(pool.flush_usage().await.is_err());
        repository.fail_flush.store(false, Ordering::Relaxed);
        pool.flush_usage().await.expect("retry flush");
        assert_eq!(repository.flushes.lock().expect("flushes")[0][0].calls, 1);
    }

    #[tokio::test]
    async fn five_key_faults_mark_unhealthy_and_success_recovers_without_reviving_limited() {
        let crypto = crypto();
        let (pool, _) = pool_with(
            vec![row(&crypto, "1000", 0, "healthy")],
            Arc::new(StaticProbe::default()),
        )
        .await;
        for _ in 0..5 {
            pool.record_failure("1000", true).await;
        }
        assert_eq!(pool.status()[0].status, KeyStatus::Unhealthy);
        pool.record_failure("1000", false).await;
        assert_eq!(pool.status()[0].consecutive_failures, 5);
        pool.record_success("1000").await;
        assert_eq!(pool.status()[0].status, KeyStatus::Healthy);
        assert_eq!(pool.status()[0].consecutive_failures, 0);

        for _ in 0..7_400 {
            pool.increment_usage("1000");
        }
        assert_eq!(pool.status()[0].status, KeyStatus::Limited);
        pool.record_success("1000").await;
        assert_eq!(pool.status()[0].status, KeyStatus::Limited);
    }

    #[tokio::test]
    async fn exhausted_pool_revival_is_single_flight_for_concurrent_callers() {
        let crypto = crypto();
        let probe = Arc::new(StaticProbe::default());
        probe.reports.lock().expect("reports").insert(
            "1000".to_owned(),
            Some(AuthoritativeUsage {
                used: 0,
                reported_limit: Some(7_500),
            }),
        );
        let (pool, _) =
            pool_with(vec![row(&crypto, "1000", 7_500, "limited")], probe.clone()).await;

        let results = join_all((0..32).map(|_| {
            let pool = pool.clone();
            async move { pool.active_key().await.expect("revived").dev_id }
        }))
        .await;

        assert!(results.iter().all(|dev_id| dev_id == "1000"));
        assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
        assert_eq!(pool.status()[0].used_24h, 0);
        assert_eq!(pool.status()[0].status, KeyStatus::Healthy);
    }

    #[tokio::test]
    async fn reload_flushes_pending_usage_before_replacing_keys() {
        let crypto = crypto();
        let (pool, repository) = pool_with(
            vec![row(&crypto, "1000", 0, "healthy")],
            Arc::new(StaticProbe::default()),
        )
        .await;
        pool.increment_usage("1000");
        pool.reload(None).await.expect("reload");

        assert_eq!(pool.status()[0].used_24h, 1);
        assert_eq!(pool.status()[0].calls_total, 51);
        assert_eq!(repository.flushes.lock().expect("flushes").len(), 1);
    }

    #[tokio::test]
    async fn key_file_bootstrap_encrypts_plaintext_and_preserves_existing_rows() {
        let repository = Arc::new(MemoryRepository::default());
        let crypto = crypto();
        repository
            .rows
            .lock()
            .expect("rows")
            .push(row(&crypto, "1000", 5, "healthy"));
        let pool = KeyPool::new(
            DEFAULT_API_KEY_RESERVE_CALLS,
            repository.clone(),
            Arc::new(StaticProbe::default()),
            crypto.clone(),
            Arc::new(FixedClock::new(
                datetime!(2026-07-28 23:59:59 UTC).unix_timestamp() * 1_000,
            )),
        );
        let path = std::env::temp_dir().join(format!("pc-rust-keys-{}.json", uuid::Uuid::new_v4()));
        fs::write(
            &path,
            r#"{"keys":[{"devId":"1000","authKey":"existing"},{"dev_id":2000,"auth_key":"new-secret"}]}"#,
        )
        .expect("write fixture");

        pool.initialize(Some(&path)).await.expect("initialize");
        fs::remove_file(&path).expect("remove fixture");

        let bootstrapped = repository.bootstrap.lock().expect("bootstrap");
        assert_eq!(bootstrapped.len(), 1);
        assert_eq!(bootstrapped[0].dev_id, "2000");
        assert_eq!(
            crypto
                .decrypt(&bootstrapped[0].encrypted_auth_key)
                .expect("decrypt"),
            "new-secret"
        );
        assert_eq!(pool.status().len(), 2);
    }
}
