use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::RwLock;

use crate::{cache::RedisCache, database::format_json_timestamp};

pub const DEPLOYMENT_STATE_KEY: &str = "paladinscat:deployment:state";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentPhase {
    Idle,
    Announced,
    Draining,
    Switching,
    Warming,
    Complete,
    Failed,
}

impl DeploymentPhase {
    pub fn is_blocking(self) -> bool {
        matches!(self, Self::Draining | Self::Switching | Self::Warming)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentState {
    pub id: String,
    pub phase: DeploymentPhase,
    pub message: Option<String>,
    pub started_at: Option<String>,
    pub updated_at: String,
    pub expires_at: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DeploymentStateInput {
    pub id: String,
    pub phase: DeploymentPhase,
    pub message: Option<String>,
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum DeploymentError {
    #[error("Deployment id is required")]
    MissingDeploymentId,
    #[error("Redis could not persist deployment state")]
    RedisUnavailable,
}

#[derive(Clone)]
pub struct DeploymentControl {
    redis: RedisCache,
    local: Arc<RwLock<DeploymentState>>,
}

impl DeploymentControl {
    pub fn new(redis: RedisCache) -> Self {
        Self {
            redis,
            local: Arc::new(RwLock::new(idle_state(OffsetDateTime::UNIX_EPOCH))),
        }
    }

    pub fn with_local_state(redis: RedisCache, state: DeploymentState) -> Self {
        Self {
            redis,
            local: Arc::new(RwLock::new(state)),
        }
    }

    pub async fn initialize(&self, redis_startup_timeout: Duration) -> DeploymentState {
        let _ = self.redis.wait_ready(redis_startup_timeout).await;
        let now = OffsetDateTime::now_utc();
        let state = self
            .redis
            .get::<DeploymentState>(DEPLOYMENT_STATE_KEY)
            .await
            .filter(|state| !is_expired(state, now))
            .unwrap_or_else(|| idle_state(now));
        self.apply_local(state.clone()).await;
        state
    }

    pub async fn local_state(&self) -> DeploymentState {
        self.local_state_at(OffsetDateTime::now_utc()).await
    }

    pub async fn local_state_at(&self, now: OffsetDateTime) -> DeploymentState {
        let current = self.local.read().await.clone();
        if is_expired(&current, now) {
            let idle = idle_state(now);
            self.apply_local(idle.clone()).await;
            idle
        } else {
            current
        }
    }

    pub async fn refresh(&self) -> DeploymentState {
        let now = OffsetDateTime::now_utc();
        let state = self
            .redis
            .get::<DeploymentState>(DEPLOYMENT_STATE_KEY)
            .await
            .filter(|state| !is_expired(state, now))
            .unwrap_or_else(|| idle_state(now));
        self.apply_local(state.clone()).await;
        state
    }

    pub async fn set_state(
        &self,
        input: DeploymentStateInput,
    ) -> Result<DeploymentState, DeploymentError> {
        let now = OffsetDateTime::now_utc();
        let previous = self.local_state_at(now).await;
        let (state, ttl_seconds) = build_state(input, &previous, now)?;
        if !self
            .redis
            .set_required(DEPLOYMENT_STATE_KEY, &state, Some(ttl_seconds))
            .await
        {
            return Err(DeploymentError::RedisUnavailable);
        }
        self.apply_local(state.clone()).await;
        Ok(state)
    }

    async fn apply_local(&self, state: DeploymentState) {
        *self.local.write().await = state;
    }
}

fn build_state(
    input: DeploymentStateInput,
    previous: &DeploymentState,
    now: OffsetDateTime,
) -> Result<(DeploymentState, u64), DeploymentError> {
    let id = truncate_chars(input.id.trim(), 128);
    if id.is_empty() && input.phase != DeploymentPhase::Idle {
        return Err(DeploymentError::MissingDeploymentId);
    }
    let ttl_seconds = input.ttl_seconds.unwrap_or(1_800).clamp(30, 7_200);
    let started_at = if previous.id == id {
        previous
            .started_at
            .clone()
            .or_else(|| Some(format_json_timestamp(now)))
    } else {
        Some(format_json_timestamp(now))
    };
    Ok((
        DeploymentState {
            id,
            phase: input.phase,
            message: input
                .message
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| truncate_chars(value, 500)),
            started_at,
            updated_at: format_json_timestamp(now),
            expires_at: Some(format_json_timestamp(
                now + TimeDuration::seconds(ttl_seconds as i64),
            )),
        },
        ttl_seconds,
    ))
}

fn idle_state(now: OffsetDateTime) -> DeploymentState {
    DeploymentState {
        id: String::new(),
        phase: DeploymentPhase::Idle,
        message: None,
        started_at: None,
        updated_at: format_json_timestamp(now),
        expires_at: None,
    }
}

fn is_expired(state: &DeploymentState, now: OffsetDateTime) -> bool {
    state
        .expires_at
        .as_deref()
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
        .is_some_and(|expires_at| expires_at <= now)
}

fn truncate_chars(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("timestamp")
    }

    #[test]
    fn phases_match_the_typescript_blocking_contract() {
        assert!(!DeploymentPhase::Idle.is_blocking());
        assert!(!DeploymentPhase::Announced.is_blocking());
        assert!(DeploymentPhase::Draining.is_blocking());
        assert!(DeploymentPhase::Switching.is_blocking());
        assert!(DeploymentPhase::Warming.is_blocking());
        assert!(!DeploymentPhase::Complete.is_blocking());
        assert!(!DeploymentPhase::Failed.is_blocking());
    }

    #[test]
    fn state_builder_clamps_and_normalizes_the_public_contract() {
        let previous = idle_state(OffsetDateTime::UNIX_EPOCH);
        let (state, ttl) = build_state(
            DeploymentStateInput {
                id: format!("  {}  ", "d".repeat(140)),
                phase: DeploymentPhase::Draining,
                message: Some(format!("  {}  ", "m".repeat(520))),
                ttl_seconds: Some(2),
            },
            &previous,
            at(1_000),
        )
        .expect("state");
        assert_eq!(state.id.len(), 128);
        assert_eq!(state.message.as_deref().map(str::len), Some(500));
        assert_eq!(
            state.started_at.as_deref(),
            Some("1970-01-01T00:16:40.000Z")
        );
        assert_eq!(state.updated_at, "1970-01-01T00:16:40.000Z");
        assert_eq!(
            state.expires_at.as_deref(),
            Some("1970-01-01T00:17:10.000Z")
        );
        assert_eq!(ttl, 30);
    }

    #[test]
    fn state_builder_preserves_start_time_for_the_same_deployment() {
        let previous = DeploymentState {
            id: "deploy-1".to_owned(),
            phase: DeploymentPhase::Announced,
            message: None,
            started_at: Some("1970-01-01T00:10:00.000Z".to_owned()),
            updated_at: "1970-01-01T00:10:00.000Z".to_owned(),
            expires_at: Some("1970-01-01T00:40:00.000Z".to_owned()),
        };
        let (state, ttl) = build_state(
            DeploymentStateInput {
                id: "deploy-1".to_owned(),
                phase: DeploymentPhase::Warming,
                message: Some(" warming ".to_owned()),
                ttl_seconds: Some(9_000),
            },
            &previous,
            at(1_000),
        )
        .expect("state");
        assert_eq!(
            state.started_at.as_deref(),
            Some("1970-01-01T00:10:00.000Z")
        );
        assert_eq!(state.message.as_deref(), Some("warming"));
        assert_eq!(ttl, 7_200);
    }

    #[test]
    fn non_idle_state_requires_an_identifier() {
        assert_eq!(
            build_state(
                DeploymentStateInput {
                    id: " ".to_owned(),
                    phase: DeploymentPhase::Switching,
                    message: None,
                    ttl_seconds: None,
                },
                &idle_state(OffsetDateTime::UNIX_EPOCH),
                at(1_000),
            ),
            Err(DeploymentError::MissingDeploymentId)
        );
    }

    #[tokio::test]
    async fn expired_local_state_becomes_idle_without_redis_io() {
        let control =
            DeploymentControl::new(RedisCache::new("redis://127.0.0.1:9").expect("redis"));
        control
            .apply_local(DeploymentState {
                id: "deploy-1".to_owned(),
                phase: DeploymentPhase::Draining,
                message: None,
                started_at: Some("1970-01-01T00:10:00.000Z".to_owned()),
                updated_at: "1970-01-01T00:10:00.000Z".to_owned(),
                expires_at: Some("1970-01-01T00:11:00.000Z".to_owned()),
            })
            .await;
        let state = control.local_state_at(at(1_000)).await;
        assert_eq!(state.phase, DeploymentPhase::Idle);
        assert_eq!(state.updated_at, "1970-01-01T00:16:40.000Z");
    }
}
