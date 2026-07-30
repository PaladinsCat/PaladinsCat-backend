use serde_json::Value;
use thiserror::Error;

use crate::model::MatchDetails;

#[derive(Debug, Error)]
pub enum RelayError {
    #[error("{0}")]
    Validation(String),
    #[error("{0}")]
    Upstream(String),
    #[error("{0}")]
    Unsupported(String),
    #[error("{0}")]
    Unavailable(String),
}

impl RelayError {
    pub fn status_code(&self) -> u16 {
        match self {
            Self::Validation(_) => 400,
            Self::Unsupported(_) => 400,
            Self::Unavailable(_) => 503,
            Self::Upstream(_) => 502,
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Validation(_) | Self::Unsupported(_) => "VALIDATION_ERROR",
            Self::Upstream(_) => "RELAY_OPERATION_FAILED",
            Self::Unavailable(_) => "RELAY_NOT_READY",
        }
    }
}

pub trait CompletedMatchProvider: Send + Sync {
    async fn get_match_details_batch(
        &self,
        match_ids: &[u64],
    ) -> Result<Vec<MatchDetails>, RelayError>;

    async fn get_player_batch_from_match(&self, match_id: u64) -> Result<Vec<Value>, RelayError>;

    async fn get_match_history(
        &self,
        player_id: u64,
        match_id: u64,
    ) -> Result<Vec<Value>, RelayError>;

    async fn get_match_history_with_usage(
        &self,
        player_id: u64,
        match_id: u64,
    ) -> Result<(Vec<Value>, u32), RelayError> {
        self.get_match_history(player_id, match_id)
            .await
            .map(|rows| (rows, 1))
    }

    async fn get_demo_details(&self, match_id: u64) -> Result<Value, RelayError>;

    async fn get_local_recovery_players(&self, _match_id: u64) -> Result<Vec<Value>, RelayError> {
        Ok(Vec::new())
    }
}
