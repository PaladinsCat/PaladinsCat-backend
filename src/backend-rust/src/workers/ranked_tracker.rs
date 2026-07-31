use paladinscat_core::{
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use serde::Serialize;
use serde_json::{Value, json};

use super::relay::{WorkerRelayClient, WorkerRelayError};

#[derive(Debug, thiserror::Error)]
pub enum RankedTrackerError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Relay(#[from] WorkerRelayError),
    #[error("current ranked season is unavailable")]
    MissingSeason,
    #[error("no leaderboard players returned across tiers 21-26")]
    EmptyLeaderboard,
}

#[derive(Clone, Debug, Serialize)]
pub struct RankedTrackerSummary {
    pub season: i32,
    pub players: i32,
    pub failed_tiers: Vec<i32>,
}

#[derive(Clone)]
pub struct RankedTracker {
    database: Database,
    relay: WorkerRelayClient,
}

impl RankedTracker {
    pub fn new(database: Database, config: &BackendConfig) -> Result<Self, WorkerRelayError> {
        Ok(Self {
            database,
            relay: WorkerRelayClient::new(config)?,
        })
    }

    pub async fn track(&self) -> Result<RankedTrackerSummary, RankedTrackerError> {
        let job = self.database.one_json(
            "INSERT INTO sync_jobs(job_type,status,started_at) VALUES('ranked_tracker','running',now()) RETURNING id",
            &[],
        ).await?;
        let job_id = job
            .as_ref()
            .and_then(|row| integer(row, "id"))
            .unwrap_or_default();
        let result = self.track_inner().await;
        let (status, players, error) = match &result {
            Ok(summary) if summary.failed_tiers.is_empty() => ("completed", summary.players, None),
            Ok(summary) => (
                "failed",
                summary.players,
                Some(format!("Failed tiers: {:?}", summary.failed_tiers)),
            ),
            Err(error) => ("failed", 0, Some(error.to_string())),
        };
        let _ = self.database.query_json(
            "UPDATE sync_jobs SET status=$2,completed_at=now(),players_processed=$3,error_message=$4 WHERE id=$1",
            &[&job_id, &status, &players, &error],
        ).await;
        result
    }

    async fn track_inner(&self) -> Result<RankedTrackerSummary, RankedTrackerError> {
        let seasons = self
            .relay
            .call_value(
                "getLeagueSeasons",
                vec![json!(486)],
                "ranked_leaderboard_tracker",
            )
            .await?;
        let season = seasons
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| {
                ["Season", "season", "Id", "id"]
                    .iter()
                    .find_map(|key| integer(row, key))
            })
            .and_then(|value| i32::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or(RankedTrackerError::MissingSeason)?;
        let mut total = 0_i32;
        let mut failed_tiers = Vec::new();
        for tier in 21_i32..=26 {
            match self.fetch_tier(tier, season).await {
                Ok(count) => total = total.saturating_add(count),
                Err(error) => {
                    tracing::error!(tier, %error, "ranked tier fetch failed");
                    failed_tiers.push(tier);
                }
            }
        }
        if total == 0 && failed_tiers.is_empty() {
            return Err(RankedTrackerError::EmptyLeaderboard);
        }
        Ok(RankedTrackerSummary {
            season,
            players: total,
            failed_tiers,
        })
    }

    async fn fetch_tier(&self, tier: i32, season: i32) -> Result<i32, RankedTrackerError> {
        let data = self
            .relay
            .call_value(
                "getLeagueLeaderboard",
                vec![json!(486), json!(tier), json!(season)],
                "ranked_leaderboard_tracker",
            )
            .await?;
        let entries = data.as_array().cloned().unwrap_or_default();
        let normalized = Value::Array(
            entries
                .iter()
                .enumerate()
                .filter_map(|(index, row)| {
                    let player_id = ["ActivePlayerId", "playerId", "player_id"]
                        .iter()
                        .find_map(|key| integer(row, key))?;
                    if player_id <= 0 {
                        return None;
                    }
                    let name = ["Name", "name"]
                        .iter()
                        .find_map(|key| row.get(*key)?.as_str())
                        .unwrap_or_default()
                        .replace('\0', "");
                    let value = |keys: &[&str]| {
                        keys.iter()
                            .find_map(|key| integer(row, key))
                            .unwrap_or_default()
                    };
                    Some(json!({
                        "player_id":player_id,
                        "name":name,
                        "points":value(&["Points","points"]),
                        "wins":value(&["Wins","wins"]),
                        "losses":value(&["Losses","losses"]),
                        "leaves":value(&["Leaves","leaves"]),
                        "rank":index+1,
                    }))
                })
                .collect(),
        );
        let count = normalized.as_array().map_or(0, Vec::len);
        self.database.query_json(
            "WITH incoming AS(SELECT * FROM jsonb_to_recordset($1::JSONB) AS row(\
               player_id BIGINT,name TEXT,points INT,wins INT,losses INT,leaves INT,rank INT\
             )),prepared AS(SELECT incoming.*,COALESCE(previous.rank,0) prev_rank,previous.tier prev_tier \
               FROM incoming LEFT JOIN leaderboard_current previous USING(player_id))\
             INSERT INTO leaderboard_current(player_id,name,tier,points,rank,prev_rank,prev_tier,trend,tier_change,wins,losses,leaves,season,updated_at)\
             SELECT player_id,name,$2,points,rank,prev_rank,prev_tier,\
               CASE WHEN prev_rank>0 THEN prev_rank-rank ELSE 0 END,CASE WHEN prev_tier IS NOT NULL THEN $2-prev_tier ELSE 0 END,\
               wins,losses,leaves,$3,now() FROM prepared ON CONFLICT(player_id) DO UPDATE SET\
               name=EXCLUDED.name,tier=EXCLUDED.tier,points=EXCLUDED.points,rank=EXCLUDED.rank,prev_rank=EXCLUDED.prev_rank,\
               prev_tier=EXCLUDED.prev_tier,trend=EXCLUDED.trend,tier_change=EXCLUDED.tier_change,wins=EXCLUDED.wins,\
               losses=EXCLUDED.losses,leaves=EXCLUDED.leaves,season=EXCLUDED.season,updated_at=now()",
            &[&normalized, &tier, &season],
        ).await?;
        Ok(i32::try_from(count).unwrap_or(i32::MAX))
    }

    pub async fn top_players(&self, tier: i32, limit: i64) -> Result<Vec<Value>, DatabaseError> {
        self.database
            .query_json(
                "SELECT *,CASE WHEN prev_rank IS NULL THEN 0 ELSE prev_rank-rank END AS trend \
             FROM leaderboard_current WHERE tier=$1 ORDER BY points DESC LIMIT $2",
                &[&tier, &limit],
            )
            .await
    }

    pub async fn all_players(&self, limit: i64) -> Result<Vec<Value>, DatabaseError> {
        self.database
            .query_json(
                "SELECT * FROM leaderboard_current ORDER BY tier ASC,points DESC LIMIT $1",
                &[&limit],
            )
            .await
    }
}

fn integer(row: &Value, key: &str) -> Option<i64> {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
