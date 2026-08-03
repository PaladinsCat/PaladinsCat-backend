use std::collections::BTreeSet;
use std::time::Duration;

use paladinscat_core::{
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use serde_json::{Value, json};
use tokio_postgres::types::ToSql;

use crate::raw_hirez_audit::{RawHirezAudit, record_raw_hirez_response};

use super::{
    policy::{ACTIVITY_PROFILE_BATCH_SIZE, ACTIVITY_PROFILE_TTL_HOURS},
    relay::{WorkerRelayClient, WorkerRelayError},
};

const SEED_UNKNOWN_IDENTITIES_SQL: &str = r#"
INSERT INTO player_activity_profile_refresh (player_id)
SELECT player_id
FROM player_presence_24h
WHERE last_observed_at >= now() - interval '24 hours'
ON CONFLICT (player_id) DO NOTHING
"#;

const RECONCILE_PRESENCE_SQL: &str = r#"
WITH recent_discoveries AS MATERIALIZED (
  SELECT d.match_id,d.queue_id,q.queue_name,q.stats_scope,q.participant_model,
    (COALESCE(d.entry_datetime AT TIME ZONE 'UTC',d.source_date+(d.source_hour*interval '1 hour')) AT TIME ZONE 'UTC') observed_at
  FROM match_count_discoveries d JOIN queue_types q ON q.queue_id=d.queue_id
  WHERE d.source_date>=((now() AT TIME ZONE 'UTC')-interval '25 hours')::date
    AND COALESCE(d.entry_datetime AT TIME ZONE 'UTC',d.source_date+(d.source_hour*interval '1 hour'))>=(now() AT TIME ZONE 'UTC')-interval '24 hours'
    AND q.track_presence=TRUE
),roster_evidence AS MATERIALIZED (
  SELECT mp.player_id,mp.match_id,d.queue_id,d.stats_scope,d.observed_at,
    CASE WHEN mp.player_id>0 THEN 'human' WHEN UPPER(COALESCE(mp.player_name,''))='PRIVATEACCOUNT' OR COALESCE(mp.private_slot,0)>0 THEN 'private' ELSE 'unknown' END participant_kind
  FROM recent_discoveries d JOIN match_players mp ON mp.match_id=d.match_id
  WHERE mp.entry_datetime>=now()-interval '25 hours' AND COALESCE(mp.source,'direct') IN('direct','recovered')
  UNION ALL
  SELECT cmp.player_id,cmp.match_id,d.queue_id,d.stats_scope,d.observed_at,COALESCE(cmp.participant_kind,'human')
  FROM recent_discoveries d JOIN casual_match_players cmp ON cmp.match_id=d.match_id
  UNION ALL
  SELECT smp.player_id,smp.match_id,d.queue_id,d.stats_scope,d.observed_at,COALESCE(smp.participant_kind,'human')
  FROM recent_discoveries d JOIN special_match_players smp ON smp.match_id=d.match_id
),participation AS MATERIALIZED (
  SELECT player_id,match_id,queue_id,stats_scope,observed_at FROM roster_evidence
  WHERE player_id>0 AND participant_kind='human'
),deduplicated_participation AS MATERIALIZED (
  SELECT DISTINCT player_id,match_id,queue_id,stats_scope,observed_at FROM participation
),global_rows AS MATERIALIZED (
  SELECT DISTINCT ON(player_id) player_id,MIN(observed_at) OVER(PARTITION BY player_id) first_observed_at,
    observed_at last_observed_at,match_id last_match_id,queue_id last_queue_id,stats_scope last_stats_scope
  FROM deduplicated_participation ORDER BY player_id,observed_at DESC,match_id DESC,queue_id DESC
),queue_rows AS MATERIALIZED (
  SELECT DISTINCT ON(player_id,queue_id) player_id,queue_id,stats_scope,
    MIN(observed_at) OVER(PARTITION BY player_id,queue_id) first_observed_at,
    observed_at last_observed_at,match_id last_match_id
  FROM deduplicated_participation ORDER BY player_id,queue_id,observed_at DESC,match_id DESC
),global_upsert AS (
  INSERT INTO player_presence_24h(player_id,first_observed_at,last_observed_at,last_match_id,last_queue_id,last_stats_scope)
  SELECT player_id,first_observed_at,last_observed_at,last_match_id,last_queue_id,last_stats_scope FROM global_rows
  ON CONFLICT(player_id) DO UPDATE SET
    first_observed_at=LEAST(player_presence_24h.first_observed_at,EXCLUDED.first_observed_at),
    last_observed_at=GREATEST(player_presence_24h.last_observed_at,EXCLUDED.last_observed_at),
    last_match_id=CASE WHEN EXCLUDED.last_observed_at>=player_presence_24h.last_observed_at THEN EXCLUDED.last_match_id ELSE player_presence_24h.last_match_id END,
    last_queue_id=CASE WHEN EXCLUDED.last_observed_at>=player_presence_24h.last_observed_at THEN EXCLUDED.last_queue_id ELSE player_presence_24h.last_queue_id END,
    last_stats_scope=CASE WHEN EXCLUDED.last_observed_at>=player_presence_24h.last_observed_at THEN EXCLUDED.last_stats_scope ELSE player_presence_24h.last_stats_scope END,
    updated_at=now() RETURNING player_id
)
INSERT INTO player_queue_presence_24h(player_id,queue_id,stats_scope,first_observed_at,last_observed_at,last_match_id)
SELECT player_id,queue_id,stats_scope,first_observed_at,last_observed_at,last_match_id FROM queue_rows
ON CONFLICT(player_id,queue_id) DO UPDATE SET
  first_observed_at=LEAST(player_queue_presence_24h.first_observed_at,EXCLUDED.first_observed_at),
  last_observed_at=GREATEST(player_queue_presence_24h.last_observed_at,EXCLUDED.last_observed_at),
  last_match_id=CASE WHEN EXCLUDED.last_observed_at>=player_queue_presence_24h.last_observed_at THEN EXCLUDED.last_match_id ELSE player_queue_presence_24h.last_match_id END,
  stats_scope=EXCLUDED.stats_scope,updated_at=now()
"#;

const CLAIM_UNKNOWN_IDENTITIES_SQL: &str = r#"
WITH due AS (
  SELECT refresh.player_id
  FROM player_activity_profile_refresh refresh
  JOIN player_presence_24h presence
    ON presence.player_id = refresh.player_id
   AND presence.last_observed_at >= now() - interval '24 hours'
  LEFT JOIN LATERAL (
    SELECT profile.hirez_profile_refreshed_at
    FROM players profile
    WHERE profile.id = refresh.player_id
       OR (profile.active_player_id = refresh.player_id AND profile.active_player_id > 0)
    ORDER BY CASE WHEN profile.id = refresh.player_id THEN 0 ELSE 1 END,
             profile.hirez_profile_refreshed_at DESC NULLS LAST
    LIMIT 1
  ) profile ON TRUE
  WHERE (profile.hirez_profile_refreshed_at IS NULL
      OR profile.hirez_profile_refreshed_at < now() - ($3::int * interval '1 hour'))
    AND (
      refresh.status = 'pending'
      OR (refresh.status = 'failed' AND refresh.next_retry_at <= now())
      OR (refresh.status = 'fetching' AND refresh.lease_until <= now())
    )
    AND (refresh.lease_until IS NULL OR refresh.lease_until <= now())
  ORDER BY presence.last_observed_at DESC, refresh.player_id
  LIMIT $1
  FOR UPDATE OF refresh SKIP LOCKED
)
UPDATE player_activity_profile_refresh refresh
SET status = 'fetching',
    lease_until = now() + ($2::int * interval '1 second'),
    error_message = NULL,
    updated_at = now()
FROM due
WHERE refresh.player_id = due.player_id
RETURNING refresh.player_id, false AS needs_platform, false AS needs_region
"#;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedPlayerIdentity {
    pub player_id: i64,
    pub needs_platform: bool,
    pub needs_region: bool,
}

#[derive(Clone)]
pub struct ProfileEnrichmentRepository {
    database: Database,
}

const PROFILE_CLAIM_LEASE: Duration = Duration::from_secs(30 * 60);
const FAILED_RETRY_MINUTES: i32 = 60;

#[derive(Clone, Copy, Debug, Default)]
pub struct ProfileEnrichmentResult {
    pub calls: usize,
    pub claimed: usize,
    pub refreshed: usize,
    pub unavailable: usize,
    pub skipped_recent: usize,
    pub failed: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum ProfileEnrichmentError {
    #[error("Hi-Rez returned no usable player profile")]
    InvalidProfile,
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Relay(#[from] WorkerRelayError),
    #[error(transparent)]
    Query(#[from] tokio_postgres::Error),
}

impl ProfileEnrichmentRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn claim_unknown_batches(
        &self,
        max_calls: usize,
        lease: Duration,
    ) -> Result<Vec<Vec<ClaimedPlayerIdentity>>, DatabaseError> {
        if max_calls == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(max_calls.saturating_mul(ACTIVITY_PROFILE_BATCH_SIZE))
            .unwrap_or(i64::MAX);
        let lease_seconds = i32::try_from(lease.as_secs()).unwrap_or(i32::MAX);
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        transaction
            .execute(SEED_UNKNOWN_IDENTITIES_SQL, &[])
            .await?;
        let rows = transaction
            .query(
                CLAIM_UNKNOWN_IDENTITIES_SQL,
                &[&limit, &lease_seconds, &ACTIVITY_PROFILE_TTL_HOURS],
            )
            .await?;
        transaction.commit().await?;
        let claimed: Vec<_> = rows
            .into_iter()
            .map(|row| ClaimedPlayerIdentity {
                player_id: row.get("player_id"),
                needs_platform: row.get("needs_platform"),
                needs_region: row.get("needs_region"),
            })
            .collect();
        Ok(claimed
            .chunks(ACTIVITY_PROFILE_BATCH_SIZE)
            .map(<[ClaimedPlayerIdentity]>::to_vec)
            .collect())
    }

    pub async fn run(
        &self,
        config: &BackendConfig,
        max_calls: usize,
        reason: &str,
    ) -> Result<ProfileEnrichmentResult, ProfileEnrichmentError> {
        self.database
            .query_json(RECONCILE_PRESENCE_SQL, &[])
            .await?;
        let batches = self
            .claim_unknown_batches(max_calls, PROFILE_CLAIM_LEASE)
            .await?;
        let relay = WorkerRelayClient::new(config)?;
        let mut result = ProfileEnrichmentResult::default();
        for (index, batch) in batches.iter().enumerate() {
            result.claimed += batch.len();
            let claimed_ids = batch.iter().map(|row| row.player_id).collect::<Vec<_>>();
            let (ids, recent) = self.filter_still_stale(&claimed_ids).await?;
            if !recent.is_empty() {
                self.mark_skipped_recent(&recent).await?;
                result.skipped_recent += recent.len();
            }
            if ids.is_empty() {
                continue;
            }
            let response = relay
                .call_value("getPlayerBatch", vec![json!(ids)], "backend_unattributed")
                .await;
            let payload = match response {
                Ok(payload) => payload,
                Err(error) => {
                    let message = error.to_string();
                    self.database
                        .query_json(
                            "UPDATE player_activity_profile_refresh SET status='failed',attempts=attempts+1,\
                             last_attempt_at=now(),error_message=$2,lease_until=NULL,\
                             next_retry_at=now()+($3::int*INTERVAL '1 minute'),updated_at=now() \
                             WHERE player_id=ANY($1::BIGINT[])",
                            &[&ids, &message, &FAILED_RETRY_MINUTES],
                        )
                        .await?;
                    result.failed += ids.len();
                    for unprocessed in batches.iter().skip(index + 1) {
                        self.release_unprocessed(
                            &unprocessed
                                .iter()
                                .map(|claim| claim.player_id)
                                .collect::<Vec<_>>(),
                        )
                        .await?;
                    }
                    break;
                }
            };
            result.calls += 1;
            if let Err(error) = record_raw_hirez_response(
                &self.database,
                RawHirezAudit {
                    endpoint: "getplayerbatch",
                    operation: "getPlayerBatch",
                    entity_type: "player_activity_profile_enrichment",
                    entity_id: String::new(),
                    params: json!({"playerIds":ids,"reason":reason}),
                    raw_response: &payload,
                    source: "player-activity-profile-enrichment",
                },
            )
            .await
            {
                let message = error.to_string();
                self.database
                    .query_json(
                        "UPDATE player_activity_profile_refresh SET status='failed',attempts=attempts+1,last_attempt_at=now(),error_message=$2,lease_until=NULL,next_retry_at=now()+($3::int*INTERVAL '1 minute'),updated_at=now() WHERE player_id=ANY($1::BIGINT[])",
                        &[&ids, &message, &FAILED_RETRY_MINUTES],
                    )
                    .await?;
                result.failed += ids.len();
                for unprocessed in batches.iter().skip(index + 1) {
                    self.release_unprocessed(
                        &unprocessed
                            .iter()
                            .map(|claim| claim.player_id)
                            .collect::<Vec<_>>(),
                    )
                    .await?;
                }
                break;
            }
            let mut rows = Vec::new();
            for profile in payload.as_array().into_iter().flatten() {
                if !value_text(profile, &["ret_msg"])
                    .unwrap_or_default()
                    .is_empty()
                {
                    continue;
                }
                match persist_player_profile(&self.database, profile).await {
                    Ok(_) => rows.push(profile.clone()),
                    Err(error) => tracing::error!(%error, "profile enrichment persistence failed"),
                }
            }
            let requested = ids.iter().copied().collect::<BTreeSet<_>>();
            let mut returned = std::collections::BTreeMap::new();
            for profile in &rows {
                for player_id in profile_identity_ids(profile) {
                    if requested.contains(&player_id) {
                        returned.insert(player_id, profile);
                    }
                }
            }
            for claim in batch {
                if !ids.contains(&claim.player_id) {
                    continue;
                }
                let Some(_) = returned.get(&claim.player_id) else {
                    self.finish_unavailable(
                        claim.player_id,
                        "Hi-Rez getplayerbatch returned no usable profile for this player ID.",
                    )
                    .await?;
                    result.unavailable += 1;
                    continue;
                };
                self.database
                    .query_json(
                        "UPDATE player_activity_profile_refresh SET status='success',attempts=attempts+1,last_attempt_at=now(),\
                         last_success_at=now(),lease_until=NULL,error_message=NULL,next_retry_at=NULL,updated_at=now() \
                         WHERE player_id=$1",
                        &[&claim.player_id],
                    )
                    .await?;
                result.refreshed += 1;
            }
        }
        self.cleanup_old_state().await?;
        Ok(result)
    }

    async fn finish_unavailable(&self, player_id: i64, reason: &str) -> Result<(), DatabaseError> {
        self.database
            .query_json(
                "UPDATE player_activity_profile_refresh SET status='unavailable',attempts=attempts+1,\
                 last_attempt_at=now(),error_message=$2,lease_until=NULL,next_retry_at=NULL,updated_at=now() \
                 WHERE player_id=$1",
                &[&player_id, &reason],
            )
            .await?;
        Ok(())
    }

    async fn filter_still_stale(
        &self,
        player_ids: &[i64],
    ) -> Result<(Vec<i64>, Vec<i64>), DatabaseError> {
        if player_ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let rows = self
            .database
            .query_json(
                "SELECT requested.player_id,COALESCE(resolved.hirez_profile_refreshed_at>=now()-($2::int*INTERVAL '1 hour'),FALSE) is_recent \
                 FROM unnest($1::BIGINT[]) requested(player_id) LEFT JOIN LATERAL(\
                   SELECT candidate.hirez_profile_refreshed_at FROM(\
                     SELECT profile.hirez_profile_refreshed_at,0 identity_priority FROM players profile WHERE profile.id=requested.player_id \
                     UNION ALL SELECT profile.hirez_profile_refreshed_at,1 identity_priority FROM players profile WHERE profile.active_player_id=requested.player_id AND profile.active_player_id>0 AND profile.id<>requested.player_id\
                   ) candidate ORDER BY candidate.identity_priority,candidate.hirez_profile_refreshed_at DESC NULLS LAST LIMIT 1\
                 ) resolved ON TRUE",
                &[&player_ids, &ACTIVITY_PROFILE_TTL_HOURS],
            )
            .await?;
        let recent = rows
            .iter()
            .filter(|row| {
                row.get("is_recent")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
            .filter_map(|row| value_i64(row, &["player_id"]))
            .collect::<Vec<_>>();
        let recent_set = recent.iter().copied().collect::<BTreeSet<_>>();
        let stale = player_ids
            .iter()
            .copied()
            .filter(|player_id| !recent_set.contains(player_id))
            .collect();
        Ok((stale, recent))
    }

    async fn mark_skipped_recent(&self, player_ids: &[i64]) -> Result<(), DatabaseError> {
        self.database
            .query_json(
                "UPDATE player_activity_profile_refresh SET status='skipped_recent',last_success_at=now(),next_retry_at=NULL,lease_until=NULL,error_message=NULL,updated_at=now() WHERE player_id=ANY($1::BIGINT[])",
                &[&player_ids],
            )
            .await?;
        Ok(())
    }

    async fn release_unprocessed(&self, player_ids: &[i64]) -> Result<(), DatabaseError> {
        if player_ids.is_empty() {
            return Ok(());
        }
        self.database
            .query_json(
                "UPDATE player_activity_profile_refresh SET status='pending',lease_until=NULL,\
                 error_message=NULL,next_retry_at=now(),updated_at=now() WHERE player_id=ANY($1::BIGINT[])",
                &[&player_ids],
            )
            .await?;
        Ok(())
    }

    async fn cleanup_old_state(&self) -> Result<(), DatabaseError> {
        self.database
            .query_json(
                "DELETE FROM player_queue_presence_24h WHERE last_observed_at<now()-INTERVAL '7 days'",
                &[],
            )
            .await?;
        self.database
            .query_json(
                "DELETE FROM player_activity_profile_refresh refresh WHERE refresh.updated_at<now()-INTERVAL '7 days' AND NOT EXISTS(SELECT 1 FROM player_presence_24h presence WHERE presence.player_id=refresh.player_id AND presence.last_observed_at>=now()-INTERVAL '24 hours')",
                &[],
            )
            .await?;
        Ok(())
    }
}

fn value_i64(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
    })
}

fn profile_identity_ids(value: &Value) -> BTreeSet<i64> {
    [
        "player_id",
        "Id",
        "id",
        "ActivePlayerId",
        "active_player_id",
    ]
    .into_iter()
    .filter_map(|key| value_i64(value, &[key]))
    .filter(|player_id| *player_id > 0)
    .collect()
}

fn value_text(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.replace('\0', "").replace("\\u0000", ""))
    })
}

fn synthetic_name(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    (lower.starts_with("dummyplayer")
        && lower["dummyplayer".len()..]
            .chars()
            .all(|character| character.is_ascii_digit()))
        || (lower.len() >= 27
            && lower.contains("user-")
            && lower.split("user-").next().is_some_and(|prefix| {
                prefix.len() >= 20
                    && prefix
                        .chars()
                        .all(|character| character.is_ascii_hexdigit())
            }))
}

fn normalized_region(value: Option<String>) -> String {
    let value = value.unwrap_or_default();
    match value.trim().to_ascii_lowercase().as_str() {
        "north america" | "na" => "North America".to_owned(),
        "europe" | "eu" => "Europe".to_owned(),
        "brazil" | "br" => "Brazil".to_owned(),
        "latin america north" | "latam north" => "Latin America North".to_owned(),
        "latin america south" | "latam south" => "Latin America South".to_owned(),
        "southeast asia" | "sea" => "Southeast Asia".to_owned(),
        "australia" | "oceania" => "Australia".to_owned(),
        "japan" => "Japan".to_owned(),
        "" => "Unknown".to_owned(),
        _ => value.trim().to_owned(),
    }
}

fn calculated_level(total_xp: i64, api_level: i64) -> i64 {
    if total_xp <= 0 {
        return api_level.max(0);
    }
    ((((1.0 + 4.0 * total_xp as f64 / 10_000.0).sqrt() + 1.0) / 2.0).floor() as i64).max(api_level)
}

pub(super) async fn persist_player_profile(
    database: &Database,
    raw: &Value,
) -> Result<i64, ProfileEnrichmentError> {
    let mut client = database.connection().await?;
    let transaction = client.transaction().await?;
    let player_id = persist_player_profile_in_transaction(&transaction, raw).await?;
    transaction.commit().await?;
    Ok(player_id)
}

pub(super) async fn persist_player_profile_in_transaction(
    transaction: &tokio_postgres::Transaction<'_>,
    raw: &Value,
) -> Result<i64, ProfileEnrichmentError> {
    let player_id = value_i64(raw, &["Id", "ActivePlayerId"]).unwrap_or_default();
    if player_id <= 0 {
        return Err(ProfileEnrichmentError::InvalidProfile);
    }
    let platform_name = value_text(raw, &["Name"]);
    let hz_player_name = value_text(raw, &["hz_player_name"]);
    let hz_gamer_tag = value_text(raw, &["hz_gamer_tag"]);
    let (name_source, name) = [
        ("hz_player_name", hz_player_name.as_deref()),
        ("hz_gamer_tag", hz_gamer_tag.as_deref()),
        ("name", platform_name.as_deref()),
    ]
    .into_iter()
    .find(|(_, value)| value.is_some_and(|value| !synthetic_name(value)))
    .map(|(source, value)| (source.to_owned(), value.unwrap_or_default().to_owned()))
    .unwrap_or_else(|| ("none".to_owned(), format!("Player {player_id}")));
    let anomaly = [
        platform_name.as_ref(),
        hz_player_name.as_ref(),
        hz_gamer_tag.as_ref(),
    ]
    .into_iter()
    .flatten()
    .any(|value| synthetic_name(value));
    let api_level = value_i64(raw, &["Level", "level"]).unwrap_or_default();
    let total_xp = value_i64(raw, &["Total_XP", "total_xp"]).unwrap_or_default();
    let active_id = value_i64(raw, &["ActivePlayerId", "Id"]).unwrap_or(player_id);
    let ranked = |field: &str| {
        raw.get(field)
            .filter(|value| value.is_object())
            .unwrap_or(&Value::Null)
    };
    let mut owned: Vec<Box<dyn ToSql + Sync + Send>> = Vec::with_capacity(70);
    macro_rules! param {
        ($value:expr) => {
            owned.push(Box::new($value));
        };
    }
    param!(player_id);
    param!(active_id);
    param!(name);
    param!(i32::try_from(calculated_level(total_xp, api_level)).unwrap_or_default());
    param!(i32::try_from(api_level).unwrap_or_default());
    for field in [
        "Wins",
        "Losses",
        "Leaves",
        "HoursPlayed",
        "MinutesPlayed",
        "MasteryLevel",
    ] {
        param!(i32::try_from(value_i64(raw, &[field]).unwrap_or_default()).unwrap_or_default());
    }
    param!(normalized_region(value_text(raw, &["Region"])));
    param!(value_text(raw, &["Platform"]));
    param!(value_text(raw, &["ret_msg"]));
    param!(total_xp);
    param!(value_i64(raw, &["Total_Worshippers"]).unwrap_or_default());
    param!(
        i32::try_from(value_i64(raw, &["Total_Achievements"]).unwrap_or_default())
            .unwrap_or_default()
    );
    param!(i32::try_from(value_i64(raw, &["AvatarId"]).unwrap_or_default()).unwrap_or_default());
    param!(value_text(raw, &["AvatarURL"]));
    param!(value_text(raw, &["Title"]).unwrap_or_default());
    param!(value_text(raw, &["LoadingFrame"]).unwrap_or_default());
    param!(value_text(raw, &["Created_Datetime"]));
    param!(value_text(raw, &["Last_Login_Datetime"]));
    param!(value_text(raw, &["Personal_Status_Message"]).unwrap_or_default());
    param!(i32::try_from(value_i64(raw, &["TeamId"]).unwrap_or_default()).unwrap_or_default());
    param!(value_text(raw, &["Team_Name"]).unwrap_or_default());
    param!(
        raw.get("MergedPlayers")
            .and_then(Value::as_array)
            .map(|rows| rows
                .iter()
                .filter_map(|row| value_i64(row, &["playerId", "player_id"]))
                .filter(|id| *id > 0)
                .map(|id| id.to_string())
                .collect::<Vec<_>>())
    );
    param!(if value_text(raw, &["privacy_flag"])
        .unwrap_or_default()
        .eq_ignore_ascii_case("y")
    {
        "y".to_owned()
    } else {
        "n".to_owned()
    });
    for (queue, fallback) in [
        (
            ranked("RankedKBM"),
            value_i64(raw, &["Tier_RankedKBM"]).unwrap_or_default(),
        ),
        (
            ranked("RankedController"),
            value_i64(raw, &["Tier_RankedController"]).unwrap_or_default(),
        ),
        (
            ranked("RankedConquest"),
            value_i64(raw, &["Tier_Conquest"]).unwrap_or_default(),
        ),
    ] {
        param!(value_text(queue, &["Name"]).unwrap_or_default());
        param!(
            i32::try_from(value_i64(queue, &["Points"]).unwrap_or_default()).unwrap_or_default()
        );
        param!(
            i32::try_from(
                value_i64(queue, &["Tier"])
                    .unwrap_or_default()
                    .max(fallback)
            )
            .unwrap_or_default()
        );
        for field in [
            "Rank", "Wins", "Losses", "Leaves", "Trend", "PrevRank", "Season",
        ] {
            param!(
                i32::try_from(value_i64(queue, &[field]).unwrap_or_default()).unwrap_or_default()
            );
        }
        param!(value_i64(queue, &["player_id"]).filter(|value| *value > 0));
        param!(value_text(queue, &["ret_msg"]));
    }
    param!(platform_name);
    param!(hz_player_name);
    param!(hz_gamer_tag);
    param!(name_source);
    param!(anomaly);
    param!(anomaly.then(|| "profile contained a synthetic display identity".to_owned()));
    let fields = owned
        .iter()
        .map(|value| value.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();
    transaction.execute(
        "INSERT INTO players (id,active_player_id,name,level,api_level,wins,losses,leaves,hours_played,minutes_played,mastery_level,region,platform,ret_msg,total_xp,total_worshippers,total_achievements,avatar_id,avatar_url,title,loading_frame,created_datetime,last_login_datetime,personal_status_message,team_id,team_name,merged_players,privacy_flag,kbm_name,kbm_points,kbm_tier,kbm_rank,kbm_wins,kbm_losses,kbm_leaves,kbm_trend,kbm_prev_rank,kbm_season,kbm_player_id,kbm_ret_msg,controller_name,controller_points,controller_tier,controller_rank,controller_wins,controller_losses,controller_leaves,controller_trend,controller_prev_rank,controller_season,controller_player_id,controller_ret_msg,conquest_name,conquest_points,conquest_tier,conquest_rank,conquest_wins,conquest_losses,conquest_leaves,conquest_trend,conquest_prev_rank,conquest_season,conquest_player_id,conquest_ret_msg,platform_name,hz_player_name,hz_gamer_tag,name_source,name_anomaly,name_anomaly_reason,name_anomaly_detected_at,first_seen,last_seen,last_updated,hirez_profile_refreshed_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22::TEXT::TIMESTAMPTZ,$23::TEXT::TIMESTAMPTZ,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,CASE WHEN $69 THEN now() ELSE NULL END,now(),now(),now(),now()) \
         ON CONFLICT(id) DO UPDATE SET active_player_id=EXCLUDED.active_player_id,name=CASE WHEN EXCLUDED.name_source<>'none' AND NULLIF(EXCLUDED.name,'') IS NOT NULL THEN EXCLUDED.name WHEN players.name~*'^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$' THEN 'Player '||players.id::text ELSE players.name END,level=EXCLUDED.level,api_level=EXCLUDED.api_level,wins=EXCLUDED.wins,losses=EXCLUDED.losses,leaves=EXCLUDED.leaves,hours_played=EXCLUDED.hours_played,minutes_played=EXCLUDED.minutes_played,mastery_level=EXCLUDED.mastery_level,region=CASE WHEN NULLIF(BTRIM(EXCLUDED.region),'') IS NOT NULL AND UPPER(EXCLUDED.region)<>'UNKNOWN' THEN EXCLUDED.region ELSE players.region END,platform=CASE WHEN NULLIF(BTRIM(EXCLUDED.platform),'') IS NOT NULL AND UPPER(EXCLUDED.platform)<>'UNKNOWN' THEN EXCLUDED.platform ELSE players.platform END,ret_msg=EXCLUDED.ret_msg,total_xp=EXCLUDED.total_xp,total_worshippers=EXCLUDED.total_worshippers,total_achievements=EXCLUDED.total_achievements,avatar_id=EXCLUDED.avatar_id,avatar_url=EXCLUDED.avatar_url,title=EXCLUDED.title,loading_frame=EXCLUDED.loading_frame,created_datetime=EXCLUDED.created_datetime,last_login_datetime=EXCLUDED.last_login_datetime,personal_status_message=EXCLUDED.personal_status_message,team_id=EXCLUDED.team_id,team_name=EXCLUDED.team_name,merged_players=EXCLUDED.merged_players,privacy_flag=EXCLUDED.privacy_flag,kbm_name=EXCLUDED.kbm_name,kbm_points=EXCLUDED.kbm_points,kbm_tier=EXCLUDED.kbm_tier,kbm_rank=EXCLUDED.kbm_rank,kbm_wins=EXCLUDED.kbm_wins,kbm_losses=EXCLUDED.kbm_losses,kbm_leaves=EXCLUDED.kbm_leaves,kbm_trend=EXCLUDED.kbm_trend,kbm_prev_rank=EXCLUDED.kbm_prev_rank,kbm_season=EXCLUDED.kbm_season,kbm_player_id=EXCLUDED.kbm_player_id,kbm_ret_msg=EXCLUDED.kbm_ret_msg,controller_name=EXCLUDED.controller_name,controller_points=EXCLUDED.controller_points,controller_tier=EXCLUDED.controller_tier,controller_rank=EXCLUDED.controller_rank,controller_wins=EXCLUDED.controller_wins,controller_losses=EXCLUDED.controller_losses,controller_leaves=EXCLUDED.controller_leaves,controller_trend=EXCLUDED.controller_trend,controller_prev_rank=EXCLUDED.controller_prev_rank,controller_season=EXCLUDED.controller_season,controller_player_id=EXCLUDED.controller_player_id,controller_ret_msg=EXCLUDED.controller_ret_msg,conquest_name=EXCLUDED.conquest_name,conquest_points=EXCLUDED.conquest_points,conquest_tier=EXCLUDED.conquest_tier,conquest_rank=EXCLUDED.conquest_rank,conquest_wins=EXCLUDED.conquest_wins,conquest_losses=EXCLUDED.conquest_losses,conquest_leaves=EXCLUDED.conquest_leaves,conquest_trend=EXCLUDED.conquest_trend,conquest_prev_rank=EXCLUDED.conquest_prev_rank,conquest_season=EXCLUDED.conquest_season,conquest_player_id=EXCLUDED.conquest_player_id,conquest_ret_msg=EXCLUDED.conquest_ret_msg,platform_name=EXCLUDED.platform_name,hz_player_name=EXCLUDED.hz_player_name,hz_gamer_tag=EXCLUDED.hz_gamer_tag,name_source=CASE WHEN EXCLUDED.name_source<>'none' THEN EXCLUDED.name_source ELSE players.name_source END,name_anomaly=EXCLUDED.name_anomaly,name_anomaly_reason=CASE WHEN EXCLUDED.name_anomaly THEN EXCLUDED.name_anomaly_reason ELSE players.name_anomaly_reason END,name_anomaly_detected_at=CASE WHEN EXCLUDED.name_anomaly THEN COALESCE(players.name_anomaly_detected_at,now()) ELSE players.name_anomaly_detected_at END,hirez_profile_refreshed_at=now(),last_seen=now(),last_updated=now()",
        &fields,
    ).await?;
    transaction
        .execute(
            "DELETE FROM player_profile_merged_players WHERE player_id=$1",
            &[&player_id],
        )
        .await?;
    if let Some(rows) = raw.get("MergedPlayers").and_then(Value::as_array) {
        for row in rows {
            let merged_id = value_i64(row, &["playerId", "player_id"]).unwrap_or_default();
            if merged_id <= 0 {
                continue;
            }
            let portal_id = value_i64(row, &["portalId", "portal_id"]).filter(|value| *value > 0);
            let merged_at = value_text(row, &["mergeDatetime", "merge_datetime"]);
            transaction.execute("INSERT INTO player_profile_merged_players(player_id,merged_player_id,portal_id,merge_datetime,profile_refreshed_at) VALUES($1,$2,$3,$4::TEXT::TIMESTAMPTZ,now()) ON CONFLICT(player_id,merged_player_id) DO UPDATE SET portal_id=EXCLUDED.portal_id,merge_datetime=EXCLUDED.merge_datetime,profile_refreshed_at=now()", &[&player_id,&merged_id,&portal_id,&merged_at]).await?;
        }
    }
    Ok(player_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paladinscat_core::config::BackendConfig;

    #[test]
    fn preserves_typescript_claim_and_retry_windows() {
        assert_eq!(PROFILE_CLAIM_LEASE, Duration::from_secs(30 * 60));
        assert_eq!(FAILED_RETRY_MINUTES, 60);
    }

    #[test]
    fn accepts_merged_profile_identity_as_the_requested_id() {
        let profile = json!({ "Id": 42, "ActivePlayerId": 7 });
        assert_eq!(profile_identity_ids(&profile), BTreeSet::from([7, 42]));
    }

    #[test]
    fn seeds_and_claims_by_profile_ttl_not_unknown_fields() {
        assert!(SEED_UNKNOWN_IDENTITIES_SQL.contains("ON CONFLICT (player_id) DO NOTHING"));
        assert!(!SEED_UNKNOWN_IDENTITIES_SQL.contains("needs_platform"));
        assert!(!CLAIM_UNKNOWN_IDENTITIES_SQL.contains("refresh.needs_platform"));
        assert!(CLAIM_UNKNOWN_IDENTITIES_SQL.contains("hirez_profile_refreshed_at"));
        assert!(!CLAIM_UNKNOWN_IDENTITIES_SQL.contains("claim_owner"));
        assert!(CLAIM_UNKNOWN_IDENTITIES_SQL.contains("false AS needs_platform"));
        assert!(CLAIM_UNKNOWN_IDENTITIES_SQL.contains("false AS needs_region"));
    }

    #[test]
    fn scheduled_profile_call_uses_typescript_attribution_and_audit_shape() {
        let source = include_str!("profile_enrichment.rs")
            .split_once("#[cfg(test)]")
            .unwrap()
            .0;
        assert!(source.contains("\"backend_unattributed\""));
        assert!(source.contains("entity_id: String::new()"));
        assert!(source.contains("\"reason\":reason"));
        assert!(!source.contains("rust_profile_enrichment"));
    }

    #[test]
    fn presence_reconciliation_uses_all_current_fact_populations() {
        assert!(RECONCILE_PRESENCE_SQL.contains("JOIN match_players"));
        assert!(RECONCILE_PRESENCE_SQL.contains("JOIN casual_match_players"));
        assert!(RECONCILE_PRESENCE_SQL.contains("JOIN special_match_players"));
        assert!(RECONCILE_PRESENCE_SQL.contains("participant_kind='human'"));
    }

    #[test]
    fn profile_normalization_matches_current_typescript_basics() {
        assert_eq!(normalized_region(Some("NA".to_owned())), "North America");
        assert_eq!(calculated_level(0, 999), 999);
        assert!(synthetic_name("DummyPlayer1234"));
        assert!(!synthetic_name("Real Player"));
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL with migration 108"]
    async fn live_repository_claims_stale_active_identities_in_twenty_player_batches() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.clone()),
            "REDIS_URL" => Some("redis://127.0.0.1:9".to_owned()),
            _ => None,
        })
        .expect("config");
        let database = Database::new(&config, "profile-enrichment-integration").expect("database");
        let repository = ProfileEnrichmentRepository::new(database.clone());
        let base_id = 9_881_000_000_i64;
        let client = database.connection().await.expect("connection");
        client
            .execute(
                "DELETE FROM player_activity_profile_refresh WHERE player_id BETWEEN $1 AND $2",
                &[&base_id, &(base_id + 22)],
            )
            .await
            .expect("refresh fixture cleanup");
        for player_id in base_id..=base_id + 22 {
            // TS claims active identities by profile TTL, including known
            // platform/region rows and rows without a profile yet.
            if player_id != base_id + 22 {
                client
                    .execute(
                        r#"
                        INSERT INTO players (id, name, platform, region)
                        VALUES (
                          $1,
                          'integration-player',
                          CASE WHEN $2 THEN 'Steam' ELSE NULL END,
                          CASE WHEN $2 THEN 'North America' ELSE NULL END
                        )
                        ON CONFLICT (id) DO UPDATE SET
                          platform = EXCLUDED.platform,
                          region = EXCLUDED.region
                        "#,
                        &[&player_id, &(player_id == base_id)],
                    )
                    .await
                    .expect("player fixture");
            }
            client
                .execute(
                    r#"
                    INSERT INTO player_presence_24h (
                      player_id, first_observed_at, last_observed_at,
                      last_match_id, last_queue_id, last_stats_scope
                    )
                    VALUES ($1, now(), now(), 1, 424, 'casual')
                    ON CONFLICT (player_id) DO UPDATE SET last_observed_at = now()
                    "#,
                    &[&player_id],
                )
                .await
                .expect("presence fixture");
        }
        drop(client);

        let batches = repository
            .claim_unknown_batches(2, Duration::from_secs(60))
            .await
            .expect("claim stale");
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![20, 2]
        );
        assert!(
            batches
                .iter()
                .flatten()
                .any(|player| player.player_id == base_id)
        );
        assert!(
            batches
                .iter()
                .flatten()
                .any(|player| player.player_id == base_id + 22)
        );

        let client = database.connection().await.expect("connection");
        let known = client
            .query_one(
                r#"
                SELECT status, needs_platform, needs_region, lease_until
                FROM player_activity_profile_refresh
                WHERE player_id = $1
                "#,
                &[&base_id],
            )
            .await
            .expect("known identity state");
        assert_eq!(known.get::<_, String>("status"), "fetching");
        assert!(
            known
                .get::<_, Option<time::OffsetDateTime>>("lease_until")
                .is_some()
        );
        client
            .execute(
                "DELETE FROM player_activity_profile_refresh WHERE player_id BETWEEN $1 AND $2",
                &[&base_id, &(base_id + 22)],
            )
            .await
            .expect("refresh cleanup");
        client
            .execute(
                "DELETE FROM player_presence_24h WHERE player_id BETWEEN $1 AND $2",
                &[&base_id, &(base_id + 22)],
            )
            .await
            .expect("presence cleanup");
        client
            .execute(
                "DELETE FROM players WHERE id BETWEEN $1 AND $2",
                &[&base_id, &(base_id + 22)],
            )
            .await
            .expect("player cleanup");
    }
}
