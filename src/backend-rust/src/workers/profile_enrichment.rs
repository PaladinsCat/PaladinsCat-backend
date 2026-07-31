use std::collections::BTreeSet;
use std::time::Duration;

use paladinscat_core::{
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use serde_json::{Value, json};

use super::relay::{WorkerRelayClient, WorkerRelayError};

pub const PLAYER_PROFILE_BATCH_SIZE: usize = 20;

const SEED_UNKNOWN_IDENTITIES_SQL: &str = r#"
WITH candidate_profiles AS MATERIALIZED (
  SELECT
    presence.player_id,
    COALESCE(NULLIF(BTRIM(resolved.platform), ''), '') AS platform,
    COALESCE(NULLIF(BTRIM(resolved.region), ''), '') AS region
  FROM player_presence_24h presence
  LEFT JOIN LATERAL (
    SELECT candidate.platform, candidate.region
    FROM (
      SELECT profile.platform, profile.region, 0 AS identity_priority
      FROM players profile
      WHERE profile.id = presence.player_id
      UNION ALL
      SELECT profile.platform, profile.region, 1 AS identity_priority
      FROM players profile
      WHERE profile.active_player_id = presence.player_id
        AND profile.active_player_id > 0
        AND profile.id <> presence.player_id
    ) candidate
    ORDER BY candidate.identity_priority
    LIMIT 1
  ) resolved ON TRUE
  WHERE presence.last_observed_at >= now() - interval '24 hours'
)
INSERT INTO player_activity_profile_refresh (
  player_id, needs_platform, needs_region, status, updated_at
)
SELECT
  player_id,
  LOWER(platform) IN ('', 'unknown', 'unavailable'),
  LOWER(region) IN ('', 'unknown', 'unavailable'),
  CASE
    WHEN LOWER(platform) NOT IN ('', 'unknown', 'unavailable')
      AND LOWER(region) NOT IN ('', 'unknown', 'unavailable')
    THEN 'success'
    ELSE 'pending'
  END,
  now()
FROM candidate_profiles
ON CONFLICT (player_id) DO UPDATE SET
  needs_platform = EXCLUDED.needs_platform,
  needs_region = EXCLUDED.needs_region,
  status = CASE
    WHEN NOT EXCLUDED.needs_platform AND NOT EXCLUDED.needs_region
      THEN 'success'
    WHEN (
      player_activity_profile_refresh.needs_platform IS DISTINCT FROM EXCLUDED.needs_platform
      OR player_activity_profile_refresh.needs_region IS DISTINCT FROM EXCLUDED.needs_region
    ) THEN 'pending'
    ELSE player_activity_profile_refresh.status
  END,
  claim_owner = CASE
    WHEN NOT EXCLUDED.needs_platform AND NOT EXCLUDED.needs_region
      OR (
        player_activity_profile_refresh.needs_platform IS DISTINCT FROM EXCLUDED.needs_platform
        OR player_activity_profile_refresh.needs_region IS DISTINCT FROM EXCLUDED.needs_region
      )
    THEN NULL
    ELSE player_activity_profile_refresh.claim_owner
  END,
  lease_until = CASE
    WHEN NOT EXCLUDED.needs_platform AND NOT EXCLUDED.needs_region
      OR (
        player_activity_profile_refresh.needs_platform IS DISTINCT FROM EXCLUDED.needs_platform
        OR player_activity_profile_refresh.needs_region IS DISTINCT FROM EXCLUDED.needs_region
      )
    THEN NULL
    ELSE player_activity_profile_refresh.lease_until
  END,
  next_retry_at = CASE
    WHEN (
      player_activity_profile_refresh.needs_platform IS DISTINCT FROM EXCLUDED.needs_platform
      OR player_activity_profile_refresh.needs_region IS DISTINCT FROM EXCLUDED.needs_region
    )
    THEN NULL
    ELSE player_activity_profile_refresh.next_retry_at
  END,
  updated_at = CASE
    WHEN player_activity_profile_refresh.needs_platform IS DISTINCT FROM EXCLUDED.needs_platform
      OR player_activity_profile_refresh.needs_region IS DISTINCT FROM EXCLUDED.needs_region
    THEN now()
    ELSE player_activity_profile_refresh.updated_at
  END
"#;

const CLAIM_UNKNOWN_IDENTITIES_SQL: &str = r#"
WITH due AS (
  SELECT refresh.player_id
  FROM player_activity_profile_refresh refresh
  WHERE (refresh.needs_platform OR refresh.needs_region)
    AND (
      refresh.status = 'pending'
      OR (
        refresh.status IN ('success', 'unavailable', 'failed', 'skipped_recent')
        AND (refresh.next_retry_at IS NULL OR refresh.next_retry_at <= now())
      )
      OR (refresh.status = 'fetching' AND refresh.lease_until <= now())
    )
    AND (refresh.lease_until IS NULL OR refresh.lease_until <= now())
  ORDER BY refresh.player_id
  LIMIT $1
  FOR UPDATE OF refresh SKIP LOCKED
)
UPDATE player_activity_profile_refresh refresh
SET status = 'fetching',
    claim_owner = $2,
    lease_until = now() + ($3::int * interval '1 second'),
    error_message = NULL,
    updated_at = now()
FROM due
WHERE refresh.player_id = due.player_id
RETURNING refresh.player_id, refresh.needs_platform, refresh.needs_region
"#;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlayerIdentityState {
    pub player_id: i64,
    pub platform: Option<String>,
    pub region: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedPlayerIdentity {
    pub player_id: i64,
    pub needs_platform: bool,
    pub needs_region: bool,
}

impl PlayerIdentityState {
    pub fn needs_platform(&self) -> bool {
        unknown_identity_value(self.platform.as_deref())
    }

    pub fn needs_region(&self) -> bool {
        unknown_identity_value(self.region.as_deref())
    }

    pub fn needs_enrichment(&self) -> bool {
        self.needs_platform() || self.needs_region()
    }
}

pub fn unknown_identity_value(value: Option<&str>) -> bool {
    value.is_none_or(|value| {
        let normalized = value.trim();
        normalized.is_empty()
            || normalized.eq_ignore_ascii_case("unknown")
            || normalized.eq_ignore_ascii_case("unavailable")
    })
}

pub fn unknown_identity_batches(
    players: impl IntoIterator<Item = PlayerIdentityState>,
) -> Vec<Vec<PlayerIdentityState>> {
    let mut seen = BTreeSet::new();
    let candidates: Vec<_> = players
        .into_iter()
        .filter(PlayerIdentityState::needs_enrichment)
        .filter(|player| player.player_id > 0 && seen.insert(player.player_id))
        .collect();
    candidates
        .chunks(PLAYER_PROFILE_BATCH_SIZE)
        .map(<[PlayerIdentityState]>::to_vec)
        .collect()
}

#[derive(Clone)]
pub struct ProfileEnrichmentRepository {
    database: Database,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProfileEnrichmentResult {
    pub calls: usize,
    pub claimed: usize,
    pub updated: usize,
    pub unavailable: usize,
    pub failed: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum ProfileEnrichmentError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Relay(#[from] WorkerRelayError),
}

impl ProfileEnrichmentRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn claim_unknown_batches(
        &self,
        max_calls: usize,
        owner: &str,
        lease: Duration,
    ) -> Result<Vec<Vec<ClaimedPlayerIdentity>>, DatabaseError> {
        if max_calls == 0 {
            return Ok(Vec::new());
        }
        let limit =
            i64::try_from(max_calls.saturating_mul(PLAYER_PROFILE_BATCH_SIZE)).unwrap_or(i64::MAX);
        let lease_seconds = i32::try_from(lease.as_secs()).unwrap_or(i32::MAX);
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        transaction
            .execute(SEED_UNKNOWN_IDENTITIES_SQL, &[])
            .await?;
        let rows = transaction
            .query(
                CLAIM_UNKNOWN_IDENTITIES_SQL,
                &[&limit, &owner, &lease_seconds],
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
            .chunks(PLAYER_PROFILE_BATCH_SIZE)
            .map(<[ClaimedPlayerIdentity]>::to_vec)
            .collect())
    }

    pub async fn run(
        &self,
        config: &BackendConfig,
        max_calls: usize,
        owner: &str,
    ) -> Result<ProfileEnrichmentResult, ProfileEnrichmentError> {
        let batches = self
            .claim_unknown_batches(max_calls, owner, Duration::from_secs(300))
            .await?;
        let relay = WorkerRelayClient::new(config)?;
        let mut result = ProfileEnrichmentResult::default();
        for batch in batches {
            result.calls += 1;
            result.claimed += batch.len();
            let ids = batch.iter().map(|row| row.player_id).collect::<Vec<_>>();
            let response = relay
                .call_value(
                    "getPlayerBatch",
                    vec![json!(ids)],
                    "rust_profile_enrichment",
                )
                .await;
            let payload = match response {
                Ok(payload) => payload,
                Err(error) => {
                    let message = error.to_string();
                    self.database
                        .query_json(
                            "UPDATE player_activity_profile_refresh SET status='failed',attempts=attempts+1,\
                             error_message=$2,claim_owner=NULL,lease_until=NULL,next_retry_at=now()+INTERVAL '30 minutes',updated_at=now() \
                             WHERE player_id=ANY($1::BIGINT[])",
                            &[&ids, &message],
                        )
                        .await?;
                    result.failed += ids.len();
                    continue;
                }
            };
            let rows = payload.as_array().cloned().unwrap_or_default();
            let returned = rows
                .iter()
                .filter_map(|row| {
                    let player_id = value_i64(row, &["player_id", "Id"])?;
                    (player_id > 0).then_some((player_id, row))
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            for claim in &batch {
                let Some(profile) = returned.get(&claim.player_id) else {
                    self.finish_unavailable(claim.player_id, "player absent from getplayerbatch")
                        .await?;
                    result.unavailable += 1;
                    continue;
                };
                let platform = value_text(profile, &["platform", "Platform", "portal"])
                    .filter(|value| !unknown_identity_value(Some(value)));
                let region = value_text(profile, &["region", "Region"])
                    .filter(|value| !unknown_identity_value(Some(value)));
                self.database
                    .query_json(
                        "UPDATE players SET platform=CASE WHEN $2<>'' AND lower(COALESCE(platform,'')) IN('','unknown','unavailable') THEN $2 ELSE platform END,\
                         region=CASE WHEN $3<>'' AND lower(COALESCE(region,'')) IN('','unknown','unavailable') THEN $3 ELSE region END,\
                         last_updated=now() WHERE id=$1 OR active_player_id=$1",
                        &[
                            &claim.player_id,
                            &platform.clone().unwrap_or_default(),
                            &region.clone().unwrap_or_default(),
                        ],
                    )
                    .await?;
                let needs_platform = claim.needs_platform && platform.is_none();
                let needs_region = claim.needs_region && region.is_none();
                let status = if needs_platform || needs_region {
                    "unavailable"
                } else {
                    "success"
                };
                self.database
                    .query_json(
                        "UPDATE player_activity_profile_refresh SET needs_platform=$2,needs_region=$3,status=$4,\
                         attempts=attempts+1,last_fetched_at=now(),claim_owner=NULL,lease_until=NULL,error_message=NULL,\
                         next_retry_at=CASE WHEN $4='success' THEN now()+INTERVAL '7 days' ELSE now()+INTERVAL '24 hours' END,updated_at=now() \
                         WHERE player_id=$1",
                        &[&claim.player_id, &needs_platform, &needs_region, &status],
                    )
                    .await?;
                if status == "success" {
                    result.updated += 1;
                } else {
                    result.unavailable += 1;
                }
            }
        }
        Ok(result)
    }

    async fn finish_unavailable(&self, player_id: i64, reason: &str) -> Result<(), DatabaseError> {
        self.database
            .query_json(
                "UPDATE player_activity_profile_refresh SET status='unavailable',attempts=attempts+1,\
                 error_message=$2,claim_owner=NULL,lease_until=NULL,next_retry_at=now()+INTERVAL '24 hours',updated_at=now() \
                 WHERE player_id=$1",
                &[&player_id, &reason],
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

fn value_text(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use paladinscat_core::config::BackendConfig;

    fn player(player_id: i64, platform: Option<&str>, region: Option<&str>) -> PlayerIdentityState {
        PlayerIdentityState {
            player_id,
            platform: platform.map(str::to_owned),
            region: region.map(str::to_owned),
        }
    }

    #[test]
    fn known_platform_and_region_are_never_fetched() {
        let batches = unknown_identity_batches([
            player(1, Some("Steam"), Some("North America")),
            player(2, Some("Xbox"), Some("Europe")),
        ]);
        assert!(batches.is_empty());
    }

    #[test]
    fn either_unknown_field_makes_player_eligible() {
        let batches = unknown_identity_batches([
            player(1, Some("Steam"), None),
            player(2, None, Some("Europe")),
            player(3, Some("Unknown"), Some("Unknown")),
        ]);
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0]
                .iter()
                .map(|player| player.player_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn deduplicates_globally_and_batches_exactly_twenty() {
        let players = (1..=41)
            .flat_map(|player_id| [player(player_id, None, None), player(player_id, None, None)])
            .collect::<Vec<_>>();
        let batches = unknown_identity_batches(players);
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![20, 20, 1]
        );
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL with migration 108"]
    async fn live_repository_claims_only_unknown_identities_in_twenty_player_batches() {
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
            .claim_unknown_batches(2, "profile-worker", Duration::from_secs(60))
            .await
            .expect("claim unknown");
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![20, 2]
        );
        assert!(
            batches
                .iter()
                .flatten()
                .all(|player| player.player_id != base_id)
        );
        assert!(
            batches
                .iter()
                .flatten()
                .all(|player| player.needs_platform && player.needs_region)
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
        assert_eq!(known.get::<_, String>("status"), "success");
        assert!(!known.get::<_, bool>("needs_platform"));
        assert!(!known.get::<_, bool>("needs_region"));
        assert!(
            known
                .get::<_, Option<time::OffsetDateTime>>("lease_until")
                .is_none()
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
