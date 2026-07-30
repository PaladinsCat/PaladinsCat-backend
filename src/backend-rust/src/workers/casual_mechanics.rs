use paladinscat_core::database::{Database, DatabaseError};

const CLAIM_ITEM_PROJECTION_SQL: &str = r#"
WITH match_context AS (
  SELECT
    m.match_id,
    m.queue_id,
    (
      SELECT COUNT(*)::SMALLINT
      FROM match_players player
      WHERE player.match_id = m.match_id
        AND player.entry_datetime = m.entry_datetime
        AND lower(COALESCE(player.win_status, ''))
          IN ('winner', 'win', 'loser', 'loss')
    ) AS eligible_players,
    COALESCE(
      NULLIF(special.stats_scope, ''),
      CASE WHEN casual.match_id IS NOT NULL THEN 'casual' END,
      NULLIF(queue.stats_scope, 'ranked'),
      'other'
    ) AS stats_scope
  FROM matches m
  JOIN match_ingest_status ingest ON ingest.match_id = m.match_id
  LEFT JOIN casual_matches casual ON casual.match_id = m.match_id
  LEFT JOIN special_matches special ON special.match_id = m.match_id
  LEFT JOIN queue_types queue ON queue.queue_id = m.queue_id
  WHERE m.match_id = $1
    AND m.queue_id <> 486
    AND NOT COALESCE(queue.is_ranked, m.is_ranked, false)
    AND (
      ingest.status = 'complete'
      OR ingest.completed_stages @> ARRAY['player_facts']::text[]
    )
  ORDER BY m.entry_datetime DESC
  LIMIT 1
),
claimed AS (
  INSERT INTO item_counts_casual_matches (
    match_id, stats_scope, queue_id, eligible_players
  )
  SELECT match_id, stats_scope, queue_id, eligible_players
  FROM match_context
  ON CONFLICT (match_id) DO NOTHING
  RETURNING stats_scope, queue_id
)
SELECT
  'projected'::text AS result,
  claimed.stats_scope,
  claimed.queue_id
FROM claimed
UNION ALL
SELECT
  'already_projected'::text AS result,
  existing.stats_scope,
  existing.queue_id
FROM item_counts_casual_matches existing
JOIN match_context context ON context.match_id = existing.match_id
WHERE existing.match_id = $1
  AND NOT EXISTS (SELECT 1 FROM claimed)
LIMIT 1
"#;

const APPLY_ITEM_PROJECTION_SQL: &str = r#"
INSERT INTO item_counts_casual (
  stats_scope, queue_id, item_id, item_name, slot, item_level,
  count, wins, losses, winrate, updated_at
)
SELECT
  $2,
  $3,
  item_fact.item_id,
  item.item_name,
  item_fact.slot,
  COALESCE(item_fact.item_level, 0)::SMALLINT,
  COUNT(*)::BIGINT,
  COUNT(*) FILTER (
    WHERE lower(COALESCE(player.win_status, '')) IN ('winner', 'win')
  )::BIGINT,
  COUNT(*) FILTER (
    WHERE lower(COALESCE(player.win_status, '')) IN ('loser', 'loss')
  )::BIGINT,
  ROUND(
    COUNT(*) FILTER (
      WHERE lower(COALESCE(player.win_status, '')) IN ('winner', 'win')
    )::NUMERIC
    / NULLIF(
      COUNT(*) FILTER (
        WHERE lower(COALESCE(player.win_status, ''))
          IN ('winner', 'win', 'loser', 'loss')
      ),
      0
    )::NUMERIC
    * 100,
    2
  ),
  now()
FROM match_player_items item_fact
JOIN match_players player
  ON player.match_id = item_fact.match_id
 AND player.player_id = item_fact.player_id
JOIN items item ON item.item_id = item_fact.item_id
WHERE item_fact.match_id = $1
  AND lower(COALESCE(player.win_status, ''))
    IN ('winner', 'win', 'loser', 'loss')
GROUP BY
  item_fact.item_id,
  item.item_name,
  item_fact.slot,
  COALESCE(item_fact.item_level, 0)
ON CONFLICT (stats_scope, queue_id, item_id, slot, item_level)
DO UPDATE SET
  item_name = EXCLUDED.item_name,
  count = item_counts_casual.count + EXCLUDED.count,
  wins = item_counts_casual.wins + EXCLUDED.wins,
  losses = item_counts_casual.losses + EXCLUDED.losses,
  winrate = ROUND(
    (item_counts_casual.wins + EXCLUDED.wins)::NUMERIC
    / NULLIF(
      item_counts_casual.wins + EXCLUDED.wins
      + item_counts_casual.losses + EXCLUDED.losses,
      0
    )::NUMERIC
    * 100,
    2
  ),
  updated_at = EXCLUDED.updated_at
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CasualItemProjectionResult {
    Projected,
    AlreadyProjected,
}

#[derive(Debug, thiserror::Error)]
pub enum CasualItemProjectionError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error("PostgreSQL casual item projection transaction failed: {0}")]
    Query(#[from] tokio_postgres::Error),
    #[error(
        "Casual item projection rejected match {match_id}: complete non-ranked canonical facts were not available"
    )]
    Rejected { match_id: i64 },
    #[error("casual item projection returned unknown result {0}")]
    InvalidResult(String),
}

#[derive(Clone)]
pub struct CasualMechanicsRepository {
    database: Database,
}

impl CasualMechanicsRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Project one complete non-ranked match from the shared canonical item
    /// facts. This is deliberately not scheduled while TypeScript owns the
    /// production projection path.
    pub async fn upsert_item_projection_for_match(
        &self,
        match_id: i64,
    ) -> Result<CasualItemProjectionResult, CasualItemProjectionError> {
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let claim = transaction
            .query_opt(CLAIM_ITEM_PROJECTION_SQL, &[&match_id])
            .await?
            .ok_or(CasualItemProjectionError::Rejected { match_id })?;
        let result = match claim.get::<_, String>("result").as_str() {
            "projected" => CasualItemProjectionResult::Projected,
            "already_projected" => CasualItemProjectionResult::AlreadyProjected,
            value => return Err(CasualItemProjectionError::InvalidResult(value.to_owned())),
        };
        if result == CasualItemProjectionResult::Projected {
            let stats_scope = claim.get::<_, String>("stats_scope");
            let queue_id = claim.get::<_, i32>("queue_id");
            transaction
                .execute(
                    APPLY_ITEM_PROJECTION_SQL,
                    &[&match_id, &stats_scope, &queue_id],
                )
                .await?;
        }
        transaction.commit().await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use paladinscat_core::config::BackendConfig;

    use super::*;

    #[test]
    fn projection_reads_only_canonical_match_facts() {
        assert!(APPLY_ITEM_PROJECTION_SQL.contains("FROM match_player_items"));
        assert!(APPLY_ITEM_PROJECTION_SQL.contains("JOIN match_players"));
        assert!(!APPLY_ITEM_PROJECTION_SQL.contains("nonranked_match_"));
        assert!(CLAIM_ITEM_PROJECTION_SQL.contains("m.queue_id <> 486"));
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL with the casual item projection fixture"]
    async fn live_projection_matches_typescript_idempotency_and_ranked_isolation() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.clone()),
            "REDIS_URL" => Some("redis://127.0.0.1:9".to_owned()),
            _ => None,
        })
        .expect("config");
        let database =
            Database::new(&config, "casual-item-projection-integration").expect("database");
        let repository = CasualMechanicsRepository::new(database.clone());

        assert_eq!(
            repository
                .upsert_item_projection_for_match(9_100_001)
                .await
                .expect("first projection"),
            CasualItemProjectionResult::Projected
        );
        assert_eq!(
            repository
                .upsert_item_projection_for_match(9_100_001)
                .await
                .expect("idempotent replay"),
            CasualItemProjectionResult::AlreadyProjected
        );
        assert!(matches!(
            repository.upsert_item_projection_for_match(9_100_002).await,
            Err(CasualItemProjectionError::Rejected {
                match_id: 9_100_002
            })
        ));

        let client = database
            .connection()
            .await
            .expect("verification connection");
        let aggregate = client
            .query_one(
                "SELECT count, wins, losses, winrate::TEXT AS winrate \
                 FROM item_counts_casual \
                 WHERE stats_scope = 'casual' AND queue_id = 424 \
                   AND item_id = 100 AND slot = 1 AND item_level = 1",
                &[],
            )
            .await
            .expect("aggregate row");
        assert_eq!(aggregate.get::<_, i64>("count"), 2);
        assert_eq!(aggregate.get::<_, i64>("wins"), 1);
        assert_eq!(aggregate.get::<_, i64>("losses"), 1);
        assert_eq!(aggregate.get::<_, String>("winrate"), "50.00");

        let ledgers = client
            .query_one(
                "SELECT COUNT(*)::BIGINT AS count, \
                        MAX(eligible_players)::SMALLINT AS eligible_players \
                 FROM item_counts_casual_matches",
                &[],
            )
            .await
            .expect("ledger row");
        assert_eq!(ledgers.get::<_, i64>("count"), 1);
        assert_eq!(ledgers.get::<_, i16>("eligible_players"), 2);

        let ranked_aggregates = client
            .query_one(
                "SELECT COUNT(*)::BIGINT AS count FROM item_counts_ranked",
                &[],
            )
            .await
            .expect("ranked aggregate count");
        assert_eq!(ranked_aggregates.get::<_, i64>("count"), 1);
    }
}
