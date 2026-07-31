use paladinscat_core::database::{Database, DatabaseError};

const CLAIM_ITEM_PROJECTION_SQL: &str = r#"
WITH match_context AS (
  SELECT facts.match_id,facts.queue_id,facts.eligible_players,facts.stats_scope
  FROM (
    SELECT m.match_id,m.queue_id,'casual'::TEXT stats_scope,
      count(p.*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss'))::SMALLINT eligible_players
    FROM casual_matches m LEFT JOIN casual_match_players p ON p.match_id=m.match_id
    WHERE m.match_id=$1 GROUP BY m.match_id,m.queue_id
    UNION ALL
    SELECT m.match_id,m.queue_id,m.stats_scope,
      count(p.*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss'))::SMALLINT eligible_players
    FROM special_matches m LEFT JOIN special_match_players p ON p.match_id=m.match_id
    WHERE m.match_id=$1 GROUP BY m.match_id,m.queue_id,m.stats_scope
  )facts
  JOIN match_ingest_status ingest ON ingest.match_id=facts.match_id
  WHERE ingest.population IN('casual','special')
    AND(ingest.status='complete' OR ingest.completed_stages@>ARRAY['player_facts']::TEXT[])
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
FROM nonranked_match_items item_fact
JOIN (
  SELECT match_id,roster_slot,win_status FROM casual_match_players
  UNION ALL
  SELECT match_id,roster_slot,win_status FROM special_match_players
)player ON player.match_id=item_fact.match_id AND player.roster_slot=item_fact.roster_slot
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CasualMechanicsProjectionResult {
    pub items: CasualItemProjectionResult,
    pub talents: CasualItemProjectionResult,
    pub cards: CasualItemProjectionResult,
}

#[derive(Clone)]
pub struct CasualMechanicsRepository {
    database: Database,
}

impl CasualMechanicsRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Project one complete non-ranked match from its physically isolated
    /// mechanics facts.
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

    pub async fn project_all_for_match(
        &self,
        match_id: i64,
    ) -> Result<CasualMechanicsProjectionResult, CasualItemProjectionError> {
        let items = self.upsert_item_projection_for_match(match_id).await?;
        let talents = self
            .project_fact_family(
                match_id,
                "talent_counts_casual_matches",
                r#"
                INSERT INTO talent_counts_casual(
                  stats_scope,queue_id,talent_id,champion_name,talent_name,
                  count,wins,losses,winrate,updated_at
                )
                SELECT $2,$3,f.talent_id,c.name,t.talent_name,count(*)::bigint,
                  count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win'))::bigint,
                  count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('loser','loss'))::bigint,
                  round(count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win'))::numeric
                    /NULLIF(count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss')),0)*100,2),
                  now()
                FROM nonranked_match_talents f
                JOIN (
                  SELECT match_id,roster_slot,win_status FROM casual_match_players
                  UNION ALL SELECT match_id,roster_slot,win_status FROM special_match_players
                )p ON p.match_id=f.match_id AND p.roster_slot=f.roster_slot
                JOIN talents t ON t.talent_id=f.talent_id
                LEFT JOIN champions c ON c.id=t.champion_id
                WHERE f.match_id=$1 AND lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss')
                GROUP BY f.talent_id,c.name,t.talent_name
                ON CONFLICT(stats_scope,queue_id,talent_id) DO UPDATE SET
                  champion_name=EXCLUDED.champion_name,talent_name=EXCLUDED.talent_name,
                  count=talent_counts_casual.count+EXCLUDED.count,
                  wins=talent_counts_casual.wins+EXCLUDED.wins,
                  losses=talent_counts_casual.losses+EXCLUDED.losses,
                  winrate=round((talent_counts_casual.wins+EXCLUDED.wins)::numeric/
                    NULLIF(talent_counts_casual.wins+EXCLUDED.wins+talent_counts_casual.losses+EXCLUDED.losses,0)*100,2),
                  updated_at=now()
                "#,
            )
            .await?;
        let cards = self
            .project_fact_family(
                match_id,
                "card_counts_casual_matches",
                r#"
                INSERT INTO card_counts_casual(
                  stats_scope,queue_id,card_id,champion_name,card_name,card_level,
                  count,wins,losses,winrate,updated_at
                )
                SELECT $2,$3,f.card_id,c.name,card.card_name,COALESCE(f.card_level,0)::smallint,count(*)::bigint,
                  count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win'))::bigint,
                  count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('loser','loss'))::bigint,
                  round(count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win'))::numeric
                    /NULLIF(count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss')),0)*100,2),
                  now()
                FROM nonranked_match_cards f
                JOIN (
                  SELECT match_id,roster_slot,win_status,champion_id FROM casual_match_players
                  UNION ALL SELECT match_id,roster_slot,win_status,champion_id FROM special_match_players
                )p ON p.match_id=f.match_id AND p.roster_slot=f.roster_slot
                LEFT JOIN cards card ON card.card_id=f.card_id
                LEFT JOIN champions c ON c.id=COALESCE(card.champion_id,p.champion_id)
                WHERE f.match_id=$1 AND lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss')
                GROUP BY f.card_id,c.name,card.card_name,COALESCE(f.card_level,0)
                ON CONFLICT(stats_scope,queue_id,card_id,card_level) DO UPDATE SET
                  champion_name=EXCLUDED.champion_name,card_name=EXCLUDED.card_name,
                  count=card_counts_casual.count+EXCLUDED.count,
                  wins=card_counts_casual.wins+EXCLUDED.wins,
                  losses=card_counts_casual.losses+EXCLUDED.losses,
                  winrate=round((card_counts_casual.wins+EXCLUDED.wins)::numeric/
                    NULLIF(card_counts_casual.wins+EXCLUDED.wins+card_counts_casual.losses+EXCLUDED.losses,0)*100,2),
                  updated_at=now()
                "#,
            )
            .await?;
        self.database
            .query_json(
                "UPDATE match_ingest_status SET completed_stages=(SELECT array_agg(DISTINCT stage ORDER BY stage) \
                   FROM unnest(completed_stages||ARRAY[CASE WHEN population='special' THEN 'special_mechanics_stats' \
                     ELSE 'casual_mechanics_stats' END]::TEXT[]) stage),\
                 status='complete',acquisition_state='complete',completed_at=COALESCE(completed_at,now()),\
                 lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE match_id=$1 AND population IN('casual','special')",
                &[&match_id],
            )
            .await?;
        Ok(CasualMechanicsProjectionResult {
            items,
            talents,
            cards,
        })
    }

    async fn project_fact_family(
        &self,
        match_id: i64,
        ledger: &'static str,
        projection_sql: &'static str,
    ) -> Result<CasualItemProjectionResult, CasualItemProjectionError> {
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let claim_sql = format!(
            r#"
            WITH context AS(
              SELECT facts.match_id,facts.queue_id,facts.eligible_players,facts.stats_scope
              FROM(
                SELECT m.match_id,m.queue_id,'casual'::TEXT stats_scope,
                  count(p.*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss'))::SMALLINT eligible_players
                FROM casual_matches m LEFT JOIN casual_match_players p ON p.match_id=m.match_id
                WHERE m.match_id=$1 GROUP BY m.match_id,m.queue_id
                UNION ALL
                SELECT m.match_id,m.queue_id,m.stats_scope,
                  count(p.*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss'))::SMALLINT eligible_players
                FROM special_matches m LEFT JOIN special_match_players p ON p.match_id=m.match_id
                WHERE m.match_id=$1 GROUP BY m.match_id,m.queue_id,m.stats_scope
              )facts JOIN match_ingest_status mis ON mis.match_id=facts.match_id
              WHERE mis.population IN('casual','special')
                AND(mis.status='complete' OR mis.completed_stages@>ARRAY['player_facts']::TEXT[])
            ), claimed AS(
              INSERT INTO {ledger}(match_id,stats_scope,queue_id,eligible_players)
              SELECT match_id,stats_scope,queue_id,eligible_players FROM context
              ON CONFLICT(match_id) DO NOTHING RETURNING stats_scope,queue_id
            )
            SELECT 'projected' result,stats_scope,queue_id FROM claimed
            UNION ALL
            SELECT 'already_projected',l.stats_scope,l.queue_id FROM {ledger} l
            JOIN context c ON c.match_id=l.match_id WHERE l.match_id=$1 AND NOT EXISTS(SELECT 1 FROM claimed)
            LIMIT 1
            "#
        );
        let claim = transaction
            .query_opt(&claim_sql, &[&match_id])
            .await?
            .ok_or(CasualItemProjectionError::Rejected { match_id })?;
        let result = match claim.get::<_, String>("result").as_str() {
            "projected" => CasualItemProjectionResult::Projected,
            "already_projected" => CasualItemProjectionResult::AlreadyProjected,
            value => return Err(CasualItemProjectionError::InvalidResult(value.to_owned())),
        };
        if result == CasualItemProjectionResult::Projected {
            let scope = claim.get::<_, String>("stats_scope");
            let queue_id = claim.get::<_, i32>("queue_id");
            transaction
                .execute(projection_sql, &[&match_id, &scope, &queue_id])
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
    fn projection_reads_only_nonranked_fact_ownership() {
        assert!(APPLY_ITEM_PROJECTION_SQL.contains("FROM nonranked_match_items"));
        assert!(APPLY_ITEM_PROJECTION_SQL.contains("casual_match_players"));
        assert!(APPLY_ITEM_PROJECTION_SQL.contains("special_match_players"));
        assert!(!APPLY_ITEM_PROJECTION_SQL.contains("FROM match_player_items"));
        assert!(CLAIM_ITEM_PROJECTION_SQL.contains("ingest.population IN('casual','special')"));
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
        database
            .connection()
            .await
            .expect("fixture connection")
            .batch_execute(
                r#"
                INSERT INTO items(item_id,item_name)
                VALUES(100,'Chronos')
                ON CONFLICT(item_id) DO UPDATE SET item_name=EXCLUDED.item_name;
                INSERT INTO casual_matches(
                  match_id,queue_id,entry_datetime,region,map,duration_seconds,
                  winning_task_force,quality,stats_eligible,player_count,source
                )VALUES(
                  9100001,424,'2026-07-30T12:00:00Z','NA','Stone Keep',600,
                  1,'complete',TRUE,2,'fixture'
                );
                INSERT INTO match_ingest_status(
                  match_id,status,completed_stages,source,queue_id,population,
                  acquisition_state
                )VALUES(
                  9100001,'complete',ARRAY['core','player_facts'],'fixture',424,
                  'casual','complete'
                );
                INSERT INTO casual_match_players(
                  match_id,roster_slot,player_id,player_name,champion_id,
                  task_force,win_status,participant_kind,source,stats_eligible
                )VALUES
                  (9100001,1,1,'Winner',1,1,'Winner','human','fixture',TRUE),
                  (9100001,2,2,'Loser',2,2,'Loser','human','fixture',TRUE);
                INSERT INTO nonranked_match_items(
                  match_id,population,stats_scope,queue_id,roster_slot,
                  player_id,slot,item_id,item_level
                )VALUES
                  (9100001,'casual','casual',424,1,1,1,100,1),
                  (9100001,'casual','casual',424,2,2,1,100,1);
                INSERT INTO item_counts_ranked(item_id,item_name,slot,item_level)
                VALUES(100,'Chronos',1,1)
                ON CONFLICT(item_id,slot,item_level) DO NOTHING
                "#,
            )
            .await
            .expect("seed casual projection fixture");

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
