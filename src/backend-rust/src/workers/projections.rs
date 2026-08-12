use paladinscat_core::database::{Database, DatabaseError};
use serde_json::Value;
use tokio_postgres::Transaction;

const ROLE_NAME_SQL: &str = r#"CASE
  WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%'
    OR c.name IN ('Ash','Atlas','Azaan','Barik','Fernando','Inara','Khan','Makoa','Nyx','Raum','Ruckus','Terminus','Torvald','Yagorath')
    THEN 'Frontline'
  WHEN c.roles ILIKE '%Damage%'
    OR c.name IN ('Betty La Bomba','Betty la Bomba','Bomb King','Cassie','Dredge','Drogoz','Imani','Kinessa','Lian','Octavia','Omen','Saati','Sha Lin','Strix','Tiberius','Tyra','Viktor','Vivian','Willo')
    THEN 'Damage'
  WHEN c.roles ILIKE '%Flank%'
    OR c.name IN ('Androxus','Buck','Caspian','Evie','Kasumi','Koga','Lex','Maeve','Skye','Talus','Vatu','VII','Vora','Zhin')
    THEN 'Flank'
  WHEN c.roles ILIKE '%Support%'
    OR c.name IN ('Corvus','Furia','Grohk','Grover','Io','Jenos','Lillith','Mal Damba','Mal''Damba','Moji','Pip','Rei','Seris','Ying')
    THEN 'Support'
  ELSE COALESCE(NULLIF(c.roles,''),'Unknown')
END"#;

fn role_id_sql() -> String {
    format!(
        "CASE {} WHEN 'Damage' THEN 1 WHEN 'Flank' THEN 2 \
         WHEN 'Support' THEN 3 WHEN 'Frontline' THEN 4 ELSE NULL END",
        ROLE_NAME_SQL
    )
}

pub async fn refresh_performance_metric_stats(database: &Database) -> Result<u64, DatabaseError> {
    let mut client = database.connection().await?;
    let transaction = client.transaction().await?;
    let locked = transaction
        .query_one(
            "SELECT pg_try_advisory_xact_lock(hashtext('baseline:refresh')) locked",
            &[],
        )
        .await?
        .get::<_, bool>("locked");
    if !locked {
        transaction.rollback().await?;
        return Ok(0);
    }
    transaction
        .execute("DELETE FROM performance_metric_stats", &[])
        .await?;
    let inserted = transaction
        .execute(
            r#"
            WITH histogram AS (
              SELECT queue_id,role_id,role_name,metric,value,sample_count::BIGINT sample_count,
                sum(sample_count) OVER(PARTITION BY queue_id,role_id,metric ORDER BY value) cumulative,
                sum(sample_count) OVER(PARTITION BY queue_id,role_id,metric) sample_size
              FROM performance_metric_histogram WHERE sample_count>0
            ), groups AS (
              SELECT queue_id,role_id,max(role_name) role_name,metric,max(sample_size) sample_size,
                min(value) min_value,max(value) max_value,
                sum(value*sample_count)/sum(sample_count) mean_value
              FROM histogram GROUP BY queue_id,role_id,metric
            ), fractions(fraction,name) AS (
              VALUES (0.5::DOUBLE PRECISION,'median'),(0.1,'p10'),(0.25,'p25'),(0.75,'p75'),(0.9,'p90')
            ), percentile_values AS (
              SELECT g.queue_id,g.role_id,g.metric,f.name,
                lower_row.value+(upper_row.value-lower_row.value)*
                  (((g.sample_size-1)*f.fraction)-floor((g.sample_size-1)*f.fraction)) value
              FROM groups g CROSS JOIN fractions f
              JOIN LATERAL(SELECT h.value FROM histogram h WHERE h.queue_id=g.queue_id AND h.role_id=g.role_id AND h.metric=g.metric AND h.cumulative>floor((g.sample_size-1)*f.fraction) ORDER BY h.value LIMIT 1) lower_row ON TRUE
              JOIN LATERAL(SELECT h.value FROM histogram h WHERE h.queue_id=g.queue_id AND h.role_id=g.role_id AND h.metric=g.metric AND h.cumulative>ceil((g.sample_size-1)*f.fraction) ORDER BY h.value LIMIT 1) upper_row ON TRUE
            ), modes AS (
              SELECT DISTINCT ON(queue_id,role_id,metric) queue_id,role_id,metric,mode_value
              FROM(SELECT queue_id,role_id,metric,CASE WHEN metric='kda' THEN round(value::NUMERIC,1)::DOUBLE PRECISION ELSE round(value::NUMERIC,0)::DOUBLE PRECISION END mode_value,sum(sample_count) mode_count FROM histogram GROUP BY 1,2,3,4) counts
              ORDER BY queue_id,role_id,metric,mode_count DESC,mode_value
            )
            INSERT INTO performance_metric_stats(queue_id,role_id,role_name,metric,min_value,max_value,mean_value,median_value,mode_value,p10_value,p25_value,p75_value,p90_value,sample_size,updated_at)
            SELECT g.queue_id,g.role_id,g.role_name,g.metric,round(g.min_value::NUMERIC,2),round(g.max_value::NUMERIC,2),round(g.mean_value::NUMERIC,2),
              round(max(p.value) FILTER(WHERE p.name='median')::NUMERIC,2),round(m.mode_value::NUMERIC,2),
              round(max(p.value) FILTER(WHERE p.name='p10')::NUMERIC,2),round(max(p.value) FILTER(WHERE p.name='p25')::NUMERIC,2),
              round(max(p.value) FILTER(WHERE p.name='p75')::NUMERIC,2),round(max(p.value) FILTER(WHERE p.name='p90')::NUMERIC,2),g.sample_size,now()
            FROM groups g JOIN percentile_values p USING(queue_id,role_id,metric) JOIN modes m USING(queue_id,role_id,metric)
            GROUP BY g.queue_id,g.role_id,g.role_name,g.metric,g.min_value,g.max_value,g.mean_value,m.mode_value,g.sample_size
            "#,
            &[],
        )
        .await?;
    transaction.commit().await?;
    Ok(inserted)
}

pub async fn project_performance(
    transaction: &Transaction<'_>,
    match_id: i64,
) -> Result<bool, tokio_postgres::Error> {
    Ok(project_performance_many(transaction, &[match_id]).await? > 0)
}

pub async fn project_performance_many(
    transaction: &Transaction<'_>,
    match_ids: &[i64],
) -> Result<usize, tokio_postgres::Error> {
    if match_ids.is_empty() {
        return Ok(0);
    }
    let requested_ids = match_ids.to_vec();
    let claimed_ids: Vec<i64> = transaction
        .query(
            "INSERT INTO performance_projection_matches(match_id,projected_at) \
             SELECT requested.match_id,now() \
             FROM unnest($1::BIGINT[]) AS requested(match_id) \
             JOIN matches m ON m.match_id=requested.match_id \
             WHERE COALESCE(m.limited,FALSE)=FALSE \
             ON CONFLICT(match_id) DO NOTHING RETURNING match_id",
            &[&requested_ids],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    if claimed_ids.is_empty() {
        return Ok(0);
    }

    let role_name = ROLE_NAME_SQL;
    let role_id = role_id_sql();
    transaction
        .execute(
            &format!(
                r#"
                INSERT INTO performance_records_ranked(
                  match_id,entry_datetime,player_id,champion_id,champion_name,
                  role_id,role_name,queue_id,region,platform,gpm,dpm,hpm,mpm
                )
                SELECT mp.match_id,mp.entry_datetime,mp.player_id,mp.champion_id,c.name,
                  {role_id},{role_name},m.queue_id,NULLIF(mp.region,''),NULLIF(mp.platform,''),
                  mp.gold_per_minute,mp.damage_per_minute,mp.healing_per_minute,mp.mitigation_per_minute
                FROM match_players mp
                JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
                JOIN champions c ON c.id=mp.champion_id
                WHERE m.match_id=ANY($1::BIGINT[]) AND m.queue_id=486 AND COALESCE(m.limited,FALSE)=FALSE
                  AND (NOT COALESCE(m.broken,FALSE) OR COALESCE(m.recovered,FALSE))
                  AND COALESCE(mp.source,'direct') IN('direct','recovered')
                  AND mp.player_id>0 AND mp.champion_id>0 AND mp.task_force IN(1,2)
                  AND lower(COALESCE(mp.win_status,'')) IN('winner','win','loser','loss')
                  AND m.duration_seconds>120
                ON CONFLICT(match_id,entry_datetime,player_id) DO UPDATE SET
                  champion_id=EXCLUDED.champion_id,champion_name=EXCLUDED.champion_name,
                  role_id=EXCLUDED.role_id,role_name=EXCLUDED.role_name,queue_id=EXCLUDED.queue_id,
                  region=EXCLUDED.region,platform=EXCLUDED.platform,gpm=EXCLUDED.gpm,
                  dpm=EXCLUDED.dpm,hpm=EXCLUDED.hpm,mpm=EXCLUDED.mpm
                "#
            ),
            &[&claimed_ids],
        )
        .await?;

    transaction
        .execute(
            &format!(
                r#"
                WITH metric_values AS(
                  SELECT m.queue_id,{role_id} AS match_role_id,{role_name} AS match_role_name,
                    metric.metric,metric.value
                  FROM match_players mp
                  JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
                  JOIN champions c ON c.id=mp.champion_id
                  CROSS JOIN LATERAL(VALUES
                    ('dpm'::TEXT,mp.damage_per_minute::DOUBLE PRECISION),
                    ('wpm'::TEXT,CASE WHEN COALESCE(mp.source,'direct')<>'recovered'
                      THEN COALESCE(mp.damage_done_in_hand,0)/(m.duration_seconds/60.0) END::DOUBLE PRECISION),
                    ('apm'::TEXT,CASE WHEN COALESCE(mp.source,'direct')<>'recovered'
                      THEN GREATEST(COALESCE(mp.damage_done_physical,0)-COALESCE(mp.damage_done_in_hand,0),0)
                        /(m.duration_seconds/60.0) END::DOUBLE PRECISION),
                    ('hpm'::TEXT,mp.healing_per_minute::DOUBLE PRECISION),
                    ('gpm'::TEXT,mp.gold_per_minute::DOUBLE PRECISION),
                    ('egpm'::TEXT,mp.egpm::DOUBLE PRECISION),
                    ('mpm'::TEXT,mp.mitigation_per_minute::DOUBLE PRECISION),
                    ('kda'::TEXT,mp.kda::DOUBLE PRECISION)
                  )metric(metric,value)
                  WHERE m.match_id=ANY($1::BIGINT[]) AND m.queue_id=486 AND COALESCE(m.limited,FALSE)=FALSE
                    AND (NOT COALESCE(m.broken,FALSE) OR COALESCE(m.recovered,FALSE))
                    AND COALESCE(mp.source,'direct') IN('direct','recovered')
                    AND mp.player_id>0 AND mp.champion_id>0 AND mp.task_force IN(1,2)
                    AND lower(COALESCE(mp.win_status,'')) IN('winner','win','loser','loss')
                    AND m.duration_seconds>120
                ),scoped AS(
                  SELECT metric_values.queue_id,scope.role_id,scope.role_name,metric_values.metric,
                    CASE WHEN metric_values.metric IN('wpm','apm')
                      THEN round(metric_values.value::NUMERIC,0)::DOUBLE PRECISION
                      ELSE metric_values.value END value
                  FROM metric_values
                  CROSS JOIN LATERAL(VALUES
                    (0,'Global'::TEXT),(metric_values.match_role_id,metric_values.match_role_name)
                  )scope(role_id,role_name)
                  WHERE scope.role_id IS NOT NULL AND metric_values.value IS NOT NULL
                    AND(metric_values.value>0 OR(metric_values.metric IN('wpm','apm','egpm') AND metric_values.value=0))
                )
                INSERT INTO performance_metric_histogram(
                  queue_id,role_id,role_name,metric,value,sample_count,updated_at
                )
                SELECT queue_id,role_id,role_name,metric,value,count(*)::BIGINT,now()
                FROM scoped GROUP BY queue_id,role_id,role_name,metric,value
                ON CONFLICT(queue_id,role_id,metric,value) DO UPDATE SET
                  role_name=EXCLUDED.role_name,
                  sample_count=performance_metric_histogram.sample_count+EXCLUDED.sample_count,
                  updated_at=now()
                "#
            ),
            &[&claimed_ids],
        )
        .await?;

    transaction
        .execute(
            r#"
            INSERT INTO player_queue_rating_summary(
              queue_id,player_id,total_matches,total_wins,total_losses,updated_at
            )
            SELECT m.queue_id,mp.player_id,count(*)::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('loser','loss'))::BIGINT,now()
            FROM match_players mp
            JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
            WHERE mp.match_id=ANY($1::BIGINT[]) AND m.queue_id=486 AND mp.player_id>0 AND mp.champion_id>0
              AND COALESCE(mp.source,'direct') IN('direct','recovered')
              AND lower(COALESCE(mp.win_status,'')) IN('winner','win','loser','loss')
            GROUP BY m.queue_id,mp.player_id
            ON CONFLICT(queue_id,player_id) DO UPDATE SET
              total_matches=player_queue_rating_summary.total_matches+EXCLUDED.total_matches,
              total_wins=player_queue_rating_summary.total_wins+EXCLUDED.total_wins,
              total_losses=player_queue_rating_summary.total_losses+EXCLUDED.total_losses,updated_at=now()
            "#,
            &[&claimed_ids],
        )
        .await?;
    transaction
        .execute(
            r#"
            INSERT INTO player_champion_outcome_summary(
              queue_id,player_id,champion_id,total_matches,total_wins,total_losses,last_match_at,updated_at
            )
            SELECT m.queue_id,mp.player_id,mp.champion_id,count(*)::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('loser','loss'))::BIGINT,
              max(m.entry_datetime),now()
            FROM match_players mp
            JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
            WHERE mp.match_id=ANY($1::BIGINT[]) AND m.queue_id=486 AND mp.player_id>0 AND mp.champion_id>0
              AND COALESCE(mp.source,'direct') IN('direct','recovered')
              AND lower(COALESCE(mp.win_status,'')) IN('winner','win','loser','loss')
            GROUP BY m.queue_id,mp.player_id,mp.champion_id
            ON CONFLICT(queue_id,player_id,champion_id) DO UPDATE SET
              total_matches=player_champion_outcome_summary.total_matches+EXCLUDED.total_matches,
              total_wins=player_champion_outcome_summary.total_wins+EXCLUDED.total_wins,
              total_losses=player_champion_outcome_summary.total_losses+EXCLUDED.total_losses,
              last_match_at=GREATEST(player_champion_outcome_summary.last_match_at,EXCLUDED.last_match_at),
              updated_at=now()
            "#,
            &[&claimed_ids],
        )
        .await?;

    transaction
        .execute(
            r#"
            DELETE FROM player_best_champion_ratings best
            USING(
              SELECT DISTINCT player_id
              FROM match_players
              WHERE match_id=ANY($1::BIGINT[]) AND player_id>0
            ) affected
            WHERE best.queue_id=486
              AND best.player_id=affected.player_id
            "#,
            &[&claimed_ids],
        )
        .await?;

    transaction
        .execute(
            &format!(
                r#"
                WITH candidates AS(
                  SELECT 486 queue_id,pcr.player_id,pcr.champion_id,pcr.mu,pcr.phi,
                    outcomes.total_matches matches_played,outcomes.total_wins wins,outcomes.total_losses losses,
                    {role_id} champion_role_id,{role_name} champion_role_name
                  FROM player_champion_ratings pcr
                  JOIN player_champion_outcome_summary outcomes
                    ON outcomes.queue_id=486 AND outcomes.player_id=pcr.player_id
                    AND outcomes.champion_id=pcr.champion_id
                  JOIN champions c ON c.id=pcr.champion_id
                  WHERE outcomes.total_matches>0
                    AND pcr.player_id IN(SELECT player_id FROM match_players WHERE match_id=ANY($1::BIGINT[]) AND player_id>0)
                ),scoped AS(
                  SELECT candidates.*,scope.role_id,scope.role_name FROM candidates
                  CROSS JOIN LATERAL(VALUES
                    (0,'Global'::TEXT),(candidates.champion_role_id,candidates.champion_role_name)
                  )scope(role_id,role_name) WHERE scope.role_id IS NOT NULL
                ),ranked AS(
                  SELECT scoped.*,row_number() OVER(
                    PARTITION BY queue_id,role_id,player_id
                    ORDER BY mu DESC,matches_played DESC,wins DESC,champion_id
                  )best_rank FROM scoped
                )
                INSERT INTO player_best_champion_ratings(
                  queue_id,role_id,role_name,player_id,champion_id,mu,phi,
                  matches_played,wins,losses,updated_at
                )
                SELECT queue_id,role_id,role_name,player_id,champion_id,mu,phi,
                  matches_played,wins,losses,now() FROM ranked WHERE best_rank=1
                ON CONFLICT(queue_id,role_id,player_id) DO UPDATE SET
                  role_name=EXCLUDED.role_name,champion_id=EXCLUDED.champion_id,mu=EXCLUDED.mu,
                  phi=EXCLUDED.phi,matches_played=EXCLUDED.matches_played,wins=EXCLUDED.wins,
                  losses=EXCLUDED.losses,updated_at=now()
                "#
            ),
            &[&claimed_ids],
        )
        .await?;
    Ok(claimed_ids.len())
}

pub async fn rebuild_best_champion_ratings(
    database: &paladinscat_core::database::Database,
) -> Result<u64, paladinscat_core::database::DatabaseError> {
    let mut client = database.connection().await?;
    let transaction = client.transaction().await?;
    transaction
        .execute("DELETE FROM player_best_champion_ratings", &[])
        .await?;
    let role_name = ROLE_NAME_SQL;
    let role_id = role_id_sql();
    let inserted = transaction
        .execute(
            &format!(
                r#"
                WITH candidates AS(
                  SELECT 486 queue_id,pcr.player_id,pcr.champion_id,pcr.mu,pcr.phi,
                    outcomes.total_matches matches_played,outcomes.total_wins wins,outcomes.total_losses losses,
                    {role_id} champion_role_id,{role_name} champion_role_name
                  FROM player_champion_ratings pcr
                  JOIN player_champion_outcome_summary outcomes
                    ON outcomes.queue_id=486 AND outcomes.player_id=pcr.player_id
                    AND outcomes.champion_id=pcr.champion_id
                  JOIN champions c ON c.id=pcr.champion_id
                  WHERE outcomes.total_matches>0
                ),scoped AS(
                  SELECT candidates.*,scope.role_id,scope.role_name FROM candidates
                  CROSS JOIN LATERAL(VALUES
                    (0,'Global'::TEXT),(candidates.champion_role_id,candidates.champion_role_name)
                  )scope(role_id,role_name) WHERE scope.role_id IS NOT NULL
                ),ranked AS(
                  SELECT scoped.*,row_number() OVER(
                    PARTITION BY queue_id,role_id,player_id
                    ORDER BY mu DESC,matches_played DESC,wins DESC,champion_id
                  )best_rank FROM scoped
                )
                INSERT INTO player_best_champion_ratings(
                  queue_id,role_id,role_name,player_id,champion_id,mu,phi,
                  matches_played,wins,losses,updated_at
                )
                SELECT queue_id,role_id,role_name,player_id,champion_id,mu,phi,
                  matches_played,wins,losses,now() FROM ranked WHERE best_rank=1
                "#
            ),
            &[],
        )
        .await?;
    transaction.commit().await?;
    Ok(inserted)
}

pub async fn project_scalable(
    transaction: &Transaction<'_>,
    match_id: i64,
) -> Result<bool, tokio_postgres::Error> {
    Ok(project_scalable_many(transaction, &[match_id]).await? > 0)
}

pub async fn project_scalable_many(
    transaction: &Transaction<'_>,
    match_ids: &[i64],
) -> Result<usize, tokio_postgres::Error> {
    if match_ids.is_empty() {
        return Ok(0);
    }
    let requested_ids = match_ids.to_vec();
    let claimed_ids: Vec<i64> = transaction
        .query(
            "INSERT INTO stats_projection_matches(projection_version,match_id) \
             SELECT 1,requested.match_id FROM unnest($1::BIGINT[]) AS requested(match_id) \
             JOIN matches m ON m.match_id=requested.match_id WHERE m.queue_id=486 \
             AND COALESCE(m.limited,FALSE)=FALSE ON CONFLICT DO NOTHING RETURNING match_id",
            &[&requested_ids],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    if claimed_ids.is_empty() {
        return Ok(0);
    }
    let scope = r#"
      SELECT m.match_id,m.entry_datetime,m.queue_id,
        COALESCE(mlt.lobby_tier,0)::SMALLINT lobby_tier,
        COALESCE(NULLIF(m.map,''),'Unknown') map_name,m.duration_seconds
      FROM matches m LEFT JOIN match_lobby_tiers mlt
        ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
      WHERE m.match_id=ANY($1::BIGINT[]) AND m.queue_id=486 AND COALESCE(m.limited,FALSE)=FALSE
    "#;

    transaction
        .execute(
            r#"
        INSERT INTO stats_match_aggregate(
          queue_id,lobby_tier,stat_date,region,map_name,match_count,duration_sum,updated_at
        )
        SELECT m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT,m.entry_datetime::DATE,
          COALESCE(NULLIF(m.region,''),'Unknown'),COALESCE(NULLIF(m.map,''),'Unknown'),
          count(*)::BIGINT,COALESCE(sum(m.duration_seconds),0)::BIGINT,now()
        FROM matches m LEFT JOIN match_lobby_tiers mlt
          ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
        WHERE m.match_id=ANY($1::BIGINT[]) GROUP BY 1,2,3,4,5
        ON CONFLICT(queue_id,lobby_tier,stat_date,region,map_name) DO UPDATE SET
          match_count=stats_match_aggregate.match_count+EXCLUDED.match_count,
          duration_sum=stats_match_aggregate.duration_sum+EXCLUDED.duration_sum,updated_at=now()
        "#,
            &[&claimed_ids],
        )
        .await?;

    transaction.execute(&format!(r#"
        WITH scope AS({scope}),source AS(
          SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,
            CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
              WHEN c.roles ILIKE '%Support%' THEN 3
              WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 ELSE 0 END::SMALLINT,
            scope.map_name,COALESCE(NULLIF(mp.platform,''),'Unknown'),count(*)::BIGINT,
            count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::BIGINT,
            count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('loser','loss'))::BIGINT,
            COALESCE(sum(mp.kills),0)::BIGINT,COALESCE(sum(mp.deaths),0)::BIGINT,
            COALESCE(sum(mp.assists),0)::BIGINT,COALESCE(sum(mp.damage_done_physical),0)::BIGINT,
            COALESCE(sum(mp.gold_earned),0)::BIGINT,COALESCE(sum(mp.healing),0)::BIGINT,
            COALESCE(sum(mp.damage_mitigated),0)::BIGINT,
            COALESCE(sum(mp.damage_per_minute),0)::DOUBLE PRECISION,
            COALESCE(sum(mp.healing_per_minute),0)::DOUBLE PRECISION,
            COALESCE(sum(mp.gold_per_minute),0)::DOUBLE PRECISION,
            COALESCE(sum(mp.mitigation_per_minute),0)::DOUBLE PRECISION,
            COALESCE(sum(mp.egpm),0)::DOUBLE PRECISION,
            count(*) FILTER(WHERE mp.time_in_match>0)::BIGINT
          FROM scope JOIN match_players mp
            ON mp.match_id=scope.match_id AND mp.entry_datetime=scope.entry_datetime
          LEFT JOIN champions c ON c.id=mp.champion_id
          WHERE mp.champion_id>0 AND COALESCE(mp.source,'direct') IN('direct','recovered')
          GROUP BY 1,2,3,4,5,6
        )INSERT INTO stats_player_aggregate SELECT *,now() FROM source
        ON CONFLICT(queue_id,lobby_tier,champion_id,map_name,platform) DO UPDATE SET
          role_id=EXCLUDED.role_id,plays=stats_player_aggregate.plays+EXCLUDED.plays,
          wins=stats_player_aggregate.wins+EXCLUDED.wins,losses=stats_player_aggregate.losses+EXCLUDED.losses,
          kills_sum=stats_player_aggregate.kills_sum+EXCLUDED.kills_sum,
          deaths_sum=stats_player_aggregate.deaths_sum+EXCLUDED.deaths_sum,
          assists_sum=stats_player_aggregate.assists_sum+EXCLUDED.assists_sum,
          damage_sum=stats_player_aggregate.damage_sum+EXCLUDED.damage_sum,
          gold_sum=stats_player_aggregate.gold_sum+EXCLUDED.gold_sum,
          healing_sum=stats_player_aggregate.healing_sum+EXCLUDED.healing_sum,
          mitigation_sum=stats_player_aggregate.mitigation_sum+EXCLUDED.mitigation_sum,
          dpm_sum=stats_player_aggregate.dpm_sum+EXCLUDED.dpm_sum,
          hpm_sum=stats_player_aggregate.hpm_sum+EXCLUDED.hpm_sum,
          gpm_sum=stats_player_aggregate.gpm_sum+EXCLUDED.gpm_sum,
          mpm_sum=stats_player_aggregate.mpm_sum+EXCLUDED.mpm_sum,
          egpm_sum=stats_player_aggregate.egpm_sum+EXCLUDED.egpm_sum,
          metric_samples=stats_player_aggregate.metric_samples+EXCLUDED.metric_samples,updated_at=now()
    "#),&[&claimed_ids]).await?;

    let projections = [
        format!(
            r#"
          WITH scope AS({scope}),source AS(
            SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,mpt.talent_id,mpc.card_id,
              COALESCE(mpc.card_level,0)::SMALLINT,count(*)::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('loser','loss'))::BIGINT
            FROM scope JOIN match_players mp
              ON mp.match_id=scope.match_id AND mp.entry_datetime=scope.entry_datetime
            JOIN match_player_talents mpt ON mpt.match_id=scope.match_id
              AND mpt.match_id=mp.match_id AND mpt.player_id=mp.player_id
            JOIN talents t ON t.talent_id=mpt.talent_id AND t.champion_id=mp.champion_id
            JOIN match_player_cards mpc ON mpc.match_id=scope.match_id
              AND mpc.match_id=mp.match_id AND mpc.player_id=mp.player_id
            WHERE mp.champion_id>0 GROUP BY 1,2,3,4,5,6
          )INSERT INTO stats_talent_card_aggregate SELECT *,now() FROM source
          ON CONFLICT(queue_id,lobby_tier,champion_id,talent_id,card_id,card_level) DO UPDATE SET
            uses=stats_talent_card_aggregate.uses+EXCLUDED.uses,
            wins=stats_talent_card_aggregate.wins+EXCLUDED.wins,
            losses=stats_talent_card_aggregate.losses+EXCLUDED.losses,updated_at=now()
        "#
        ),
        format!(
            r#"
          WITH scope AS({scope}),team_row AS(
            SELECT scope.queue_id,scope.lobby_tier,scope.map_name,mp.task_force,m.winning_task_force,
              count(*) FILTER(WHERE c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%')::SMALLINT frontline,
              count(*) FILTER(WHERE c.roles ILIKE '%Damage%')::SMALLINT damage,
              count(*) FILTER(WHERE c.roles ILIKE '%Flank%')::SMALLINT flank,
              count(*) FILTER(WHERE c.roles ILIKE '%Support%')::SMALLINT support
            FROM scope JOIN matches m ON m.match_id=scope.match_id AND m.entry_datetime=scope.entry_datetime
            JOIN match_players mp ON mp.match_id=m.match_id AND mp.entry_datetime=m.entry_datetime
            JOIN champions c ON c.id=mp.champion_id
            WHERE mp.task_force IN(1,2) AND mp.champion_id>0
              AND COALESCE(mp.source,'direct') IN('direct','recovered')
            GROUP BY 1,2,3,4,5 HAVING count(*)=5
          ),source AS(
            SELECT queue_id,lobby_tier,map_name,frontline||'-'||damage||'-'||flank||'-'||support comp_id,
              frontline,damage,flank,support,1::BIGINT uses,
              (task_force=winning_task_force)::INT::BIGINT wins,
              (task_force<>winning_task_force)::INT::BIGINT losses
            FROM team_row WHERE frontline+damage+flank+support=5
          ),collapsed AS(
            SELECT queue_id,lobby_tier,map_name,comp_id,frontline,damage,flank,support,
              sum(uses)::BIGINT uses,sum(wins)::BIGINT wins,sum(losses)::BIGINT losses
            FROM source GROUP BY 1,2,3,4,5,6,7,8
          )INSERT INTO stats_composition_aggregate SELECT *,now() FROM collapsed
          ON CONFLICT(queue_id,lobby_tier,map_name,comp_id) DO UPDATE SET
            uses=stats_composition_aggregate.uses+EXCLUDED.uses,
            wins=stats_composition_aggregate.wins+EXCLUDED.wins,
            losses=stats_composition_aggregate.losses+EXCLUDED.losses,updated_at=now()
        "#
        ),
        format!(
            r#"
          WITH scope AS({scope}),source AS(
            SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,scope.map_name,mpi.item_id,
              COALESCE(mpi.slot,0)::SMALLINT,COALESCE(mpi.item_level,0)::SMALLINT,count(*)::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('loser','loss'))::BIGINT
            FROM scope JOIN match_players mp
              ON mp.match_id=scope.match_id AND mp.entry_datetime=scope.entry_datetime
            JOIN match_player_items mpi ON mpi.match_id=scope.match_id
              AND mpi.match_id=mp.match_id AND mpi.player_id=mp.player_id
            WHERE mp.champion_id>0 GROUP BY 1,2,3,4,5,6,7
          )INSERT INTO stats_item_aggregate SELECT *,now() FROM source
          ON CONFLICT(queue_id,lobby_tier,champion_id,map_name,item_id,slot,item_level) DO UPDATE SET
            uses=stats_item_aggregate.uses+EXCLUDED.uses,wins=stats_item_aggregate.wins+EXCLUDED.wins,
            losses=stats_item_aggregate.losses+EXCLUDED.losses,updated_at=now()
        "#
        ),
        format!(
            r#"
          WITH scope AS({scope}),source AS(
            SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,scope.map_name,mpt.talent_id,count(*)::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('loser','loss'))::BIGINT,
              COALESCE(sum(mp.kills),0)::BIGINT,COALESCE(sum(mp.deaths),0)::BIGINT,
              COALESCE(sum(mp.assists),0)::BIGINT
            FROM scope JOIN match_players mp
              ON mp.match_id=scope.match_id AND mp.entry_datetime=scope.entry_datetime
            JOIN match_player_talents mpt ON mpt.match_id=scope.match_id
              AND mpt.match_id=mp.match_id AND mpt.player_id=mp.player_id
            JOIN talents t ON t.talent_id=mpt.talent_id AND t.champion_id=mp.champion_id
            WHERE mp.champion_id>0 GROUP BY 1,2,3,4,5
          )INSERT INTO stats_talent_aggregate SELECT *,now() FROM source
          ON CONFLICT(queue_id,lobby_tier,champion_id,map_name,talent_id) DO UPDATE SET
            uses=stats_talent_aggregate.uses+EXCLUDED.uses,wins=stats_talent_aggregate.wins+EXCLUDED.wins,
            losses=stats_talent_aggregate.losses+EXCLUDED.losses,
            kills_sum=stats_talent_aggregate.kills_sum+EXCLUDED.kills_sum,
            deaths_sum=stats_talent_aggregate.deaths_sum+EXCLUDED.deaths_sum,
            assists_sum=stats_talent_aggregate.assists_sum+EXCLUDED.assists_sum,updated_at=now()
        "#
        ),
        format!(
            r#"
          WITH scope AS({scope}),source AS(
            SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,mpc.card_id,
              COALESCE(mpc.card_level,0)::SMALLINT,count(*)::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::BIGINT,
              count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('loser','loss'))::BIGINT,
              COALESCE(sum(mp.kills),0)::BIGINT,COALESCE(sum(mp.deaths),0)::BIGINT,
              COALESCE(sum(mp.assists),0)::BIGINT
            FROM scope JOIN match_players mp
              ON mp.match_id=scope.match_id AND mp.entry_datetime=scope.entry_datetime
            JOIN match_player_cards mpc ON mpc.match_id=scope.match_id
              AND mpc.match_id=mp.match_id AND mpc.player_id=mp.player_id
            WHERE mp.champion_id>0 GROUP BY 1,2,3,4,5
          )INSERT INTO stats_card_aggregate SELECT *,now() FROM source
          ON CONFLICT(queue_id,lobby_tier,champion_id,card_id,card_level) DO UPDATE SET
            uses=stats_card_aggregate.uses+EXCLUDED.uses,wins=stats_card_aggregate.wins+EXCLUDED.wins,
            losses=stats_card_aggregate.losses+EXCLUDED.losses,
            kills_sum=stats_card_aggregate.kills_sum+EXCLUDED.kills_sum,
            deaths_sum=stats_card_aggregate.deaths_sum+EXCLUDED.deaths_sum,
            assists_sum=stats_card_aggregate.assists_sum+EXCLUDED.assists_sum,updated_at=now()
        "#
        ),
        format!(
            r#"
          WITH scope AS({scope}),source AS(
            SELECT scope.queue_id,scope.lobby_tier,scope.map_name,mb.champion_id,
              COALESCE(mb.ban_slot,0)::SMALLINT,count(*)::BIGINT
            FROM scope JOIN match_bans mb ON mb.match_id=scope.match_id
            WHERE mb.champion_id>0 GROUP BY 1,2,3,4,5
          )INSERT INTO stats_ban_aggregate SELECT *,now() FROM source
          ON CONFLICT(queue_id,lobby_tier,map_name,champion_id,ban_slot) DO UPDATE SET
            bans=stats_ban_aggregate.bans+EXCLUDED.bans,updated_at=now()
        "#
        ),
    ];
    for sql in projections {
        transaction.execute(&sql, &[&claimed_ids]).await?;
    }

    project_metric_histograms(transaction, &claimed_ids, scope).await?;
    Ok(claimed_ids.len())
}

async fn project_metric_histograms(
    transaction: &Transaction<'_>,
    match_ids: &[i64],
    scope: &str,
) -> Result<(), tokio_postgres::Error> {
    let eligible = format!(
        r#"
        WITH scope AS({scope}),eligible AS(
          SELECT scope.queue_id,scope.lobby_tier,
            CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
              WHEN c.roles ILIKE '%Support%' THEN 3
              WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 ELSE 0 END::SMALLINT role_id,
            mp.champion_id,mp.damage_per_minute,mp.healing_per_minute,mp.gold_per_minute,
            mp.egpm,mp.mitigation_per_minute,mp.kda,
            CASE WHEN COALESCE(mp.source,'direct')<>'recovered'
              THEN COALESCE(mp.damage_done_in_hand,0)/(scope.duration_seconds/60.0) END weapon_per_minute,
            CASE WHEN COALESCE(mp.source,'direct')<>'recovered'
              THEN GREATEST(COALESCE(mp.damage_done_physical,0)-COALESCE(mp.damage_done_in_hand,0),0)
                /(scope.duration_seconds/60.0) END ability_per_minute
          FROM scope JOIN match_players mp
            ON mp.match_id=scope.match_id AND mp.entry_datetime=scope.entry_datetime
          LEFT JOIN champions c ON c.id=mp.champion_id
          WHERE mp.champion_id>0 AND scope.duration_seconds>120
            AND COALESCE(mp.source,'direct') IN('direct','recovered')
        )
        "#
    );
    let global = format!(
        r#"{eligible},metric_values AS(
          SELECT e.queue_id,e.lobby_tier,roles.role_id,metric.metric,
            CASE WHEN metric.metric='kda' THEN round(metric.value::NUMERIC,1)::DOUBLE PRECISION
              ELSE round(metric.value::NUMERIC,0)::DOUBLE PRECISION END value
          FROM eligible e
          CROSS JOIN LATERAL(SELECT DISTINCT role_id FROM(VALUES(0::SMALLINT),(e.role_id))r(role_id))roles
          CROSS JOIN LATERAL(VALUES
            ('dpm',e.damage_per_minute::DOUBLE PRECISION),('wpm',e.weapon_per_minute::DOUBLE PRECISION),
            ('apm',e.ability_per_minute::DOUBLE PRECISION),('hpm',e.healing_per_minute::DOUBLE PRECISION),
            ('gpm',e.gold_per_minute::DOUBLE PRECISION),('egpm',e.egpm::DOUBLE PRECISION),
            ('mpm',e.mitigation_per_minute::DOUBLE PRECISION),('kda',e.kda::DOUBLE PRECISION)
          )metric(metric,value)
          WHERE metric.value IS NOT NULL
            AND(metric.value>0 OR(metric.metric IN('wpm','apm','egpm') AND metric.value=0))
        )INSERT INTO stats_metric_histogram
        SELECT queue_id,lobby_tier,role_id,metric,value,count(*)::BIGINT,now()
        FROM metric_values GROUP BY 1,2,3,4,5
        ON CONFLICT(queue_id,lobby_tier,role_id,metric,value) DO UPDATE SET
          sample_count=stats_metric_histogram.sample_count+EXCLUDED.sample_count,updated_at=now()"#
    );
    transaction.execute(&global, &[&match_ids]).await?;
    let champion = format!(
        r#"{eligible},metric_values AS(
          SELECT e.queue_id,e.lobby_tier,e.champion_id,metric.metric,
            CASE WHEN metric.metric='kda' THEN round(metric.value::NUMERIC,1)::DOUBLE PRECISION
              ELSE round(metric.value::NUMERIC,0)::DOUBLE PRECISION END value
          FROM eligible e CROSS JOIN LATERAL(VALUES
            ('dpm',e.damage_per_minute::DOUBLE PRECISION),('wpm',e.weapon_per_minute::DOUBLE PRECISION),
            ('apm',e.ability_per_minute::DOUBLE PRECISION),('hpm',e.healing_per_minute::DOUBLE PRECISION),
            ('gpm',e.gold_per_minute::DOUBLE PRECISION),('egpm',e.egpm::DOUBLE PRECISION),
            ('mpm',e.mitigation_per_minute::DOUBLE PRECISION),('kda',e.kda::DOUBLE PRECISION)
          )metric(metric,value)
          WHERE metric.value IS NOT NULL
            AND(metric.value>0 OR(metric.metric IN('wpm','apm','egpm') AND metric.value=0))
        )INSERT INTO stats_champion_metric_histogram
        SELECT queue_id,lobby_tier,champion_id,metric,value,count(*)::BIGINT,now()
        FROM metric_values GROUP BY 1,2,3,4,5
        ON CONFLICT(queue_id,lobby_tier,champion_id,metric,value) DO UPDATE SET
          sample_count=stats_champion_metric_histogram.sample_count+EXCLUDED.sample_count,updated_at=now()"#
    );
    transaction.execute(&champion, &[&match_ids]).await?;
    Ok(())
}

/// Bounded repair drain for ranked cumulative projections.
///
/// Mirrors the legacy TS `repairScalableStatsProjectionGapsWithClient`: instead
/// of sweeping the whole backlog in one unbounded statement (which exceeded the
/// 30s statement timeout and wedged the drain), we select a small *page* of
/// complete ranked matches that are missing from the `stats_projection_matches`
/// registry and project each page inside its own short transaction.
///
/// Calling `tick_derived_projections` with this repeatedly (via the scheduler
/// or a manual admin op) lets the backlog clear incrementally without any single
/// query timing out. Returns the number of additional matches projected this call.
pub async fn repair_ranked_projection_gaps(
    database: &Database,
    page_size: usize,
) -> Result<usize, DatabaseError> {
    let page_size = page_size.clamp(1, 1000);
    let page = database
        .query_json(
            "SELECT m.match_id AS match_id \
             FROM matches m \
             JOIN match_ingest_status mis ON mis.match_id=m.match_id AND mis.status='complete' \
             LEFT JOIN stats_projection_matches spm \
               ON spm.projection_version=1 AND spm.match_id=m.match_id \
             WHERE spm.match_id IS NULL \
               AND m.queue_id=486 AND COALESCE(m.limited,FALSE)=FALSE \
             ORDER BY m.entry_datetime,m.match_id \
             LIMIT $1",
            &[&i64::try_from(page_size).unwrap_or(250)],
        )
        .await?;
    let ids = page
        .iter()
        .filter_map(|row| {
            row.get("match_id")
                .and_then(Value::as_i64)
                .filter(|id| *id > 0)
        })
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(0);
    }
    let mut projected = 0usize;
    for chunk in ids.chunks(page_size) {
        let mut client = database.connection().await?;
        let transaction = client.transaction().await?;
        // Claim atomically so concurrent drain passes don't double-project;
        // anything not claimed this pass is picked up by a later page.
        let claimed = transaction
            .query(
                "INSERT INTO stats_projection_matches(projection_version,match_id) \
                 SELECT 1,requested.match_id FROM unnest($1::BIGINT[]) AS requested(match_id) \
                 ON CONFLICT DO NOTHING RETURNING match_id",
                &[&chunk.to_vec()],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();
        if claimed.is_empty() {
            let _ = transaction.rollback().await;
            continue;
        }
        // Same set-based projection the batch drain uses, but over a bounded page.
        if let Err(error) = project_scalable_many(&transaction, &claimed).await {
            tracing::error!(page = chunk.len(), error = %error, "projection repair page failed");
            let _ = transaction.rollback().await;
            continue;
        }
        if let Err(error) = project_performance_many(&transaction, &claimed).await {
            tracing::error!(page = chunk.len(), error = %error, "projection repair performance page failed");
            let _ = transaction.rollback().await;
            continue;
        }
        match transaction.commit().await {
            Ok(()) => projected += claimed.len(),
            Err(error) => {
                tracing::error!(error = %error, "projection repair page commit failed");
            }
        }
    }
    Ok(projected)
}

#[cfg(test)]
mod tests {
    const SOURCE: &str = include_str!("projections.rs");

    fn between(start: &str, end: &str) -> &'static str {
        let body = SOURCE.split_once(start).expect("start marker").1;
        body.split_once(end).expect("end marker").0
    }

    #[test]
    fn performance_batch_is_one_claim_plus_six_set_based_mutations() {
        let body = between(
            "pub async fn project_performance_many",
            "pub async fn rebuild_best_champion_ratings",
        );
        assert!(body.contains("unnest($1::BIGINT[])"));
        assert_eq!(body.matches(".query(").count(), 1);
        assert_eq!(body.matches(".execute(").count(), 6);
        assert!(!body.contains("WHERE m.match_id=$1"));
    }

    #[test]
    fn scalable_batch_has_exact_ts_statement_count_and_array_scope() {
        let body = between(
            "pub async fn project_scalable_many",
            "async fn project_metric_histograms",
        );
        let projections = body
            .split_once("let projections = [")
            .expect("projection array")
            .1
            .split_once("];")
            .expect("projection array end")
            .0;
        assert!(body.contains("unnest($1::BIGINT[])"));
        assert!(body.contains("m.match_id=ANY($1::BIGINT[])"));
        assert!(body.contains("mpt.match_id=scope.match_id"));
        assert!(body.contains("mpc.match_id=scope.match_id"));
        assert!(body.contains("mpi.match_id=scope.match_id"));
        assert_eq!(projections.matches("format!(").count(), 6);
        // claim + match aggregate + player aggregate + six aggregate families
        // + two metric histograms, matching projectMatchesWithClient.
        assert_eq!(1 + 1 + 1 + 6 + 2, 11);
        assert!(!body.contains("WHERE m.match_id=$1"));
        assert!(
            body.matches("mp.entry_datetime=scope.entry_datetime")
                .count()
                >= 5
        );
    }

    #[test]
    fn cumulative_stage_order_is_performance_then_scalable() {
        let raw = include_str!("raw_buffer.rs");
        let body = raw
            .split_once("async fn apply_adaptive_ranked_projection_batches")
            .expect("batch function")
            .1
            .split_once("enum CumulativeProjectionStage")
            .expect("batch function end")
            .0;
        let performance = body.find("CumulativeProjectionStage::Performance").unwrap();
        let scalable = body.find("CumulativeProjectionStage::Scalable").unwrap();
        assert!(performance < scalable);
    }
}
