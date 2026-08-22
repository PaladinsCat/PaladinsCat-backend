use paladinscat_core::database::{Database, DatabaseError};
use serde_json::Value;
use tokio_postgres::Transaction;

const ROLE_NAME_SQL: &str = r#"CASE
  WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%'
    OR c.name IN ('Ash','Atlas','Azaan','Barik','Fernando','Inara','Khan','Makoa','Nyx','Raum','Ruckus','Terminus','Torvald','Yagorath')
    THEN 'Frontline'
  WHEN c.roles ILIKE '%Damage%'
    OR c.name IN ('Betty La Bomba','Betty la Bomba','Bomb King','Cassie','Dredge','Drogoz','Imani','Kinessa','Lian','Octavia','Omen','Saati','Sha Lin','Strix','Tiberius','Tyra','Viktor','Vivian','Willow')
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

// Derive all weighted percentile bounds while scanning the windowed casual
// histogram once (mirrors PERFORMANCE_METRIC_STATS_REFRESH_SQL in
// projections.rs, minus the queue_id partition key — casual is one isolated
// population).
const CASUAL_PERFORMANCE_METRIC_STATS_REFRESH_SQL: &str = r#"
WITH histogram AS MATERIALIZED (
  SELECT role_id,role_name,metric,value,sample_count::BIGINT sample_count,
    sum(sample_count) OVER(PARTITION BY role_id,metric ORDER BY value) cumulative,
    sum(sample_count) OVER(PARTITION BY role_id,metric) sample_size
  FROM casual_performance_metric_histogram WHERE sample_count>0
), grouped AS (
  SELECT role_id,max(role_name) role_name,metric,max(sample_size) sample_size,
    min(value) min_value,max(value) max_value,
    sum(value*sample_count)/sum(sample_count) mean_value,
    min(value) FILTER(WHERE cumulative>floor((sample_size-1)*0.10)) p10_lower,
    min(value) FILTER(WHERE cumulative>ceil((sample_size-1)*0.10)) p10_upper,
    min(value) FILTER(WHERE cumulative>floor((sample_size-1)*0.25)) p25_lower,
    min(value) FILTER(WHERE cumulative>ceil((sample_size-1)*0.25)) p25_upper,
    min(value) FILTER(WHERE cumulative>floor((sample_size-1)*0.50)) median_lower,
    min(value) FILTER(WHERE cumulative>ceil((sample_size-1)*0.50)) median_upper,
    min(value) FILTER(WHERE cumulative>floor((sample_size-1)*0.75)) p75_lower,
    min(value) FILTER(WHERE cumulative>ceil((sample_size-1)*0.75)) p75_upper,
    min(value) FILTER(WHERE cumulative>floor((sample_size-1)*0.90)) p90_lower,
    min(value) FILTER(WHERE cumulative>ceil((sample_size-1)*0.90)) p90_upper
  FROM histogram GROUP BY role_id,metric
), modes AS (
  SELECT DISTINCT ON(role_id,metric) role_id,metric,mode_value
  FROM (
    SELECT role_id,metric,
      round(value::NUMERIC,0)::DOUBLE PRECISION mode_value,
      sum(sample_count) mode_count
    FROM histogram GROUP BY 1,2,3
  ) counts
  ORDER BY role_id,metric,mode_count DESC,mode_value
)
INSERT INTO casual_performance_metric_stats(
  role_id,role_name,metric,min_value,max_value,mean_value,
  median_value,mode_value,p10_value,p25_value,p75_value,p90_value,
  sample_size,updated_at
)
SELECT g.role_id,g.role_name,g.metric,
  round(g.min_value::NUMERIC,2),round(g.max_value::NUMERIC,2),
  round(g.mean_value::NUMERIC,2),
  round((g.median_lower+(g.median_upper-g.median_lower)*
    (((g.sample_size-1)*0.50)-floor((g.sample_size-1)*0.50)))::NUMERIC,2),
  round(m.mode_value::NUMERIC,2),
  round((g.p10_lower+(g.p10_upper-g.p10_lower)*
    (((g.sample_size-1)*0.10)-floor((g.sample_size-1)*0.10)))::NUMERIC,2),
  round((g.p25_lower+(g.p25_upper-g.p25_lower)*
    (((g.sample_size-1)*0.25)-floor((g.sample_size-1)*0.25)))::NUMERIC,2),
  round((g.p75_lower+(g.p75_upper-g.p75_lower)*
    (((g.sample_size-1)*0.75)-floor((g.sample_size-1)*0.75)))::NUMERIC,2),
  round((g.p90_lower+(g.p90_upper-g.p90_lower)*
    (((g.sample_size-1)*0.90)-floor((g.sample_size-1)*0.90)))::NUMERIC,2),
  g.sample_size,now()
FROM grouped g JOIN modes m USING(role_id,metric)
"#;

/// Hourly atomic rebuild of the casual performance stats table from the
/// accumulated histogram. Mirrors `refresh_performance_metric_stats`
/// (projections.rs): advisory-locked, `DELETE`+`INSERT`, 5s lock timeout,
/// 2min statement timeout. Distinct advisory key from the ranked refresh so
/// the two populations never contend.
pub async fn refresh_casual_performance_metric_stats(
    database: &Database,
) -> Result<u64, DatabaseError> {
    let mut client = database.connection().await?;
    let transaction = client.transaction().await?;
    transaction
        .batch_execute("SET LOCAL lock_timeout='5s';SET LOCAL statement_timeout='2min'")
        .await?;
    let locked = transaction
        .query_one(
            "SELECT pg_try_advisory_xact_lock(hashtext('casual:performance:refresh')) locked",
            &[],
        )
        .await?
        .get::<_, bool>("locked");
    if !locked {
        transaction.rollback().await?;
        return Ok(0);
    }
    transaction
        .execute("DELETE FROM casual_performance_metric_stats", &[])
        .await?;
    let inserted = transaction
        .execute(CASUAL_PERFORMANCE_METRIC_STATS_REFRESH_SQL, &[])
        .await?;
    transaction.commit().await?;
    Ok(inserted)
}

/// Ingestion-time incremental projection of one or more complete casual
/// matches into the accumulated histogram. Mirrors `project_performance_many`
/// (projections.rs): one ledger claim, then one set-based upsert. The claim
/// is restricted to the physically isolated casual population (queue 424/452)
/// so Special matches never enter the casual projection.
pub async fn project_casual_performance_many(
    transaction: &Transaction<'_>,
    match_ids: &[i64],
) -> Result<usize, tokio_postgres::Error> {
    if match_ids.is_empty() {
        return Ok(0);
    }
    let requested_ids = match_ids.to_vec();
    let claimed_ids: Vec<i64> = transaction
        .query(
            "INSERT INTO casual_performance_projection_matches(match_id,projected_at) \
             SELECT requested.match_id,now() \
             FROM unnest($1::BIGINT[]) AS requested(match_id) \
             JOIN casual_matches cm ON cm.match_id=requested.match_id \
             WHERE cm.queue_id IN (424,452) \
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
                WITH metric_values AS(
                  SELECT {role_id} AS match_role_id,{role_name} AS match_role_name,
                    metric.metric,metric.value
                  FROM casual_match_players cmp
                  JOIN casual_matches cm ON cm.match_id=cmp.match_id
                  LEFT JOIN champions c ON c.id=cmp.champion_id
                  CROSS JOIN LATERAL(VALUES
                    ('dpm'::TEXT,(cmp.damage*60.0/NULLIF(cm.duration_seconds,0))::DOUBLE PRECISION),
                    ('hpm'::TEXT,(cmp.healing*60.0/NULLIF(cm.duration_seconds,0))::DOUBLE PRECISION),
                    ('gpm'::TEXT,(cmp.credits*60.0/NULLIF(cm.duration_seconds,0))::DOUBLE PRECISION),
                    ('mpm'::TEXT,(cmp.mitigation*60.0/NULLIF(cm.duration_seconds,0))::DOUBLE PRECISION)
                  )metric(metric,value)
                  WHERE cmp.match_id=ANY($1::BIGINT[]) AND cm.queue_id IN(424,452)
                    AND cm.stats_eligible AND cm.quality='complete'
                    AND cmp.stats_eligible AND cmp.participant_kind='human'
                    AND cmp.player_id>0 AND cmp.task_force IN(1,2)
                    AND lower(COALESCE(cmp.win_status,'')) IN('winner','win','loser','loss')
                    AND cm.duration_seconds>0
                ),scoped AS(
                  SELECT scope.role_id,scope.role_name,metric_values.metric,
                    -- The histogram is an accumulated distribution, not a raw-fact
                    -- store. Exact floating-point keys created an unbounded number
                    -- of buckets and expensive index writes for every new match.
                    round(metric_values.value::NUMERIC,0)::DOUBLE PRECISION value
                  FROM metric_values
                  CROSS JOIN LATERAL(VALUES
                    (0,'Global'::TEXT),(metric_values.match_role_id,metric_values.match_role_name)
                  )scope(role_id,role_name)
                  WHERE scope.role_id IS NOT NULL AND metric_values.value IS NOT NULL
                    AND metric_values.value>0
                )
                INSERT INTO casual_performance_metric_histogram(
                  role_id,role_name,metric,value,sample_count,updated_at
                )
                SELECT role_id,role_name,metric,value,count(*)::BIGINT,now()
                FROM scoped GROUP BY role_id,role_name,metric,value
                ON CONFLICT(role_id,metric,value) DO UPDATE SET
                  role_name=EXCLUDED.role_name,
                  sample_count=casual_performance_metric_histogram.sample_count+EXCLUDED.sample_count,
                  updated_at=now()
                "#
            ),
            &[&claimed_ids],
        )
        .await?;
    Ok(claimed_ids.len())
}

/// Bounded backfill/repair drain for the casual cumulative projection.
///
/// Mirrors `repair_ranked_projection_gaps` (projections.rs): select a small
/// page of complete casual matches missing from the
/// `casual_performance_projection_matches` ledger and project each page in
/// its own short transaction, so the historical population seeds the
/// histogram incrementally without a single query exceeding the statement
/// timeout. Returns the number of additional matches projected this call.
pub async fn repair_casual_projection_gaps(
    database: &Database,
    page_size: usize,
) -> Result<usize, DatabaseError> {
    let page_size = page_size.clamp(1, 1000);
    let page = database
        .query_json(
            "SELECT cm.match_id AS match_id \
             FROM casual_matches cm \
             JOIN match_ingest_status mis ON mis.match_id=cm.match_id AND mis.status='complete' \
             LEFT JOIN casual_performance_projection_matches cp \
               ON cp.match_id=cm.match_id \
             WHERE cp.match_id IS NULL \
               AND cm.queue_id IN(424,452) \
             ORDER BY cm.entry_datetime,cm.match_id \
             LIMIT $1",
            &[&i64::try_from(page_size).unwrap_or(500)],
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
                "INSERT INTO casual_performance_projection_matches(match_id,projected_at) \
                 SELECT requested.match_id,now() FROM unnest($1::BIGINT[]) AS requested(match_id) \
                 ON CONFLICT(match_id) DO NOTHING RETURNING match_id",
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
        if let Err(error) = project_casual_performance_many(&transaction, &claimed).await {
            tracing::error!(page = chunk.len(), error = %error, "casual projection repair page failed");
            let _ = transaction.rollback().await;
            continue;
        }
        match transaction.commit().await {
            Ok(()) => projected += claimed.len(),
            Err(error) => {
                tracing::error!(error = %error, "casual projection repair page commit failed");
            }
        }
    }
    Ok(projected)
}

#[cfg(test)]
mod tests {
    const SOURCE: &str = include_str!("casual_performance.rs");

    /// True when `ranked_table` appears in `body` without the `casual_`
    /// prefix (the casual tables are prefixed, so masking them first leaves
    /// only genuine ranked references).
    fn contains_bare_ranked_table(body: &str, ranked_table: &str) -> bool {
        body.replace(&format!("casual_{ranked_table}"), "")
            .contains(ranked_table)
    }

    #[test]
    fn casual_projection_is_claim_plus_single_set_based_upsert() {
        let body = SOURCE
            .split_once("pub async fn project_casual_performance_many")
            .expect("start marker")
            .1
            .split_once("pub async fn repair_casual_projection_gaps")
            .expect("end marker")
            .0;
        assert!(body.contains("unnest($1::BIGINT[])"));
        assert!(body.contains("cmp.match_id=ANY($1::BIGINT[])"));
        assert_eq!(body.matches(".query(").count(), 1);
        assert_eq!(body.matches(".execute(").count(), 1);
        // The queue-486 rule: the incremental path only ever touches the
        // physically isolated casual population.
        assert!(body.contains("cm.queue_id IN(424,452)"));
        // Bare ranked table names must not appear (the casual tables are
        // prefixed `casual_`, so reject only the non-prefixed references).
        assert!(!contains_bare_ranked_table(
            body,
            "performance_metric_histogram"
        ));
        assert!(!contains_bare_ranked_table(
            body,
            "performance_metric_stats"
        ));
        assert!(!body.contains("486"));
    }

    #[test]
    fn casual_stats_refresh_is_isolated_from_ranked_tables() {
        let body = SOURCE
            .split_once("const CASUAL_PERFORMANCE_METRIC_STATS_REFRESH_SQL")
            .expect("start marker")
            .1
            .split_once("\"#;")
            .expect("end marker")
            .0;
        assert!(body.contains("FROM casual_performance_metric_histogram"));
        assert!(body.contains("INSERT INTO casual_performance_metric_stats"));
        // No ranked-table references and no queue partition key.
        assert!(!contains_bare_ranked_table(
            body,
            "performance_metric_histogram"
        ));
        assert!(!contains_bare_ranked_table(
            body,
            "performance_metric_stats"
        ));
        assert!(!body.contains("queue_id"));
    }
}
