use std::collections::BTreeMap;

use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct BufferBatchResult {
    pub processed: i32,
    pub failed: i32,
    pub deferred: i32,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetentionResult {
    pub processed_deleted: i32,
    pub failed_deleted: i32,
    pub total_deleted: i32,
}

#[derive(Clone, Copy, Debug)]
pub struct RefreshJobResult {
    pub job_id: i64,
    pub rows: i32,
}

#[derive(Clone, Debug)]
pub struct ProjectionRefreshResult {
    pub job_id: i64,
    pub counts: BTreeMap<String, i64>,
}

pub async fn process_buffer_batch(
    database: &Database,
    batch_size: usize,
) -> Result<BufferBatchResult, super::raw_buffer::RawBufferError> {
    let result = super::raw_buffer::process_raw_buffer_batch(database, batch_size).await?;
    Ok(BufferBatchResult {
        processed: result.processed,
        failed: result.failed,
        deferred: result.deferred,
    })
}

pub async fn cleanup_raw_ingest_buffer_retention(
    database: &Database,
    reason: &str,
) -> Result<RetentionResult, DatabaseError> {
    database.query_json(
        "CREATE TABLE IF NOT EXISTS raw_ingest_buffer_retention_audit(\
           id BIGSERIAL PRIMARY KEY,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),reason TEXT NOT NULL,\
           status TEXT NOT NULL,endpoint TEXT NOT NULL,entity_type TEXT NOT NULL,retention_seconds INT NOT NULL,\
           deleted_count INT NOT NULL,oldest_created_at TIMESTAMPTZ,newest_created_at TIMESTAMPTZ,\
           oldest_processed_at TIMESTAMPTZ,newest_processed_at TIMESTAMPTZ)",
        &[],
    ).await?;
    let processed_hours = env_i32("RAW_BUFFER_PROCESSED_RETENTION_HOURS", 168);
    let failed_hours = env_i32("RAW_BUFFER_FAILED_RETENTION_HOURS", 720);
    let limit = env_i32("RAW_BUFFER_RETENTION_BATCH_SIZE", 10_000);
    let processed = delete_retained(database, "processed", processed_hours, limit, reason).await?;
    let failed = delete_retained(database, "failed", failed_hours, limit, reason).await?;
    Ok(RetentionResult {
        processed_deleted: processed,
        failed_deleted: failed,
        total_deleted: processed.saturating_add(failed),
    })
}

async fn delete_retained(
    database: &Database,
    status: &str,
    retention_hours: i32,
    limit: i32,
    reason: &str,
) -> Result<i32, DatabaseError> {
    let row = database.one_json(
        "WITH doomed AS(\
           SELECT id FROM raw_ingest_buffer WHERE status=$1 \
             AND COALESCE(processed_at,created_at)<now()-($2::INT*INTERVAL '1 hour') \
           ORDER BY COALESCE(processed_at,created_at),id LIMIT $3\
         ),deleted AS(\
           DELETE FROM raw_ingest_buffer rib USING doomed d WHERE rib.id=d.id \
           RETURNING rib.status,rib.endpoint,rib.entity_type,rib.created_at,rib.processed_at\
         ),inserted AS(\
           INSERT INTO raw_ingest_buffer_retention_audit(reason,status,endpoint,entity_type,retention_seconds,\
             deleted_count,oldest_created_at,newest_created_at,oldest_processed_at,newest_processed_at)\
           SELECT $4,status,COALESCE(endpoint,''),COALESCE(entity_type,''),$2::INT*3600,count(*)::INT,\
             min(created_at),max(created_at),min(processed_at),max(processed_at) FROM deleted \
           GROUP BY status,endpoint,entity_type RETURNING deleted_count\
         ) SELECT COALESCE(sum(deleted_count),0)::INT AS deleted FROM inserted",
        &[&status, &retention_hours, &limit, &reason],
    ).await?;
    Ok(row
        .as_ref()
        .and_then(|row| number(row.get("deleted")))
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default())
}

pub async fn refresh_baselines_with_job(
    database: &Database,
    _source: &str,
) -> Result<RefreshJobResult, DatabaseError> {
    let job = database.one_json(
        "INSERT INTO sync_jobs(job_type,status,started_at) VALUES('baseline_tracker','running',now()) RETURNING id",
        &[],
    ).await?;
    let job_id = job
        .as_ref()
        .and_then(|row| number(row.get("id")))
        .unwrap_or_default();
    let result = calculate_baselines(database).await;
    match result {
        Ok(rows) => {
            database.query_json(
                "UPDATE sync_jobs SET status='completed',completed_at=now(),players_processed=$1 WHERE id=$2",
                &[&rows, &job_id],
            ).await?;
            Ok(RefreshJobResult { job_id, rows })
        }
        Err(error) => {
            let message = error.to_string();
            let _ = database.query_json(
                "UPDATE sync_jobs SET status='failed',completed_at=now(),error_message=$1 WHERE id=$2",
                &[&message, &job_id],
            ).await;
            Err(error)
        }
    }
}

async fn calculate_baselines(database: &Database) -> Result<i32, DatabaseError> {
    database
        .query_json("DELETE FROM baselines WHERE queue_id<>486", &[])
        .await?;
    let row = database.one_json(
        "WITH roles(role_id,role_name) AS(VALUES(0,'Global'),(1,'Damage'),(2,'Flank'),(3,'Support'),(4,'Frontline')),\
         samples AS(\
           SELECT roles.role_id,roles.role_name,mp.gold_per_minute::DOUBLE PRECISION AS gpm,\
             mp.damage_per_minute::DOUBLE PRECISION AS dpm,mp.healing_per_minute::DOUBLE PRECISION AS hpm,\
             mp.healing_self_per_minute::DOUBLE PRECISION AS shpm,mp.kda::DOUBLE PRECISION AS kda,\
             mp.egpm::DOUBLE PRECISION AS egpm\
           FROM roles JOIN match_players mp ON TRUE \
           JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime \
           LEFT JOIN match_ingest_status mis ON mis.match_id=m.match_id JOIN champions c ON c.id=mp.champion_id \
           WHERE (roles.role_name='Global' OR \
             CASE WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 'Frontline' \
                  WHEN c.roles ILIKE '%Damage%' THEN 'Damage' WHEN c.roles ILIKE '%Flank%' THEN 'Flank' \
                  WHEN c.roles ILIKE '%Support%' THEN 'Support' ELSE 'Unknown' END=roles.role_name)\
             AND m.queue_id=486 AND COALESCE(mis.status,'complete')='complete' \
             AND COALESCE(mp.source,'direct') IN('direct','recovered') AND mp.champion_id>0 \
             AND mp.task_force IN(1,2) AND lower(COALESCE(mp.win_status,'')) IN('winner','win','loser','loss') \
             AND m.duration_seconds>120 AND mp.gold_per_minute>0\
         ),aggregated AS(\
           SELECT role_id,role_name,count(*)::INT AS sample_size,\
             avg(gpm) avg_gpm,percentile_cont(.1) within group(order by gpm) p10_gpm,percentile_cont(.25) within group(order by gpm) p25_gpm,percentile_cont(.75) within group(order by gpm) p75_gpm,percentile_cont(.9) within group(order by gpm) p90_gpm,max(gpm) max_gpm,\
             avg(dpm) avg_dpm,percentile_cont(.1) within group(order by dpm) p10_dpm,percentile_cont(.25) within group(order by dpm) p25_dpm,percentile_cont(.75) within group(order by dpm) p75_dpm,percentile_cont(.9) within group(order by dpm) p90_dpm,max(dpm) max_dpm,\
             avg(hpm) avg_hpm,percentile_cont(.1) within group(order by hpm) p10_hpm,percentile_cont(.25) within group(order by hpm) p25_hpm,percentile_cont(.75) within group(order by hpm) p75_hpm,percentile_cont(.9) within group(order by hpm) p90_hpm,max(hpm) max_hpm,\
             avg(shpm) avg_shpm,percentile_cont(.1) within group(order by shpm) p10_shpm,percentile_cont(.25) within group(order by shpm) p25_shpm,percentile_cont(.75) within group(order by shpm) p75_shpm,percentile_cont(.9) within group(order by shpm) p90_shpm,max(shpm) max_shpm,\
             avg(kda) avg_kda,percentile_cont(.1) within group(order by kda) p10_kda,percentile_cont(.25) within group(order by kda) p25_kda,percentile_cont(.75) within group(order by kda) p75_kda,percentile_cont(.9) within group(order by kda) p90_kda,max(kda) max_kda,\
             avg(egpm) avg_egpm,percentile_cont(.1) within group(order by egpm) p10_egpm,percentile_cont(.25) within group(order by egpm) p25_egpm,percentile_cont(.75) within group(order by egpm) p75_egpm,percentile_cont(.9) within group(order by egpm) p90_egpm,max(egpm) max_egpm\
           FROM samples GROUP BY role_id,role_name HAVING count(*)>=10\
         ),deleted AS(DELETE FROM baselines WHERE queue_id=486 RETURNING role_id),inserted AS(\
           INSERT INTO baselines(role_id,role_name,queue_id,avg_gpm,p10_gpm,p25_gpm,p75_gpm,p90_gpm,max_gpm,\
             avg_dpm,p10_dpm,p25_dpm,p75_dpm,p90_dpm,max_dpm,avg_hpm,p10_hpm,p25_hpm,p75_hpm,p90_hpm,max_hpm,\
             avg_shpm,p10_shpm,p25_shpm,p75_shpm,p90_shpm,max_shpm,avg_kda,p10_kda,p25_kda,p75_kda,p90_kda,max_kda,\
             avg_egpm,p10_egpm,p25_egpm,p75_egpm,p90_egpm,max_egpm,sample_size,updated_at)\
           SELECT role_id,role_name,486,avg_gpm,p10_gpm,p25_gpm,p75_gpm,p90_gpm,max_gpm,\
             avg_dpm,p10_dpm,p25_dpm,p75_dpm,p90_dpm,max_dpm,avg_hpm,p10_hpm,p25_hpm,p75_hpm,p90_hpm,max_hpm,\
             avg_shpm,p10_shpm,p25_shpm,p75_shpm,p90_shpm,max_shpm,avg_kda,p10_kda,p25_kda,p75_kda,p90_kda,max_kda,\
             avg_egpm,p10_egpm,p25_egpm,p75_egpm,p90_egpm,max_egpm,sample_size,now() FROM aggregated RETURNING role_id\
         ) SELECT count(*)::INT AS count FROM inserted",
        &[],
    ).await?;
    Ok(row
        .as_ref()
        .and_then(|row| number(row.get("count")))
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default())
}

pub async fn refresh_derived_projections_with_job(
    database: &Database,
    _source: &str,
) -> Result<ProjectionRefreshResult, DatabaseError> {
    let job = database.one_json(
        "INSERT INTO sync_jobs(job_type,status,started_at) VALUES('derived_projection_tracker','running',now()) RETURNING id",
        &[],
    ).await?;
    let job_id = job
        .as_ref()
        .and_then(|row| number(row.get("id")))
        .unwrap_or_default();
    let result = rebuild_derived_projections(database).await;
    match result {
        Ok(counts) => {
            database.query_json(
                "UPDATE sync_jobs SET status='completed',completed_at=now(),players_processed=$1 WHERE id=$2",
                &[&i32::try_from(counts.values().sum::<i64>()).unwrap_or(i32::MAX), &job_id],
            ).await?;
            Ok(ProjectionRefreshResult { job_id, counts })
        }
        Err(error) => {
            let message = error.to_string();
            let _ = database.query_json(
                "UPDATE sync_jobs SET status='failed',completed_at=now(),error_message=$1 WHERE id=$2",
                &[&message, &job_id],
            ).await;
            Err(error)
        }
    }
}

async fn rebuild_derived_projections(
    database: &Database,
) -> Result<BTreeMap<String, i64>, DatabaseError> {
    let mut client = database.connection().await?;
    let transaction = client.transaction().await?;
    transaction
        .execute("DELETE FROM hourly_match_counts WHERE total_matches>0", &[])
        .await?;
    transaction.execute(
        "INSERT INTO hourly_match_counts(date,hour,queue_id,matches_na,matches_eu,matches_asia,matches_sea,matches_jpn,matches_rus,matches_br,matches_oce,matches_sa,matches_unknown,total_matches,fetched_at)\
         SELECT (entry_datetime AT TIME ZONE 'UTC')::DATE,extract(hour FROM entry_datetime AT TIME ZONE 'UTC')::INT,queue_id,\
         count(*) FILTER(WHERE region='NA')::INT,count(*) FILTER(WHERE region='EU')::INT,count(*) FILTER(WHERE region IN('ASIA','Asia'))::INT,\
         count(*) FILTER(WHERE region='SEA')::INT,count(*) FILTER(WHERE region='JPN')::INT,count(*) FILTER(WHERE region='RUS')::INT,\
         count(*) FILTER(WHERE region='BR')::INT,count(*) FILTER(WHERE region='OCE')::INT,count(*) FILTER(WHERE region='SA')::INT,\
         count(*) FILTER(WHERE COALESCE(region,'') NOT IN('NA','EU','ASIA','Asia','SEA','JPN','RUS','BR','OCE','SA'))::INT,count(*)::INT,now() \
         FROM matches WHERE queue_id=486 AND NOT COALESCE(limited,FALSE) GROUP BY 1,2,queue_id \
         ON CONFLICT(date,hour,queue_id) DO UPDATE SET matches_na=EXCLUDED.matches_na,matches_eu=EXCLUDED.matches_eu,\
         matches_asia=EXCLUDED.matches_asia,matches_sea=EXCLUDED.matches_sea,matches_jpn=EXCLUDED.matches_jpn,matches_rus=EXCLUDED.matches_rus,\
         matches_br=EXCLUDED.matches_br,matches_oce=EXCLUDED.matches_oce,matches_sa=EXCLUDED.matches_sa,matches_unknown=EXCLUDED.matches_unknown,\
         total_matches=EXCLUDED.total_matches,fetched_at=now()",
        &[],
    ).await?;
    // Ranked and non-ranked mechanics projections are rebuilt into physically
    // isolated tables. Queue classification never crosses this boundary.
    rebuild_item_population(&transaction, "item_counts_ranked", true).await?;
    rebuild_simple_population(
        &transaction,
        "talent_counts_ranked",
        "match_player_talents",
        "talent_id",
        true,
    )
    .await?;
    rebuild_simple_population(
        &transaction,
        "card_counts_ranked",
        "match_player_cards",
        "card_id",
        true,
    )
    .await?;
    rebuild_casual_mechanics(&transaction).await?;
    transaction.commit().await?;
    let mut counts = BTreeMap::new();
    for table in [
        "hourly_match_counts",
        "item_counts_ranked",
        "item_counts_casual",
        "talent_counts_ranked",
        "talent_counts_casual",
        "card_counts_ranked",
        "card_counts_casual",
    ] {
        let sql = format!("SELECT COUNT(*)::BIGINT AS count FROM {table}");
        let row = database.one_json(&sql, &[]).await?;
        counts.insert(
            table.to_owned(),
            row.as_ref()
                .and_then(|row| number(row.get("count")))
                .unwrap_or_default(),
        );
    }
    Ok(counts)
}

async fn rebuild_item_population(
    transaction: &tokio_postgres::Transaction<'_>,
    table: &str,
    ranked: bool,
) -> Result<(), tokio_postgres::Error> {
    transaction
        .execute(&format!("DELETE FROM {table}"), &[])
        .await?;
    let population = if ranked {
        "m.queue_id=486"
    } else {
        "m.queue_id<>486"
    };
    transaction.execute(
        &format!(
            "INSERT INTO {table}(item_id,item_name,slot,item_level,count,wins,losses,winrate,updated_at)\
             SELECT mpi.item_id,COALESCE(i.item_name,'Item '||mpi.item_id::TEXT),mpi.slot,COALESCE(mpi.item_level,0)::SMALLINT,\
             count(*)::INT,count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::INT,\
             count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('loser','loss'))::INT,\
             round(100.0*count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::NUMERIC/\
               NULLIF(count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win','loser','loss'))::NUMERIC,0),2),now()\
             FROM match_player_items mpi JOIN match_players mp ON mp.match_id=mpi.match_id AND mp.player_id=mpi.player_id \
             JOIN matches m ON m.match_id=mpi.match_id LEFT JOIN items i ON i.item_id=mpi.item_id \
             WHERE {population} AND NOT COALESCE(m.limited,FALSE) \
             GROUP BY mpi.item_id,COALESCE(i.item_name,'Item '||mpi.item_id::TEXT),mpi.slot,COALESCE(mpi.item_level,0)"
        ),
        &[],
    ).await?;
    Ok(())
}

async fn rebuild_casual_mechanics(
    transaction: &tokio_postgres::Transaction<'_>,
) -> Result<(), tokio_postgres::Error> {
    transaction
        .batch_execute(
            "DELETE FROM item_counts_casual;\
             DELETE FROM talent_counts_casual;\
             DELETE FROM card_counts_casual;",
        )
        .await?;
    let participants = "SELECT match_id,roster_slot,win_status,champion_id FROM casual_match_players \
        UNION ALL SELECT match_id,roster_slot,win_status,champion_id FROM special_match_players";
    transaction.execute(&format!(
        "INSERT INTO item_counts_casual(stats_scope,queue_id,item_id,item_name,slot,item_level,count,wins,losses,winrate,updated_at)\
         SELECT f.stats_scope,f.queue_id,f.item_id,COALESCE(i.item_name,'Item '||f.item_id),f.slot,f.item_level,\
           count(*)::BIGINT,count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win'))::BIGINT,\
           count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('loser','loss'))::BIGINT,\
           round(100.0*count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win'))::NUMERIC/\
             NULLIF(count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss')),0),2),now()\
         FROM nonranked_match_items f JOIN({participants})p ON p.match_id=f.match_id AND p.roster_slot=f.roster_slot\
         LEFT JOIN items i ON i.item_id=f.item_id GROUP BY f.stats_scope,f.queue_id,f.item_id,i.item_name,f.slot,f.item_level"
    ),&[]).await?;
    transaction.execute(&format!(
        "INSERT INTO talent_counts_casual(stats_scope,queue_id,talent_id,champion_name,talent_name,count,wins,losses,winrate,updated_at)\
         SELECT f.stats_scope,f.queue_id,f.talent_id,c.name,t.talent_name,count(*)::BIGINT,\
           count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win'))::BIGINT,\
           count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('loser','loss'))::BIGINT,\
           round(100.0*count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win'))::NUMERIC/\
             NULLIF(count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss')),0),2),now()\
         FROM nonranked_match_talents f JOIN({participants})p ON p.match_id=f.match_id AND p.roster_slot=f.roster_slot\
         LEFT JOIN talents t ON t.talent_id=f.talent_id LEFT JOIN champions c ON c.id=f.champion_id\
         GROUP BY f.stats_scope,f.queue_id,f.talent_id,c.name,t.talent_name"
    ),&[]).await?;
    transaction.execute(&format!(
        "INSERT INTO card_counts_casual(stats_scope,queue_id,card_id,champion_name,card_name,card_level,count,wins,losses,winrate,updated_at)\
         SELECT f.stats_scope,f.queue_id,f.card_id,c.name,card.card_name,f.card_level,count(*)::BIGINT,\
           count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win'))::BIGINT,\
           count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('loser','loss'))::BIGINT,\
           round(100.0*count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win'))::NUMERIC/\
             NULLIF(count(*) FILTER(WHERE lower(COALESCE(p.win_status,'')) IN('winner','win','loser','loss')),0),2),now()\
         FROM nonranked_match_cards f JOIN({participants})p ON p.match_id=f.match_id AND p.roster_slot=f.roster_slot\
         LEFT JOIN cards card ON card.card_id=f.card_id LEFT JOIN champions c ON c.id=f.champion_id\
         GROUP BY f.stats_scope,f.queue_id,f.card_id,c.name,card.card_name,f.card_level"
    ),&[]).await?;
    Ok(())
}

async fn rebuild_simple_population(
    transaction: &tokio_postgres::Transaction<'_>,
    table: &str,
    facts: &str,
    id_column: &str,
    ranked: bool,
) -> Result<(), tokio_postgres::Error> {
    transaction
        .execute(&format!("DELETE FROM {table}"), &[])
        .await?;
    let population = if ranked {
        "m.queue_id=486"
    } else {
        "m.queue_id<>486"
    };
    let sql = format!(
        "INSERT INTO {table}({id_column},count,wins,losses,winrate,updated_at)\
         SELECT fact.{id_column},count(*)::INT,\
         count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::INT,\
         count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('loser','loss'))::INT,\
         round(100.0*count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win'))::NUMERIC/\
           NULLIF(count(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN('winner','win','loser','loss'))::NUMERIC,0),2),now()\
         FROM {facts} fact JOIN match_players mp ON mp.match_id=fact.match_id AND mp.player_id=fact.player_id \
         JOIN matches m ON m.match_id=fact.match_id WHERE {population} AND NOT COALESCE(m.limited,FALSE) \
         GROUP BY fact.{id_column}"
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

fn env_i32(name: &str, fallback: i32) -> i32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &i32| *value > 0)
        .unwrap_or(fallback)
}

fn number(value: Option<&Value>) -> Option<i64> {
    value.and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
