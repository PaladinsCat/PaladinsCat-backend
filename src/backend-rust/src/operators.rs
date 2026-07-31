use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, anyhow, bail};
use paladinscat_core::{config::BackendConfig, database::Database};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::NoTls;

use crate::workers::{
    maintenance::process_buffer_batch, policy::MATCH_COUNT_QUEUE_DEFINITIONS,
    private_identity::backfill_private_account_identities, rating::RatingRepository,
    relay::WorkerRelayClient,
};

const NONRANKED_RAW_JSON_GUARD_SQL: &str = r#"
CREATE OR REPLACE FUNCTION paladinscat_drop_nonranked_raw_match()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.raw_match = NULL;
  RETURN NEW;
END
$$;
CREATE OR REPLACE FUNCTION paladinscat_compact_nonranked_raw_player()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.raw_player IS NOT NULL THEN
    NEW.raw_player = jsonb_strip_nulls(jsonb_build_object(
      '_storage','compact-equipment-v1',
      'active_id_1',NEW.raw_player->'active_id_1','item_active_1',NEW.raw_player->'item_active_1','active_level_1',NEW.raw_player->'active_level_1',
      'active_id_2',NEW.raw_player->'active_id_2','item_active_2',NEW.raw_player->'item_active_2','active_level_2',NEW.raw_player->'active_level_2',
      'active_id_3',NEW.raw_player->'active_id_3','item_active_3',NEW.raw_player->'item_active_3','active_level_3',NEW.raw_player->'active_level_3',
      'active_id_4',NEW.raw_player->'active_id_4','item_active_4',NEW.raw_player->'item_active_4','active_level_4',NEW.raw_player->'active_level_4',
      'item_id_1',NEW.raw_player->'item_id_1','item_purch_1',NEW.raw_player->'item_purch_1','item_level_1',NEW.raw_player->'item_level_1',
      'item_id_2',NEW.raw_player->'item_id_2','item_purch_2',NEW.raw_player->'item_purch_2','item_level_2',NEW.raw_player->'item_level_2',
      'item_id_3',NEW.raw_player->'item_id_3','item_purch_3',NEW.raw_player->'item_purch_3','item_level_3',NEW.raw_player->'item_level_3',
      'item_id_4',NEW.raw_player->'item_id_4','item_purch_4',NEW.raw_player->'item_purch_4','item_level_4',NEW.raw_player->'item_level_4',
      'item_id_5',NEW.raw_player->'item_id_5','item_purch_5',NEW.raw_player->'item_purch_5','item_level_5',NEW.raw_player->'item_level_5',
      'item_id_6',NEW.raw_player->'item_id_6','item_purch_6',NEW.raw_player->'item_purch_6'
    ));
  END IF;
  RETURN NEW;
END
$$;
DROP TRIGGER IF EXISTS trg_compact_casual_raw_match ON casual_matches;
CREATE TRIGGER trg_compact_casual_raw_match BEFORE INSERT OR UPDATE OF raw_match ON casual_matches
FOR EACH ROW EXECUTE FUNCTION paladinscat_drop_nonranked_raw_match();
DROP TRIGGER IF EXISTS trg_compact_special_raw_match ON special_matches;
CREATE TRIGGER trg_compact_special_raw_match BEFORE INSERT OR UPDATE OF raw_match ON special_matches
FOR EACH ROW EXECUTE FUNCTION paladinscat_drop_nonranked_raw_match();
DROP TRIGGER IF EXISTS trg_compact_casual_raw_player ON casual_match_players;
CREATE TRIGGER trg_compact_casual_raw_player BEFORE INSERT OR UPDATE OF raw_player ON casual_match_players
FOR EACH ROW EXECUTE FUNCTION paladinscat_compact_nonranked_raw_player();
DROP TRIGGER IF EXISTS trg_compact_special_raw_player ON special_match_players;
CREATE TRIGGER trg_compact_special_raw_player BEFORE INSERT OR UPDATE OF raw_player ON special_match_players
FOR EACH ROW EXECUTE FUNCTION paladinscat_compact_nonranked_raw_player();
"#;

const RAW_JSON_TABLES: [(&str, &str, &str); 4] = [
    ("casual_matches", "raw_match", "raw_match IS NOT NULL"),
    ("special_matches", "raw_match", "raw_match IS NOT NULL"),
    (
        "casual_match_players",
        "raw_player",
        "raw_player IS NOT NULL AND raw_player->>'_storage' IS DISTINCT FROM 'compact-equipment-v1'",
    ),
    (
        "special_match_players",
        "raw_player",
        "raw_player IS NOT NULL AND raw_player->>'_storage' IS DISTINCT FROM 'compact-equipment-v1'",
    ),
];

#[derive(Clone)]
pub struct OperatorServices {
    pub database: Database,
    pub relay: WorkerRelayClient,
}

impl OperatorServices {
    pub fn from_environment() -> Result<Self> {
        let config = BackendConfig::from_environment()?;
        Ok(Self {
            database: Database::new(&config, "paladinscat-admin")?,
            relay: WorkerRelayClient::new(&config)?,
        })
    }
}

pub fn options(arguments: &[String]) -> BTreeMap<String, String> {
    let mut result = BTreeMap::new();
    let mut index = 0;
    while index < arguments.len() {
        if let Some(name) = arguments[index].strip_prefix("--") {
            if let Some(value) = arguments
                .get(index + 1)
                .filter(|value| !value.starts_with("--"))
            {
                result.insert(name.to_owned(), value.clone());
                index += 2;
            } else {
                result.insert(name.to_owned(), "true".to_owned());
                index += 1;
            }
        } else {
            index += 1;
        }
    }
    result
}

pub async fn nonranked_raw_json_status(database: &Database) -> Result<Value> {
    let client = database.connection().await?;
    let mut tables = serde_json::Map::new();
    for (table, column, pending) in RAW_JSON_TABLES {
        let row = client
            .query_one(
                &format!(
                    "SELECT count(*) FILTER(WHERE {column} IS NOT NULL)::BIGINT retained,\
                     count(*) FILTER(WHERE {pending})::BIGINT pending FROM {table}"
                ),
                &[],
            )
            .await?;
        tables.insert(
            table.to_owned(),
            json!({
                "retained": row.get::<_, i64>("retained"),
                "pending": row.get::<_, i64>("pending"),
            }),
        );
    }
    let guard_count = client
        .query_one(
            "SELECT count(*)::BIGINT FROM pg_trigger WHERE NOT tgisinternal \
             AND tgname IN('trg_compact_casual_raw_match','trg_compact_special_raw_match',\
             'trg_compact_casual_raw_player','trg_compact_special_raw_player')",
            &[],
        )
        .await?
        .get::<_, i64>(0);
    Ok(json!({"guard_triggers":guard_count,"tables":tables}))
}

pub async fn mitigate_nonranked_raw_json(database: &Database, batch_size: usize) -> Result<Value> {
    let batch_size = batch_size.clamp(1, 50_000) as i64;
    let mut client = database.connection().await?;
    let transaction = client.transaction().await?;
    transaction
        .batch_execute("SET LOCAL lock_timeout='5s';SET LOCAL statement_timeout='5min'")
        .await?;
    transaction
        .batch_execute(NONRANKED_RAW_JSON_GUARD_SQL)
        .await?;
    let mut updated = serde_json::Map::new();
    for (table, column, pending) in RAW_JSON_TABLES {
        let changed = transaction
            .execute(
                &format!(
                    "WITH selected AS(SELECT ctid FROM {table} WHERE {pending} LIMIT $1 FOR UPDATE SKIP LOCKED) \
                     UPDATE {table} target SET {column}={column} FROM selected \
                     WHERE target.ctid=selected.ctid"
                ),
                &[&batch_size],
            )
            .await?;
        updated.insert(table.to_owned(), json!(changed));
    }
    transaction.commit().await?;
    Ok(json!({
        "guard_installed": true,
        "batch_size_per_table": batch_size,
        "updated": updated,
        "note": "rerun bounded batches and use storage raw-json status until pending is zero"
    }))
}

pub async fn remove_nonranked_raw_json_guard(database: &Database) -> Result<Value> {
    let client = database.connection().await?;
    client
        .batch_execute(
            "SET lock_timeout='5s';\
             DROP TRIGGER IF EXISTS trg_compact_casual_raw_match ON casual_matches;\
             DROP TRIGGER IF EXISTS trg_compact_special_raw_match ON special_matches;\
             DROP TRIGGER IF EXISTS trg_compact_casual_raw_player ON casual_match_players;\
             DROP TRIGGER IF EXISTS trg_compact_special_raw_player ON special_match_players;\
             DROP FUNCTION IF EXISTS paladinscat_drop_nonranked_raw_match();\
             DROP FUNCTION IF EXISTS paladinscat_compact_nonranked_raw_player();\
             RESET lock_timeout",
        )
        .await?;
    Ok(json!({
        "guard_removed": true,
        "warning": "already compacted rows are not expanded; the legacy writer may resume full JSON storage"
    }))
}

pub async fn pipeline_populate(
    services: &OperatorServices,
    opts: &BTreeMap<String, String>,
) -> Result<Value> {
    let from = opts
        .get("from")
        .map(String::as_str)
        .unwrap_or("2026-05-01T00:00:00Z");
    let date = from.split('T').next().unwrap_or(from).replace('-', "");
    let hour = from
        .split('T')
        .nth(1)
        .and_then(|part| part[..part.len().min(2)].parse().ok())
        .unwrap_or(0);
    let queues = if let Some(queue) = opts.get("queue") {
        vec![queue.parse::<i32>().context("invalid --queue")?]
    } else {
        MATCH_COUNT_QUEUE_DEFINITIONS
            .iter()
            .filter(|queue| queue.track_presence)
            .map(|queue| queue.queue_id)
            .collect()
    };
    let mut discovered = 0usize;
    let mut inserted = 0u64;
    for queue_id in queues {
        let response = services
            .relay
            .call_value(
                "getMatchIdsByQueueDetails",
                vec![json!(queue_id), json!(date), json!(hour)],
                "rust_operator_populate",
            )
            .await?;
        let ids = response
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|row| integer(row, &["Match", "match_id", "matchId", "id"]))
            .filter(|id| *id > 0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        discovered += ids.len();
        if !ids.is_empty() {
            inserted += services
                .database
                .connection()
                .await?
                .execute(
                    "INSERT INTO match_pull_list(match_id,queue_id,entry_datetime,status) \
                 SELECT id,$2,now(),'pending' FROM unnest($1::BIGINT[])id \
                 ON CONFLICT(match_id) DO NOTHING",
                    &[&ids, &queue_id],
                )
                .await?;
        }
    }
    Ok(json!({"discovered":discovered,"inserted":inserted}))
}

pub async fn pipeline_ingest(
    services: &OperatorServices,
    opts: &BTreeMap<String, String>,
) -> Result<Value> {
    let batch_size = option_usize(opts, "batch-size", 10)?.clamp(1, 10);
    let limit = option_usize(opts, "limit", 100)?;
    let mut buffered = 0usize;
    let mut failed = 0usize;
    for _ in 0..limit {
        let mut client = services.database.connection().await?;
        let tx = client.transaction().await?;
        let claimed = tx.query(
            "WITH candidates AS(SELECT match_id,queue_id FROM match_pull_list \
               WHERE status='pending' ORDER BY entry_datetime,match_id FOR UPDATE SKIP LOCKED LIMIT $1)\
             UPDATE match_pull_list pull SET status='pulling' FROM candidates c \
             WHERE pull.match_id=c.match_id RETURNING pull.match_id,pull.queue_id",
            &[&(batch_size as i64)],
        ).await?;
        tx.commit().await?;
        if claimed.is_empty() {
            break;
        }
        let requested = claimed
            .iter()
            .map(|row| row.get::<_, i64>("match_id"))
            .collect::<BTreeSet<_>>();
        let request = claimed
            .iter()
            .map(|row| {
                json!({
                    "matchId":row.get::<_,i64>("match_id"),"queueId":row.get::<_,i32>("queue_id")
                })
            })
            .collect::<Vec<_>>();
        let response = services
            .relay
            .call_value(
                "getMatchDetailsBatch",
                vec![Value::Array(request)],
                "rust_operator_ingest",
            )
            .await;
        match response {
            Ok(response) => {
                let rows = response
                    .as_array()
                    .cloned()
                    .unwrap_or_else(|| vec![response]);
                let mut returned = BTreeSet::new();
                let mut client = services.database.connection().await?;
                let tx = client.transaction().await?;
                for row in rows {
                    let Some(match_id) = integer(&row, &["Match", "match_id", "matchId", "id"])
                    else {
                        continue;
                    };
                    if !requested.contains(&match_id) {
                        continue;
                    }
                    returned.insert(match_id);
                    tx.execute(
                        "INSERT INTO raw_ingest_buffer(raw_data,endpoint,entity_type,entity_id,status) \
                         VALUES($1,'getmatchdetailsbatch','match',$2::TEXT,'pending')",&[&row,&match_id]
                    ).await?;
                    tx.execute(
                        "UPDATE match_pull_list SET status='completed' WHERE match_id=$1",
                        &[&match_id],
                    )
                    .await?;
                    buffered += 1;
                }
                let missing = requested.difference(&returned).copied().collect::<Vec<_>>();
                if !missing.is_empty() {
                    tx.execute("UPDATE match_pull_list SET status='pending' WHERE match_id=ANY($1::BIGINT[])",&[&missing]).await?;
                    failed += missing.len();
                }
                tx.commit().await?;
            }
            Err(error) => {
                let ids = requested.into_iter().collect::<Vec<_>>();
                services.database.query_json(
                    "UPDATE match_pull_list SET status='pending' WHERE match_id=ANY($1::BIGINT[])",&[&ids]
                ).await?;
                failed += ids.len();
                if buffered == 0 {
                    return Err(error.into());
                }
            }
        }
    }
    Ok(json!({"buffered":buffered,"failed":failed}))
}

pub async fn reference_ingest(services: &OperatorServices, kind: &str) -> Result<Value> {
    let (operation, endpoint, entity_type, keys): (&str, &str, &str, &[&str]) = match kind {
        "champions" => (
            "getChampions",
            "getchampions",
            "champion",
            &["id", "ChampionId"],
        ),
        "items" => ("getItems", "getitems", "item", &["id", "ItemId"]),
        "esports" => (
            "getEsportsProLeagueDetails",
            "getesportsproleaguedetails",
            "esports",
            &["id", "LeagueId"],
        ),
        _ => bail!("unknown reference kind {kind}"),
    };
    let response = services
        .relay
        .call_value(operation, Vec::new(), "rust_operator_static_ingest")
        .await?;
    let rows = response
        .as_array()
        .cloned()
        .unwrap_or_else(|| vec![response]);
    let mut client = services.database.connection().await?;
    let tx = client.transaction().await?;
    for row in &rows {
        let entity_id = integer(row, keys).map(|id| id.to_string());
        tx.execute(
            "INSERT INTO raw_ingest_buffer(raw_data,endpoint,entity_type,entity_id,status) \
             VALUES($1,$2,$3,$4,'pending')",
            &[row, &endpoint, &entity_type, &entity_id],
        )
        .await?;
    }
    tx.commit().await?;
    let processed = process_buffer_batch(&services.database, rows.len()).await?;
    Ok(
        json!({"received":rows.len(),"processed":processed.processed,
        "failed":processed.failed,"deferred":processed.deferred}),
    )
}

pub async fn pipeline_process(
    services: &OperatorServices,
    opts: &BTreeMap<String, String>,
) -> Result<Value> {
    let batch_size = option_usize(opts, "batch-size", 50)?;
    let limit = option_usize(opts, "limit", 100)?;
    let (mut processed, mut failed, mut deferred) = (0, 0, 0);
    for _ in 0..limit {
        let result = process_buffer_batch(&services.database, batch_size).await?;
        processed += result.processed;
        failed += result.failed;
        deferred += result.deferred;
        if result.processed + result.failed + result.deferred == 0 {
            break;
        }
    }
    Ok(json!({"processed":processed,"failed":failed,"deferred":deferred}))
}

pub async fn pipeline_run(
    services: &OperatorServices,
    opts: &BTreeMap<String, String>,
) -> Result<Value> {
    let cycles = option_usize(opts, "cycles", 10)?;
    let mut buffered = 0i64;
    let mut processed = 0i64;
    for _ in 0..cycles {
        let ingest = pipeline_ingest(
            services,
            &BTreeMap::from([
                (
                    "batch-size".to_owned(),
                    opts.get("ingest-batch-size")
                        .cloned()
                        .unwrap_or_else(|| "10".to_owned()),
                ),
                ("limit".to_owned(), "1".to_owned()),
            ]),
        )
        .await?;
        let process = pipeline_process(
            services,
            &BTreeMap::from([
                (
                    "batch-size".to_owned(),
                    opts.get("process-batch-size")
                        .cloned()
                        .unwrap_or_else(|| "50".to_owned()),
                ),
                ("limit".to_owned(), "1".to_owned()),
            ]),
        )
        .await?;
        let cycle_buffered = ingest["buffered"].as_i64().unwrap_or(0);
        let cycle_processed = process["processed"].as_i64().unwrap_or(0);
        buffered += cycle_buffered;
        processed += cycle_processed;
        if cycle_buffered
            + cycle_processed
            + process["failed"].as_i64().unwrap_or(0)
            + process["deferred"].as_i64().unwrap_or(0)
            == 0
        {
            break;
        }
    }
    Ok(json!({"buffered":buffered,"processed":processed}))
}

pub async fn pipeline_check(database: &Database) -> Result<Value> {
    Ok(json!({"pullList":database.query_json(
        "SELECT status,count(*)::BIGINT count FROM match_pull_list GROUP BY status ORDER BY status",&[]
    ).await?}))
}
pub async fn pipeline_reset_stuck(database: &Database) -> Result<Value> {
    let reset = database
        .connection()
        .await?
        .execute(
            "UPDATE match_pull_list SET status='pending' WHERE status='pulling'",
            &[],
        )
        .await?;
    Ok(json!({"reset":reset}))
}
pub async fn pipeline_buffer_status(database: &Database) -> Result<Value> {
    Ok(json!({"buffer":database.query_json(
        "SELECT status,entity_type,endpoint,count(*)::BIGINT count FROM raw_ingest_buffer \
         GROUP BY status,entity_type,endpoint ORDER BY status,entity_type,endpoint",&[]
    ).await?}))
}
pub async fn pipeline_status(database: &Database) -> Result<Value> {
    Ok(json!({
        "summary":database.one_json(
            "SELECT(SELECT count(*) FROM matches)::BIGINT matches,\
             (SELECT count(*) FROM players)::BIGINT players,\
             (SELECT count(*) FROM match_pull_list)::BIGINT pull_list",&[]
        ).await?,
        "pullList":pipeline_check(database).await?["pullList"].clone(),
        "buffer":pipeline_buffer_status(database).await?["buffer"].clone()
    }))
}

pub async fn ratings_reingest(database: &Database, apply: bool) -> Result<Value> {
    if !apply {
        return Ok(json!({"apply":false,"summary":database.one_json(
            "SELECT count(*)::BIGINT eligible_matches FROM matches m WHERE m.queue_id=486 \
             AND COALESCE(m.limited,FALSE)=FALSE AND EXISTS(SELECT 1 FROM match_players p WHERE p.match_id=m.match_id)",&[]
        ).await?}));
    }
    Ok(serde_json::to_value(
        RatingRepository::new(database.clone()).reingest().await?,
    )?)
}
pub async fn private_accounts_backfill(database: &Database, apply: bool) -> Result<Value> {
    Ok(serde_json::to_value(
        backfill_private_account_identities(database, apply).await?,
    )?)
}

pub async fn recovery_forecast(database: &Database) -> Result<Value> {
    let reserve = env_i64("API_KEY_RESERVE_CALLS", 100);
    let queue_id = env_i64("RECOVERY_FORECAST_QUEUE_ID", 486) as i32;
    let keys=database.query_json(
        "SELECT dev_id,status,daily_limit,total_24h,GREATEST(daily_limit-total_24h,0) remaining,\
         GREATEST(daily_limit-total_24h-$1::INT,0) usable_before_reserve FROM api_keys ORDER BY dev_id",
        &[&(reserve as i32)]
    ).await?;
    let backlog=database.one_json(
        "SELECT count(*)::BIGINT hours,COALESCE(sum(raw_match_count),0)::BIGINT raw,\
         COALESCE(sum(staged_match_count),0)::BIGINT staged,\
         COALESCE(sum(GREATEST(raw_match_count-staged_match_count,0)),0)::BIGINT unresolved,\
         COALESCE(max(raw_match_count),0)::INT peak_raw,\
         COALESCE(max(GREATEST(raw_match_count-staged_match_count,0)),0)::INT peak_unresolved \
         FROM hourly_ingest_state WHERE queue_id=$1 AND status IN('pending','fetching','staged','failed','empty') \
         AND GREATEST(raw_match_count-staged_match_count,0)>0",&[&queue_id]
    ).await?.unwrap_or_else(||json!({}));
    let unresolved = json_i64(&backlog, "unresolved");
    let fixed = unresolved * env_i64("RECOVERY_FORECAST_FIXED_CALLS_PER_UNRESOLVED", 2);
    Ok(
        json!({"queueId":queue_id,"reservePerKey":reserve,"keys":keys,"backlog":backlog,
        "estimate":{"baseBatchCalls":(unresolved+9)/10,"worstOrderedDetailCalls":unresolved,
        "fixedRecoveryCalls":fixed,
        "expectedHistoryAssistedTotal":unresolved+fixed+unresolved*env_i64("RECOVERY_FORECAST_HISTORY_CALLS_PER_UNRESOLVED",3),
        "worstHistoryAssistedTotal":unresolved+fixed+unresolved*env_i64("RECOVERY_FORECAST_HISTORY_CALLS_WORST_PER_UNRESOLVED",10)},
        "topEndpoints24h":database.query_json(
            "SELECT endpoint,sum(call_count)::INT calls FROM api_log WHERE hour>=now()-INTERVAL '24 hours' \
             GROUP BY endpoint ORDER BY calls DESC,endpoint LIMIT 12",&[]
        ).await?}),
    )
}

const CRITICAL_TABLES: &[&str] = &[
    "matches",
    "match_players",
    "casual_matches",
    "casual_match_players",
    "special_matches",
    "special_match_players",
    "raw_ingest_buffer",
    "match_ingest_status",
    "match_player_items",
    "match_player_cards",
    "match_player_talents",
    "item_counts_ranked",
    "item_counts_casual",
];
pub async fn migrations_compare(local_url: &str, remote_url: &str) -> Result<Value> {
    let local = summarize_database(local_url).await?;
    let remote = summarize_database(remote_url).await?;
    let differences = CRITICAL_TABLES
        .iter()
        .filter_map(|table| {
            let left = local.get(*table);
            let right = remote.get(*table);
            (left != right).then(|| json!({"table":table,"local":left,"remote":right}))
        })
        .collect::<Vec<_>>();
    Ok(json!({"matches":differences.is_empty(),"differences":differences}))
}
async fn summarize_database(url: &str) -> Result<BTreeMap<String, Value>> {
    let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let mut result = BTreeMap::new();
    for table in CRITICAL_TABLES {
        let exists = client
            .query_one("SELECT to_regclass($1) IS NOT NULL exists", &[table])
            .await?
            .get::<_, bool>(0);
        if !exists {
            result.insert((*table).to_owned(), json!({"exists":false}));
            continue;
        }
        let columns=client.query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1",&[table]
        ).await?.into_iter().map(|row|row.get::<_,String>(0)).collect::<BTreeSet<_>>();
        let mut expressions = vec!["count(*)::TEXT count".to_owned()];
        for (column, alias) in [
            ("created_at", "max_created_at"),
            ("updated_at", "max_updated_at"),
            ("id", "max_id"),
            ("match_id", "max_match_id"),
            ("player_id", "max_player_id"),
        ] {
            if columns.contains(column) {
                expressions.push(format!("max({column})::TEXT {alias}"));
            }
        }
        if columns.contains("match_id") {
            expressions.push("count(DISTINCT match_id)::TEXT distinct_match_ids".to_owned());
        }
        if columns.contains("player_id") {
            expressions.push("count(DISTINCT player_id)::TEXT distinct_player_ids".to_owned());
        }
        let row = client
            .query_one(
                &format!("SELECT {} FROM {}", expressions.join(","), table),
                &[],
            )
            .await?;
        let mut summary = serde_json::Map::from_iter([("exists".to_owned(), Value::Bool(true))]);
        for (index, column) in row.columns().iter().enumerate() {
            summary.insert(
                column.name().to_owned(),
                row.get::<_, Option<String>>(index)
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            );
        }
        result.insert((*table).to_owned(), Value::Object(summary));
    }
    Ok(result)
}

pub async fn migrations_apply(database: &Database) -> Result<Value> {
    let directory = migration_directory()?;
    let mut files = std::fs::read_dir(&directory)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension().and_then(|value| value.to_str()) == Some("sql")
                && path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .is_some_and(valid_migration_name)
        })
        .collect::<Vec<_>>();
    files.sort();
    let mut versions = BTreeSet::new();
    for path in &files {
        let version = migration_version(path)?;
        if !versions.insert(version.clone()) {
            bail!("duplicate migration version {version}");
        }
    }
    let mut client = database.connection().await?;
    client.batch_execute(
        "SELECT pg_advisory_lock(hashtext('paladinscat-schema-migrations'));\
         CREATE TABLE IF NOT EXISTS schema_migrations(version TEXT PRIMARY KEY,file_name TEXT NOT NULL UNIQUE,\
         checksum_sha256 TEXT NOT NULL,git_commit TEXT,applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),execution_ms INTEGER NOT NULL)"
    ).await?;
    let result = apply_migration_files(&mut client, &files).await;
    let _ = client
        .execute(
            "SELECT pg_advisory_unlock(hashtext('paladinscat-schema-migrations'))",
            &[],
        )
        .await;
    result
}
async fn apply_migration_files(
    client: &mut deadpool_postgres::Object,
    files: &[PathBuf],
) -> Result<Value> {
    let applied = client
        .query(
            "SELECT version,file_name,checksum_sha256 FROM schema_migrations",
            &[],
        )
        .await?
        .into_iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                (row.get::<_, String>(1), row.get::<_, String>(2)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut count = 0;
    for path in files {
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow!("invalid migration name"))?
            .to_owned();
        let version = migration_version(path)?;
        let contents = std::fs::read_to_string(path)?;
        let checksum = format!("{:x}", Sha256::digest(contents.as_bytes()));
        if let Some((name, digest)) = applied.get(&version) {
            if name != &file_name || digest != &checksum {
                bail!("applied migration {version} no longer matches {file_name}");
            }
            continue;
        }
        let header = contents.lines().take(8).collect::<Vec<_>>();
        let transaction_off = header.iter().any(|line| {
            line.trim()
                .eq_ignore_ascii_case("-- paladinscat:transaction=off")
        });
        let requires_backup = header.iter().any(|line| {
            line.trim()
                .eq_ignore_ascii_case("-- paladinscat:requires-full-backup")
        });
        if requires_backup
            && std::env::var("PALADINSCAT_DESTRUCTIVE_MIGRATIONS_CONFIRMED").as_deref() != Ok("yes")
        {
            bail!("{file_name} requires explicit full-backup confirmation");
        }
        let started = Instant::now();
        let git_commit = std::env::var("PALADINSCAT_GIT_COMMIT").ok();
        if transaction_off {
            client
                .batch_execute("SET lock_timeout='5s';SET statement_timeout='10min'")
                .await?;
            client.batch_execute(&contents).await?;
            client
                .batch_execute("RESET lock_timeout;RESET statement_timeout")
                .await?;
            let elapsed = started.elapsed().as_millis().min(i32::MAX as u128) as i32;
            client.execute("INSERT INTO schema_migrations(version,file_name,checksum_sha256,git_commit,execution_ms)VALUES($1,$2,$3,$4,$5)",
                &[&version,&file_name,&checksum,&git_commit,&elapsed]).await?;
        } else {
            let tx = client.transaction().await?;
            tx.batch_execute("SET LOCAL lock_timeout='5s';SET LOCAL statement_timeout='10min'")
                .await?;
            tx.batch_execute(&contents).await?;
            let elapsed = started.elapsed().as_millis().min(i32::MAX as u128) as i32;
            tx.execute("INSERT INTO schema_migrations(version,file_name,checksum_sha256,git_commit,execution_ms)VALUES($1,$2,$3,$4,$5)",
                &[&version,&file_name,&checksum,&git_commit,&elapsed]).await?;
            tx.commit().await?;
        }
        count += 1;
    }
    Ok(
        json!({"directory":files.first().and_then(|path|path.parent()).map(|path|path.display().to_string()),
        "applied":count,"total":files.len()}),
    )
}
fn migration_directory() -> Result<PathBuf> {
    if let Ok(configured) = std::env::var("PALADINSCAT_MIGRATIONS_DIR") {
        let path = PathBuf::from(configured);
        if path.is_dir() {
            return Ok(path);
        }
    }
    for path in [
        PathBuf::from("/app/migrations/tracked"),
        PathBuf::from("migrations/tracked"),
        PathBuf::from("../../migrations/tracked"),
    ] {
        if path.is_dir() {
            return Ok(path);
        }
    }
    bail!("migration directory not found")
}
fn valid_migration_name(name: &str) -> bool {
    let Some((version, suffix)) = name.split_once('_') else {
        return false;
    };
    version.len() >= 3
        && version.chars().all(|character| character.is_ascii_digit())
        && suffix.ends_with(".sql")
        && suffix[..suffix.len() - 4].chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}
fn migration_version(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|value| value.to_str())
        .and_then(|name| name.split_once('_').map(|(version, _)| version.to_owned()))
        .ok_or_else(|| anyhow!("invalid migration file name {}", path.display()))
}
fn option_usize(options: &BTreeMap<String, String>, name: &str, default: usize) -> Result<usize> {
    options
        .get(name)
        .map(|value| value.parse().with_context(|| format!("invalid --{name}")))
        .transpose()
        .map(|value| value.unwrap_or(default))
}
fn integer(value: &Value, keys: &[&str]) -> Option<i64> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
    })
}
fn json_i64(value: &Value, key: &str) -> i64 {
    value
        .get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default()
}
fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod storage_mitigation_tests {
    use super::{NONRANKED_RAW_JSON_GUARD_SQL, RAW_JSON_TABLES};

    #[test]
    fn raw_json_guard_preserves_only_legacy_equipment_fallback_fields() {
        for key in [
            "active_id_1",
            "item_active_4",
            "active_level_4",
            "item_id_1",
            "item_purch_5",
            "item_level_5",
            "item_id_6",
            "item_purch_6",
        ] {
            assert!(NONRANKED_RAW_JSON_GUARD_SQL.contains(key), "missing {key}");
        }
        assert!(NONRANKED_RAW_JSON_GUARD_SQL.contains("NEW.raw_match = NULL"));
        assert!(NONRANKED_RAW_JSON_GUARD_SQL.contains("compact-equipment-v1"));
        assert!(!NONRANKED_RAW_JSON_GUARD_SQL.contains("player_name"));
    }

    #[test]
    fn raw_json_cleanup_targets_only_the_four_nonranked_payload_columns() {
        assert_eq!(RAW_JSON_TABLES.len(), 4);
        assert_eq!(
            RAW_JSON_TABLES.map(|(table, column, _)| (table, column)),
            [
                ("casual_matches", "raw_match"),
                ("special_matches", "raw_match"),
                ("casual_match_players", "raw_player"),
                ("special_match_players", "raw_player"),
            ]
        );
    }
}
