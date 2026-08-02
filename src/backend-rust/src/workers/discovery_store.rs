use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use paladinscat_core::database::{Database, DatabaseError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MatchIdObservation {
    pub match_id: i64,
    pub entry_datetime: Option<String>,
    pub region: Option<String>,
    #[serde(default)]
    pub active_flag: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MatchIngestGuardResult {
    pub fetch_ids: Vec<i64>,
    pub skipped_ids: Vec<i64>,
    pub skipped: MatchIngestGuardCounts,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MatchIngestGuardCounts {
    pub matches: usize,
    pub match_players: usize,
    pub raw_buffer: usize,
    pub pull_list: usize,
    pub total_unique: usize,
}

pub async fn ensure_match_count_discovery_tables(database: &Database) -> Result<(), DatabaseError> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS match_count_discoveries(\
          match_id BIGINT NOT NULL,queue_id INT NOT NULL,region VARCHAR(20) NOT NULL DEFAULT 'Unknown',\
          entry_datetime TIMESTAMPTZ,active_flag BOOLEAN NOT NULL DEFAULT FALSE,source_date DATE NOT NULL,\
          source_hour INT NOT NULL CHECK(source_hour BETWEEN 0 AND 23),first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),\
          last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),PRIMARY KEY(match_id,queue_id))",
        "CREATE INDEX IF NOT EXISTS idx_mcd_window_queue ON match_count_discoveries(source_date DESC,source_hour,queue_id)",
        "CREATE INDEX IF NOT EXISTS idx_mcd_queue_region_window ON match_count_discoveries(queue_id,region,source_date DESC,source_hour)",
        "CREATE TABLE IF NOT EXISTS match_count_discovery_region_hours(\
          date DATE NOT NULL,hour INT NOT NULL CHECK(hour BETWEEN 0 AND 23),queue_id INT NOT NULL,region VARCHAR(20) NOT NULL,\
          match_count INT NOT NULL DEFAULT 0,updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),PRIMARY KEY(date,hour,queue_id,region))",
        "CREATE INDEX IF NOT EXISTS idx_mcdrh_window_queue ON match_count_discovery_region_hours(date DESC,hour,queue_id)",
    ] {
        database.query_json(statement, &[]).await?;
    }
    Ok(())
}

pub async fn record_match_count_discovery_result(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    raw: &[MatchIdObservation],
    _source: &str,
) -> Result<usize, DatabaseError> {
    ensure_match_count_discovery_tables(database).await?;
    let mut by_id = BTreeMap::new();
    for observation in raw {
        if observation.match_id <= 0 {
            continue;
        }
        by_id.insert(
            observation.match_id,
            json!({
                "match_id":observation.match_id,
                "entry_datetime":observation.entry_datetime,
                "region":normalize_region(observation.region.as_deref()),
                "active_flag":observation.active_flag,
            }),
        );
    }
    let observations = Value::Array(by_id.into_values().collect());
    let count = observations.as_array().map_or(0, Vec::len);
    let mut client = database.connection().await?;
    let transaction = client.transaction().await?;
    transaction.execute(
        "INSERT INTO match_count_discoveries(match_id,queue_id,region,entry_datetime,active_flag,source_date,source_hour)\
         SELECT observation.match_id,$2,observation.region,observation.entry_datetime,$3::BOOLEAN OR observation.active_flag,$4::TEXT::DATE,$5 \
         FROM jsonb_to_recordset($1::JSONB) AS observation(match_id BIGINT,region TEXT,entry_datetime TIMESTAMPTZ,active_flag BOOLEAN)\
         ON CONFLICT(match_id,queue_id) DO UPDATE SET region=CASE WHEN EXCLUDED.region<>'Unknown' THEN EXCLUDED.region ELSE match_count_discoveries.region END,\
         entry_datetime=COALESCE(match_count_discoveries.entry_datetime,EXCLUDED.entry_datetime),active_flag=EXCLUDED.active_flag,last_seen_at=now()",
        &[&observations, &queue_id, &false, &date, &hour],
    ).await?;
    if queue_id != 486 && count > 0 {
        transaction.execute(
            "INSERT INTO nonranked_match_acquisition(match_id,queue_id,stats_scope,source_date,source_hour,region,\
             discovered_entry_datetime,active_flag,status,first_discovered_at,last_observed_at)\
             SELECT discovery.match_id,discovery.queue_id,COALESCE(queue.stats_scope,'other'),discovery.source_date,\
             discovery.source_hour,discovery.region,discovery.entry_datetime,discovery.active_flag,\
             CASE WHEN discovery.active_flag THEN 'waiting_for_completion' ELSE 'discovered' END,\
             discovery.first_seen_at,discovery.last_seen_at FROM match_count_discoveries discovery \
             JOIN queue_types queue ON queue.queue_id=discovery.queue_id WHERE discovery.queue_id=$3 \
             AND discovery.source_date=$1::TEXT::DATE AND discovery.source_hour=$2 ON CONFLICT(match_id) DO UPDATE SET\
             queue_id=EXCLUDED.queue_id,stats_scope=EXCLUDED.stats_scope,\
             last_observed_at=GREATEST(nonranked_match_acquisition.last_observed_at,EXCLUDED.last_observed_at),\
             region=CASE WHEN EXCLUDED.region<>'Unknown' THEN EXCLUDED.region ELSE nonranked_match_acquisition.region END,\
             discovered_entry_datetime=COALESCE(nonranked_match_acquisition.discovered_entry_datetime,EXCLUDED.discovered_entry_datetime),\
             active_flag=EXCLUDED.active_flag,status=CASE \
               WHEN nonranked_match_acquisition.status IN('discovered','waiting_for_completion') AND EXCLUDED.active_flag THEN 'waiting_for_completion'\
               WHEN nonranked_match_acquisition.status='waiting_for_completion' AND NOT EXCLUDED.active_flag THEN 'discovered'\
               ELSE nonranked_match_acquisition.status END,updated_at=now()",
            &[&date, &hour, &queue_id],
        ).await?;
    }
    transaction.execute(
        "DELETE FROM match_count_discovery_region_hours WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3",
        &[&date, &hour, &queue_id],
    ).await?;
    transaction.execute(
        "INSERT INTO match_count_discovery_region_hours(date,hour,queue_id,region,match_count)\
         SELECT $2::TEXT::DATE,$3,$4,observation.region,count(*)::INT \
         FROM jsonb_to_recordset($1::JSONB) AS observation(region TEXT) GROUP BY observation.region",
        &[&observations, &date, &hour, &queue_id],
    ).await?;
    transaction.commit().await?;
    Ok(count)
}

pub async fn filter_already_handled_match_ids(
    database: &Database,
    match_ids: &[i64],
    include_raw_buffer: bool,
    include_pull_list: bool,
) -> Result<MatchIngestGuardResult, DatabaseError> {
    let ids = match_ids
        .iter()
        .copied()
        .filter(|id| *id > 0)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(MatchIngestGuardResult::default());
    }
    let status_rows = database
        .query_json(
            "SELECT match_id,status FROM match_ingest_status WHERE match_id=ANY($1)",
            &[&ids],
        )
        .await
        .unwrap_or_default();
    let statuses = status_rows
        .into_iter()
        .filter_map(|row| {
            Some((
                integer(&row, "match_id")?,
                row.get("status")?.as_str()?.to_owned(),
            ))
        })
        .collect::<HashMap<_, _>>();
    let terminal = |id: i64| {
        statuses
            .get(&id)
            .is_none_or(|status| matches!(status.as_str(), "complete" | "limited"))
    };
    let matches = database
        .query_json(
            "SELECT match_id FROM matches WHERE match_id=ANY($1)",
            &[&ids],
        )
        .await?
        .into_iter()
        .filter_map(|row| integer(&row, "match_id"))
        .filter(|id| terminal(*id))
        .collect::<HashSet<_>>();
    let players = database.query_json(
        "SELECT match_id FROM match_players WHERE match_id=ANY($1) GROUP BY match_id HAVING count(*)>=10",
        &[&ids],
    ).await?.into_iter().filter_map(|row| integer(&row, "match_id")).filter(|id| terminal(*id)).collect::<HashSet<_>>();
    let raw = if include_raw_buffer {
        let text_ids = ids.iter().map(i64::to_string).collect::<Vec<_>>();
        database.query_json(
            "SELECT DISTINCT entity_id FROM raw_ingest_buffer WHERE entity_type='match' AND entity_id=ANY($1) AND status IN('pending','processing')",
            &[&text_ids],
        ).await?.into_iter().filter_map(|row| row.get("entity_id")?.as_str()?.parse::<i64>().ok()).collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };
    let pull = if include_pull_list {
        database.query_json(
            "SELECT match_id FROM match_pull_list WHERE match_id=ANY($1) AND status IN('pending','pulling','completed')",
            &[&ids],
        ).await?.into_iter().filter_map(|row| integer(&row, "match_id")).collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };
    let skipped = ids
        .iter()
        .copied()
        .filter(|id| {
            matches.contains(id) || players.contains(id) || raw.contains(id) || pull.contains(id)
        })
        .collect::<Vec<_>>();
    let skipped_set = skipped.iter().copied().collect::<HashSet<_>>();
    Ok(MatchIngestGuardResult {
        fetch_ids: ids
            .into_iter()
            .filter(|id| !skipped_set.contains(id))
            .collect(),
        skipped_ids: skipped,
        skipped: MatchIngestGuardCounts {
            matches: matches.len(),
            match_players: players.len(),
            raw_buffer: raw.len(),
            pull_list: pull.len(),
            total_unique: skipped_set.len(),
        },
    })
}

fn normalize_region(value: Option<&str>) -> &'static str {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "NA" | "NORTH AMERICA" => "NA",
        "EU" | "EUROPE" => "EU",
        "ASIA" => "Asia",
        "SEA" => "SEA",
        "JPN" | "JAPAN" => "JPN",
        "RUS" | "RUSSIA" => "RUS",
        "BR" | "BRAZIL" => "BR",
        "OCE" | "OCEANIA" => "OCE",
        "SA" | "SOUTH AMERICA" => "SA",
        _ => "Unknown",
    }
}

fn integer(row: &Value, key: &str) -> Option<i64> {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
