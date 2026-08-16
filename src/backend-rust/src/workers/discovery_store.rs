use std::collections::BTreeMap;

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

/// Purpose: ensure the local discovery-ID authority exists. Input: database.
/// Output: schema is ready or a typed database error; no match work is queued.
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

/// Purpose: persist the complete local ID authority for one discovered hour.
/// Input: database, UTC date/hour, queue ID, typed observations, audit source.
/// Output: unique positive match count; it never creates acquisition/debt work.
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

/// Purpose: normalize vendor region aliases before durable aggregation.
/// Input: optional text. Output: one stable static region code.
fn normalize_region(value: Option<&str>) -> &'static str {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "NA" | "NORTH AMERICA" => "NA",
        "EU" | "EUROPE" => "EU",
        "ASIA" => "ASIA",
        "SEA" => "SEA",
        "JPN" | "JAPAN" => "JPN",
        "RUS" | "RUSSIA" => "RUS",
        "BR" | "BRAZIL" => "BR",
        "OCE" | "OCEANIA" => "OCE",
        "SA" | "SOUTH AMERICA" => "SA",
        _ => "Unknown",
    }
}
