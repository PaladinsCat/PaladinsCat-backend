use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlayerHistoryRetentionResult {
    pub cache_expired_deleted: i32,
    pub entry_expired_deleted: i32,
    pub entry_authoritative_deleted: i32,
    pub total_deleted: i32,
}

struct RetentionClass<'a> {
    table: &'a str,
    class: &'a str,
    keys: &'a str,
    predicate: &'a str,
    hours: i32,
}

pub async fn cleanup_player_history_retention(
    database: &Database,
    reason: &str,
) -> Result<PlayerHistoryRetentionResult, DatabaseError> {
    database.query_json(
        "CREATE TABLE IF NOT EXISTS player_history_retention_audit(\
          id BIGSERIAL PRIMARY KEY,reason TEXT NOT NULL,table_name TEXT NOT NULL,delete_class TEXT NOT NULL,\
          deleted_count INT NOT NULL,retention_seconds INT NOT NULL,oldest_observed_at TIMESTAMPTZ,\
          newest_observed_at TIMESTAMPTZ,oldest_expires_at TIMESTAMPTZ,newest_expires_at TIMESTAMPTZ,\
          created_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        &[],
    ).await?;
    database.query_json(
        "CREATE INDEX IF NOT EXISTS idx_player_history_retention_audit_created ON player_history_retention_audit(created_at DESC)",
        &[],
    ).await?;
    let limit = env_i32("PLAYER_HISTORY_RETENTION_BATCH_SIZE", 5_000);
    let cache = delete_class(
        database,
        reason,
        RetentionClass {
            table: "player_match_history_cache",
            class: "expired_cache",
            keys: "player_id",
            predicate: "expires_at<now()-($1::INT*INTERVAL '1 hour')",
            hours: env_i32("PLAYER_HISTORY_CACHE_EXPIRED_GRACE_HOURS", 24),
        },
        limit,
    )
    .await?;
    let expired = delete_class(
        database,
        reason,
        RetentionClass {
            table: "player_match_history_entries",
            class: "expired_entry",
            keys: "match_id,player_id",
            predicate: "expires_at IS NOT NULL AND expires_at<now()-($1::INT*INTERVAL '1 hour')",
            hours: env_i32("PLAYER_HISTORY_ENTRY_EXPIRED_GRACE_HOURS", 24),
        },
        limit,
    )
    .await?;
    let authority_hours = env_i32("PLAYER_HISTORY_ENTRY_AUTHORITY_GRACE_HOURS", 24);
    let row = database.one_json(
        "WITH doomed AS(SELECT e.match_id,e.player_id FROM player_match_history_entries e \
           WHERE e.observed_at<now()-($1::INT*INTERVAL '1 hour') AND EXISTS(SELECT 1 FROM match_players mp \
             WHERE mp.match_id=e.match_id AND mp.player_id=e.player_id AND mp.source IN('direct','recovered')) \
           ORDER BY e.observed_at,e.match_id,e.player_id LIMIT $2::INT),deleted AS(\
           DELETE FROM player_match_history_entries e USING doomed d WHERE e.match_id=d.match_id AND e.player_id=d.player_id \
           RETURNING e.observed_at,e.expires_at),inserted AS(\
           INSERT INTO player_history_retention_audit(reason,table_name,delete_class,deleted_count,retention_seconds,\
             oldest_observed_at,newest_observed_at,oldest_expires_at,newest_expires_at)\
           SELECT $3,'player_match_history_entries','covered_by_authoritative_match_players',count(*)::INT,$1*3600,\
             min(observed_at),max(observed_at),min(expires_at),max(expires_at) FROM deleted HAVING count(*)>0 RETURNING deleted_count\
         ) SELECT COALESCE(sum(deleted_count),0)::INT deleted FROM inserted",
        &[&authority_hours, &limit, &reason],
    ).await?;
    let authority = row
        .as_ref()
        .and_then(|row| integer(row, "deleted"))
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default();
    Ok(PlayerHistoryRetentionResult {
        cache_expired_deleted: cache,
        entry_expired_deleted: expired,
        entry_authoritative_deleted: authority,
        total_deleted: cache.saturating_add(expired).saturating_add(authority),
    })
}

async fn delete_class(
    database: &Database,
    reason: &str,
    retention: RetentionClass<'_>,
    limit: i32,
) -> Result<i32, DatabaseError> {
    let RetentionClass {
        table,
        class,
        keys,
        predicate,
        hours,
    } = retention;
    let key_columns = keys.split(',').collect::<Vec<_>>();
    let join = key_columns
        .iter()
        .map(|key| format!("target.{key}=doomed.{key}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let order = if table.ends_with("_cache") {
        "expires_at,player_id"
    } else {
        "expires_at,match_id,player_id"
    };
    let sql = format!(
        "WITH doomed AS(SELECT {keys} FROM {table} WHERE {predicate} ORDER BY {order} LIMIT $2::INT),deleted AS(\
         DELETE FROM {table} target USING doomed WHERE {join} RETURNING target.{observed},target.expires_at),inserted AS(\
         INSERT INTO player_history_retention_audit(reason,table_name,delete_class,deleted_count,retention_seconds,\
           oldest_observed_at,newest_observed_at,oldest_expires_at,newest_expires_at)\
         SELECT $3,'{table}','{class}',count(*)::INT,$1*3600,min({observed}),max({observed}),min(expires_at),max(expires_at)\
         FROM deleted HAVING count(*)>0 RETURNING deleted_count) SELECT COALESCE(sum(deleted_count),0)::INT deleted FROM inserted",
        observed = if table.ends_with("_cache") {
            "fetched_at"
        } else {
            "observed_at"
        },
    );
    let row = database.one_json(&sql, &[&hours, &limit, &reason]).await?;
    Ok(row
        .as_ref()
        .and_then(|row| integer(row, "deleted"))
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default())
}

fn integer(row: &Value, key: &str) -> Option<i64> {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
fn env_i32(name: &str, fallback: i32) -> i32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &i32| *value > 0)
        .unwrap_or(fallback)
}
