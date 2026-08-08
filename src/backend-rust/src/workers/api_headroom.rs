use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiHeadroomSnapshot {
    pub total_keys: u64,
    pub usable_keys: u64,
    pub total_usable_before_reserve: u64,
    pub has_usable_keys: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiHeadroomError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

pub async fn get_api_headroom(
    database: &Database,
    reserve_per_key: i32,
) -> Result<ApiHeadroomSnapshot, ApiHeadroomError> {
    let row = database
        .one_json(
            "SELECT COUNT(*) AS total_keys,\
             COUNT(*) FILTER(WHERE status NOT IN('limited','unhealthy','exhausted') AND GREATEST(daily_limit-total_24h,0)>$1) AS usable_keys,\
             COALESCE(SUM(GREATEST(daily_limit-total_24h-$1,0)),0) AS total_usable_before_reserve FROM api_keys",
            &[&reserve_per_key],
        )
        .await?
        .unwrap_or(Value::Null);
    let total_keys = value_u64(row.get("total_keys"));
    let usable_keys = value_u64(row.get("usable_keys"));
    Ok(ApiHeadroomSnapshot {
        total_keys,
        usable_keys,
        total_usable_before_reserve: value_u64(row.get("total_usable_before_reserve")),
        has_usable_keys: total_keys == 0 || usable_keys > 0,
    })
}

pub async fn check_api_budget(database: &Database) -> Result<bool, ApiHeadroomError> {
    let reserve = std::env::var("API_KEY_RESERVE_CALLS")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(100)
        .max(0);
    let snapshot = get_api_headroom(database, reserve).await?;
    Ok(snapshot.has_usable_keys)
}

pub async fn get_api_budget_summary(
    database: &Database,
) -> Result<Vec<ApiHeadroomSnapshot>, ApiHeadroomError> {
    let reserve = std::env::var("API_KEY_RESERVE_CALLS")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(100)
        .max(0);
    let row = database
        .one_json(
            "SELECT COUNT(*) AS total_keys,\
             COUNT(*) FILTER(WHERE status NOT IN('limited','unhealthy','exhausted') AND GREATEST(daily_limit-total_24h,0)>$1) AS usable_keys,\
             COALESCE(SUM(GREATEST(daily_limit-total_24h-$1,0)),0) AS total_usable_before_reserve FROM api_keys",
            &[&reserve],
        )
        .await?
        .unwrap_or(Value::Null);
    let total_keys = value_u64(row.get("total_keys"));
    let usable_keys = value_u64(row.get("usable_keys"));
    Ok(vec![ApiHeadroomSnapshot {
        total_keys,
        usable_keys,
        total_usable_before_reserve: value_u64(row.get("total_usable_before_reserve")),
        has_usable_keys: total_keys == 0 || usable_keys > 0,
    }])
}

fn value_u64(value: Option<&Value>) -> u64 {
    value
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default()
}
