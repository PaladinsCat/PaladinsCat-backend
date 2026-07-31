use paladinscat_core::database::{Database, DatabaseError};
use regex::Regex;
use serde::Serialize;
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const MATCH_DETAIL_SERVICE_OUTAGE_KEY: &str = "match_detail_server_regions";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HirezServiceOutageClassification {
    pub service_key: &'static str,
    pub code: &'static str,
    pub title: &'static str,
    pub severity: &'static str,
    pub reason: String,
    pub public_message: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct HirezServiceOutageState {
    pub service_key: String,
    pub status: String,
    pub reason: Option<String>,
    pub first_detected_at: Option<String>,
    pub last_detected_at: Option<String>,
    pub next_probe_at: Option<String>,
    pub probe_count: i32,
    pub last_success_at: Option<String>,
    pub updated_at: Option<String>,
}

pub fn classify_hirez_service_outage_message(
    message: impl ToString,
) -> Option<HirezServiceOutageClassification> {
    let message = message.to_string();
    if message.trim().is_empty() {
        return None;
    }
    if Regex::new(r"(?i)Server_Regions|##sql_paladins_api|Invalid object name")
        .ok()?
        .is_match(&message)
    {
        return Some(HirezServiceOutageClassification {
            service_key: MATCH_DETAIL_SERVICE_OUTAGE_KEY,
            code: "HIREZ_DETAIL_SERVER_REGIONS",
            title: "Hi-Rez match detail outage",
            severity: "critical",
            reason: message,
            public_message: "Hi-Rez match detail endpoints are returning a server-side error. Ranked match backfill is being held to one safe probe until service recovers.",
        });
    }
    if Regex::new(r"(?i)maintenance|temporarily unavailable|service unavailable|API temporarily unavailable|HTTP 503")
        .ok()?
        .is_match(&message)
    {
        return Some(HirezServiceOutageClassification {
            service_key: "hirez_api_service_unavailable",
            code: "HIREZ_SERVICE_UNAVAILABLE",
            title: "Hi-Rez API service degraded",
            severity: "warning",
            reason: message,
            public_message: "Hi-Rez API is reporting temporary service issues. Live lookups may be delayed while PaladinsCat keeps using local data.",
        });
    }
    None
}

pub async fn ensure_hirez_service_outage_table(database: &Database) -> Result<(), DatabaseError> {
    database.query_json(
        "CREATE TABLE IF NOT EXISTS hirez_service_outage_state(\
          service_key TEXT PRIMARY KEY,status TEXT NOT NULL DEFAULT 'active' CHECK(status IN('active','recovered')),\
          reason TEXT,first_detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),last_detected_at TIMESTAMPTZ,\
          next_probe_at TIMESTAMPTZ,probe_count INT NOT NULL DEFAULT 0,last_success_at TIMESTAMPTZ,\
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        &[],
    ).await?;
    database
        .query_json(
            "CREATE INDEX IF NOT EXISTS idx_hirez_service_outage_active_probe \
         ON hirez_service_outage_state(status,next_probe_at) WHERE status='active'",
            &[],
        )
        .await?;
    Ok(())
}

pub async fn record_hirez_service_outage(
    database: &Database,
    service_key: &str,
    reason: &str,
    retry_minutes: Option<i32>,
) -> Result<(), DatabaseError> {
    ensure_hirez_service_outage_table(database).await?;
    let retry =
        retry_minutes.unwrap_or_else(|| env_i32("HIREZ_DETAIL_OUTAGE_PROBE_MINUTES", 45).max(15));
    database.query_json(
        "INSERT INTO hirez_service_outage_state(service_key,status,reason,first_detected_at,last_detected_at,next_probe_at,probe_count,updated_at)\
         VALUES($1,'active',$2,now(),now(),now()+($3::INT*INTERVAL '1 minute'),1,now()) \
         ON CONFLICT(service_key) DO UPDATE SET status='active',reason=EXCLUDED.reason,last_detected_at=now(),\
         next_probe_at=now()+($3::INT*INTERVAL '1 minute'),probe_count=hirez_service_outage_state.probe_count+1,updated_at=now()",
        &[&service_key, &reason, &retry],
    ).await?;
    Ok(())
}

pub async fn mark_hirez_service_recovered(
    database: &Database,
    service_key: &str,
    reason: Option<&str>,
) -> Result<(), DatabaseError> {
    ensure_hirez_service_outage_table(database).await?;
    let reason = reason.unwrap_or("authoritative detail response returned");
    database
        .query_json(
            "UPDATE hirez_service_outage_state SET status='recovered',reason=$2,next_probe_at=NULL,\
         last_success_at=now(),updated_at=now() WHERE service_key=$1 AND status='active'",
            &[&service_key, &reason],
        )
        .await?;
    Ok(())
}

pub async fn active_hirez_service_outage(
    database: &Database,
    service_key: &str,
) -> Result<Option<HirezServiceOutageState>, DatabaseError> {
    ensure_hirez_service_outage_table(database).await?;
    Ok(database.one_json(
        "SELECT service_key,status,reason,first_detected_at::TEXT,last_detected_at::TEXT,next_probe_at::TEXT,\
         probe_count,last_success_at::TEXT,updated_at::TEXT FROM hirez_service_outage_state \
         WHERE service_key=$1 AND status='active' LIMIT 1",
        &[&service_key],
    ).await?.map(map_state))
}

pub async fn active_hirez_service_outages(
    database: &Database,
) -> Result<Vec<HirezServiceOutageState>, DatabaseError> {
    ensure_hirez_service_outage_table(database).await?;
    Ok(database.query_json(
        "SELECT service_key,status,reason,first_detected_at::TEXT,last_detected_at::TEXT,next_probe_at::TEXT,\
         probe_count,last_success_at::TEXT,updated_at::TEXT FROM hirez_service_outage_state \
         WHERE status='active' ORDER BY CASE service_key WHEN $1 THEN 0 ELSE 1 END,updated_at DESC",
        &[&MATCH_DETAIL_SERVICE_OUTAGE_KEY],
    ).await?.into_iter().map(map_state).collect())
}

pub fn is_hirez_service_outage_probe_due(
    outage: Option<&HirezServiceOutageState>,
    now: OffsetDateTime,
) -> bool {
    let Some(outage) = outage else {
        return false;
    };
    let Some(next) = outage.next_probe_at.as_deref() else {
        return true;
    };
    OffsetDateTime::parse(next, &Rfc3339).map_or(true, |next| next <= now)
}

fn map_state(row: Value) -> HirezServiceOutageState {
    let string = |key: &str| row.get(key).and_then(Value::as_str).map(str::to_owned);
    HirezServiceOutageState {
        service_key: string("service_key").unwrap_or_default(),
        status: string("status").unwrap_or_default(),
        reason: string("reason"),
        first_detected_at: string("first_detected_at"),
        last_detected_at: string("last_detected_at"),
        next_probe_at: string("next_probe_at"),
        probe_count: row
            .get("probe_count")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
        last_success_at: string("last_success_at"),
        updated_at: string("updated_at"),
    }
}

fn env_i32(name: &str, fallback: i32) -> i32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}
