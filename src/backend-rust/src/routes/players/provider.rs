use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::Response,
};
use paladinscat_core::web_compat::parse_js_integer;
use serde::Deserialize;
use serde_json::{Value, json};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    error::ApiError,
    raw_hirez_audit::{RawHirezAudit, record_raw_hirez_response},
    request::RequestId,
    routes::live::{request_identity, vendor_guard},
};

use super::{DISPLAY_NAME_SQL, PlayersState, json_response, loadout_rows, map_database, player_id};

const PROFILE_TTL_SECONDS: i64 = 24 * 60 * 60;
const CHAMPION_TTL_SECONDS: i64 = 10 * 60;
const DISCORD_CHAMPION_TTL_SECONDS: i64 = 24 * 60 * 60;
const LOADOUT_TTL_SECONDS: i64 = 24 * 60 * 60;
const LOADOUT_COOLDOWN_SECONDS: i64 = 10 * 60;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ProviderQuery {
    player: Option<String>,
    history: Option<String>,
    discord_user_id: Option<String>,
    player_id: Option<String>,
    id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SavedPlayerBody {
    discord_user_id: Option<String>,
    player_id: Option<Value>,
}

#[derive(Clone)]
pub(super) struct ProfileFreshness {
    pub(super) ttl_seconds: i64,
    pub(super) refreshed_at: Option<String>,
    pub(super) expires_at: Option<String>,
    pub(super) remaining_seconds: i64,
    pub(super) expired: bool,
}

impl ProfileFreshness {
    fn json(&self) -> Value {
        json!({
            "ttl_seconds":self.ttl_seconds,
            "refreshed_at":self.refreshed_at,
            "expires_at":self.expires_at,
            "remaining_seconds":self.remaining_seconds,
            "expired":self.expired
        })
    }
}

pub(super) fn profile_freshness(player: &Value, ttl_seconds: i64) -> ProfileFreshness {
    let refreshed = player
        .get("hirez_profile_refreshed_at")
        .and_then(Value::as_str)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok());
    let Some(refreshed) = refreshed else {
        return ProfileFreshness {
            ttl_seconds,
            refreshed_at: None,
            expires_at: None,
            remaining_seconds: 0,
            expired: true,
        };
    };
    let expires = refreshed + Duration::seconds(ttl_seconds);
    let remaining = (expires - OffsetDateTime::now_utc()).whole_seconds().max(0);
    ProfileFreshness {
        ttl_seconds,
        refreshed_at: refreshed.format(&Rfc3339).ok(),
        expires_at: expires.format(&Rfc3339).ok(),
        remaining_seconds: remaining,
        expired: remaining == 0,
    }
}

fn relay<'a>(
    state: &'a PlayersState,
    request_id: &RequestId,
) -> Result<&'a crate::workers::relay::WorkerRelayClient, ApiError> {
    state
        .relay
        .as_ref()
        .ok_or_else(|| ApiError::internal(request_id))
}

async fn guard(
    state: &PlayersState,
    headers: &HeaderMap,
    scope: &str,
    entity: impl ToString,
    window_ms: u64,
    limit: u64,
) -> Result<(), ApiError> {
    vendor_guard(
        &state.redis,
        &request_identity(headers),
        scope,
        entity,
        window_ms,
        limit,
    )
    .await?;
    Ok(())
}

fn value_rows(value: &Value) -> Vec<&Value> {
    value
        .as_array()
        .map(|rows| rows.iter().filter(|row| row.is_object()).collect())
        .unwrap_or_else(|| value.is_object().then_some(value).into_iter().collect())
}

fn value_i64(row: &Value, fields: &[&str]) -> i64 {
    fields
        .iter()
        .find_map(|field| {
            let value = row.get(*field)?;
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                .or_else(|| value.as_str()?.trim().parse().ok())
        })
        .unwrap_or_default()
}

fn value_text(row: &Value, fields: &[&str]) -> String {
    fields
        .iter()
        .find_map(|field| {
            row.get(*field)
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_default()
        .replace('\0', "")
        .replace("\\u0000", "")
}

fn optional_text(row: &Value, fields: &[&str]) -> Option<String> {
    let value = value_text(row, fields);
    (!value.is_empty()).then_some(value)
}

fn synthetic_name(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    (lower.starts_with("dummyplayer")
        && lower["dummyplayer".len()..]
            .chars()
            .all(|character| character.is_ascii_digit()))
        || (lower.len() >= 27
            && lower.contains("user-")
            && lower.split("user-").next().is_some_and(|prefix| {
                prefix.len() >= 20 && prefix.chars().all(|c| c.is_ascii_hexdigit())
            }))
}

fn normalize_region(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "north america" | "na" => "North America".to_owned(),
        "europe" | "eu" => "Europe".to_owned(),
        "brazil" | "br" => "Brazil".to_owned(),
        "latin america north" | "latam north" => "Latin America North".to_owned(),
        "latin america south" | "latam south" => "Latin America South".to_owned(),
        "southeast asia" | "sea" => "Southeast Asia".to_owned(),
        "australia" | "oceania" => "Australia".to_owned(),
        "japan" => "Japan".to_owned(),
        _ if value.trim().is_empty() => "Unknown".to_owned(),
        _ => value.trim().to_owned(),
    }
}

fn calculated_level(total_xp: i64, api_level: i64) -> i64 {
    if total_xp <= 0 {
        return api_level.max(0);
    }
    // Preserve the canonical profile level formula: level N requires
    // 20,000 * N * (N - 1) / 2 cumulative XP.
    ((((1.0 + 4.0 * total_xp as f64 / 10_000.0).sqrt() + 1.0) / 2.0).floor() as i64).max(api_level)
}

fn ranked<'a>(row: &'a Value, field: &str) -> &'a Value {
    row.get(field)
        .filter(|value| value.is_object())
        .unwrap_or(&Value::Null)
}

async fn upsert_profile(
    state: &PlayersState,
    raw: &Value,
    request_id: &RequestId,
) -> Result<i64, ApiError> {
    let player_id = value_i64(raw, &["Id", "ActivePlayerId"]);
    if player_id <= 0 {
        return Err(ApiError::coded(
            StatusCode::BAD_GATEWAY,
            "REFRESH_FAILED",
            format!("Hi-Rez returned no usable player profile for {player_id}"),
        ));
    }
    let platform_name = optional_text(raw, &["Name"]);
    let hz_player_name = optional_text(raw, &["hz_player_name"]);
    let hz_gamer_tag = optional_text(raw, &["hz_gamer_tag"]);
    let (name_source, name) = [
        ("hz_player_name", hz_player_name.as_deref()),
        ("hz_gamer_tag", hz_gamer_tag.as_deref()),
        ("name", platform_name.as_deref()),
    ]
    .into_iter()
    .find(|(_, value)| value.is_some_and(|value| !synthetic_name(value)))
    .map(|(source, value)| (source.to_owned(), value.unwrap_or_default().to_owned()))
    .unwrap_or_else(|| ("none".to_owned(), format!("Player {player_id}")));
    let anomaly = [
        platform_name.as_ref(),
        hz_player_name.as_ref(),
        hz_gamer_tag.as_ref(),
    ]
    .into_iter()
    .flatten()
    .any(|value| synthetic_name(value));
    let api_level = value_i64(raw, &["Level", "level"]);
    let total_xp = value_i64(raw, &["Total_XP", "total_xp"]);
    let kbm = ranked(raw, "RankedKBM");
    let controller = ranked(raw, "RankedController");
    let conquest = ranked(raw, "RankedConquest");
    let region = normalize_region(&value_text(raw, &["Region"]));
    let platform = optional_text(raw, &["Platform"]);
    let privacy = value_text(raw, &["privacy_flag"]).eq_ignore_ascii_case("y");
    let ret_msg = optional_text(raw, &["ret_msg"]);
    let avatar_url = optional_text(raw, &["AvatarURL"]);
    let created = optional_text(raw, &["Created_Datetime"]);
    let last_login = optional_text(raw, &["Last_Login_Datetime"]);
    let active_id = value_i64(raw, &["ActivePlayerId", "Id"]);
    let merged = raw
        .get("MergedPlayers")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|row| {
                    let id = value_i64(row, &["playerId", "player_id"]);
                    (id > 0).then_some(id.to_string())
                })
                .collect::<Vec<_>>()
        });
    let mut owned: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        Vec::with_capacity(70);
    macro_rules! param {
        ($value:expr) => {
            owned.push(Box::new($value));
        };
    }
    param!(player_id);
    param!(active_id);
    param!(name);
    param!(i32::try_from(calculated_level(total_xp, api_level)).unwrap_or_default());
    param!(i32::try_from(api_level).unwrap_or_default());
    for field in [
        "Wins",
        "Losses",
        "Leaves",
        "HoursPlayed",
        "MinutesPlayed",
        "MasteryLevel",
    ] {
        param!(i32::try_from(value_i64(raw, &[field])).unwrap_or_default());
    }
    param!(region);
    param!(platform);
    param!(ret_msg);
    param!(total_xp);
    param!(value_i64(raw, &["Total_Worshippers"]));
    param!(i32::try_from(value_i64(raw, &["Total_Achievements"])).unwrap_or_default());
    param!(i32::try_from(value_i64(raw, &["AvatarId"])).unwrap_or_default());
    param!(avatar_url);
    param!(value_text(raw, &["Title"]));
    param!(value_text(raw, &["LoadingFrame"]));
    param!(created);
    param!(last_login);
    param!(value_text(raw, &["Personal_Status_Message"]));
    param!(i32::try_from(value_i64(raw, &["TeamId"])).unwrap_or_default());
    param!(value_text(raw, &["Team_Name"]));
    param!(merged);
    param!(if privacy {
        "y".to_owned()
    } else {
        "n".to_owned()
    });
    for (ranked, tier_fallback) in [
        (kbm, value_i64(raw, &["Tier_RankedKBM"])),
        (controller, value_i64(raw, &["Tier_RankedController"])),
        (conquest, value_i64(raw, &["Tier_Conquest"])),
    ] {
        param!(value_text(ranked, &["Name"]));
        param!(i32::try_from(value_i64(ranked, &["Points"])).unwrap_or_default());
        param!(i32::try_from(value_i64(ranked, &["Tier"]).max(tier_fallback)).unwrap_or_default());
        for field in [
            "Rank", "Wins", "Losses", "Leaves", "Trend", "PrevRank", "Season",
        ] {
            param!(i32::try_from(value_i64(ranked, &[field])).unwrap_or_default());
        }
        param!(optional_positive(value_i64(ranked, &["player_id"])));
        param!(optional_text(ranked, &["ret_msg"]));
    }
    param!(platform_name);
    param!(hz_player_name);
    param!(hz_gamer_tag);
    param!(name_source);
    param!(anomaly);
    param!(anomaly.then(|| "profile contained a synthetic display identity".to_owned()));
    let fields = owned
        .iter()
        .map(|value| value.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect::<Vec<&(dyn tokio_postgres::types::ToSql + Sync)>>();
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| map_database(error, request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), request_id))?;
    transaction
        .execute(
            "INSERT INTO players (id,active_player_id,name,level,api_level,wins,losses,leaves,hours_played,minutes_played,mastery_level,region,platform,ret_msg,total_xp,total_worshippers,total_achievements,avatar_id,avatar_url,title,loading_frame,created_datetime,last_login_datetime,personal_status_message,team_id,team_name,merged_players,privacy_flag,kbm_name,kbm_points,kbm_tier,kbm_rank,kbm_wins,kbm_losses,kbm_leaves,kbm_trend,kbm_prev_rank,kbm_season,kbm_player_id,kbm_ret_msg,controller_name,controller_points,controller_tier,controller_rank,controller_wins,controller_losses,controller_leaves,controller_trend,controller_prev_rank,controller_season,controller_player_id,controller_ret_msg,conquest_name,conquest_points,conquest_tier,conquest_rank,conquest_wins,conquest_losses,conquest_leaves,conquest_trend,conquest_prev_rank,conquest_season,conquest_player_id,conquest_ret_msg,platform_name,hz_player_name,hz_gamer_tag,name_source,name_anomaly,name_anomaly_reason,name_anomaly_detected_at,first_seen,last_seen,last_updated,hirez_profile_refreshed_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22::TEXT::TIMESTAMPTZ,$23::TEXT::TIMESTAMPTZ,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,CASE WHEN $69 THEN now() ELSE NULL END,now(),now(),now(),now()) \
             ON CONFLICT(id) DO UPDATE SET \
               active_player_id=EXCLUDED.active_player_id,name=CASE WHEN EXCLUDED.name_source<>'none' AND NULLIF(EXCLUDED.name,'') IS NOT NULL THEN EXCLUDED.name WHEN players.name~*'^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$' THEN 'Player '||players.id::text ELSE players.name END, \
               level=EXCLUDED.level,api_level=EXCLUDED.api_level,wins=EXCLUDED.wins,losses=EXCLUDED.losses,leaves=EXCLUDED.leaves,hours_played=EXCLUDED.hours_played,minutes_played=EXCLUDED.minutes_played,mastery_level=EXCLUDED.mastery_level, \
               region=CASE WHEN NULLIF(BTRIM(EXCLUDED.region),'') IS NOT NULL AND UPPER(EXCLUDED.region)<>'UNKNOWN' THEN EXCLUDED.region ELSE players.region END, \
               platform=CASE WHEN NULLIF(BTRIM(EXCLUDED.platform),'') IS NOT NULL AND UPPER(EXCLUDED.platform)<>'UNKNOWN' THEN EXCLUDED.platform ELSE players.platform END, \
               ret_msg=EXCLUDED.ret_msg,total_xp=EXCLUDED.total_xp,total_worshippers=EXCLUDED.total_worshippers,total_achievements=EXCLUDED.total_achievements,avatar_id=EXCLUDED.avatar_id,avatar_url=EXCLUDED.avatar_url,title=EXCLUDED.title,loading_frame=EXCLUDED.loading_frame,created_datetime=EXCLUDED.created_datetime,last_login_datetime=EXCLUDED.last_login_datetime,personal_status_message=EXCLUDED.personal_status_message,team_id=EXCLUDED.team_id,team_name=EXCLUDED.team_name,merged_players=EXCLUDED.merged_players,privacy_flag=EXCLUDED.privacy_flag, \
               kbm_name=EXCLUDED.kbm_name,kbm_points=EXCLUDED.kbm_points,kbm_tier=EXCLUDED.kbm_tier,kbm_rank=EXCLUDED.kbm_rank,kbm_wins=EXCLUDED.kbm_wins,kbm_losses=EXCLUDED.kbm_losses,kbm_leaves=EXCLUDED.kbm_leaves,kbm_trend=EXCLUDED.kbm_trend,kbm_prev_rank=EXCLUDED.kbm_prev_rank,kbm_season=EXCLUDED.kbm_season,kbm_player_id=EXCLUDED.kbm_player_id,kbm_ret_msg=EXCLUDED.kbm_ret_msg, \
               controller_name=EXCLUDED.controller_name,controller_points=EXCLUDED.controller_points,controller_tier=EXCLUDED.controller_tier,controller_rank=EXCLUDED.controller_rank,controller_wins=EXCLUDED.controller_wins,controller_losses=EXCLUDED.controller_losses,controller_leaves=EXCLUDED.controller_leaves,controller_trend=EXCLUDED.controller_trend,controller_prev_rank=EXCLUDED.controller_prev_rank,controller_season=EXCLUDED.controller_season,controller_player_id=EXCLUDED.controller_player_id,controller_ret_msg=EXCLUDED.controller_ret_msg, \
               conquest_name=EXCLUDED.conquest_name,conquest_points=EXCLUDED.conquest_points,conquest_tier=EXCLUDED.conquest_tier,conquest_rank=EXCLUDED.conquest_rank,conquest_wins=EXCLUDED.conquest_wins,conquest_losses=EXCLUDED.conquest_losses,conquest_leaves=EXCLUDED.conquest_leaves,conquest_trend=EXCLUDED.conquest_trend,conquest_prev_rank=EXCLUDED.conquest_prev_rank,conquest_season=EXCLUDED.conquest_season,conquest_player_id=EXCLUDED.conquest_player_id,conquest_ret_msg=EXCLUDED.conquest_ret_msg, \
               platform_name=EXCLUDED.platform_name,hz_player_name=EXCLUDED.hz_player_name,hz_gamer_tag=EXCLUDED.hz_gamer_tag,name_source=CASE WHEN EXCLUDED.name_source<>'none' THEN EXCLUDED.name_source ELSE players.name_source END,name_anomaly=EXCLUDED.name_anomaly,name_anomaly_reason=CASE WHEN EXCLUDED.name_anomaly THEN EXCLUDED.name_anomaly_reason ELSE players.name_anomaly_reason END,name_anomaly_detected_at=CASE WHEN EXCLUDED.name_anomaly THEN COALESCE(players.name_anomaly_detected_at,now()) ELSE players.name_anomaly_detected_at END,hirez_profile_refreshed_at=now(),last_seen=now(),last_updated=now()",
            &fields,
        )
        .await
        .map_err(|error| ApiError::database(error.into(), request_id))?;
    transaction
        .execute(
            "DELETE FROM player_profile_merged_players WHERE player_id=$1",
            &[&player_id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), request_id))?;
    if let Some(rows) = raw.get("MergedPlayers").and_then(Value::as_array) {
        for row in rows {
            let merged_id = value_i64(row, &["playerId", "player_id"]);
            if merged_id <= 0 {
                continue;
            }
            let portal_id = optional_positive(value_i64(row, &["portalId", "portal_id"]));
            let merged_at = optional_text(row, &["mergeDatetime", "merge_datetime"]);
            transaction
                .execute(
                    "INSERT INTO player_profile_merged_players(player_id,merged_player_id,portal_id,merge_datetime,profile_refreshed_at) VALUES($1,$2,$3,$4::TEXT::TIMESTAMPTZ,now()) \
                     ON CONFLICT(player_id,merged_player_id) DO UPDATE SET portal_id=EXCLUDED.portal_id,merge_datetime=EXCLUDED.merge_datetime,profile_refreshed_at=now()",
                    &[&player_id, &merged_id, &portal_id, &merged_at],
                )
                .await
                .map_err(|error| ApiError::database(error.into(), request_id))?;
        }
    }
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), request_id))?;
    Ok(player_id)
}

fn optional_positive(value: i64) -> Option<i64> {
    (value > 0).then_some(value)
}

async fn fetch_profile(
    state: &PlayersState,
    player_id: i64,
    consumer: &str,
    reason: &str,
    source: &str,
    request_id: &RequestId,
) -> Result<(Value, Option<Value>), ApiError> {
    let raw = relay(state, request_id)?
        .call_value("getPlayerBatch", vec![json!([player_id])], consumer)
        .await
        .map_err(|error| {
            tracing::warn!(%error, player_id, "player profile relay call failed");
            ApiError::coded(StatusCode::BAD_GATEWAY, "REFRESH_FAILED", error.to_string())
        })?;
    let audit = record_raw_hirez_response(
        &state.database,
        RawHirezAudit {
            endpoint: "getplayerbatch",
            operation: "getPlayerBatch",
            entity_type: "player",
            entity_id: player_id.to_string(),
            params: json!({"playerIds":[player_id],"reason":reason}),
            raw_response: &raw,
            source,
        },
    )
    .await
    .map_err(|error| map_database(error, request_id))?;
    let profile = value_rows(&raw)
        .into_iter()
        .find(|row| value_i64(row, &["Id", "ActivePlayerId"]) == player_id)
        .or_else(|| value_rows(&raw).into_iter().next())
        .ok_or_else(|| {
            ApiError::coded(
                StatusCode::BAD_GATEWAY,
                "REFRESH_FAILED",
                format!("Hi-Rez returned no usable player profile for {player_id}"),
            )
        })?;
    upsert_profile(state, profile, request_id).await?;
    Ok((raw, audit))
}

async fn refresh_profile_record(
    state: &PlayersState,
    player_id: i64,
    force: bool,
    consumer: &str,
    reason: &str,
    source: &str,
    request_id: &RequestId,
) -> Result<(bool, ProfileFreshness, Option<Value>), ApiError> {
    let lock_name = format!("player-profile-refresh:{player_id}");
    let client = state
        .database
        .connection()
        .await
        .map_err(|error| map_database(error, request_id))?;
    client
        .execute("SELECT pg_advisory_lock(hashtext($1))", &[&lock_name])
        .await
        .map_err(|error| ApiError::database(error.into(), request_id))?;
    let current = state
        .database
        .one_json(
            "SELECT id,hirez_profile_refreshed_at FROM players WHERE id=$1",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, request_id))?;
    let freshness = current
        .as_ref()
        .map(|row| profile_freshness(row, PROFILE_TTL_SECONDS))
        .unwrap_or_else(|| profile_freshness(&Value::Null, PROFILE_TTL_SECONDS));
    let result = if !force && current.is_some() && !freshness.expired {
        Ok((false, freshness, None))
    } else {
        let (_, audit) =
            fetch_profile(state, player_id, consumer, reason, source, request_id).await?;
        let refreshed = state
            .database
            .one_json(
                "SELECT id,hirez_profile_refreshed_at FROM players WHERE id=$1",
                &[&player_id],
            )
            .await
            .map_err(|error| map_database(error, request_id))?
            .unwrap_or(Value::Null);
        Ok((
            true,
            profile_freshness(&refreshed, PROFILE_TTL_SECONDS),
            audit,
        ))
    };
    if let Err(error) = client
        .execute("SELECT pg_advisory_unlock(hashtext($1))", &[&lock_name])
        .await
    {
        tracing::warn!(%error, player_id, "failed to release player profile advisory lock");
    }
    result
}

pub(super) async fn global_stats(
    state: &PlayersState,
    player_id: i64,
    request_id: &RequestId,
) -> Result<Option<Value>, ApiError> {
    let row = state
        .database
        .one_json(
            "SELECT COALESCE(SUM(wins),0)::BIGINT AS wins,COALESCE(SUM(losses),0)::BIGINT AS losses, \
               COALESCE(SUM(kills),0)::BIGINT AS kills,COALESCE(SUM(deaths),0)::BIGINT AS deaths, \
               COALESCE(SUM(assists),0)::BIGINT AS assists \
             FROM player_champions WHERE player_id=$1::BIGINT AND stats_populated",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, request_id))?;
    Ok(row.filter(|row| {
        ["wins", "losses", "kills", "deaths", "assists"]
            .iter()
            .any(|field| super::value_i64(row.get(*field)) > 0)
    }))
}

async fn champion_freshness(
    state: &PlayersState,
    player_id: i64,
    ttl_seconds: i64,
    request_id: &RequestId,
) -> Result<ProfileFreshness, ApiError> {
    let row = state
        .database
        .one_json(
            "SELECT MAX(last_updated)::text AS hirez_profile_refreshed_at,COUNT(*)>0 AS stats_populated \
             FROM player_champions WHERE player_id=$1::BIGINT AND stats_populated",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, request_id))?
        .unwrap_or(Value::Null);
    if !row
        .get("stats_populated")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(profile_freshness(&Value::Null, ttl_seconds));
    }
    Ok(profile_freshness(&row, ttl_seconds))
}

async fn refresh_champion_rows(
    state: &PlayersState,
    player_id: i64,
    ttl_seconds: i64,
    consumer: &str,
    source: &str,
    request_id: &RequestId,
) -> Result<bool, ApiError> {
    let freshness = champion_freshness(state, player_id, ttl_seconds, request_id).await?;
    if !freshness.expired {
        return Ok(false);
    }
    let raw = relay(state, request_id)?
        .call_value("getChampionRanks", vec![json!(player_id)], consumer)
        .await
        .map_err(|error| {
            ApiError::coded(
                StatusCode::BAD_GATEWAY,
                "CHAMPION_STATS_REFRESH_FAILED",
                error.to_string(),
            )
        })?;
    record_raw_hirez_response(
        &state.database,
        RawHirezAudit {
            endpoint: "getchampionranks",
            operation: "getChampionRanks",
            entity_type: "player_champions",
            entity_id: player_id.to_string(),
            params: json!({"playerId":player_id}),
            raw_response: &raw,
            source,
        },
    )
    .await
    .map_err(|error| map_database(error, request_id))?;
    let mut written = 0usize;
    for row in value_rows(&raw) {
        if !value_text(row, &["ret_msg"]).is_empty() {
            continue;
        }
        let champion_id = value_i64(row, &["ChampionId", "champion_id"]);
        let row_player_id = value_i64(row, &["PlayerId", "player_id"]);
        if champion_id <= 0 || row_player_id != player_id {
            continue;
        }
        state
            .database
            .query_json(
                "INSERT INTO player_champions(player_id,champion_id,champion_name,xp,ownership_type,wins,losses,kills,deaths,assists,minutes_played,stats_populated,last_updated) \
                 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,true,now()) \
                 ON CONFLICT(player_id,champion_id) DO UPDATE SET champion_name=EXCLUDED.champion_name, \
                   xp=CASE WHEN EXCLUDED.xp>0 THEN EXCLUDED.xp ELSE player_champions.xp END, \
                   ownership_type=COALESCE(NULLIF(EXCLUDED.ownership_type,''),player_champions.ownership_type), \
                   wins=EXCLUDED.wins,losses=EXCLUDED.losses,kills=EXCLUDED.kills,deaths=EXCLUDED.deaths, \
                   assists=EXCLUDED.assists,minutes_played=EXCLUDED.minutes_played,stats_populated=true,last_updated=now()",
                &[
                    &i32::try_from(player_id).unwrap_or_default(),
                    &i32::try_from(champion_id).unwrap_or_default(),
                    &value_text(row, &["Champion", "champion"]),
                    &value_i64(row, &["XP", "Worshippers"]),
                    &value_text(row, &["OwnershipType"]),
                    &i32::try_from(value_i64(row, &["Wins"])).unwrap_or_default(),
                    &i32::try_from(value_i64(row, &["Losses"])).unwrap_or_default(),
                    &i32::try_from(value_i64(row, &["Kills"])).unwrap_or_default(),
                    &i32::try_from(value_i64(row, &["Deaths"])).unwrap_or_default(),
                    &i32::try_from(value_i64(row, &["Assists"])).unwrap_or_default(),
                    &i32::try_from(value_i64(row, &["Minutes"])).unwrap_or_default(),
                ],
            )
            .await
            .map_err(|error| map_database(error, request_id))?;
        written += 1;
    }
    Ok(written > 0)
}

fn loadout_freshness_from(row: Option<&Value>) -> Value {
    let base = row.cloned().unwrap_or(Value::Null);
    let fetched = base
        .get("fetched_at")
        .and_then(Value::as_str)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok());
    let manual = base
        .get("last_manual_refresh_at")
        .and_then(Value::as_str)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok());
    let now = OffsetDateTime::now_utc();
    let expires = fetched.map(|value| value + Duration::seconds(LOADOUT_TTL_SECONDS));
    let available = manual.map(|value| value + Duration::seconds(LOADOUT_COOLDOWN_SECONDS));
    let remaining = expires
        .map(|value| (value - now).whole_seconds().max(0))
        .unwrap_or(0);
    let manual_remaining = available
        .map(|value| (value - now).whole_seconds().max(0))
        .unwrap_or(0);
    json!({
        "ttl_seconds":LOADOUT_TTL_SECONDS,
        "refreshed_at":fetched.and_then(|value| value.format(&Rfc3339).ok()),
        "expires_at":expires.and_then(|value| value.format(&Rfc3339).ok()),
        "remaining_seconds":remaining,
        "expired":expires.is_none() || remaining==0,
        "manual_refresh_available_at":available.and_then(|value| value.format(&Rfc3339).ok()),
        "manual_refresh_remaining_seconds":manual_remaining
    })
}

pub(super) async fn loadout_freshness(
    state: &PlayersState,
    player_id: i64,
    request_id: &RequestId,
) -> Result<Value, ApiError> {
    let row = state
        .database
        .one_json(
            "SELECT player_id,fetched_at,last_manual_refresh_at FROM player_loadout_fetches WHERE player_id=$1",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, request_id))?;
    Ok(loadout_freshness_from(row.as_ref()))
}

type NormalizedLoadout = (i64, i64, String, String, Vec<i32>, Vec<i32>);

fn normalize_loadout(raw: &Value) -> Option<NormalizedLoadout> {
    let champion_id = value_i64(raw, &["ChampionId", "champion_id", "championId"]);
    if champion_id <= 0 {
        return None;
    }
    let deck_id = value_i64(raw, &["DeckId", "deck_id", "deckId"]);
    let supplied_name = value_text(raw, &["DeckName", "deck_name", "deckName"]);
    let cards = raw
        .get("LoadoutItems")
        .or_else(|| raw.get("loadout_items"))
        .and_then(Value::as_array)
        .map(|cards| {
            cards
                .iter()
                .filter_map(|card| {
                    let id = value_i64(card, &["ItemId", "item_id", "id"]);
                    (id > 0).then(|| {
                        (
                            i32::try_from(id).unwrap_or_default(),
                            i32::try_from(
                                value_i64(card, &["Points", "points", "level"]).clamp(0, 5),
                            )
                            .unwrap_or_default(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let card_ids = cards.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let levels = cards.iter().map(|(_, level)| *level).collect::<Vec<_>>();
    let normalized_name = if supplied_name.is_empty() {
        "Unnamed Loadout".to_owned()
    } else {
        supplied_name.clone()
    };
    let legacy_name = supplied_name
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
        .chars()
        .take(80)
        .collect::<String>()
        .trim()
        .to_owned();
    let key = if deck_id > 0 {
        format!("id:{deck_id}")
    } else {
        format!(
            "legacy:{champion_id}:{}:{}",
            if legacy_name.is_empty() {
                "unnamed"
            } else {
                legacy_name.as_str()
            },
            card_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("-")
        )
    };
    Some((champion_id, deck_id, key, normalized_name, card_ids, levels))
}

async fn refresh_loadout_rows(
    state: &PlayersState,
    player_id: i64,
    request_id: &RequestId,
) -> Result<Value, ApiError> {
    let before = loadout_freshness(state, player_id, request_id).await?;
    if before
        .get("manual_refresh_remaining_seconds")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        > 0
    {
        return Err(ApiError::coded(
            StatusCode::TOO_MANY_REQUESTS,
            "LOADOUT_REFRESH_COOLDOWN",
            "Loadouts were refreshed recently. Try again after the cooldown.",
        ));
    }
    state
        .database
        .query_json(
            "INSERT INTO player_loadout_fetches(player_id,fetched_at,last_manual_refresh_at) VALUES($1,to_timestamp(0),now()) \
             ON CONFLICT(player_id) DO UPDATE SET last_manual_refresh_at=now()",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, request_id))?;
    let raw = relay(state, request_id)?
        .call_value(
            "getPlayerLoadouts",
            vec![json!(player_id)],
            "manual_profile_refresh",
        )
        .await
        .map_err(|error| {
            ApiError::coded(
                StatusCode::BAD_GATEWAY,
                "LOADOUT_REFRESH_FAILED",
                error.to_string(),
            )
        })?;
    record_raw_hirez_response(
        &state.database,
        RawHirezAudit {
            endpoint: "getplayerloadouts",
            operation: "getPlayerLoadouts",
            entity_type: "player_loadout",
            entity_id: player_id.to_string(),
            params: json!({"playerId":player_id,"reason":"manual_loadout_refresh"}),
            raw_response: &raw,
            source: "player-loadout-manual-refresh",
        },
    )
    .await
    .map_err(|error| map_database(error, request_id))?;
    let known = state
        .database
        .query_json("SELECT id FROM champions WHERE id>0", &[])
        .await
        .map_err(|error| map_database(error, request_id))?
        .into_iter()
        .map(|row| super::value_i64(row.get("id")))
        .collect::<std::collections::HashSet<_>>();
    let decks = value_rows(&raw)
        .into_iter()
        .filter_map(normalize_loadout)
        .filter(|(champion_id, ..)| known.contains(champion_id))
        .collect::<Vec<_>>();
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| map_database(error, request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), request_id))?;
    let mut keys = Vec::new();
    for (champion_id, deck_id, key, name, card_ids, levels) in &decks {
        transaction
            .execute(
                "INSERT INTO player_loadouts(player_id,champion_id,deck_id,deck_key,loadout_name,card_ids,card_levels,talent_id,fetched_at,updated_at) \
                 VALUES($1,$2,$3,$4,$5,$6,$7,NULL,now(),now()) \
                 ON CONFLICT(player_id,deck_key) DO UPDATE SET champion_id=EXCLUDED.champion_id,deck_id=EXCLUDED.deck_id, \
                   loadout_name=EXCLUDED.loadout_name,card_ids=EXCLUDED.card_ids,card_levels=EXCLUDED.card_levels,fetched_at=now(),updated_at=now()",
                &[
                    &player_id,
                    &i32::try_from(*champion_id).unwrap_or_default(),
                    &(*deck_id > 0).then_some(*deck_id),
                    key,
                    name,
                    card_ids,
                    levels,
                ],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), request_id))?;
        keys.push(key.clone());
    }
    if keys.is_empty() {
        transaction
            .execute(
                "DELETE FROM player_loadouts WHERE player_id=$1",
                &[&player_id],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), request_id))?;
    } else {
        transaction
            .execute(
                "DELETE FROM player_loadouts WHERE player_id=$1 AND NOT(deck_key=ANY($2::text[]))",
                &[&player_id, &keys],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), request_id))?;
    }
    transaction
        .execute(
            "INSERT INTO player_loadout_fetches(player_id,fetched_at,last_manual_refresh_at) VALUES($1,now(),now()) \
             ON CONFLICT(player_id) DO UPDATE SET fetched_at=now(),last_manual_refresh_at=now()",
            &[&player_id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), request_id))?;
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), request_id))?;
    loadout_freshness(state, player_id, request_id).await
}

pub(super) async fn discord(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<ProviderQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let input = query.player.as_deref().unwrap_or_default().trim();
    if input.is_empty() || input.len() > 128 {
        return Err(ApiError::validation("Missing or invalid player name or ID"));
    }
    let lookup_key = input.to_lowercase();
    let numeric = input
        .chars()
        .all(|character| character.is_ascii_digit())
        .then(|| parse_js_integer(input))
        .flatten()
        .filter(|value| *value > 0);
    let local = if let Some(id) = numeric {
        state
            .database
            .one_json("SELECT id FROM players WHERE id=$1", &[&id])
            .await
    } else {
        state
            .database
            .one_json(
                "SELECT id FROM players WHERE lower(name)=lower($1) OR lower(COALESCE(hz_player_name,''))=lower($1) \
                 OR lower(COALESCE(hz_gamer_tag,''))=lower($1) ORDER BY id LIMIT 1",
                &[&input],
            )
            .await
    }
    .map_err(|error| map_database(error, &request_id))?;
    let mut resolved = local
        .as_ref()
        .map(|row| super::value_i64(row.get("id")))
        .unwrap_or_default();
    if resolved == 0
        && let Some(cached) = state
            .database
            .one_json(
                "SELECT player_id FROM discord_player_lookup_cache WHERE lookup_key=$1 AND expires_at>now()",
                &[&lookup_key],
            )
            .await
            .map_err(|error| map_database(error, &request_id))?
    {
        resolved = super::value_i64(cached.get("player_id"));
        if resolved == 0 {
            return Err(ApiError::not_found(
                "Player not found",
                json!({"player":input,"cached":true}),
            ));
        }
    }
    if resolved == 0 {
        if let Some(id) = numeric {
            resolved = id;
        } else {
            guard(
                &state,
                &headers,
                "discord-player-name",
                &lookup_key,
                120_000,
                8,
            )
            .await?;
            let remote = relay(&state, &request_id)?
                .call_value(
                    "getPlayerIdByName",
                    vec![json!(input)],
                    "discord_player_command",
                )
                .await
                .map_err(|error| {
                    ApiError::coded(StatusCode::BAD_GATEWAY, "LOOKUP_FAILED", error.to_string())
                })?;
            resolved = value_rows(&remote)
                .first()
                .map(|row| value_i64(row, &["player_id", "playerId", "Id", "id"]))
                .unwrap_or_default();
            let optional_id = optional_positive(resolved);
            state
                .database
                .query_json(
                    "INSERT INTO discord_player_lookup_cache(lookup_key,player_id,fetched_at,expires_at) VALUES($1,$2,now(),now()+interval '24 hours') \
                     ON CONFLICT(lookup_key) DO UPDATE SET player_id=EXCLUDED.player_id,fetched_at=now(),expires_at=EXCLUDED.expires_at",
                    &[&lookup_key, &optional_id],
                )
                .await
                .map_err(|error| map_database(error, &request_id))?;
            if resolved == 0 {
                return Err(ApiError::not_found(
                    "Player not found",
                    json!({"player":input}),
                ));
            }
        }
    }
    let current = state
        .database
        .one_json(
            "SELECT id,hirez_profile_refreshed_at FROM players WHERE id=$1",
            &[&resolved],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let stale = current
        .as_ref()
        .map(|row| profile_freshness(row, PROFILE_TTL_SECONDS).expired)
        .unwrap_or(true);
    if stale
        && guard(
            &state,
            &headers,
            "discord-player-profile",
            resolved,
            120_000,
            8,
        )
        .await
        .is_ok()
    {
        let _ = refresh_profile_record(
            &state,
            resolved,
            false,
            "discord_player_command",
            "discord_player_lookup",
            "discord-player-lookup",
            &request_id,
        )
        .await;
    }
    let player = state
        .database
        .one_json("SELECT p.* FROM players p WHERE p.id=$1", &[&resolved])
        .await
        .map_err(|error| map_database(error, &request_id))?
        .ok_or_else(|| ApiError::not_found("Player not found", json!({"player":input})))?;
    if champion_freshness(&state, resolved, DISCORD_CHAMPION_TTL_SECONDS, &request_id)
        .await?
        .expired
        && guard(
            &state,
            &headers,
            "discord-player-champions",
            resolved,
            120_000,
            8,
        )
        .await
        .is_ok()
    {
        let _ = refresh_champion_rows(
            &state,
            resolved,
            DISCORD_CHAMPION_TTL_SECONDS,
            "discord_player_command",
            "discord-player-champion-stats",
            &request_id,
        )
        .await;
    }
    let global = global_stats(&state, resolved, &request_id).await?;
    let wants_history = query.history.as_deref() == Some("true");
    let history = if wants_history {
        let cached = state
            .database
            .one_json(
                "SELECT 1 FROM player_match_history_cache WHERE player_id=$1 AND fetched_at>=now()-interval '15 minutes' AND expires_at>now()",
                &[&resolved],
            )
            .await
            .ok()
            .flatten()
            .is_some();
        if !cached {
            guard(
                &state,
                &headers,
                "discord-player-history",
                resolved,
                120_000,
                8,
            )
            .await?;
        }
        Some(
            relay(&state, &request_id)?
                .call_value(
                    "getMatchHistory",
                    vec![json!(resolved), json!(50), json!(false)],
                    "discord_player_command",
                )
                .await
                .map_err(|error| {
                    ApiError::coded(StatusCode::BAD_GATEWAY, "HISTORY_FAILED", error.to_string())
                })?,
        )
    } else {
        None
    };
    let mut payload = json!({
        "player":player,
        "globalStats":global,
        "profileRefresh":{
            "ttl_seconds":PROFILE_TTL_SECONDS,
            "refreshed_at":profile_freshness(&player,PROFILE_TTL_SECONDS).refreshed_at,
            "expires_at":profile_freshness(&player,PROFILE_TTL_SECONDS).expires_at,
            "remaining_seconds":profile_freshness(&player,PROFILE_TTL_SECONDS).remaining_seconds,
            "expired":profile_freshness(&player,PROFILE_TTL_SECONDS).expired,
            "source":"database-or-hirez"
        }
    });
    if let Some(history) = history {
        payload["history"] = history;
    }
    Ok(json_response(payload))
}

pub(super) async fn saved_player(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<ProviderQuery>,
) -> Result<Response, ApiError> {
    let discord_id = query.discord_user_id.as_deref().unwrap_or_default().trim();
    if discord_id.is_empty()
        || discord_id.len() > 32
        || !discord_id.chars().all(|value| value.is_ascii_digit())
    {
        return Err(ApiError::validation("Missing or invalid Discord user ID"));
    }
    let saved = state
        .database
        .one_json(
            &format!(
                "SELECT p.id::text AS id,{DISPLAY_NAME_SQL} AS name FROM discord_saved_players dsp \
                 JOIN players p ON p.id=dsp.player_id WHERE dsp.discord_user_id=$1"
            ),
            &[&discord_id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?
        .ok_or_else(|| {
            ApiError::coded(
                StatusCode::NOT_FOUND,
                "NO_SAVED_PLAYER",
                "No saved player is linked to this Discord account",
            )
        })?;
    let mut response = json_response(json!({"player":saved}));
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("private, no-store"));
    Ok(response)
}

pub(super) async fn save_player(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Json(body): Json<SavedPlayerBody>,
) -> Result<Response, ApiError> {
    let discord_id = body.discord_user_id.as_deref().unwrap_or_default().trim();
    if discord_id.is_empty()
        || discord_id.len() > 32
        || !discord_id.chars().all(|value| value.is_ascii_digit())
    {
        return Err(ApiError::validation("Missing or invalid Discord user ID"));
    }
    let raw_id = body
        .player_id
        .as_ref()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string())
        })
        .unwrap_or_default();
    let id = parse_js_integer(&raw_id)
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation("Missing or invalid Paladins player ID"))?;
    let player = state
        .database
        .one_json(
            &format!(
                "SELECT p.id::text AS id,{DISPLAY_NAME_SQL} AS name FROM players p WHERE p.id=$1"
            ),
            &[&id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?
        .ok_or_else(|| ApiError::not_found("Player not found", json!({"playerId":raw_id})))?;
    state
        .database
        .query_json(
            "INSERT INTO discord_saved_players(discord_user_id,player_id,saved_at,updated_at) VALUES($1,$2,now(),now()) \
             ON CONFLICT(discord_user_id) DO UPDATE SET player_id=EXCLUDED.player_id,updated_at=now()",
            &[&discord_id, &id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let mut response = json_response(json!({"player":player}));
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("private, no-store"));
    Ok(response)
}

pub(super) async fn raw_profile(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<ProviderQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let id = query
        .player_id
        .as_deref()
        .or(query.id.as_deref())
        .and_then(parse_js_integer)
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation("Missing or invalid query param: playerId"))?;
    guard(&state, &headers, "raw-player-profile", id, 120_000, 8).await?;
    let raw = relay(&state, &request_id)?
        .call_value("getPlayerBatch", vec![json!([id])], "operator_raw_audit")
        .await
        .map_err(|error| ApiError::coded(StatusCode::BAD_GATEWAY, "UPSTREAM", error.to_string()))?;
    let audit = record_raw_hirez_response(
        &state.database,
        RawHirezAudit {
            endpoint: "getplayerbatch",
            operation: "getPlayerBatch",
            entity_type: "player",
            entity_id: id.to_string(),
            params: json!({"playerIds":[id]}),
            raw_response: &raw,
            source: "operator-raw-audit",
        },
    )
    .await
    .map_err(|error| map_database(error, &request_id))?;
    Ok(json_response(json!({
        "endpoint":"getplayerbatch","player_id":id,
        "count":raw.as_array().map(Vec::len),"audit":audit,"data":raw
    })))
}

pub(super) async fn raw_loadouts(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<ProviderQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let id = query
        .player_id
        .as_deref()
        .or(query.id.as_deref())
        .and_then(parse_js_integer)
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation("Missing or invalid query param: playerId"))?;
    guard(&state, &headers, "raw-player-loadouts", id, 120_000, 8).await?;
    let raw = relay(&state, &request_id)?
        .call_value(
            "getPlayerLoadouts",
            vec![json!(id)],
            "manual_profile_refresh",
        )
        .await
        .map_err(|error| ApiError::coded(StatusCode::BAD_GATEWAY, "UPSTREAM", error.to_string()))?;
    let audit = record_raw_hirez_response(
        &state.database,
        RawHirezAudit {
            endpoint: "getplayerloadouts",
            operation: "getPlayerLoadouts",
            entity_type: "player_loadout",
            entity_id: id.to_string(),
            params: json!({"playerId":id}),
            raw_response: &raw,
            source: "operator-raw-audit",
        },
    )
    .await
    .map_err(|error| map_database(error, &request_id))?;
    Ok(json_response(json!({
        "endpoint":"getplayerloadouts","player_id":id,
        "count":raw.as_array().map(Vec::len),"audit":audit,"data":raw
    })))
}

pub(super) async fn refresh_profile(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let id = player_id(&id)?;
    let identity = request_identity(&headers);
    let quota = state
        .redis
        .check_rate_limit(
            &format!("player-refresh:{identity}:{id}"),
            5,
            10 * 60 * 1000,
            false,
        )
        .await;
    if !quota.backend_available || !quota.allowed {
        return Err(ApiError::request_security(
            StatusCode::TOO_MANY_REQUESTS,
            "RATE_LIMITED",
            "Too many player refresh attempts.",
            quota
                .reset_at_ms
                .saturating_sub(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|value| value.as_millis() as u64)
                        .unwrap_or_default(),
                )
                .div_ceil(1000),
        ));
    }
    guard(&state, &headers, "player-profile-refresh", id, 120_000, 5).await?;
    let (refreshed, freshness, audit) = refresh_profile_record(
        &state,
        id,
        true,
        "manual_profile_refresh",
        "manual_profile_refresh",
        "player-profile-manual-refresh",
        &request_id,
    )
    .await?;
    let history = match guard(
        &state,
        &headers,
        "player-history-refresh",
        id,
        120_000,
        5,
    )
    .await
    {
        Ok(()) => relay(&state, &request_id)?
            .call_value(
                "getMatchHistory",
                vec![json!(id), json!(50), json!(true)],
                "manual_profile_refresh",
            )
            .await
            .map(|value| json!({"refreshed":true,"count":value.as_array().map(Vec::len).unwrap_or(0)}))
            .unwrap_or_else(|error| json!({"refreshed":false,"error":error.to_string()})),
        Err(error) => json!({"refreshed":false,"error":format!("{error:?}")}),
    };
    let champions = match guard(&state, &headers, "player-champions-refresh", id, 120_000, 5).await
    {
        Ok(()) => refresh_champion_rows(
            &state,
            id,
            CHAMPION_TTL_SECONDS,
            "manual_profile_refresh",
            "player-champion-stats-manual-refresh",
            &request_id,
        )
        .await
        .map(|refreshed| json!({"refreshed":refreshed}))
        .unwrap_or_else(|error| json!({"refreshed":false,"error":format!("{error:?}")})),
        Err(error) => json!({"refreshed":false,"error":format!("{error:?}")}),
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or_default();
    Ok(json_response(json!({
        "success":true,
        "message":if refreshed {"Player data refreshed"} else {"Player data is already current"},
        "profileRefresh":{
            "ttl_seconds":freshness.ttl_seconds,"refreshed_at":freshness.refreshed_at,
            "expires_at":freshness.expires_at,"remaining_seconds":freshness.remaining_seconds,
            "expired":freshness.expired,"attempted":true,"refreshed":refreshed,
            "source":if refreshed {"hirez"} else {"database"}
        },
        "historyRefresh":history,
        "championStatsRefresh":champions,
        "refreshQuota":{"limit":quota.total,"remaining":quota.remaining,
            "reset_at":OffsetDateTime::from_unix_timestamp_nanos(i128::from(quota.reset_at_ms)*1_000_000).ok().and_then(|value|value.format(&Rfc3339).ok()),
            "remaining_seconds":quota.reset_at_ms.saturating_sub(now_ms).div_ceil(1000)},
        "audit":audit
    })))
}

pub(super) async fn refresh_champions(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let id = player_id(&id)?;
    if state
        .database
        .one_json("SELECT 1 FROM players WHERE id=$1", &[&id])
        .await
        .map_err(|error| map_database(error, &request_id))?
        .is_none()
    {
        return Err(ApiError::not_found_without_details("Player not found"));
    }
    let before = champion_freshness(&state, id, CHAMPION_TTL_SECONDS, &request_id).await?;
    if !before.expired {
        return Err(ApiError::coded(
            StatusCode::TOO_MANY_REQUESTS,
            "CHAMPION_STATS_REFRESH_COOLDOWN",
            "Champion stats were refreshed recently. Try again after the cooldown.",
        ));
    }
    guard(&state, &headers, "player-champions-refresh", id, 120_000, 8).await?;
    let refreshed = refresh_champion_rows(
        &state,
        id,
        CHAMPION_TTL_SECONDS,
        "manual_profile_refresh",
        "player-champion-stats-manual-refresh",
        &request_id,
    )
    .await?;
    let freshness = champion_freshness(&state, id, CHAMPION_TTL_SECONDS, &request_id).await?;
    Ok(json_response(
        json!({"refreshed":refreshed,"freshness":freshness.json()}),
    ))
}

pub(super) async fn refresh_loadouts(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let id = player_id(&id)?;
    guard(&state, &headers, "player-loadouts-refresh", id, 120_000, 8).await?;
    match refresh_loadout_rows(&state, id, &request_id).await {
        Ok(freshness) => {
            let loadouts = loadout_rows(&state, id, &request_id).await?;
            Ok(json_response(json!({
                "loadouts":loadouts,"freshness":freshness,"refreshed":true
            })))
        }
        Err(error) => {
            let loadouts = loadout_rows(&state, id, &request_id).await?;
            let freshness = loadout_freshness(&state, id, &request_id).await?;
            Ok(json_response(json!({
                "loadouts":loadouts,"freshness":freshness,"refreshed":false,
                "refresh_error":format!("{error:?}")
            })))
        }
    }
}
