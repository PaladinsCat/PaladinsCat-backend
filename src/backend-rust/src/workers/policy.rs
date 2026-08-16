use std::collections::{BTreeSet, HashSet};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use paladinscat_core::database::{Database, DatabaseError};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::Date;

pub const RANKED_STATS_QUEUE_ID: i32 = 486;
pub const ACTIVITY_PROFILE_TTL_HOURS: i32 = 24;
pub const ACTIVITY_PROFILE_BATCH_SIZE: usize = 20;
pub const LIMITED_MATCH_REASON_ROSTER_UNAVAILABLE: &str = "roster_anchor_unavailable";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchStatScope {
    Ranked,
    Casual,
    Bot,
    TeamDeathmatch,
    Arcade,
    WaveDefense,
    Experiment,
    Newcomer,
    Custom,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchParticipantModel {
    Pvp,
    Pve,
    Bots,
    Custom,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MatchCountQueueDefinition {
    pub queue_id: i32,
    pub name: &'static str,
    pub ranked: bool,
    pub scope: MatchStatScope,
    pub participant_model: MatchParticipantModel,
    pub stats_enabled: bool,
    pub track_presence: bool,
}

const fn queue(
    queue_id: i32,
    name: &'static str,
    ranked: bool,
    scope: MatchStatScope,
    participant_model: MatchParticipantModel,
) -> MatchCountQueueDefinition {
    MatchCountQueueDefinition {
        queue_id,
        name,
        ranked,
        scope,
        participant_model,
        stats_enabled: true,
        track_presence: true,
    }
}

pub const MATCH_COUNT_QUEUE_DEFINITIONS: &[MatchCountQueueDefinition] = &[
    queue(
        424,
        "Casual Siege",
        false,
        MatchStatScope::Casual,
        MatchParticipantModel::Pvp,
    ),
    queue(
        425,
        "Siege Training",
        false,
        MatchStatScope::Bot,
        MatchParticipantModel::Bots,
    ),
    queue(
        452,
        "Casual Onslaught",
        false,
        MatchStatScope::Casual,
        MatchParticipantModel::Pvp,
    ),
    queue(
        453,
        "Onslaught Training",
        false,
        MatchStatScope::Bot,
        MatchParticipantModel::Bots,
    ),
    queue(
        469,
        "Team Deathmatch",
        false,
        MatchStatScope::TeamDeathmatch,
        MatchParticipantModel::Pvp,
    ),
    queue(
        486,
        "Ranked Siege",
        true,
        MatchStatScope::Ranked,
        MatchParticipantModel::Pvp,
    ),
    queue(
        10297,
        "Team Deathmatch Training",
        false,
        MatchStatScope::Bot,
        MatchParticipantModel::Bots,
    ),
    queue(
        10332,
        "Arcade",
        false,
        MatchStatScope::Arcade,
        MatchParticipantModel::Pvp,
    ),
    queue(
        10348,
        "Wave Defense Party Beta",
        false,
        MatchStatScope::WaveDefense,
        MatchParticipantModel::Pve,
    ),
    queue(
        10362,
        "Wave Defense Public Beta",
        false,
        MatchStatScope::WaveDefense,
        MatchParticipantModel::Pve,
    ),
    queue(
        10367,
        "Newcomer",
        false,
        MatchStatScope::Newcomer,
        MatchParticipantModel::Pvp,
    ),
    queue(
        10369,
        "Experiment: Subclasses",
        false,
        MatchStatScope::Experiment,
        MatchParticipantModel::Pvp,
    ),
];

pub fn get_match_queue_definition(queue_id: i32) -> MatchCountQueueDefinition {
    MATCH_COUNT_QUEUE_DEFINITIONS
        .iter()
        .find(|definition| definition.queue_id == queue_id)
        .copied()
        .unwrap_or(MatchCountQueueDefinition {
            queue_id,
            name: if queue_id > 0 {
                "Unknown queue"
            } else {
                "Unknown"
            },
            ranked: false,
            scope: MatchStatScope::Other,
            participant_model: MatchParticipantModel::Unknown,
            stats_enabled: false,
            track_presence: false,
        })
}

pub fn is_public_stats_scope(scope: MatchStatScope) -> bool {
    !matches!(scope, MatchStatScope::Custom | MatchStatScope::Other)
}

pub fn is_ranked_stats_queue(queue_id: i64) -> bool {
    queue_id == i64::from(RANKED_STATS_QUEUE_ID)
}

pub fn champion_page_slug(name: &str) -> String {
    name.chars()
        .filter(|value| value.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

pub fn champion_page_warm_urls(rows: &[(String, Option<i32>)]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    for (name, talent_id) in rows {
        let slug = champion_page_slug(name);
        if slug.is_empty() {
            continue;
        }
        let champion = format!("/champions/{slug}/page-data");
        if seen.insert(champion.clone()) {
            urls.push(champion);
        }
        if let Some(talent_id) = talent_id.filter(|id| *id > 0) {
            let talent = format!("/champions/{slug}/talents/{talent_id}/page-data");
            if seen.insert(talent.clone()) {
                urls.push(talent);
            }
        }
    }
    urls
}

#[derive(Clone, Debug)]
pub struct LimitedMatchCandidate {
    pub player_count: i32,
    pub team_one_count: i32,
    pub team_two_count: i32,
    pub all_rows_authoritative: bool,
    pub recovery_source: String,
    pub recovery_terminal: bool,
    pub recovery_api_calls: i32,
    pub anchor_player_count: i32,
}

pub fn limited_match_reason(candidate: &LimitedMatchCandidate) -> Option<&'static str> {
    let terminal = candidate.recovery_terminal
        || matches!(
            candidate.recovery_source.to_ascii_lowercase().as_str(),
            "no_player_anchors" | "getplayerbatchfrommatch_failed"
        )
        || candidate.anchor_player_count == 0;
    (candidate.player_count >= 1
        && candidate.player_count < 10
        && candidate.all_rows_authoritative
        && (1..=5).contains(&candidate.team_one_count)
        && (1..=5).contains(&candidate.team_two_count)
        && candidate.team_one_count + candidate.team_two_count == candidate.player_count
        && terminal
        && candidate.recovery_api_calls == 1)
        .then_some(LIMITED_MATCH_REASON_ROSTER_UNAVAILABLE)
}

pub fn unique_player_ids(values: &[Value]) -> Vec<i64> {
    let mut unique = BTreeSet::new();
    for value in values {
        let id = value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|id| i64::try_from(id).ok()))
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()));
        if let Some(id) = id.filter(|id| *id > 0 && *id <= 9_007_199_254_740_991) {
            unique.insert(id);
        }
    }
    unique.into_iter().collect()
}

pub fn chunk_activity_profile_ids(values: &[Value], batch_size: usize) -> Vec<Vec<i64>> {
    let size = batch_size.clamp(1, ACTIVITY_PROFILE_BATCH_SIZE);
    unique_player_ids(values)
        .chunks(size)
        .map(<[i64]>::to_vec)
        .collect()
}

pub fn requested_ids_satisfied_by_profiles(
    requested_ids: &[i64],
    profiles: &[Value],
) -> HashSet<i64> {
    let requested = requested_ids.iter().copied().collect::<HashSet<_>>();
    let mut satisfied = HashSet::new();
    for profile in profiles {
        if profile
            .get("ret_msg")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
        {
            continue;
        }
        for key in [
            "Id",
            "id",
            "player_id",
            "ActivePlayerId",
            "active_player_id",
        ] {
            if let Some(id) = profile
                .get(key)
                .and_then(|value| {
                    value
                        .as_i64()
                        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
                })
                .filter(|id| requested.contains(id))
            {
                satisfied.insert(id);
            }
        }
    }
    satisfied
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceDetailCursor {
    pub date: String,
    pub hour: i32,
    pub match_id: String,
    pub queue_id: i32,
}

pub fn parse_presence_detail_queue_id(value: Option<&str>) -> Option<i32> {
    match value {
        None | Some("") => None,
        Some(value) => value.parse::<i32>().ok().filter(|id| *id >= 0),
    }
}

pub fn parse_presence_detail_limit(value: Option<&str>) -> usize {
    bounded_number(value, 25, 10, 50)
}

pub fn parse_presence_evidence_limit(value: Option<&str>) -> usize {
    bounded_number(value, 250, 50, 500)
}

pub fn parse_presence_evidence_page(value: Option<&str>) -> usize {
    bounded_number(value, 1, 1, 1_000_000)
}

fn bounded_number(value: Option<&str>, fallback: usize, min: usize, max: usize) -> usize {
    value
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .map(|value| (value.trunc() as isize).clamp(min as isize, max as isize) as usize)
        .unwrap_or(fallback)
}

pub fn decode_presence_detail_cursor(value: Option<&str>) -> Option<PresenceDetailCursor> {
    let decoded = URL_SAFE_NO_PAD
        .decode(value.filter(|value| !value.is_empty())?)
        .ok()?;
    let cursor = serde_json::from_slice::<PresenceDetailCursor>(&decoded).ok()?;
    let format = time::macros::format_description!("[year]-[month]-[day]");
    Date::parse(&cursor.date, format).ok()?;
    if !(0..=23).contains(&cursor.hour)
        || cursor.queue_id < 0
        || !Regex::new(r"^\d+$").ok()?.is_match(&cursor.match_id)
        || cursor.match_id.parse::<i64>().ok()? < 0
    {
        return None;
    }
    Some(cursor)
}

pub fn encode_presence_detail_cursor(cursor: &PresenceDetailCursor) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(cursor).unwrap_or_default())
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiHeadroomSnapshot {
    pub total_keys: u64,
    pub usable_keys: u64,
    pub total_usable_before_reserve: u64,
    pub has_usable_keys: bool,
}

pub async fn api_headroom_snapshot(
    database: &Database,
    reserve_per_key: i32,
) -> Result<ApiHeadroomSnapshot, DatabaseError> {
    let row = database.one_json(
        "SELECT COUNT(*) AS total_keys,\
         COUNT(*) FILTER(WHERE status NOT IN('limited','unhealthy','exhausted') AND GREATEST(daily_limit-total_24h,0)>$1) AS usable_keys,\
         COALESCE(SUM(GREATEST(daily_limit-total_24h-$1,0)),0) AS total_usable_before_reserve FROM api_keys",
        &[&reserve_per_key],
    ).await?.unwrap_or(Value::Null);
    let total_keys = value_u64(row.get("total_keys"));
    let usable_keys = value_u64(row.get("usable_keys"));
    Ok(ApiHeadroomSnapshot {
        total_keys,
        usable_keys,
        total_usable_before_reserve: value_u64(row.get("total_usable_before_reserve")),
        has_usable_keys: total_keys == 0 || usable_keys > 0,
    })
}

fn value_u64(value: Option<&Value>) -> u64 {
    value
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default()
}
