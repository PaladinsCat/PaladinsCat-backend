use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletedMatchRequest {
    pub match_id: u64,
    #[serde(default)]
    pub queue_id: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletedMatchResolutionStatus {
    CompleteDirect,
    CompleteRecovered,
    RecoveryPending,
    Limited,
    RosterOnly,
    Dropped,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletedMatchResolution {
    pub match_id: u64,
    pub queue_id: u32,
    pub status: CompletedMatchResolutionStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#match: Option<MatchDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roster: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MatchDetails {
    pub match_id: u64,
    #[serde(default)]
    pub entry_datetime: String,
    #[serde(default)]
    pub map: String,
    #[serde(default)]
    pub queue_id: u32,
    #[serde(default)]
    pub duration_seconds: u32,
    #[serde(default)]
    pub minutes: u32,
    #[serde(default = "unknown_region")]
    pub region: String,
    #[serde(default)]
    pub team1_score: Option<i32>,
    #[serde(default)]
    pub team2_score: Option<i32>,
    #[serde(default)]
    pub winning_task_force: Option<i32>,
    #[serde(default)]
    pub direct_score_observations: Option<Vec<Value>>,
    #[serde(default)]
    pub has_replay: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_api_calls: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_attempted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_terminal: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_pending: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limited: Option<bool>,
    #[serde(default)]
    pub players: Vec<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

fn unknown_region() -> String {
    "Unknown".to_owned()
}

pub fn player_number(player: &Value, keys: &[&str]) -> u64 {
    keys.iter()
        .find_map(|key| {
            let value = player.get(*key)?;
            value
                .as_u64()
                .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
                .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
        })
        .unwrap_or_default()
}

pub fn player_string<'a>(player: &'a Value, keys: &[&str]) -> &'a str {
    keys.iter()
        .find_map(|key| player.get(*key).and_then(Value::as_str))
        .unwrap_or_default()
}

pub fn player_has_error(player: &Value) -> bool {
    player.get("has_ret_msg").and_then(Value::as_bool) == Some(true)
        || !player_string(player, &["ret_msg"]).trim().is_empty()
}

pub fn usable_players(players: &[Value]) -> Vec<Value> {
    players
        .iter()
        .filter(|player| !player_has_error(player))
        .cloned()
        .collect()
}

pub fn usable_player_count(players: &[Value]) -> usize {
    players
        .iter()
        .filter(|player| !player_has_error(player))
        .count()
}

pub fn sort_players(players: &mut [Value]) {
    players.sort_by(|left, right| {
        let left_team = player_number(left, &["task_force", "team_id"]);
        let right_team = player_number(right, &["task_force", "team_id"]);
        left_team
            .cmp(&right_team)
            .then_with(|| {
                let left_id = player_number(left, &["player_id"]);
                let right_id = player_number(right, &["player_id"]);
                match (left_id > 0, right_id > 0) {
                    (true, false) => std::cmp::Ordering::Less,
                    (false, true) => std::cmp::Ordering::Greater,
                    _ => left_id.cmp(&right_id),
                }
            })
            .then_with(|| {
                player_string(left, &["player_name"]).cmp(player_string(right, &["player_name"]))
            })
            .then_with(|| {
                player_number(left, &["champion_id"]).cmp(&player_number(right, &["champion_id"]))
            })
    });
}

pub fn sanitize_consumer(value: &str) -> String {
    let mut normalized = String::new();
    let mut replacing = false;
    for character in value.trim().to_ascii_lowercase().chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
            normalized.push(character);
            replacing = false;
        } else if !replacing {
            normalized.push('_');
            replacing = true;
        }
    }
    let bounded: String = normalized.trim_matches('_').chars().take(80).collect();
    if bounded.is_empty() {
        "unattributed".to_owned()
    } else {
        bounded
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;

    #[test]
    fn consumer_matches_typescript_sanitizer() {
        assert_eq!(
            sanitize_consumer("  Presence Discovery / Hourly  "),
            "presence_discovery_hourly"
        );
        assert_eq!(sanitize_consumer(""), "unattributed");
    }
}
