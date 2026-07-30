use std::collections::HashMap;

use serde_json::{Map, Number, Value};
use time::{OffsetDateTime, PrimitiveDateTime, macros::format_description};

use crate::{
    model::MatchDetails,
    profile_store::{MergedPlayer, PlayerProfile, RankedQueue},
};

pub fn normalize_region(raw: &str) -> &str {
    match raw {
        "North America" => "NA",
        "Europe" => "EU",
        "Brazil" => "BR",
        "Australia" => "OCE",
        "Southeast Asia" => "SEA",
        "Japan" => "JPN",
        "Russia" => "RUS",
        "NA" | "EU" | "BR" | "OCE" | "SEA" | "JPN" | "RUS" | "SA" => raw,
        _ => "Unknown",
    }
}

pub fn normalize_flat_match_detail_rows(data: &[Value]) -> Vec<MatchDetails> {
    let mut groups: Vec<Vec<Value>> = Vec::new();
    let mut indexes = HashMap::<String, usize>::new();
    for row in data {
        let match_id = row
            .get("match_id")
            .or_else(|| row.get("Match"))
            .map(js_string)
            .unwrap_or_default();
        if match_id.is_empty() {
            continue;
        }
        let index = match indexes.get(&match_id) {
            Some(index) => *index,
            None => {
                let index = groups.len();
                indexes.insert(match_id, index);
                groups.push(Vec::new());
                index
            }
        };
        groups[index].push(row.clone());
    }
    groups
        .iter()
        .filter_map(|rows| normalize_match_details(rows))
        .collect()
}

pub fn normalize_match_player(raw: &Value) -> Value {
    let mut output = Map::new();
    macro_rules! raw_field {
        ($name:literal, [$($key:literal),+], $default:expr) => {
            output.insert(
                $name.to_owned(),
                first_truthy(raw, &[$($key),+]).unwrap_or_else(|| $default),
            );
        };
    }
    macro_rules! number_field {
        ($name:literal, [$($key:literal),+]) => {
            output.insert(
                $name.to_owned(),
                first_number(raw, &[$($key),+]).unwrap_or_else(zero),
            );
        };
    }

    number_field!("player_id", ["playerId", "PlayerId", "player_id"]);
    raw_field!(
        "player_name",
        ["playerName", "Player_Name", "player_name"],
        Value::String("PRIVATEACCOUNT".to_owned())
    );
    raw_field!("match_id", ["Match", "match_id"], zero());
    raw_field!(
        "entry_datetime",
        ["Entry_Datetime", "entry_datetime"],
        empty_string()
    );
    raw_field!("queue_id", ["match_queue_id", "Match_Queue_Id"], zero());
    let region = first_truthy(raw, &["Region", "region"])
        .map(|value| js_string(&value))
        .unwrap_or_default();
    output.insert(
        "region".to_owned(),
        Value::String(normalize_region(&region).to_owned()),
    );

    raw_field!("champion_id", ["ChampionId", "champion_id"], zero());
    raw_field!(
        "champion_name",
        ["Champion", "ChampionName", "champion_name"],
        empty_string()
    );
    raw_field!("skin_id", ["SkinId", "skin_id"], zero());
    raw_field!("skin_name", ["Skin", "skin_name"], empty_string());
    raw_field!("kills", ["Kills_Player", "kills"], zero());
    raw_field!("deaths", ["Deaths", "deaths"], zero());
    raw_field!("assists", ["Assists", "assists"], zero());
    raw_field!(
        "damage_done_in_hand",
        ["Damage_Done_In_Hand", "damage_done_in_hand"],
        zero()
    );
    raw_field!(
        "damage_done_physical",
        [
            "Damage_Player",
            "damage_done_physical",
            "Damage_Done_Physical"
        ],
        zero()
    );
    raw_field!(
        "damage_done_magical",
        ["Damage_Done_Magical", "damage_done_magical"],
        zero()
    );
    raw_field!("damage_taken", ["Damage_Taken", "damage_taken"], zero());
    raw_field!(
        "damage_mitigated",
        ["Damage_Mitigated", "damage_mitigated"],
        zero()
    );
    raw_field!("healing", ["Healing", "healing"], zero());
    raw_field!(
        "healing_self",
        ["Healing_Player_Self", "healing_self"],
        zero()
    );
    raw_field!("gold_earned", ["Gold_Earned", "gold_earned"], zero());
    let time_in_match = first_f64_dynamic(
        raw,
        &["Time_In_Match_Seconds", "Time_In_Match", "time_in_match"],
    )
    .unwrap_or_default();
    let gold = first_f64_dynamic(raw, &["Gold_Earned", "gold_earned"]).unwrap_or_default();
    output.insert(
        "gold_per_minute".to_owned(),
        number_value(if time_in_match > 0.0 {
            round_to_2(gold / (time_in_match / 60.0))
        } else {
            0.0
        }),
    );
    raw_field!(
        "objective_assists",
        ["Objective_Assists", "objective_assists"],
        zero()
    );
    raw_field!("killing_spree", ["Killing_Spree", "killing_spree"], zero());
    raw_field!(
        "multi_kill_max",
        ["Multi_kill_Max", "multi_kill_max"],
        zero()
    );
    raw_field!("win_status", ["Win_Status", "win_status"], empty_string());
    raw_field!("task_force", ["TaskForce", "task_force"], zero());
    number_field!("league_tier", ["League_Tier", "league_tier"]);
    number_field!("league_points", ["League_Points", "league_points"]);
    number_field!("account_level", ["Account_Level", "account_level"]);
    number_field!("mastery_level", ["Mastery_Level", "mastery_level"]);
    number_field!("party_id", ["PartyId", "party_id"]);
    raw_field!(
        "time_in_match",
        ["Time_In_Match_Seconds", "time_in_match"],
        zero()
    );
    raw_field!(
        "distance_traveled",
        ["Distance_Traveled", "distance_traveled"],
        zero()
    );
    raw_field!(
        "structure_damage",
        ["Structure_Damage", "structure_damage"],
        zero()
    );
    raw_field!("camps_cleared", ["Camps_Cleared", "camps_cleared"], zero());
    let source = first_truthy(raw, &["source"])
        .map(|value| js_string(&value).to_ascii_lowercase())
        .filter(|source| matches!(source.as_str(), "direct" | "recovered" | "minimal"))
        .unwrap_or_else(|| "direct".to_owned());
    output.insert("source".to_owned(), Value::String(source));
    number_field!("portal_id", ["playerPortalId"]);
    raw_field!("portal_user_id", ["playerPortalUserId"], empty_string());
    raw_field!("kills_player", ["Kills_Player", "kills_player"], zero());
    raw_field!(
        "healing_player_self",
        ["Healing_Player_Self", "healing_player_self"],
        zero()
    );
    raw_field!(
        "damage_taken_physical",
        ["Damage_Taken_Physical", "damage_taken_physical"],
        zero()
    );
    raw_field!(
        "damage_taken_magical",
        ["Damage_Taken_Magical", "damage_taken_magical"],
        zero()
    );
    for (name, keys) in [
        ("kills_fire_giant", ["Kills_Fire_Giant", "kills_fire_giant"]),
        ("kills_gold_fury", ["Kills_Gold_Fury", "kills_gold_fury"]),
        ("kills_phoenix", ["Kills_Phoenix", "kills_phoenix"]),
        (
            "kills_siege_jugg",
            ["Kills_Siege_Juggernaut", "kills_siege_jugg"],
        ),
        (
            "kills_wild_jugg",
            ["Kills_Wild_Juggernaut", "kills_wild_jugg"],
        ),
        ("kills_bot", ["Kills_Bot", "kills_bot"]),
        ("kills_single", ["Kills_Single", "kills_single"]),
        ("kills_double", ["Kills_Double", "kills_double"]),
        ("kills_triple", ["Kills_Triple", "kills_triple"]),
        ("kills_quadra", ["Kills_Quadra", "kills_quadra"]),
        ("kills_penta", ["Kills_Penta", "kills_penta"]),
        (
            "kills_first_blood",
            ["Kills_First_Blood", "kills_first_blood"],
        ),
        ("towers_destroyed", ["Towers_Destroyed", "towers_destroyed"]),
        ("league_wins", ["League_Wins", "league_wins"]),
        ("league_losses", ["League_Losses", "league_losses"]),
    ] {
        output.insert(
            name.to_owned(),
            first_number(raw, &keys).unwrap_or_else(zero),
        );
    }
    raw_field!("wards_placed", ["Wards_Placed", "wards_placed"], zero());
    raw_field!("healing_bot", ["Healing_Bot", "healing_bot"], zero());
    raw_field!("damage_bot", ["Damage_Bot", "damage_bot"], zero());
    raw_field!("platform", ["Platform", "platform"], empty_string());
    raw_field!("surrendered", ["Surrendered", "surrendered"], zero());
    number_field!("team_id", ["TeamId", "team_id"]);
    raw_field!("team_name", ["Team_Name", "team_name"], empty_string());
    number_field!("rank_stat_league", ["Rank_Stat_League", "rank_stat_league"]);
    number_field!(
        "final_match_level",
        ["Final_Match_Level", "final_match_level"]
    );
    number_field!("match_duration", ["Match_Duration", "match_duration"]);
    for index in 1..=4 {
        let active_id = format!("ActiveId{index}");
        let normalized_active_id = format!("active_id_{index}");
        output.insert(
            normalized_active_id.clone(),
            first_truthy_dynamic(raw, &[&active_id, &normalized_active_id]).unwrap_or_else(zero),
        );
        let active_level = format!("ActiveLevel{index}");
        let normalized_active_level = format!("active_level_{index}");
        output.insert(
            normalized_active_level.clone(),
            first_truthy_dynamic(raw, &[&active_level, &normalized_active_level])
                .unwrap_or_else(zero),
        );
        let active_name = format!("Item_Active_{index}");
        let normalized_active_name = format!("item_active_{index}");
        output.insert(
            normalized_active_name.clone(),
            first_truthy_dynamic(raw, &[&active_name, &normalized_active_name])
                .unwrap_or_else(empty_string),
        );
    }
    for index in 1..=6 {
        for (prefix, direct_prefix, default) in [
            ("item_id_", "ItemId", zero()),
            ("item_level_", "ItemLevel", zero()),
            ("item_purch_", "Item_Purch_", empty_string()),
        ] {
            let direct = format!("{direct_prefix}{index}");
            let normalized = format!("{prefix}{index}");
            output.insert(
                normalized.clone(),
                first_truthy_dynamic(raw, &[&direct, &normalized])
                    .unwrap_or_else(|| default.clone()),
            );
        }
    }
    for index in 1..=8 {
        let direct = format!("BanId{index}");
        let normalized = format!("ban_id_{index}");
        output.insert(
            normalized.clone(),
            first_number_dynamic(raw, &[&direct, &normalized]).unwrap_or_else(zero),
        );
    }
    output.insert(
        "merged_players".to_owned(),
        normalize_direct_merged_players(raw.get("MergedPlayers")),
    );
    let has_ret_msg = raw
        .get("ret_msg")
        .filter(|value| !value.is_null())
        .map(js_string)
        .is_some_and(|value| !value.trim().is_empty());
    output.insert("has_ret_msg".to_owned(), Value::Bool(has_ret_msg));
    Value::Object(output)
}

fn normalize_match_details(rows: &[Value]) -> Option<MatchDetails> {
    let first = rows.first()?;
    let region = rows
        .iter()
        .find_map(|row| {
            first_truthy(row, &["Region", "region"])
                .map(|value| js_string(&value))
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_default();
    let observations = rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "team1": row_nullable_number(row, &["Team1Score", "Team1_Score", "team1_score"]),
                "team2": row_nullable_number(row, &["Team2Score", "Team2_Score", "team2_score"]),
                "winner": row_nullable_number(row, &["Winning_TaskForce", "Winning_Task_Force", "winning_task_force"])
            })
        })
        .collect();
    Some(MatchDetails {
        match_id: first_u64(first, &["Match", "match_id"]).unwrap_or_default(),
        entry_datetime: first_truthy(
            first,
            &[
                "Entry_Datetime",
                "entry_datetime",
                "Match_Time",
                "match_time",
            ],
        )
        .map(|value| js_string(&value))
        .unwrap_or_default(),
        map: first_truthy(first, &["Map_Game", "map"])
            .map(|value| js_string(&value))
            .unwrap_or_default(),
        queue_id: first_u64(
            first,
            &["match_queue_id", "Match_Queue_Id", "queue_id", "Queue"],
        )
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or_default(),
        duration_seconds: first_u64(first, &["Match_Duration", "duration_seconds"])
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or_default(),
        minutes: first_u64(first, &["Minutes", "minutes"])
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or_default(),
        region: normalize_region(&region).to_owned(),
        team1_score: match_number(first, &["Team1Score", "Team1_Score", "team1_score"]),
        team2_score: match_number(first, &["Team2Score", "Team2_Score", "team2_score"]),
        winning_task_force: match_number(
            first,
            &[
                "Winning_TaskForce",
                "Winning_Task_Force",
                "winning_task_force",
            ],
        ),
        direct_score_observations: Some(observations),
        has_replay: Some(
            first_truthy(first, &["hasReplay", "Has_Replay"])
                .map(|value| js_string(&value).eq_ignore_ascii_case("y"))
                .unwrap_or(false),
        ),
        players: rows.iter().map(normalize_match_player).collect(),
        ..MatchDetails::default()
    })
}

fn normalize_direct_merged_players(raw: Option<&Value>) -> Value {
    let Some(values) = raw.and_then(Value::as_array) else {
        return Value::Null;
    };
    Value::Array(
        values
            .iter()
            .map(|value| {
                serde_json::json!({
                    "player_id": first_u64(value, &["playerId"]).unwrap_or_default(),
                    "portal_id": first_u64(value, &["portalId"]).filter(|value| *value > 0),
                    "merge_datetime": first_truthy(value, &["merge_datetime"])
                        .map(|value| js_string(&value))
                        .unwrap_or_default()
                })
            })
            .collect(),
    )
}

fn round_to_2(value: f64) -> f64 {
    if !value.is_finite() || value == 0.0 {
        return 0.0;
    }
    if value < 1.0 {
        (value * 100.0).ceil() / 100.0
    } else {
        (value * 100.0).round() / 100.0
    }
}

fn first_u64(raw: &Value, keys: &[&str]) -> Option<u64> {
    first_truthy(raw, keys)
        .and_then(|value| js_number(&value))
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|value| value as u64)
}

fn match_number(raw: &Value, keys: &[&str]) -> Option<i32> {
    for key in keys {
        let Some(value) = raw.get(*key) else {
            continue;
        };
        if value.is_null() {
            return None;
        }
        if value.as_str().is_some_and(str::is_empty) {
            continue;
        }
        return js_number(value)
            .filter(|number| number.is_finite())
            .map(|number| number as i32);
    }
    None
}

fn row_nullable_number(raw: &Value, keys: &[&str]) -> Value {
    for key in keys {
        let Some(value) = raw.get(*key) else {
            continue;
        };
        if value.is_null() || value.as_str().is_some_and(str::is_empty) {
            continue;
        }
        return js_number(value).map_or(Value::Null, number_value);
    }
    Value::Null
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values.iter().map(js_string).collect::<Vec<_>>().join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

pub fn normalize_match_history_player(raw: &Value) -> Value {
    let mut output = Map::new();
    macro_rules! raw_field {
        ($name:literal, [$($key:literal),+], $default:expr) => {
            output.insert(
                $name.to_owned(),
                first_truthy(raw, &[$($key),+]).unwrap_or_else(|| $default),
            );
        };
    }
    macro_rules! number_field {
        ($name:literal, [$($key:literal),+]) => {
            output.insert(
                $name.to_owned(),
                first_number(raw, &[$($key),+]).unwrap_or_else(zero),
            );
        };
    }

    number_field!("player_id", ["playerId", "player_id"]);
    raw_field!(
        "player_name",
        ["playerName", "player_name"],
        Value::String("PRIVATEACCOUNT".to_owned())
    );
    raw_field!("match_id", ["Match", "match_id"], zero());
    raw_field!(
        "entry_datetime",
        ["Match_Time", "entry_datetime"],
        empty_string()
    );
    raw_field!("queue_id", ["Match_Queue_Id", "match_queue_id"], zero());
    let region = first_truthy(raw, &["Region", "region"])
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default();
    output.insert(
        "region".to_owned(),
        Value::String(normalize_region(&region).to_owned()),
    );
    raw_field!("champion_id", ["ChampionId"], zero());
    raw_field!(
        "champion_name",
        ["Champion", "ChampionName", "champion_name"],
        empty_string()
    );
    raw_field!("skin_id", ["SkinId"], zero());
    raw_field!("skin_name", ["Skin"], empty_string());
    raw_field!("kills", ["Kills"], zero());
    raw_field!("deaths", ["Deaths"], zero());
    raw_field!("assists", ["Assists"], zero());
    raw_field!("damage_done_in_hand", ["Damage_Done_In_Hand"], zero());
    raw_field!(
        "damage_done_physical",
        [
            "Damage",
            "damage_done_physical",
            "Damage_Player",
            "Damage_Done_Physical"
        ],
        zero()
    );
    raw_field!("damage_done_magical", ["Damage_Done_Magical"], zero());
    raw_field!("damage_taken", ["Damage_Taken"], zero());
    raw_field!("damage_mitigated", ["Damage_Mitigated"], zero());
    raw_field!("healing", ["Healing"], zero());
    raw_field!("healing_self", ["Healing_Player_Self"], zero());
    raw_field!("gold_earned", ["Gold"], zero());
    output.insert("gold_per_minute".to_owned(), zero());
    raw_field!("objective_assists", ["Objective_Assists"], zero());
    raw_field!("killing_spree", ["Killing_Spree"], zero());
    raw_field!("multi_kill_max", ["Multi_kill_Max"], zero());
    let win_status = match first_truthy(raw, &["Win_Status"])
        .as_ref()
        .and_then(Value::as_str)
    {
        Some("Win") => "Winner".to_owned(),
        Some("Loss") => "Loser".to_owned(),
        Some(value) => value.to_owned(),
        None => String::new(),
    };
    output.insert("win_status".to_owned(), Value::String(win_status));
    raw_field!("task_force", ["TaskForce"], zero());
    output.insert(
        "history_team1_score".to_owned(),
        nullable_number(raw, &["history_team1_score", "Team1Score", "Team1_Score"]),
    );
    output.insert(
        "history_team2_score".to_owned(),
        nullable_number(raw, &["history_team2_score", "Team2Score", "Team2_Score"]),
    );
    output.insert(
        "history_winning_task_force".to_owned(),
        nullable_number(
            raw,
            &[
                "history_winning_task_force",
                "Winning_TaskForce",
                "Winning_Task_Force",
            ],
        ),
    );
    number_field!("league_tier", ["League_Tier", "league_tier"]);
    number_field!("league_points", ["League_Points", "league_points"]);
    number_field!("account_level", ["Account_Level", "account_level"]);
    number_field!("mastery_level", ["Mastery_Level", "mastery_level"]);
    number_field!("party_id", ["PartyId", "party_id"]);
    raw_field!("time_in_match", ["Time_In_Match_Seconds"], zero());
    raw_field!("distance_traveled", ["Distance_Traveled"], zero());
    raw_field!(
        "structure_damage",
        ["Damage_Structure", "Structure_Damage"],
        zero()
    );
    raw_field!("camps_cleared", ["Creeps"], zero());
    output.insert(
        "source".to_owned(),
        Value::String("match_history".to_owned()),
    );
    number_field!("portal_id", ["playerPortalId", "portal_id"]);
    raw_field!(
        "portal_user_id",
        ["playerPortalUserId", "portal_user_id"],
        empty_string()
    );
    raw_field!("kills_player", ["Kills"], zero());
    raw_field!("healing_player_self", ["Healing_Player_Self"], zero());
    raw_field!("damage_taken_physical", ["Damage_Taken_Physical"], zero());
    raw_field!("damage_taken_magical", ["Damage_Taken_Magical"], zero());
    number_field!("kills_fire_giant", ["Kills_Fire_Giant", "kills_fire_giant"]);
    number_field!("kills_gold_fury", ["Kills_Gold_Fury", "kills_gold_fury"]);
    number_field!("kills_phoenix", ["Kills_Phoenix", "kills_phoenix"]);
    number_field!(
        "kills_siege_jugg",
        ["Kills_Siege_Juggernaut", "kills_siege_jugg"]
    );
    number_field!(
        "kills_wild_jugg",
        ["Kills_Wild_Juggernaut", "kills_wild_jugg"]
    );
    number_field!("kills_bot", ["Kills_Bot", "kills_bot"]);
    number_field!("kills_single", ["Kills_Single", "kills_single"]);
    number_field!("kills_double", ["Kills_Double", "kills_double"]);
    number_field!("kills_triple", ["Kills_Triple", "kills_triple"]);
    number_field!("kills_quadra", ["Kills_Quadra", "kills_quadra"]);
    number_field!("kills_penta", ["Kills_Penta", "kills_penta"]);
    number_field!(
        "kills_first_blood",
        ["Kills_First_Blood", "kills_first_blood"]
    );
    raw_field!("wards_placed", ["Wards_Placed"], zero());
    number_field!("towers_destroyed", ["Towers_Destroyed", "towers_destroyed"]);
    number_field!("league_wins", ["League_Wins", "league_wins"]);
    number_field!("league_losses", ["League_Losses", "league_losses"]);
    raw_field!("healing_bot", ["Healing_Bot"], zero());
    raw_field!("damage_bot", ["Damage_Bot"], zero());
    raw_field!("platform", ["Platform", "platform"], empty_string());
    raw_field!("surrendered", ["Surrendered"], zero());
    number_field!("team_id", ["TeamId", "team_id"]);
    raw_field!("team_name", ["Team_Name", "team_name"], empty_string());
    number_field!("rank_stat_league", ["Rank_Stat_League", "rank_stat_league"]);
    number_field!(
        "final_match_level",
        ["Final_Match_Level", "final_match_level"]
    );
    number_field!("match_duration", ["Match_Duration", "match_duration"]);
    for index in 1..=4 {
        let id_key = format!("ActiveId{index}");
        output.insert(
            format!("active_id_{index}"),
            first_truthy_dynamic(raw, &[&id_key]).unwrap_or_else(zero),
        );
        let level_key = format!("ActiveLevel{index}");
        let scaled = first_f64_dynamic(raw, &[&level_key]).unwrap_or(0.0) / 4.0;
        output.insert(
            format!("active_level_{index}"),
            Value::Number(Number::from(js_round(scaled))),
        );
        let active_key = format!("Active_{index}");
        output.insert(
            format!("item_active_{index}"),
            first_truthy_dynamic(raw, &[&active_key]).unwrap_or_else(empty_string),
        );
    }
    for index in 1..=6 {
        let id_key = format!("ItemId{index}");
        output.insert(
            format!("item_id_{index}"),
            first_truthy_dynamic(raw, &[&id_key]).unwrap_or_else(zero),
        );
        let level_key = format!("ItemLevel{index}");
        output.insert(
            format!("item_level_{index}"),
            first_truthy_dynamic(raw, &[&level_key]).unwrap_or_else(zero),
        );
        let purchase_key = format!("Item_{index}");
        output.insert(
            format!("item_purch_{index}"),
            first_truthy_dynamic(raw, &[&purchase_key]).unwrap_or_else(empty_string),
        );
    }
    for index in 1..=8 {
        let direct = format!("BanId{index}");
        let normalized = format!("ban_id_{index}");
        output.insert(
            format!("ban_id_{index}"),
            first_number_dynamic(raw, &[&direct, &normalized]).unwrap_or_else(zero),
        );
    }
    output.insert("merged_players".to_owned(), Value::Null);
    output.insert("has_ret_msg".to_owned(), Value::Bool(false));
    Value::Object(output)
}

pub fn normalize_player_profile(raw: &Value) -> PlayerProfile {
    let platform_name = nullable_text(raw.get("Name"));
    let hz_player_name = nullable_text(raw.get("hz_player_name"));
    let hz_gamer_tag = nullable_text(raw.get("hz_gamer_tag"));
    let platform_reason = synthetic_reason(platform_name.as_deref(), "Name");
    let player_reason = synthetic_reason(hz_player_name.as_deref(), "hz_player_name");
    let gamer_reason = synthetic_reason(hz_gamer_tag.as_deref(), "hz_gamer_tag");
    let reasons = [
        platform_reason.as_deref(),
        player_reason.as_deref(),
        gamer_reason.as_deref(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("; ");
    let anomaly_reason = (!reasons.is_empty()).then_some(reasons);
    let (player_name, name_source, name_anomaly_reason) =
        if hz_player_name.is_some() && player_reason.is_none() {
            (
                hz_player_name.clone().unwrap_or_default(),
                "hz_player_name".to_owned(),
                anomaly_reason.clone(),
            )
        } else if hz_gamer_tag.is_some() && gamer_reason.is_none() {
            (
                hz_gamer_tag.clone().unwrap_or_default(),
                "hz_gamer_tag".to_owned(),
                anomaly_reason.clone(),
            )
        } else if platform_name.is_some() && platform_reason.is_none() {
            (
                platform_name.clone().unwrap_or_default(),
                "name".to_owned(),
                anomaly_reason.clone(),
            )
        } else {
            (
                String::new(),
                "none".to_owned(),
                Some(anomaly_reason.as_ref().map_or_else(
                    || "profile payload did not contain a usable display name".to_owned(),
                    |reason| format!("{reason}; no usable display fallback was present"),
                )),
            )
        };
    let api_level = number_from_present(raw, &["Level", "level"]).unwrap_or(0.0);
    let total_xp = raw
        .get("Total_XP")
        .or_else(|| raw.get("total_xp"))
        .and_then(js_number)
        .filter(|value| value.is_finite() && *value >= 0.0);
    let level = resolve_player_level(total_xp, api_level);
    let player_id = first_number_i64(raw, &["Id", "ActivePlayerId"]).unwrap_or(0);
    let active_player_id = first_number_i64(raw, &["ActivePlayerId", "Id"]).unwrap_or(0);
    let region = first_truthy(raw, &["Region"])
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default();
    let platform = nullable_text(first_truthy(raw, &["Platform"]).as_ref());

    PlayerProfile {
        player_id,
        active_player_id,
        player_name,
        platform_name,
        level,
        api_level: positive_floor_i32(api_level),
        wins: first_number_i32(raw, &["Wins"]).unwrap_or(0),
        losses: first_number_i32(raw, &["Losses"]).unwrap_or(0),
        leaves: first_number_i32(raw, &["Leaves"]).unwrap_or(0),
        mastery_level: first_number_i32(raw, &["MasteryLevel"]).unwrap_or(0),
        region: normalize_region(&region).to_owned(),
        platform,
        hours_played: first_number_i32(raw, &["HoursPlayed"]).unwrap_or(0),
        minutes_played: first_number_i32(raw, &["MinutesPlayed"]).unwrap_or(0),
        total_xp: total_xp.map(|value| value.floor() as i64).unwrap_or(0),
        total_worshippers: first_number_i64(raw, &["Total_Worshippers"]).unwrap_or(0),
        total_achievements: first_number_i32(raw, &["Total_Achievements"]).unwrap_or(0),
        title: text_or_default(raw.get("Title")),
        avatar_id: first_number_i32(raw, &["AvatarId"]).unwrap_or(0),
        avatar_url: nullable_text(raw.get("AvatarURL")),
        team_id: first_number_i32(raw, &["TeamId"]).unwrap_or(0),
        team_name: text_or_default(raw.get("Team_Name")),
        hz_gamer_tag,
        hz_player_name,
        name_source,
        name_anomaly: anomaly_reason.is_some(),
        name_anomaly_reason,
        ret_msg: nullable_text(raw.get("ret_msg")),
        privacy_flag: raw
            .get("privacy_flag")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case("y")),
        created_at: raw
            .get("Created_Datetime")
            .and_then(value_text)
            .and_then(parse_hirez_timestamp),
        last_login: raw
            .get("Last_Login_Datetime")
            .and_then(value_text)
            .and_then(parse_hirez_timestamp),
        loading_frame: text_or_default(raw.get("LoadingFrame")),
        personal_status_message: text_or_default(raw.get("Personal_Status_Message")),
        ranked_kbm: normalize_ranked_queue(raw.get("RankedKBM")),
        ranked_controller: normalize_ranked_queue(raw.get("RankedController")),
        ranked_conquest: normalize_ranked_queue(raw.get("RankedConquest")),
        tier_ranked_kbm: first_number_i32(raw, &["Tier_RankedKBM"]).unwrap_or(0),
        tier_ranked_controller: first_number_i32(raw, &["Tier_RankedController"]).unwrap_or(0),
        tier_conquest: first_number_i32(raw, &["Tier_Conquest"]).unwrap_or(0),
        merged_players: normalize_merged_players(raw.get("MergedPlayers")),
    }
}

pub fn calculate_player_level(total_xp: f64) -> Option<i32> {
    if !total_xp.is_finite() || total_xp < 0.0 {
        return None;
    }
    let experience = total_xp.floor() as i64;
    if experience >= 25_480_000 {
        return i32::try_from((experience - 25_480_000) / 1_000_000 + 50).ok();
    }
    let mut threshold = 0_i64;
    for level in 2..=50 {
        threshold += i64::from(level * 20_000);
        if threshold > experience {
            return Some(level - 1);
        }
    }
    Some(50)
}

pub fn resolve_player_level(total_xp: Option<f64>, api_level: f64) -> i32 {
    total_xp
        .and_then(calculate_player_level)
        .unwrap_or_else(|| positive_floor_i32(api_level))
}

pub fn parse_hirez_timestamp(value: &str) -> Option<OffsetDateTime> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(value) = OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
    {
        return Some(value);
    }
    const HIREZ_FORMAT: &[time::format_description::BorrowedFormatItem<'_>] = format_description!(
        "[month padding:none]/[day padding:none]/[year] [hour repr:12 padding:none]:[minute]:[second] [period case:upper]"
    );
    PrimitiveDateTime::parse(value, HIREZ_FORMAT)
        .ok()
        .map(|value| value.assume_utc())
}

fn normalize_ranked_queue(raw: Option<&Value>) -> RankedQueue {
    let Some(raw) = raw.filter(|value| !value.is_null()) else {
        return RankedQueue::default();
    };
    RankedQueue {
        name: text_or_default(raw.get("Name")),
        rank: first_number_i32(raw, &["Rank"]).unwrap_or(0),
        tier: first_number_i32(raw, &["Tier"]).unwrap_or(0),
        points: first_number_i32(raw, &["Points"]).unwrap_or(0),
        wins: first_number_i32(raw, &["Wins"]).unwrap_or(0),
        losses: first_number_i32(raw, &["Losses"]).unwrap_or(0),
        leaves: first_number_i32(raw, &["Leaves"]).unwrap_or(0),
        trend: first_number_i32(raw, &["Trend"]).unwrap_or(0),
        prev_rank: first_number_i32(raw, &["PrevRank"]).unwrap_or(0),
        season: first_number_i32(raw, &["Season"]).unwrap_or(0),
        ret_msg: nullable_text(raw.get("ret_msg")),
        player_id: first_number_i64(raw, &["player_id"]).filter(|value| *value != 0),
    }
}

fn normalize_merged_players(raw: Option<&Value>) -> Vec<MergedPlayer> {
    raw.and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .map(|value| MergedPlayer {
                    player_id: first_number_i64(value, &["playerId"]).unwrap_or(0),
                    portal_id: first_number_i32(value, &["portalId"]).filter(|value| *value != 0),
                    merge_datetime: value
                        .get("merge_datetime")
                        .and_then(value_text)
                        .and_then(parse_hirez_timestamp),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn synthetic_reason(value: Option<&str>, field_name: &str) -> Option<String> {
    let value = value?;
    if is_epic_synthetic(value) {
        return Some(format!(
            "profile {field_name} is an obfuscated Epic platform identifier"
        ));
    }
    if is_dummy_synthetic(value) {
        return Some(format!(
            "profile {field_name} is a HirezRelay dummy-mode synthetic name"
        ));
    }
    None
}

fn is_epic_synthetic(value: &str) -> bool {
    let Some((prefix, suffix)) = split_case_insensitive(value, "User-") else {
        return false;
    };
    prefix.len() >= 20
        && suffix.len() >= 6
        && prefix.bytes().all(|value| value.is_ascii_hexdigit())
        && suffix.bytes().all(|value| value.is_ascii_hexdigit())
}

fn is_dummy_synthetic(value: &str) -> bool {
    value
        .get(..11)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("DummyPlayer"))
        && value.len() > 11
        && value[11..].bytes().all(|value| value.is_ascii_digit())
}

fn split_case_insensitive<'a>(value: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    let index = value
        .to_ascii_lowercase()
        .find(&needle.to_ascii_lowercase())?;
    Some((&value[..index], &value[index + needle.len()..]))
}

fn nullable_text(value: Option<&Value>) -> Option<String> {
    value
        .and_then(value_text)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn text_or_default(value: Option<&Value>) -> String {
    nullable_text(value).unwrap_or_default()
}

fn value_text(value: &Value) -> Option<&str> {
    value.as_str()
}

fn number_from_present(raw: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .find_map(|key| raw.get(*key))
        .filter(|value| !value.is_null())
        .and_then(js_number)
}

fn first_number_i64(raw: &Value, keys: &[&str]) -> Option<i64> {
    first_truthy(raw, keys)
        .and_then(|value| js_number(&value))
        .filter(|value| value.is_finite())
        .map(|value| value as i64)
}

fn first_number_i32(raw: &Value, keys: &[&str]) -> Option<i32> {
    first_number_i64(raw, keys).and_then(|value| i32::try_from(value).ok())
}

fn positive_floor_i32(value: f64) -> i32 {
    if value.is_finite() && value > 0.0 {
        value.floor().clamp(0.0, f64::from(i32::MAX)) as i32
    } else {
        0
    }
}

fn first_truthy(raw: &Value, keys: &[&str]) -> Option<Value> {
    first_truthy_dynamic(raw, keys)
}

fn first_truthy_dynamic(raw: &Value, keys: &[&str]) -> Option<Value> {
    keys.iter()
        .filter_map(|key| raw.get(*key))
        .find(|value| js_truthy(value))
        .cloned()
}

fn first_number(raw: &Value, keys: &[&str]) -> Option<Value> {
    first_number_dynamic(raw, keys)
}

fn first_number_dynamic(raw: &Value, keys: &[&str]) -> Option<Value> {
    first_truthy_dynamic(raw, keys)
        .and_then(|value| js_number(&value))
        .map(number_value)
}

fn first_f64_dynamic(raw: &Value, keys: &[&str]) -> Option<f64> {
    first_truthy_dynamic(raw, keys).and_then(|value| js_number(&value))
}

fn nullable_number(raw: &Value, keys: &[&str]) -> Value {
    for key in keys {
        let Some(value) = raw.get(*key) else {
            continue;
        };
        if value.is_null() || value.as_str().is_some_and(str::is_empty) {
            continue;
        }
        if let Some(value) = js_number(value)
            && value.is_finite()
        {
            return number_value(value);
        }
    }
    Value::Null
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value
            .as_f64()
            .is_some_and(|value| value != 0.0 && !value.is_nan()),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn js_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.trim().parse().ok(),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::Null => Some(0.0),
        _ => None,
    }
}

fn number_value(value: f64) -> Value {
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        Value::Number(Number::from(value as i64))
    } else {
        Number::from_f64(value).map_or(Value::Null, Value::Number)
    }
}

fn js_round(value: f64) -> i64 {
    (value + 0.5).floor() as i64
}

fn zero() -> Value {
    Value::Number(Number::from(0))
}

fn empty_string() -> Value {
    Value::String(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_normalizer_matches_recovery_specific_rules() {
        let normalized = normalize_match_history_player(&serde_json::json!({
            "playerId": "7",
            "playerName": "Name",
            "Match": 100,
            "Match_Time": "7/28/2026 1:00:00 PM",
            "Match_Queue_Id": 486,
            "Region": "North America",
            "Win_Status": "Win",
            "Damage": 123,
            "Kills": 4,
            "ActiveLevel1": 9,
            "Team1Score": "4",
            "Team2Score": 2,
            "Winning_TaskForce": 1
        }));
        assert_eq!(normalized["player_id"], 7);
        assert_eq!(normalized["region"], "NA");
        assert_eq!(normalized["win_status"], "Winner");
        assert_eq!(normalized["damage_done_physical"], 123);
        assert_eq!(normalized["kills_player"], 4);
        assert_eq!(normalized["active_level_1"], 2);
        assert_eq!(normalized["history_team1_score"], 4);
        assert_eq!(normalized["history_team2_score"], 2);
        assert_eq!(normalized["history_winning_task_force"], 1);
        assert_eq!(normalized["source"], "match_history");
        assert_eq!(normalized["merged_players"], Value::Null);
        assert_eq!(normalized["has_ret_msg"], false);
    }

    #[test]
    fn damage_normalizers_prefer_combined_player_damage() {
        let direct = normalize_match_player(&serde_json::json!({
            "Damage_Player": 31310,
            "Damage_Done_Physical": 28979,
            "Damage_Done_Magical": 1039,
            "Damage_Done_In_Hand": 22162
        }));
        assert_eq!(direct["damage_done_physical"], 31310);
        assert_eq!(direct["damage_done_magical"], 1039);
        assert_eq!(direct["damage_done_in_hand"], 22162);

        let recovered = normalize_match_history_player(&serde_json::json!({
            "Damage": 31310,
            "Damage_Done_Physical": 28979,
            "Damage_Done_Magical": 1039
        }));
        assert_eq!(recovered["damage_done_physical"], 31310);
        assert_eq!(recovered["damage_done_in_hand"], 0);
    }

    #[test]
    fn null_ret_msg_is_not_a_recovery_error() {
        let normalized = normalize_match_player(&serde_json::json!({
            "playerId": 7,
            "playerName": "Name",
            "Match": 100,
            "ret_msg": null
        }));
        assert_eq!(normalized["has_ret_msg"], false);
    }

    #[test]
    fn unknown_region_and_missing_score_remain_explicit() {
        let normalized = normalize_match_history_player(&serde_json::json!({}));
        assert_eq!(normalized["player_name"], "PRIVATEACCOUNT");
        assert_eq!(normalized["region"], "Unknown");
        assert_eq!(normalized["history_team1_score"], Value::Null);
        assert_eq!(normalized["active_level_1"], 0);
    }

    #[test]
    fn direct_match_normalizer_preserves_order_region_and_score_evidence() {
        let rows = vec![
            serde_json::json!({
                "Match": 200,
                "Entry_Datetime": "7/28/2026 1:00:00 PM",
                "Map_Game": "LIVE Jaguar Falls",
                "match_queue_id": 486,
                "Match_Duration": 900,
                "Minutes": 15,
                "Region": null,
                "Team1Score": null,
                "Team2Score": 1,
                "Winning_TaskForce": 1,
                "playerId": "7",
                "playerName": "One",
                "TaskForce": 1,
                "Gold_Earned": 1000,
                "Time_In_Match_Seconds": 600,
                "ret_msg": null
            }),
            serde_json::json!({
                "Match": 100,
                "Entry_Datetime": "7/28/2026 12:00:00 PM",
                "Map_Game": "LIVE Stone Keep",
                "match_queue_id": 424,
                "Region": "NA",
                "Team1Score": 4,
                "Team2Score": 2,
                "Winning_TaskForce": 1,
                "playerId": 8
            }),
            serde_json::json!({
                "Match": 200,
                "Region": "Europe",
                "Team1Score": 4,
                "Team2Score": 1,
                "Winning_TaskForce": 1,
                "playerId": 9,
                "MergedPlayers": [{
                    "playerId": 10,
                    "portalId": 1,
                    "merge_datetime": "2026-07-28T00:00:00Z"
                }]
            }),
        ];

        let matches = normalize_flat_match_detail_rows(&rows);
        assert_eq!(
            matches
                .iter()
                .map(|r#match| r#match.match_id)
                .collect::<Vec<_>>(),
            vec![200, 100]
        );
        assert_eq!(matches[0].region, "EU");
        assert_eq!(matches[0].team1_score, None);
        assert_eq!(matches[0].team2_score, Some(1));
        assert_eq!(
            matches[0].direct_score_observations.as_ref().unwrap().len(),
            2
        );
        assert_eq!(matches[0].players[0]["gold_per_minute"], 100);
        assert_eq!(matches[0].players[0]["source"], "direct");
        assert_eq!(
            matches[0].players[1]["merged_players"][0],
            serde_json::json!({
                "player_id": 10,
                "portal_id": 1,
                "merge_datetime": "2026-07-28T00:00:00Z"
            })
        );
    }

    #[test]
    fn profile_normalizer_prefers_hirez_name_and_calculates_uncapped_level() {
        let profile = normalize_player_profile(&serde_json::json!({
            "Id": 7,
            "ActivePlayerId": 7,
            "Name": "0123456789abcdef0123User-abcdef",
            "hz_player_name": "Public Name",
            "hz_gamer_tag": "Gamer",
            "Level": 999,
            "Total_XP": 1025480000_i64,
            "Region": "North America",
            "Platform": "Steam",
            "privacy_flag": "Y",
            "RankedKBM": {"Tier": 12, "Points": 34},
            "MergedPlayers": [{"playerId": 8, "portalId": 1}]
        }));
        assert_eq!(profile.player_name, "Public Name");
        assert_eq!(profile.name_source, "hz_player_name");
        assert!(profile.name_anomaly);
        assert_eq!(profile.level, 1050);
        assert_eq!(profile.api_level, 999);
        assert_eq!(profile.region, "NA");
        assert!(profile.privacy_flag);
        assert_eq!(profile.ranked_kbm.tier, 12);
        assert_eq!(profile.merged_players[0].player_id, 8);
    }

    #[test]
    fn profile_normalizer_blocks_dummy_name_and_uses_level_fallback() {
        let profile = normalize_player_profile(&serde_json::json!({
            "Id": 9,
            "Name": "DummyPlayer123",
            "Level": 42,
            "Total_XP": ""
        }));
        assert!(profile.player_name.is_empty());
        assert_eq!(profile.name_source, "none");
        assert_eq!(profile.level, 42);
        assert!(
            profile
                .name_anomaly_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("dummy-mode"))
        );
    }
}
