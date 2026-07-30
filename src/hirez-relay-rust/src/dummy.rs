use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::RwLock;

use crate::{
    contract::parse_completed_match_requests,
    hirez_client::ApiRequestOptions,
    model::{MatchDetails, player_number},
    normalizer::normalize_match_history_player,
    operations::ApiCaller,
    provider::{CompletedMatchProvider, RelayError},
    resolver::get_match_details_batch,
};

const CHAMPIONS: &[(u64, &str)] = &[
    (2288, "Cassie"),
    (2285, "Fernando"),
    (2493, "Koga"),
    (2362, "Jenos"),
    (2314, "Inara"),
    (2431, "Lian"),
    (2512, "Vora"),
    (2557, "Nyx"),
    (2404, "Furia"),
    (2548, "Betty la Bomba"),
];

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DummyScenario {
    #[default]
    Complete,
    BrokenSkin,
    OmitFromMulti,
    RosterFailure,
    HistoryMissing,
    NoPlayerAnchors,
    PveSingleHuman,
    VendorFailure,
    HardBrokenSkin,
    HardBrokenSkinDemoFailure,
    LocalPreflight,
}

#[derive(Default)]
pub struct DummyProvider {
    scenarios: RwLock<HashMap<u64, DummyScenario>>,
    direct_calls: AtomicU64,
    roster_calls: AtomicU64,
    history_calls: AtomicU64,
    demo_calls: AtomicU64,
    other_calls: Mutex<BTreeMap<&'static str, u64>>,
}

impl DummyProvider {
    pub async fn set_scenario(&self, match_id: u64, scenario: DummyScenario) {
        let mut scenarios = self.scenarios.write().await;
        if scenario == DummyScenario::Complete {
            scenarios.remove(&match_id);
        } else {
            scenarios.insert(match_id, scenario);
        }
    }

    pub async fn reset_scenarios(&self) {
        self.scenarios.write().await.clear();
    }

    pub fn reset_counts(&self) {
        self.direct_calls.store(0, Ordering::Relaxed);
        self.roster_calls.store(0, Ordering::Relaxed);
        self.history_calls.store(0, Ordering::Relaxed);
        self.demo_calls.store(0, Ordering::Relaxed);
        self.other_calls.lock().expect("dummy call counts").clear();
    }

    pub fn counts(&self) -> BTreeMap<&'static str, u64> {
        let mut counts: BTreeMap<_, _> = [
            ("getdemodetails", self.demo_calls.load(Ordering::Relaxed)),
            (
                "getmatchdetailsbatch",
                self.direct_calls.load(Ordering::Relaxed),
            ),
            (
                "getmatchhistory",
                self.history_calls.load(Ordering::Relaxed),
            ),
            (
                "getplayerbatchfrommatch",
                self.roster_calls.load(Ordering::Relaxed),
            ),
        ]
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .collect();
        counts.extend(
            self.other_calls
                .lock()
                .expect("dummy call counts")
                .iter()
                .filter(|(_, count)| **count > 0)
                .map(|(endpoint, count)| (*endpoint, *count)),
        );
        counts
    }

    pub fn bump(&self, endpoint: &'static str) {
        let mut counts = self.other_calls.lock().expect("dummy call counts");
        *counts.entry(endpoint).or_default() += 1;
    }

    async fn scenario(&self, match_id: u64) -> DummyScenario {
        self.scenarios
            .read()
            .await
            .get(&match_id)
            .copied()
            .unwrap_or_default()
    }
}

impl CompletedMatchProvider for DummyProvider {
    async fn get_match_details_batch(
        &self,
        match_ids: &[u64],
    ) -> Result<Vec<MatchDetails>, RelayError> {
        self.direct_calls.fetch_add(1, Ordering::Relaxed);
        let mut matches = Vec::with_capacity(match_ids.len());
        for &match_id in match_ids {
            let scenario = self.scenario(match_id).await;
            match scenario {
                DummyScenario::VendorFailure => {
                    return Err(RelayError::Upstream(
                        "Synthetic Hi-Rez service-wide failure".to_owned(),
                    ));
                }
                DummyScenario::HardBrokenSkin | DummyScenario::HardBrokenSkinDemoFailure => {
                    return Err(RelayError::Upstream(
                        "HIREZ_UNKNOWN_RETURN: Value was either too large or too small for an Int16 while reading SkinId".to_owned(),
                    ));
                }
                DummyScenario::OmitFromMulti if match_ids.len() > 1 => continue,
                DummyScenario::NoPlayerAnchors => continue,
                DummyScenario::BrokenSkin
                | DummyScenario::RosterFailure
                | DummyScenario::HistoryMissing
                | DummyScenario::LocalPreflight => {
                    let mut r#match = dummy_match(match_id, 486, 10);
                    r#match.players.truncate(7);
                    r#match.players.push(json!({
                        "match_id": match_id,
                        "player_id": 0,
                        "player_name": "BROKEN_SKIN_SENTINEL",
                        "champion_id": 0,
                        "skin_id": 32768,
                        "has_ret_msg": true,
                        "ret_msg": "Value was either too large or too small for an Int16 while reading SkinId"
                    }));
                    r#match.direct_score_observations =
                        r#match.direct_score_observations.map(|mut observations| {
                            observations.truncate(8);
                            observations
                        });
                    matches.push(r#match);
                }
                DummyScenario::PveSingleHuman => {
                    matches.push(dummy_match(match_id, 425, 1));
                }
                _ => matches.push(dummy_match(match_id, 486, 10)),
            }
        }
        Ok(matches)
    }

    async fn get_player_batch_from_match(&self, match_id: u64) -> Result<Vec<Value>, RelayError> {
        self.roster_calls.fetch_add(1, Ordering::Relaxed);
        match self.scenario(match_id).await {
            DummyScenario::RosterFailure => Err(RelayError::Upstream(
                "Synthetic getplayerbatchfrommatch failure".to_owned(),
            )),
            DummyScenario::NoPlayerAnchors => Ok(Vec::new()),
            _ => Ok((0..10)
                .map(|index| dummy_roster_player(match_id, index))
                .collect()),
        }
    }

    async fn get_match_history(
        &self,
        player_id: u64,
        match_id: u64,
    ) -> Result<Vec<Value>, RelayError> {
        self.history_calls.fetch_add(1, Ordering::Relaxed);
        if self.scenario(match_id).await == DummyScenario::HistoryMissing {
            return Ok(Vec::new());
        }
        let index = player_id.saturating_sub(match_id * 100 + 1) as usize;
        if index >= 10 {
            return Ok(Vec::new());
        }
        Ok(vec![dummy_history_player(match_id, index)])
    }

    async fn get_demo_details(&self, match_id: u64) -> Result<Value, RelayError> {
        self.demo_calls.fetch_add(1, Ordering::Relaxed);
        if self.scenario(match_id).await == DummyScenario::HardBrokenSkinDemoFailure {
            return Err(RelayError::Upstream(
                "Synthetic getdemodetails failure".to_owned(),
            ));
        }
        Ok(json!({
            "Match": match_id,
            "Queue": 486,
            "Entry_Datetime": entry_datetime(match_id),
            "Map_Game": "LIVE Jaguar Falls",
            "Match_Time": 1000,
            "Minutes": 17,
            "hasReplay": "y",
            "ret_msg": null
        }))
    }

    async fn get_local_recovery_players(&self, match_id: u64) -> Result<Vec<Value>, RelayError> {
        if self.scenario(match_id).await != DummyScenario::LocalPreflight {
            return Ok(Vec::new());
        }
        Ok((7..10)
            .map(|index| dummy_history_player(match_id, index))
            .collect())
    }
}

#[async_trait]
impl ApiCaller for DummyProvider {
    async fn call(
        &self,
        method: &str,
        params: &[String],
        _options: ApiRequestOptions,
        _consumer: &str,
    ) -> Result<Value, RelayError> {
        match method {
            "getmatchhistory" => {
                self.bump("getmatchhistory");
                let player_id = params
                    .first()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or_default();
                let encoded_match_id = player_id / 100;
                let mut matches = dummy_match_history(player_id, 50);
                if self.scenario(encoded_match_id).await == DummyScenario::HistoryMissing {
                    matches.retain(|row| {
                        player_number(row, &["Match", "match_id"]) != encoded_match_id
                    });
                }
                Ok(json!({"matches": matches, "ret_msg": null}))
            }
            _ => Err(RelayError::Unsupported(format!(
                "Unsupported dummy Hi-Rez endpoint: {method}"
            ))),
        }
    }
}

pub fn dummy_match(match_id: u64, queue_id: u32, player_count: usize) -> MatchDetails {
    let duration = 900 + (match_id % 420) as u32;
    let team2_score = (match_id % 2) as i32;
    MatchDetails {
        match_id,
        entry_datetime: entry_datetime(match_id),
        map: "LIVE Jaguar Falls".to_owned(),
        queue_id,
        duration_seconds: duration,
        minutes: (duration + 30) / 60,
        region: if match_id.is_multiple_of(2) {
            "NA".to_owned()
        } else {
            "EU".to_owned()
        },
        team1_score: Some(4),
        team2_score: Some(team2_score),
        winning_task_force: Some(1),
        direct_score_observations: Some(
            (0..player_count)
                .map(|_| {
                    json!({
                        "team1": 4,
                        "team2": team2_score,
                        "winner": 1
                    })
                })
                .collect(),
        ),
        has_replay: Some(true),
        players: (0..player_count)
            .map(|index| dummy_player(match_id, index, queue_id))
            .collect(),
        ..MatchDetails::default()
    }
}

fn dummy_player(match_id: u64, index: usize, queue_id: u32) -> Value {
    let player_id = match_id * 100 + index as u64 + 1;
    let task_force = if index < 5 { 1 } else { 2 };
    let (champion_id, champion_name) = CHAMPIONS[index % CHAMPIONS.len()];
    let deaths = 1 + ((match_id as usize + index) % 8);
    let kills = 2 + ((match_id as usize + index * 3) % 15);
    let assists = 4 + ((match_id as usize + index * 5) % 18);
    let gold = 1400 + ((match_id as usize + index * 97) % 2200);
    let duration = 900 + (match_id % 420);
    let region = if match_id.is_multiple_of(2) {
        "NA"
    } else {
        "EU"
    };
    let win_status = if task_force == 1 { "Winner" } else { "Loser" };
    let team_name = if task_force == 1 { "Blue" } else { "Red" };
    let damage_done = 12000 + index * 600;
    let damage_in_hand = 9000 + index * 500;
    let damage_taken = 16000 + index * 800;
    let damage_mitigated = if index.is_multiple_of(2) { 12000 } else { 2000 };
    let healing = if index == 3 || index == 8 {
        18000
    } else {
        1200
    };
    let healing_self = 800 + index * 10;
    let damage_taken_physical = 12000 + index * 500;
    let damage_taken_magical = 4000 + index * 300;
    let killing_spree = kills.saturating_sub(deaths);
    let player_name = format!("DummyPlayer{}", index + 1);
    let entry = entry_datetime(match_id);
    json!({
        "player_id": player_id,
        "player_name": player_name,
        "match_id": match_id,
        "entry_datetime": entry,
        "queue_id": queue_id,
        "champion_id": champion_id,
        "champion_name": champion_name,
        "skin_id": 10000 + index,
        "skin_name": "Default",
        "kills": kills,
        "deaths": deaths,
        "assists": assists,
        "damage_done_in_hand": damage_in_hand,
        "damage_done_physical": damage_done,
        "damage_done_magical": 0,
        "damage_taken": damage_taken,
        "damage_mitigated": damage_mitigated,
        "healing": healing,
        "healing_self": healing_self,
        "gold_earned": gold,
        "gold_per_minute": ((gold as f64) / (duration as f64 / 60.0) * 100.0).round() / 100.0,
        "objective_assists": index % 4,
        "killing_spree": killing_spree,
        "multi_kill_max": index % 3,
        "task_force": task_force,
        "team_id": task_force,
        "win_status": win_status,
        "league_tier": 18 + (index % 5),
        "league_points": 40 + index * 10,
        "account_level": 100 + index,
        "mastery_level": 20 + index,
        "party_id": index % 5,
        "time_in_match": duration,
        "distance_traveled": 120000 + index * 1000,
        "structure_damage": 0,
        "camps_cleared": 0,
        "source": "direct",
        "portal_id": 0,
        "portal_user_id": "",
        "kills_player": kills,
        "healing_player_self": healing_self,
        "damage_taken_physical": damage_taken_physical,
        "damage_taken_magical": damage_taken_magical,
        "kills_fire_giant": 0,
        "kills_gold_fury": 0,
        "kills_phoenix": 0,
        "kills_siege_jugg": 0,
        "kills_wild_jugg": 0,
        "kills_bot": 0,
        "kills_single": kills,
        "kills_double": index % 2,
        "kills_triple": 0,
        "kills_quadra": 0,
        "kills_penta": 0,
        "kills_first_blood": if index == 0 { 1 } else { 0 },
        "wards_placed": 0,
        "towers_destroyed": 0,
        "league_wins": 50 + index,
        "league_losses": 45 + index,
        "healing_bot": 0,
        "damage_bot": 0,
        "platform": "Steam",
        "region": region,
        "surrendered": 0,
        "team_name": team_name,
        "rank_stat_league": 0,
        "final_match_level": 15,
        "match_duration": duration,
        "active_id_1": 23453,
        "active_id_2": 23454,
        "active_id_3": 23455,
        "active_id_4": 23456,
        "active_level_1": 1,
        "active_level_2": 1,
        "active_level_3": 0,
        "active_level_4": 0,
        "item_active_1": "Haven",
        "item_active_2": "Nimble",
        "item_active_3": "",
        "item_active_4": "",
        "item_id_1": 30000 + index,
        "item_id_2": 30010 + index,
        "item_id_3": 30020 + index,
        "item_id_4": 30030 + index,
        "item_id_5": 30040 + index,
        "item_id_6": 40000 + index,
        "item_level_1": 5,
        "item_level_2": 4,
        "item_level_3": 3,
        "item_level_4": 2,
        "item_level_5": 1,
        "item_level_6": 0,
        "item_purch_1": "Dummy Card 1",
        "item_purch_2": "Dummy Card 2",
        "item_purch_3": "Dummy Card 3",
        "item_purch_4": "Dummy Card 4",
        "item_purch_5": "Dummy Card 5",
        "item_purch_6": "Dummy Talent",
        "ban_id_1": 2288,
        "ban_id_2": 2493,
        "ban_id_3": 2362,
        "ban_id_4": 2314,
        "ban_id_5": 0,
        "ban_id_6": 0,
        "ban_id_7": 0,
        "ban_id_8": 0,
        "merged_players": null,
        "has_ret_msg": false
    })
}

fn dummy_raw_player(match_id: u64, index: usize) -> Value {
    let mut player = dummy_roster_player(match_id, index);
    let object = player
        .as_object_mut()
        .expect("dummy raw player must be an object");
    for profile_only in [
        "Id",
        "ActivePlayerId",
        "Name",
        "hz_player_name",
        "Level",
        "Platform",
        "RankedConquest",
    ] {
        object.remove(profile_only);
    }
    player
}

fn dummy_roster_player(match_id: u64, index: usize) -> Value {
    // getplayerbatchfrommatch in the TypeScript dummy provider returns the
    // untouched Hi-Rez-style player object, including both normalized fields
    // and legacy aliases, plus current-profile aliases. Preserve that exact
    // semantic JSON contract so parity tests can compare the whole result.
    let mut player = dummy_player(match_id, index, 486);
    let object = player
        .as_object_mut()
        .expect("dummy player must be a JSON object");
    let player_id = match_id * 100 + index as u64 + 1;
    let player_name = format!("DummyPlayer{}", index + 1);
    let duration = 900 + (match_id % 420);
    let deaths = 1 + ((match_id as usize + index) % 8);
    let kills = 2 + ((match_id as usize + index * 3) % 15);
    let assists = 4 + ((match_id as usize + index * 5) % 18);
    let gold = 1400 + ((match_id as usize + index * 97) % 2200);
    let task_force = if index < 5 { 1 } else { 2 };
    let region = if match_id.is_multiple_of(2) {
        "NA"
    } else {
        "EU"
    };
    let win_status = if task_force == 1 { "Winner" } else { "Loser" };
    let champion_id = object["champion_id"].clone();
    let champion_name = object["champion_name"].clone();
    let healing = if index == 3 || index == 8 {
        18000
    } else {
        1200
    };
    let damage_mitigated = if index.is_multiple_of(2) { 12000 } else { 2000 };
    object.insert(
        "gold_per_minute".to_owned(),
        json!((gold * 60 + duration as usize / 2) / duration as usize),
    );
    object.insert("portal_id".to_owned(), json!(5));
    object.insert(
        "portal_user_id".to_owned(),
        json!(format!("dummy-{match_id}-{index}")),
    );

    let aliases = json!({
        "Match": match_id,
        "Entry_Datetime": entry_datetime(match_id),
        "Map_Game": "LIVE Jaguar Falls",
        "match_queue_id": 486,
        "Match_Queue_Id": 486,
        "Match_Duration": duration,
        "Minutes": ((duration as f64) / 60.0).round() as u64,
        "Region": region,
        "Team1Score": 4,
        "Team2Score": match_id % 2,
        "Winning_TaskForce": 1,
        "hasReplay": "y",
        "playerName": player_name,
        "ChampionId": champion_id,
        "Champion": champion_name,
        "SkinId": 10000 + index,
        "Skin": "Default",
        "Kills_Player": kills,
        "Deaths": deaths,
        "Assists": assists,
        "Damage": 12000 + index * 600,
        "Damage_Done_In_Hand": 9000 + index * 500,
        "Damage_Mitigated": damage_mitigated,
        "Damage_Taken": 16000 + index * 800,
        "Damage_Taken_Physical": 12000 + index * 500,
        "Damage_Taken_Magical": 4000 + index * 300,
        "Healing": healing,
        "Healing_Player_Self": 800 + index * 10,
        "Gold_Earned": gold,
        "Objective_Assists": index % 4,
        "Killing_Spree": kills.saturating_sub(deaths),
        "Multi_kill_Max": index % 3,
        "Win_Status": win_status,
        "TaskForce": task_force,
        "League_Tier": 18 + (index % 5),
        "League_Points": 40 + index * 10,
        "Account_Level": 100 + index,
        "Mastery_Level": 20 + index,
        "PartyId": index % 5,
        "Time_In_Match_Seconds": duration,
        "Distance_Traveled": 120000 + index * 1000,
        "Structure_Damage": 0,
        "ActiveId1": 23453,
        "ActiveId2": 23454,
        "ActiveId3": 23455,
        "ActiveId4": 23456,
        "ActiveLevel1": 1,
        "ActiveLevel2": 1,
        "ActiveLevel3": 0,
        "ActiveLevel4": 0,
        "Item_Active_1": "Haven",
        "Item_Active_2": "Nimble",
        "Item_Active_3": "",
        "Item_Active_4": "",
        "ItemId1": 30000 + index,
        "ItemId2": 30010 + index,
        "ItemId3": 30020 + index,
        "ItemId4": 30030 + index,
        "ItemId5": 30040 + index,
        "ItemId6": 40000 + index,
        "ItemLevel1": 5,
        "ItemLevel2": 4,
        "ItemLevel3": 3,
        "ItemLevel4": 2,
        "ItemLevel5": 1,
        "ItemLevel6": 0,
        "Item_Purch_1": "Dummy Card 1",
        "Item_Purch_2": "Dummy Card 2",
        "Item_Purch_3": "Dummy Card 3",
        "Item_Purch_4": "Dummy Card 4",
        "Item_Purch_5": "Dummy Card 5",
        "Item_Purch_6": "Dummy Talent",
        "BanId1": 2288,
        "BanId2": 2493,
        "BanId3": 2362,
        "BanId4": 2314,
        "BanId5": 0,
        "BanId6": 0,
        "BanId7": 0,
        "BanId8": 0,
        "Id": player_id,
        "ActivePlayerId": player_id,
        "Name": player_name,
        "hz_player_name": player_name,
        "Level": 100 + index,
        "Platform": "Steam",
        "RankedConquest": {
            "Tier": 18 + (index % 5),
            "Points": 40 + index * 10
        },
        "ret_msg": null
    });
    object.extend(
        aliases
            .as_object()
            .expect("dummy aliases must be an object")
            .clone(),
    );
    player
}

fn dummy_history_player(match_id: u64, index: usize) -> Value {
    let player_id = match_id * 100 + index as u64 + 1;
    let mut player = dummy_player(match_id, index, 486);
    let object = player
        .as_object_mut()
        .expect("dummy player must be a JSON object");

    object.insert(
        "player_name".to_owned(),
        json!(format!("DummyPlayer{player_id}")),
    );
    object.insert("gold_per_minute".to_owned(), json!(0));
    object.insert("active_level_1".to_owned(), json!(0));
    object.insert("active_level_2".to_owned(), json!(0));
    object.insert("active_level_3".to_owned(), json!(0));
    object.insert("active_level_4".to_owned(), json!(0));
    for slot in 1..=4 {
        object.insert(format!("item_active_{slot}"), json!(""));
    }
    for slot in 1..=6 {
        object.insert(format!("item_purch_{slot}"), json!(""));
    }
    object.insert("time_in_match".to_owned(), json!(1000));
    object.insert("match_duration".to_owned(), json!(1000));
    object.insert("portal_id".to_owned(), json!(5));
    object.insert(
        "portal_user_id".to_owned(),
        json!(format!("dummy-{match_id}-{index}")),
    );
    object.insert("source".to_owned(), json!("recovered"));
    object.insert("map".to_owned(), json!("LIVE Jaguar Falls"));
    object.insert("history_team1_score".to_owned(), json!(4));
    object.insert("history_team2_score".to_owned(), json!(match_id % 2));
    object.insert("history_winning_task_force".to_owned(), json!(1));
    player
}

fn dummy_match_history(player_id: u64, limit: usize) -> Vec<Value> {
    let capped = limit.clamp(1, 50);
    let encoded_match_id = player_id / 100;
    let encoded_slot = player_id.saturating_sub(encoded_match_id * 100);
    let target_match_id = if encoded_match_id > 0 && (1..=10).contains(&encoded_slot) {
        encoded_match_id
    } else {
        player_id / 10
    };
    let target_slot = if (1..=10).contains(&encoded_slot) {
        encoded_slot.saturating_sub(1) as usize
    } else {
        0
    };
    (0..capped)
        .map(|index| {
            let match_id = target_match_id + index as u64;
            let slot = if index == 0 { target_slot } else { index % 10 };
            let mut player = dummy_raw_player(match_id, slot);
            let object = player
                .as_object_mut()
                .expect("dummy history player must be an object");
            object.insert("Match".to_owned(), json!(match_id));
            object.insert("Match_Time".to_owned(), json!(entry_datetime(match_id)));
            object.insert("match_duration".to_owned(), json!(1000));
            object.insert("time_in_match".to_owned(), json!(1000));
            object.insert("Match_Duration".to_owned(), json!(1000));
            object.insert("Time_In_Match_Seconds".to_owned(), json!(1000));
            object.insert("playerId".to_owned(), json!(player_id));
            object.insert(
                "playerName".to_owned(),
                json!(format!("DummyPlayer{player_id}")),
            );
            for (target, source) in [
                ("Kills", "kills"),
                ("Deaths", "deaths"),
                ("Assists", "assists"),
                ("Damage", "damage_done_physical"),
                ("Gold", "gold_earned"),
                ("Match_Queue_Id", "queue_id"),
            ] {
                object.insert(target.to_owned(), object[source].clone());
            }
            object.insert("Map_Game".to_owned(), json!("LIVE Jaguar Falls"));
            object.insert("ret_msg".to_owned(), Value::Null);
            player
        })
        .collect()
}

pub async fn dispatch_dummy_operation(
    provider: &DummyProvider,
    operation: &str,
    args: &[Value],
) -> Result<Value, RelayError> {
    let result = match operation {
        "getMatchDetailsBatch" => {
            let requests = parse_completed_match_requests(
                args.first()
                    .ok_or_else(|| RelayError::Validation("requests are required".to_owned()))?,
            )?;
            return serde_json::to_value(get_match_details_batch(provider, &requests).await?)
                .map_err(|error| RelayError::Upstream(error.to_string()));
        }
        "getMatchIdsByQueue" | "getMatchIdsByQueueDetails" => {
            provider.bump("getmatchidsbyqueue");
            let queue_id = number_arg(args, 0) as u64;
            let date = text_arg(args, 1);
            let hour = number_arg(args, 2) as u64;
            let suffix = queue_id.to_string();
            let seed = format!(
                "{}{}{:02}",
                &suffix[suffix.len().saturating_sub(2)..],
                date.get(date.len().saturating_sub(2)..).unwrap_or_default(),
                hour
            )
            .parse::<u64>()
            .unwrap_or_default();
            let ids: Vec<_> = (1..=6).map(|index| seed * 100 + index).collect();
            if operation == "getMatchIdsByQueue" {
                json!(ids)
            } else {
                Value::Array(
                    ids.into_iter()
                        .map(|match_id| {
                            json!({
                                "matchId": match_id,
                                "entryDatetime": entry_datetime(match_id),
                                "region": if match_id.is_multiple_of(2) { "NA" } else { "EU" },
                                "activeFlag": false
                            })
                        })
                        .collect(),
                )
            }
        }
        "getMatchDetailsBatchRaw" => {
            provider.bump("getmatchdetailsbatch");
            Value::Array(
                number_array_arg(args, 0)
                    .into_iter()
                    .flat_map(|match_id| {
                        (0..10).map(move |index| dummy_raw_player(match_id, index))
                    })
                    .collect(),
            )
        }
        "getMatchDetailsRaw" => {
            provider.bump("getmatchdetails");
            let match_id = number_arg(args, 0) as u64;
            Value::Array(
                (0..10)
                    .map(|index| dummy_raw_player(match_id, index))
                    .collect(),
            )
        }
        "getPlayerChampions" | "getChampionRanks" => {
            provider.bump(if operation == "getPlayerChampions" {
                "getplayerchampions"
            } else {
                "getchampionranks"
            });
            let player_id = number_arg(args, 0) as u64;
            Value::Array(
                CHAMPIONS
                    .iter()
                    .enumerate()
                    .map(|(index, (champion_id, champion_name))| {
                        if operation == "getPlayerChampions" {
                            json!({
                                "PlayerId": player_id,
                                "ChampionId": champion_id,
                                "Champion": champion_name,
                                "XP": 1000 + index,
                                "OwnershipType": "Purchased",
                                "ret_msg": null
                            })
                        } else {
                            json!({
                                "PlayerId": player_id,
                                "ChampionId": champion_id,
                                "Champion": champion_name,
                                "Worshippers": 1000 + index,
                                "Wins": 20 + index,
                                "Losses": 10 + index,
                                "Kills": 300 + index,
                                "Deaths": 100 + index,
                                "Assists": 200 + index,
                                "Minutes": 600 + index,
                                "ret_msg": null
                            })
                        }
                    })
                    .collect(),
            )
        }
        "getChampions" => {
            provider.bump("getchampions");
            Value::Array(
                CHAMPIONS
                    .iter()
                    .map(|(champion_id, champion_name)| {
                        json!({
                            "id": champion_id,
                            "Name": champion_name,
                            "Roles": "Paladins Champion",
                            "ret_msg": null
                        })
                    })
                    .collect(),
            )
        }
        "getItems" => {
            provider.bump("getitems");
            json!([{
                "ItemId": 30000,
                "DeviceName": "Dummy Item",
                "Description": "Synthetic item for relay parity tests",
                "ret_msg": null
            }])
        }
        "getEsportsProLeagueDetails" => {
            provider.bump("getesportsproleaguedetails");
            json!([{
                "LeagueId": 1,
                "LeagueName": "Dummy League",
                "ret_msg": null
            }])
        }
        "getPlayerLoadouts" => {
            provider.bump("getplayerloadouts");
            json!([{
                "DeckId": 1,
                "playerId": number_arg(args, 0) as u64,
                "ChampionId": CHAMPIONS[0].0,
                "LoadoutItems": [],
                "ret_msg": null
            }])
        }
        "getPlayerStatus" => {
            provider.bump("getplayerstatus");
            json!([{
                "player_id": number_arg(args, 0) as u64,
                "status": 3,
                "status_string": "In Game",
                "Match": 990000001,
                "match_queue_id": 486,
                "privacy_flag": "n",
                "ret_msg": null
            }])
        }
        "getMatchPlayerDetails" => {
            provider.bump("getmatchplayerdetails");
            let match_id = number_arg(args, 0) as u64;
            Value::Array(
                (0..10)
                    .map(|index| {
                        let mut player = dummy_raw_player(match_id, index);
                        let object = player.as_object_mut().expect("dummy match player details");
                        object.insert("playerId".to_owned(), object["player_id"].clone());
                        object.insert("playerName".to_owned(), object["player_name"].clone());
                        object.insert("playerPortalId".to_owned(), object["portal_id"].clone());
                        object.insert("ChampionName".to_owned(), object["champion_name"].clone());
                        object.insert("Tier".to_owned(), object["league_tier"].clone());
                        object.insert("tierWins".to_owned(), object["league_wins"].clone());
                        object.insert("tierLosses".to_owned(), object["league_losses"].clone());
                        object.insert("taskForce".to_owned(), json!(if index < 5 { 1 } else { 2 }));
                        object.insert("mapGame".to_owned(), json!("LIVE Jaguar Falls"));
                        object.insert("playerRegion".to_owned(), object["region"].clone());
                        object.insert("ret_msg".to_owned(), Value::Null);
                        player
                    })
                    .collect(),
            )
        }
        "getLeagueLeaderboard" | "getMatchLeaderboard" => {
            provider.bump(if operation == "getLeagueLeaderboard" {
                "getleagueleaderboard"
            } else {
                "getmatchleaderboard"
            });
            json!([{
                "player_id": 900001,
                "name": "DummyPlayer1",
                "tier": number_arg(args, 1) as i64,
                "rank": 1,
                "points": 100,
                "ret_msg": null
            }])
        }
        "getLeagueSeasons" => {
            provider.bump("getleagueseasons");
            json!([{"id": 12, "name": "Dummy Season", "ret_msg": null}])
        }
        "getPlayerBatchFromMatch" => {
            provider.bump("getplayerbatchfrommatch");
            let match_id = number_arg(args, 0) as u64;
            Value::Array(
                (0..10)
                    .map(|index| {
                        let player = dummy_player(match_id, index, 486);
                        json!({
                            "Id": player_number(&player, &["player_id"]),
                            "Name": player["player_name"],
                            "Platform": player["platform"],
                            "ret_msg": null
                        })
                    })
                    .collect(),
            )
        }
        "getDemoDetails" => {
            provider.bump("getdemodetails");
            let match_id = number_arg(args, 0) as u64;
            json!({
                "Match": match_id,
                "Queue": "486",
                "Entry_Datetime": entry_datetime(match_id),
                "Match_Time": 1000,
                "Team1_Score": 4,
                "Team2_Score": 1,
                "Winning_Team": 1,
                "ret_msg": null
            })
        }
        "getPlayerBatch" | "getPlayerBatchLookup" => {
            provider.bump("getplayerbatch");
            Value::Array(
                number_array_arg(args, 0)
                    .into_iter()
                    .enumerate()
                    .map(|(index, player_id)| dummy_profile(player_id, index))
                    .collect(),
            )
        }
        "getMatchHistory" => {
            provider.bump("getmatchhistory");
            let player_id = number_arg(args, 0) as u64;
            let limit = args
                .get(1)
                .map_or(50, |value| value.as_f64().unwrap_or(50.0) as usize);
            Value::Array(
                dummy_match_history(player_id, limit)
                    .iter()
                    .map(normalize_match_history_player)
                    .collect(),
            )
        }
        "getPlayers" => {
            provider.bump("getplayers");
            Value::Array(
                args.first()
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .enumerate()
                    .map(|(index, name)| {
                        json!({
                            "Id": 900000 + index,
                            "Name": name.as_str().unwrap_or_default(),
                            "Level": 100 + index,
                            "Platform": "Steam",
                            "Region": "NA",
                            "ret_msg": null
                        })
                    })
                    .collect(),
            )
        }
        "getPlayerIdByName" | "searchPlayers" => {
            provider.bump(if operation == "getPlayerIdByName" {
                "getplayeridbyname"
            } else {
                "searchplayers"
            });
            let name = text_arg(args, 0);
            let mut row = json!({
                "player_id": 900000,
                "Id": 900000,
                "Name": name,
                "hz_player_name": name,
                "Platform": "Steam",
                "portal_id": 1,
                "ret_msg": null
            });
            if operation == "getPlayerIdByName" {
                row["ActivePlayerId"] = json!(900000);
            } else {
                row["privacy_flag"] = json!("n");
            }
            json!([row])
        }
        "getPlayerIdsByGamerTag" => {
            provider.bump("getplayeridsbygamertag");
            json!([{
                "player_id": 900001,
                "Id": 900001,
                "Name": text_arg(args, 1),
                "hz_gamer_tag": text_arg(args, 1),
                "portal_id": number_arg(args, 0),
                "ret_msg": null
            }])
        }
        "getPlayerIdByPortalUserId" => {
            provider.bump("getplayeridbyportaluserid");
            json!([{
                "player_id": 900002,
                "Id": 900002,
                "ActivePlayerId": 900002,
                "portal_user_id": text_arg(args, 1),
                "portal_id": number_arg(args, 0),
                "ret_msg": null
            }])
        }
        "getDataUsed" => {
            provider.bump("getdataused");
            json!({
                "Active_Sessions": 0,
                "Concurrent_Sessions": 50,
                "Request_Limit_Daily": 10000,
                "Session_Cap": 0,
                "Session_Time_Limit": 15,
                "Total_Requests_Today": 0,
                "Total_Sessions_Today": 0,
                "dummy": true
            })
        }
        "syncApiKeyUsage" | "reloadApiKeyPool" => Value::Bool(true),
        "getApiKeyStatus" => Value::Array(Vec::new()),
        "dumpRawPayloads" => json!(args.first().and_then(Value::as_array).map_or(0, Vec::len)),
        "cleanupFetchedPlayersCache" | "clearMatchHistoryCache" => Value::Bool(true),
        "setDummyMatchScenario" => {
            let match_id = number_arg(args, 0) as u64;
            let scenario = serde_json::from_value(args.get(1).cloned().unwrap_or(Value::Null))
                .map_err(|error| {
                    RelayError::Validation(format!("unsupported dummy match scenario: {error}"))
                })?;
            provider.set_scenario(match_id, scenario).await;
            Value::Bool(true)
        }
        "resetDummyMatchScenarios" => {
            provider.reset_scenarios().await;
            Value::Bool(true)
        }
        "resetDummyApiCallCounts" => {
            provider.reset_counts();
            json!(provider.counts())
        }
        "getDummyApiCallCounts" => json!(provider.counts()),
        _ => {
            return Err(RelayError::Unsupported(format!(
                "Unsupported dummy HirezRelay operation: {operation}"
            )));
        }
    };
    Ok(result)
}

fn dummy_profile(player_id: u64, index: usize) -> Value {
    json!({
        "Id": player_id,
        "ActivePlayerId": player_id,
        "Name": format!("DummyPlayer{player_id}"),
        "hz_player_name": format!("DummyPlayer{player_id}"),
        "Level": 100 + index,
        "Platform": "Steam",
        "Region": "NA",
        "ret_msg": null
    })
}

fn number_arg(args: &[Value], index: usize) -> f64 {
    args.get(index).and_then(Value::as_f64).unwrap_or_default()
}

fn number_array_arg(args: &[Value], index: usize) -> Vec<u64> {
    args.get(index)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_f64)
        .map(|value| value as u64)
        .collect()
}

fn text_arg(args: &[Value], index: usize) -> &str {
    args.get(index).and_then(Value::as_str).unwrap_or_default()
}

fn entry_datetime(match_id: u64) -> String {
    let elapsed = match_id % 100_000;
    let day = 1 + elapsed / 86_400;
    let within_day = elapsed % 86_400;
    format!(
        "2026-01-{day:02}T{:02}:{:02}:{:02}.000Z",
        within_day / 3_600,
        (within_day / 60) % 60,
        within_day % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dummy_call_counts_match_typescript_sparse_reset_semantics() {
        let provider = DummyProvider::default();
        assert!(provider.counts().is_empty());

        provider
            .get_match_details_batch(&[128_000_000])
            .await
            .expect("dummy batch");
        assert_eq!(
            provider.counts(),
            BTreeMap::from([("getmatchdetailsbatch", 1)])
        );

        provider.reset_counts();
        assert!(provider.counts().is_empty());
    }
}
