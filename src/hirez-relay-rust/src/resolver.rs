use std::collections::{HashMap, HashSet};

use futures::{StreamExt, future::join_all, stream};
use serde_json::Value;

use crate::{
    model::{
        CompletedMatchRequest, CompletedMatchResolution, CompletedMatchResolutionStatus,
        MatchDetails, player_number, sort_players, usable_player_count, usable_players,
    },
    provider::{CompletedMatchProvider, RelayError},
};

const RANKED_QUEUE_ID: u32 = 486;

pub async fn get_match_details_batch<P: CompletedMatchProvider>(
    provider: &P,
    requests: &[CompletedMatchRequest],
) -> Result<Vec<CompletedMatchResolution>, RelayError> {
    validate_requests(requests)?;

    let ids: Vec<_> = requests.iter().map(|request| request.match_id).collect();
    let direct_matches = match provider.get_match_details_batch(&ids).await {
        Ok(matches) => matches,
        Err(error) if requests.len() == 1 && is_recoverable_match_detail_error(&error) => {
            Vec::new()
        }
        Err(error) => return Err(error),
    };
    let mut direct_by_id: HashMap<_, _> = direct_matches
        .into_iter()
        .map(|r#match| (r#match.match_id, r#match))
        .collect();

    let mut work = Vec::new();
    for request in requests {
        let direct = direct_by_id.remove(&request.match_id);
        // Multi-match omissions are deliberately absent from the relay
        // response. The worker isolates the first missing ordered ID through
        // this same operation and refills its continuous batch.
        if direct.is_some() || requests.len() == 1 {
            work.push((request.clone(), direct));
        }
    }

    let outcomes = join_all(work.into_iter().map(|(mut request, direct)| async move {
        // Discovery's queue hint controls recovery scope. A malformed partial
        // row must not promote known casual/PvE work into ranked history/demo
        // fan-out. Exact-ID requests have no hint and may inherit the row.
        if request.queue_id.is_none()
            && let Some(r#match) = direct.as_ref()
            && r#match.queue_id > 0
        {
            request.queue_id = Some(r#match.queue_id);
        }
        if let Some(r#match) = direct.as_ref()
            && !needs_recovery(r#match)
        {
            return Ok(terminalize(
                &request,
                r#match.clone(),
                CompletedMatchResolutionStatus::CompleteDirect,
                None,
            ));
        }
        if request
            .queue_id
            .is_none_or(|queue_id| queue_id == RANKED_QUEUE_ID)
        {
            recover_ranked(provider, &request, direct).await
        } else {
            recover_presence(provider, &request, direct).await
        }
    }))
    .await;

    outcomes.into_iter().collect()
}

fn validate_requests(requests: &[CompletedMatchRequest]) -> Result<(), RelayError> {
    if requests.is_empty() || requests.len() > 10 {
        return Err(RelayError::Validation(
            "requests must contain between 1 and 10 matches".to_owned(),
        ));
    }
    let mut seen = HashSet::with_capacity(requests.len());
    for request in requests {
        if request.match_id == 0 {
            return Err(RelayError::Validation(
                "matchId must be a positive integer".to_owned(),
            ));
        }
        if !seen.insert(request.match_id) {
            return Err(RelayError::Validation(format!(
                "requests contains duplicate matchId {}",
                request.match_id
            )));
        }
        if request.queue_id == Some(0) {
            return Err(RelayError::Validation(
                "queueId must be a positive integer".to_owned(),
            ));
        }
    }
    Ok(())
}

async fn recover_ranked<P: CompletedMatchProvider>(
    provider: &P,
    request: &CompletedMatchRequest,
    direct: Option<MatchDetails>,
) -> Result<CompletedMatchResolution, RelayError> {
    let direct_players = direct
        .as_ref()
        .map(|r#match| usable_players(&r#match.players))
        .unwrap_or_default();
    let direct_ids: HashSet<_> = direct_players
        .iter()
        .map(|player| player_number(player, &["player_id"]))
        .filter(|player_id| *player_id > 0)
        .collect();
    let known_private_count = direct_players
        .iter()
        .filter(|player| player_number(player, &["player_id"]) == 0)
        .count();

    if known_private_count == 0 {
        let local_players = provider
            .get_local_recovery_players(request.match_id)
            .await?;
        let local_missing: Vec<_> = local_players
            .into_iter()
            .filter(|player| {
                let player_id = player_number(player, &["player_id"]);
                player_id > 0 && !direct_ids.contains(&player_id)
            })
            .collect();
        let locally_merged = merge_players(&direct_players, &local_missing);
        if locally_merged.len() == 10
            && let Some(mut shell) = direct.clone()
            && shell.queue_id > 0
            && !shell.entry_datetime.is_empty()
            && let Some(score) =
                resolve_direct_score(&shell).or_else(|| resolve_history_score(&local_missing))
        {
            shell.players = locally_merged;
            shell.team1_score = Some(score.team1);
            shell.team2_score = Some(score.team2);
            shell.winning_task_force = Some(score.winner);
            shell.recovery_source = Some("local_preflight".to_owned());
            shell.recovery_api_calls = Some(0);
            shell.recovery_attempted = Some(true);
            shell.recovery_terminal = Some(false);
            shell.recovery_pending = Some(false);
            shell.limited = Some(false);
            attach_recovery_bans(&mut shell);
            return Ok(terminalize(
                request,
                shell,
                CompletedMatchResolutionStatus::CompleteRecovered,
                None,
            ));
        }
    }

    let roster = match provider.get_player_batch_from_match(request.match_id).await {
        Ok(roster) => roster
            .into_iter()
            .filter(|player| {
                player
                    .get("ret_msg")
                    .and_then(Value::as_str)
                    .is_none_or(|message| message.trim().is_empty())
            })
            .collect::<Vec<_>>(),
        Err(_) => {
            return Ok(match direct {
                Some(mut partial) if !direct_players.is_empty() => {
                    partial.players = direct_players;
                    partial.recovery_source = Some("getplayerbatchfrommatch_failed".to_owned());
                    partial.recovery_api_calls = Some(1);
                    partial.recovery_attempted = Some(true);
                    partial.recovery_terminal = Some(false);
                    partial.recovery_pending = Some(false);
                    partial.limited = Some(true);
                    attach_recovery_bans(&mut partial);
                    terminalize(
                        request,
                        partial,
                        CompletedMatchResolutionStatus::Limited,
                        Some("getplayerbatchfrommatch_failed".to_owned()),
                    )
                }
                _ => dropped(
                    request,
                    "relay recovery returned no authoritative match or roster facts",
                ),
            });
        }
    };

    let roster_ids: Vec<_> = roster
        .iter()
        .map(|player| player_number(player, &["ActivePlayerId", "playerId", "player_id", "Id"]))
        .filter(|player_id| *player_id > 0)
        .collect();

    if roster_ids.is_empty() {
        return Ok(match direct {
            Some(mut partial) if !direct_players.is_empty() => {
                partial.players = direct_players;
                partial.recovery_source = Some("no_player_anchors".to_owned());
                partial.recovery_api_calls = Some(1);
                partial.recovery_attempted = Some(true);
                partial.recovery_terminal = Some(true);
                partial.recovery_pending = Some(false);
                partial.limited = Some(true);
                attach_recovery_bans(&mut partial);
                terminalize(
                    request,
                    partial,
                    CompletedMatchResolutionStatus::Limited,
                    Some("no player anchors".to_owned()),
                )
            }
            _ => dropped(
                request,
                "relay recovery returned no authoritative match or roster facts",
            ),
        });
    }

    let missing_ids: Vec<_> = roster_ids
        .into_iter()
        .filter(|player_id| !direct_ids.contains(player_id))
        .collect();
    let history_results = stream::iter(missing_ids.iter().copied())
        .map(|player_id| async move {
            provider
                .get_match_history_with_usage(player_id, request.match_id)
                .await
        })
        .buffered(5)
        .collect::<Vec<_>>()
        .await;

    let mut recovered = Vec::new();
    let mut unresolved = 0usize;
    let mut history_api_calls = 0u32;
    for history in history_results {
        match history {
            Ok((rows, api_calls)) => {
                history_api_calls = history_api_calls.saturating_add(api_calls);
                let target = rows.into_iter().find(|row| {
                    player_number(row, &["Match", "match_id", "MatchId"]) == request.match_id
                });
                if let Some(player) = target {
                    recovered.push(player);
                } else {
                    unresolved += 1;
                }
            }
            Err(_) => {
                history_api_calls = history_api_calls.saturating_add(1);
                unresolved += 1;
            }
        }
    }

    enrich_recovered_players(&mut recovered, &roster);
    let mut players = merge_players(&direct_players, &recovered);
    let mut recovery_api_calls = 1u32.saturating_add(history_api_calls);
    let direct_score = direct.as_ref().and_then(resolve_direct_score);
    let history_score = resolve_history_score(&recovered);
    let resolved_score = direct_score.or(history_score);
    let mut shell = match direct {
        Some(r#match) => r#match,
        None => {
            recovery_api_calls = recovery_api_calls.saturating_add(1);
            let demo = provider
                .get_demo_details(request.match_id)
                .await
                .unwrap_or(Value::Null);
            shell_from_demo(request, demo).await
        }
    };

    if unresolved > 0 || players.len() < 10 || resolved_score.is_none() {
        shell.players = players;
        shell.recovery_source = Some("target_history_unresolved".to_owned());
        shell.recovery_api_calls = Some(recovery_api_calls);
        shell.recovery_attempted = Some(true);
        shell.recovery_terminal = Some(false);
        shell.recovery_pending = Some(true);
        shell.limited = Some(false);
        attach_recovery_bans(&mut shell);
        return Ok(terminalize(
            request,
            shell,
            CompletedMatchResolutionStatus::RecoveryPending,
            Some("target_history_unresolved".to_owned()),
        ));
    }

    sort_players(&mut players);
    shell.players = players;
    shell.recovery_source = Some(if direct_score.is_some() {
        "broken_match".to_owned()
    } else {
        "history".to_owned()
    });
    shell.recovery_api_calls = Some(recovery_api_calls);
    shell.recovery_attempted = Some(true);
    shell.recovery_terminal = Some(false);
    shell.recovery_pending = Some(false);
    shell.limited = Some(false);
    attach_recovery_bans(&mut shell);
    if let Some(score) = resolved_score {
        shell.team1_score = Some(score.team1);
        shell.team2_score = Some(score.team2);
        shell.winning_task_force = Some(score.winner);
    }
    Ok(terminalize(
        request,
        shell,
        CompletedMatchResolutionStatus::CompleteRecovered,
        None,
    ))
}

fn enrich_recovered_players(recovered: &mut [Value], roster: &[Value]) {
    let profiles: HashMap<_, _> = roster
        .iter()
        .filter_map(|profile| {
            let player_id =
                player_number(profile, &["ActivePlayerId", "playerId", "player_id", "Id"]);
            (player_id > 0).then_some((player_id, profile))
        })
        .collect();

    for player in recovered {
        let player_id = player_number(player, &["player_id", "playerId", "Id"]);
        let Some(profile) = profiles.get(&player_id) else {
            continue;
        };
        let Some(object) = player.as_object_mut() else {
            continue;
        };
        let platform_missing = object
            .get("platform")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty);
        if platform_missing && let Some(platform) = profile.get("Platform").and_then(Value::as_str)
        {
            object.insert("platform".to_owned(), Value::String(platform.to_owned()));
        }
        let account_level = object
            .get("account_level")
            .and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            })
            .unwrap_or_default();
        if account_level == 0
            && let Some(level) = profile
                .get("Level")
                .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        {
            object.insert("account_level".to_owned(), Value::from(level));
        }
    }
}

async fn shell_from_demo(request: &CompletedMatchRequest, demo: Value) -> MatchDetails {
    MatchDetails {
        match_id: request.match_id,
        entry_datetime: string_field(&demo, &["Entry_Datetime", "entry_datetime"]),
        map: string_field(&demo, &["Map_Game", "map"]),
        queue_id: number_field(&demo, &["Queue", "match_queue_id", "Queue_Id"])
            .and_then(|value| u32::try_from(value).ok())
            .or(request.queue_id)
            .unwrap_or_default(),
        duration_seconds: number_field(
            &demo,
            &["Match_Time", "Match_Duration", "duration_seconds"],
        )
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or_default(),
        minutes: number_field(&demo, &["Minutes", "minutes"])
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or_default(),
        region: {
            let region = string_field(&demo, &["Region", "region"]);
            if region.is_empty() {
                "Unknown".to_owned()
            } else {
                region
            }
        },
        has_replay: Some(
            demo.get("hasReplay")
                .or_else(|| demo.get("has_replay"))
                .is_some_and(|value| {
                    value.as_bool() == Some(true)
                        || value
                            .as_str()
                            .is_some_and(|text| text.eq_ignore_ascii_case("y"))
                }),
        ),
        ..MatchDetails::default()
    }
}

async fn recover_presence<P: CompletedMatchProvider>(
    provider: &P,
    request: &CompletedMatchRequest,
    direct: Option<MatchDetails>,
) -> Result<CompletedMatchResolution, RelayError> {
    if let Some(r#match) = direct.as_ref()
        && !needs_recovery(r#match)
    {
        return Ok(terminalize(
            request,
            r#match.clone(),
            CompletedMatchResolutionStatus::CompleteDirect,
            None,
        ));
    }

    let roster_result = provider.get_player_batch_from_match(request.match_id).await;
    let direct_players = direct
        .as_ref()
        .map(|r#match| usable_players(&r#match.players))
        .unwrap_or_default();
    match (direct, roster_result) {
        (Some(mut partial), Ok(roster)) if !direct_players.is_empty() => {
            partial.players = direct_players;
            partial.recovery_attempted = Some(true);
            partial.recovery_terminal = Some(true);
            partial.recovery_pending = Some(false);
            partial.limited = Some(true);
            let usable_roster = usable_roster(roster);
            Ok(CompletedMatchResolution {
                match_id: request.match_id,
                queue_id: partial.queue_id.max(request.queue_id.unwrap_or_default()),
                status: CompletedMatchResolutionStatus::Limited,
                r#match: Some(partial),
                roster: (!usable_roster.is_empty()).then_some(usable_roster),
                reason: Some("presence roster recovered from incomplete detail".to_owned()),
            })
        }
        (Some(mut partial), Err(error)) if !direct_players.is_empty() => {
            partial.players = direct_players;
            partial.recovery_attempted = Some(true);
            partial.recovery_terminal = Some(true);
            partial.recovery_pending = Some(false);
            partial.limited = Some(true);
            Ok(terminalize(
                request,
                partial,
                CompletedMatchResolutionStatus::Limited,
                Some(format!(
                    "presence detail retained without roster anchors: {error}"
                )),
            ))
        }
        (_, Ok(roster)) => {
            let usable_roster = usable_roster(roster);
            if usable_roster.is_empty() {
                Ok(dropped(
                    request,
                    "single relay pass returned no match or roster facts",
                ))
            } else {
                Ok(CompletedMatchResolution {
                    match_id: request.match_id,
                    queue_id: request.queue_id.unwrap_or_default(),
                    status: CompletedMatchResolutionStatus::RosterOnly,
                    r#match: None,
                    roster: Some(usable_roster),
                    reason: Some("presence roster recovered without match detail".to_owned()),
                })
            }
        }
        (_, Err(error)) => Ok(dropped(
            request,
            format!("single relay pass returned no match or roster facts: {error}"),
        )),
    }
}

fn usable_roster(roster: Vec<Value>) -> Vec<Value> {
    roster
        .into_iter()
        .filter(|player| {
            player
                .get("ret_msg")
                .and_then(Value::as_str)
                .is_none_or(|message| message.trim().is_empty())
        })
        .collect()
}

fn needs_recovery(r#match: &MatchDetails) -> bool {
    let count = usable_player_count(&r#match.players);
    if is_variable_human_roster_queue(r#match.queue_id) {
        count == 0
    } else {
        count != 10
    }
}

fn is_variable_human_roster_queue(queue_id: u32) -> bool {
    matches!(queue_id, 425 | 453 | 10297 | 10348 | 10362)
}

fn is_recoverable_match_detail_error(error: &RelayError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("hirez_unknown_return")
        || message.contains("int16")
        || message.contains("skin_id")
        || message.contains("skin id")
        || message.contains("too large")
        || message.contains("too small")
}

fn merge_players(direct: &[Value], recovered: &[Value]) -> Vec<Value> {
    let mut merged = Vec::with_capacity(direct.len() + recovered.len());
    let mut public_ids = HashSet::new();
    let target_private = direct
        .iter()
        .filter(|player| player_number(player, &["player_id"]) == 0)
        .count()
        .max(
            recovered
                .iter()
                .filter(|player| player_number(player, &["player_id"]) == 0)
                .count(),
        );
    let mut private_count = 0usize;

    for player in direct.iter().chain(recovered) {
        let player_id = player_number(player, &["player_id"]);
        if player_id > 0 {
            if public_ids.insert(player_id) {
                merged.push(player.clone());
            }
        } else if private_count < target_private {
            private_count += 1;
            merged.push(player.clone());
        }
    }
    merged
}

#[derive(Clone, Copy)]
struct ResolvedScore {
    team1: i32,
    team2: i32,
    winner: i32,
}

fn resolve_direct_score(r#match: &MatchDetails) -> Option<ResolvedScore> {
    let observations = r#match.direct_score_observations.as_deref()?;
    resolve_observed_score(
        observations,
        &["team1", "team1_score", "Team1Score", "Team1_Score"],
        &["team2", "team2_score", "Team2Score", "Team2_Score"],
        &[
            "winner",
            "winning_task_force",
            "Winning_TaskForce",
            "Winning_Task_Force",
        ],
    )
}

fn resolve_history_score(players: &[Value]) -> Option<ResolvedScore> {
    resolve_observed_score(
        players,
        &["history_team1_score"],
        &["history_team2_score"],
        &["history_winning_task_force"],
    )
}

fn resolve_observed_score(
    observations: &[Value],
    team1_keys: &[&str],
    team2_keys: &[&str],
    winner_keys: &[&str],
) -> Option<ResolvedScore> {
    let score_bearing: Vec<_> = observations
        .iter()
        .filter(|observation| {
            team1_keys
                .iter()
                .chain(team2_keys)
                .chain(winner_keys)
                .any(|key| {
                    observation.get(*key).is_some_and(|value| {
                        !value.is_null() && !value.as_str().is_some_and(str::is_empty)
                    })
                })
        })
        .collect();
    if score_bearing.len() < 2 {
        return None;
    }

    let mut resolved: Option<ResolvedScore> = None;
    for observation in score_bearing {
        let team1 = signed_number_field(observation, team1_keys)?;
        let team2 = signed_number_field(observation, team2_keys)?;
        let winner = signed_number_field(observation, winner_keys)?;
        if !valid_completed_score(team1, team2, winner) {
            return None;
        }
        let candidate = ResolvedScore {
            team1,
            team2,
            winner,
        };
        if let Some(previous) = resolved
            && (previous.team1 != team1 || previous.team2 != team2 || previous.winner != winner)
        {
            return None;
        }
        resolved = Some(candidate);
    }
    resolved
}

fn signed_number_field(value: &Value, keys: &[&str]) -> Option<i32> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        value
            .as_i64()
            .and_then(|number| i32::try_from(number).ok())
            .or_else(|| value.as_u64().and_then(|number| i32::try_from(number).ok()))
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn valid_completed_score(team1: i32, team2: i32, winner: i32) -> bool {
    team1 >= 0
        && team2 >= 0
        && matches!(winner, 1 | 2)
        && if winner == 1 {
            team1 > team2
        } else {
            team2 > team1
        }
}

fn attach_recovery_bans(r#match: &mut MatchDetails) {
    let Some(player) = r#match.players.first() else {
        return;
    };
    for slot in 1..=8 {
        let key = format!("ban_id_{slot}");
        let value = number_field(player, &[&key]).unwrap_or_default();
        r#match.extra.insert(key, Value::from(value));
    }
}

fn terminalize(
    request: &CompletedMatchRequest,
    mut r#match: MatchDetails,
    status: CompletedMatchResolutionStatus,
    reason: Option<String>,
) -> CompletedMatchResolution {
    sort_players(&mut r#match.players);
    CompletedMatchResolution {
        match_id: request.match_id,
        queue_id: r#match.queue_id.max(request.queue_id.unwrap_or_default()),
        status,
        r#match: Some(r#match),
        roster: None,
        reason,
    }
}

fn dropped(request: &CompletedMatchRequest, reason: impl Into<String>) -> CompletedMatchResolution {
    CompletedMatchResolution {
        match_id: request.match_id,
        queue_id: request.queue_id.unwrap_or_default(),
        status: CompletedMatchResolutionStatus::Dropped,
        r#match: None,
        roster: None,
        reason: Some(reason.into()),
    }
}

fn number_field(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn string_field(value: &Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .unwrap_or_default()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dummy::{DummyProvider, DummyScenario};
    use std::collections::BTreeMap;

    fn request(match_id: u64, queue_id: u32) -> CompletedMatchRequest {
        CompletedMatchRequest {
            match_id,
            queue_id: Some(queue_id),
        }
    }

    fn call_count(counts: &BTreeMap<&'static str, u64>, endpoint: &str) -> u64 {
        counts.get(endpoint).copied().unwrap_or(0)
    }

    #[test]
    fn recovered_scores_require_two_coherent_rows() {
        assert!(
            resolve_history_score(&[serde_json::json!({
                "history_team1_score": 4,
                "history_team2_score": 1,
                "history_winning_task_force": 1
            })])
            .is_none()
        );

        let coherent = resolve_history_score(&[
            serde_json::json!({
                "history_team1_score": 4,
                "history_team2_score": 1,
                "history_winning_task_force": 1
            }),
            serde_json::json!({
                "history_team1_score": "4",
                "history_team2_score": "1",
                "history_winning_task_force": "1"
            }),
        ])
        .expect("coherent repeated history score");
        assert_eq!((coherent.team1, coherent.team2, coherent.winner), (4, 1, 1));

        assert!(
            resolve_history_score(&[
                serde_json::json!({
                    "history_team1_score": 4,
                    "history_team2_score": 1,
                    "history_winning_task_force": 1
                }),
                serde_json::json!({
                    "history_team1_score": 1,
                    "history_team2_score": 4,
                    "history_winning_task_force": 2
                }),
            ])
            .is_none()
        );
    }

    #[tokio::test]
    async fn direct_batch_is_zero_recovery() {
        let provider = DummyProvider::default();
        let requests = (1..=10)
            .map(|id| request(900_000_000 + id, 486))
            .collect::<Vec<_>>();
        let outcomes = get_match_details_batch(&provider, &requests).await.unwrap();
        assert!(outcomes.iter().all(|outcome| matches!(
            outcome.status,
            CompletedMatchResolutionStatus::CompleteDirect
        )));
        let counts = provider.counts();
        assert_eq!(call_count(&counts, "getmatchdetailsbatch"), 1);
        assert_eq!(call_count(&counts, "getplayerbatchfrommatch"), 0);
        assert_eq!(call_count(&counts, "getmatchhistory"), 0);
    }

    #[tokio::test]
    async fn complete_local_preflight_spends_no_recovery_calls() {
        let provider = DummyProvider::default();
        let request = request(905_000_001, 486);
        provider
            .set_scenario(request.match_id, DummyScenario::LocalPreflight)
            .await;
        let outcomes = get_match_details_batch(&provider, &[request])
            .await
            .unwrap();
        assert!(matches!(
            outcomes[0].status,
            CompletedMatchResolutionStatus::CompleteRecovered
        ));
        let r#match = outcomes[0].r#match.as_ref().unwrap();
        assert_eq!(r#match.players.len(), 10);
        assert_eq!(r#match.recovery_source.as_deref(), Some("local_preflight"));
        assert_eq!(r#match.recovery_api_calls, Some(0));
        let counts = provider.counts();
        assert_eq!(call_count(&counts, "getmatchdetailsbatch"), 1);
        assert_eq!(call_count(&counts, "getplayerbatchfrommatch"), 0);
        assert_eq!(call_count(&counts, "getmatchhistory"), 0);
        assert_eq!(call_count(&counts, "getdemodetails"), 0);
    }

    #[tokio::test]
    async fn omitted_blocker_is_returned_to_worker_for_continuous_batching() {
        let provider = DummyProvider::default();
        let requests = (1..=10)
            .map(|id| request(910_000_000 + id, 486))
            .collect::<Vec<_>>();
        provider
            .set_scenario(requests[4].match_id, DummyScenario::OmitFromMulti)
            .await;
        let outcomes = get_match_details_batch(&provider, &requests).await.unwrap();
        assert_eq!(outcomes.len(), 9);
        assert!(
            !outcomes
                .iter()
                .any(|outcome| outcome.match_id == requests[4].match_id)
        );
        assert!(outcomes.iter().all(|outcome| matches!(
            outcome.status,
            CompletedMatchResolutionStatus::CompleteDirect
        )));
        assert_eq!(call_count(&provider.counts(), "getmatchdetailsbatch"), 1);
    }

    #[tokio::test]
    async fn ranked_skin_overflow_recovers_three_histories() {
        let provider = DummyProvider::default();
        let request = request(920_000_001, 486);
        provider
            .set_scenario(request.match_id, DummyScenario::BrokenSkin)
            .await;
        let outcomes = get_match_details_batch(&provider, &[request])
            .await
            .unwrap();
        assert!(matches!(
            outcomes[0].status,
            CompletedMatchResolutionStatus::CompleteRecovered
        ));
        assert_eq!(outcomes[0].r#match.as_ref().unwrap().players.len(), 10);
        let counts = provider.counts();
        assert_eq!(call_count(&counts, "getmatchdetailsbatch"), 1);
        assert_eq!(call_count(&counts, "getplayerbatchfrommatch"), 1);
        assert_eq!(call_count(&counts, "getmatchhistory"), 3);
        assert_eq!(call_count(&counts, "getdemodetails"), 0);
    }

    #[tokio::test]
    async fn missing_target_history_remains_retryable_recovery_pending() {
        let provider = DummyProvider::default();
        let request = request(925_000_001, 486);
        provider
            .set_scenario(request.match_id, DummyScenario::HistoryMissing)
            .await;
        let outcomes = get_match_details_batch(&provider, &[request])
            .await
            .unwrap();
        assert!(matches!(
            outcomes[0].status,
            CompletedMatchResolutionStatus::RecoveryPending
        ));
        let r#match = outcomes[0].r#match.as_ref().unwrap();
        assert_eq!(r#match.players.len(), 7);
        assert_eq!(r#match.recovery_pending, Some(true));
        assert_eq!(r#match.recovery_terminal, Some(false));
    }

    #[tokio::test]
    async fn ranked_roster_failure_retains_direct_prefix() {
        let provider = DummyProvider::default();
        let request = request(926_000_001, 486);
        provider
            .set_scenario(request.match_id, DummyScenario::RosterFailure)
            .await;
        let outcomes = get_match_details_batch(&provider, &[request])
            .await
            .unwrap();
        assert!(matches!(
            outcomes[0].status,
            CompletedMatchResolutionStatus::Limited
        ));
        assert_eq!(outcomes[0].r#match.as_ref().unwrap().players.len(), 7);
        assert_eq!(call_count(&provider.counts(), "getmatchhistory"), 0);
    }

    #[tokio::test]
    async fn no_roster_anchors_is_terminal_dropped() {
        let provider = DummyProvider::default();
        let request = request(927_000_001, 486);
        provider
            .set_scenario(request.match_id, DummyScenario::NoPlayerAnchors)
            .await;
        let outcomes = get_match_details_batch(&provider, &[request])
            .await
            .unwrap();
        assert!(matches!(
            outcomes[0].status,
            CompletedMatchResolutionStatus::Dropped
        ));
        assert!(outcomes[0].r#match.is_none());
    }

    #[tokio::test]
    async fn pve_single_human_is_complete() {
        let provider = DummyProvider::default();
        let request = request(930_000_001, 425);
        provider
            .set_scenario(request.match_id, DummyScenario::PveSingleHuman)
            .await;
        let outcomes = get_match_details_batch(&provider, &[request])
            .await
            .unwrap();
        assert!(matches!(
            outcomes[0].status,
            CompletedMatchResolutionStatus::CompleteDirect
        ));
        assert_eq!(call_count(&provider.counts(), "getplayerbatchfrommatch"), 0);
    }

    #[tokio::test]
    async fn casual_incomplete_detail_uses_roster_only_recovery() {
        let provider = DummyProvider::default();
        let request = request(935_000_001, 424);
        provider
            .set_scenario(request.match_id, DummyScenario::BrokenSkin)
            .await;
        let outcomes = get_match_details_batch(&provider, &[request])
            .await
            .unwrap();
        assert!(matches!(
            outcomes[0].status,
            CompletedMatchResolutionStatus::Limited
        ));
        assert_eq!(outcomes[0].r#match.as_ref().unwrap().players.len(), 7);
        assert_eq!(outcomes[0].roster.as_ref().unwrap().len(), 10);
        let counts = provider.counts();
        assert_eq!(call_count(&counts, "getplayerbatchfrommatch"), 1);
        assert_eq!(call_count(&counts, "getmatchhistory"), 0);
        assert_eq!(call_count(&counts, "getdemodetails"), 0);
    }

    #[tokio::test]
    async fn vendor_failure_is_not_mislabeled_dropped() {
        let provider = DummyProvider::default();
        let request = request(940_000_001, 486);
        provider
            .set_scenario(request.match_id, DummyScenario::VendorFailure)
            .await;
        let error = get_match_details_batch(&provider, &[request])
            .await
            .unwrap_err();
        assert!(matches!(error, RelayError::Upstream(_)));
    }

    #[tokio::test]
    async fn singleton_hard_skin_error_enters_recovery() {
        let provider = DummyProvider::default();
        let request = request(945_000_001, 486);
        provider
            .set_scenario(request.match_id, DummyScenario::HardBrokenSkin)
            .await;
        let outcomes = get_match_details_batch(&provider, &[request])
            .await
            .unwrap();
        assert!(matches!(
            outcomes[0].status,
            CompletedMatchResolutionStatus::CompleteRecovered
        ));
        assert_eq!(outcomes[0].r#match.as_ref().unwrap().players.len(), 10);
        let counts = provider.counts();
        assert_eq!(call_count(&counts, "getmatchdetailsbatch"), 1);
        assert_eq!(call_count(&counts, "getplayerbatchfrommatch"), 1);
        assert_eq!(call_count(&counts, "getmatchhistory"), 10);
        assert_eq!(call_count(&counts, "getdemodetails"), 1);
    }

    #[tokio::test]
    async fn hard_multi_match_error_remains_worker_owned() {
        let provider = DummyProvider::default();
        let requests = vec![request(946_000_001, 486), request(946_000_002, 486)];
        provider
            .set_scenario(requests[0].match_id, DummyScenario::HardBrokenSkin)
            .await;
        let error = get_match_details_batch(&provider, &requests)
            .await
            .unwrap_err();
        assert!(matches!(error, RelayError::Upstream(_)));
        assert_eq!(call_count(&provider.counts(), "getplayerbatchfrommatch"), 0);
    }

    #[tokio::test]
    async fn failed_demo_shell_does_not_abort_recovered_match() {
        let provider = DummyProvider::default();
        let request = request(947_000_001, 486);
        provider
            .set_scenario(request.match_id, DummyScenario::HardBrokenSkinDemoFailure)
            .await;
        let outcomes = get_match_details_batch(&provider, &[request])
            .await
            .unwrap();
        assert!(matches!(
            outcomes[0].status,
            CompletedMatchResolutionStatus::CompleteRecovered
        ));
        assert_eq!(outcomes[0].r#match.as_ref().unwrap().players.len(), 10);
        assert_eq!(call_count(&provider.counts(), "getdemodetails"), 1);
    }
}
