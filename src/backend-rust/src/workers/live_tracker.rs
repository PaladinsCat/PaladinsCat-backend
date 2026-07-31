use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct DropDetectionResult {
    pub ended: i32,
    pub dropped: i32,
    pub suspects: i32,
}

/// Terminalizes stale live snapshots from local canonical match facts only.
/// Detection never spends Hi-Rez quota: live lookup owns acquisition and the
/// canonical completed-match pipeline owns the ended-match evidence.
pub async fn detect_dropped_matches(
    database: &Database,
) -> Result<DropDetectionResult, DatabaseError> {
    let stale = database
        .query_json(
            "SELECT lm.match_id::TEXT,EXISTS(SELECT 1 FROM matches m WHERE m.match_id=lm.match_id) ended \
             FROM live_matches lm WHERE lm.status='active' AND lm.detected_at<now()-INTERVAL '30 minutes' \
             ORDER BY lm.detected_at FOR UPDATE SKIP LOCKED",
            &[],
        )
        .await?;
    let mut result = DropDetectionResult::default();
    for row in stale {
        let Some(match_id) = integer(&row, "match_id") else {
            continue;
        };
        if row.get("ended").and_then(Value::as_bool).unwrap_or(false) {
            database
                .query_json(
                    "WITH ended AS(UPDATE live_matches SET status='ended',ended_at=now(),dropped=false \
                       WHERE match_id=$1 AND status='active' RETURNING match_id)\
                     DELETE FROM live_match_players p USING ended WHERE p.match_id=ended.match_id",
                    &[&match_id],
                )
                .await?;
            result.ended = result.ended.saturating_add(1);
            continue;
        }
        let count = database
            .one_json(
                "WITH dropped AS(\
                   UPDATE live_matches SET status='dropped',dropped=true,ended_at=now() \
                   WHERE match_id=$1 AND status='active' RETURNING match_id\
                 ),upserted AS(\
                   INSERT INTO drop_hack_suspects(\
                     player_id,player_name,match_id,champion_id,champion_name,is_cassie,dropped_at,incident_count\
                   )
                   SELECT p.player_id,p.player_name,p.match_id,p.champion_id,p.champion_name,p.champion_id=67,now(),1 \
                   FROM live_match_players p JOIN dropped d ON d.match_id=p.match_id
                   ON CONFLICT(player_id,match_id) DO UPDATE SET \
                     incident_count=drop_hack_suspects.incident_count+1,dropped_at=now() RETURNING player_id\
                 ) SELECT count(*)::INT count FROM upserted",
                &[&match_id],
            )
            .await?
            .and_then(|row| integer(&row, "count"))
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default();
        result.dropped = result.dropped.saturating_add(1);
        result.suspects = result.suspects.saturating_add(count);
    }
    Ok(result)
}

fn integer(row: &Value, key: &str) -> Option<i64> {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
