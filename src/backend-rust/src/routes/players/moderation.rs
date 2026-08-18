use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::Response,
};
use paladinscat_core::{
    database::QueryParam,
    web_compat::{paginate, parse_js_integer},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{error::ApiError, request::RequestId};

use super::{
    PlayerQuery, PlayersState, escape_like, json_response, map_database, player_id, rows_response,
    value_i64,
};

#[derive(Clone, Copy)]
struct Session {
    user_id: i32,
    is_admin: bool,
    is_approved: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RelationBody {
    other_player_id: Option<Value>,
    other_role: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ReportBody {
    #[serde(rename = "type")]
    report_type: Option<String>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ClearTagBody {
    tag: Option<String>,
}

async fn require_session(
    state: &PlayersState,
    headers: &HeaderMap,
    request_id: &RequestId,
) -> Result<Session, ApiError> {
    let token = crate::routes::identity::session_token(headers).ok_or_else(|| {
        ApiError::coded(StatusCode::UNAUTHORIZED, "AUTH", "Authentication required")
    })?;
    let token_hash = format!("{:x}", Sha256::digest(token.as_bytes()));
    let row = state
        .database
        .one_json(
            "SELECT s.user_id,u.is_admin,u.is_approved FROM sessions s JOIN users u ON u.id=s.user_id \
             WHERE s.token=$1 AND s.expires_at>now()",
            &[&token_hash],
        )
        .await
        .map_err(|error| map_database(error, request_id))?
        .ok_or_else(|| {
            ApiError::coded(StatusCode::UNAUTHORIZED, "AUTH", "Invalid session")
        })?;
    let user_id = i32::try_from(value_i64(row.get("user_id")))
        .map_err(|_| ApiError::coded(StatusCode::UNAUTHORIZED, "AUTH", "Invalid session"))?;
    Ok(Session {
        user_id,
        is_admin: row
            .get("is_admin")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        is_approved: row
            .get("is_approved")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

pub(super) async fn alt_account_relations(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let pagination = paginate(query.page.as_deref(), query.per_page.as_deref());
    let (page, per_page, offset) = (pagination.page, pagination.per_page, pagination.offset);
    let search = query.q.as_deref().unwrap_or_default().trim();
    let mut params = Vec::<QueryParam>::new();
    let search_clause = if search.is_empty() {
        String::new()
    } else {
        params.push(QueryParam::Text(format!("%{}%", escape_like(search))));
        let pattern = params.len();
        params.push(QueryParam::Text(search.to_owned()));
        format!(
            "WHERE main_player.name ILIKE ${pattern} ESCAPE '\\' \
             OR alt_player.name ILIKE ${pattern} ESCAPE '\\' \
             OR main_player.id::TEXT=${} OR alt_player.id::TEXT=${}",
            params.len(),
            params.len()
        )
    };
    params.push(QueryParam::Int64(per_page));
    let limit_parameter = params.len();
    params.push(QueryParam::Int64(offset));
    let rows = state
        .database
        .query_json_params(
            &format!(
                "WITH pair_votes AS ( \
                   SELECT main_player_id,alt_player_id,COUNT(*)::INT AS vote_count,MAX(updated_at) AS last_voted_at \
                   FROM player_alt_account_votes GROUP BY main_player_id,alt_player_id \
                 ), matching_mains AS ( \
                   SELECT DISTINCT relation.main_player_id FROM pair_votes relation \
                   JOIN players main_player ON main_player.id=relation.main_player_id \
                   JOIN players alt_player ON alt_player.id=relation.alt_player_id {search_clause} \
                 ), main_totals AS ( \
                   SELECT relation.main_player_id,SUM(relation.vote_count)::INT AS total_votes,COUNT(*)::INT AS alt_count, \
                     MAX(relation.last_voted_at) AS last_voted_at FROM pair_votes relation \
                   JOIN matching_mains matched ON matched.main_player_id=relation.main_player_id GROUP BY relation.main_player_id \
                 ), paged_mains AS ( \
                   SELECT totals.*,COUNT(*) OVER()::INT AS total_count FROM main_totals totals \
                   JOIN players main_player ON main_player.id=totals.main_player_id \
                   ORDER BY totals.total_votes DESC,totals.last_voted_at DESC,main_player.name ASC \
                   LIMIT ${limit_parameter} OFFSET ${} \
                 ) SELECT paged.total_count,paged.total_votes,paged.alt_count,paged.last_voted_at, \
                   main_player.id AS main_player_id,main_player.name AS main_player_name,main_player.region AS main_player_region, \
                   main_player.platform AS main_player_platform,main_player.cheater AS main_player_cheater, \
                   main_player.sus_count AS main_player_sus_count,main_player.dropper AS main_player_dropper, \
                   main_player.afk_wintrade AS main_player_afk_wintrade,main_player.alt_account AS main_player_alt_account, \
                   jsonb_agg(jsonb_build_object('id',alt_player.id,'name',alt_player.name,'region',alt_player.region, \
                     'platform',alt_player.platform,'cheater',alt_player.cheater,'sus_count',alt_player.sus_count, \
                     'dropper',alt_player.dropper,'afk_wintrade',alt_player.afk_wintrade,'alt_account',alt_player.alt_account, \
                     'vote_count',relation.vote_count,'last_voted_at',relation.last_voted_at) \
                     ORDER BY relation.vote_count DESC,relation.last_voted_at DESC,alt_player.name ASC) AS alt_accounts \
                 FROM paged_mains paged JOIN players main_player ON main_player.id=paged.main_player_id \
                 JOIN pair_votes relation ON relation.main_player_id=paged.main_player_id \
                 JOIN players alt_player ON alt_player.id=relation.alt_player_id \
                 GROUP BY paged.total_count,paged.total_votes,paged.alt_count,paged.last_voted_at,main_player.id, \
                   main_player.name,main_player.region,main_player.platform,main_player.cheater,main_player.sus_count, \
                   main_player.dropper,main_player.afk_wintrade,main_player.alt_account \
                 ORDER BY paged.total_votes DESC,paged.last_voted_at DESC,main_player.name ASC",
                params.len()
            ),
            &params,
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let mut response = rows_response(rows);
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=60"),
    );
    let _ = page;
    Ok(response)
}

pub(super) async fn my_alt_account_relations(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let session = require_session(&state, &headers, &request_id).await?;
    let rows = state
        .database
        .query_json(
            "SELECT relation.id,relation.main_player_id,main_player.name AS main_player_name, \
               relation.alt_player_id,alt_player.name AS alt_player_name,relation.created_at,relation.updated_at \
             FROM player_alt_account_votes relation JOIN players main_player ON main_player.id=relation.main_player_id \
             JOIN players alt_player ON alt_player.id=relation.alt_player_id \
             WHERE relation.user_id=$1 AND (relation.main_player_id=$2 OR relation.alt_player_id=$2) \
             ORDER BY relation.updated_at DESC,relation.id DESC",
            &[&session.user_id, &player_id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let mut response = rows_response(rows);
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("private, no-store"));
    Ok(response)
}

pub(super) async fn create_alt_account_relation(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<RelationBody>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let other_id = body
        .other_player_id
        .as_ref()
        .and_then(|value| match value {
            Value::String(value) => parse_js_integer(value),
            Value::Number(value) => value.as_i64(),
            _ => None,
        })
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation("Both player IDs must be valid"))?;
    if player_id == other_id {
        return Err(ApiError::validation(
            "An account cannot be linked to itself",
        ));
    }
    let other_role = body.other_role.as_deref().unwrap_or_default();
    if !matches!(other_role, "main" | "alt") {
        return Err(ApiError::validation(
            "otherRole must be \"main\" or \"alt\"",
        ));
    }
    let session = require_session(&state, &headers, &request_id).await?;
    let players = state
        .database
        .query_json(
            "SELECT id,name FROM players WHERE id=ANY($1::bigint[])",
            &[&vec![player_id, other_id]],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    if players.len() != 2 {
        return Err(ApiError::not_found_without_details(
            "One of the selected players does not exist",
        ));
    }
    let main_id = if other_role == "main" {
        other_id
    } else {
        player_id
    };
    let alt_id = if other_role == "alt" {
        other_id
    } else {
        player_id
    };
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let replaced = transaction
        .execute(
            "DELETE FROM player_alt_account_votes WHERE user_id=$1 \
             AND LEAST(main_player_id,alt_player_id)=LEAST($2::bigint,$3::bigint) \
             AND GREATEST(main_player_id,alt_player_id)=GREATEST($2::bigint,$3::bigint)",
            &[&session.user_id, &player_id, &other_id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?
        > 0;
    transaction
        .execute(
            "INSERT INTO player_alt_account_votes(user_id,main_player_id,alt_player_id) VALUES($1,$2,$3)",
            &[&session.user_id, &main_id, &alt_id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .execute(
            "UPDATE players player SET alt_account=EXISTS( \
               SELECT 1 FROM player_alt_account_votes relation WHERE relation.alt_player_id=player.id \
             ) WHERE player.id=ANY($1::bigint[])",
            &[&vec![player_id, other_id]],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let name = |id: i64| {
        players
            .iter()
            .find(|row| value_i64(row.get("id")) == id)
            .and_then(|row| row.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    Ok(json_response(json!({
        "success":true,
        "replaced":replaced,
        "relation":{
            "main_player_id":main_id,
            "main_player_name":name(main_id),
            "alt_player_id":alt_id,
            "alt_player_name":name(alt_id)
        }
    })))
}

pub(super) async fn delete_alt_account_relation(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path((id, other_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let player_id_value = player_id(&id)?;
    let other_id = player_id(&other_id)?;
    if player_id_value == other_id {
        return Err(ApiError::validation("Invalid player relationship"));
    }
    let session = require_session(&state, &headers, &request_id).await?;
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let removed = transaction
        .execute(
            "DELETE FROM player_alt_account_votes WHERE user_id=$1 \
             AND LEAST(main_player_id,alt_player_id)=LEAST($2::bigint,$3::bigint) \
             AND GREATEST(main_player_id,alt_player_id)=GREATEST($2::bigint,$3::bigint)",
            &[&session.user_id, &player_id_value, &other_id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?
        > 0;
    transaction
        .execute(
            "UPDATE players player SET alt_account=EXISTS( \
               SELECT 1 FROM player_alt_account_votes relation WHERE relation.alt_player_id=player.id \
             ) WHERE player.id=ANY($1::bigint[])",
            &[&vec![player_id_value, other_id]],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    Ok(json_response(json!({"success":true,"removed":removed})))
}

pub(super) async fn report(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ReportBody>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let session = require_session(&state, &headers, &request_id).await?;
    let report_type = body.report_type.as_deref().unwrap_or_default();
    if !matches!(
        report_type,
        "suspicious"
            | "cheater"
            | "approve"
            | "weirdo"
            | "hall_of_fame"
            | "dropper"
            | "afk_wintrade"
    ) {
        return Err(ApiError::validation("Invalid report type."));
    }
    let reason = body
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if report_type != "approve"
        && !matches!(report_type, "dropper" | "afk_wintrade")
        && reason.is_none()
    {
        return Err(ApiError::validation(
            "A reason is required for every player report or vote",
        ));
    }
    if matches!(report_type, "cheater" | "approve") && !session.is_admin && !session.is_approved {
        return Err(ApiError::coded(
            StatusCode::FORBIDDEN,
            "PERMISSION",
            "Action requires admin or approved status",
        ));
    }
    let existing = state
        .database
        .one_json(
            "SELECT id,name,cheater,sus_count,weirdo_count,hall_of_fame_count,dropper,afk_wintrade,alt_account \
             FROM players WHERE id=$1",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?
        .ok_or_else(|| ApiError::not_found("Player not found", json!({"playerId":player_id})))?;
    if report_type == "approve" {
        state
            .database
            .query_json(
                "UPDATE players SET cheater=TRUE WHERE id=$1 RETURNING id",
                &[&player_id],
            )
            .await
            .map_err(|error| map_database(error, &request_id))?;
        return Ok(json_response(json!({
            "success":true,
            "message":"Player confirmed as cheater",
            "cheater":true,
            "reason":body.reason
        })));
    }
    if matches!(report_type, "suspicious" | "weirdo" | "hall_of_fame") {
        let column = match report_type {
            "suspicious" => "sus_count",
            "weirdo" => "weirdo_count",
            _ => "hall_of_fame_count",
        };
        let rows = state
            .database
            .query_json(
                &format!(
                    "WITH inserted_vote AS ( \
                       INSERT INTO player_community_votes(player_id,user_id,vote_type,reason) VALUES($1,$2,$3,$4) \
                       ON CONFLICT(player_id,user_id,vote_type) DO NOTHING RETURNING id \
                     ), updated_player AS ( \
                       UPDATE players SET {column}=( \
                         SELECT COUNT(*)::INT FROM player_community_votes \
                         WHERE player_id=$1 AND vote_type=$3 \
                       )+CASE WHEN EXISTS(SELECT 1 FROM inserted_vote) THEN 1 ELSE 0 END WHERE id=$1 \
                       RETURNING {column} AS count \
                     ) SELECT EXISTS(SELECT 1 FROM inserted_vote) AS created,(SELECT count FROM updated_player) AS count"
                ),
                &[&player_id, &session.user_id, &report_type, &reason],
            )
            .await
            .map_err(|error| map_database(error, &request_id))?;
        let vote = rows.first();
        let created = vote
            .and_then(|row| row.get("created"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let count = vote
            .map(|row| value_i64(row.get("count")))
            .filter(|count| *count > 0)
            .unwrap_or_else(|| value_i64(existing.get(column)));
        let message = if created {
            match report_type {
                "suspicious" => "Player reported as Suspicious",
                "weirdo" => "Player added to Weirdo",
                _ => "Player added to Hall of Fame",
            }
        } else {
            match report_type {
                "suspicious" => "You have already submitted a Suspicious report for this player",
                "weirdo" => "You have already submitted a Weirdo vote for this player",
                _ => "You have already submitted a Hall of Fame vote for this player",
            }
        };
        return Ok(json_response(json!({
            "success":true,"message":message,"already_voted":!created,"count":count
        })));
    }
    if matches!(report_type, "dropper" | "afk_wintrade") {
        let label = if report_type == "dropper" {
            "Dropper"
        } else {
            "AFK / Wintrade"
        };
        let rows = state
            .database
            .query_json(
                &format!(
                    "WITH inserted_vote AS ( \
                       INSERT INTO player_community_votes(player_id,user_id,vote_type,reason) VALUES($1,$2,$3,$4) \
                       ON CONFLICT(player_id,user_id,vote_type) DO NOTHING RETURNING id \
                     ) UPDATE players SET {report_type}=TRUE WHERE id=$1 \
                     RETURNING EXISTS(SELECT 1 FROM inserted_vote) AS created"
                ),
                &[&player_id, &session.user_id, &report_type, &reason.unwrap_or_default()],
            )
            .await
            .map_err(|error| map_database(error, &request_id))?;
        let created = rows
            .first()
            .and_then(|row| row.get("created"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mut payload = json!({
            "success":true,
            "message":if created {format!("{label} vote recorded")} else {format!("You have already voted {label} for this player")},
            "already_voted":!created
        });
        payload[report_type] = Value::Bool(true);
        return Ok(json_response(payload));
    }
    let rows = state
        .database
        .query_json(
            "WITH inserted_vote AS ( \
               INSERT INTO player_community_votes(player_id,user_id,vote_type,reason) VALUES($1,$2,'cheater',$3) \
               ON CONFLICT(player_id,user_id,vote_type) DO NOTHING RETURNING id \
             ) UPDATE players SET cheater=TRUE WHERE id=$1 \
             RETURNING EXISTS(SELECT 1 FROM inserted_vote) AS created",
            &[&player_id, &session.user_id, &reason],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let created = rows
        .first()
        .and_then(|row| row.get("created"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok(json_response(json!({
        "success":true,
        "message":if created {"Player confirmed as cheater"} else {"Cheater report already recorded for this player"},
        "cheater":true,
        "already_voted":!created
    })))
}

pub(super) async fn clear_tag(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ClearTagBody>,
) -> Result<Response, ApiError> {
    let session = require_session(&state, &headers, &request_id).await?;
    if !session.is_admin {
        return Err(ApiError::coded(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Admin access required",
        ));
    }
    let player_id = player_id(&id)?;
    let tag = body.tag.as_deref().unwrap_or_default();
    if !matches!(
        tag,
        "cheater" | "suspicious" | "dropper" | "afk_wintrade" | "alt_account"
    ) {
        return Err(ApiError::validation("Invalid moderation tag"));
    }
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let existing = transaction
        .query_opt(
            "SELECT cheater,sus_count,dropper,afk_wintrade,alt_account FROM players WHERE id=$1 FOR UPDATE",
            &[&player_id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?
        .ok_or_else(|| ApiError::not_found("Player not found", json!({"playerId":player_id})))?;
    let was_tagged = match tag {
        "suspicious" => existing.get::<_, i32>("sus_count") > 0,
        "cheater" => existing.get("cheater"),
        "dropper" => existing.get("dropper"),
        "afk_wintrade" => existing.get("afk_wintrade"),
        "alt_account" => existing.get("alt_account"),
        _ => false,
    };
    let update = if tag == "suspicious" {
        "UPDATE players SET sus_count=0 WHERE id=$1".to_owned()
    } else {
        format!("UPDATE players SET {tag}=FALSE WHERE id=$1")
    };
    transaction
        .execute(&update, &[&player_id])
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let mut removed = transaction
        .execute(
            "DELETE FROM player_community_votes WHERE player_id=$1 AND vote_type=$2",
            &[&player_id, &tag],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    if tag == "alt_account" {
        removed += transaction
            .execute(
                "DELETE FROM player_alt_account_votes WHERE alt_player_id=$1",
                &[&player_id],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), &request_id))?;
    }
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let label = match tag {
        "suspicious" => "Suspicious",
        "afk_wintrade" => "AFK / Wintrade",
        "alt_account" => "Alt account",
        "cheater" => "Cheater",
        _ => "Dropper",
    };
    Ok(json_response(json!({
        "success":true,
        "cleared":was_tagged,
        "removed_reports":removed,
        "message":if was_tagged {format!("{label} tag cleared")} else {format!("{label} tag was already clear")}
    })))
}
