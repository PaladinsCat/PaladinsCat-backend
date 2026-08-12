use axum::{
    Json,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use regex::Regex;
use serde_json::{Value, json};

use crate::{
    error::ApiError,
    request::RequestId,
    workers::private_identity::{PRIVATE_IDENTITY_VERSION, backfill_private_account_identities},
};

use super::{AdminState, require_admin};

pub(super) async fn reconcile(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let apply = body.get("apply").and_then(Value::as_bool).unwrap_or(false);
    match backfill_private_account_identities(&state.database, apply).await {
        Ok(report) => Ok(Json(json!(report)).into_response()),
        Err(error) => Ok(super::coded_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "PRIVATE_RECONCILIATION_FAILED",
            &error.to_string(),
        )),
    }
}

pub(super) async fn verify_name(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Path(private_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let Some(private_id) = private_id.parse::<i32>().ok().filter(|id| *id > 0) else {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "Invalid private account ID",
        ));
    };
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    let evidence_ref = body
        .get("evidenceRef")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    let evidence_sha256 = body
        .get("evidenceSha256")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let evidence_sha256 = (!evidence_sha256.is_empty()).then_some(evidence_sha256);
    let notes = body
        .get("notes")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    let notes = (!notes.is_empty()).then_some(notes);
    let verified_by = body
        .get("verifiedBy")
        .and_then(Value::as_str)
        .unwrap_or("admin-api")
        .trim()
        .chars()
        .take(100)
        .collect::<String>();
    if name.is_empty() || name.chars().count() > 100 || name.chars().any(|value| value.is_control())
    {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "Name must contain 1-100 printable characters",
        ));
    }
    if evidence_ref.is_empty() || evidence_ref.chars().count() > 2_000 {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "evidenceRef is required and must be at most 2000 characters",
        ));
    }
    if evidence_sha256.as_ref().is_some_and(|value| {
        let Some(re) = Regex::new("^[0-9a-f]{64}$").ok() else {
            return true;
        };
        !re.is_match(value)
    }) {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "evidenceSha256 must be a lowercase SHA-256 hex digest",
        ));
    }
    if notes
        .as_ref()
        .is_some_and(|value| value.chars().count() > 5_000)
    {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "notes must be at most 5000 characters",
        ));
    }
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let identity = transaction.query_opt(
        "SELECT id,verified_name FROM players_private WHERE id=$1 AND tracking_version=$2 AND is_active FOR UPDATE",
        &[&private_id, &PRIVATE_IDENTITY_VERSION],
    ).await.map_err(|error| ApiError::database(error.into(), &request_id))?;
    if identity.is_none() {
        return Ok(super::coded_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "Active private identity not found",
        ));
    }
    transaction.execute(
        "UPDATE private_account_name_verifications SET is_active=FALSE,revoked_at=now() WHERE private_player_id=$1 AND is_active",
        &[&private_id],
    ).await.map_err(|error| ApiError::database(error.into(), &request_id))?;
    let verification = transaction.query_one(
        "INSERT INTO private_account_name_verifications(private_player_id,verified_name,evidence_ref,evidence_sha256,notes,verified_by) \
         VALUES($1,$2,$3,$4,$5,$6) RETURNING id,created_at",
        &[&private_id, &name, &evidence_ref, &evidence_sha256, &notes, &verified_by],
    ).await.map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction.execute(
        "UPDATE players_private SET verified_name=$2,name_verified_at=now(),name_verified_by=$3,\
         name_evidence_ref=$4,identity_status='verified',updated_at=now() WHERE id=$1",
        &[&private_id, &name, &verified_by, &evidence_ref],
    ).await.map_err(|error| ApiError::database(error.into(), &request_id))?;
    let duplicates = transaction.query(
        "SELECT id FROM players_private WHERE id<>$1 AND is_active AND lower(verified_name)=lower($2) ORDER BY id",
        &[&private_id, &name],
    ).await.map_err(|error| ApiError::database(error.into(), &request_id))?
        .into_iter().map(|row| row.get::<_, i32>("id")).collect::<Vec<_>>();
    let verification_id = verification.get::<_, i64>("id");
    let verified_at = verification.get::<_, time::OffsetDateTime>("created_at");
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    Ok(Json(json!({
        "privateId":private_id,
        "name":name,
        "verificationId":verification_id,
        "verifiedAt":verified_at,
        "possibleDuplicates":duplicates,
    }))
    .into_response())
}

pub(super) async fn moderation(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Path(private_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let Some(private_id) = private_id.parse::<i32>().ok().filter(|id| *id > 0) else {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "Invalid private account ID",
        ));
    };
    let Some(cheater) = body.get("cheater").and_then(Value::as_bool) else {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "cheater must be a boolean",
        ));
    };
    let reason = body
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if cheater && reason.is_empty() {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "A reason is required when confirming a cheater",
        ));
    }
    if reason.chars().count() > 2_000 {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "reason must be at most 2000 characters",
        ));
    }
    let reason: Option<String> = (!reason.is_empty()).then_some(reason);
    let updated = state.database.one_json(
        "WITH RECURSIVE identity_chain AS(\
           SELECT id,merged_into_id,is_active,0 AS depth FROM players_private WHERE id=$1 \
           UNION ALL SELECT next.id,next.merged_into_id,next.is_active,chain.depth+1 \
           FROM players_private next JOIN identity_chain chain ON next.id=chain.merged_into_id WHERE chain.depth<16\
         ),canonical AS(SELECT id FROM identity_chain WHERE is_active ORDER BY depth DESC LIMIT 1)\
         UPDATE players_private account SET cheater=$2,\
           cheater_reason=CASE WHEN $2 THEN $3 ELSE NULL END,\
           cheater_marked_at=CASE WHEN $2 THEN now() ELSE NULL END,updated_at=now() \
         FROM canonical WHERE account.id=canonical.id \
         RETURNING account.id,account.alias,account.verified_name,account.cheater,account.cheater_reason,account.cheater_marked_at",
        &[&private_id, &cheater, &reason],
    ).await.map_err(|error| ApiError::database(error, &request_id))?;
    let Some(updated) = updated else {
        return Ok(super::coded_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "Private account not found",
        ));
    };
    Ok(Json(json!({"account":updated})).into_response())
}
