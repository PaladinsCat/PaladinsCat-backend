use std::{collections::HashMap, env, time::Duration};

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::{
        HeaderValue, StatusCode,
        header::{CACHE_CONTROL, HeaderName},
    },
    response::{IntoResponse, Response},
    routing::get,
};
use paladinscat_core::{
    database::{Database, DatabaseError, QueryParam},
    web_compat::{paginate, parse_js_integer, sorting},
};
use serde_json::{Map, Value, json};

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
    route_cache::{ColdMissLease, RouteCache, canonical_route_cache_url, now_millis},
};

use super::lobby_tier::parse_tier_bounds;

pub const ROUTE_COUNT: usize = 9;

const CHANGELOG_FRESH_TTL_SECONDS: u64 = 300;
const CHANGELOG_STALE_TTL_SECONDS: u64 = 900;

#[derive(Clone)]
struct MetaState {
    database: Database,
    cache: RouteCache,
}

pub fn router(database: Database, cache: RouteCache) -> Router {
    Router::new()
        .route("/meta/version", get(version))
        .route("/meta/changelog", get(changelog))
        .route("/meta/items", get(items))
        .route("/meta/talents", get(talents))
        .route("/meta/cards", get(cards))
        .route("/meta/compositions", get(compositions))
        .route("/meta/items/{item_id}", get(item_detail))
        .route("/meta/talents/{talent_id}", get(talent_detail))
        .route("/meta/cards/{card_id}", get(card_detail))
        .route("/meta/top", get(top))
        .with_state(MetaState { database, cache })
}

async fn version(
    State(state): State<MetaState>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Response, ApiError> {
    let stack = state
        .database
        .one_json(
            "SELECT id, component, environment, version, git_commit, git_commit_short, \
                    git_branch, git_dirty, build_timestamp, deployed_at, \
                    db_schema_version, source, notes, metadata \
             FROM stack_versions \
             WHERE component = 'stack' \
             ORDER BY deployed_at DESC, id DESC \
             LIMIT 1",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;

    let payload = if let Some(stack) = stack {
        let components = state
            .database
            .query_json(
                "SELECT DISTINCT ON (component) \
                        id, component, environment, version, git_commit, git_commit_short, \
                        git_branch, git_dirty, build_timestamp, deployed_at, \
                        db_schema_version, source, notes, metadata \
                 FROM stack_versions \
                 WHERE component <> 'stack' \
                 ORDER BY component, deployed_at DESC, id DESC",
                &[],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?;
        map_stack_version(&stack, &components)
    } else {
        let legacy = state
            .database
            .one_json(
                "SELECT id, timestamp, version \
                 FROM site_versions \
                 ORDER BY timestamp DESC, id DESC \
                 LIMIT 1",
                &[],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?;
        legacy_version(legacy.as_ref())
    };

    let mut response = (StatusCode::OK, Json(payload)).into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    Ok(response)
}

async fn changelog(
    State(state): State<MetaState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let key = format!("route:meta:changelog:{}", canonical_route_cache_url(&uri));
    if let Some(cached) = state.cache.get(&key).await {
        let stale = cached.fresh_until <= now_millis();
        if stale && state.cache.begin_refresh(&key).await {
            spawn_changelog_refresh(state.clone(), key.clone(), query.clone());
        }
        return Ok(cache_response(
            cached.payload,
            if stale { "STALE" } else { "HIT" },
            cached.fresh_until,
        ));
    }

    let lease = state.cache.acquire_cold_miss(&key).await;
    if matches!(lease, ColdMissLease::Follower)
        && let Some(cached) = state.cache.wait_for_cold_miss(&key).await
    {
        return Ok(cache_response(
            cached.payload,
            "COALESCED",
            cached.fresh_until,
        ));
    }

    let payload = match load_changelog(&state.database, &query).await {
        Ok(payload) => payload,
        Err(error) => {
            state.cache.release(&lease).await;
            return Err(ApiError::database(error, &request_id));
        }
    };
    state
        .cache
        .store(
            &key,
            payload.clone(),
            CHANGELOG_FRESH_TTL_SECONDS,
            CHANGELOG_STALE_TTL_SECONDS,
        )
        .await;
    state.cache.release(&lease).await;
    Ok(cache_response(
        payload,
        "MISS",
        now_millis().saturating_add((CHANGELOG_FRESH_TTL_SECONDS * 1_000) as i64),
    ))
}

fn spawn_changelog_refresh(state: MetaState, key: String, query: HashMap<String, String>) {
    tokio::spawn(async move {
        let lease = state.cache.acquire_cold_miss(&key).await;
        if matches!(lease, ColdMissLease::Owner { .. })
            && let Ok(payload) = load_changelog(&state.database, &query).await
        {
            state
                .cache
                .store(
                    &key,
                    payload,
                    CHANGELOG_FRESH_TTL_SECONDS,
                    CHANGELOG_STALE_TTL_SECONDS,
                )
                .await;
        }
        state.cache.release(&lease).await;
        state.cache.finish_refresh(&key).await;
    });
}

async fn load_changelog(
    database: &Database,
    query: &HashMap<String, String>,
) -> Result<Value, DatabaseError> {
    if query.get("preview").is_some_and(|value| value == "true") {
        let row = database
            .one_json(
                "SELECT id, component, version, git_commit, git_commit_short, git_branch, \
                        deployed_at, source, metadata, changelog \
                 FROM stack_versions \
                 WHERE component = 'stack' AND changelog IS NOT NULL AND changelog <> '' \
                 ORDER BY deployed_at DESC, id DESC \
                 LIMIT 1",
                &[],
            )
            .await?;
        return Ok(row.as_ref().map(map_changelog_entry).unwrap_or(Value::Null));
    }

    let page = paginate(
        query.get("page").map(String::as_str),
        query.get("perPage").map(String::as_str),
    );
    let public_versions = "\
      SELECT DISTINCT ON (COALESCE(NULLIF(git_commit, ''), 'row:' || id::text)) \
        id, component, version, git_commit, git_commit_short, git_branch, deployed_at, \
        source, metadata, changelog \
      FROM stack_versions \
      WHERE component = 'stack' \
      ORDER BY \
        COALESCE(NULLIF(git_commit, ''), 'row:' || id::text), \
        (changelog IS NOT NULL AND changelog <> '') DESC, \
        deployed_at DESC, \
        id DESC";
    let total_row = database
        .one_json(
            &format!("SELECT COUNT(*)::INT AS total FROM ({public_versions}) AS public_versions"),
            &[],
        )
        .await?;
    let total = total_row
        .as_ref()
        .and_then(|row| row.get("total"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let rows = database
        .query_json(
            &format!(
                "SELECT id, component, version, git_commit, git_commit_short, git_branch, \
                        deployed_at, source, metadata, changelog \
                 FROM ({public_versions}) AS public_versions \
                 ORDER BY deployed_at DESC, id DESC \
                 LIMIT $1 OFFSET $2"
            ),
            &[&page.per_page, &page.offset],
        )
        .await?;
    Ok(json!({
        "data": rows.iter().map(map_changelog_entry).collect::<Vec<_>>(),
        "total": total,
        "page": page.page,
        "perPage": page.per_page,
        "totalPages": ceiling_division(total, page.per_page),
    }))
}

async fn items(
    State(state): State<MetaState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let mode = mode(&query);
    let page = paginate(
        query.get("page").map(String::as_str),
        query.get("perPage").map(String::as_str),
    );
    let mut params = Vec::new();
    let mut clauses = Vec::new();
    if let Some(raw_slot) = query.get("slot") {
        params.push(integer_query_param(raw_slot));
        clauses.push(format!("slot = ${}::SMALLINT", params.len()));
    }
    if let Some(raw_level) = query.get("itemLevel") {
        params.push(integer_query_param(raw_level));
        clauses.push(format!("item_level = ${}::SMALLINT", params.len()));
    }
    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    let order = sorting(
        query.get("sort").map(String::as_str),
        query.get("order").map(String::as_str),
        &[
            "count",
            "winrate",
            "wins",
            "losses",
            "item_name",
            "slot",
            "item_level",
        ],
    );
    params.push(QueryParam::Int64(page.per_page));
    let limit_index = params.len();
    params.push(QueryParam::Int64(page.offset));
    let offset_index = params.len();
    let sql = format!(
        "SELECT * FROM item_counts_{mode}{where_clause}{order} \
         LIMIT ${limit_index} OFFSET ${offset_index}"
    );
    query_rows(&state.database, &sql, &params, &request_id).await
}

async fn talents(
    State(state): State<MetaState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    simple_count_rows(&state.database, "talent_counts", &query, &request_id).await
}

async fn cards(
    State(state): State<MetaState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    simple_count_rows(&state.database, "card_counts", &query, &request_id).await
}

async fn simple_count_rows(
    database: &Database,
    table_prefix: &str,
    query: &HashMap<String, String>,
    request_id: &RequestId,
) -> Result<Json<Value>, ApiError> {
    let mode = mode(query);
    let page = paginate(
        query.get("page").map(String::as_str),
        query.get("perPage").map(String::as_str),
    );
    let order = sorting(
        query.get("sort").map(String::as_str),
        query.get("order").map(String::as_str),
        &["count", "winrate", "wins"],
    );
    let sql = format!("SELECT * FROM {table_prefix}_{mode}{order} LIMIT $1 OFFSET $2");
    query_rows(
        database,
        &sql,
        &[
            QueryParam::Int64(page.per_page),
            QueryParam::Int64(page.offset),
        ],
        request_id,
    )
    .await
}

async fn compositions(
    State(state): State<MetaState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let Some(bounds) = parse_tier_bounds(&query) else {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Tier bounds must be between 1 and 26." })),
        )
            .into_response());
    };
    let sort_by = query.get("sortBy").map_or("count", String::as_str);
    let sort = if [
        "count",
        "winrate",
        "wins",
        "frontline",
        "damage",
        "flank",
        "support",
    ]
    .contains(&sort_by)
    {
        sort_by
    } else {
        "count"
    };
    let order = if query.get("order").is_some_and(|value| value == "asc") {
        "ASC"
    } else {
        "DESC"
    };
    let limit = legacy_limit(query.get("limit").map(String::as_str), 50, 200);
    let mut params = Vec::new();
    let mut clauses = Vec::new();
    if let Some(minimum) = bounds.minimum {
        params.push(QueryParam::Int16(minimum));
        clauses.push(format!("mcr.lobby_tier >= ${}", params.len()));
    }
    if let Some(maximum) = bounds.maximum {
        params.push(QueryParam::Int16(maximum));
        clauses.push(format!("mcr.lobby_tier <= ${}", params.len()));
    }
    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    params.push(QueryParam::Int64(limit));
    let sql = format!(
        "SELECT \
           mcr.comp_id, \
           mcr.frontline, \
           mcr.damage, \
           mcr.flank, \
           mcr.support, \
           SUM(mcr.count)::INT AS count, \
           SUM(mcr.wins)::INT AS wins, \
           SUM(mcr.losses)::INT AS losses, \
           ROUND( \
             100.0 * SUM(mcr.wins)::NUMERIC \
             / NULLIF((SUM(mcr.wins) + SUM(mcr.losses))::NUMERIC, 0), \
             2 \
           ) AS winrate \
         FROM match_compositions_ranked mcr \
         {where_clause} \
         GROUP BY mcr.comp_id, mcr.frontline, mcr.damage, mcr.flank, mcr.support \
         ORDER BY {sort} {order}, mcr.comp_id \
         LIMIT ${}",
        params.len()
    );
    let rows = state
        .database
        .query_json_params(&sql, &params)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(json!({ "total": rows.len(), "data": rows })).into_response())
}

async fn item_detail(
    State(state): State<MetaState>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    detail(
        &state.database,
        "item_counts",
        "item_id",
        "item",
        "Item",
        &raw_id,
        &query,
        &request_id,
    )
    .await
}

async fn talent_detail(
    State(state): State<MetaState>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    detail(
        &state.database,
        "talent_counts",
        "talent_id",
        "talent",
        "Talent",
        &raw_id,
        &query,
        &request_id,
    )
    .await
}

async fn card_detail(
    State(state): State<MetaState>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    detail(
        &state.database,
        "card_counts",
        "card_id",
        "card",
        "Card",
        &raw_id,
        &query,
        &request_id,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn detail(
    database: &Database,
    table_prefix: &str,
    id_column: &str,
    detail_key: &str,
    label: &str,
    raw_id: &str,
    query: &HashMap<String, String>,
    request_id: &RequestId,
) -> Result<Json<Value>, ApiError> {
    let identifier = parse_js_integer(raw_id)
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| ApiError::validation(format!("Invalid {detail_key} ID")))?;
    let mode = mode(query);
    let rows = database
        .query_json(
            &format!("SELECT * FROM {table_prefix}_{mode} WHERE {id_column} = $1"),
            &[&identifier],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    let Some(row) = rows.into_iter().next() else {
        return Err(ApiError::not_found(
            format!("{label} stats not found"),
            json!({ format!("{detail_key}Id"): identifier, "mode": mode }),
        ));
    };
    Ok(Json(row))
}

async fn top(
    State(state): State<MetaState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let mode = mode(&query);
    let limit = legacy_limit(query.get("limit").map(String::as_str), 10, 50);
    let parameter = [QueryParam::Int64(limit)];
    let items_sql = format!(
        "SELECT item_id, item_name AS name, count, winrate \
         FROM item_counts_{mode} ORDER BY count DESC LIMIT $1"
    );
    let talents_sql = format!(
        "SELECT talent_id, talent_name AS name, count, winrate \
         FROM talent_counts_{mode} ORDER BY count DESC LIMIT $1"
    );
    let cards_sql = format!(
        "SELECT card_id, card_name AS name, count, winrate \
         FROM card_counts_{mode} ORDER BY count DESC LIMIT $1"
    );
    let items = state.database.query_json_params(&items_sql, &parameter);
    let talents = state.database.query_json_params(&talents_sql, &parameter);
    let cards = state.database.query_json_params(&cards_sql, &parameter);
    let (top_items, top_talents, top_cards) = tokio::try_join!(items, talents, cards)
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(json!({
        "mode": mode,
        "topItems": top_items,
        "topTalents": top_talents,
        "topCards": top_cards,
    })))
}

async fn query_rows(
    database: &Database,
    sql: &str,
    params: &[QueryParam],
    request_id: &RequestId,
) -> Result<Json<Value>, ApiError> {
    let rows = database
        .query_json_params(sql, params)
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    Ok(Json(Value::Array(rows)))
}

fn mode(query: &HashMap<String, String>) -> &'static str {
    if query.get("mode").is_some_and(|value| value == "casual") {
        "casual"
    } else {
        "ranked"
    }
}

fn integer_query_param(raw: &str) -> QueryParam {
    parse_js_integer(raw)
        .and_then(|value| i16::try_from(value).ok())
        .map(QueryParam::Int16)
        .unwrap_or_else(|| QueryParam::Text("NaN".to_owned()))
}

fn legacy_limit(raw: Option<&str>, default: i64, maximum: i64) -> i64 {
    match raw.and_then(parse_js_integer) {
        None | Some(0) => default,
        Some(value) => value.min(maximum),
    }
}

fn map_stack_version(row: &Value, components: &[Value]) -> Value {
    json!({
        "id": value(row, "id"),
        "timestamp": value(row, "deployed_at"),
        "version": value(row, "version"),
        "component": value(row, "component"),
        "environment": value(row, "environment"),
        "gitCommit": string_or_empty(row, "git_commit"),
        "gitCommitShort": normalized_commit_short(row),
        "gitBranch": string_or_empty(row, "git_branch"),
        "gitDirty": row.get("git_dirty").and_then(Value::as_bool).unwrap_or(false),
        "buildTimestamp": value(row, "build_timestamp"),
        "deployedAt": value(row, "deployed_at"),
        "dbSchemaVersion": string_or_empty(row, "db_schema_version"),
        "source": string_or_empty(row, "source"),
        "notes": string_or_empty(row, "notes"),
        "metadata": object_or_empty(row, "metadata"),
        "components": components.iter().map(map_component_version).collect::<Vec<_>>(),
    })
}

fn map_component_version(row: &Value) -> Value {
    json!({
        "id": value(row, "id"),
        "component": value(row, "component"),
        "environment": value(row, "environment"),
        "version": value(row, "version"),
        "gitCommit": string_or_empty(row, "git_commit"),
        "gitCommitShort": normalized_commit_short(row),
        "gitBranch": string_or_empty(row, "git_branch"),
        "gitDirty": row.get("git_dirty").and_then(Value::as_bool).unwrap_or(false),
        "buildTimestamp": value(row, "build_timestamp"),
        "deployedAt": value(row, "deployed_at"),
        "dbSchemaVersion": string_or_empty(row, "db_schema_version"),
        "source": string_or_empty(row, "source"),
        "metadata": object_or_empty(row, "metadata"),
    })
}

fn legacy_version(row: Option<&Value>) -> Value {
    let timestamp = row.map_or(Value::Null, |row| value(row, "timestamp"));
    let full_commit = env::var("PALADINSCAT_GIT_COMMIT").unwrap_or_default();
    let short_commit = env::var("PALADINSCAT_GIT_COMMIT_SHORT")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| full_commit.chars().take(7).collect());
    json!({
        "id": row.map_or(Value::from(0), |row| value(row, "id")),
        "timestamp": timestamp,
        "version": row
            .and_then(|row| row.get("version"))
            .cloned()
            .unwrap_or_else(|| Value::String(env::var("PALADINSCAT_VERSION").unwrap_or_default())),
        "component": "stack",
        "environment": env::var("NODE_ENV").unwrap_or_else(|_| "unknown".to_owned()),
        "gitCommit": full_commit,
        "gitCommitShort": short_commit,
        "gitBranch": env::var("PALADINSCAT_GIT_BRANCH").unwrap_or_default(),
        "gitDirty": env::var("PALADINSCAT_GIT_DIRTY").as_deref() == Ok("true"),
        "buildTimestamp": env::var("PALADINSCAT_BUILD_TIMESTAMP")
            .ok()
            .filter(|value| !value.is_empty()),
        "deployedAt": timestamp,
        "dbSchemaVersion": "036_stack_versions",
        "source": if row.is_some() { "site_versions_legacy" } else { "runtime_env_fallback" },
        "metadata": {},
        "components": [],
    })
}

fn map_changelog_entry(row: &Value) -> Value {
    let changelog = string_or_empty(row, "changelog");
    let change_count = release_change_count(row.get("metadata"), &changelog);
    let component_version = changelog_component_version(row);
    json!({
        "id": value(row, "id"),
        "component": changelog_component(row),
        "version": component_version.clone(),
        "componentVersion": component_version,
        "totalVersion": changelog_total_version(row),
        "gitCommit": string_or_empty(row, "git_commit"),
        "gitCommitShort": normalized_commit_short(row),
        "gitBranch": string_or_empty(row, "git_branch"),
        "deployedAt": value(row, "deployed_at"),
        "source": string_or_empty(row, "source"),
        "changelog": changelog,
        "changeCount": change_count,
        "releaseType": if change_count >= 10 {
            "major"
        } else if change_count >= 5 {
            "minor"
        } else {
            "patch"
        },
    })
}

fn changelog_component_version(row: &Value) -> Value {
    row.get("metadata")
        .and_then(|metadata| metadata.get("componentVersion"))
        .filter(|version| {
            version
                .as_str()
                .is_some_and(|value| !value.trim().is_empty())
        })
        .cloned()
        .unwrap_or_else(|| value(row, "version"))
}

fn changelog_total_version(row: &Value) -> Value {
    if row
        .get("metadata")
        .and_then(|metadata| metadata.get("componentVersion"))
        .is_some()
        || changelog_component(row) == "legacy-monorepo"
    {
        value(row, "version")
    } else {
        Value::Null
    }
}

fn changelog_component(row: &Value) -> String {
    let stored = string_or_empty(row, "component");
    if !stored.is_empty() && stored != "stack" {
        return stored;
    }
    if let Some(component) = row
        .get("metadata")
        .and_then(|metadata| metadata.get("releaseComponent"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return component.to_owned();
    }
    let services = row
        .get("metadata")
        .and_then(|metadata| metadata.get("services"));
    let parsed = match services {
        Some(Value::String(value)) => value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    let repositories = parsed
        .iter()
        .map(|service| match service.as_str() {
            "backend"
            | "backend-rust-api"
            | "backend-rust-auto-ingester"
            | "backend-rust-hourly-gap-checker"
            | "hirezrelay" => "backend",
            "discordbot" | "discord-bot" => "discord-bot",
            other => other,
        })
        .collect::<std::collections::BTreeSet<_>>();
    match repositories.into_iter().collect::<Vec<_>>().as_slice() {
        [component] => (*component).to_owned(),
        [] => "legacy-monorepo".to_owned(),
        _ => "stack".to_owned(),
    }
}

fn release_change_count(metadata: Option<&Value>, changelog: &str) -> usize {
    if let Some(stored) = metadata.and_then(|metadata| metadata.get("changeCount")) {
        let number = match stored {
            Value::Null => Some(0.0),
            Value::Number(number) => number.as_f64(),
            Value::String(value) => value.trim().parse::<f64>().ok(),
            Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
            _ => None,
        };
        if let Some(number) = number
            && number.is_finite()
            && number >= 0.0
            && number.fract() == 0.0
        {
            return number as usize;
        }
    }
    let lines = changelog
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let commit_lines = lines
        .iter()
        .filter(|line| is_commit_changelog_line(line))
        .count();
    if commit_lines > 0 {
        return commit_lines;
    }
    lines
        .iter()
        .filter(|line| !is_changelog_heading(line))
        .count()
}

fn is_commit_changelog_line(line: &str) -> bool {
    let mut parts = line.splitn(2, char::is_whitespace);
    let hash = parts.next().unwrap_or_default();
    let subject = parts.next().unwrap_or_default().trim();
    (7..=40).contains(&hash.len())
        && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        && !subject.is_empty()
}

fn is_changelog_heading(line: &str) -> bool {
    let trimmed = line.trim();
    let without_optional_bullet = if trimmed.starts_with("- ") || trimmed.starts_with("* ") {
        trimmed[1..].trim_start()
    } else {
        trimmed
    };
    let normalized = without_optional_bullet.to_ascii_lowercase();
    [
        "**added**",
        "**changed**",
        "**fixed**",
        "**removed**",
        "**refactored**",
        "**improved**",
        "**security**",
    ]
    .contains(&normalized.as_str())
}

fn normalized_commit_short(row: &Value) -> String {
    let short = string_or_empty(row, "git_commit_short");
    if !short.is_empty() {
        return short;
    }
    string_or_empty(row, "git_commit").chars().take(7).collect()
}

fn value(row: &Value, key: &str) -> Value {
    row.get(key).cloned().unwrap_or(Value::Null)
}

fn string_or_empty(row: &Value, key: &str) -> String {
    row.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn object_or_empty(row: &Value, key: &str) -> Value {
    row.get(key)
        .filter(|value| value.is_object())
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()))
}

fn ceiling_division(total: i64, divisor: i64) -> i64 {
    if total == 0 {
        0
    } else {
        (total + divisor - 1) / divisor
    }
}

fn cache_response(payload: Value, status: &'static str, fresh_until: i64) -> Response {
    let mut response = (StatusCode::OK, Json(payload)).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("x-cache"),
        HeaderValue::from_static(status),
    );
    if status == "HIT" || status == "STALE" {
        let age = now_millis()
            .saturating_sub(fresh_until)
            .div_euclid(Duration::from_secs(1).as_millis() as i64)
            .max(0);
        if let Ok(value) = HeaderValue::from_str(&age.to_string()) {
            response
                .headers_mut()
                .insert(HeaderName::from_static("x-cache-age"), value);
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::super::lobby_tier::TierBounds;
    use super::*;

    #[test]
    fn tier_bounds_match_number_semantics_and_validation() {
        assert_eq!(
            parse_tier_bounds(&HashMap::from([
                ("tierMin".to_owned(), "1.0".to_owned()),
                ("tierMax".to_owned(), "26".to_owned()),
            ])),
            Some(TierBounds {
                minimum: Some(1),
                maximum: Some(26),
            })
        );
        assert_eq!(
            parse_tier_bounds(&HashMap::from([
                ("tierMin".to_owned(), "1tail".to_owned(),)
            ])),
            None
        );
        assert_eq!(
            parse_tier_bounds(&HashMap::from([
                ("tierMin".to_owned(), "12".to_owned()),
                ("tierMax".to_owned(), "2".to_owned()),
            ])),
            None
        );
    }

    #[test]
    fn release_significance_matches_stored_commit_and_manual_forms() {
        assert_eq!(
            release_change_count(Some(&json!({"changeCount": "12"})), "ignored"),
            12
        );
        assert_eq!(
            release_change_count(None, "abcdef1 first\nabcdef2 second"),
            2
        );
        assert_eq!(release_change_count(None, "**Added**\n- One\n- Two"), 2);
    }

    #[test]
    fn changelog_component_separates_split_repositories_from_legacy_history() {
        assert_eq!(
            changelog_component(
                &json!({"component": "stack", "metadata": {"services": "backend,backend-rust-api"}})
            ),
            "backend"
        );
        assert_eq!(
            changelog_component(
                &json!({"component": "stack", "metadata": {"releaseComponent": "frontend", "services": "frontend"}})
            ),
            "frontend"
        );
        assert_eq!(
            changelog_component(&json!({"component": "stack", "metadata": {}})),
            "legacy-monorepo"
        );
        assert_eq!(
            changelog_component(&json!({"component": "discordbot", "metadata": {}})),
            "discordbot"
        );
    }

    #[test]
    fn changelog_exposes_total_and_component_versions() {
        assert_eq!(
            changelog_component_version(&json!({
                "version": "v2.4.82",
                "metadata": {"componentVersion": "v0.1.46"}
            })),
            "v0.1.46"
        );
        assert_eq!(
            changelog_total_version(&json!({
                "component": "stack",
                "version": "v2.4.82",
                "metadata": {"componentVersion": "v0.1.46", "services": "backend"}
            })),
            "v2.4.82"
        );
        assert_eq!(
            changelog_component_version(&json!({"version": "v0.6.44", "metadata": {}})),
            "v0.6.44"
        );
    }

    #[test]
    fn limits_retain_legacy_truthiness_and_negative_behavior() {
        assert_eq!(legacy_limit(None, 10, 50), 10);
        assert_eq!(legacy_limit(Some("0"), 10, 50), 10);
        assert_eq!(legacy_limit(Some("500tail"), 10, 50), 50);
        assert_eq!(legacy_limit(Some("-2"), 10, 50), -2);
    }

    #[test]
    fn legacy_version_preserves_site_version_fallback() {
        let payload = legacy_version(Some(&json!({
            "id": 9,
            "timestamp": "2026-03-01T00:00:00.000Z",
            "version": "v0.legacy"
        })));
        assert_eq!(payload["id"], 9);
        assert_eq!(payload["version"], "v0.legacy");
        assert_eq!(payload["component"], "stack");
        assert_eq!(payload["source"], "site_versions_legacy");
        assert_eq!(payload["deployedAt"], "2026-03-01T00:00:00.000Z");
        assert_eq!(payload["components"], json!([]));
    }
}
