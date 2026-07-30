use std::collections::HashMap;

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    routing::get,
};
use paladinscat_core::{
    cache::RedisCache,
    database::{Database, QueryParam},
};
use serde_json::Value;

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
};

const REFERENCE_CACHE_TTL_SECONDS: u64 = 3_600;
const VALID_REFERENCE_TYPES: &str = "champions, items, bounty-items, maps, tiers, regions, talents, queues, patches, cards, skins, abilities";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IdKind {
    Int32,
    Int64,
    Text,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReferenceSpec {
    table: &'static str,
    route: &'static str,
    cache_key: &'static str,
    id_column: &'static str,
    id_kind: IdKind,
}

const REFERENCE_SPECS: &[ReferenceSpec] = &[
    ReferenceSpec {
        table: "champions",
        route: "champions",
        cache_key: "ref:champions",
        id_column: "id",
        id_kind: IdKind::Int32,
    },
    ReferenceSpec {
        table: "items",
        route: "items",
        cache_key: "ref:items",
        id_column: "item_id",
        id_kind: IdKind::Int32,
    },
    ReferenceSpec {
        table: "bounty_items",
        route: "bounty-items",
        cache_key: "ref:bounty_items",
        id_column: "bounty_item_id",
        id_kind: IdKind::Int64,
    },
    ReferenceSpec {
        table: "maps",
        route: "maps",
        cache_key: "ref:maps",
        id_column: "map_id",
        id_kind: IdKind::Int32,
    },
    ReferenceSpec {
        table: "ranked_tiers",
        route: "tiers",
        cache_key: "ref:ranked_tiers",
        id_column: "tier_id",
        id_kind: IdKind::Int32,
    },
    ReferenceSpec {
        table: "regions",
        route: "regions",
        cache_key: "ref:regions",
        id_column: "region_code",
        id_kind: IdKind::Text,
    },
    ReferenceSpec {
        table: "talents",
        route: "talents",
        cache_key: "ref:talents",
        id_column: "talent_id",
        id_kind: IdKind::Int32,
    },
    ReferenceSpec {
        table: "queue_types",
        route: "queues",
        cache_key: "ref:queue_types",
        id_column: "queue_id",
        id_kind: IdKind::Int32,
    },
    ReferenceSpec {
        table: "patches",
        route: "patches",
        cache_key: "ref:patches",
        id_column: "id",
        id_kind: IdKind::Int32,
    },
    ReferenceSpec {
        table: "cards",
        route: "cards",
        cache_key: "ref:cards",
        id_column: "card_id",
        id_kind: IdKind::Int32,
    },
    ReferenceSpec {
        table: "skins",
        route: "skins",
        cache_key: "ref:skins",
        id_column: "skin_id",
        id_kind: IdKind::Int32,
    },
    ReferenceSpec {
        table: "championsquick",
        route: "abilities",
        cache_key: "ref:championsquick",
        id_column: "id",
        id_kind: IdKind::Int32,
    },
];

#[derive(Clone)]
struct ReferenceState {
    database: Database,
    cache: RedisCache,
}

pub fn router(database: Database, cache: RedisCache) -> Router {
    let mut router = Router::new().route("/reference/lookup", get(generic_lookup));
    for spec in REFERENCE_SPECS {
        router = router
            .route(&format!("/reference/{}", spec.route), get(reference_list))
            .route(
                &format!("/reference/{}/{{id}}", spec.route),
                get(reference_by_id),
            );
    }
    router.with_state(ReferenceState { database, cache })
}

async fn reference_list(
    State(state): State<ReferenceState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
) -> Result<Json<Value>, ApiError> {
    let spec = spec_for_uri(&uri).ok_or_else(|| ApiError::internal(&request_id))?;
    load_list(&state, spec, &request_id).await
}

async fn reference_by_id(
    State(state): State<ReferenceState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(raw_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let spec = spec_for_uri(&uri).ok_or_else(|| ApiError::internal(&request_id))?;
    let key = format!("{}:{raw_id}", spec.cache_key);
    if let Some(cached) = state.cache.get::<Value>(&key).await {
        return Ok(Json(cached));
    }

    let row = query_by_id(&state.database, spec, &raw_id, &request_id).await?;
    let Some(row) = row else {
        return Err(ApiError::not_found(
            format!("{} not found", spec.route),
            serde_json::json!({ "id": raw_id }),
        ));
    };
    state
        .cache
        .set(&key, &row, Some(REFERENCE_CACHE_TTL_SECONDS))
        .await;
    Ok(Json(row))
}

async fn generic_lookup(
    State(state): State<ReferenceState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let reference_type = query
        .get("type")
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::validation("Missing required query param: type"))?;
    let Some(spec) = spec_for_route(reference_type) else {
        return Err(ApiError::validation(format!(
            "Unknown reference type: {reference_type}. Valid types: {VALID_REFERENCE_TYPES}"
        )));
    };

    if let Some(raw_id) = query
        .get("id")
        .map(String::as_str)
        .filter(|value| !value.is_empty())
    {
        let row = query_by_id(&state.database, spec, raw_id, &request_id).await?;
        let Some(row) = row else {
            return Err(ApiError::not_found(
                format!("{reference_type} not found"),
                serde_json::json!({ "id": raw_id }),
            ));
        };
        return Ok(Json(row));
    }

    load_list(&state, spec, &request_id).await
}

async fn load_list(
    state: &ReferenceState,
    spec: &'static ReferenceSpec,
    request_id: &RequestId,
) -> Result<Json<Value>, ApiError> {
    if let Some(cached) = state.cache.get::<Value>(spec.cache_key).await {
        return Ok(Json(cached));
    }
    let rows = state
        .database
        .query_json(
            &format!("SELECT * FROM {} ORDER BY {}", spec.table, spec.id_column),
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    let payload = Value::Array(rows);
    state
        .cache
        .set(spec.cache_key, &payload, Some(REFERENCE_CACHE_TTL_SECONDS))
        .await;
    Ok(Json(payload))
}

async fn query_by_id(
    database: &Database,
    spec: &'static ReferenceSpec,
    raw_id: &str,
    request_id: &RequestId,
) -> Result<Option<Value>, ApiError> {
    let parameter = match spec.id_kind {
        IdKind::Int32 => raw_id
            .parse::<i32>()
            .map(QueryParam::Int32)
            .map_err(|_| ApiError::internal(request_id))?,
        IdKind::Int64 => raw_id
            .parse::<i64>()
            .map(QueryParam::Int64)
            .map_err(|_| ApiError::internal(request_id))?,
        IdKind::Text => QueryParam::Text(raw_id.to_owned()),
    };
    database
        .one_json_params(
            &format!("SELECT * FROM {} WHERE {} = $1", spec.table, spec.id_column),
            &[parameter],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))
}

fn spec_for_uri(uri: &axum::http::Uri) -> Option<&'static ReferenceSpec> {
    let route = uri.path().strip_prefix("/reference/")?.split('/').next()?;
    spec_for_route(route)
}

fn spec_for_route(route: &str) -> Option<&'static ReferenceSpec> {
    REFERENCE_SPECS.iter().find(|spec| spec.route == route)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_allowlist_has_all_twelve_unique_types() {
        assert_eq!(REFERENCE_SPECS.len(), 12);
        let mut routes = REFERENCE_SPECS
            .iter()
            .map(|spec| spec.route)
            .collect::<Vec<_>>();
        routes.sort_unstable();
        routes.dedup();
        assert_eq!(routes.len(), REFERENCE_SPECS.len());
    }

    #[test]
    fn schema_drifted_reference_ids_use_canonical_columns() {
        assert_eq!(spec_for_route("tiers").expect("tiers").id_column, "tier_id");
        assert_eq!(spec_for_route("patches").expect("patches").id_column, "id");
        assert_eq!(
            spec_for_route("abilities").expect("abilities").id_column,
            "id"
        );
    }
}
