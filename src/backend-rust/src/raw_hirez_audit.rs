use paladinscat_core::database::{Database, DatabaseError};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub(crate) struct RawHirezAudit<'a> {
    pub endpoint: &'a str,
    pub operation: &'a str,
    pub entity_type: &'a str,
    pub entity_id: String,
    pub params: Value,
    pub raw_response: &'a Value,
    pub source: &'a str,
}

pub(crate) async fn record_raw_hirez_response(
    database: &Database,
    input: RawHirezAudit<'_>,
) -> Result<Option<Value>, DatabaseError> {
    let raw_text = serde_json::to_string(input.raw_response).unwrap_or_else(|_| "null".to_owned());
    let response_sha256 = format!("{:x}", Sha256::digest(raw_text.as_bytes()));
    let response_shape = match input.raw_response {
        Value::Array(_) => "array",
        Value::Null => "null",
        Value::Object(_) => "object",
        Value::String(_) => "string",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
    };
    let response_count = match input.raw_response {
        Value::Array(rows) => Some(i32::try_from(rows.len()).unwrap_or(i32::MAX)),
        Value::Object(_) => Some(1),
        _ => None,
    };

    let entity_id = (!input.entity_id.is_empty()).then_some(input.entity_id.as_str());
    database
        .one_json(
            "INSERT INTO hirez_raw_api_responses (\
               endpoint, operation, entity_type, entity_id, params, raw_response, \
               raw_response_text, response_sha256, response_shape, response_count, \
               status_code, success, error_message, source\
             ) VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9,$10,200,true,NULL,$11) \
             RETURNING id::text, response_sha256, response_shape, response_count, created_at::text",
            &[
                &input.endpoint,
                &input.operation,
                &input.entity_type,
                &entity_id,
                &input.params,
                input.raw_response,
                &raw_text,
                &response_sha256,
                &response_shape,
                &response_count,
                &input.source,
            ],
        )
        .await
}
