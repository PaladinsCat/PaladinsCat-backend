use std::{
    collections::{HashMap, HashSet},
    sync::OnceLock,
};

use serde::Deserialize;
use serde_json::Value;

use crate::{model::CompletedMatchRequest, provider::RelayError};

const CONTRACT_JSON: &str =
    include_str!("../../backend/contracts/hirez-relay-operation-contract.json");

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelayOperationManifest {
    pub schema_version: u32,
    pub dummy_match_scenarios: Vec<String>,
    pub operations: Vec<RelayOperationDefinition>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelayOperationDefinition {
    pub name: String,
    pub modes: Vec<String>,
    pub validation: RelayOperationValidation,
    pub valid_args: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelayOperationValidation {
    pub max_args: Option<usize>,
    pub max_args_error: Option<String>,
    pub rules: Vec<RelayValidationRule>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelayValidationRule {
    pub index: usize,
    pub kind: RelayValidationRuleKind,
    pub label: String,
    #[serde(default)]
    pub optional: bool,
    pub min_items: Option<usize>,
    pub max_items: Option<usize>,
    pub values_from: Option<String>,
    pub error_template: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RelayValidationRuleKind {
    Boolean,
    CompletedMatchRequests,
    Enum,
    FiniteNumber,
    FiniteNumberArray,
    NonEmptyString,
    NonEmptyStringArray,
    PositiveInteger,
    RawPayloadArray,
}

static MANIFEST: OnceLock<RelayOperationManifest> = OnceLock::new();
static OPERATION_INDEX: OnceLock<HashMap<String, usize>> = OnceLock::new();

pub fn manifest() -> &'static RelayOperationManifest {
    MANIFEST.get_or_init(|| {
        serde_json::from_str(CONTRACT_JSON).expect("parse shared HirezRelay operation contract")
    })
}

pub fn verify_manifest() -> Result<(), RelayError> {
    let contract = manifest();
    if contract.schema_version != 1 {
        return Err(RelayError::Validation(format!(
            "unsupported relay operation contract schema {}",
            contract.schema_version,
        )));
    }
    let mut names = HashSet::new();
    for operation in &contract.operations {
        if !names.insert(operation.name.as_str()) {
            return Err(RelayError::Validation(format!(
                "duplicate relay operation contract entry: {}",
                operation.name,
            )));
        }
        for mode in &operation.modes {
            validate_operation(&operation.name, &operation.valid_args, mode)?;
        }
    }
    Ok(())
}

fn operation_index() -> &'static HashMap<String, usize> {
    OPERATION_INDEX.get_or_init(|| {
        manifest()
            .operations
            .iter()
            .enumerate()
            .map(|(index, operation)| (operation.name.clone(), index))
            .collect()
    })
}

pub fn operation_definition(operation: &str) -> Option<&'static RelayOperationDefinition> {
    operation_index()
        .get(operation)
        .and_then(|index| manifest().operations.get(*index))
}

fn js_number(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Null => Some(0.0),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::Number(value) => value.as_f64().filter(|number| number.is_finite()),
        Value::String(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Some(0.0)
            } else {
                trimmed
                    .parse::<f64>()
                    .ok()
                    .filter(|number| number.is_finite())
            }
        }
        Value::Array(values) if values.is_empty() => Some(0.0),
        Value::Array(values) if values.len() == 1 => js_number(values.first()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn positive_integer(value: Option<&Value>) -> Option<u64> {
    let number = js_number(value)?;
    if number > 0.0 && number.fract() == 0.0 && number <= u64::MAX as f64 {
        Some(number as u64)
    } else {
        None
    }
}

fn validate_completed_match_requests(
    value: Option<&Value>,
    rule: &RelayValidationRule,
) -> Result<(), RelayError> {
    let requests = value.and_then(Value::as_array).ok_or_else(|| {
        RelayError::Validation(format!(
            "{} must contain between {} and {} matches",
            rule.label,
            rule.min_items.unwrap_or(0),
            rule.max_items
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unbounded".to_owned()),
        ))
    })?;
    let min_items = rule.min_items.unwrap_or(0);
    let max_items = rule.max_items.unwrap_or(usize::MAX);
    if requests.len() < min_items || requests.len() > max_items {
        return Err(RelayError::Validation(format!(
            "{} must contain between {min_items} and {} matches",
            rule.label,
            rule.max_items
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unbounded".to_owned()),
        )));
    }

    let mut seen = HashSet::new();
    for (index, request) in requests.iter().enumerate() {
        let object = request.as_object().ok_or_else(|| {
            RelayError::Validation(format!("{}[{index}] must be an object", rule.label))
        })?;
        let match_id = positive_integer(object.get("matchId")).ok_or_else(|| {
            RelayError::Validation(format!(
                "{}[{index}].matchId must be a positive integer",
                rule.label,
            ))
        })?;
        if !seen.insert(match_id) {
            return Err(RelayError::Validation(format!(
                "{} contains duplicate matchId {match_id}",
                rule.label,
            )));
        }
        if object.contains_key("queueId") && positive_integer(object.get("queueId")).is_none() {
            return Err(RelayError::Validation(format!(
                "{}[{index}].queueId must be a positive integer",
                rule.label,
            )));
        }
    }
    Ok(())
}

fn validate_raw_payload_array(value: Option<&Value>, label: &str) -> Result<(), RelayError> {
    let payloads = value
        .and_then(Value::as_array)
        .ok_or_else(|| RelayError::Validation(format!("{label} must be an array")))?;
    for (index, payload) in payloads.iter().enumerate() {
        let object = payload
            .as_object()
            .ok_or_else(|| RelayError::Validation(format!("{label}[{index}] must be an object")))?;
        for field in ["endpoint", "entity_type"] {
            let valid = object
                .get(field)
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty());
            if !valid {
                return Err(RelayError::Validation(format!(
                    "{label}[{index}].{field} must be a non-empty string",
                )));
            }
        }
        if !object.get("raw_data").is_some_and(Value::is_array) {
            return Err(RelayError::Validation(format!(
                "{label}[{index}].raw_data must be an array",
            )));
        }
    }
    Ok(())
}

fn validate_rule(args: &[Value], rule: &RelayValidationRule) -> Result<(), RelayError> {
    let value = args.get(rule.index);
    if rule.optional && value.is_none() {
        return Ok(());
    }

    match rule.kind {
        RelayValidationRuleKind::Boolean => {
            if !value.is_some_and(Value::is_boolean) {
                return Err(RelayError::Validation(format!(
                    "{} must be a boolean",
                    rule.label,
                )));
            }
        }
        RelayValidationRuleKind::CompletedMatchRequests => {
            validate_completed_match_requests(value, rule)?;
        }
        RelayValidationRuleKind::Enum => {
            let values: &[String] = match rule.values_from.as_deref() {
                Some("dummyMatchScenarios") => &manifest().dummy_match_scenarios,
                _ => &[],
            };
            let candidate = value.and_then(Value::as_str).unwrap_or_default();
            if !values.iter().any(|allowed| allowed == candidate) {
                let error = rule
                    .error_template
                    .as_deref()
                    .unwrap_or("{} must be one of: {values}")
                    .replace("{}", &rule.label)
                    .replace("{values}", &values.join(", "));
                return Err(RelayError::Validation(error));
            }
        }
        RelayValidationRuleKind::FiniteNumber => {
            if js_number(value).is_none() {
                return Err(RelayError::Validation(format!(
                    "{} must be a finite number",
                    rule.label,
                )));
            }
        }
        RelayValidationRuleKind::FiniteNumberArray => {
            let valid = value
                .and_then(Value::as_array)
                .is_some_and(|values| values.iter().all(|value| js_number(Some(value)).is_some()));
            if !valid {
                return Err(RelayError::Validation(format!(
                    "{} must be an array of finite numbers",
                    rule.label,
                )));
            }
        }
        RelayValidationRuleKind::NonEmptyString => {
            let valid = value
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty());
            if !valid {
                return Err(RelayError::Validation(format!(
                    "{} must be a non-empty string",
                    rule.label,
                )));
            }
        }
        RelayValidationRuleKind::NonEmptyStringArray => {
            let valid = value.and_then(Value::as_array).is_some_and(|values| {
                values
                    .iter()
                    .all(|value| value.as_str().is_some_and(|value| !value.trim().is_empty()))
            });
            if !valid {
                return Err(RelayError::Validation(format!(
                    "{} must be an array of non-empty strings",
                    rule.label,
                )));
            }
        }
        RelayValidationRuleKind::PositiveInteger => {
            if positive_integer(value).is_none() {
                return Err(RelayError::Validation(format!(
                    "{} must be a positive integer",
                    rule.label,
                )));
            }
        }
        RelayValidationRuleKind::RawPayloadArray => {
            validate_raw_payload_array(value, &rule.label)?;
        }
    }
    Ok(())
}

pub fn validate_operation(
    operation: &str,
    args: &[Value],
    mode: &str,
) -> Result<&'static RelayOperationDefinition, RelayError> {
    let definition = operation_definition(operation).ok_or_else(|| {
        RelayError::Validation(format!("Unsupported HirezRelay operation: {operation}"))
    })?;
    if !definition.modes.iter().any(|allowed| allowed == mode) {
        return Err(RelayError::Validation(format!(
            "Unsupported HirezRelay operation: {operation}",
        )));
    }
    if definition
        .validation
        .max_args
        .is_some_and(|max_args| args.len() > max_args)
    {
        return Err(RelayError::Validation(
            definition
                .validation
                .max_args_error
                .clone()
                .unwrap_or_else(|| {
                    format!(
                        "{operation} accepts at most {} args",
                        definition.validation.max_args.unwrap_or_default(),
                    )
                }),
        ));
    }
    for rule in &definition.validation.rules {
        validate_rule(args, rule)?;
    }
    Ok(definition)
}

pub fn parse_completed_match_requests(
    value: &Value,
) -> Result<Vec<CompletedMatchRequest>, RelayError> {
    let requests = value
        .as_array()
        .ok_or_else(|| RelayError::Validation("requests are required".to_owned()))?;
    requests
        .iter()
        .map(|request| {
            let object = request
                .as_object()
                .ok_or_else(|| RelayError::Validation("invalid requests".to_owned()))?;
            let match_id = positive_integer(object.get("matchId"))
                .ok_or_else(|| RelayError::Validation("invalid requests".to_owned()))?;
            let queue_id = object
                .get("queueId")
                .map(|value| {
                    positive_integer(Some(value))
                        .and_then(|number| u32::try_from(number).ok())
                        .ok_or_else(|| RelayError::Validation("invalid requests".to_owned()))
                })
                .transpose()?;
            Ok(CompletedMatchRequest { match_id, queue_id })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_manifest_has_unique_complete_operation_inventory() {
        verify_manifest().expect("shared manifest must verify");
        assert_eq!(manifest().schema_version, 1);
        assert_eq!(manifest().operations.len(), 37);
        let names = manifest()
            .operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(names.len(), manifest().operations.len());
    }

    #[test]
    fn every_manifest_fixture_passes_its_declared_modes() {
        for operation in &manifest().operations {
            for mode in &operation.modes {
                validate_operation(&operation.name, &operation.valid_args, mode).unwrap_or_else(
                    |error| panic!("{} valid fixture failed in {mode}: {error}", operation.name,),
                );
            }
        }
    }

    #[test]
    fn dummy_only_operations_are_rejected_in_real_mode() {
        for operation in manifest()
            .operations
            .iter()
            .filter(|operation| !operation.modes.iter().any(|mode| mode == "real"))
        {
            let error = validate_operation(&operation.name, &operation.valid_args, "real")
                .expect_err("dummy-only operation must fail in real mode");
            assert_eq!(error.status_code(), 400);
            assert_eq!(error.error_code(), "VALIDATION_ERROR");
        }
    }

    #[test]
    fn manifest_drives_exact_boundary_errors() {
        let cases = [
            (
                "getMatchDetailsBatch",
                vec![serde_json::json!([])],
                "requests must contain between 1 and 10 matches",
            ),
            (
                "getMatchDetailsBatch",
                vec![serde_json::json!([
                    {"matchId": 12},
                    {"matchId": "12"}
                ])],
                "requests contains duplicate matchId 12",
            ),
            (
                "getMatchHistory",
                vec![
                    serde_json::json!(12),
                    serde_json::json!(50),
                    serde_json::json!("false"),
                ],
                "forceRefresh must be a boolean",
            ),
            (
                "dumpRawPayloads",
                vec![serde_json::json!([{"endpoint": "", "entity_type": "match", "raw_data": []}])],
                "payloads[0].endpoint must be a non-empty string",
            ),
            (
                "resetDummyApiCallCounts",
                vec![serde_json::json!(true)],
                "resetDummyApiCallCounts takes no args",
            ),
        ];
        for (operation, args, expected) in cases {
            let error = validate_operation(operation, &args, "dummy")
                .expect_err("invalid fixture must fail");
            assert_eq!(error.to_string(), expected);
        }
    }
}
