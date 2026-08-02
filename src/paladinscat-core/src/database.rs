use std::str::FromStr;
use std::{error::Error, fmt};

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use serde_json::{Map, Value, json};
use thiserror::Error;
use time::{
    Date, OffsetDateTime, PrimitiveDateTime, UtcOffset, format_description::well_known::Rfc3339,
};
use tokio_postgres::{
    NoTls, Row,
    types::{FromSql, ToSql, Type},
};

use crate::config::BackendConfig;

#[derive(Clone)]
pub struct Database {
    pool: Pool,
}

#[derive(Clone, Debug)]
pub enum QueryParam {
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("invalid DATABASE_URL: {0}")]
    InvalidConfiguration(String),
    #[error("failed to build PostgreSQL pool: {0}")]
    PoolBuild(String),
    #[error("PostgreSQL pool error: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
    #[error("PostgreSQL query error: {0}")]
    Query(#[from] tokio_postgres::Error),
    #[error("unsupported PostgreSQL response type {type_name} for column {column}")]
    UnsupportedType { column: String, type_name: String },
}

impl Database {
    pub fn new(config: &BackendConfig, application_name: &str) -> Result<Self, DatabaseError> {
        let postgres = postgres_config(config, application_name)?;
        let manager = Manager::from_config(
            postgres,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(config.database_pool_max)
            .runtime(Runtime::Tokio1)
            .build()
            .map_err(|error| DatabaseError::PoolBuild(error.to_string()))?;
        Ok(Self { pool })
    }

    pub async fn health_check(&self) -> bool {
        let Ok(client) = self.pool.get().await else {
            return false;
        };
        client.simple_query("SELECT 1").await.is_ok()
    }

    pub async fn connection(&self) -> Result<deadpool_postgres::Object, DatabaseError> {
        Ok(self.pool.get().await?)
    }

    pub async fn query_json(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Value>, DatabaseError> {
        let client = self.pool.get().await?;
        let rows = client.query(sql, params).await.map_err(|error| {
            tracing::error!(sql, error=?error, "PostgreSQL query failed");
            error
        })?;
        rows.iter().map(row_to_json).collect()
    }

    pub async fn one_json(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Value>, DatabaseError> {
        Ok(self.query_json(sql, params).await?.into_iter().next())
    }

    pub async fn query_json_params(
        &self,
        sql: &str,
        params: &[QueryParam],
    ) -> Result<Vec<Value>, DatabaseError> {
        let refs = query_param_refs(params);
        self.query_json(sql, &refs).await
    }

    pub async fn one_json_params(
        &self,
        sql: &str,
        params: &[QueryParam],
    ) -> Result<Option<Value>, DatabaseError> {
        Ok(self
            .query_json_params(sql, params)
            .await?
            .into_iter()
            .next())
    }

    pub fn status(&self) -> DatabasePoolStatus {
        let status = self.pool.status();
        DatabasePoolStatus {
            max_size: status.max_size,
            size: status.size,
            available: status.available,
            waiting: status.waiting,
        }
    }
}

fn postgres_config(
    config: &BackendConfig,
    application_name: &str,
) -> Result<tokio_postgres::Config, DatabaseError> {
    let mut postgres = tokio_postgres::Config::from_str(&config.database_url)
        .map_err(|error| DatabaseError::InvalidConfiguration(error.to_string()))?;
    postgres.application_name(application_name);

    // tokio-postgres replaces the complete libpq options value when
    // Config::options is called. Preserve options supplied in DATABASE_URL so
    // test/shadow session constraints cannot be silently discarded by the
    // normal statement timeout.
    let mut options = postgres
        .get_options()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_default();
    append_session_option(
        &mut options,
        &format!(
            "-c statement_timeout={}",
            config.database_statement_timeout_ms
        ),
    );
    if config.database_default_transaction_read_only {
        append_session_option(&mut options, "-c default_transaction_read_only=on");
    }
    postgres.options(options);
    Ok(postgres)
}

fn append_session_option(options: &mut String, option: &str) {
    if !options.is_empty() {
        options.push(' ');
    }
    options.push_str(option);
}

fn query_param_refs(params: &[QueryParam]) -> Vec<&(dyn ToSql + Sync)> {
    params
        .iter()
        .map(|parameter| match parameter {
            QueryParam::Bool(value) => value as &(dyn ToSql + Sync),
            QueryParam::Int16(value) => value as &(dyn ToSql + Sync),
            QueryParam::Int32(value) => value as &(dyn ToSql + Sync),
            QueryParam::Int64(value) => value as &(dyn ToSql + Sync),
            QueryParam::Float64(value) => value as &(dyn ToSql + Sync),
            QueryParam::Text(value) => value as &(dyn ToSql + Sync),
        })
        .collect()
}

#[cfg(test)]
mod session_option_tests {
    use super::*;

    fn config(database_url: &str, read_only: bool) -> BackendConfig {
        BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.to_owned()),
            "DATABASE_DEFAULT_TRANSACTION_READ_ONLY" if read_only => Some("true".to_owned()),
            _ => None,
        })
        .expect("database config")
    }

    #[test]
    fn preserves_database_url_options_when_adding_session_defaults() {
        let config = config(
            "postgres://fixture/db?options=-c%20search_path%3Dpublic",
            false,
        );
        let postgres = postgres_config(&config, "fixture").expect("postgres config");
        assert_eq!(
            postgres.get_options(),
            Some("-c search_path=public -c statement_timeout=30000")
        );
    }

    #[test]
    fn combines_read_only_and_statement_timeout_options() {
        let config = config("postgres://fixture/db", true);
        let postgres = postgres_config(&config, "fixture").expect("postgres config");
        assert_eq!(
            postgres.get_options(),
            Some("-c statement_timeout=30000 -c default_transaction_read_only=on")
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatabasePoolStatus {
    pub max_size: usize,
    pub size: usize,
    pub available: usize,
    pub waiting: usize,
}

fn row_to_json(row: &Row) -> Result<Value, DatabaseError> {
    let mut object = Map::with_capacity(row.len());
    for (index, column) in row.columns().iter().enumerate() {
        let value = column_value(row, index, column.name(), column.type_())?;
        object.insert(column.name().to_owned(), value);
    }
    Ok(Value::Object(object))
}

fn column_value(row: &Row, index: usize, name: &str, ty: &Type) -> Result<Value, DatabaseError> {
    macro_rules! nullable {
        ($rust_type:ty, $map:expr) => {
            row.try_get::<_, Option<$rust_type>>(index)?
                .map($map)
                .unwrap_or(Value::Null)
        };
    }

    let value = match *ty {
        Type::BOOL => nullable!(bool, Value::Bool),
        Type::INT2 => nullable!(i16, |value| json!(value)),
        Type::INT4 => nullable!(i32, |value| json!(value)),
        // node-postgres intentionally returns int8 as a string.
        Type::INT8 => nullable!(i64, |value| Value::String(value.to_string())),
        Type::FLOAT4 => nullable!(f32, |value| json!(value)),
        Type::FLOAT8 => nullable!(f64, |value| json!(value)),
        // node-postgres leaves arbitrary-precision NUMERIC values as strings.
        // Decode PostgreSQL's binary base-10000 representation without a
        // floating-point conversion so API precision and declared scale match.
        Type::NUMERIC => nullable!(PgNumeric, |value| Value::String(value.0)),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            nullable!(String, Value::String)
        }
        Type::JSON | Type::JSONB => nullable!(Value, |value| value),
        Type::TIMESTAMPTZ => nullable!(OffsetDateTime, |value| {
            Value::String(format_json_timestamp(value.to_offset(UtcOffset::UTC)))
        }),
        Type::TIMESTAMP => nullable!(PrimitiveDateTime, |value| {
            Value::String(format_json_timestamp(value.assume_utc()))
        }),
        Type::DATE => nullable!(Date, |value| Value::String(format!(
            "{value}T00:00:00.000Z"
        ))),
        Type::BOOL_ARRAY => nullable!(Vec<bool>, |value| json!(value)),
        Type::INT2_ARRAY => nullable!(Vec<i16>, |value| json!(value)),
        Type::INT4_ARRAY => nullable!(Vec<i32>, |value| json!(value)),
        Type::INT8_ARRAY => nullable!(Vec<i64>, |values| Value::Array(
            values
                .into_iter()
                .map(|value| Value::String(value.to_string()))
                .collect()
        )),
        Type::FLOAT4_ARRAY => nullable!(Vec<f32>, |value| json!(value)),
        Type::FLOAT8_ARRAY => nullable!(Vec<f64>, |value| json!(value)),
        Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
            nullable!(Vec<String>, |value| json!(value))
        }
        _ => {
            return Err(DatabaseError::UnsupportedType {
                column: name.to_owned(),
                type_name: ty.name().to_owned(),
            });
        }
    };
    Ok(value)
}

#[derive(Debug)]
struct PgNumeric(String);

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        decode_numeric(raw)
            .map(Self)
            .map_err(|error| Box::new(error) as Box<dyn Error + Sync + Send>)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

#[derive(Debug)]
struct NumericDecodeError(&'static str);

impl fmt::Display for NumericDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for NumericDecodeError {}

fn decode_numeric(raw: &[u8]) -> Result<String, NumericDecodeError> {
    const POSITIVE: u16 = 0x0000;
    const NEGATIVE: u16 = 0x4000;
    const NAN: u16 = 0xC000;
    const POSITIVE_INFINITY: u16 = 0xD000;
    const NEGATIVE_INFINITY: u16 = 0xF000;

    if raw.len() < 8 || !(raw.len() - 8).is_multiple_of(2) {
        return Err(NumericDecodeError("invalid PostgreSQL NUMERIC payload"));
    }
    let ndigits = read_i16(raw, 0)?;
    let weight = read_i16(raw, 2)?;
    let sign = read_u16(raw, 4)?;
    let dscale = read_i16(raw, 6)?;
    if ndigits < 0 || dscale < 0 || raw.len() != 8 + ndigits as usize * 2 {
        return Err(NumericDecodeError("invalid PostgreSQL NUMERIC header"));
    }
    match sign {
        NAN => return Ok("NaN".to_owned()),
        POSITIVE_INFINITY => return Ok("Infinity".to_owned()),
        NEGATIVE_INFINITY => return Ok("-Infinity".to_owned()),
        POSITIVE | NEGATIVE => {}
        _ => return Err(NumericDecodeError("unknown PostgreSQL NUMERIC sign")),
    }

    let mut digits = Vec::with_capacity(ndigits as usize);
    for index in 0..ndigits as usize {
        let digit = read_i16(raw, 8 + index * 2)?;
        if !(0..10_000).contains(&digit) {
            return Err(NumericDecodeError("invalid PostgreSQL NUMERIC digit"));
        }
        digits.push(digit as u16);
    }

    let group_at = |group_weight: i32| -> u16 {
        let index = weight as i32 - group_weight;
        if index < 0 {
            return 0;
        }
        digits.get(index as usize).copied().unwrap_or(0)
    };

    let mut rendered = String::new();
    let is_zero = digits.iter().all(|digit| *digit == 0);
    if sign == NEGATIVE && !is_zero {
        rendered.push('-');
    }

    if weight < 0 {
        rendered.push('0');
    } else {
        for group_weight in (0..=weight as i32).rev() {
            let digit = group_at(group_weight);
            if group_weight == weight as i32 {
                rendered.push_str(&digit.to_string());
            } else {
                rendered.push_str(&format!("{digit:04}"));
            }
        }
    }

    if dscale > 0 {
        rendered.push('.');
        let fractional_groups = (dscale as usize).div_ceil(4);
        let mut fractional = String::with_capacity(fractional_groups * 4);
        for group in 1..=fractional_groups {
            fractional.push_str(&format!("{:04}", group_at(-(group as i32))));
        }
        rendered.push_str(&fractional[..dscale as usize]);
    }
    Ok(rendered)
}

fn read_i16(raw: &[u8], offset: usize) -> Result<i16, NumericDecodeError> {
    let bytes = raw
        .get(offset..offset + 2)
        .ok_or(NumericDecodeError("truncated PostgreSQL NUMERIC payload"))?;
    Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u16(raw: &[u8], offset: usize) -> Result<u16, NumericDecodeError> {
    let bytes = raw
        .get(offset..offset + 2)
        .ok_or(NumericDecodeError("truncated PostgreSQL NUMERIC payload"))?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

pub fn format_json_timestamp(value: OffsetDateTime) -> String {
    let rendered = value.format(&Rfc3339).expect("valid RFC3339 timestamp");
    if rendered.ends_with('Z') {
        let body = rendered.trim_end_matches('Z');
        if !body.contains('.') {
            return format!("{body}.000Z");
        }
        let (prefix, fraction) = body.rsplit_once('.').expect("fraction");
        return format!(
            "{prefix}.{fraction:0<3}Z",
            fraction = &fraction[..fraction.len().min(3)]
        );
    }
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_formatter_matches_node_date_json_shape() {
        let timestamp = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
        assert_eq!(format_json_timestamp(timestamp), "2023-11-14T22:13:20.000Z");
    }

    #[test]
    fn numeric_decoder_preserves_node_postgres_string_scale() {
        assert_eq!(
            decode_numeric(&numeric_payload(1, 0, 0x0000, 2, &[1500])).expect("whole numeric"),
            "1500.00"
        );
        assert_eq!(
            decode_numeric(&numeric_payload(2, 0, 0x0000, 4, &[12, 3400]))
                .expect("fractional numeric"),
            "12.3400"
        );
        assert_eq!(
            decode_numeric(&numeric_payload(1, -1, 0x0000, 4, &[625])).expect("subunit numeric"),
            "0.0625"
        );
        assert_eq!(
            decode_numeric(&numeric_payload(2, 1, 0x4000, 0, &[1, 2345]))
                .expect("negative numeric"),
            "-12345"
        );
        assert_eq!(
            decode_numeric(&numeric_payload(0, 0, 0x0000, 3, &[])).expect("zero numeric"),
            "0.000"
        );
    }

    fn numeric_payload(
        ndigits: i16,
        weight: i16,
        sign: u16,
        dscale: i16,
        digits: &[i16],
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&ndigits.to_be_bytes());
        payload.extend_from_slice(&weight.to_be_bytes());
        payload.extend_from_slice(&sign.to_be_bytes());
        payload.extend_from_slice(&dscale.to_be_bytes());
        for digit in digits {
            payload.extend_from_slice(&digit.to_be_bytes());
        }
        payload
    }
}
