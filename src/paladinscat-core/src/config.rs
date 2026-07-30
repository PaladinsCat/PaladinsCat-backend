use std::env;

use serde::Serialize;
use thiserror::Error;

const DEFAULT_DATABASE_POOL_MAX: usize = 20;
const MAX_DATABASE_POOL_MAX: usize = 50;
const DEFAULT_DATABASE_SLOW_QUERY_MS: u64 = 500;
const MIN_DATABASE_SLOW_QUERY_MS: u64 = 50;
const DEFAULT_DATABASE_STATEMENT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_REDIS_URL: &str = "redis://localhost:6379";
const DEFAULT_MEILISEARCH_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_HIREZ_RELAY_URL: &str = "http://127.0.0.1:3015";
const DEFAULT_HIREZ_RELAY_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_CORS_ORIGINS: &str = "https://paladinscat.com,https://www.paladinscat.com";
const DEFAULT_PUBLIC_API_RATE_LIMIT_PER_MINUTE: u64 = 300;
const DEFAULT_PUBLIC_API_GLOBAL_LIMIT_PER_MINUTE: u64 = 6_000;
const DEFAULT_ACCOUNT_AUTH_ATTEMPTS_PER_WINDOW: u64 = 10;
const DEFAULT_ACCOUNT_AUTH_WINDOW_MS: u64 = 15 * 60_000;
const DEFAULT_DEVELOPER_API_RATE_LIMIT_PER_MINUTE: u64 = 120;
const DEFAULT_DEVELOPER_API_CONCURRENCY_LIMIT: usize = 10;
const DEFAULT_DEPLOYMENT_REDIS_STARTUP_TIMEOUT_MS: u64 = 5_000;
const MIN_DEPLOYMENT_REDIS_STARTUP_TIMEOUT_MS: u64 = 250;
const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_API_HOST: &str = "127.0.0.1";
const DEFAULT_API_PORT: u16 = 3_310;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendConfig {
    pub environment: String,
    #[serde(skip_serializing)]
    pub database_url: String,
    pub database_pool_max: usize,
    pub database_slow_query_ms: u64,
    pub database_statement_timeout_ms: u64,
    pub database_default_transaction_read_only: bool,
    #[serde(skip_serializing)]
    pub redis_url: String,
    pub meilisearch_url: Option<String>,
    #[serde(skip_serializing)]
    pub meilisearch_api_key: Option<String>,
    pub meilisearch_timeout_ms: u64,
    pub hirez_relay_url: String,
    pub hirez_relay_timeout_ms: u64,
    pub cors_origins: Vec<String>,
    pub trust_cloudflare_headers: bool,
    pub public_api_rate_limit_per_minute: u64,
    pub public_api_global_limit_per_minute: u64,
    pub account_auth_attempts_per_window: u64,
    pub account_auth_window_ms: u64,
    #[serde(skip_serializing)]
    pub service_token: Option<String>,
    #[serde(skip_serializing)]
    pub previous_service_token: Option<String>,
    #[serde(skip_serializing)]
    pub admin_secret: Option<String>,
    #[serde(skip_serializing)]
    pub developer_api_key_sha256: Option<String>,
    #[serde(skip_serializing)]
    pub developer_api_key_sha256_file: Option<String>,
    pub developer_api_rate_limit_per_minute: u64,
    pub developer_api_concurrency_limit: usize,
    pub deployment_redis_startup_timeout_ms: u64,
    pub shutdown_drain_timeout_ms: u64,
    pub api_host: String,
    pub api_port: u16,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ConfigError {
    #[error("missing required environment variable: DATABASE_URL")]
    MissingDatabaseUrl,
    #[error("{0} must contain at least 32 bytes when configured")]
    ServiceTokenTooShort(&'static str),
    #[error("PALADINSCAT_SERVICE_TOKEN_PREVIOUS requires PALADINSCAT_SERVICE_TOKEN")]
    PreviousServiceTokenWithoutCurrent,
    #[error("PALADINSCAT_SERVICE_TOKEN_PREVIOUS must differ from the current token")]
    DuplicateServiceTokens,
    #[error("PALADINSCAT_DEVELOPER_API_KEY_SHA256 must contain exactly 64 hexadecimal characters")]
    InvalidDeveloperApiKeyHash,
}

impl BackendConfig {
    pub fn from_environment() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let database_url =
            nonempty(lookup("DATABASE_URL")).ok_or(ConfigError::MissingDatabaseUrl)?;
        let database_pool_max = parse_or(lookup("DB_POOL_MAX"), DEFAULT_DATABASE_POOL_MAX)
            .clamp(1, MAX_DATABASE_POOL_MAX);
        let database_slow_query_ms = parse_or(
            nonempty(lookup("DB_SLOW_QUERY_MS")).or_else(|| nonempty(lookup("SLOW_QUERY_MS"))),
            DEFAULT_DATABASE_SLOW_QUERY_MS,
        )
        .max(MIN_DATABASE_SLOW_QUERY_MS);
        let meilisearch_timeout_ms = parse_or(
            lookup("MEILISEARCH_TIMEOUT_MS"),
            DEFAULT_MEILISEARCH_TIMEOUT_MS,
        );
        let relay_url = nonempty(lookup("HIREZ_RELAY_URL"))
            .unwrap_or_else(|| DEFAULT_HIREZ_RELAY_URL.to_owned())
            .trim_end_matches('/')
            .to_owned();
        let service_token = nonempty(lookup("PALADINSCAT_SERVICE_TOKEN"));
        let previous_service_token = nonempty(lookup("PALADINSCAT_SERVICE_TOKEN_PREVIOUS"));
        validate_service_tokens(service_token.as_deref(), previous_service_token.as_deref())?;
        let developer_api_key_sha256 = nonempty(lookup("PALADINSCAT_DEVELOPER_API_KEY_SHA256"));
        if developer_api_key_sha256
            .as_deref()
            .is_some_and(|value| !is_sha256_hex(value))
        {
            return Err(ConfigError::InvalidDeveloperApiKeyHash);
        }

        Ok(Self {
            environment: nonempty(lookup("NODE_ENV"))
                .or_else(|| nonempty(lookup("PALADINSCAT_ENVIRONMENT")))
                .unwrap_or_else(|| "development".to_owned()),
            database_url,
            database_pool_max,
            database_slow_query_ms,
            database_statement_timeout_ms: DEFAULT_DATABASE_STATEMENT_TIMEOUT_MS,
            database_default_transaction_read_only: parse_true(lookup(
                "DATABASE_DEFAULT_TRANSACTION_READ_ONLY",
            )),
            redis_url: nonempty(lookup("REDIS_URL"))
                .unwrap_or_else(|| DEFAULT_REDIS_URL.to_owned()),
            meilisearch_url: nonempty(lookup("MEILISEARCH_URL")),
            meilisearch_api_key: nonempty(lookup("MEILISEARCH_API_KEY")),
            meilisearch_timeout_ms,
            hirez_relay_url: relay_url,
            hirez_relay_timeout_ms: parse_or(
                lookup("HIREZ_RELAY_TIMEOUT_MS"),
                DEFAULT_HIREZ_RELAY_TIMEOUT_MS,
            ),
            cors_origins: csv_values(lookup("CORS_ORIGINS"), DEFAULT_CORS_ORIGINS),
            trust_cloudflare_headers: parse_true(lookup("TRUST_CLOUDFLARE_HEADERS")),
            public_api_rate_limit_per_minute: positive_or(
                lookup("PUBLIC_API_RATE_LIMIT_PER_MINUTE"),
                DEFAULT_PUBLIC_API_RATE_LIMIT_PER_MINUTE,
            ),
            public_api_global_limit_per_minute: positive_or(
                lookup("PUBLIC_API_GLOBAL_LIMIT_PER_MINUTE"),
                DEFAULT_PUBLIC_API_GLOBAL_LIMIT_PER_MINUTE,
            ),
            account_auth_attempts_per_window: positive_or(
                lookup("ACCOUNT_AUTH_ATTEMPTS_PER_WINDOW"),
                DEFAULT_ACCOUNT_AUTH_ATTEMPTS_PER_WINDOW,
            ),
            account_auth_window_ms: positive_or(
                lookup("ACCOUNT_AUTH_WINDOW_MS"),
                DEFAULT_ACCOUNT_AUTH_WINDOW_MS,
            ),
            service_token,
            previous_service_token,
            admin_secret: nonempty(lookup("ADMIN_SECRET")),
            developer_api_key_sha256,
            developer_api_key_sha256_file: nonempty(lookup(
                "PALADINSCAT_DEVELOPER_API_KEY_SHA256_FILE",
            )),
            developer_api_rate_limit_per_minute: positive_or(
                lookup("DEVELOPER_API_RATE_LIMIT_PER_MINUTE"),
                DEFAULT_DEVELOPER_API_RATE_LIMIT_PER_MINUTE,
            ),
            developer_api_concurrency_limit: positive_or(
                lookup("DEVELOPER_API_CONCURRENCY_LIMIT"),
                DEFAULT_DEVELOPER_API_CONCURRENCY_LIMIT,
            ),
            deployment_redis_startup_timeout_ms: positive_or(
                lookup("DEPLOYMENT_REDIS_STARTUP_TIMEOUT_MS"),
                DEFAULT_DEPLOYMENT_REDIS_STARTUP_TIMEOUT_MS,
            )
            .max(MIN_DEPLOYMENT_REDIS_STARTUP_TIMEOUT_MS),
            shutdown_drain_timeout_ms: positive_or(
                lookup("SHUTDOWN_DRAIN_TIMEOUT_MS"),
                DEFAULT_SHUTDOWN_DRAIN_TIMEOUT_MS,
            ),
            api_host: nonempty(lookup("PALADINSCAT_RUST_API_HOST"))
                .unwrap_or_else(|| DEFAULT_API_HOST.to_owned()),
            api_port: parse_or(
                nonempty(lookup("PALADINSCAT_RUST_API_PORT")).or_else(|| nonempty(lookup("PORT"))),
                DEFAULT_API_PORT,
            ),
        })
    }
}

fn nonempty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn parse_or<T>(value: Option<String>, fallback: T) -> T
where
    T: std::str::FromStr,
{
    value
        .and_then(|value| value.trim().parse::<T>().ok())
        .unwrap_or(fallback)
}

fn positive_or<T>(value: Option<String>, fallback: T) -> T
where
    T: std::str::FromStr + PartialOrd + From<u8>,
{
    value
        .and_then(|value| value.trim().parse::<T>().ok())
        .filter(|value| *value > T::from(0))
        .unwrap_or(fallback)
}

fn parse_true(value: Option<String>) -> bool {
    value.as_deref() == Some("true")
}

fn csv_values(value: Option<String>, fallback: &str) -> Vec<String> {
    value
        .unwrap_or_else(|| fallback.to_owned())
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_service_tokens(
    current: Option<&str>,
    previous: Option<&str>,
) -> Result<(), ConfigError> {
    for (name, value) in [
        ("PALADINSCAT_SERVICE_TOKEN", current),
        ("PALADINSCAT_SERVICE_TOKEN_PREVIOUS", previous),
    ] {
        if value.is_some_and(|value| value.len() < 32) {
            return Err(ConfigError::ServiceTokenTooShort(name));
        }
    }
    if previous.is_some() && current.is_none() {
        return Err(ConfigError::PreviousServiceTokenWithoutCurrent);
    }
    if current.is_some() && current == previous {
        return Err(ConfigError::DuplicateServiceTokens);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn load_config(values: &[(&str, &str)]) -> Result<BackendConfig, ConfigError> {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>();
        BackendConfig::from_lookup(|name| values.get(name).cloned())
    }

    #[test]
    fn requires_the_existing_database_contract() {
        assert_eq!(load_config(&[]), Err(ConfigError::MissingDatabaseUrl));
    }

    #[test]
    fn matches_typescript_defaults_and_bounds() {
        let config = load_config(&[("DATABASE_URL", "postgres://fixture")]).expect("config");
        assert_eq!(config.database_pool_max, 20);
        assert_eq!(config.database_slow_query_ms, 500);
        assert_eq!(config.database_statement_timeout_ms, 30_000);
        assert!(!config.database_default_transaction_read_only);
        assert_eq!(config.redis_url, "redis://localhost:6379");
        assert_eq!(config.meilisearch_url, None);
        assert_eq!(config.meilisearch_api_key, None);
        assert_eq!(config.meilisearch_timeout_ms, 5_000);
        assert_eq!(config.hirez_relay_url, "http://127.0.0.1:3015");
        assert_eq!(config.hirez_relay_timeout_ms, 120_000);
        assert_eq!(
            config.cors_origins,
            ["https://paladinscat.com", "https://www.paladinscat.com"]
        );
        assert!(!config.trust_cloudflare_headers);
        assert_eq!(config.public_api_rate_limit_per_minute, 300);
        assert_eq!(config.public_api_global_limit_per_minute, 6_000);
        assert_eq!(config.account_auth_attempts_per_window, 10);
        assert_eq!(config.account_auth_window_ms, 15 * 60_000);
        assert_eq!(config.developer_api_rate_limit_per_minute, 120);
        assert_eq!(config.developer_api_concurrency_limit, 10);
        assert_eq!(config.deployment_redis_startup_timeout_ms, 5_000);
        assert_eq!(config.shutdown_drain_timeout_ms, 60_000);
        assert_eq!(config.api_host, "127.0.0.1");
        assert_eq!(config.api_port, 3_310);

        let bounded = load_config(&[
            ("DATABASE_URL", "postgres://fixture"),
            ("DB_POOL_MAX", "500"),
            ("DB_SLOW_QUERY_MS", "5"),
        ])
        .expect("bounded");
        assert_eq!(bounded.database_pool_max, 50);
        assert_eq!(bounded.database_slow_query_ms, 50);
    }

    #[test]
    fn new_names_take_precedence_but_legacy_aliases_remain_compatible() {
        let legacy = load_config(&[
            ("DATABASE_URL", "postgres://fixture"),
            ("SLOW_QUERY_MS", "650"),
            ("NODE_ENV", "production"),
        ])
        .expect("legacy");
        assert_eq!(legacy.database_slow_query_ms, 650);
        assert_eq!(legacy.environment, "production");

        let preferred = load_config(&[
            ("DATABASE_URL", "postgres://fixture"),
            ("SLOW_QUERY_MS", "650"),
            ("DB_SLOW_QUERY_MS", "700"),
            ("DATABASE_DEFAULT_TRANSACTION_READ_ONLY", "true"),
            ("PALADINSCAT_ENVIRONMENT", "staging"),
        ])
        .expect("preferred");
        assert_eq!(preferred.database_slow_query_ms, 700);
        assert!(preferred.database_default_transaction_read_only);
        assert_eq!(preferred.environment, "staging");
    }

    #[test]
    fn normalizes_optional_urls_and_invalid_numeric_values() {
        let config = load_config(&[
            ("DATABASE_URL", " postgres://fixture "),
            ("DB_POOL_MAX", "not-a-number"),
            ("MEILISEARCH_URL", " "),
            ("MEILISEARCH_TIMEOUT_MS", "invalid"),
            ("HIREZ_RELAY_URL", "http://hirezrelay:3015///"),
        ])
        .expect("config");
        assert_eq!(config.database_url, "postgres://fixture");
        assert_eq!(config.database_pool_max, 20);
        assert_eq!(config.meilisearch_url, None);
        assert_eq!(config.meilisearch_timeout_ms, 5_000);
        assert_eq!(config.hirez_relay_url, "http://hirezrelay:3015");
    }

    #[test]
    fn maps_foundation_security_and_lifecycle_environment() {
        let config = load_config(&[
            ("DATABASE_URL", "postgres://fixture"),
            (
                "CORS_ORIGINS",
                " https://one.example, ,https://two.example ",
            ),
            ("TRUST_CLOUDFLARE_HEADERS", "true"),
            ("PUBLIC_API_RATE_LIMIT_PER_MINUTE", "450"),
            ("PUBLIC_API_GLOBAL_LIMIT_PER_MINUTE", "9000"),
            ("ACCOUNT_AUTH_ATTEMPTS_PER_WINDOW", "12"),
            ("ACCOUNT_AUTH_WINDOW_MS", "120000"),
            ("PALADINSCAT_SERVICE_TOKEN", &"s".repeat(32)),
            ("PALADINSCAT_SERVICE_TOKEN_PREVIOUS", &"p".repeat(32)),
            ("ADMIN_SECRET", "operator"),
            ("PALADINSCAT_DEVELOPER_API_KEY_SHA256", &"a".repeat(64)),
            ("PALADINSCAT_DEVELOPER_API_KEY_SHA256_FILE", "secret/hash"),
            ("DEVELOPER_API_RATE_LIMIT_PER_MINUTE", "240"),
            ("DEVELOPER_API_CONCURRENCY_LIMIT", "20"),
            ("DEPLOYMENT_REDIS_STARTUP_TIMEOUT_MS", "100"),
            ("SHUTDOWN_DRAIN_TIMEOUT_MS", "75000"),
            ("MEILISEARCH_API_KEY", "meili-secret"),
            ("PORT", "3005"),
        ])
        .expect("foundation config");

        assert_eq!(
            config.cors_origins,
            ["https://one.example", "https://two.example"]
        );
        assert!(config.trust_cloudflare_headers);
        assert_eq!(config.public_api_rate_limit_per_minute, 450);
        assert_eq!(config.public_api_global_limit_per_minute, 9_000);
        assert_eq!(config.account_auth_attempts_per_window, 12);
        assert_eq!(config.account_auth_window_ms, 120_000);
        assert_eq!(config.service_token.as_deref(), Some(&*"s".repeat(32)));
        assert_eq!(
            config.previous_service_token.as_deref(),
            Some(&*"p".repeat(32))
        );
        assert_eq!(config.admin_secret.as_deref(), Some("operator"));
        assert_eq!(
            config.developer_api_key_sha256.as_deref(),
            Some(&*"a".repeat(64))
        );
        assert_eq!(
            config.developer_api_key_sha256_file.as_deref(),
            Some("secret/hash")
        );
        assert_eq!(config.developer_api_rate_limit_per_minute, 240);
        assert_eq!(config.developer_api_concurrency_limit, 20);
        assert_eq!(config.deployment_redis_startup_timeout_ms, 250);
        assert_eq!(config.shutdown_drain_timeout_ms, 75_000);
        assert_eq!(config.meilisearch_api_key.as_deref(), Some("meili-secret"));
        assert_eq!(config.api_port, 3_005);

        let serialized = serde_json::to_value(config).expect("serialize");
        for secret in [
            "databaseUrl",
            "redisUrl",
            "meilisearchApiKey",
            "serviceToken",
            "previousServiceToken",
            "adminSecret",
            "developerApiKeySha256",
            "developerApiKeySha256File",
        ] {
            assert_eq!(serialized.get(secret), None, "{secret} leaked");
        }
    }

    #[test]
    fn rejects_unsafe_service_token_rotation_and_developer_hashes() {
        assert_eq!(
            load_config(&[
                ("DATABASE_URL", "postgres://fixture"),
                ("PALADINSCAT_SERVICE_TOKEN", "short"),
            ]),
            Err(ConfigError::ServiceTokenTooShort(
                "PALADINSCAT_SERVICE_TOKEN"
            )),
        );
        assert_eq!(
            load_config(&[
                ("DATABASE_URL", "postgres://fixture"),
                ("PALADINSCAT_SERVICE_TOKEN_PREVIOUS", &"p".repeat(32)),
            ]),
            Err(ConfigError::PreviousServiceTokenWithoutCurrent),
        );
        assert_eq!(
            load_config(&[
                ("DATABASE_URL", "postgres://fixture"),
                ("PALADINSCAT_SERVICE_TOKEN", &"s".repeat(32)),
                ("PALADINSCAT_SERVICE_TOKEN_PREVIOUS", &"s".repeat(32)),
            ]),
            Err(ConfigError::DuplicateServiceTokens),
        );
        assert_eq!(
            load_config(&[
                ("DATABASE_URL", "postgres://fixture"),
                ("PALADINSCAT_DEVELOPER_API_KEY_SHA256", "not-a-hash"),
            ]),
            Err(ConfigError::InvalidDeveloperApiKeyHash),
        );
    }
}
