use std::{
    fs,
    net::{IpAddr, SocketAddr},
    sync::LazyLock,
};

use axum::http::HeaderMap;
use paladinscat_core::config::BackendConfig;
use regex::Regex;
use serde::Serialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

const VERSION_PREFIX: &str = "/v1";
const INTERNAL_REQUEST_HEADER: &str = "x-pc-internal-request";
const SERVICE_TOKEN_HEADER: &str = "x-paladinscat-service-token";

static KEY_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^pc_(?:live|test)_[A-Za-z0-9_-]{43,}$").expect("developer key regex")
});
static SAFE_PATHS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    [
        r"^/notifications$",
        r"^/operations/stats$",
        r"^/system/hirez-status$",
        r"^/hirez-status$",
        r"^/search/(?:universal|players|matches)$",
        r"^/reference/(?:champions|items|bounty-items|maps|tiers|regions|talents|queues|patches|cards|skins|abilities)(?:/\d+)?$",
        r"^/reference/lookup$",
        r"^/champions$",
        r"^/champions/(?:overview|tiers|top-winrate)$",
        r"^/champions/[^/]+$",
        r"^/champions/[^/]+/(?:page-data|patch-history|counters)$",
        r"^/champions/[^/]+/talents/\d+/page-data$",
        r"^/players/(?:overview|search|bulk)$",
        r"^/players/leaderboard/(?:class|champion-elo|performance)$",
        r"^/players/\d+$",
        r"^/players/\d+/(?:matches|champions|charts|loadouts|card-winrates)$",
        r"^/players/\d+/loadouts/decks/\d+$",
        r"^/player-ext/(?:name-history|merges|status|achievements)/\d+$",
        r"^/matches/(?:overview|batch|recent|search|bans|hourly-stats|compositions)$",
        r"^/matches/queue/\d+$",
        r"^/matches/fact/\d+$",
        r"^/matches/\d+$",
        r"^/live/matches$",
        r"^/live/matches/\d+$",
        r"^/live/ended$",
        r"^/stats(?:/.*)?$",
        r"^/ratings(?:/.*)?$",
        r"^/coplay(?:/.*)?$",
        r"^/meta/changelog$",
        r"^/meta/(?:items|talents|cards|compositions|top)(?:/\d+)?$",
    ]
    .into_iter()
    .map(|pattern| Regex::new(pattern).expect("safe developer API path regex"))
    .collect()
});
static GUARDED_WRITE_PATHS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![Regex::new(r"^/players/\d+/refresh$").expect("guarded developer API write regex")]
});

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeveloperApiDecision {
    pub attempted: bool,
    pub supported: bool,
    pub anonymous: bool,
    pub target_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<&'static str>,
}

#[derive(Clone)]
pub struct SecurityContext {
    pub environment: String,
    pub cors_origins: Vec<String>,
    pub trust_cloudflare_headers: bool,
    pub public_api_rate_limit_per_minute: u64,
    pub public_api_global_limit_per_minute: u64,
    pub account_auth_attempts_per_window: u64,
    pub account_auth_window_ms: u64,
    pub developer_api_rate_limit_per_minute: u64,
    pub developer_api_concurrency_limit: usize,
    service_token: Option<String>,
    previous_service_token: Option<String>,
    admin_secret: Option<String>,
    developer_key_hash: Option<[u8; 32]>,
    developer_key_identity: String,
    internal_request_token: String,
}

#[derive(Debug, Error)]
pub enum SecurityContextError {
    #[error("Unable to read PALADINSCAT_DEVELOPER_API_KEY_SHA256_FILE: {0}")]
    DeveloperKeyFile(String),
    #[error("PALADINSCAT_DEVELOPER_API_KEY_SHA256 must contain exactly 64 hexadecimal characters")]
    InvalidDeveloperKeyHash,
}

impl SecurityContext {
    pub fn from_config(config: &BackendConfig) -> Result<Self, SecurityContextError> {
        let configured_hash = match config.developer_api_key_sha256.as_deref() {
            Some(value) => Some(value.to_owned()),
            None => match config.developer_api_key_sha256_file.as_deref() {
                Some(path) => Some(
                    fs::read_to_string(path)
                        .map_err(|error| SecurityContextError::DeveloperKeyFile(error.to_string()))?
                        .trim()
                        .to_owned(),
                ),
                None => None,
            },
        };
        let developer_key_hash = configured_hash.as_deref().map(decode_sha256).transpose()?;
        let developer_key_identity = developer_key_hash
            .as_ref()
            .map(|hash| encode_hex(&hash[..8]))
            .unwrap_or_else(|| "unconfigured".to_owned());
        Ok(Self {
            environment: config.environment.clone(),
            cors_origins: config.cors_origins.clone(),
            trust_cloudflare_headers: config.trust_cloudflare_headers,
            public_api_rate_limit_per_minute: config.public_api_rate_limit_per_minute,
            public_api_global_limit_per_minute: config.public_api_global_limit_per_minute,
            account_auth_attempts_per_window: config.account_auth_attempts_per_window,
            account_auth_window_ms: config.account_auth_window_ms,
            developer_api_rate_limit_per_minute: config.developer_api_rate_limit_per_minute,
            developer_api_concurrency_limit: config.developer_api_concurrency_limit,
            service_token: config.service_token.clone(),
            previous_service_token: config.previous_service_token.clone(),
            admin_secret: config.admin_secret.clone(),
            developer_key_hash,
            developer_key_identity,
            internal_request_token: format!(
                "{}{}",
                Uuid::new_v4().simple(),
                Uuid::new_v4().simple()
            ),
        })
    }

    pub fn developer_key_configured(&self) -> bool {
        self.developer_key_hash.is_some()
    }

    pub fn developer_key_identity(&self) -> &str {
        &self.developer_key_identity
    }

    pub fn authenticate_developer_key(&self, candidate: &str) -> bool {
        let Some(expected_hash) = self.developer_key_hash.as_ref() else {
            return false;
        };
        if !KEY_PATTERN.is_match(candidate) {
            return false;
        }
        let candidate_hash: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
        bool::from(candidate_hash.ct_eq(expected_hash))
    }

    pub fn is_allowed_cors_origin(&self, origin: &str) -> bool {
        if self.cors_origins.iter().any(|allowed| allowed == origin) {
            return true;
        }
        if self.environment == "production" {
            return false;
        }
        Url::parse(origin)
            .is_ok_and(|url| matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1")))
    }

    pub fn is_internal_request(&self, headers: &HeaderMap) -> bool {
        header_text(headers, INTERNAL_REQUEST_HEADER)
            .is_some_and(|candidate| constant_time_equal(candidate, &self.internal_request_token))
    }

    pub fn is_service_request(&self, headers: &HeaderMap) -> bool {
        let Some(current) = self.service_token.as_deref() else {
            return false;
        };
        let Some(candidate) = service_token(headers) else {
            return false;
        };
        let current_matches = constant_time_equal(candidate, current);
        let previous_matches = self
            .previous_service_token
            .as_deref()
            .is_some_and(|previous| constant_time_equal(candidate, previous));
        current_matches || previous_matches
    }

    pub fn is_operator_request(&self, headers: &HeaderMap, authenticated_developer: bool) -> bool {
        if self.is_internal_request(headers)
            || self.is_service_request(headers)
            || authenticated_developer
        {
            return true;
        }
        self.admin_secret
            .as_deref()
            .zip(bearer_token_case_sensitive(headers))
            .is_some_and(|(expected, candidate)| constant_time_equal(candidate, expected))
    }

    pub fn service_auth_configured(&self) -> bool {
        self.service_token.is_some()
    }
}

pub fn resolve_developer_api_route(method: &str, raw_url: &str) -> DeveloperApiDecision {
    let Ok(base) = Url::parse("http://paladinscat.internal") else {
        unreachable!("static developer API base URL")
    };
    let Ok(parsed) = base.join(raw_url) else {
        return untouched_decision(raw_url);
    };
    let pathname = parsed.path();
    if pathname != VERSION_PREFIX && !pathname.starts_with("/v1/") {
        return untouched_decision(raw_url);
    }

    let path = pathname
        .strip_prefix(VERSION_PREFIX)
        .filter(|value| !value.is_empty())
        .unwrap_or("/");
    let normalized_method = method.to_ascii_uppercase();
    let anonymous_target = match path {
        "/health" => Some("/health"),
        "/version" => Some("/meta/version"),
        _ => None,
    };
    let guarded_write = GUARDED_WRITE_PATHS
        .iter()
        .any(|pattern| pattern.is_match(path));
    let safe_path = anonymous_target.is_some()
        || SAFE_PATHS.iter().any(|pattern| pattern.is_match(path))
        || guarded_write;
    if !safe_path {
        return rejected_decision(
            raw_url,
            404,
            "API_ROUTE_NOT_FOUND",
            "This endpoint is not part of the PaladinsCat v1 API.",
        );
    }
    let method_supported = matches!(normalized_method.as_str(), "GET" | "HEAD")
        || (normalized_method == "POST" && guarded_write);
    if !method_supported {
        return rejected_decision(
            raw_url,
            405,
            "METHOD_NOT_ALLOWED",
            "This method is not available for the requested PaladinsCat v1 endpoint.",
        );
    }
    if path == "/search/universal"
        && parsed
            .query_pairs()
            .find(|(key, _)| key == "remote")
            .is_some_and(|(_, value)| matches!(value.to_ascii_lowercase().as_str(), "true" | "1"))
    {
        return rejected_decision(
            raw_url,
            400,
            "REMOTE_LOOKUP_NOT_AVAILABLE",
            "Developer API searches are database-only and cannot request an upstream lookup.",
        );
    }
    let query = parsed
        .query()
        .map(|query| format!("?{query}"))
        .unwrap_or_default();
    DeveloperApiDecision {
        attempted: true,
        supported: true,
        anonymous: anonymous_target.is_some(),
        target_url: format!("{}{query}", anonymous_target.unwrap_or(path)),
        status_code: None,
        code: None,
        message: None,
    }
}

pub fn developer_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let header = header_text(headers, "authorization")?;
    let (scheme, value) = header.split_once(char::is_whitespace)?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

pub fn resolve_client_address(
    headers: &HeaderMap,
    fallback: Option<SocketAddr>,
    trust_cloudflare_headers: bool,
) -> String {
    if trust_cloudflare_headers
        && let Some(address) = header_text(headers, "cf-connecting-ip").and_then(normalize_ip)
    {
        return address;
    }
    if let Some(forwarded) = header_text(headers, "x-forwarded-for")
        && let Some(address) = forwarded.split(',').filter_map(normalize_ip).next_back()
    {
        return address;
    }
    fallback
        .map(|address| address.ip().to_string())
        .unwrap_or_else(|| "unknown".to_owned())
}

pub fn client_rate_limit_identity(address: &str) -> String {
    let digest = Sha256::digest(address.as_bytes());
    format!("client:{}", encode_hex(&digest[..16]))
}

pub fn is_sensitive_operator_route(method: &str, path: &str) -> bool {
    path.starts_with("/recovery/")
        || path == "/recovery"
        || path.starts_with("/api/hirez-raw-responses")
        || path.starts_with("/api/raw-responses")
        || path.starts_with("/matches/raw/")
        || path.starts_with("/players/raw/")
        || path == "/api-keys/status"
        || (method.eq_ignore_ascii_case("POST")
            && matches!(path, "/matches/pull" | "/matches/discover"))
}

pub fn is_service_only_route(path: &str) -> bool {
    path == "/players/discord" || path.starts_with("/players/discord/")
}

pub fn requires_configured_service_route(path: &str) -> bool {
    path.starts_with("/players/discord/")
}

fn untouched_decision(raw_url: &str) -> DeveloperApiDecision {
    DeveloperApiDecision {
        attempted: false,
        supported: false,
        anonymous: false,
        target_url: raw_url.to_owned(),
        status_code: None,
        code: None,
        message: None,
    }
}

fn rejected_decision(
    raw_url: &str,
    status_code: u16,
    code: &'static str,
    message: &'static str,
) -> DeveloperApiDecision {
    DeveloperApiDecision {
        attempted: true,
        supported: false,
        anonymous: false,
        target_url: raw_url.to_owned(),
        status_code: Some(status_code),
        code: Some(code),
        message: Some(message),
    }
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

fn service_token(headers: &HeaderMap) -> Option<&str> {
    header_text(headers, SERVICE_TOKEN_HEADER)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| bearer_token_case_sensitive(headers))
}

fn bearer_token_case_sensitive(headers: &HeaderMap) -> Option<&str> {
    header_text(headers, "authorization")?
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn normalize_ip(value: &str) -> Option<String> {
    let trimmed = value.trim().trim_start_matches('[').trim_end_matches(']');
    if let Ok(address) = trimmed.parse::<IpAddr>() {
        return Some(address.to_string());
    }
    trimmed
        .parse::<SocketAddr>()
        .ok()
        .filter(|address| address.is_ipv4())
        .map(|address| address.ip().to_string())
}

fn constant_time_equal(candidate: &str, expected: &str) -> bool {
    let candidate_digest: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
    let expected_digest: [u8; 32] = Sha256::digest(expected.as_bytes()).into();
    bool::from(candidate_digest.ct_eq(&expected_digest))
}

pub fn constant_time_equal_public(candidate: &str, expected: &str) -> bool {
    constant_time_equal(candidate, expected)
}

fn decode_sha256(value: &str) -> Result<[u8; 32], SecurityContextError> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SecurityContextError::InvalidDeveloperKeyHash);
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(decoded)
}

fn hex_nibble(value: u8) -> Result<u8, SecurityContextError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(SecurityContextError::InvalidDeveloperKeyHash),
    }
}

fn encode_hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn config(values: &[(&str, &str)]) -> BackendConfig {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>();
        BackendConfig::from_lookup(|name| values.get(name).cloned()).expect("config")
    }

    #[test]
    fn v1_resolver_matches_safe_reads_and_guarded_refresh() {
        assert_eq!(
            resolve_developer_api_route("GET", "/v1/stats/champions?limit=5"),
            DeveloperApiDecision {
                attempted: true,
                supported: true,
                anonymous: false,
                target_url: "/stats/champions?limit=5".to_owned(),
                status_code: None,
                code: None,
                message: None,
            }
        );
        assert_eq!(
            resolve_developer_api_route("GET", "/v1/health").target_url,
            "/health"
        );
        assert_eq!(
            resolve_developer_api_route("GET", "/v1/version").target_url,
            "/meta/version"
        );
        assert!(!resolve_developer_api_route("GET", "/stats/champions").attempted);
        assert_eq!(
            resolve_developer_api_route("GET", "/v1/admin/database").status_code,
            Some(404)
        );
        assert_eq!(
            resolve_developer_api_route("POST", "/v1/stats/champions").status_code,
            Some(405)
        );
        assert!(resolve_developer_api_route("POST", "/v1/players/716515038/refresh").supported);
        assert_eq!(
            resolve_developer_api_route("GET", "/v1/search/universal?q=name&remote=true").code,
            Some("REMOTE_LOOKUP_NOT_AVAILABLE")
        );
    }

    #[test]
    fn documented_v1_families_are_allowlisted_and_private_families_are_not() {
        for path in [
            "/notifications",
            "/operations/stats",
            "/system/hirez-status",
            "/search/universal?q=Androxus",
            "/reference/champions/2205",
            "/champions/androxus/page-data",
            "/players/716515038/matches",
            "/matches/1280340959",
            "/live/matches",
            "/stats/champions",
            "/ratings/queue/716515038",
            "/coplay/teammates/716515038",
            "/meta/items/1",
        ] {
            assert!(
                resolve_developer_api_route("GET", &format!("/v1{path}")).supported,
                "{path}"
            );
        }
        for path in [
            "/admin/database",
            "/recovery/pending",
            "/matches/raw/demo",
            "/players/discord",
            "/auth/me",
            "/builds",
            "/community/posts",
            "/api/raw-responses",
            "/esports/leagues",
        ] {
            assert!(
                !resolve_developer_api_route("GET", &format!("/v1{path}")).supported,
                "{path}"
            );
        }
    }

    #[test]
    fn client_address_uses_trusted_cloudflare_or_rightmost_proxy_address() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "203.0.113.99, 198.51.100.24".parse().expect("header"),
        );
        headers.insert("cf-connecting-ip", "192.0.2.10".parse().expect("header"));
        assert_eq!(
            resolve_client_address(
                &headers,
                Some("172.21.0.4:3005".parse().expect("peer")),
                false
            ),
            "198.51.100.24"
        );
        assert_eq!(
            resolve_client_address(
                &headers,
                Some("172.21.0.4:3005".parse().expect("peer")),
                true
            ),
            "192.0.2.10"
        );
        assert_eq!(
            client_rate_limit_identity("198.51.100.24").len(),
            "client:".len() + 32
        );
    }

    #[test]
    fn security_context_matches_key_token_rotation_and_cors_rules() {
        let developer_key = format!("pc_test_{}", "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc");
        let developer_hash = encode_hex(&Sha256::digest(developer_key.as_bytes()));
        let current = "s".repeat(32);
        let previous = "p".repeat(32);
        let context = SecurityContext::from_config(&config(&[
            ("DATABASE_URL", "postgres://fixture"),
            ("NODE_ENV", "production"),
            ("PALADINSCAT_SERVICE_TOKEN", &current),
            ("PALADINSCAT_SERVICE_TOKEN_PREVIOUS", &previous),
            ("ADMIN_SECRET", "operator"),
            ("PALADINSCAT_DEVELOPER_API_KEY_SHA256", &developer_hash),
        ]))
        .expect("security");
        assert!(context.authenticate_developer_key(&developer_key));
        assert!(!context.authenticate_developer_key("pc_test_short"));
        assert!(context.is_allowed_cors_origin("https://paladinscat.com"));
        assert!(!context.is_allowed_cors_origin("http://localhost:3000"));

        let mut headers = HeaderMap::new();
        headers.insert(
            SERVICE_TOKEN_HEADER,
            current.parse().expect("current token"),
        );
        assert!(context.is_service_request(&headers));
        headers.insert(
            SERVICE_TOKEN_HEADER,
            previous.parse().expect("previous token"),
        );
        assert!(context.is_service_request(&headers));
        headers.insert(
            "authorization",
            "Bearer operator".parse().expect("operator"),
        );
        headers.remove(SERVICE_TOKEN_HEADER);
        assert!(context.is_operator_request(&headers, false));
    }

    #[test]
    fn development_cors_accepts_only_local_loopback_hosts_beyond_allowlist() {
        let context =
            SecurityContext::from_config(&config(&[("DATABASE_URL", "postgres://fixture")]))
                .expect("security");
        assert!(context.is_allowed_cors_origin("http://localhost:3000"));
        assert!(context.is_allowed_cors_origin("http://127.0.0.1:3000"));
        // Node's WHATWG URL hostname includes brackets for IPv6, so the
        // current TypeScript allowlist also rejects this form.
        assert!(!context.is_allowed_cors_origin("http://[::1]:3000"));
        assert!(!context.is_allowed_cors_origin("https://example.com"));
    }
}
