//! Fail-closed OIDC access-token checks.  Discovery/JWKS URLs are never token-controlled.
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use url::Url;

#[derive(Debug, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    aud: serde_json::Value,
    exp: usize,
    iat: usize,
    #[serde(default)]
    nbf: Option<usize>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct AccessIdentity {
    pub issuer: String,
    pub subject: String,
}

#[derive(Debug, Eq, PartialEq)]
pub enum TokenError {
    Header,
    Algorithm,
    TokenType,
    KeyId,
    Invalid,
}

#[derive(Clone)]
pub struct OidcVerifier {
    issuer: String,
    audience: String,
    jwks_url: Url,
    client: reqwest::Client,
    cache: Arc<Mutex<JwksCache>>,
}
struct JwksCache {
    keys: HashMap<String, DecodingKey>,
    expires: Instant,
    last_unknown_refresh: Instant,
}
#[derive(Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}
#[derive(Deserialize)]
struct Jwk {
    kid: String,
    kty: String,
    #[serde(rename = "use")]
    usage: Option<String>,
    alg: Option<String>,
    n: String,
    e: String,
}

impl OidcVerifier {
    pub fn new(issuer: String, audience: String) -> Result<Self, TokenError> {
        let base = Url::parse(&issuer).map_err(|_| TokenError::Invalid)?;
        if base.scheme() != "https"
            || !base.username().is_empty()
            || base.password().is_some()
            || !matches!(base.port(), None | Some(443))
            || base.query().is_some()
            || base.fragment().is_some()
            || !base.path().starts_with("/realms/")
        {
            return Err(TokenError::Invalid);
        }
        let jwks_url = Url::parse(&format!(
            "{}/protocol/openid-connect/certs",
            issuer.trim_end_matches('/')
        ))
        .map_err(|_| TokenError::Invalid)?;
        if jwks_url.scheme() != "https" || jwks_url.host_str() != base.host_str() {
            return Err(TokenError::Invalid);
        }
        Ok(Self {
            issuer,
            audience,
            jwks_url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| TokenError::Invalid)?,
            cache: Arc::new(Mutex::new(JwksCache {
                keys: HashMap::new(),
                expires: Instant::now(),
                last_unknown_refresh: Instant::now() - Duration::from_secs(61),
            })),
        })
    }
    pub async fn validate(&self, token: &str) -> Result<AccessIdentity, TokenError> {
        let header = decode_header(token).map_err(|_| TokenError::Header)?;
        let kid = header
            .kid
            .filter(|v| !v.is_empty())
            .ok_or(TokenError::KeyId)?;
        let key = self.key(&kid).await?;
        validate_access_token(token, &self.issuer, &self.audience, &key)
    }
    async fn key(&self, kid: &str) -> Result<DecodingKey, TokenError> {
        {
            let cache = self.cache.lock().await;
            if cache.expires > Instant::now() {
                if let Some(key) = cache.keys.get(kid) {
                    return Ok(key.clone());
                }
                if cache.last_unknown_refresh.elapsed() < Duration::from_secs(60) {
                    return Err(TokenError::KeyId);
                }
            }
        }
        let response = self
            .client
            .get(self.jwks_url.clone())
            .send()
            .await
            .map_err(|_| TokenError::Invalid)?;
        if !response.status().is_success()
            || response.content_length().is_some_and(|size| size > 262_144)
        {
            return Err(TokenError::Invalid);
        }
        let document: Jwks = response.json().await.map_err(|_| TokenError::Invalid)?;
        let mut keys = HashMap::new();
        for jwk in document.keys.into_iter().filter(|jwk| {
            jwk.kty == "RSA"
                && jwk.usage.as_deref().is_none_or(|u| u == "sig")
                && jwk.alg.as_deref().is_none_or(|a| a == "RS256")
        }) {
            if let Ok(key) = DecodingKey::from_rsa_components(&jwk.n, &jwk.e) {
                keys.insert(jwk.kid, key);
            }
        }
        let mut cache = self.cache.lock().await;
        cache.keys = keys;
        cache.expires = Instant::now() + Duration::from_secs(300);
        cache.last_unknown_refresh = Instant::now();
        cache.keys.get(kid).cloned().ok_or(TokenError::KeyId)
    }
}

/// Validates only RS256 access tokens. ID tokens and claims supplied by clients never grant roles.
pub fn validate_access_token(
    token: &str,
    issuer: &str,
    audience: &str,
    key: &DecodingKey,
) -> Result<AccessIdentity, TokenError> {
    let header = decode_header(token).map_err(|_| TokenError::Header)?;
    if header.alg != Algorithm::RS256 {
        return Err(TokenError::Algorithm);
    }
    if !matches!(header.typ.as_deref(), Some("at+jwt" | "JWT")) {
        return Err(TokenError::TokenType);
    }
    if header.kid.as_deref().filter(|v| !v.is_empty()).is_none() {
        return Err(TokenError::KeyId);
    }
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[issuer]);
    validation.validate_nbf = true;
    let decoded = decode::<Claims>(token, key, &validation).map_err(|_| TokenError::Invalid)?;
    let audience_matches = match decoded.claims.aud {
        serde_json::Value::String(value) => value == audience,
        serde_json::Value::Array(values) => values.iter().any(|v| v.as_str() == Some(audience)),
        _ => false,
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TokenError::Invalid)?
        .as_secs() as usize;
    if !audience_matches
        || decoded.claims.iss != issuer
        || decoded.claims.sub.is_empty()
        || decoded.claims.exp == 0
        || decoded.claims.iat > now.saturating_add(60)
        || decoded.claims.nbf.is_some_and(|n| n > decoded.claims.exp)
    {
        return Err(TokenError::Invalid);
    }
    Ok(AccessIdentity {
        issuer: decoded.claims.iss,
        subject: decoded.claims.sub,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use jsonwebtoken::{EncodingKey, Header, encode};
    #[test]
    fn rejects_malformed_and_unsigned_tokens_before_identity_lookup() {
        let key = DecodingKey::from_secret(b"not-used");
        assert_eq!(
            validate_access_token("x.y.z", "https://issuer", "api", &key),
            Err(TokenError::Header)
        );
        let token = encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({"sub":"u"}),
            &EncodingKey::from_secret(b"test"),
        )
        .expect("test token");
        assert_eq!(
            validate_access_token(&token, "https://issuer", "api", &key),
            Err(TokenError::Algorithm)
        );
        let token = format!(
            "{}.e30.signature",
            URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","typ":"id+jwt","kid":"x"}"#)
        );
        assert_eq!(
            validate_access_token(&token, "https://issuer", "api", &key),
            Err(TokenError::TokenType)
        );
    }
}
