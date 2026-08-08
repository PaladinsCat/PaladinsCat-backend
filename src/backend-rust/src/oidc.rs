//! Fail-closed OIDC access-token checks.  Discovery/JWKS URLs are never token-controlled.
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    aud: serde_json::Value,
    exp: usize,
    #[serde(default)]
    nbf: Option<usize>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct AccessIdentity { pub issuer: String, pub subject: String }

#[derive(Debug, Eq, PartialEq)]
pub enum TokenError { Header, Algorithm, TokenType, KeyId, Invalid }

/// Validates only RS256 access tokens. ID tokens and claims supplied by clients never grant roles.
pub fn validate_access_token(token: &str, issuer: &str, audience: &str, key: &DecodingKey) -> Result<AccessIdentity, TokenError> {
    let header = decode_header(token).map_err(|_| TokenError::Header)?;
    if header.alg != Algorithm::RS256 { return Err(TokenError::Algorithm); }
    if header.typ.as_deref() != Some("at+jwt") { return Err(TokenError::TokenType); }
    if header.kid.as_deref().filter(|v| !v.is_empty()).is_none() { return Err(TokenError::KeyId); }
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[issuer]);
    validation.validate_nbf = true;
    let decoded = decode::<Claims>(token, key, &validation).map_err(|_| TokenError::Invalid)?;
    let audience_matches = match decoded.claims.aud {
        serde_json::Value::String(value) => value == audience,
        serde_json::Value::Array(values) => values.iter().any(|v| v.as_str() == Some(audience)),
        _ => false,
    };
    if !audience_matches || decoded.claims.iss != issuer || decoded.claims.sub.is_empty() || decoded.claims.exp == 0 || decoded.claims.nbf.is_some_and(|n| n > decoded.claims.exp) { return Err(TokenError::Invalid); }
    Ok(AccessIdentity { issuer: decoded.claims.iss, subject: decoded.claims.sub })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    #[test]
    fn rejects_malformed_and_unsigned_tokens_before_identity_lookup() {
        let key = DecodingKey::from_secret(b"not-used");
        assert_eq!(validate_access_token("x.y.z", "https://issuer", "api", &key), Err(TokenError::Header));
        let token = encode(&Header::new(Algorithm::HS256), &serde_json::json!({"sub":"u"}), &EncodingKey::from_secret(b"test")).expect("test token");
        assert_eq!(validate_access_token(&token, "https://issuer", "api", &key), Err(TokenError::Algorithm));
        let token = format!("{}.e30.signature", URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","typ":"JWT","kid":"x"}"#));
        assert_eq!(validate_access_token(&token, "https://issuer", "api", &key), Err(TokenError::TokenType));
    }
}
