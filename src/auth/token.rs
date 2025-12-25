//! Token-based authentication.
//!
//! Provides JWT token creation and validation.

use crate::auth::AuthInfo;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Token validation errors.
#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("Invalid token format")]
    InvalidFormat,
    #[error("Token signature invalid")]
    InvalidSignature,
    #[error("Token expired")]
    Expired,
    #[error("Token not yet valid")]
    NotYetValid,
    #[error("Missing required claim: {0}")]
    MissingClaim(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
}

/// Minimum secret length for security.
const MIN_SECRET_LENGTH: usize = 32;

/// Token configuration.
#[derive(Debug, Clone)]
pub struct TokenConfig {
    /// Secret key for signing tokens.
    pub secret: String,
    /// Token validity duration.
    pub validity: Duration,
    /// Issuer claim.
    pub issuer: String,
    /// Audience claim.
    pub audience: String,
}

impl TokenConfig {
    /// Create a new token configuration with the given secret.
    ///
    /// # Panics
    /// Panics if the secret is empty. Use `try_new` for fallible construction.
    pub fn new(secret: impl Into<String>) -> Self {
        Self::try_new(secret).expect("Token secret must not be empty")
    }

    /// Create a new token configuration with validation.
    ///
    /// Returns an error if the secret is empty.
    pub fn try_new(secret: impl Into<String>) -> Result<Self, TokenError> {
        let secret = secret.into();
        if secret.is_empty() {
            return Err(TokenError::InvalidConfiguration(
                "Token secret must not be empty".to_string(),
            ));
        }
        if secret.len() < MIN_SECRET_LENGTH {
            tracing::warn!(
                "Token secret is shorter than {} bytes. Consider using a longer secret for production.",
                MIN_SECRET_LENGTH
            );
        }
        Ok(Self {
            secret,
            validity: Duration::from_secs(3600), // 1 hour
            issuer: "strata".to_string(),
            audience: "strata-api".to_string(),
        })
    }

    /// Set the token validity duration.
    pub fn with_validity(mut self, validity: Duration) -> Self {
        self.validity = validity;
        self
    }

    /// Set the issuer claim.
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = issuer.into();
        self
    }

    /// Set the audience claim.
    pub fn with_audience(mut self, audience: impl Into<String>) -> Self {
        self.audience = audience.into();
        self
    }

    /// Create a configuration for testing only.
    /// This uses a predictable secret and should NEVER be used in production.
    #[cfg(test)]
    pub fn for_testing() -> Self {
        Self {
            secret: "test-secret-only-for-unit-tests-not-production".to_string(),
            validity: Duration::from_secs(3600),
            issuer: "strata".to_string(),
            audience: "strata-api".to_string(),
        }
    }
}

/// JWT claims.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Claims {
    /// Subject (user ID).
    pub sub: String,
    /// Issuer.
    pub iss: String,
    /// Audience.
    pub aud: String,
    /// Expiration time (Unix timestamp).
    pub exp: u64,
    /// Issued at (Unix timestamp).
    pub iat: u64,
    /// Not before (Unix timestamp).
    pub nbf: u64,
    /// Username (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// Roles.
    #[serde(default)]
    pub roles: Vec<String>,
    /// Is service account.
    #[serde(default)]
    pub is_service: bool,
}

impl Claims {
    /// Create new claims for a user.
    pub fn new(user_id: impl Into<String>, config: &TokenConfig) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            sub: user_id.into(),
            iss: config.issuer.clone(),
            aud: config.audience.clone(),
            exp: now + config.validity.as_secs(),
            iat: now,
            nbf: now,
            username: None,
            roles: vec![],
            is_service: false,
        }
    }

    /// Set username.
    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Set roles.
    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.roles = roles;
        self
    }

    /// Mark as service account.
    pub fn as_service(mut self) -> Self {
        self.is_service = true;
        self
    }

    /// Convert to AuthInfo.
    pub fn to_auth_info(&self) -> AuthInfo {
        AuthInfo {
            user_id: self.sub.clone(),
            username: self.username.clone(),
            roles: self.roles.clone(),
            is_service: self.is_service,
        }
    }
}

/// A signed token.
#[derive(Debug, Clone)]
pub struct Token {
    /// The encoded token string.
    raw: String,
    /// The decoded claims.
    claims: Claims,
}

impl Token {
    /// Create a new token from claims.
    ///
    /// Returns an error if claims cannot be serialized.
    pub fn try_new(claims: Claims, secret: &str) -> Result<Self, TokenError> {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let claims_json = serde_json::to_string(&claims)
            .map_err(|e| TokenError::InvalidConfiguration(format!("Failed to serialize claims: {}", e)))?;
        let payload = URL_SAFE_NO_PAD.encode(&claims_json);

        let signature_input = format!("{}.{}", header, payload);
        let signature = compute_hmac_sha256(&signature_input, secret);
        let signature_b64 = URL_SAFE_NO_PAD.encode(&signature);

        let raw = format!("{}.{}.{}", header, payload, signature_b64);

        Ok(Self { raw, claims })
    }

    /// Create a new token from claims.
    ///
    /// # Panics
    /// Panics if claims cannot be serialized. Use `try_new` for fallible construction.
    pub fn new(claims: Claims, secret: &str) -> Self {
        Self::try_new(claims, secret).expect("Failed to create token")
    }

    /// Parse and validate a token.
    pub fn parse(raw: &str, secret: &str, config: &TokenConfig) -> Result<Self, TokenError> {
        let parts: Vec<&str> = raw.split('.').collect();
        if parts.len() != 3 {
            return Err(TokenError::InvalidFormat);
        }

        // Verify signature using constant-time comparison
        let signature_input = format!("{}.{}", parts[0], parts[1]);
        let expected_signature = compute_hmac_sha256(&signature_input, secret);
        let actual_signature = URL_SAFE_NO_PAD
            .decode(parts[2])
            .map_err(|_| TokenError::InvalidFormat)?;

        // Use constant-time comparison to prevent timing attacks
        if !constant_time_compare(&expected_signature, &actual_signature) {
            return Err(TokenError::InvalidSignature);
        }

        // Decode and validate header to ensure algorithm matches
        let header_json = URL_SAFE_NO_PAD
            .decode(parts[0])
            .map_err(|_| TokenError::InvalidFormat)?;
        let header: serde_json::Value =
            serde_json::from_slice(&header_json).map_err(|_| TokenError::InvalidFormat)?;

        // Verify algorithm to prevent algorithm confusion attacks
        if header.get("alg").and_then(|v| v.as_str()) != Some("HS256") {
            return Err(TokenError::InvalidFormat);
        }

        // Decode claims
        let claims_bytes = URL_SAFE_NO_PAD
            .decode(parts[1])
            .map_err(|_| TokenError::InvalidFormat)?;
        let claims: Claims =
            serde_json::from_slice(&claims_bytes).map_err(|_| TokenError::InvalidFormat)?;

        // Validate issuer and audience
        if claims.iss != config.issuer {
            return Err(TokenError::MissingClaim("iss".to_string()));
        }
        if claims.aud != config.audience {
            return Err(TokenError::MissingClaim("aud".to_string()));
        }

        // Validate time
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if claims.exp < now {
            return Err(TokenError::Expired);
        }

        if claims.nbf > now {
            return Err(TokenError::NotYetValid);
        }

        Ok(Self {
            raw: raw.to_string(),
            claims,
        })
    }

    /// Get the raw token string.
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Get the claims.
    pub fn claims(&self) -> &Claims {
        &self.claims
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raw)
    }
}

/// Token validator.
#[derive(Clone)]
pub struct TokenValidator {
    config: TokenConfig,
}

impl TokenValidator {
    /// Create a new token validator.
    pub fn new(config: TokenConfig) -> Self {
        Self { config }
    }

    /// Create a token for a user.
    pub fn create_token(&self, claims: Claims) -> Token {
        Token::new(claims, &self.config.secret)
    }

    /// Validate a token string.
    pub fn validate(&self, token: &str) -> Result<AuthInfo, TokenError> {
        let token = Token::parse(token, &self.config.secret, &self.config)?;
        Ok(token.claims().to_auth_info())
    }

    /// Create new claims.
    pub fn new_claims(&self, user_id: impl Into<String>) -> Claims {
        Claims::new(user_id, &self.config)
    }
}

// Crypto helper functions using standard libraries

/// Compute HMAC-SHA256 signature using the hmac crate.
fn compute_hmac_sha256(message: &str, key: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_compare(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    // XOR all bytes and accumulate - takes same time regardless of where mismatch occurs
    let result = a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    result == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_claims_creation() {
        let config = TokenConfig::for_testing();
        let claims = Claims::new("user123", &config);

        assert_eq!(claims.sub, "user123");
        assert_eq!(claims.iss, "strata");
        assert!(claims.exp > claims.iat);
    }

    #[test]
    fn test_token_creation() {
        let config = TokenConfig::for_testing();
        let claims = Claims::new("user123", &config);
        let token = Token::new(claims.clone(), &config.secret);

        assert!(!token.as_str().is_empty());
        assert!(token.as_str().contains('.'));
    }

    #[test]
    fn test_token_validation() {
        let config = TokenConfig::for_testing();
        let claims = Claims::new("user123", &config).with_roles(vec!["user".to_string()]);
        let token = Token::new(claims, &config.secret);

        let parsed = Token::parse(token.as_str(), &config.secret, &config);
        assert!(parsed.is_ok());

        let parsed = parsed.unwrap();
        assert_eq!(parsed.claims().sub, "user123");
        assert_eq!(parsed.claims().roles, vec!["user".to_string()]);
    }

    #[test]
    fn test_token_invalid_signature() {
        let config = TokenConfig::for_testing();
        let claims = Claims::new("user123", &config);
        let token = Token::new(claims, &config.secret);

        // Try to validate with wrong secret
        let result = Token::parse(token.as_str(), "wrong-secret", &config);
        assert!(matches!(result, Err(TokenError::InvalidSignature)));
    }

    #[test]
    fn test_token_validator() {
        let config = TokenConfig::for_testing();
        let validator = TokenValidator::new(config);

        let claims = validator.new_claims("user123").with_username("test_user");
        let token = validator.create_token(claims);

        let auth = validator.validate(token.as_str());
        assert!(auth.is_ok());

        let auth = auth.unwrap();
        assert_eq!(auth.user_id, "user123");
        assert_eq!(auth.username, Some("test_user".to_string()));
    }

    #[test]
    fn test_base64_roundtrip() {
        let original = "Hello, World!";
        let encoded = URL_SAFE_NO_PAD.encode(original);
        let decoded = URL_SAFE_NO_PAD.decode(&encoded).unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), original);
    }

    #[test]
    fn test_config_validation() {
        // Empty secret should fail
        let result = TokenConfig::try_new("");
        assert!(matches!(result, Err(TokenError::InvalidConfiguration(_))));

        // Valid secret should succeed
        let result = TokenConfig::try_new("my-secret-key");
        assert!(result.is_ok());
    }

    #[test]
    fn test_algorithm_verification() {
        let config = TokenConfig::for_testing();
        let claims = Claims::new("user123", &config);
        let token = Token::new(claims, &config.secret);

        // Valid token should parse
        let result = Token::parse(token.as_str(), &config.secret, &config);
        assert!(result.is_ok());

        // Tampered algorithm header should fail
        let parts: Vec<&str> = token.as_str().split('.').collect();
        let bad_header = URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
        let tampered = format!("{}.{}.{}", bad_header, parts[1], parts[2]);
        let result = Token::parse(&tampered, &config.secret, &config);
        assert!(result.is_err());
    }
}
