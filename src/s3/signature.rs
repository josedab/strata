//! AWS Signature Version 4 validation for S3 requests.
//!
//! Implements signature validation compatible with AWS SDKs and tools.

use axum::http::{HeaderMap, Method, Uri};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Signature validation errors.
#[derive(Debug, thiserror::Error)]
pub enum SignatureError {
    #[error("Missing Authorization header")]
    MissingAuthHeader,
    #[error("Invalid Authorization header format")]
    InvalidAuthFormat,
    #[error("Missing required header: {0}")]
    MissingHeader(String),
    #[error("Invalid date format")]
    InvalidDate,
    #[error("Signature mismatch")]
    SignatureMismatch,
    #[error("Request expired")]
    RequestExpired,
    #[error("Unsupported algorithm: {0}")]
    UnsupportedAlgorithm(String),
}

/// Parsed components from the Authorization header.
#[derive(Debug, Clone)]
pub struct AuthorizationHeader {
    /// The algorithm (e.g., "AWS4-HMAC-SHA256").
    pub algorithm: String,
    /// The credential scope (access_key_id/date/region/service/aws4_request).
    pub credential: String,
    /// The access key ID extracted from credential.
    pub access_key_id: String,
    /// The date (YYYYMMDD) from credential scope.
    pub date: String,
    /// The region from credential scope.
    pub region: String,
    /// The service from credential scope (usually "s3").
    pub service: String,
    /// List of signed header names.
    pub signed_headers: Vec<String>,
    /// The signature to validate.
    pub signature: String,
}

impl AuthorizationHeader {
    /// Parse an Authorization header string.
    ///
    /// Expected format:
    /// `AWS4-HMAC-SHA256 Credential=AKID.../20230101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=...`
    pub fn parse(header: &str) -> Result<Self, SignatureError> {
        let header = header.trim();

        // Split algorithm from rest
        let (algorithm, rest) = header
            .split_once(' ')
            .ok_or(SignatureError::InvalidAuthFormat)?;

        if algorithm != "AWS4-HMAC-SHA256" {
            return Err(SignatureError::UnsupportedAlgorithm(algorithm.to_string()));
        }

        // Parse key=value pairs
        let mut credential = None;
        let mut signed_headers = None;
        let mut signature = None;

        for part in rest.split(", ") {
            let part = part.trim();
            if let Some((key, value)) = part.split_once('=') {
                match key {
                    "Credential" => credential = Some(value.to_string()),
                    "SignedHeaders" => signed_headers = Some(value.to_string()),
                    "Signature" => signature = Some(value.to_string()),
                    _ => {} // Ignore unknown fields
                }
            }
        }

        let credential = credential.ok_or(SignatureError::InvalidAuthFormat)?;
        let signed_headers_str = signed_headers.ok_or(SignatureError::InvalidAuthFormat)?;
        let signature = signature.ok_or(SignatureError::InvalidAuthFormat)?;

        // Parse credential: access_key_id/date/region/service/aws4_request
        let cred_parts: Vec<&str> = credential.split('/').collect();
        if cred_parts.len() != 5 {
            return Err(SignatureError::InvalidAuthFormat);
        }

        let access_key_id = cred_parts[0].to_string();
        let date = cred_parts[1].to_string();
        let region = cred_parts[2].to_string();
        let service = cred_parts[3].to_string();

        if cred_parts[4] != "aws4_request" {
            return Err(SignatureError::InvalidAuthFormat);
        }

        // Parse signed headers
        let signed_headers: Vec<String> = signed_headers_str
            .split(';')
            .map(|s| s.to_lowercase())
            .collect();

        Ok(Self {
            algorithm: algorithm.to_string(),
            credential,
            access_key_id,
            date,
            region,
            service,
            signed_headers,
            signature,
        })
    }
}

/// AWS Signature V4 validator.
pub struct SignatureValidator {
    /// The expected region.
    region: String,
    /// The service name (usually "s3").
    service: String,
}

impl SignatureValidator {
    /// Create a new signature validator.
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            service: "s3".to_string(),
        }
    }

    /// Validate a request signature.
    pub fn validate(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        body: &[u8],
        secret_key: &str,
    ) -> Result<AuthorizationHeader, SignatureError> {
        // Get and parse Authorization header
        let auth_header = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or(SignatureError::MissingAuthHeader)?;

        let auth = AuthorizationHeader::parse(auth_header)?;

        // Get the x-amz-date or Date header
        let amz_date = headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
            .or_else(|| headers.get("date").and_then(|v| v.to_str().ok()))
            .ok_or(SignatureError::MissingHeader("x-amz-date".to_string()))?;

        // Validate timestamp to prevent replay attacks
        validate_request_timestamp(amz_date)?;

        // Build canonical request
        let canonical_request = self.build_canonical_request(
            method,
            uri,
            headers,
            &auth.signed_headers,
            body,
        )?;

        // Build string to sign
        let string_to_sign = self.build_string_to_sign(
            amz_date,
            &auth.date,
            &auth.region,
            &canonical_request,
        );

        // Calculate expected signature
        let signing_key = self.derive_signing_key(
            secret_key,
            &auth.date,
            &auth.region,
        );
        let expected_signature = hex_encode(&hmac_sha256(&signing_key, &string_to_sign));

        // Compare signatures (constant-time)
        if !constant_time_compare(expected_signature.as_bytes(), auth.signature.as_bytes()) {
            return Err(SignatureError::SignatureMismatch);
        }

        Ok(auth)
    }

    /// Build the canonical request string.
    fn build_canonical_request(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        signed_headers: &[String],
        body: &[u8],
    ) -> Result<String, SignatureError> {
        // HTTP method
        let http_method = method.as_str();

        // Canonical URI (path)
        let canonical_uri = uri.path();
        let canonical_uri = if canonical_uri.is_empty() {
            "/"
        } else {
            canonical_uri
        };

        // Canonical query string (sorted)
        let canonical_query = self.build_canonical_query(uri.query().unwrap_or(""));

        // Canonical headers (sorted, lowercase)
        let canonical_headers = self.build_canonical_headers(headers, signed_headers)?;

        // Signed headers string
        let signed_headers_str = signed_headers.join(";");

        // Payload hash
        let payload_hash = hex_encode(&sha256(body));

        // Combine all parts
        Ok(format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            http_method,
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers_str,
            payload_hash
        ))
    }

    /// Build canonical query string (sorted parameters).
    fn build_canonical_query(&self, query: &str) -> String {
        if query.is_empty() {
            return String::new();
        }

        let mut params: BTreeMap<String, String> = BTreeMap::new();
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                params.insert(
                    uri_encode(key, true),
                    uri_encode(value, true),
                );
            } else if !pair.is_empty() {
                params.insert(uri_encode(pair, true), String::new());
            }
        }

        params
            .into_iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// Build canonical headers string.
    fn build_canonical_headers(
        &self,
        headers: &HeaderMap,
        signed_headers: &[String],
    ) -> Result<String, SignatureError> {
        let mut canonical = String::new();

        for header_name in signed_headers {
            let value = headers
                .get(header_name.as_str())
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| SignatureError::MissingHeader(header_name.clone()))?;

            // Trim and collapse whitespace
            let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
            canonical.push_str(&format!("{}:{}\n", header_name, value));
        }

        Ok(canonical)
    }

    /// Build the string to sign.
    fn build_string_to_sign(
        &self,
        amz_date: &str,
        date: &str,
        region: &str,
        canonical_request: &str,
    ) -> String {
        let canonical_hash = hex_encode(&sha256(canonical_request.as_bytes()));
        let scope = format!("{}/{}/{}/aws4_request", date, region, self.service);

        format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date, scope, canonical_hash
        )
    }

    /// Derive the signing key.
    fn derive_signing_key(&self, secret_key: &str, date: &str, region: &str) -> Vec<u8> {
        let k_secret = format!("AWS4{}", secret_key);
        let k_date = hmac_sha256(k_secret.as_bytes(), date);
        let k_region = hmac_sha256(&k_date, region);
        let k_service = hmac_sha256(&k_region, &self.service);
        hmac_sha256(&k_service, "aws4_request")
    }
}

/// Compute SHA-256 hash.
fn sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute HMAC-SHA256.
fn hmac_sha256(key: &[u8], data: &str) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

/// Hex encode bytes.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// URI encode a string according to AWS requirements.
fn uri_encode(input: &str, encode_slash: bool) -> String {
    let mut result = String::with_capacity(input.len() * 3);
    for c in input.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '-' | '~' | '.' => {
                result.push(c);
            }
            '/' if !encode_slash => {
                result.push(c);
            }
            _ => {
                for b in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    result
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_compare(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let result = a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    result == 0
}

/// Maximum allowed clock skew between client and server (15 minutes per AWS spec).
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(900);

/// Parse AWS date format (ISO 8601 basic format: YYYYMMDDTHHMMSSZ).
fn parse_amz_date(amz_date: &str) -> Result<SystemTime, SignatureError> {
    // Expected format: YYYYMMDDTHHMMSSZ (e.g., 20130524T000000Z)
    if amz_date.len() != 16 || !amz_date.ends_with('Z') || !amz_date.contains('T') {
        return Err(SignatureError::InvalidDate);
    }

    let year: i32 = amz_date[0..4].parse().map_err(|_| SignatureError::InvalidDate)?;
    let month: u32 = amz_date[4..6].parse().map_err(|_| SignatureError::InvalidDate)?;
    let day: u32 = amz_date[6..8].parse().map_err(|_| SignatureError::InvalidDate)?;
    let hour: u32 = amz_date[9..11].parse().map_err(|_| SignatureError::InvalidDate)?;
    let minute: u32 = amz_date[11..13].parse().map_err(|_| SignatureError::InvalidDate)?;
    let second: u32 = amz_date[13..15].parse().map_err(|_| SignatureError::InvalidDate)?;

    // Basic validation
    if month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59 {
        return Err(SignatureError::InvalidDate);
    }

    // Calculate Unix timestamp (simplified - doesn't account for all leap years correctly)
    let days_since_epoch = days_from_date(year, month, day);
    let seconds_since_epoch = (days_since_epoch as u64) * 86400
        + (hour as u64) * 3600
        + (minute as u64) * 60
        + (second as u64);

    Ok(UNIX_EPOCH + Duration::from_secs(seconds_since_epoch))
}

/// Calculate days since Unix epoch from a date.
fn days_from_date(year: i32, month: u32, day: u32) -> i64 {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe as i32 - 719468) as i64
}

/// Validate that the request timestamp is within allowed clock skew.
fn validate_request_timestamp(amz_date: &str) -> Result<(), SignatureError> {
    let request_time = parse_amz_date(amz_date)?;
    let now = SystemTime::now();

    // Check if request is too old
    let min_time = now.checked_sub(MAX_CLOCK_SKEW).unwrap_or(UNIX_EPOCH);
    if request_time < min_time {
        return Err(SignatureError::RequestExpired);
    }

    // Check if request is too far in the future
    let max_time = now + MAX_CLOCK_SKEW;
    if request_time > max_time {
        return Err(SignatureError::RequestExpired);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_authorization_header() {
        let header = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024";

        let auth = AuthorizationHeader::parse(header).unwrap();

        assert_eq!(auth.algorithm, "AWS4-HMAC-SHA256");
        assert_eq!(auth.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(auth.date, "20130524");
        assert_eq!(auth.region, "us-east-1");
        assert_eq!(auth.service, "s3");
        assert_eq!(auth.signed_headers, vec!["host", "range", "x-amz-date"]);
        assert_eq!(
            auth.signature,
            "fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024"
        );
    }

    #[test]
    fn test_parse_invalid_auth_header() {
        let result = AuthorizationHeader::parse("Basic dXNlcjpwYXNz");
        assert!(matches!(result, Err(SignatureError::UnsupportedAlgorithm(_))));

        let result = AuthorizationHeader::parse("AWS4-HMAC-SHA256");
        assert!(matches!(result, Err(SignatureError::InvalidAuthFormat)));
    }

    #[test]
    fn test_canonical_query_string() {
        let validator = SignatureValidator::new("us-east-1");

        // Empty query
        assert_eq!(validator.build_canonical_query(""), "");

        // Single param
        assert_eq!(validator.build_canonical_query("key=value"), "key=value");

        // Multiple params (should be sorted)
        let result = validator.build_canonical_query("b=2&a=1&c=3");
        assert_eq!(result, "a=1&b=2&c=3");

        // Params with special chars
        let result = validator.build_canonical_query("key=hello%20world");
        assert!(result.contains("key="));
    }

    #[test]
    fn test_uri_encode() {
        assert_eq!(uri_encode("hello", true), "hello");
        assert_eq!(uri_encode("hello world", true), "hello%20world");
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(uri_encode("a/b", false), "a/b");
    }

    #[test]
    fn test_signing_key_derivation() {
        let validator = SignatureValidator::new("us-east-1");
        let key = validator.derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
        );

        // Key should be 32 bytes (SHA-256 output)
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn test_constant_time_compare() {
        assert!(constant_time_compare(b"hello", b"hello"));
        assert!(!constant_time_compare(b"hello", b"world"));
        assert!(!constant_time_compare(b"hello", b"hell"));
    }

    #[test]
    fn test_parse_amz_date() {
        // Valid date
        let result = parse_amz_date("20130524T000000Z");
        assert!(result.is_ok());

        // Another valid date
        let result = parse_amz_date("20231215T143052Z");
        assert!(result.is_ok());

        // Invalid format - too short
        let result = parse_amz_date("20130524");
        assert!(matches!(result, Err(SignatureError::InvalidDate)));

        // Invalid format - no Z
        let result = parse_amz_date("20130524T000000X");
        assert!(matches!(result, Err(SignatureError::InvalidDate)));

        // Invalid format - no T
        let result = parse_amz_date("20130524-000000Z");
        assert!(matches!(result, Err(SignatureError::InvalidDate)));
    }

    #[test]
    fn test_validate_request_timestamp_current() {
        // Generate a current timestamp
        let now = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap();
        let secs = now.as_secs();

        // Calculate date/time components
        let days = secs / 86400;
        let remaining = secs % 86400;
        let hours = remaining / 3600;
        let minutes = (remaining % 3600) / 60;
        let seconds = remaining % 60;

        // Approximate year/month/day (simplified)
        let (year, month, day) = days_to_ymd(days as i64);

        let amz_date = format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            year, month, day, hours, minutes, seconds
        );

        // Current time should be valid
        let result = validate_request_timestamp(&amz_date);
        assert!(result.is_ok(), "Current timestamp should be valid: {:?}", result);
    }

    #[test]
    fn test_validate_request_timestamp_expired() {
        // Old timestamp (2010)
        let result = validate_request_timestamp("20100101T000000Z");
        assert!(matches!(result, Err(SignatureError::RequestExpired)));
    }

    // Helper for test: convert days since epoch to year/month/day
    fn days_to_ymd(days: i64) -> (i32, u32, u32) {
        let z = days + 719468;
        let era = if z >= 0 { z } else { z - 146096 } / 146097;
        let doe = (z - era * 146097) as u32;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let y = yoe as i64 + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        (y as i32, m, d)
    }
}
