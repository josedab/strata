//! S3 presigned URL generation and validation.
//!
//! Presigned URLs allow temporary access to S3 objects without requiring credentials.
//! The signature is embedded in the URL query parameters.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

/// Default expiration time for presigned URLs (15 minutes).
pub const DEFAULT_EXPIRATION: Duration = Duration::from_secs(900);

/// Maximum expiration time for presigned URLs (7 days).
pub const MAX_EXPIRATION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Minimum expiration time for presigned URLs (1 second).
pub const MIN_EXPIRATION: Duration = Duration::from_secs(1);

/// Errors that can occur during presigned URL operations.
#[derive(Debug, thiserror::Error)]
pub enum PresignedError {
    #[error("URL expired")]
    Expired,
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Missing required parameter: {0}")]
    MissingParameter(String),
    #[error("Invalid expiration: {0}")]
    InvalidExpiration(String),
    #[error("Invalid date format")]
    InvalidDate,
}

/// Presigned URL generator for S3 operations.
pub struct PresignedUrlGenerator {
    /// AWS access key ID.
    access_key_id: String,
    /// AWS secret access key.
    secret_key: String,
    /// AWS region.
    region: String,
    /// Service name (always "s3").
    service: String,
    /// Base URL for the S3 endpoint.
    endpoint: String,
}

impl PresignedUrlGenerator {
    /// Create a new presigned URL generator.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_key: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_key: secret_key.into(),
            region: region.into(),
            service: "s3".to_string(),
            endpoint: endpoint.into(),
        }
    }

    /// Generate a presigned URL for GET (download) operation.
    pub fn presign_get(
        &self,
        bucket: &str,
        key: &str,
        expiration: Duration,
    ) -> Result<String, PresignedError> {
        self.presign("GET", bucket, key, expiration, None)
    }

    /// Generate a presigned URL for PUT (upload) operation.
    pub fn presign_put(
        &self,
        bucket: &str,
        key: &str,
        expiration: Duration,
        content_type: Option<&str>,
    ) -> Result<String, PresignedError> {
        self.presign("PUT", bucket, key, expiration, content_type)
    }

    /// Generate a presigned URL for DELETE operation.
    pub fn presign_delete(
        &self,
        bucket: &str,
        key: &str,
        expiration: Duration,
    ) -> Result<String, PresignedError> {
        self.presign("DELETE", bucket, key, expiration, None)
    }

    /// Generate a presigned URL for the given operation.
    fn presign(
        &self,
        method: &str,
        bucket: &str,
        key: &str,
        expiration: Duration,
        content_type: Option<&str>,
    ) -> Result<String, PresignedError> {
        // Validate expiration
        if expiration < MIN_EXPIRATION {
            return Err(PresignedError::InvalidExpiration(
                "Expiration too short".to_string(),
            ));
        }
        if expiration > MAX_EXPIRATION {
            return Err(PresignedError::InvalidExpiration(
                "Expiration exceeds 7 days".to_string(),
            ));
        }

        let now = SystemTime::now();
        let amz_date = format_amz_date(now);
        let date_stamp = &amz_date[..8]; // YYYYMMDD

        // Build credential scope
        let credential_scope = format!(
            "{}/{}/{}/aws4_request",
            date_stamp, self.region, self.service
        );

        // Build path
        let path = format!("/{}/{}", bucket, key);

        // Build signed headers
        let mut signed_headers = vec!["host".to_string()];
        if content_type.is_some() {
            signed_headers.push("content-type".to_string());
        }
        let signed_headers_str = signed_headers.join(";");

        // Build canonical query string
        let mut query_params: BTreeMap<String, String> = BTreeMap::new();
        query_params.insert(
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        );
        query_params.insert(
            "X-Amz-Credential".to_string(),
            uri_encode(&format!("{}/{}", self.access_key_id, credential_scope), true),
        );
        query_params.insert("X-Amz-Date".to_string(), amz_date.clone());
        query_params.insert(
            "X-Amz-Expires".to_string(),
            expiration.as_secs().to_string(),
        );
        query_params.insert(
            "X-Amz-SignedHeaders".to_string(),
            signed_headers_str.clone(),
        );

        let canonical_query = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        // Extract host from endpoint
        let host = self
            .endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .split('/')
            .next()
            .unwrap_or(&self.endpoint);

        // Build canonical headers
        let mut canonical_headers = format!("host:{}\n", host);
        if let Some(ct) = content_type {
            canonical_headers.push_str(&format!("content-type:{}\n", ct));
        }

        // Build canonical request
        // For presigned URLs, payload hash is "UNSIGNED-PAYLOAD"
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
            method,
            uri_encode(&path, false),
            canonical_query,
            canonical_headers,
            signed_headers_str
        );

        // Build string to sign
        let canonical_hash = hex_encode(&sha256(canonical_request.as_bytes()));
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date, credential_scope, canonical_hash
        );

        // Derive signing key and calculate signature
        let signing_key = self.derive_signing_key(date_stamp);
        let signature = hex_encode(&hmac_sha256(&signing_key, &string_to_sign));

        // Build final URL
        let url = format!(
            "{}{}?{}&X-Amz-Signature={}",
            self.endpoint, path, canonical_query, signature
        );

        Ok(url)
    }

    /// Derive the signing key.
    fn derive_signing_key(&self, date_stamp: &str) -> Vec<u8> {
        let k_secret = format!("AWS4{}", self.secret_key);
        let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp);
        let k_region = hmac_sha256(&k_date, &self.region);
        let k_service = hmac_sha256(&k_region, &self.service);
        hmac_sha256(&k_service, "aws4_request")
    }
}

/// Parsed presigned URL parameters.
#[derive(Debug, Clone)]
pub struct PresignedParams {
    /// The algorithm (should be AWS4-HMAC-SHA256).
    pub algorithm: String,
    /// The credential (access_key_id/date/region/service/aws4_request).
    pub credential: String,
    /// The access key ID extracted from credential.
    pub access_key_id: String,
    /// The date stamp (YYYYMMDD) from credential.
    pub date_stamp: String,
    /// The region from credential.
    pub region: String,
    /// The full datetime (YYYYMMDDTHHMMSSZ).
    pub amz_date: String,
    /// Expiration in seconds.
    pub expires: u64,
    /// The signed headers.
    pub signed_headers: String,
    /// The signature.
    pub signature: String,
}

impl PresignedParams {
    /// Parse presigned URL parameters from query string.
    pub fn from_query(query: &str) -> Result<Self, PresignedError> {
        let mut params: BTreeMap<String, String> = BTreeMap::new();
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                params.insert(
                    uri_decode(key),
                    uri_decode(value),
                );
            }
        }

        let algorithm = params
            .get("X-Amz-Algorithm")
            .ok_or_else(|| PresignedError::MissingParameter("X-Amz-Algorithm".to_string()))?
            .clone();

        let credential = params
            .get("X-Amz-Credential")
            .ok_or_else(|| PresignedError::MissingParameter("X-Amz-Credential".to_string()))?
            .clone();

        let amz_date = params
            .get("X-Amz-Date")
            .ok_or_else(|| PresignedError::MissingParameter("X-Amz-Date".to_string()))?
            .clone();

        let expires: u64 = params
            .get("X-Amz-Expires")
            .ok_or_else(|| PresignedError::MissingParameter("X-Amz-Expires".to_string()))?
            .parse()
            .map_err(|_| PresignedError::InvalidExpiration("Invalid expires value".to_string()))?;

        let signed_headers = params
            .get("X-Amz-SignedHeaders")
            .ok_or_else(|| PresignedError::MissingParameter("X-Amz-SignedHeaders".to_string()))?
            .clone();

        let signature = params
            .get("X-Amz-Signature")
            .ok_or_else(|| PresignedError::MissingParameter("X-Amz-Signature".to_string()))?
            .clone();

        // Parse credential: access_key_id/date/region/service/aws4_request
        let cred_parts: Vec<&str> = credential.split('/').collect();
        if cred_parts.len() != 5 {
            return Err(PresignedError::MissingParameter(
                "Invalid credential format".to_string(),
            ));
        }

        let access_key_id = cred_parts[0].to_string();
        let date_stamp = cred_parts[1].to_string();
        let region = cred_parts[2].to_string();

        Ok(Self {
            algorithm,
            credential,
            access_key_id,
            date_stamp,
            region,
            amz_date,
            expires,
            signed_headers,
            signature,
        })
    }

    /// Check if the presigned URL has expired.
    pub fn is_expired(&self) -> Result<bool, PresignedError> {
        let request_time = parse_amz_date(&self.amz_date)?;
        let expiration = request_time + Duration::from_secs(self.expires);
        let now = SystemTime::now();
        Ok(now > expiration)
    }
}

/// Presigned URL validator.
pub struct PresignedValidator {
    /// Service name (always "s3").
    service: String,
}

impl PresignedValidator {
    /// Create a new validator.
    pub fn new() -> Self {
        Self {
            service: "s3".to_string(),
        }
    }

    /// Validate a presigned URL request.
    pub fn validate(
        &self,
        method: &str,
        path: &str,
        query: &str,
        host: &str,
        secret_key: &str,
    ) -> Result<PresignedParams, PresignedError> {
        let params = PresignedParams::from_query(query)?;

        // Check expiration
        if params.is_expired()? {
            return Err(PresignedError::Expired);
        }

        // Rebuild query string without signature (for signing)
        let mut query_params: BTreeMap<String, String> = BTreeMap::new();
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                let key = uri_decode(key);
                if key != "X-Amz-Signature" {
                    query_params.insert(key, uri_decode(value));
                }
            }
        }

        // Re-encode for canonical query
        let canonical_query = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
            .collect::<Vec<_>>()
            .join("&");

        // Build canonical headers
        let canonical_headers = format!("host:{}\n", host);

        // Build canonical request
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
            method,
            uri_encode(path, false),
            canonical_query,
            canonical_headers,
            params.signed_headers
        );

        // Build credential scope
        let credential_scope = format!(
            "{}/{}/{}/aws4_request",
            params.date_stamp, params.region, self.service
        );

        // Build string to sign
        let canonical_hash = hex_encode(&sha256(canonical_request.as_bytes()));
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            params.amz_date, credential_scope, canonical_hash
        );

        // Derive signing key and calculate expected signature
        let signing_key = derive_signing_key(secret_key, &params.date_stamp, &params.region);
        let expected_signature = hex_encode(&hmac_sha256(&signing_key, &string_to_sign));

        // Compare signatures (constant-time)
        if !constant_time_compare(expected_signature.as_bytes(), params.signature.as_bytes()) {
            return Err(PresignedError::InvalidSignature);
        }

        Ok(params)
    }
}

impl Default for PresignedValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Derive the signing key.
fn derive_signing_key(secret_key: &str, date_stamp: &str, region: &str) -> Vec<u8> {
    let k_secret = format!("AWS4{}", secret_key);
    let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp);
    let k_region = hmac_sha256(&k_date, region);
    let k_service = hmac_sha256(&k_region, "s3");
    hmac_sha256(&k_service, "aws4_request")
}

/// Format a SystemTime as AWS date format (YYYYMMDDTHHMMSSZ).
fn format_amz_date(time: SystemTime) -> String {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = duration.as_secs();

    // Calculate date/time components
    let days = secs / 86400;
    let remaining = secs % 86400;
    let hours = remaining / 3600;
    let minutes = (remaining % 3600) / 60;
    let seconds = remaining % 60;

    let (year, month, day) = days_to_ymd(days as i64);

    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Parse AWS date format (YYYYMMDDTHHMMSSZ) to SystemTime.
fn parse_amz_date(amz_date: &str) -> Result<SystemTime, PresignedError> {
    if amz_date.len() != 16 || !amz_date.ends_with('Z') || !amz_date.contains('T') {
        return Err(PresignedError::InvalidDate);
    }

    let year: i32 = amz_date[0..4]
        .parse()
        .map_err(|_| PresignedError::InvalidDate)?;
    let month: u32 = amz_date[4..6]
        .parse()
        .map_err(|_| PresignedError::InvalidDate)?;
    let day: u32 = amz_date[6..8]
        .parse()
        .map_err(|_| PresignedError::InvalidDate)?;
    let hour: u32 = amz_date[9..11]
        .parse()
        .map_err(|_| PresignedError::InvalidDate)?;
    let minute: u32 = amz_date[11..13]
        .parse()
        .map_err(|_| PresignedError::InvalidDate)?;
    let second: u32 = amz_date[13..15]
        .parse()
        .map_err(|_| PresignedError::InvalidDate)?;

    let days_since_epoch = days_from_date(year, month, day);
    let seconds_since_epoch = (days_since_epoch as u64) * 86400
        + (hour as u64) * 3600
        + (minute as u64) * 60
        + (second as u64);

    Ok(UNIX_EPOCH + Duration::from_secs(seconds_since_epoch))
}

/// Calculate days since Unix epoch from a date.
fn days_from_date(year: i32, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe as i32 - 719468) as i64
}

/// Convert days since epoch to year/month/day.
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

/// URI decode a string.
fn uri_decode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                    continue;
                }
            }
            result.push('%');
            result.push_str(&hex);
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_presigned_url_generation() {
        let generator = PresignedUrlGenerator::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
            "http://localhost:9000",
        );

        let url = generator
            .presign_get("test-bucket", "test-key", Duration::from_secs(3600))
            .unwrap();

        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.contains("X-Amz-Credential="));
        assert!(url.contains("X-Amz-Date="));
        assert!(url.contains("X-Amz-Expires=3600"));
        assert!(url.contains("X-Amz-Signature="));
        assert!(url.contains("/test-bucket/test-key"));
    }

    #[test]
    fn test_presigned_url_put() {
        let generator = PresignedUrlGenerator::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
            "http://localhost:9000",
        );

        let url = generator
            .presign_put(
                "test-bucket",
                "test-key",
                Duration::from_secs(3600),
                Some("application/octet-stream"),
            )
            .unwrap();

        assert!(url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.contains("X-Amz-Expires=3600"));
    }

    #[test]
    fn test_presigned_expiration_validation() {
        let generator = PresignedUrlGenerator::new(
            "AKIAIOSFODNN7EXAMPLE",
            "secret",
            "us-east-1",
            "http://localhost:9000",
        );

        // Too short
        let result = generator.presign_get("bucket", "key", Duration::from_millis(100));
        assert!(matches!(result, Err(PresignedError::InvalidExpiration(_))));

        // Too long
        let result = generator.presign_get("bucket", "key", Duration::from_secs(8 * 24 * 60 * 60));
        assert!(matches!(result, Err(PresignedError::InvalidExpiration(_))));

        // Valid
        let result = generator.presign_get("bucket", "key", Duration::from_secs(3600));
        assert!(result.is_ok());
    }

    #[test]
    fn test_presigned_params_parsing() {
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123";

        let params = PresignedParams::from_query(query).unwrap();

        assert_eq!(params.algorithm, "AWS4-HMAC-SHA256");
        assert_eq!(params.access_key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(params.date_stamp, "20130524");
        assert_eq!(params.region, "us-east-1");
        assert_eq!(params.expires, 86400);
        assert_eq!(params.signature, "abc123");
    }

    #[test]
    fn test_presigned_params_missing() {
        let query = "X-Amz-Algorithm=AWS4-HMAC-SHA256";

        let result = PresignedParams::from_query(query);
        assert!(matches!(result, Err(PresignedError::MissingParameter(_))));
    }

    #[test]
    fn test_format_amz_date() {
        let time = UNIX_EPOCH + Duration::from_secs(1684857600); // 2023-05-23T16:00:00Z
        let formatted = format_amz_date(time);
        assert_eq!(formatted.len(), 16);
        assert!(formatted.ends_with('Z'));
        assert!(formatted.contains('T'));
    }

    #[test]
    fn test_uri_encode_decode() {
        assert_eq!(uri_encode("hello world", true), "hello%20world");
        assert_eq!(uri_encode("a/b", true), "a%2Fb");
        assert_eq!(uri_encode("a/b", false), "a/b");

        assert_eq!(uri_decode("hello%20world"), "hello world");
        assert_eq!(uri_decode("a%2Fb"), "a/b");
    }

    #[test]
    fn test_presigned_roundtrip() {
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let region = "us-east-1";
        let endpoint = "http://localhost:9000";

        let generator = PresignedUrlGenerator::new(access_key, secret_key, region, endpoint);

        let url = generator
            .presign_get("test-bucket", "test-key", Duration::from_secs(3600))
            .unwrap();

        // Extract query string from URL
        let query = url.split('?').nth(1).unwrap();

        // Validate the URL
        let validator = PresignedValidator::new();
        let result = validator.validate(
            "GET",
            "/test-bucket/test-key",
            query,
            "localhost:9000",
            secret_key,
        );

        assert!(result.is_ok(), "Validation failed: {:?}", result);
        let params = result.unwrap();
        assert_eq!(params.access_key_id, access_key);
        assert_eq!(params.region, region);
    }
}
