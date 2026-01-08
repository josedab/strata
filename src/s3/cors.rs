//! S3 CORS (Cross-Origin Resource Sharing) support.
//!
//! Implements CORS configuration storage and request handling for S3 buckets.

use axum::{
    body::Body,
    extract::Request,
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::debug;

/// Maximum number of CORS rules per bucket.
pub const MAX_CORS_RULES: usize = 100;

/// Maximum age for preflight cache (in seconds).
pub const MAX_AGE_SECONDS: u32 = 86400;

/// CORS configuration for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsConfiguration {
    /// List of CORS rules.
    pub rules: Vec<CorsRule>,
}

impl Default for CorsConfiguration {
    fn default() -> Self {
        Self::new()
    }
}

impl CorsConfiguration {
    /// Create an empty CORS configuration.
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add a CORS rule.
    pub fn add_rule(&mut self, rule: CorsRule) -> Result<(), CorsError> {
        if self.rules.len() >= MAX_CORS_RULES {
            return Err(CorsError::TooManyRules);
        }
        self.rules.push(rule);
        Ok(())
    }

    /// Find the first matching rule for a request.
    pub fn find_matching_rule(
        &self,
        origin: &str,
        method: &Method,
    ) -> Option<&CorsRule> {
        self.rules.iter().find(|rule| {
            rule.matches_origin(origin) && rule.matches_method(method)
        })
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), CorsError> {
        if self.rules.len() > MAX_CORS_RULES {
            return Err(CorsError::TooManyRules);
        }

        for rule in &self.rules {
            rule.validate()?;
        }

        Ok(())
    }
}

/// A single CORS rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsRule {
    /// Unique identifier for this rule (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// Allowed origins (can include wildcards like "*" or "*.example.com").
    pub allowed_origins: Vec<String>,

    /// Allowed HTTP methods.
    pub allowed_methods: Vec<String>,

    /// Allowed headers in preflight requests.
    #[serde(default)]
    pub allowed_headers: Vec<String>,

    /// Headers exposed to the client.
    #[serde(default)]
    pub expose_headers: Vec<String>,

    /// Max age for preflight cache (seconds).
    #[serde(default)]
    pub max_age_seconds: Option<u32>,

    /// Whether credentials (cookies, authorization headers, TLS client certs) are allowed.
    /// When true, the response will include `Access-Control-Allow-Credentials: true`
    /// and the wildcard "*" cannot be used for allowed origins.
    #[serde(default)]
    pub allow_credentials: bool,
}

impl CorsRule {
    /// Create a new CORS rule with required fields.
    pub fn new(allowed_origins: Vec<String>, allowed_methods: Vec<String>) -> Self {
        Self {
            id: None,
            allowed_origins,
            allowed_methods,
            allowed_headers: Vec::new(),
            expose_headers: Vec::new(),
            max_age_seconds: None,
            allow_credentials: false,
        }
    }

    /// Set allowed headers.
    pub fn with_allowed_headers(mut self, headers: Vec<String>) -> Self {
        self.allowed_headers = headers;
        self
    }

    /// Set exposed headers.
    pub fn with_expose_headers(mut self, headers: Vec<String>) -> Self {
        self.expose_headers = headers;
        self
    }

    /// Set max age.
    pub fn with_max_age(mut self, seconds: u32) -> Self {
        self.max_age_seconds = Some(seconds);
        self
    }

    /// Enable credentials support.
    /// Note: When credentials are enabled, wildcard "*" cannot be used for origins.
    pub fn with_credentials(mut self, allow: bool) -> Self {
        self.allow_credentials = allow;
        self
    }

    /// Check if this rule matches the given origin.
    pub fn matches_origin(&self, origin: &str) -> bool {
        for allowed in &self.allowed_origins {
            if allowed == "*" {
                return true;
            }
            if allowed == origin {
                return true;
            }
            // Handle wildcard subdomain matching (e.g., "*.example.com")
            if allowed.starts_with("*.") {
                let suffix = &allowed[1..]; // ".example.com"
                if origin.ends_with(suffix) {
                    return true;
                }
            }
        }
        false
    }

    /// Check if this rule matches the given method.
    pub fn matches_method(&self, method: &Method) -> bool {
        let method_str = method.as_str();
        self.allowed_methods
            .iter()
            .any(|m| m.eq_ignore_ascii_case(method_str) || m == "*")
    }

    /// Check if a header is allowed.
    pub fn is_header_allowed(&self, header: &str) -> bool {
        // Always allow common headers
        let common = ["content-type", "authorization", "x-amz-date", "x-amz-content-sha256"];
        if common.iter().any(|h| h.eq_ignore_ascii_case(header)) {
            return true;
        }

        self.allowed_headers
            .iter()
            .any(|h| h == "*" || h.eq_ignore_ascii_case(header))
    }

    /// Validate the rule.
    pub fn validate(&self) -> Result<(), CorsError> {
        if self.allowed_origins.is_empty() {
            return Err(CorsError::MissingOrigin);
        }
        if self.allowed_methods.is_empty() {
            return Err(CorsError::MissingMethod);
        }

        // When credentials are allowed, wildcard "*" origin is not permitted
        // per the CORS specification
        if self.allow_credentials {
            if self.allowed_origins.iter().any(|o| o == "*") {
                return Err(CorsError::WildcardWithCredentials);
            }
        }

        // Validate methods
        let valid_methods = ["GET", "PUT", "POST", "DELETE", "HEAD", "*"];
        for method in &self.allowed_methods {
            if !valid_methods.iter().any(|m| m.eq_ignore_ascii_case(method)) {
                return Err(CorsError::InvalidMethod(method.clone()));
            }
        }

        // Validate max age
        if let Some(age) = self.max_age_seconds {
            if age > MAX_AGE_SECONDS {
                return Err(CorsError::InvalidMaxAge);
            }
        }

        Ok(())
    }

    /// Get the max age value, using default if not specified.
    pub fn get_max_age(&self) -> u32 {
        self.max_age_seconds.unwrap_or(3600) // Default 1 hour
    }
}

/// CORS-related errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum CorsError {
    #[error("Too many CORS rules (max {MAX_CORS_RULES})")]
    TooManyRules,

    #[error("Missing allowed origin")]
    MissingOrigin,

    #[error("Missing allowed method")]
    MissingMethod,

    #[error("Invalid method: {0}")]
    InvalidMethod(String),

    #[error("Invalid max age (max {MAX_AGE_SECONDS} seconds)")]
    InvalidMaxAge,

    #[error("CORS configuration not found")]
    NotFound,

    #[error("Origin not allowed")]
    OriginNotAllowed,

    #[error("Method not allowed")]
    MethodNotAllowed,

    #[error("Invalid XML: {0}")]
    InvalidXml(String),

    #[error("Wildcard origin '*' cannot be used with AllowCredentials")]
    WildcardWithCredentials,
}

/// In-memory CORS configuration store.
#[derive(Clone)]
pub struct CorsStore {
    /// Map of bucket name to CORS configuration.
    configs: Arc<RwLock<HashMap<String, CorsConfiguration>>>,
}

impl Default for CorsStore {
    fn default() -> Self {
        Self::new()
    }
}

impl CorsStore {
    /// Create a new empty CORS store.
    pub fn new() -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the CORS configuration for a bucket.
    pub fn get(&self, bucket: &str) -> Option<CorsConfiguration> {
        self.configs.read().ok()?.get(bucket).cloned()
    }

    /// Set the CORS configuration for a bucket.
    pub fn set(&self, bucket: &str, config: CorsConfiguration) -> Result<(), CorsError> {
        config.validate()?;
        if let Ok(mut configs) = self.configs.write() {
            configs.insert(bucket.to_string(), config);
        }
        Ok(())
    }

    /// Remove the CORS configuration for a bucket.
    pub fn remove(&self, bucket: &str) -> bool {
        if let Ok(mut configs) = self.configs.write() {
            return configs.remove(bucket).is_some();
        }
        false
    }
}

/// State for CORS middleware.
#[derive(Clone)]
pub struct CorsState {
    /// CORS configuration store.
    pub store: CorsStore,
}

impl Default for CorsState {
    fn default() -> Self {
        Self::new()
    }
}

impl CorsState {
    /// Create new CORS state.
    pub fn new() -> Self {
        Self {
            store: CorsStore::new(),
        }
    }
}

/// Extract bucket name from the request path.
fn extract_bucket_from_path(path: &str) -> Option<&str> {
    let path = path.trim_start_matches('/');
    path.split('/').next().filter(|s| !s.is_empty())
}

/// CORS middleware for S3 gateway.
///
/// Handles OPTIONS preflight requests and adds CORS headers to responses
/// based on bucket-specific CORS configuration.
pub async fn s3_cors_middleware(
    cors_state: CorsState,
    request: Request,
    next: Next,
) -> Response {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    // If no Origin header, not a CORS request - proceed normally
    let origin = match origin {
        Some(o) => o,
        None => return next.run(request).await,
    };

    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let bucket = extract_bucket_from_path(&path);

    // Get CORS config for this bucket
    let cors_config = bucket.and_then(|b| cors_state.store.get(b));

    // Handle OPTIONS preflight request
    if method == Method::OPTIONS {
        return handle_preflight(&origin, &request, cors_config.as_ref());
    }

    // For actual requests, find matching rule and add headers
    let response = next.run(request).await;

    if let Some(config) = cors_config {
        if let Some(rule) = config.find_matching_rule(&origin, &method) {
            return add_cors_headers(response, &origin, rule);
        }
    }

    response
}

/// Handle CORS preflight (OPTIONS) request.
fn handle_preflight(
    origin: &str,
    request: &Request,
    cors_config: Option<&CorsConfiguration>,
) -> Response {
    // Get requested method from Access-Control-Request-Method header
    let requested_method = request
        .headers()
        .get("access-control-request-method")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<Method>().ok());

    let requested_method = match requested_method {
        Some(m) => m,
        None => {
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from("Missing Access-Control-Request-Method"))
                .unwrap();
        }
    };

    // Check if we have CORS config and a matching rule
    let config = match cors_config {
        Some(c) => c,
        None => {
            debug!("No CORS config for bucket");
            return Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Body::from("CORS not configured"))
                .unwrap();
        }
    };

    let rule = match config.find_matching_rule(origin, &requested_method) {
        Some(r) => r,
        None => {
            debug!(origin = %origin, "Origin not allowed by CORS config");
            return Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Body::from("Origin not allowed"))
                .unwrap();
        }
    };

    // Check requested headers
    if let Some(requested_headers) = request
        .headers()
        .get("access-control-request-headers")
        .and_then(|v| v.to_str().ok())
    {
        for header in requested_headers.split(',') {
            let header = header.trim();
            if !rule.is_header_allowed(header) {
                debug!(header = %header, "Header not allowed");
                return Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body(Body::from(format!("Header not allowed: {}", header)))
                    .unwrap();
            }
        }
    }

    // Build preflight response
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin)
        .header(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            rule.allowed_methods.join(", "),
        )
        .header(
            header::ACCESS_CONTROL_MAX_AGE,
            rule.get_max_age().to_string(),
        );

    if !rule.allowed_headers.is_empty() {
        builder = builder.header(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            rule.allowed_headers.join(", "),
        );
    }

    // Add credentials header if allowed
    if rule.allow_credentials {
        builder = builder.header(header::ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
    }

    // Add Vary header
    builder = builder.header(header::VARY, "Origin");

    builder.body(Body::empty()).unwrap()
}

/// Add CORS headers to a response.
fn add_cors_headers(response: Response, origin: &str, rule: &CorsRule) -> Response {
    let (mut parts, body) = response.into_parts();

    // Add CORS headers
    parts.headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_str(origin).unwrap_or_else(|_| HeaderValue::from_static("*")),
    );

    if !rule.expose_headers.is_empty() {
        if let Ok(value) = HeaderValue::from_str(&rule.expose_headers.join(", ")) {
            parts
                .headers
                .insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, value);
        }
    }

    // Add credentials header if allowed
    if rule.allow_credentials {
        parts.headers.insert(
            header::ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
    }

    // Add Vary header to indicate response varies by Origin
    parts
        .headers
        .insert(header::VARY, HeaderValue::from_static("Origin"));

    Response::from_parts(parts, body)
}

/// Parse CORS configuration from XML.
pub fn parse_cors_xml(xml: &str) -> Result<CorsConfiguration, CorsError> {
    // Simple XML parsing for CORS configuration
    // Format:
    // <CORSConfiguration>
    //   <CORSRule>
    //     <AllowedOrigin>*</AllowedOrigin>
    //     <AllowedMethod>GET</AllowedMethod>
    //     <AllowedHeader>*</AllowedHeader>
    //     <ExposeHeader>ETag</ExposeHeader>
    //     <MaxAgeSeconds>3600</MaxAgeSeconds>
    //   </CORSRule>
    // </CORSConfiguration>

    let mut config = CorsConfiguration::new();

    // Find all CORSRule blocks
    let mut pos = 0;
    while let Some(rule_start) = xml[pos..].find("<CORSRule>") {
        let rule_start = pos + rule_start;
        let rule_end = xml[rule_start..]
            .find("</CORSRule>")
            .map(|i| rule_start + i + 11)
            .ok_or_else(|| CorsError::InvalidXml("Missing </CORSRule>".to_string()))?;

        let rule_xml = &xml[rule_start..rule_end];
        let rule = parse_cors_rule(rule_xml)?;
        config.add_rule(rule)?;

        pos = rule_end;
    }

    if config.rules.is_empty() {
        return Err(CorsError::InvalidXml("No CORS rules found".to_string()));
    }

    Ok(config)
}

/// Parse a single CORS rule from XML.
fn parse_cors_rule(xml: &str) -> Result<CorsRule, CorsError> {
    let mut allowed_origins = Vec::new();
    let mut allowed_methods = Vec::new();
    let mut allowed_headers = Vec::new();
    let mut expose_headers = Vec::new();
    let mut max_age_seconds = None;
    let mut id = None;

    // Parse ID
    if let Some(value) = extract_xml_value(xml, "ID") {
        id = Some(value);
    }

    // Parse AllowedOrigin elements
    let mut pos = 0;
    while let Some(value) = find_xml_value(&xml[pos..], "AllowedOrigin") {
        allowed_origins.push(value.0);
        pos += value.1;
    }

    // Parse AllowedMethod elements
    pos = 0;
    while let Some(value) = find_xml_value(&xml[pos..], "AllowedMethod") {
        allowed_methods.push(value.0);
        pos += value.1;
    }

    // Parse AllowedHeader elements
    pos = 0;
    while let Some(value) = find_xml_value(&xml[pos..], "AllowedHeader") {
        allowed_headers.push(value.0);
        pos += value.1;
    }

    // Parse ExposeHeader elements
    pos = 0;
    while let Some(value) = find_xml_value(&xml[pos..], "ExposeHeader") {
        expose_headers.push(value.0);
        pos += value.1;
    }

    // Parse MaxAgeSeconds
    if let Some(value) = extract_xml_value(xml, "MaxAgeSeconds") {
        max_age_seconds = value.parse().ok();
    }

    // Parse AllowCredentials (S3 uses "true" string)
    let allow_credentials = extract_xml_value(xml, "AllowCredentials")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if allowed_origins.is_empty() {
        return Err(CorsError::MissingOrigin);
    }
    if allowed_methods.is_empty() {
        return Err(CorsError::MissingMethod);
    }

    let mut rule = CorsRule::new(allowed_origins, allowed_methods);
    rule.id = id;
    rule.allowed_headers = allowed_headers;
    rule.expose_headers = expose_headers;
    rule.max_age_seconds = max_age_seconds;
    rule.allow_credentials = allow_credentials;

    rule.validate()?;
    Ok(rule)
}

/// Extract a single XML element value.
fn extract_xml_value(xml: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    let start = xml.find(&start_tag)? + start_tag.len();
    let end = xml[start..].find(&end_tag)?;

    Some(xml[start..start + end].trim().to_string())
}

/// Find XML element value and return with position.
fn find_xml_value(xml: &str, tag: &str) -> Option<(String, usize)> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    let tag_start = xml.find(&start_tag)?;
    let value_start = tag_start + start_tag.len();
    let value_end = xml[value_start..].find(&end_tag)?;
    let end_pos = value_start + value_end + end_tag.len();

    Some((xml[value_start..value_start + value_end].trim().to_string(), end_pos))
}

/// Generate XML for CORS configuration.
pub fn cors_to_xml(config: &CorsConfiguration) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str("<CORSConfiguration>\n");

    for rule in &config.rules {
        xml.push_str("  <CORSRule>\n");

        if let Some(ref id) = rule.id {
            xml.push_str(&format!("    <ID>{}</ID>\n", xml_escape(id)));
        }

        for origin in &rule.allowed_origins {
            xml.push_str(&format!("    <AllowedOrigin>{}</AllowedOrigin>\n", xml_escape(origin)));
        }

        for method in &rule.allowed_methods {
            xml.push_str(&format!("    <AllowedMethod>{}</AllowedMethod>\n", xml_escape(method)));
        }

        for header in &rule.allowed_headers {
            xml.push_str(&format!("    <AllowedHeader>{}</AllowedHeader>\n", xml_escape(header)));
        }

        for header in &rule.expose_headers {
            xml.push_str(&format!("    <ExposeHeader>{}</ExposeHeader>\n", xml_escape(header)));
        }

        if let Some(max_age) = rule.max_age_seconds {
            xml.push_str(&format!("    <MaxAgeSeconds>{}</MaxAgeSeconds>\n", max_age));
        }

        // Only include AllowCredentials when it's true (default is false)
        if rule.allow_credentials {
            xml.push_str("    <AllowCredentials>true</AllowCredentials>\n");
        }

        xml.push_str("  </CORSRule>\n");
    }

    xml.push_str("</CORSConfiguration>");
    xml
}

/// Escape special XML characters.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cors_rule_matches_origin() {
        let rule = CorsRule::new(
            vec!["https://example.com".to_string(), "*.test.com".to_string()],
            vec!["GET".to_string()],
        );

        assert!(rule.matches_origin("https://example.com"));
        assert!(rule.matches_origin("https://sub.test.com"));
        assert!(rule.matches_origin("https://deep.sub.test.com"));
        assert!(!rule.matches_origin("https://other.com"));
    }

    #[test]
    fn test_cors_rule_wildcard_origin() {
        let rule = CorsRule::new(vec!["*".to_string()], vec!["GET".to_string()]);

        assert!(rule.matches_origin("https://any.com"));
        assert!(rule.matches_origin("http://localhost"));
    }

    #[test]
    fn test_cors_rule_matches_method() {
        let rule = CorsRule::new(
            vec!["*".to_string()],
            vec!["GET".to_string(), "POST".to_string()],
        );

        assert!(rule.matches_method(&Method::GET));
        assert!(rule.matches_method(&Method::POST));
        assert!(!rule.matches_method(&Method::DELETE));
    }

    #[test]
    fn test_cors_configuration_validation() {
        let mut config = CorsConfiguration::new();

        // Valid rule
        let rule = CorsRule::new(
            vec!["*".to_string()],
            vec!["GET".to_string()],
        );
        assert!(config.add_rule(rule).is_ok());
        assert!(config.validate().is_ok());

        // Invalid method
        let bad_rule = CorsRule::new(
            vec!["*".to_string()],
            vec!["INVALID".to_string()],
        );
        assert!(bad_rule.validate().is_err());
    }

    #[test]
    fn test_cors_store() {
        let store = CorsStore::new();

        let config = CorsConfiguration {
            rules: vec![CorsRule::new(
                vec!["*".to_string()],
                vec!["GET".to_string()],
            )],
        };

        assert!(store.set("test-bucket", config.clone()).is_ok());
        assert!(store.get("test-bucket").is_some());
        assert!(store.get("other-bucket").is_none());

        assert!(store.remove("test-bucket"));
        assert!(store.get("test-bucket").is_none());
    }

    #[test]
    fn test_parse_cors_xml() {
        let xml = r#"
        <CORSConfiguration>
          <CORSRule>
            <AllowedOrigin>*</AllowedOrigin>
            <AllowedMethod>GET</AllowedMethod>
            <AllowedMethod>PUT</AllowedMethod>
            <AllowedHeader>*</AllowedHeader>
            <ExposeHeader>ETag</ExposeHeader>
            <MaxAgeSeconds>3600</MaxAgeSeconds>
          </CORSRule>
        </CORSConfiguration>
        "#;

        let config = parse_cors_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 1);

        let rule = &config.rules[0];
        assert_eq!(rule.allowed_origins, vec!["*"]);
        assert_eq!(rule.allowed_methods, vec!["GET", "PUT"]);
        assert_eq!(rule.allowed_headers, vec!["*"]);
        assert_eq!(rule.expose_headers, vec!["ETag"]);
        assert_eq!(rule.max_age_seconds, Some(3600));
    }

    #[test]
    fn test_cors_to_xml() {
        let config = CorsConfiguration {
            rules: vec![CorsRule {
                id: Some("rule1".to_string()),
                allowed_origins: vec!["https://example.com".to_string()],
                allowed_methods: vec!["GET".to_string(), "POST".to_string()],
                allowed_headers: vec!["Content-Type".to_string()],
                expose_headers: vec!["ETag".to_string()],
                max_age_seconds: Some(3600),
                allow_credentials: false,
            }],
        };

        let xml = cors_to_xml(&config);
        assert!(xml.contains("<AllowedOrigin>https://example.com</AllowedOrigin>"));
        assert!(xml.contains("<AllowedMethod>GET</AllowedMethod>"));
        assert!(xml.contains("<MaxAgeSeconds>3600</MaxAgeSeconds>"));
    }

    #[test]
    fn test_extract_bucket_from_path() {
        assert_eq!(extract_bucket_from_path("/my-bucket/key"), Some("my-bucket"));
        assert_eq!(extract_bucket_from_path("/bucket"), Some("bucket"));
        assert_eq!(extract_bucket_from_path("/"), None);
        assert_eq!(extract_bucket_from_path(""), None);
    }

    #[test]
    fn test_header_allowed() {
        let rule = CorsRule::new(
            vec!["*".to_string()],
            vec!["GET".to_string()],
        ).with_allowed_headers(vec!["X-Custom-Header".to_string()]);

        assert!(rule.is_header_allowed("content-type"));
        assert!(rule.is_header_allowed("X-Custom-Header"));
        assert!(!rule.is_header_allowed("X-Other-Header"));

        // Wildcard headers
        let wildcard_rule = CorsRule::new(
            vec!["*".to_string()],
            vec!["GET".to_string()],
        ).with_allowed_headers(vec!["*".to_string()]);

        assert!(wildcard_rule.is_header_allowed("Any-Header"));
    }

    #[test]
    fn test_credentials_support() {
        // Create rule with credentials
        let rule = CorsRule::new(
            vec!["https://example.com".to_string()],
            vec!["GET".to_string()],
        ).with_credentials(true);

        assert!(rule.allow_credentials);
        assert!(rule.validate().is_ok());
    }

    #[test]
    fn test_credentials_with_wildcard_origin_fails() {
        // Credentials + wildcard origin should fail validation
        let rule = CorsRule::new(
            vec!["*".to_string()],
            vec!["GET".to_string()],
        ).with_credentials(true);

        let result = rule.validate();
        assert!(matches!(result, Err(CorsError::WildcardWithCredentials)));
    }

    #[test]
    fn test_parse_cors_xml_with_credentials() {
        let xml = r#"
        <CORSConfiguration>
          <CORSRule>
            <AllowedOrigin>https://example.com</AllowedOrigin>
            <AllowedMethod>GET</AllowedMethod>
            <AllowCredentials>true</AllowCredentials>
          </CORSRule>
        </CORSConfiguration>
        "#;

        let config = parse_cors_xml(xml).unwrap();
        assert_eq!(config.rules.len(), 1);
        assert!(config.rules[0].allow_credentials);
    }

    #[test]
    fn test_cors_to_xml_with_credentials() {
        let rule = CorsRule::new(
            vec!["https://example.com".to_string()],
            vec!["GET".to_string()],
        ).with_credentials(true);

        let config = CorsConfiguration { rules: vec![rule] };
        let xml = cors_to_xml(&config);

        assert!(xml.contains("<AllowCredentials>true</AllowCredentials>"));
        assert!(xml.contains("<AllowedOrigin>https://example.com</AllowedOrigin>"));
    }

    #[test]
    fn test_cors_to_xml_without_credentials() {
        let rule = CorsRule::new(
            vec!["https://example.com".to_string()],
            vec!["GET".to_string()],
        );

        let config = CorsConfiguration { rules: vec![rule] };
        let xml = cors_to_xml(&config);

        // AllowCredentials should NOT be in output when false (default)
        assert!(!xml.contains("AllowCredentials"));
    }
}
