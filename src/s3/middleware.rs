//! S3 authentication middleware.
//!
//! Provides Axum middleware for validating AWS Signature V4 requests,
//! including support for presigned URLs.

use crate::auth::AuthInfo;
use crate::s3::credentials::{S3AccessKey, S3CredentialStore};
use crate::s3::presigned::{PresignedError, PresignedValidator};
use crate::s3::signature::{SignatureError, SignatureValidator};
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, Response, StatusCode},
    middleware::Next,
};
use http_body_util::BodyExt;
use std::sync::Arc;
use tracing::{debug, warn};

/// Shared state for S3 authentication.
#[derive(Clone)]
pub struct S3AuthState {
    /// Credential store for looking up access keys.
    pub credentials: Arc<S3CredentialStore>,
    /// Signature validator.
    pub validator: Arc<SignatureValidator>,
    /// Presigned URL validator.
    pub presigned_validator: Arc<PresignedValidator>,
    /// Whether to allow anonymous requests.
    pub allow_anonymous: bool,
}

impl S3AuthState {
    /// Create new auth state.
    pub fn new(
        credentials: S3CredentialStore,
        region: impl Into<String>,
        allow_anonymous: bool,
    ) -> Self {
        Self {
            credentials: Arc::new(credentials),
            validator: Arc::new(SignatureValidator::new(region)),
            presigned_validator: Arc::new(PresignedValidator::new()),
            allow_anonymous,
        }
    }
}

/// Extension type for storing authenticated user info in request.
#[derive(Clone, Debug)]
pub struct S3AuthInfo {
    /// The authenticated user info.
    pub auth: AuthInfo,
    /// The access key used (if authenticated).
    pub access_key: Option<S3AccessKey>,
}

impl S3AuthInfo {
    /// Create auth info for an anonymous user.
    pub fn anonymous() -> Self {
        Self {
            auth: AuthInfo::anonymous(),
            access_key: None,
        }
    }

    /// Create auth info for an authenticated user.
    pub fn authenticated(access_key: S3AccessKey) -> Self {
        Self {
            auth: AuthInfo {
                user_id: access_key.user_id.clone(),
                username: access_key.display_name.clone(),
                roles: vec!["s3-user".to_string()],
                is_service: false,
            },
            access_key: Some(access_key),
        }
    }
}

/// S3 authentication middleware.
///
/// This middleware validates AWS Signature V4 authentication on requests,
/// including support for presigned URLs (signature in query string).
/// If `allow_anonymous` is true, unsigned requests are allowed through.
pub async fn s3_auth_middleware(
    State(auth_state): State<S3AuthState>,
    request: Request,
    next: Next,
) -> Response<Body> {
    // Check for Authorization header
    let auth_header = request.headers().get(header::AUTHORIZATION);

    match auth_header {
        Some(header_value) => {
            let header_str = match header_value.to_str() {
                Ok(s) => s,
                Err(_) => {
                    return access_denied_response("Invalid Authorization header encoding");
                }
            };

            // Check if it's AWS SigV4
            if header_str.starts_with("AWS4-HMAC-SHA256") {
                match validate_sigv4_request(&auth_state, request, next).await {
                    Ok(response) => response,
                    Err(e) => {
                        warn!("S3 auth failed: {}", e);
                        access_denied_response(&e.to_string())
                    }
                }
            } else {
                // Unsupported auth scheme
                access_denied_response("Unsupported authentication scheme")
            }
        }
        None => {
            // No auth header - check for presigned URL parameters
            let query = request.uri().query().unwrap_or("");
            if query.contains("X-Amz-Signature") {
                match validate_presigned_request(&auth_state, request, next).await {
                    Ok(response) => response,
                    Err(e) => {
                        warn!("Presigned URL validation failed: {}", e);
                        presigned_error_response(&e)
                    }
                }
            } else if auth_state.allow_anonymous {
                debug!("Allowing anonymous S3 request");
                // Insert anonymous auth info
                let mut request = request;
                request.extensions_mut().insert(S3AuthInfo::anonymous());
                next.run(request).await
            } else {
                access_denied_response("Authentication required")
            }
        }
    }
}

/// Validate a request with AWS SigV4 signature.
async fn validate_sigv4_request(
    auth_state: &S3AuthState,
    request: Request,
    next: Next,
) -> Result<Response<Body>, SignatureError> {
    // We need to read the body to validate the signature, then reconstruct the request
    let (parts, body) = request.into_parts();

    // Collect body bytes
    let body_bytes = body
        .collect()
        .await
        .map_err(|_| SignatureError::InvalidAuthFormat)?
        .to_bytes();

    // Get Authorization header to extract access key ID
    let auth_header = parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(SignatureError::MissingAuthHeader)?;

    // Parse to get access key ID
    let parsed_auth = crate::s3::signature::AuthorizationHeader::parse(auth_header)?;

    // Look up secret key
    let access_key = auth_state
        .credentials
        .get_key(&parsed_auth.access_key_id)
        .map_err(|e| {
            debug!("Credential lookup failed: {}", e);
            SignatureError::SignatureMismatch
        })?;

    // Validate signature
    auth_state.validator.validate(
        &parts.method,
        &parts.uri,
        &parts.headers,
        &body_bytes,
        &access_key.secret_key,
    )?;

    debug!(
        access_key_id = %access_key.access_key_id,
        user_id = %access_key.user_id,
        "S3 request authenticated"
    );

    // Reconstruct request with auth info
    let mut request = Request::from_parts(parts, Body::from(body_bytes));
    request
        .extensions_mut()
        .insert(S3AuthInfo::authenticated(access_key));

    Ok(next.run(request).await)
}

/// Validate a presigned URL request.
async fn validate_presigned_request(
    auth_state: &S3AuthState,
    request: Request,
    next: Next,
) -> Result<Response<Body>, PresignedError> {
    let method = request.method().as_str();
    let path = request.uri().path();
    let query = request.uri().query().unwrap_or("");

    // Extract host header
    let host = request
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");

    // Parse presigned params to get access key ID
    let params = crate::s3::presigned::PresignedParams::from_query(query)?;

    // Look up secret key
    let access_key = auth_state
        .credentials
        .get_key(&params.access_key_id)
        .map_err(|_| PresignedError::InvalidSignature)?;

    // Validate signature
    auth_state.presigned_validator.validate(
        method,
        path,
        query,
        host,
        &access_key.secret_key,
    )?;

    debug!(
        access_key_id = %access_key.access_key_id,
        user_id = %access_key.user_id,
        "S3 presigned request authenticated"
    );

    // Insert auth info
    let mut request = request;
    request
        .extensions_mut()
        .insert(S3AuthInfo::authenticated(access_key));

    Ok(next.run(request).await)
}

/// Generate an S3 AccessDenied XML error response.
fn access_denied_response(message: &str) -> Response<Body> {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>AccessDenied</Code>
    <Message>{}</Message>
</Error>"#,
        xml_escape(message)
    );

    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap()
        })
}

/// Generate an S3 error response for presigned URL failures.
fn presigned_error_response(error: &PresignedError) -> Response<Body> {
    let (code, status) = match error {
        PresignedError::Expired => ("ExpiredToken", StatusCode::FORBIDDEN),
        PresignedError::InvalidSignature => ("SignatureDoesNotMatch", StatusCode::FORBIDDEN),
        PresignedError::MissingParameter(param) => {
            return error_xml_response("InvalidArgument", &format!("Missing parameter: {}", param), StatusCode::BAD_REQUEST);
        }
        PresignedError::InvalidExpiration(msg) => {
            return error_xml_response("InvalidArgument", msg, StatusCode::BAD_REQUEST);
        }
        PresignedError::InvalidDate => ("InvalidArgument", StatusCode::BAD_REQUEST),
    };

    error_xml_response(code, &error.to_string(), status)
}

/// Generate an S3 XML error response.
fn error_xml_response(code: &str, message: &str, status: StatusCode) -> Response<Body> {
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>{}</Code>
    <Message>{}</Message>
</Error>"#,
        xml_escape(code),
        xml_escape(message)
    );

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .unwrap()
        })
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
    fn test_s3_auth_info_anonymous() {
        let info = S3AuthInfo::anonymous();
        assert_eq!(info.auth.user_id, "anonymous");
        assert!(info.access_key.is_none());
    }

    #[test]
    fn test_s3_auth_info_authenticated() {
        let key = S3AccessKey::new("AKID1", "secret1", "user1");
        let info = S3AuthInfo::authenticated(key);
        assert_eq!(info.auth.user_id, "user1");
        assert!(info.access_key.is_some());
    }

    #[test]
    fn test_xml_escape() {
        assert_eq!(xml_escape("hello"), "hello");
        assert_eq!(xml_escape("<test>"), "&lt;test&gt;");
        assert_eq!(xml_escape("a & b"), "a &amp; b");
    }
}
