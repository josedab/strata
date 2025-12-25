//! Authentication middleware for Axum.

use super::{AuthInfo, TokenConfig, TokenValidator};
use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// Shared authentication state.
#[derive(Clone)]
pub struct AuthState {
    validator: Arc<TokenValidator>,
    allow_anonymous: bool,
}

impl AuthState {
    /// Create new auth state.
    pub fn new(config: TokenConfig) -> Self {
        Self {
            validator: Arc::new(TokenValidator::new(config)),
            allow_anonymous: false,
        }
    }

    /// Allow anonymous requests.
    pub fn with_anonymous(mut self, allow: bool) -> Self {
        self.allow_anonymous = allow;
        self
    }

    /// Get the token validator.
    pub fn validator(&self) -> &TokenValidator {
        &self.validator
    }
}

/// Extract auth info from request.
pub async fn auth_middleware(
    State(state): State<AuthState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    // Try to extract token from Authorization header
    let auth_header = request
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok());

    let auth_info = match auth_header {
        Some(header) if header.starts_with("Bearer ") => {
            let token = &header[7..];
            match state.validator.validate(token) {
                Ok(info) => info,
                Err(e) => {
                    return (StatusCode::UNAUTHORIZED, format!("Invalid token: {}", e))
                        .into_response();
                }
            }
        }
        Some(_) => {
            return (StatusCode::UNAUTHORIZED, "Invalid Authorization header format")
                .into_response();
        }
        None => {
            if state.allow_anonymous {
                AuthInfo::anonymous()
            } else {
                return (StatusCode::UNAUTHORIZED, "Authorization required").into_response();
            }
        }
    };

    // Insert auth info into request extensions
    request.extensions_mut().insert(auth_info);

    next.run(request).await
}

/// Require a specific role.
pub async fn require_role(
    role: &str,
    request: Request<Body>,
    next: Next,
) -> Result<Response, (StatusCode, &'static str)> {
    let auth_info = request
        .extensions()
        .get::<AuthInfo>()
        .cloned()
        .unwrap_or_else(AuthInfo::anonymous);

    if !auth_info.has_role(role) {
        return Err((StatusCode::FORBIDDEN, "Insufficient permissions"));
    }

    Ok(next.run(request).await)
}

/// Auth layer for Axum router.
#[derive(Clone)]
pub struct AuthLayer {
    state: AuthState,
}

impl AuthLayer {
    /// Create a new auth layer.
    pub fn new(config: TokenConfig) -> Self {
        Self {
            state: AuthState::new(config),
        }
    }

    /// Allow anonymous access.
    pub fn allow_anonymous(mut self) -> Self {
        self.state = self.state.with_anonymous(true);
        self
    }

    /// Get the state.
    pub fn state(&self) -> AuthState {
        self.state.clone()
    }
}

/// Authentication middleware type alias.
pub type AuthMiddleware = axum::middleware::FromFnLayer<
    fn(State<AuthState>, Request<Body>, Next) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>>,
    AuthState,
    (State<AuthState>, Request<Body>, Next),
>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_state_creation() {
        let config = TokenConfig::for_testing();
        let state = AuthState::new(config);
        assert!(!state.allow_anonymous);
    }

    #[test]
    fn test_auth_state_with_anonymous() {
        let config = TokenConfig::for_testing();
        let state = AuthState::new(config).with_anonymous(true);
        assert!(state.allow_anonymous);
    }

    #[test]
    fn test_auth_layer_creation() {
        let config = TokenConfig::for_testing();
        let layer = AuthLayer::new(config);
        assert!(!layer.state.allow_anonymous);
    }
}
