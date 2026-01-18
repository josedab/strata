//! Authentication module for Strata.
//!
//! Provides middleware and utilities for authenticating requests.

// Deny unsafe code patterns in this security-critical module.
// Panics in authentication code can lead to security vulnerabilities.
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]

mod acl;
pub mod kms;
mod middleware;
mod token;
pub mod validation;

pub use acl::{
    check_path_access, AccessChecker, AccessType, AclEntry, AclEntryType, ExtendedAcl, Permission,
    UserCredentials,
};
pub use kms::{
    DataKeyPair, EncryptionKey, KeyAlgorithm, KeyState, KmsProvider, KmsStats, LocalKmsProvider,
    TenantKeyManager,
};
pub use middleware::{AuthLayer, AuthMiddleware, AuthState};
pub use token::{Claims, Token, TokenConfig, TokenError, TokenValidator};
pub use validation::{
    is_within_base, normalize_path, sanitize_filename, validate_filename, validate_path,
    validate_symlink_target, validate_xattr_name, PathValidationConfig, MAX_FILENAME_LENGTH,
    MAX_PATH_LENGTH, MAX_SYMLINK_DEPTH,
};

/// Authentication result.
#[derive(Debug, Clone)]
pub struct AuthInfo {
    /// User ID.
    pub user_id: String,
    /// Username.
    pub username: Option<String>,
    /// Roles or permissions.
    pub roles: Vec<String>,
    /// Whether this is a service account.
    pub is_service: bool,
}

impl AuthInfo {
    /// Create auth info for an anonymous user.
    pub fn anonymous() -> Self {
        Self {
            user_id: "anonymous".to_string(),
            username: None,
            roles: vec![],
            is_service: false,
        }
    }

    /// Create auth info for a service account.
    pub fn service(service_id: impl Into<String>) -> Self {
        Self {
            user_id: service_id.into(),
            username: None,
            roles: vec!["service".to_string()],
            is_service: true,
        }
    }

    /// Check if the user has a specific role.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    /// Check if the user is an admin.
    pub fn is_admin(&self) -> bool {
        self.has_role("admin")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_info_anonymous() {
        let info = AuthInfo::anonymous();
        assert_eq!(info.user_id, "anonymous");
        assert!(!info.is_service);
    }

    #[test]
    fn test_auth_info_service() {
        let info = AuthInfo::service("metadata-server");
        assert_eq!(info.user_id, "metadata-server");
        assert!(info.is_service);
    }

    #[test]
    fn test_has_role() {
        let info = AuthInfo {
            user_id: "test".to_string(),
            username: Some("tester".to_string()),
            roles: vec!["admin".to_string(), "user".to_string()],
            is_service: false,
        };

        assert!(info.has_role("admin"));
        assert!(info.has_role("user"));
        assert!(!info.has_role("guest"));
        assert!(info.is_admin());
    }
}
