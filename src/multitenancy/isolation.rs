// Tenant isolation and namespace management

use super::tenant::TenantId;
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Level of isolation between tenants
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    /// Soft isolation - logical separation only
    Soft,
    /// Standard isolation - separate namespaces
    #[default]
    Standard,
    /// Strong isolation - separate storage paths
    Strong,
    /// Complete isolation - dedicated resources
    Dedicated,
}

/// Isolation policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsolationPolicy {
    /// Isolation level
    pub level: IsolationLevel,
    /// Encrypt tenant data at rest
    pub encrypt_at_rest: bool,
    /// Use tenant-specific encryption keys
    pub tenant_keys: bool,
    /// Isolate network traffic
    pub network_isolation: bool,
    /// Use separate storage pools
    pub storage_isolation: bool,
    /// Enable cross-tenant access controls
    pub cross_tenant_access: CrossTenantAccess,
    /// Audit all cross-tenant operations
    pub audit_cross_tenant: bool,
}

impl Default for IsolationPolicy {
    fn default() -> Self {
        Self {
            level: IsolationLevel::Standard,
            encrypt_at_rest: true,
            tenant_keys: false,
            network_isolation: false,
            storage_isolation: false,
            cross_tenant_access: CrossTenantAccess::Denied,
            audit_cross_tenant: true,
        }
    }
}

impl IsolationPolicy {
    /// Soft isolation policy
    pub fn soft() -> Self {
        Self {
            level: IsolationLevel::Soft,
            encrypt_at_rest: false,
            tenant_keys: false,
            network_isolation: false,
            storage_isolation: false,
            cross_tenant_access: CrossTenantAccess::AllowedWithGrant,
            audit_cross_tenant: true,
        }
    }

    /// Strong isolation policy
    pub fn strong() -> Self {
        Self {
            level: IsolationLevel::Strong,
            encrypt_at_rest: true,
            tenant_keys: true,
            network_isolation: true,
            storage_isolation: true,
            cross_tenant_access: CrossTenantAccess::Denied,
            audit_cross_tenant: true,
        }
    }

    /// Dedicated isolation (maximum)
    pub fn dedicated() -> Self {
        Self {
            level: IsolationLevel::Dedicated,
            encrypt_at_rest: true,
            tenant_keys: true,
            network_isolation: true,
            storage_isolation: true,
            cross_tenant_access: CrossTenantAccess::Denied,
            audit_cross_tenant: true,
        }
    }
}

/// Cross-tenant access policy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossTenantAccess {
    /// No cross-tenant access allowed
    #[default]
    Denied,
    /// Allowed only with explicit grants
    AllowedWithGrant,
    /// Allowed for parent/child tenants
    AllowedHierarchical,
}

/// Tenant namespace for resource isolation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantNamespace {
    /// Tenant ID
    pub tenant_id: TenantId,
    /// Namespace prefix (for storage paths)
    pub prefix: String,
    /// Storage pool assignment
    pub storage_pool: Option<String>,
    /// Encryption key ID
    pub encryption_key_id: Option<String>,
    /// Network segment
    pub network_segment: Option<String>,
    /// Isolation policy
    pub policy: IsolationPolicy,
}

impl TenantNamespace {
    /// Creates a new namespace for a tenant
    pub fn new(tenant_id: TenantId) -> Self {
        let prefix = format!("tenants/{}", tenant_id);
        Self {
            tenant_id,
            prefix,
            storage_pool: None,
            encryption_key_id: None,
            network_segment: None,
            policy: IsolationPolicy::default(),
        }
    }

    /// Creates with dedicated resources
    pub fn dedicated(tenant_id: TenantId, storage_pool: String) -> Self {
        let prefix = format!("tenants/{}", tenant_id);
        Self {
            tenant_id: tenant_id.clone(),
            prefix,
            storage_pool: Some(storage_pool),
            encryption_key_id: Some(format!("key-{}", tenant_id)),
            network_segment: Some(format!("net-{}", tenant_id)),
            policy: IsolationPolicy::dedicated(),
        }
    }

    /// Gets the full path for a resource
    pub fn resource_path(&self, path: &str) -> String {
        format!("{}/{}", self.prefix, path.trim_start_matches('/'))
    }

    /// Gets the bucket path
    pub fn bucket_path(&self, bucket: &str) -> String {
        self.resource_path(&format!("buckets/{}", bucket))
    }

    /// Gets the object path
    pub fn object_path(&self, bucket: &str, key: &str) -> String {
        self.resource_path(&format!("buckets/{}/objects/{}", bucket, key))
    }

    /// Gets the metadata path
    pub fn metadata_path(&self) -> String {
        self.resource_path("metadata")
    }

    /// Validates a path belongs to this namespace.
    ///
    /// This method performs secure path validation to prevent:
    /// - Prefix collision attacks (e.g., "tenant-1" matching "tenant-12")
    /// - Path traversal attacks (e.g., "../other-tenant")
    pub fn validate_path(&self, path: &str) -> bool {
        // First, normalize the path to remove any traversal attempts
        let normalized = match normalize_path(path) {
            Some(p) => p,
            None => return false, // Path normalization failed (contains invalid sequences)
        };

        // Check with proper delimiter handling to prevent prefix collision
        is_path_within_prefix(&normalized, &self.prefix)
    }

    /// Validates a path and returns the normalized version if valid.
    ///
    /// Returns None if the path is outside the namespace or contains
    /// path traversal sequences.
    pub fn validate_and_normalize(&self, path: &str) -> Option<String> {
        let normalized = normalize_path(path)?;
        if is_path_within_prefix(&normalized, &self.prefix) {
            Some(normalized)
        } else {
            None
        }
    }

    /// Extracts the relative path from an absolute path
    pub fn relative_path<'a>(&self, path: &'a str) -> Option<&'a str> {
        // First validate the path is within namespace
        if !self.validate_path(path) {
            return None;
        }
        path.strip_prefix(&self.prefix).map(|p| p.trim_start_matches('/'))
    }
}

/// Normalizes a path by resolving `.` and `..` components.
///
/// Returns None if the path attempts to traverse above the root,
/// or contains other invalid sequences.
fn normalize_path(path: &str) -> Option<String> {
    // Reject null bytes and other control characters
    if path.bytes().any(|b| b == 0 || b < 32) {
        return None;
    }

    // Use Path for component iteration (handles various separators)
    let path_obj = Path::new(path);
    let mut normalized_components: Vec<&str> = Vec::new();

    for component in path_obj.components() {
        use std::path::Component;
        match component {
            Component::Normal(s) => {
                if let Some(s_str) = s.to_str() {
                    // Reject components that could be used for injection
                    if s_str.contains('\0') || s_str.is_empty() {
                        return None;
                    }
                    normalized_components.push(s_str);
                } else {
                    return None; // Invalid UTF-8
                }
            }
            Component::CurDir => {
                // Skip "." components
            }
            Component::ParentDir => {
                // Try to go up one level
                if normalized_components.pop().is_none() {
                    // Attempted to go above root - this is an attack
                    return None;
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                // Skip root/prefix - we're dealing with relative-style paths
            }
        }
    }

    if normalized_components.is_empty() {
        Some(String::new())
    } else {
        Some(normalized_components.join("/"))
    }
}

/// Checks if a path is within a given prefix, with proper delimiter handling.
///
/// This prevents prefix collision attacks where "tenant-1" could match "tenant-12".
fn is_path_within_prefix(path: &str, prefix: &str) -> bool {
    if path == prefix {
        return true;
    }

    // The path must either be exactly the prefix, or start with prefix + "/"
    // This prevents "tenants/tenant-1" from matching "tenants/tenant-12"
    if path.starts_with(prefix) {
        let remainder = &path[prefix.len()..];
        // The character after the prefix must be a path separator
        remainder.starts_with('/')
    } else {
        false
    }
}

/// Checks if a resource matches a grant pattern with proper delimiter handling.
fn resource_matches_pattern(resource: &str, pattern: &str) -> bool {
    if resource == pattern {
        return true;
    }

    // Pattern must be a proper prefix of resource (ends with / or resource continues with /)
    if resource.starts_with(pattern) {
        let remainder = &resource[pattern.len()..];
        // Either pattern ends with / or resource continues with /
        pattern.ends_with('/') || remainder.starts_with('/')
    } else {
        false
    }
}

/// Cross-tenant access grant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessGrant {
    /// Granting tenant
    pub granter: TenantId,
    /// Receiving tenant
    pub grantee: TenantId,
    /// Resource pattern (bucket or bucket/prefix)
    pub resource_pattern: String,
    /// Granted permissions
    pub permissions: Vec<GrantedPermission>,
    /// Expiration time
    pub expires_at: Option<std::time::SystemTime>,
    /// Created at
    pub created_at: std::time::SystemTime,
}

/// Permission types that can be granted
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrantedPermission {
    Read,
    Write,
    List,
    Delete,
}

/// Namespace manager for tenant isolation
pub struct NamespaceManager {
    /// Tenant namespaces
    namespaces: Arc<RwLock<HashMap<TenantId, TenantNamespace>>>,
    /// Cross-tenant grants
    grants: Arc<RwLock<Vec<AccessGrant>>>,
    /// Default isolation policy
    default_policy: IsolationPolicy,
}

impl NamespaceManager {
    /// Creates a new namespace manager
    pub fn new(default_policy: IsolationPolicy) -> Self {
        Self {
            namespaces: Arc::new(RwLock::new(HashMap::new())),
            grants: Arc::new(RwLock::new(Vec::new())),
            default_policy,
        }
    }

    /// Creates a namespace for a tenant
    pub async fn create_namespace(&self, tenant_id: TenantId) -> Result<TenantNamespace> {
        let mut namespaces = self.namespaces.write().await;

        if namespaces.contains_key(&tenant_id) {
            return Err(StrataError::AlreadyExists(format!(
                "Namespace for tenant {} already exists",
                tenant_id
            )));
        }

        let mut namespace = TenantNamespace::new(tenant_id.clone());
        namespace.policy = self.default_policy.clone();

        namespaces.insert(tenant_id, namespace.clone());

        Ok(namespace)
    }

    /// Creates a dedicated namespace
    pub async fn create_dedicated_namespace(
        &self,
        tenant_id: TenantId,
        storage_pool: String,
    ) -> Result<TenantNamespace> {
        let mut namespaces = self.namespaces.write().await;

        if namespaces.contains_key(&tenant_id) {
            return Err(StrataError::AlreadyExists(format!(
                "Namespace for tenant {} already exists",
                tenant_id
            )));
        }

        let namespace = TenantNamespace::dedicated(tenant_id.clone(), storage_pool);
        namespaces.insert(tenant_id, namespace.clone());

        Ok(namespace)
    }

    /// Gets a namespace
    pub async fn get_namespace(&self, tenant_id: &TenantId) -> Option<TenantNamespace> {
        self.namespaces.read().await.get(tenant_id).cloned()
    }

    /// Deletes a namespace
    pub async fn delete_namespace(&self, tenant_id: &TenantId) -> Result<()> {
        let mut namespaces = self.namespaces.write().await;

        if namespaces.remove(tenant_id).is_none() {
            return Err(StrataError::NotFound(format!(
                "Namespace for tenant {}",
                tenant_id
            )));
        }

        // Also remove any grants involving this tenant
        let mut grants = self.grants.write().await;
        grants.retain(|g| g.granter != *tenant_id && g.grantee != *tenant_id);

        Ok(())
    }

    /// Grants cross-tenant access
    pub async fn grant_access(&self, grant: AccessGrant) -> Result<()> {
        // Verify both namespaces exist
        let namespaces = self.namespaces.read().await;

        let granter_ns = namespaces.get(&grant.granter).ok_or_else(|| {
            StrataError::NotFound(format!("Granter tenant {}", grant.granter))
        })?;

        if !namespaces.contains_key(&grant.grantee) {
            return Err(StrataError::NotFound(format!(
                "Grantee tenant {}",
                grant.grantee
            )));
        }

        // Check if granter's policy allows cross-tenant access
        match granter_ns.policy.cross_tenant_access {
            CrossTenantAccess::Denied => {
                return Err(StrataError::Forbidden(
                    "Cross-tenant access is denied by policy".to_string(),
                ));
            }
            CrossTenantAccess::AllowedWithGrant | CrossTenantAccess::AllowedHierarchical => {}
        }

        drop(namespaces);

        let mut grants = self.grants.write().await;
        grants.push(grant);

        Ok(())
    }

    /// Revokes a cross-tenant access grant
    pub async fn revoke_access(
        &self,
        granter: &TenantId,
        grantee: &TenantId,
        resource_pattern: &str,
    ) {
        let mut grants = self.grants.write().await;
        grants.retain(|g| {
            !(g.granter == *granter
                && g.grantee == *grantee
                && g.resource_pattern == resource_pattern)
        });
    }

    /// Checks if a tenant has access to another tenant's resource
    pub async fn check_access(
        &self,
        accessor: &TenantId,
        owner: &TenantId,
        resource: &str,
        permission: GrantedPermission,
    ) -> bool {
        // Same tenant always has access
        if accessor == owner {
            return true;
        }

        // Normalize the resource path to prevent traversal attacks
        let normalized_resource = match normalize_path(resource) {
            Some(r) => r,
            None => return false, // Invalid path
        };

        let grants = self.grants.read().await;
        let now = std::time::SystemTime::now();

        for grant in grants.iter() {
            if grant.granter != *owner || grant.grantee != *accessor {
                continue;
            }

            // Check expiration
            if let Some(expires) = grant.expires_at {
                if now >= expires {
                    continue;
                }
            }

            // Check resource pattern match with proper delimiter handling
            if !resource_matches_pattern(&normalized_resource, &grant.resource_pattern) {
                continue;
            }

            // Check permission
            if grant.permissions.contains(&permission) {
                return true;
            }
        }

        false
    }

    /// Lists grants for a tenant
    pub async fn list_grants(&self, tenant_id: &TenantId) -> Vec<AccessGrant> {
        self.grants
            .read()
            .await
            .iter()
            .filter(|g| g.granter == *tenant_id || g.grantee == *tenant_id)
            .cloned()
            .collect()
    }

    /// Validates a request against namespace isolation.
    ///
    /// This method:
    /// - Normalizes the path to prevent traversal attacks
    /// - Checks the path is within the tenant's namespace with proper delimiter handling
    /// - Returns the namespace if valid
    pub async fn validate_request(
        &self,
        tenant_id: &TenantId,
        path: &str,
    ) -> Result<TenantNamespace> {
        let namespaces = self.namespaces.read().await;

        let namespace = namespaces
            .get(tenant_id)
            .ok_or_else(|| StrataError::NotFound(format!("Namespace for tenant {}", tenant_id)))?;

        // Normalize the path to prevent traversal attacks
        let normalized_path = normalize_path(path).ok_or_else(|| {
            StrataError::InvalidInput(format!(
                "Invalid path: contains illegal characters or traversal sequences"
            ))
        })?;

        // Validate path is within namespace with proper delimiter handling
        if !is_path_within_prefix(&normalized_path, &namespace.prefix) {
            return Err(StrataError::Forbidden(format!(
                "Path is outside tenant namespace",
            )));
        }

        Ok(namespace.clone())
    }
}

impl Default for NamespaceManager {
    fn default() -> Self {
        Self::new(IsolationPolicy::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_paths() {
        let ns = TenantNamespace::new(TenantId::new("tenant-1"));

        assert_eq!(ns.prefix, "tenants/tenant-1");
        assert_eq!(ns.bucket_path("my-bucket"), "tenants/tenant-1/buckets/my-bucket");
        assert_eq!(
            ns.object_path("my-bucket", "file.txt"),
            "tenants/tenant-1/buckets/my-bucket/objects/file.txt"
        );
    }

    #[test]
    fn test_namespace_validation() {
        let ns = TenantNamespace::new(TenantId::new("tenant-1"));

        // Valid paths
        assert!(ns.validate_path("tenants/tenant-1/buckets/test"));
        assert!(ns.validate_path("tenants/tenant-1"));

        // Invalid paths - different tenant
        assert!(!ns.validate_path("tenants/tenant-2/buckets/test"));
        assert!(!ns.validate_path("/other/path"));
    }

    #[test]
    fn test_prefix_collision_prevention() {
        let ns = TenantNamespace::new(TenantId::new("tenant-1"));

        // This should NOT match because "tenant-12" is a different tenant
        assert!(!ns.validate_path("tenants/tenant-12/buckets/test"));
        assert!(!ns.validate_path("tenants/tenant-1extra/buckets/test"));

        // But subpaths of tenant-1 should match
        assert!(ns.validate_path("tenants/tenant-1/foo"));
    }

    #[test]
    fn test_path_traversal_prevention() {
        let ns = TenantNamespace::new(TenantId::new("tenant-1"));

        // Traversal attacks should be blocked
        assert!(!ns.validate_path("tenants/tenant-1/../tenant-2/buckets/test"));
        assert!(!ns.validate_path("tenants/tenant-1/buckets/../../tenant-2"));
        assert!(!ns.validate_path("../tenants/tenant-2/buckets/test"));

        // Single dot should be normalized but still valid if in namespace
        assert!(ns.validate_path("tenants/tenant-1/./buckets/test"));
    }

    #[test]
    fn test_null_byte_rejection() {
        let ns = TenantNamespace::new(TenantId::new("tenant-1"));

        // Null bytes should be rejected
        assert!(!ns.validate_path("tenants/tenant-1/buckets/test\0evil"));
    }

    #[test]
    fn test_relative_path() {
        let ns = TenantNamespace::new(TenantId::new("tenant-1"));

        let rel = ns.relative_path("tenants/tenant-1/buckets/test");
        assert_eq!(rel, Some("buckets/test"));

        // Should return None for paths outside namespace
        let rel = ns.relative_path("tenants/tenant-2/buckets/test");
        assert_eq!(rel, None);

        // Should return None for traversal attempts
        let rel = ns.relative_path("tenants/tenant-1/../tenant-2/buckets/test");
        assert_eq!(rel, None);
    }

    #[test]
    fn test_normalize_path() {
        // Basic normalization
        assert_eq!(normalize_path("a/b/c"), Some("a/b/c".to_string()));
        assert_eq!(normalize_path("a/./b/c"), Some("a/b/c".to_string()));
        assert_eq!(normalize_path("a/b/../c"), Some("a/c".to_string()));

        // Traversal above root should fail
        assert_eq!(normalize_path("../a/b"), None);
        assert_eq!(normalize_path("a/../../b"), None);

        // Null bytes should fail
        assert_eq!(normalize_path("a/b\0c"), None);
    }

    #[test]
    fn test_resource_matches_pattern() {
        // Exact match
        assert!(resource_matches_pattern("bucket/file", "bucket/file"));

        // Pattern is prefix with /
        assert!(resource_matches_pattern("bucket/file", "bucket/"));
        assert!(resource_matches_pattern("bucket/subdir/file", "bucket/"));

        // Pattern is prefix, resource continues with /
        assert!(resource_matches_pattern("bucket/file", "bucket"));

        // Should NOT match partial names
        assert!(!resource_matches_pattern("bucket2/file", "bucket"));
        assert!(!resource_matches_pattern("bucket-extra/file", "bucket"));
    }

    #[tokio::test]
    async fn test_namespace_manager() {
        let manager = NamespaceManager::new(IsolationPolicy::default());

        let ns = manager
            .create_namespace(TenantId::new("t1"))
            .await
            .unwrap();

        assert_eq!(ns.tenant_id.as_str(), "t1");

        // Get namespace
        let retrieved = manager.get_namespace(&TenantId::new("t1")).await.unwrap();
        assert_eq!(retrieved.prefix, ns.prefix);

        // Delete
        manager.delete_namespace(&TenantId::new("t1")).await.unwrap();
        assert!(manager.get_namespace(&TenantId::new("t1")).await.is_none());
    }

    #[tokio::test]
    async fn test_cross_tenant_access() {
        let manager = NamespaceManager::new(IsolationPolicy {
            cross_tenant_access: CrossTenantAccess::AllowedWithGrant,
            ..Default::default()
        });

        manager.create_namespace(TenantId::new("t1")).await.unwrap();
        manager.create_namespace(TenantId::new("t2")).await.unwrap();

        // No access initially
        assert!(
            !manager
                .check_access(
                    &TenantId::new("t2"),
                    &TenantId::new("t1"),
                    "bucket/file",
                    GrantedPermission::Read
                )
                .await
        );

        // Grant access
        let grant = AccessGrant {
            granter: TenantId::new("t1"),
            grantee: TenantId::new("t2"),
            resource_pattern: "bucket/".to_string(),
            permissions: vec![GrantedPermission::Read],
            expires_at: None,
            created_at: std::time::SystemTime::now(),
        };
        manager.grant_access(grant).await.unwrap();

        // Now has access
        assert!(
            manager
                .check_access(
                    &TenantId::new("t2"),
                    &TenantId::new("t1"),
                    "bucket/file",
                    GrantedPermission::Read
                )
                .await
        );

        // But not write
        assert!(
            !manager
                .check_access(
                    &TenantId::new("t2"),
                    &TenantId::new("t1"),
                    "bucket/file",
                    GrantedPermission::Write
                )
                .await
        );
    }
}
