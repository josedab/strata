// Tenant context for request scoping

use super::isolation::IsolationPolicy;
use super::tenant::{TenantId, TenantStatus, TenantTier};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tokio::task_local;

// Task-local tenant context for async operations
task_local! {
    static CURRENT_TENANT: TenantContext;
}

/// Tenant context for a request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantContext {
    /// Tenant ID
    pub tenant_id: TenantId,
    /// Tenant name
    pub tenant_name: String,
    /// Tenant status
    pub status: TenantStatus,
    /// Tenant tier
    pub tier: TenantTier,
    /// Namespace prefix
    pub namespace_prefix: String,
    /// Isolation policy
    pub isolation: IsolationPolicy,
    /// User ID making the request
    pub user_id: Option<String>,
    /// Request ID
    pub request_id: String,
    /// Request timestamp
    pub timestamp: SystemTime,
    /// Additional attributes
    pub attributes: HashMap<String, String>,
}

impl TenantContext {
    /// Creates a new tenant context
    pub fn new(
        tenant_id: TenantId,
        tenant_name: String,
        tier: TenantTier,
        namespace_prefix: String,
    ) -> Self {
        Self {
            tenant_id,
            tenant_name,
            status: TenantStatus::Active,
            tier,
            namespace_prefix,
            isolation: IsolationPolicy::default(),
            user_id: None,
            request_id: uuid::Uuid::new_v4().to_string(),
            timestamp: SystemTime::now(),
            attributes: HashMap::new(),
        }
    }

    /// Sets the user ID
    pub fn with_user(mut self, user_id: String) -> Self {
        self.user_id = Some(user_id);
        self
    }

    /// Sets the request ID
    pub fn with_request_id(mut self, request_id: String) -> Self {
        self.request_id = request_id;
        self
    }

    /// Sets an attribute
    pub fn with_attribute(mut self, key: String, value: String) -> Self {
        self.attributes.insert(key, value);
        self
    }

    /// Sets the isolation policy
    pub fn with_isolation(mut self, policy: IsolationPolicy) -> Self {
        self.isolation = policy;
        self
    }

    /// Gets a namespaced path
    pub fn namespaced_path(&self, path: &str) -> String {
        format!("{}/{}", self.namespace_prefix, path.trim_start_matches('/'))
    }

    /// Validates that a path belongs to this tenant's namespace
    pub fn validate_path(&self, path: &str) -> Result<()> {
        if !path.starts_with(&self.namespace_prefix) {
            return Err(StrataError::Forbidden(format!(
                "Path {} is outside tenant namespace",
                path
            )));
        }
        Ok(())
    }

    /// Checks if the tenant is active
    pub fn is_active(&self) -> bool {
        self.status == TenantStatus::Active
    }

    /// Runs a future with this tenant context
    pub async fn scope<F, T>(self, f: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        CURRENT_TENANT.scope(self, f).await
    }

    /// Gets the current tenant context (if in scope)
    pub fn current() -> Option<TenantContext> {
        CURRENT_TENANT.try_with(|ctx| ctx.clone()).ok()
    }
}

/// Scope for tenant operations
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TenantScope {
    /// Single tenant operations
    #[default]
    SingleTenant,
    /// Cross-tenant operations (requires special permissions)
    CrossTenant,
    /// System-level operations (admin only)
    System,
}

/// Request context with tenant and operation details
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Tenant context
    pub tenant: TenantContext,
    /// Operation scope
    pub scope: TenantScope,
    /// Source IP address
    pub source_ip: Option<String>,
    /// User agent
    pub user_agent: Option<String>,
    /// API version
    pub api_version: Option<String>,
    /// Correlation ID for distributed tracing
    pub correlation_id: String,
    /// Parent span ID for tracing
    pub parent_span_id: Option<String>,
    /// Rate limit remaining
    pub rate_limit_remaining: Option<u32>,
}

impl RequestContext {
    /// Creates a new request context
    pub fn new(tenant: TenantContext) -> Self {
        Self {
            correlation_id: tenant.request_id.clone(),
            tenant,
            scope: TenantScope::SingleTenant,
            source_ip: None,
            user_agent: None,
            api_version: None,
            parent_span_id: None,
            rate_limit_remaining: None,
        }
    }

    /// Sets the scope
    pub fn with_scope(mut self, scope: TenantScope) -> Self {
        self.scope = scope;
        self
    }

    /// Sets the source IP
    pub fn with_source_ip(mut self, ip: String) -> Self {
        self.source_ip = Some(ip);
        self
    }

    /// Sets the user agent
    pub fn with_user_agent(mut self, ua: String) -> Self {
        self.user_agent = Some(ua);
        self
    }

    /// Validates the request context
    pub fn validate(&self) -> Result<()> {
        // Check tenant is active
        if !self.tenant.is_active() {
            return Err(StrataError::Forbidden(format!(
                "Tenant {} is not active (status: {:?})",
                self.tenant.tenant_id, self.tenant.status
            )));
        }

        // Additional validation can be added here
        Ok(())
    }
}

/// Tenant context provider
pub struct TenantContextProvider {
    /// Cached tenant contexts
    cache: Arc<RwLock<HashMap<TenantId, TenantContext>>>,
    /// Context TTL
    cache_ttl_secs: u64,
}

impl TenantContextProvider {
    /// Creates a new context provider
    pub fn new(cache_ttl_secs: u64) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl_secs,
        }
    }

    /// Gets or creates a tenant context
    pub async fn get_context(&self, tenant_id: &TenantId) -> Option<TenantContext> {
        self.cache.read().await.get(tenant_id).cloned()
    }

    /// Registers a tenant context
    pub async fn register(&self, context: TenantContext) {
        self.cache
            .write()
            .await
            .insert(context.tenant_id.clone(), context);
    }

    /// Removes a tenant context
    pub async fn remove(&self, tenant_id: &TenantId) {
        self.cache.write().await.remove(tenant_id);
    }

    /// Updates tenant status in cache
    pub async fn update_status(&self, tenant_id: &TenantId, status: TenantStatus) {
        let mut cache = self.cache.write().await;
        if let Some(ctx) = cache.get_mut(tenant_id) {
            ctx.status = status;
        }
    }

    /// Clears expired entries
    pub async fn clear_expired(&self) {
        let _now = SystemTime::now();
        let ttl = std::time::Duration::from_secs(self.cache_ttl_secs);

        let mut cache = self.cache.write().await;
        cache.retain(|_, ctx| {
            ctx.timestamp
                .elapsed()
                .map(|elapsed| elapsed < ttl)
                .unwrap_or(false)
        });
    }
}

impl Default for TenantContextProvider {
    fn default() -> Self {
        Self::new(300) // 5 minute default TTL
    }
}

/// Multi-tenant operation guard
pub struct TenantGuard {
    tenant_id: TenantId,
    operation: String,
    start_time: SystemTime,
}

impl TenantGuard {
    /// Creates a new guard
    pub fn new(tenant_id: TenantId, operation: &str) -> Self {
        Self {
            tenant_id,
            operation: operation.to_string(),
            start_time: SystemTime::now(),
        }
    }

    /// Gets the tenant ID
    pub fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Gets the operation name
    pub fn operation(&self) -> &str {
        &self.operation
    }

    /// Gets elapsed time
    pub fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed().unwrap_or_default()
    }
}

impl Drop for TenantGuard {
    fn drop(&mut self) {
        // Could emit metrics here
        let elapsed = self.elapsed();
        tracing::trace!(
            tenant_id = %self.tenant_id,
            operation = %self.operation,
            elapsed_ms = elapsed.as_millis() as u64,
            "tenant operation completed"
        );
    }
}

/// Extracts tenant ID from various sources with validation.
pub struct TenantExtractor;

impl TenantExtractor {
    /// Extracts and validates tenant ID from HTTP headers.
    ///
    /// Returns `None` if the header is empty or the tenant ID is invalid.
    pub fn from_header(header_value: &str) -> Option<TenantId> {
        let trimmed = header_value.trim();
        if trimmed.is_empty() {
            return None;
        }

        // Validate the tenant ID format
        TenantId::try_new(trimmed.to_lowercase()).ok()
    }

    /// Extracts and validates tenant ID from path prefix.
    ///
    /// Expected format: `/tenants/{tenant_id}/...`
    ///
    /// Performs URL decoding on the tenant ID component.
    pub fn from_path(path: &str) -> Option<TenantId> {
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        if parts.len() >= 2 && parts[0] == "tenants" {
            let raw_tenant_id = parts[1];

            // URL decode the tenant ID (handles %XX encoding)
            let decoded = Self::url_decode(raw_tenant_id)?;

            // Validate and normalize
            TenantId::try_new(decoded.to_lowercase()).ok()
        } else {
            None
        }
    }

    /// Extracts and validates tenant ID from subdomain.
    ///
    /// Expected format: `{tenant_id}.storage.example.com`
    pub fn from_subdomain(host: &str) -> Option<TenantId> {
        // Remove port if present
        let host_without_port = host.split(':').next()?;

        let parts: Vec<&str> = host_without_port.split('.').collect();
        if parts.len() >= 3 {
            // Subdomain is already constrained by DNS label rules
            TenantId::try_new(parts[0].to_lowercase()).ok()
        } else {
            None
        }
    }

    /// Extracts and validates tenant ID from API key.
    ///
    /// Expected format: `{tenant_id}.{key_id}.{secret}`
    ///
    /// Note: Changed separator from `_` to `.` to prevent ambiguity with
    /// tenant IDs that contain underscores.
    pub fn from_api_key(api_key: &str) -> Option<TenantId> {
        // API key format: tenant_id.key_id.secret_hash
        // Using '.' as separator since tenant IDs cannot contain dots
        let parts: Vec<&str> = api_key.split('.').collect();

        if parts.len() >= 3 {
            // Validate tenant ID portion
            TenantId::try_new(parts[0].to_lowercase()).ok()
        } else {
            None
        }
    }

    /// Extracts tenant ID from legacy API key format.
    ///
    /// Legacy format: `{tenant_id}_{key_id}_{secret}`
    ///
    /// # Warning
    /// This format is deprecated due to ambiguity with tenant IDs that might
    /// contain underscores. Use `from_api_key` with dot-separated format instead.
    #[deprecated(note = "Use from_api_key with dot-separated format")]
    pub fn from_legacy_api_key(api_key: &str) -> Option<TenantId> {
        // Old format used underscore which is ambiguous
        // Try to find a valid tenant ID by iterating possible splits
        let parts: Vec<&str> = api_key.split('_').collect();
        if parts.len() >= 3 {
            // Assume first part is tenant ID
            TenantId::try_new(parts[0].to_lowercase()).ok()
        } else {
            None
        }
    }

    /// URL decodes a string, returning None if decoding fails.
    fn url_decode(input: &str) -> Option<String> {
        let mut result = String::with_capacity(input.len());
        let mut chars = input.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '%' {
                // Read two hex digits
                let hex: String = chars.by_ref().take(2).collect();
                if hex.len() != 2 {
                    return None; // Incomplete escape sequence
                }
                let byte = u8::from_str_radix(&hex, 16).ok()?;
                result.push(byte as char);
            } else if c == '+' {
                result.push(' ');
            } else {
                result.push(c);
            }
        }

        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_context_creation() {
        let ctx = TenantContext::new(
            TenantId::new("test-tenant"),
            "Test Tenant".to_string(),
            TenantTier::Standard,
            "tenants/test-tenant".to_string(),
        );

        assert_eq!(ctx.tenant_id.as_str(), "test-tenant");
        assert!(ctx.is_active());
    }

    #[test]
    fn test_namespaced_path() {
        let ctx = TenantContext::new(
            TenantId::new("t1"),
            "Tenant 1".to_string(),
            TenantTier::Basic,
            "tenants/t1".to_string(),
        );

        assert_eq!(
            ctx.namespaced_path("buckets/mybucket"),
            "tenants/t1/buckets/mybucket"
        );
        assert_eq!(
            ctx.namespaced_path("/buckets/mybucket"),
            "tenants/t1/buckets/mybucket"
        );
    }

    #[test]
    fn test_path_validation() {
        let ctx = TenantContext::new(
            TenantId::new("t1"),
            "Tenant 1".to_string(),
            TenantTier::Basic,
            "tenants/t1".to_string(),
        );

        // Valid path
        assert!(ctx.validate_path("tenants/t1/buckets/test").is_ok());

        // Invalid path (different tenant)
        assert!(ctx.validate_path("tenants/t2/buckets/test").is_err());
    }

    #[test]
    fn test_tenant_extractor_from_header() {
        // Valid tenant ID
        assert_eq!(
            TenantExtractor::from_header("my-tenant"),
            Some(TenantId::new("my-tenant"))
        );

        // With whitespace (should be trimmed)
        assert_eq!(
            TenantExtractor::from_header("  my-tenant  "),
            Some(TenantId::new("my-tenant"))
        );

        // Case normalization
        assert_eq!(
            TenantExtractor::from_header("My-Tenant"),
            Some(TenantId::new("my-tenant"))
        );

        // Empty header
        assert_eq!(TenantExtractor::from_header(""), None);
        assert_eq!(TenantExtractor::from_header("   "), None);

        // Invalid characters (should fail validation)
        assert_eq!(TenantExtractor::from_header("tenant_with_underscore"), None);
        assert_eq!(TenantExtractor::from_header("tenant.with.dots"), None);
    }

    #[test]
    fn test_tenant_extractor_from_path() {
        // Valid path
        assert_eq!(
            TenantExtractor::from_path("/tenants/abc123/buckets/test"),
            Some(TenantId::new("abc123"))
        );

        // Case normalization
        assert_eq!(
            TenantExtractor::from_path("/tenants/ABC123/buckets/test"),
            Some(TenantId::new("abc123"))
        );

        // URL encoded path
        assert_eq!(
            TenantExtractor::from_path("/tenants/my%2dtenant/buckets/test"),
            Some(TenantId::new("my-tenant"))
        );

        // Invalid path format
        assert_eq!(TenantExtractor::from_path("/buckets/test"), None);
        assert_eq!(TenantExtractor::from_path("/tenants/"), None);
    }

    #[test]
    fn test_tenant_extractor_from_subdomain() {
        // Valid subdomain
        assert_eq!(
            TenantExtractor::from_subdomain("acme.storage.example.com"),
            Some(TenantId::new("acme"))
        );

        // With port
        assert_eq!(
            TenantExtractor::from_subdomain("acme.storage.example.com:8080"),
            Some(TenantId::new("acme"))
        );

        // Case normalization
        assert_eq!(
            TenantExtractor::from_subdomain("ACME.storage.example.com"),
            Some(TenantId::new("acme"))
        );

        // Not enough parts
        assert_eq!(TenantExtractor::from_subdomain("localhost"), None);
        assert_eq!(TenantExtractor::from_subdomain("example.com"), None);
    }

    #[test]
    fn test_tenant_extractor_from_api_key() {
        // Valid API key format (dot-separated)
        assert_eq!(
            TenantExtractor::from_api_key("tenant1.key123.secrethash"),
            Some(TenantId::new("tenant1"))
        );

        // With hyphen in tenant ID
        assert_eq!(
            TenantExtractor::from_api_key("my-tenant.key123.secrethash"),
            Some(TenantId::new("my-tenant"))
        );

        // Invalid format (too few parts)
        assert_eq!(TenantExtractor::from_api_key("tenant1.key123"), None);
        assert_eq!(TenantExtractor::from_api_key("tenant1"), None);
    }

    #[test]
    fn test_tenant_id_validation() {
        // Valid tenant IDs
        assert!(TenantId::try_new("valid-tenant").is_ok());
        assert!(TenantId::try_new("tenant123").is_ok());
        assert!(TenantId::try_new("a").is_ok());

        // Invalid: too long
        let long_id = "a".repeat(64);
        assert!(TenantId::try_new(long_id).is_err());

        // Invalid: empty
        assert!(TenantId::try_new("").is_err());

        // Invalid: starts with hyphen
        assert!(TenantId::try_new("-tenant").is_err());

        // Invalid: ends with hyphen
        assert!(TenantId::try_new("tenant-").is_err());

        // Invalid: consecutive hyphens
        assert!(TenantId::try_new("tenant--id").is_err());

        // Invalid: uppercase (should be lowercase)
        assert!(TenantId::try_new("TENANT").is_err());

        // Invalid: special characters
        assert!(TenantId::try_new("tenant_id").is_err());
        assert!(TenantId::try_new("tenant.id").is_err());
        assert!(TenantId::try_new("tenant@id").is_err());

        // System tenant (allowed)
        assert!(TenantId::try_new("__system__").is_ok());
    }

    #[test]
    fn test_url_decode() {
        // Basic URL decoding via from_path
        assert_eq!(
            TenantExtractor::from_path("/tenants/test%2d123/bucket"),
            Some(TenantId::new("test-123"))
        );

        // Plus sign to space (then fails validation since spaces are invalid)
        assert_eq!(
            TenantExtractor::from_path("/tenants/test+123/bucket"),
            None
        );
    }

    #[tokio::test]
    async fn test_context_provider() {
        let provider = TenantContextProvider::new(300);

        let ctx = TenantContext::new(
            TenantId::new("cached-tenant"),
            "Cached".to_string(),
            TenantTier::Professional,
            "tenants/cached-tenant".to_string(),
        );

        provider.register(ctx.clone()).await;

        let retrieved = provider
            .get_context(&TenantId::new("cached-tenant"))
            .await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().tenant_name, "Cached");
    }

    #[test]
    fn test_request_context() {
        let tenant = TenantContext::new(
            TenantId::new("req-tenant"),
            "Request Tenant".to_string(),
            TenantTier::Enterprise,
            "tenants/req-tenant".to_string(),
        );

        let req_ctx = RequestContext::new(tenant)
            .with_scope(TenantScope::SingleTenant)
            .with_source_ip("192.168.1.1".to_string())
            .with_user_agent("strata-client/1.0".to_string());

        assert!(req_ctx.validate().is_ok());
        assert_eq!(req_ctx.source_ip, Some("192.168.1.1".to_string()));
    }
}
