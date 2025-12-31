// Tenant management for multi-tenancy support

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Minimum length for tenant ID.
pub const TENANT_ID_MIN_LENGTH: usize = 1;

/// Maximum length for tenant ID (DNS subdomain label limit).
pub const TENANT_ID_MAX_LENGTH: usize = 63;

/// Reserved tenant ID prefix for system use.
const SYSTEM_PREFIX: &str = "__";

/// Unique tenant identifier.
///
/// Tenant IDs must:
/// - Be 1-63 characters long
/// - Contain only lowercase alphanumeric characters and hyphens
/// - Not start or end with a hyphen
/// - Not contain consecutive hyphens
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(pub String);

impl TenantId {
    /// Creates a new tenant ID without validation.
    ///
    /// # Warning
    /// This method does not validate the tenant ID format.
    /// Use `try_new` for user-provided input.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Creates a new tenant ID with validation.
    ///
    /// Returns an error if the tenant ID is invalid.
    pub fn try_new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        Self::validate_format(&id)?;
        Ok(Self(id))
    }

    /// Validates a tenant ID format.
    pub fn validate_format(id: &str) -> Result<()> {
        // Check length
        if id.len() < TENANT_ID_MIN_LENGTH {
            return Err(StrataError::Validation(
                "Tenant ID cannot be empty".to_string(),
            ));
        }

        if id.len() > TENANT_ID_MAX_LENGTH {
            return Err(StrataError::Validation(format!(
                "Tenant ID exceeds maximum length of {} characters",
                TENANT_ID_MAX_LENGTH
            )));
        }

        // Allow system prefixed IDs (internal use only)
        if id.starts_with(SYSTEM_PREFIX) {
            return Ok(());
        }

        // Check for valid characters (lowercase alphanumeric and hyphen)
        if !id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(StrataError::Validation(
                "Tenant ID must contain only lowercase letters, numbers, and hyphens".to_string(),
            ));
        }

        // Check start/end characters
        if id.starts_with('-') || id.ends_with('-') {
            return Err(StrataError::Validation(
                "Tenant ID cannot start or end with a hyphen".to_string(),
            ));
        }

        // Check for consecutive hyphens
        if id.contains("--") {
            return Err(StrataError::Validation(
                "Tenant ID cannot contain consecutive hyphens".to_string(),
            ));
        }

        Ok(())
    }

    /// Checks if this tenant ID has a valid format.
    pub fn is_valid(&self) -> bool {
        Self::validate_format(&self.0).is_ok()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// System tenant (for internal operations)
    pub fn system() -> Self {
        Self("__system__".to_string())
    }

    /// Default tenant (for backward compatibility)
    pub fn default_tenant() -> Self {
        Self("default".to_string())
    }

    pub fn is_system(&self) -> bool {
        self.0.starts_with(SYSTEM_PREFIX)
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Tenant status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantStatus {
    /// Tenant is being provisioned
    Provisioning,
    /// Tenant is active and operational
    #[default]
    Active,
    /// Tenant is suspended (billing issue, policy violation)
    Suspended,
    /// Tenant is being deprovisioned
    Deprovisioning,
    /// Tenant has been deleted (soft delete for audit)
    Deleted,
}

/// Tenant tier/plan
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantTier {
    /// Free tier with limited resources
    Free,
    /// Basic paid tier
    #[default]
    Basic,
    /// Standard tier
    Standard,
    /// Professional tier
    Professional,
    /// Enterprise tier with full features
    Enterprise,
    /// Custom tier with negotiated limits
    Custom,
}

/// Tenant configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantConfig {
    /// Maximum storage in bytes
    pub max_storage_bytes: u64,
    /// Maximum number of buckets
    pub max_buckets: u32,
    /// Maximum number of objects
    pub max_objects: u64,
    /// Maximum object size in bytes
    pub max_object_size: u64,
    /// Maximum IOPS
    pub max_iops: u32,
    /// Maximum bandwidth in bytes/second
    pub max_bandwidth_bps: u64,
    /// Maximum number of users
    pub max_users: u32,
    /// Maximum number of access keys per user
    pub max_keys_per_user: u32,
    /// Enable versioning
    pub versioning_enabled: bool,
    /// Enable replication
    pub replication_enabled: bool,
    /// Enable encryption at rest
    pub encryption_enabled: bool,
    /// Enable WORM/compliance features
    pub worm_enabled: bool,
    /// Custom metadata allowed
    pub custom_metadata_enabled: bool,
    /// Lifecycle policies enabled
    pub lifecycle_enabled: bool,
    /// Data retention days (0 = unlimited)
    pub data_retention_days: u32,
}

impl Default for TenantConfig {
    fn default() -> Self {
        Self {
            max_storage_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            max_buckets: 100,
            max_objects: 1_000_000,
            max_object_size: 5 * 1024 * 1024 * 1024, // 5 GB
            max_iops: 1000,
            max_bandwidth_bps: 100 * 1024 * 1024, // 100 MB/s
            max_users: 10,
            max_keys_per_user: 2,
            versioning_enabled: true,
            replication_enabled: false,
            encryption_enabled: true,
            worm_enabled: false,
            custom_metadata_enabled: true,
            lifecycle_enabled: true,
            data_retention_days: 0,
        }
    }
}

impl TenantConfig {
    /// Configuration for free tier
    pub fn free_tier() -> Self {
        Self {
            max_storage_bytes: 5 * 1024 * 1024 * 1024, // 5 GB
            max_buckets: 10,
            max_objects: 10_000,
            max_object_size: 100 * 1024 * 1024, // 100 MB
            max_iops: 100,
            max_bandwidth_bps: 10 * 1024 * 1024, // 10 MB/s
            max_users: 1,
            max_keys_per_user: 1,
            versioning_enabled: false,
            replication_enabled: false,
            encryption_enabled: false,
            worm_enabled: false,
            custom_metadata_enabled: false,
            lifecycle_enabled: false,
            data_retention_days: 30,
        }
    }

    /// Configuration for enterprise tier
    pub fn enterprise_tier() -> Self {
        Self {
            max_storage_bytes: u64::MAX,
            max_buckets: u32::MAX,
            max_objects: u64::MAX,
            max_object_size: 5 * 1024 * 1024 * 1024 * 1024, // 5 TB
            max_iops: u32::MAX,
            max_bandwidth_bps: u64::MAX,
            max_users: u32::MAX,
            max_keys_per_user: 10,
            versioning_enabled: true,
            replication_enabled: true,
            encryption_enabled: true,
            worm_enabled: true,
            custom_metadata_enabled: true,
            lifecycle_enabled: true,
            data_retention_days: 0,
        }
    }

    /// Get config for a tier
    pub fn for_tier(tier: TenantTier) -> Self {
        match tier {
            TenantTier::Free => Self::free_tier(),
            TenantTier::Basic => Self::default(),
            TenantTier::Standard => Self {
                max_storage_bytes: 1024 * 1024 * 1024 * 1024, // 1 TB
                max_buckets: 500,
                max_objects: 10_000_000,
                max_iops: 5000,
                max_bandwidth_bps: 500 * 1024 * 1024, // 500 MB/s
                max_users: 50,
                replication_enabled: true,
                ..Default::default()
            },
            TenantTier::Professional => Self {
                max_storage_bytes: 10 * 1024 * 1024 * 1024 * 1024, // 10 TB
                max_buckets: 2000,
                max_objects: 100_000_000,
                max_iops: 20000,
                max_bandwidth_bps: 1024 * 1024 * 1024, // 1 GB/s
                max_users: 200,
                replication_enabled: true,
                worm_enabled: true,
                ..Default::default()
            },
            TenantTier::Enterprise | TenantTier::Custom => Self::enterprise_tier(),
        }
    }
}

/// Tenant contact information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TenantContact {
    /// Organization name
    pub organization: String,
    /// Primary contact email
    pub email: String,
    /// Phone number
    pub phone: Option<String>,
    /// Billing email
    pub billing_email: Option<String>,
    /// Technical contact email
    pub technical_email: Option<String>,
}

/// A tenant in the multi-tenant system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    /// Unique tenant ID
    pub id: TenantId,
    /// Display name
    pub name: String,
    /// Tenant status
    pub status: TenantStatus,
    /// Tenant tier
    pub tier: TenantTier,
    /// Configuration/limits
    pub config: TenantConfig,
    /// Contact information
    pub contact: TenantContact,
    /// Custom tags/labels
    pub tags: HashMap<String, String>,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Last update timestamp
    pub updated_at: SystemTime,
    /// Suspension reason (if suspended)
    pub suspension_reason: Option<String>,
    /// Parent tenant ID (for hierarchical tenants)
    pub parent_id: Option<TenantId>,
    /// Is this a managed tenant (created by system)
    pub managed: bool,
}

impl Tenant {
    /// Creates a new tenant
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        let now = SystemTime::now();
        Self {
            id: TenantId::new(id),
            name: name.into(),
            status: TenantStatus::Provisioning,
            tier: TenantTier::Basic,
            config: TenantConfig::default(),
            contact: TenantContact::default(),
            tags: HashMap::new(),
            created_at: now,
            updated_at: now,
            suspension_reason: None,
            parent_id: None,
            managed: false,
        }
    }

    /// Sets the tier and updates config accordingly
    pub fn with_tier(mut self, tier: TenantTier) -> Self {
        self.tier = tier;
        self.config = TenantConfig::for_tier(tier);
        self
    }

    /// Sets custom config
    pub fn with_config(mut self, config: TenantConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets contact information
    pub fn with_contact(mut self, contact: TenantContact) -> Self {
        self.contact = contact;
        self
    }

    /// Adds a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Activates the tenant
    pub fn activate(&mut self) {
        self.status = TenantStatus::Active;
        self.updated_at = SystemTime::now();
    }

    /// Suspends the tenant
    pub fn suspend(&mut self, reason: impl Into<String>) {
        self.status = TenantStatus::Suspended;
        self.suspension_reason = Some(reason.into());
        self.updated_at = SystemTime::now();
    }

    /// Unsuspends the tenant
    pub fn unsuspend(&mut self) {
        if self.status == TenantStatus::Suspended {
            self.status = TenantStatus::Active;
            self.suspension_reason = None;
            self.updated_at = SystemTime::now();
        }
    }

    /// Checks if tenant is operational
    pub fn is_operational(&self) -> bool {
        self.status == TenantStatus::Active
    }

    /// Checks if tenant can create resources
    pub fn can_create_resources(&self) -> bool {
        matches!(
            self.status,
            TenantStatus::Active | TenantStatus::Provisioning
        )
    }
}

/// Tenant management service
pub struct TenantManager {
    /// All tenants
    tenants: Arc<RwLock<HashMap<TenantId, Tenant>>>,
    /// Tenant name to ID mapping (for name lookups)
    name_index: Arc<RwLock<HashMap<String, TenantId>>>,
}

impl TenantManager {
    /// Creates a new tenant manager
    pub fn new() -> Self {
        Self {
            tenants: Arc::new(RwLock::new(HashMap::new())),
            name_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a new tenant
    pub async fn create_tenant(&self, mut tenant: Tenant) -> Result<TenantId> {
        let mut tenants = self.tenants.write().await;
        let mut name_index = self.name_index.write().await;

        // Check for duplicate ID
        if tenants.contains_key(&tenant.id) {
            return Err(StrataError::AlreadyExists(format!(
                "Tenant {} already exists",
                tenant.id
            )));
        }

        // Check for duplicate name
        if name_index.contains_key(&tenant.name) {
            return Err(StrataError::AlreadyExists(format!(
                "Tenant name '{}' already exists",
                tenant.name
            )));
        }

        let id = tenant.id.clone();
        tenant.status = TenantStatus::Provisioning;

        info!(
            tenant_id = %id,
            name = %tenant.name,
            tier = ?tenant.tier,
            "Creating tenant"
        );

        name_index.insert(tenant.name.clone(), id.clone());
        tenants.insert(id.clone(), tenant);

        Ok(id)
    }

    /// Gets a tenant by ID
    pub async fn get_tenant(&self, id: &TenantId) -> Option<Tenant> {
        self.tenants.read().await.get(id).cloned()
    }

    /// Gets a tenant by name
    pub async fn get_tenant_by_name(&self, name: &str) -> Option<Tenant> {
        let name_index = self.name_index.read().await;
        let id = name_index.get(name)?;
        self.tenants.read().await.get(id).cloned()
    }

    /// Lists all tenants
    pub async fn list_tenants(&self, include_deleted: bool) -> Vec<Tenant> {
        self.tenants
            .read()
            .await
            .values()
            .filter(|t| include_deleted || t.status != TenantStatus::Deleted)
            .cloned()
            .collect()
    }

    /// Updates a tenant
    pub async fn update_tenant(&self, id: &TenantId, update: TenantUpdate) -> Result<Tenant> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get_mut(id)
            .ok_or_else(|| StrataError::NotFound(format!("Tenant {}", id)))?;

        if let Some(name) = update.name {
            // Update name index
            let mut name_index = self.name_index.write().await;
            name_index.remove(&tenant.name);
            name_index.insert(name.clone(), id.clone());
            tenant.name = name;
        }

        if let Some(config) = update.config {
            tenant.config = config;
        }

        if let Some(contact) = update.contact {
            tenant.contact = contact;
        }

        if let Some(tags) = update.tags {
            tenant.tags = tags;
        }

        tenant.updated_at = SystemTime::now();

        debug!(tenant_id = %id, "Updated tenant");

        Ok(tenant.clone())
    }

    /// Activates a provisioned tenant
    pub async fn activate_tenant(&self, id: &TenantId) -> Result<()> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get_mut(id)
            .ok_or_else(|| StrataError::NotFound(format!("Tenant {}", id)))?;

        if tenant.status != TenantStatus::Provisioning {
            return Err(StrataError::InvalidOperation(format!(
                "Cannot activate tenant in {:?} status",
                tenant.status
            )));
        }

        tenant.activate();
        info!(tenant_id = %id, "Activated tenant");

        Ok(())
    }

    /// Suspends a tenant
    pub async fn suspend_tenant(&self, id: &TenantId, reason: &str) -> Result<()> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get_mut(id)
            .ok_or_else(|| StrataError::NotFound(format!("Tenant {}", id)))?;

        if tenant.status == TenantStatus::Deleted {
            return Err(StrataError::InvalidOperation(
                "Cannot suspend deleted tenant".to_string(),
            ));
        }

        tenant.suspend(reason);
        warn!(tenant_id = %id, reason = %reason, "Suspended tenant");

        Ok(())
    }

    /// Unsuspends a tenant
    pub async fn unsuspend_tenant(&self, id: &TenantId) -> Result<()> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get_mut(id)
            .ok_or_else(|| StrataError::NotFound(format!("Tenant {}", id)))?;

        tenant.unsuspend();
        info!(tenant_id = %id, "Unsuspended tenant");

        Ok(())
    }

    /// Marks a tenant for deletion (soft delete)
    pub async fn delete_tenant(&self, id: &TenantId) -> Result<()> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get_mut(id)
            .ok_or_else(|| StrataError::NotFound(format!("Tenant {}", id)))?;

        tenant.status = TenantStatus::Deprovisioning;
        tenant.updated_at = SystemTime::now();

        info!(tenant_id = %id, "Marked tenant for deletion");

        Ok(())
    }

    /// Checks if a tenant exists and is operational
    pub async fn is_tenant_operational(&self, id: &TenantId) -> bool {
        self.tenants
            .read()
            .await
            .get(id)
            .map(|t| t.is_operational())
            .unwrap_or(false)
    }

    /// Gets tenant count by status
    pub async fn get_stats(&self) -> TenantStats {
        let tenants = self.tenants.read().await;
        let mut stats = TenantStats::default();

        for tenant in tenants.values() {
            stats.total += 1;
            match tenant.status {
                TenantStatus::Provisioning => stats.provisioning += 1,
                TenantStatus::Active => stats.active += 1,
                TenantStatus::Suspended => stats.suspended += 1,
                TenantStatus::Deprovisioning => stats.deprovisioning += 1,
                TenantStatus::Deleted => stats.deleted += 1,
            }
        }

        stats
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Tenant update request
#[derive(Debug, Clone, Default)]
pub struct TenantUpdate {
    pub name: Option<String>,
    pub config: Option<TenantConfig>,
    pub contact: Option<TenantContact>,
    pub tags: Option<HashMap<String, String>>,
}

/// Tenant statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TenantStats {
    pub total: u64,
    pub provisioning: u64,
    pub active: u64,
    pub suspended: u64,
    pub deprovisioning: u64,
    pub deleted: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_id() {
        let id = TenantId::new("test");
        assert_eq!(id.as_str(), "test");
        assert!(!id.is_system());

        let system = TenantId::system();
        assert!(system.is_system());
    }

    #[test]
    fn test_tenant_creation() {
        let tenant = Tenant::new("tenant-1", "Test Tenant")
            .with_tier(TenantTier::Standard)
            .with_tag("env", "production");

        assert_eq!(tenant.id.as_str(), "tenant-1");
        assert_eq!(tenant.name, "Test Tenant");
        assert_eq!(tenant.tier, TenantTier::Standard);
        assert_eq!(tenant.status, TenantStatus::Provisioning);
    }

    #[test]
    fn test_tenant_lifecycle() {
        let mut tenant = Tenant::new("tenant-1", "Test");

        assert!(!tenant.is_operational());

        tenant.activate();
        assert!(tenant.is_operational());
        assert!(tenant.can_create_resources());

        tenant.suspend("Testing");
        assert!(!tenant.is_operational());
        assert_eq!(tenant.suspension_reason, Some("Testing".to_string()));

        tenant.unsuspend();
        assert!(tenant.is_operational());
    }

    #[tokio::test]
    async fn test_tenant_manager() {
        let manager = TenantManager::new();

        // Create tenant
        let tenant = Tenant::new("t1", "Tenant 1");
        let id = manager.create_tenant(tenant).await.unwrap();

        // Get tenant
        let retrieved = manager.get_tenant(&id).await.unwrap();
        assert_eq!(retrieved.name, "Tenant 1");

        // Activate
        manager.activate_tenant(&id).await.unwrap();
        assert!(manager.is_tenant_operational(&id).await);

        // Suspend
        manager.suspend_tenant(&id, "Test").await.unwrap();
        assert!(!manager.is_tenant_operational(&id).await);

        // List
        let tenants = manager.list_tenants(false).await;
        assert_eq!(tenants.len(), 1);
    }

    #[tokio::test]
    async fn test_duplicate_tenant() {
        let manager = TenantManager::new();

        let tenant1 = Tenant::new("t1", "Tenant");
        manager.create_tenant(tenant1).await.unwrap();

        // Duplicate ID
        let tenant2 = Tenant::new("t1", "Another");
        assert!(manager.create_tenant(tenant2).await.is_err());

        // Duplicate name
        let tenant3 = Tenant::new("t3", "Tenant");
        assert!(manager.create_tenant(tenant3).await.is_err());
    }

    #[test]
    fn test_tier_configs() {
        let free = TenantConfig::free_tier();
        let enterprise = TenantConfig::enterprise_tier();

        assert!(free.max_storage_bytes < enterprise.max_storage_bytes);
        assert!(!free.worm_enabled);
        assert!(enterprise.worm_enabled);
    }
}
