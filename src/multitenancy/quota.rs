// Resource quota management for tenants

use super::tenant::TenantId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Resource quota configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceQuota {
    /// Maximum storage in bytes
    pub max_storage_bytes: u64,
    /// Maximum number of buckets
    pub max_buckets: u32,
    /// Maximum objects per bucket
    pub max_objects_per_bucket: u64,
    /// Maximum IOPS (read + write)
    pub max_iops: u32,
    /// Maximum read IOPS
    pub max_read_iops: u32,
    /// Maximum write IOPS
    pub max_write_iops: u32,
    /// Maximum bandwidth in bytes/sec
    pub max_bandwidth_bytes: u64,
    /// Maximum read bandwidth
    pub max_read_bandwidth_bytes: u64,
    /// Maximum write bandwidth
    pub max_write_bandwidth_bytes: u64,
    /// Maximum concurrent connections
    pub max_connections: u32,
    /// Maximum request rate (requests/sec)
    pub max_request_rate: u32,
    /// Maximum object size
    pub max_object_size_bytes: u64,
    /// Maximum metadata size per object
    pub max_metadata_size_bytes: u32,
}

impl Default for ResourceQuota {
    fn default() -> Self {
        Self {
            max_storage_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            max_buckets: 100,
            max_objects_per_bucket: 1_000_000,
            max_iops: 10_000,
            max_read_iops: 8_000,
            max_write_iops: 2_000,
            max_bandwidth_bytes: 1024 * 1024 * 1024, // 1 GB/s
            max_read_bandwidth_bytes: 800 * 1024 * 1024,
            max_write_bandwidth_bytes: 200 * 1024 * 1024,
            max_connections: 1000,
            max_request_rate: 10_000,
            max_object_size_bytes: 5 * 1024 * 1024 * 1024, // 5 GB
            max_metadata_size_bytes: 8 * 1024, // 8 KB
        }
    }
}

impl ResourceQuota {
    /// Free tier quota
    pub fn free_tier() -> Self {
        Self {
            max_storage_bytes: 5 * 1024 * 1024 * 1024, // 5 GB
            max_buckets: 3,
            max_objects_per_bucket: 10_000,
            max_iops: 100,
            max_read_iops: 80,
            max_write_iops: 20,
            max_bandwidth_bytes: 10 * 1024 * 1024, // 10 MB/s
            max_read_bandwidth_bytes: 8 * 1024 * 1024,
            max_write_bandwidth_bytes: 2 * 1024 * 1024,
            max_connections: 10,
            max_request_rate: 100,
            max_object_size_bytes: 100 * 1024 * 1024, // 100 MB
            max_metadata_size_bytes: 2 * 1024,
        }
    }

    /// Basic tier quota
    pub fn basic_tier() -> Self {
        Self {
            max_storage_bytes: 100 * 1024 * 1024 * 1024, // 100 GB
            max_buckets: 10,
            max_objects_per_bucket: 100_000,
            max_iops: 1_000,
            max_read_iops: 800,
            max_write_iops: 200,
            max_bandwidth_bytes: 100 * 1024 * 1024, // 100 MB/s
            max_read_bandwidth_bytes: 80 * 1024 * 1024,
            max_write_bandwidth_bytes: 20 * 1024 * 1024,
            max_connections: 100,
            max_request_rate: 1_000,
            max_object_size_bytes: 1024 * 1024 * 1024, // 1 GB
            max_metadata_size_bytes: 4 * 1024,
        }
    }

    /// Professional tier quota
    pub fn professional_tier() -> Self {
        Self {
            max_storage_bytes: 10 * 1024 * 1024 * 1024 * 1024, // 10 TB
            max_buckets: 1000,
            max_objects_per_bucket: 10_000_000,
            max_iops: 100_000,
            max_read_iops: 80_000,
            max_write_iops: 20_000,
            max_bandwidth_bytes: 10 * 1024 * 1024 * 1024, // 10 GB/s
            max_read_bandwidth_bytes: 8 * 1024 * 1024 * 1024,
            max_write_bandwidth_bytes: 2 * 1024 * 1024 * 1024,
            max_connections: 10_000,
            max_request_rate: 100_000,
            max_object_size_bytes: 5 * 1024 * 1024 * 1024, // 5 GB
            max_metadata_size_bytes: 16 * 1024,
        }
    }

    /// Enterprise tier quota (effectively unlimited)
    pub fn enterprise_tier() -> Self {
        Self {
            max_storage_bytes: u64::MAX,
            max_buckets: u32::MAX,
            max_objects_per_bucket: u64::MAX,
            max_iops: u32::MAX,
            max_read_iops: u32::MAX,
            max_write_iops: u32::MAX,
            max_bandwidth_bytes: u64::MAX,
            max_read_bandwidth_bytes: u64::MAX,
            max_write_bandwidth_bytes: u64::MAX,
            max_connections: u32::MAX,
            max_request_rate: u32::MAX,
            max_object_size_bytes: 5 * 1024 * 1024 * 1024 * 1024, // 5 TB
            max_metadata_size_bytes: 64 * 1024,
        }
    }
}

/// Current resource usage
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaUsage {
    /// Current storage used
    pub storage_bytes: u64,
    /// Current bucket count
    pub bucket_count: u32,
    /// Objects per bucket
    pub objects_per_bucket: HashMap<String, u64>,
    /// Current IOPS (sliding window)
    pub current_iops: u32,
    /// Current read IOPS
    pub current_read_iops: u32,
    /// Current write IOPS
    pub current_write_iops: u32,
    /// Current bandwidth usage
    pub current_bandwidth_bytes: u64,
    /// Current read bandwidth
    pub current_read_bandwidth_bytes: u64,
    /// Current write bandwidth
    pub current_write_bandwidth_bytes: u64,
    /// Active connections
    pub active_connections: u32,
    /// Current request rate
    pub current_request_rate: u32,
}

impl QuotaUsage {
    /// Calculate storage utilization percentage
    pub fn storage_utilization(&self, quota: &ResourceQuota) -> f64 {
        if quota.max_storage_bytes == 0 {
            return 0.0;
        }
        (self.storage_bytes as f64 / quota.max_storage_bytes as f64) * 100.0
    }

    /// Calculate IOPS utilization percentage
    pub fn iops_utilization(&self, quota: &ResourceQuota) -> f64 {
        if quota.max_iops == 0 {
            return 0.0;
        }
        (self.current_iops as f64 / quota.max_iops as f64) * 100.0
    }

    /// Calculate bandwidth utilization percentage
    pub fn bandwidth_utilization(&self, quota: &ResourceQuota) -> f64 {
        if quota.max_bandwidth_bytes == 0 {
            return 0.0;
        }
        (self.current_bandwidth_bytes as f64 / quota.max_bandwidth_bytes as f64) * 100.0
    }
}

/// Quota violation type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuotaViolation {
    StorageExceeded { used: u64, limit: u64 },
    BucketLimitExceeded { count: u32, limit: u32 },
    ObjectLimitExceeded { bucket: String, count: u64, limit: u64 },
    IopsExceeded { current: u32, limit: u32 },
    ReadIopsExceeded { current: u32, limit: u32 },
    WriteIopsExceeded { current: u32, limit: u32 },
    BandwidthExceeded { current: u64, limit: u64 },
    ReadBandwidthExceeded { current: u64, limit: u64 },
    WriteBandwidthExceeded { current: u64, limit: u64 },
    ConnectionLimitExceeded { current: u32, limit: u32 },
    RequestRateExceeded { current: u32, limit: u32 },
    ObjectSizeExceeded { size: u64, limit: u64 },
    MetadataSizeExceeded { size: u32, limit: u32 },
}

impl std::fmt::Display for QuotaViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StorageExceeded { used, limit } => {
                write!(f, "Storage quota exceeded: {} / {} bytes", used, limit)
            }
            Self::BucketLimitExceeded { count, limit } => {
                write!(f, "Bucket limit exceeded: {} / {}", count, limit)
            }
            Self::ObjectLimitExceeded { bucket, count, limit } => {
                write!(f, "Object limit exceeded in {}: {} / {}", bucket, count, limit)
            }
            Self::IopsExceeded { current, limit } => {
                write!(f, "IOPS limit exceeded: {} / {}", current, limit)
            }
            Self::ReadIopsExceeded { current, limit } => {
                write!(f, "Read IOPS limit exceeded: {} / {}", current, limit)
            }
            Self::WriteIopsExceeded { current, limit } => {
                write!(f, "Write IOPS limit exceeded: {} / {}", current, limit)
            }
            Self::BandwidthExceeded { current, limit } => {
                write!(f, "Bandwidth limit exceeded: {} / {} bytes/s", current, limit)
            }
            Self::ReadBandwidthExceeded { current, limit } => {
                write!(f, "Read bandwidth limit exceeded: {} / {} bytes/s", current, limit)
            }
            Self::WriteBandwidthExceeded { current, limit } => {
                write!(f, "Write bandwidth limit exceeded: {} / {} bytes/s", current, limit)
            }
            Self::ConnectionLimitExceeded { current, limit } => {
                write!(f, "Connection limit exceeded: {} / {}", current, limit)
            }
            Self::RequestRateExceeded { current, limit } => {
                write!(f, "Request rate limit exceeded: {} / {} req/s", current, limit)
            }
            Self::ObjectSizeExceeded { size, limit } => {
                write!(f, "Object size limit exceeded: {} / {} bytes", size, limit)
            }
            Self::MetadataSizeExceeded { size, limit } => {
                write!(f, "Metadata size limit exceeded: {} / {} bytes", size, limit)
            }
        }
    }
}

/// Enforcement action for quota violations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnforcementAction {
    /// Allow the operation (soft limit)
    Allow,
    /// Warn but allow
    Warn,
    /// Throttle the request
    Throttle,
    /// Reject the request
    Reject,
}

/// Quota enforcement policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnforcementPolicy {
    /// Action at 80% utilization
    pub soft_limit_action: EnforcementAction,
    /// Action at 100% utilization
    pub hard_limit_action: EnforcementAction,
    /// Grace period before enforcement (seconds)
    pub grace_period_secs: u64,
    /// Allow burst above limits
    pub allow_burst: bool,
    /// Burst multiplier (e.g., 1.5 = 50% above limit)
    pub burst_multiplier: f64,
    /// Burst duration (seconds)
    pub burst_duration_secs: u64,
}

impl Default for EnforcementPolicy {
    fn default() -> Self {
        Self {
            soft_limit_action: EnforcementAction::Warn,
            hard_limit_action: EnforcementAction::Reject,
            grace_period_secs: 60,
            allow_burst: true,
            burst_multiplier: 1.2,
            burst_duration_secs: 30,
        }
    }
}

/// Quota check result
#[derive(Debug, Clone)]
pub struct QuotaCheckResult {
    /// Whether the operation is allowed
    pub allowed: bool,
    /// Enforcement action taken
    pub action: EnforcementAction,
    /// Violations detected
    pub violations: Vec<QuotaViolation>,
    /// Warning messages
    pub warnings: Vec<String>,
}

impl QuotaCheckResult {
    fn allowed() -> Self {
        Self {
            allowed: true,
            action: EnforcementAction::Allow,
            violations: Vec::new(),
            warnings: Vec::new(),
        }
    }

    fn rejected(violations: Vec<QuotaViolation>) -> Self {
        Self {
            allowed: false,
            action: EnforcementAction::Reject,
            violations,
            warnings: Vec::new(),
        }
    }

    fn with_warning(mut self, warning: String) -> Self {
        self.warnings.push(warning);
        self
    }
}

/// Quota enforcer for a tenant
pub struct QuotaEnforcer {
    /// Tenant quotas
    quotas: Arc<RwLock<HashMap<TenantId, ResourceQuota>>>,
    /// Current usage
    usage: Arc<RwLock<HashMap<TenantId, QuotaUsage>>>,
    /// Enforcement policies
    policies: Arc<RwLock<HashMap<TenantId, EnforcementPolicy>>>,
    /// Default enforcement policy
    default_policy: EnforcementPolicy,
}

impl QuotaEnforcer {
    /// Creates a new quota enforcer
    pub fn new() -> Self {
        Self {
            quotas: Arc::new(RwLock::new(HashMap::new())),
            usage: Arc::new(RwLock::new(HashMap::new())),
            policies: Arc::new(RwLock::new(HashMap::new())),
            default_policy: EnforcementPolicy::default(),
        }
    }

    /// Sets quota for a tenant
    pub async fn set_quota(&self, tenant_id: TenantId, quota: ResourceQuota) {
        self.quotas.write().await.insert(tenant_id.clone(), quota);
        // Initialize usage if not exists
        self.usage
            .write()
            .await
            .entry(tenant_id)
            .or_insert_with(QuotaUsage::default);
    }

    /// Gets quota for a tenant
    pub async fn get_quota(&self, tenant_id: &TenantId) -> Option<ResourceQuota> {
        self.quotas.read().await.get(tenant_id).cloned()
    }

    /// Gets current usage for a tenant
    pub async fn get_usage(&self, tenant_id: &TenantId) -> Option<QuotaUsage> {
        self.usage.read().await.get(tenant_id).cloned()
    }

    /// Sets enforcement policy for a tenant
    pub async fn set_policy(&self, tenant_id: TenantId, policy: EnforcementPolicy) {
        self.policies.write().await.insert(tenant_id, policy);
    }

    /// Checks if a storage operation is allowed
    pub async fn check_storage(
        &self,
        tenant_id: &TenantId,
        additional_bytes: u64,
    ) -> QuotaCheckResult {
        let quotas = self.quotas.read().await;
        let usage = self.usage.read().await;

        let quota = match quotas.get(tenant_id) {
            Some(q) => q,
            None => return QuotaCheckResult::allowed(),
        };

        let current = usage.get(tenant_id).map(|u| u.storage_bytes).unwrap_or(0);
        let new_total = current.saturating_add(additional_bytes);

        if new_total > quota.max_storage_bytes {
            return QuotaCheckResult::rejected(vec![QuotaViolation::StorageExceeded {
                used: new_total,
                limit: quota.max_storage_bytes,
            }]);
        }

        let mut result = QuotaCheckResult::allowed();
        let utilization = (new_total as f64 / quota.max_storage_bytes as f64) * 100.0;
        if utilization > 80.0 {
            result = result.with_warning(format!(
                "Storage utilization at {:.1}%",
                utilization
            ));
        }

        result
    }

    /// Checks if creating a bucket is allowed
    pub async fn check_bucket_creation(&self, tenant_id: &TenantId) -> QuotaCheckResult {
        let quotas = self.quotas.read().await;
        let usage = self.usage.read().await;

        let quota = match quotas.get(tenant_id) {
            Some(q) => q,
            None => return QuotaCheckResult::allowed(),
        };

        let current = usage.get(tenant_id).map(|u| u.bucket_count).unwrap_or(0);
        let new_count = current + 1;

        if new_count > quota.max_buckets {
            return QuotaCheckResult::rejected(vec![QuotaViolation::BucketLimitExceeded {
                count: new_count,
                limit: quota.max_buckets,
            }]);
        }

        QuotaCheckResult::allowed()
    }

    /// Checks if an object upload is allowed
    pub async fn check_object_upload(
        &self,
        tenant_id: &TenantId,
        bucket: &str,
        object_size: u64,
        metadata_size: u32,
    ) -> QuotaCheckResult {
        let quotas = self.quotas.read().await;
        let usage = self.usage.read().await;

        let quota = match quotas.get(tenant_id) {
            Some(q) => q,
            None => return QuotaCheckResult::allowed(),
        };

        let mut violations = Vec::new();

        // Check object size
        if object_size > quota.max_object_size_bytes {
            violations.push(QuotaViolation::ObjectSizeExceeded {
                size: object_size,
                limit: quota.max_object_size_bytes,
            });
        }

        // Check metadata size
        if metadata_size > quota.max_metadata_size_bytes {
            violations.push(QuotaViolation::MetadataSizeExceeded {
                size: metadata_size,
                limit: quota.max_metadata_size_bytes,
            });
        }

        // Check storage quota
        if let Some(current_usage) = usage.get(tenant_id) {
            let new_storage = current_usage.storage_bytes.saturating_add(object_size);
            if new_storage > quota.max_storage_bytes {
                violations.push(QuotaViolation::StorageExceeded {
                    used: new_storage,
                    limit: quota.max_storage_bytes,
                });
            }

            // Check objects per bucket
            let object_count = current_usage
                .objects_per_bucket
                .get(bucket)
                .copied()
                .unwrap_or(0);
            if object_count + 1 > quota.max_objects_per_bucket {
                violations.push(QuotaViolation::ObjectLimitExceeded {
                    bucket: bucket.to_string(),
                    count: object_count + 1,
                    limit: quota.max_objects_per_bucket,
                });
            }
        }

        if violations.is_empty() {
            QuotaCheckResult::allowed()
        } else {
            QuotaCheckResult::rejected(violations)
        }
    }

    /// Checks IOPS quota
    pub async fn check_iops(&self, tenant_id: &TenantId, is_write: bool) -> QuotaCheckResult {
        let quotas = self.quotas.read().await;
        let usage = self.usage.read().await;

        let quota = match quotas.get(tenant_id) {
            Some(q) => q,
            None => return QuotaCheckResult::allowed(),
        };

        let current_usage = match usage.get(tenant_id) {
            Some(u) => u,
            None => return QuotaCheckResult::allowed(),
        };

        let mut violations = Vec::new();

        // Check total IOPS
        if current_usage.current_iops >= quota.max_iops {
            violations.push(QuotaViolation::IopsExceeded {
                current: current_usage.current_iops,
                limit: quota.max_iops,
            });
        }

        // Check read/write specific IOPS
        if is_write {
            if current_usage.current_write_iops >= quota.max_write_iops {
                violations.push(QuotaViolation::WriteIopsExceeded {
                    current: current_usage.current_write_iops,
                    limit: quota.max_write_iops,
                });
            }
        } else if current_usage.current_read_iops >= quota.max_read_iops {
            violations.push(QuotaViolation::ReadIopsExceeded {
                current: current_usage.current_read_iops,
                limit: quota.max_read_iops,
            });
        }

        if violations.is_empty() {
            QuotaCheckResult::allowed()
        } else {
            QuotaCheckResult::rejected(violations)
        }
    }

    /// Records storage usage change
    pub async fn record_storage_change(&self, tenant_id: &TenantId, delta: i64) {
        let mut usage = self.usage.write().await;
        if let Some(u) = usage.get_mut(tenant_id) {
            if delta >= 0 {
                u.storage_bytes = u.storage_bytes.saturating_add(delta as u64);
            } else {
                u.storage_bytes = u.storage_bytes.saturating_sub((-delta) as u64);
            }
        }
    }

    /// Records bucket creation
    pub async fn record_bucket_created(&self, tenant_id: &TenantId) {
        let mut usage = self.usage.write().await;
        if let Some(u) = usage.get_mut(tenant_id) {
            u.bucket_count = u.bucket_count.saturating_add(1);
        }
    }

    /// Records bucket deletion
    pub async fn record_bucket_deleted(&self, tenant_id: &TenantId, bucket: &str) {
        let mut usage = self.usage.write().await;
        if let Some(u) = usage.get_mut(tenant_id) {
            u.bucket_count = u.bucket_count.saturating_sub(1);
            u.objects_per_bucket.remove(bucket);
        }
    }

    /// Records object creation
    pub async fn record_object_created(&self, tenant_id: &TenantId, bucket: &str, size: u64) {
        let mut usage = self.usage.write().await;
        if let Some(u) = usage.get_mut(tenant_id) {
            u.storage_bytes = u.storage_bytes.saturating_add(size);
            *u.objects_per_bucket.entry(bucket.to_string()).or_insert(0) += 1;
        }
    }

    /// Records object deletion
    pub async fn record_object_deleted(&self, tenant_id: &TenantId, bucket: &str, size: u64) {
        let mut usage = self.usage.write().await;
        if let Some(u) = usage.get_mut(tenant_id) {
            u.storage_bytes = u.storage_bytes.saturating_sub(size);
            if let Some(count) = u.objects_per_bucket.get_mut(bucket) {
                *count = count.saturating_sub(1);
            }
        }
    }

    /// Initializes usage tracking for a tenant
    pub async fn initialize_tenant(&self, tenant_id: TenantId, quota: ResourceQuota) {
        self.quotas.write().await.insert(tenant_id.clone(), quota);
        self.usage
            .write()
            .await
            .insert(tenant_id.clone(), QuotaUsage::default());
        self.policies
            .write()
            .await
            .insert(tenant_id, EnforcementPolicy::default());
    }

    /// Removes a tenant from quota tracking
    pub async fn remove_tenant(&self, tenant_id: &TenantId) {
        self.quotas.write().await.remove(tenant_id);
        self.usage.write().await.remove(tenant_id);
        self.policies.write().await.remove(tenant_id);
    }
}

impl Default for QuotaEnforcer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_quota_enforcement() {
        let enforcer = QuotaEnforcer::new();
        let tenant_id = TenantId::new("test-tenant");

        // Set a small quota for testing
        let quota = ResourceQuota {
            max_storage_bytes: 1000,
            max_buckets: 5,
            max_objects_per_bucket: 10,
            ..Default::default()
        };

        enforcer.initialize_tenant(tenant_id.clone(), quota).await;

        // Should allow small upload
        let result = enforcer.check_storage(&tenant_id, 500).await;
        assert!(result.allowed);

        // Should reject upload exceeding quota
        let result = enforcer.check_storage(&tenant_id, 1500).await;
        assert!(!result.allowed);
    }

    #[tokio::test]
    async fn test_bucket_limit() {
        let enforcer = QuotaEnforcer::new();
        let tenant_id = TenantId::new("test-tenant");

        let quota = ResourceQuota {
            max_buckets: 2,
            ..Default::default()
        };

        enforcer.initialize_tenant(tenant_id.clone(), quota).await;

        // First two buckets should be allowed
        assert!(enforcer.check_bucket_creation(&tenant_id).await.allowed);
        enforcer.record_bucket_created(&tenant_id).await;

        assert!(enforcer.check_bucket_creation(&tenant_id).await.allowed);
        enforcer.record_bucket_created(&tenant_id).await;

        // Third bucket should be rejected
        assert!(!enforcer.check_bucket_creation(&tenant_id).await.allowed);
    }

    #[tokio::test]
    async fn test_usage_tracking() {
        let enforcer = QuotaEnforcer::new();
        let tenant_id = TenantId::new("test-tenant");

        enforcer
            .initialize_tenant(tenant_id.clone(), ResourceQuota::default())
            .await;

        // Record some usage
        enforcer.record_bucket_created(&tenant_id).await;
        enforcer
            .record_object_created(&tenant_id, "bucket1", 1000)
            .await;
        enforcer
            .record_object_created(&tenant_id, "bucket1", 2000)
            .await;

        let usage = enforcer.get_usage(&tenant_id).await.unwrap();
        assert_eq!(usage.bucket_count, 1);
        assert_eq!(usage.storage_bytes, 3000);
        assert_eq!(usage.objects_per_bucket.get("bucket1"), Some(&2));
    }

    #[test]
    fn test_quota_tiers() {
        let free = ResourceQuota::free_tier();
        let basic = ResourceQuota::basic_tier();
        let pro = ResourceQuota::professional_tier();
        let enterprise = ResourceQuota::enterprise_tier();

        // Verify tier progression
        assert!(free.max_storage_bytes < basic.max_storage_bytes);
        assert!(basic.max_storage_bytes < pro.max_storage_bytes);
        assert!(pro.max_storage_bytes < enterprise.max_storage_bytes);

        assert!(free.max_iops < basic.max_iops);
        assert!(basic.max_iops < pro.max_iops);
    }
}
