//! Quota management and enforcement for Strata.
//!
//! Provides per-user, per-path, and per-tenant quota enforcement.

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Quota configuration for a resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaConfig {
    /// Maximum bytes allowed.
    pub max_bytes: Option<u64>,
    /// Maximum number of files/objects.
    pub max_files: Option<u64>,
    /// Maximum number of inodes.
    pub max_inodes: Option<u64>,
    /// Soft limit (warning threshold) as percentage of max.
    pub soft_limit_percent: u8,
    /// Enable quota enforcement.
    pub enabled: bool,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            max_bytes: None,
            max_files: None,
            max_inodes: None,
            soft_limit_percent: 80,
            enabled: true,
        }
    }
}

impl QuotaConfig {
    /// Create a quota with byte limit.
    pub fn bytes(limit: u64) -> Self {
        Self {
            max_bytes: Some(limit),
            enabled: true,
            ..Default::default()
        }
    }

    /// Create a quota with file limit.
    pub fn files(limit: u64) -> Self {
        Self {
            max_files: Some(limit),
            enabled: true,
            ..Default::default()
        }
    }

    /// Create quota with both limits.
    pub fn full(max_bytes: u64, max_files: u64) -> Self {
        Self {
            max_bytes: Some(max_bytes),
            max_files: Some(max_files),
            enabled: true,
            ..Default::default()
        }
    }

    /// 1 GB quota.
    pub fn gb_1() -> Self {
        Self::bytes(1024 * 1024 * 1024)
    }

    /// 10 GB quota.
    pub fn gb_10() -> Self {
        Self::bytes(10 * 1024 * 1024 * 1024)
    }

    /// 100 GB quota.
    pub fn gb_100() -> Self {
        Self::bytes(100 * 1024 * 1024 * 1024)
    }
}

/// Current usage for a quota.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuotaUsage {
    /// Current bytes used.
    pub bytes_used: u64,
    /// Current number of files.
    pub files_used: u64,
    /// Current number of inodes.
    pub inodes_used: u64,
}

impl QuotaUsage {
    /// Add usage.
    pub fn add(&mut self, bytes: u64, files: u64, inodes: u64) {
        self.bytes_used = self.bytes_used.saturating_add(bytes);
        self.files_used = self.files_used.saturating_add(files);
        self.inodes_used = self.inodes_used.saturating_add(inodes);
    }

    /// Subtract usage.
    pub fn subtract(&mut self, bytes: u64, files: u64, inodes: u64) {
        self.bytes_used = self.bytes_used.saturating_sub(bytes);
        self.files_used = self.files_used.saturating_sub(files);
        self.inodes_used = self.inodes_used.saturating_sub(inodes);
    }
}

/// Result of a quota check.
#[derive(Debug, Clone)]
pub enum QuotaCheckResult {
    /// Operation allowed.
    Allowed,
    /// Operation allowed but approaching limit.
    Warning {
        /// Resource type approaching limit.
        resource: QuotaResource,
        /// Current usage percentage.
        usage_percent: u8,
    },
    /// Operation denied due to quota.
    Denied {
        /// Resource that exceeded quota.
        resource: QuotaResource,
        /// Current usage.
        current: u64,
        /// Maximum allowed.
        limit: u64,
    },
}

impl QuotaCheckResult {
    /// Check if the operation is allowed.
    pub fn is_allowed(&self) -> bool {
        !matches!(self, QuotaCheckResult::Denied { .. })
    }

    /// Convert to Result.
    pub fn into_result<T>(self, value: T, path: &str) -> Result<T> {
        match self {
            QuotaCheckResult::Allowed | QuotaCheckResult::Warning { .. } => Ok(value),
            QuotaCheckResult::Denied { .. } => {
                Err(StrataError::QuotaExceeded(path.to_string()))
            }
        }
    }
}

/// Type of quota resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaResource {
    /// Byte storage.
    Bytes,
    /// File count.
    Files,
    /// Inode count.
    Inodes,
}

/// Quota entry for a specific scope.
#[derive(Debug, Clone)]
pub struct QuotaEntry {
    /// Quota configuration.
    pub config: QuotaConfig,
    /// Current usage.
    pub usage: QuotaUsage,
}

impl QuotaEntry {
    /// Create a new quota entry.
    pub fn new(config: QuotaConfig) -> Self {
        Self {
            config,
            usage: QuotaUsage::default(),
        }
    }

    /// Check if an operation is allowed.
    pub fn check(&self, additional_bytes: u64, additional_files: u64) -> QuotaCheckResult {
        if !self.config.enabled {
            return QuotaCheckResult::Allowed;
        }

        // Check byte limit
        if let Some(max_bytes) = self.config.max_bytes {
            let new_usage = self.usage.bytes_used.saturating_add(additional_bytes);
            if new_usage > max_bytes {
                return QuotaCheckResult::Denied {
                    resource: QuotaResource::Bytes,
                    current: self.usage.bytes_used,
                    limit: max_bytes,
                };
            }

            let usage_percent = ((new_usage as f64 / max_bytes as f64) * 100.0) as u8;
            if usage_percent >= self.config.soft_limit_percent {
                return QuotaCheckResult::Warning {
                    resource: QuotaResource::Bytes,
                    usage_percent,
                };
            }
        }

        // Check file limit
        if let Some(max_files) = self.config.max_files {
            let new_usage = self.usage.files_used.saturating_add(additional_files);
            if new_usage > max_files {
                return QuotaCheckResult::Denied {
                    resource: QuotaResource::Files,
                    current: self.usage.files_used,
                    limit: max_files,
                };
            }

            let usage_percent = ((new_usage as f64 / max_files as f64) * 100.0) as u8;
            if usage_percent >= self.config.soft_limit_percent {
                return QuotaCheckResult::Warning {
                    resource: QuotaResource::Files,
                    usage_percent,
                };
            }
        }

        QuotaCheckResult::Allowed
    }

    /// Get usage statistics.
    pub fn get_stats(&self) -> QuotaStats {
        QuotaStats {
            bytes_used: self.usage.bytes_used,
            bytes_limit: self.config.max_bytes,
            bytes_percent: self.config.max_bytes.map(|limit| {
                ((self.usage.bytes_used as f64 / limit as f64) * 100.0) as u8
            }),
            files_used: self.usage.files_used,
            files_limit: self.config.max_files,
            files_percent: self.config.max_files.map(|limit| {
                ((self.usage.files_used as f64 / limit as f64) * 100.0) as u8
            }),
        }
    }
}

/// Quota statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaStats {
    /// Bytes used.
    pub bytes_used: u64,
    /// Bytes limit (if set).
    pub bytes_limit: Option<u64>,
    /// Bytes usage percentage (if limit set).
    pub bytes_percent: Option<u8>,
    /// Files used.
    pub files_used: u64,
    /// Files limit (if set).
    pub files_limit: Option<u64>,
    /// Files usage percentage (if limit set).
    pub files_percent: Option<u8>,
}

/// Quota scope identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QuotaScope {
    /// Global quota for the entire system.
    Global,
    /// Per-user quota.
    User(u32),
    /// Per-group quota.
    Group(u32),
    /// Per-path quota (directory tree).
    Path(String),
    /// Per-tenant quota (for multi-tenant deployments).
    Tenant(String),
}

impl QuotaScope {
    /// Create a user scope.
    pub fn user(uid: u32) -> Self {
        QuotaScope::User(uid)
    }

    /// Create a path scope.
    pub fn path(path: impl Into<String>) -> Self {
        QuotaScope::Path(path.into())
    }

    /// Create a tenant scope.
    pub fn tenant(tenant: impl Into<String>) -> Self {
        QuotaScope::Tenant(tenant.into())
    }
}

/// Quota manager for enforcing quotas across scopes.
pub struct QuotaManager {
    /// Quotas by scope.
    quotas: RwLock<HashMap<QuotaScope, QuotaEntry>>,
    /// Default quota for new users.
    default_user_quota: Option<QuotaConfig>,
    /// Global quota enabled.
    #[allow(dead_code)]
    global_enabled: bool,
}

impl QuotaManager {
    /// Create a new quota manager.
    pub fn new() -> Self {
        Self {
            quotas: RwLock::new(HashMap::new()),
            default_user_quota: None,
            global_enabled: true,
        }
    }

    /// Set default quota for new users.
    pub fn with_default_user_quota(mut self, config: QuotaConfig) -> Self {
        self.default_user_quota = Some(config);
        self
    }

    /// Set a quota for a scope.
    pub async fn set_quota(&self, scope: QuotaScope, config: QuotaConfig) {
        let mut quotas = self.quotas.write().await;
        quotas.insert(scope, QuotaEntry::new(config));
    }

    /// Remove a quota for a scope.
    pub async fn remove_quota(&self, scope: &QuotaScope) -> bool {
        let mut quotas = self.quotas.write().await;
        quotas.remove(scope).is_some()
    }

    /// Get quota for a scope.
    pub async fn get_quota(&self, scope: &QuotaScope) -> Option<QuotaEntry> {
        let quotas = self.quotas.read().await;
        quotas.get(scope).cloned()
    }

    /// Check if an operation is allowed.
    pub async fn check(
        &self,
        scopes: &[QuotaScope],
        additional_bytes: u64,
        additional_files: u64,
    ) -> QuotaCheckResult {
        let quotas = self.quotas.read().await;

        for scope in scopes {
            if let Some(entry) = quotas.get(scope) {
                let result = entry.check(additional_bytes, additional_files);
                if !result.is_allowed() {
                    return result;
                }
            }
        }

        QuotaCheckResult::Allowed
    }

    /// Check and update usage if allowed.
    pub async fn check_and_update(
        &self,
        scopes: &[QuotaScope],
        bytes: u64,
        files: u64,
    ) -> QuotaCheckResult {
        // First check all quotas
        let result = self.check(scopes, bytes, files).await;

        // If allowed, update usage
        if result.is_allowed() {
            let mut quotas = self.quotas.write().await;
            for scope in scopes {
                if let Some(entry) = quotas.get_mut(scope) {
                    entry.usage.add(bytes, files, 0);
                }
            }
        }

        result
    }

    /// Release usage (e.g., after file deletion).
    pub async fn release(&self, scopes: &[QuotaScope], bytes: u64, files: u64) {
        let mut quotas = self.quotas.write().await;
        for scope in scopes {
            if let Some(entry) = quotas.get_mut(scope) {
                entry.usage.subtract(bytes, files, 0);
            }
        }
    }

    /// Get stats for a scope.
    pub async fn get_stats(&self, scope: &QuotaScope) -> Option<QuotaStats> {
        let quotas = self.quotas.read().await;
        quotas.get(scope).map(|e| e.get_stats())
    }

    /// List all quota scopes.
    pub async fn list_scopes(&self) -> Vec<QuotaScope> {
        let quotas = self.quotas.read().await;
        quotas.keys().cloned().collect()
    }

    /// Ensure user has a quota (creates default if needed).
    pub async fn ensure_user_quota(&self, uid: u32) {
        let scope = QuotaScope::User(uid);
        let quotas = self.quotas.read().await;

        if quotas.contains_key(&scope) {
            return;
        }
        drop(quotas);

        if let Some(config) = &self.default_user_quota {
            self.set_quota(scope, config.clone()).await;
        }
    }
}

impl Default for QuotaManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper for checking quotas before operations.
pub struct QuotaChecker {
    manager: Arc<QuotaManager>,
}

impl QuotaChecker {
    /// Create a new quota checker.
    pub fn new(manager: Arc<QuotaManager>) -> Self {
        Self { manager }
    }

    /// Check quota before a write operation.
    pub async fn check_write(
        &self,
        uid: u32,
        path: &str,
        size: u64,
    ) -> Result<()> {
        let scopes = vec![
            QuotaScope::Global,
            QuotaScope::User(uid),
            QuotaScope::Path(path.to_string()),
        ];

        self.manager
            .check(&scopes, size, 0)
            .await
            .into_result((), path)
    }

    /// Check quota before creating a file.
    pub async fn check_create(
        &self,
        uid: u32,
        path: &str,
    ) -> Result<()> {
        let scopes = vec![
            QuotaScope::Global,
            QuotaScope::User(uid),
            QuotaScope::Path(path.to_string()),
        ];

        self.manager
            .check(&scopes, 0, 1)
            .await
            .into_result((), path)
    }

    /// Record usage after successful operation.
    pub async fn record_usage(
        &self,
        uid: u32,
        path: &str,
        bytes: u64,
        files: u64,
    ) {
        let scopes = vec![
            QuotaScope::Global,
            QuotaScope::User(uid),
            QuotaScope::Path(path.to_string()),
        ];

        let mut quotas = self.manager.quotas.write().await;
        for scope in &scopes {
            if let Some(entry) = quotas.get_mut(scope) {
                entry.usage.add(bytes, files, 0);
            }
        }
    }

    /// Release usage after deletion.
    pub async fn release_usage(
        &self,
        uid: u32,
        path: &str,
        bytes: u64,
        files: u64,
    ) {
        let scopes = vec![
            QuotaScope::Global,
            QuotaScope::User(uid),
            QuotaScope::Path(path.to_string()),
        ];

        self.manager.release(&scopes, bytes, files).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_config_presets() {
        let gb1 = QuotaConfig::gb_1();
        assert_eq!(gb1.max_bytes, Some(1024 * 1024 * 1024));

        let full = QuotaConfig::full(1000, 100);
        assert_eq!(full.max_bytes, Some(1000));
        assert_eq!(full.max_files, Some(100));
    }

    #[test]
    fn test_quota_usage() {
        let mut usage = QuotaUsage::default();

        usage.add(100, 1, 0);
        assert_eq!(usage.bytes_used, 100);
        assert_eq!(usage.files_used, 1);

        usage.subtract(50, 1, 0);
        assert_eq!(usage.bytes_used, 50);
        assert_eq!(usage.files_used, 0);

        // Test saturation
        usage.subtract(100, 0, 0);
        assert_eq!(usage.bytes_used, 0);
    }

    #[test]
    fn test_quota_entry_check_allowed() {
        let config = QuotaConfig::bytes(1000);
        let entry = QuotaEntry::new(config);

        let result = entry.check(500, 0);
        assert!(result.is_allowed());
    }

    #[test]
    fn test_quota_entry_check_denied() {
        let config = QuotaConfig::bytes(1000);
        let mut entry = QuotaEntry::new(config);
        entry.usage.bytes_used = 900;

        let result = entry.check(200, 0);
        assert!(!result.is_allowed());
        assert!(matches!(result, QuotaCheckResult::Denied { .. }));
    }

    #[test]
    fn test_quota_entry_check_warning() {
        let config = QuotaConfig {
            max_bytes: Some(1000),
            soft_limit_percent: 80,
            enabled: true,
            ..Default::default()
        };
        let mut entry = QuotaEntry::new(config);
        entry.usage.bytes_used = 700;

        let result = entry.check(150, 0);
        assert!(result.is_allowed());
        assert!(matches!(result, QuotaCheckResult::Warning { .. }));
    }

    #[test]
    fn test_quota_entry_disabled() {
        let config = QuotaConfig {
            max_bytes: Some(100),
            enabled: false,
            ..Default::default()
        };
        let entry = QuotaEntry::new(config);

        let result = entry.check(1000, 0);
        assert!(result.is_allowed());
    }

    #[tokio::test]
    async fn test_quota_manager_set_get() {
        let manager = QuotaManager::new();
        let scope = QuotaScope::User(1000);

        manager.set_quota(scope.clone(), QuotaConfig::gb_1()).await;

        let quota = manager.get_quota(&scope).await;
        assert!(quota.is_some());
    }

    #[tokio::test]
    async fn test_quota_manager_check() {
        let manager = QuotaManager::new();
        let scope = QuotaScope::User(1000);

        manager.set_quota(scope.clone(), QuotaConfig::bytes(1000)).await;

        let result = manager.check(&[scope.clone()], 500, 0).await;
        assert!(result.is_allowed());

        let result = manager.check(&[scope], 1500, 0).await;
        assert!(!result.is_allowed());
    }

    #[tokio::test]
    async fn test_quota_manager_check_and_update() {
        let manager = QuotaManager::new();
        let scope = QuotaScope::User(1000);

        manager.set_quota(scope.clone(), QuotaConfig::bytes(1000)).await;

        let result = manager.check_and_update(&[scope.clone()], 500, 0).await;
        assert!(result.is_allowed());

        let stats = manager.get_stats(&scope).await.unwrap();
        assert_eq!(stats.bytes_used, 500);
    }

    #[tokio::test]
    async fn test_quota_manager_release() {
        let manager = QuotaManager::new();
        let scope = QuotaScope::User(1000);

        manager.set_quota(scope.clone(), QuotaConfig::bytes(1000)).await;
        manager.check_and_update(&[scope.clone()], 500, 1).await;

        manager.release(&[scope.clone()], 200, 1).await;

        let stats = manager.get_stats(&scope).await.unwrap();
        assert_eq!(stats.bytes_used, 300);
        assert_eq!(stats.files_used, 0);
    }

    #[tokio::test]
    async fn test_quota_manager_multiple_scopes() {
        let manager = QuotaManager::new();

        manager.set_quota(QuotaScope::Global, QuotaConfig::bytes(10000)).await;
        manager.set_quota(QuotaScope::User(1000), QuotaConfig::bytes(1000)).await;

        // Allowed by both
        let result = manager
            .check(&[QuotaScope::Global, QuotaScope::User(1000)], 500, 0)
            .await;
        assert!(result.is_allowed());

        // Denied by user quota
        let result = manager
            .check(&[QuotaScope::Global, QuotaScope::User(1000)], 1500, 0)
            .await;
        assert!(!result.is_allowed());
    }

    #[tokio::test]
    async fn test_quota_checker() {
        let manager = Arc::new(QuotaManager::new());
        manager.set_quota(QuotaScope::User(1000), QuotaConfig::bytes(1000)).await;

        let checker = QuotaChecker::new(manager);

        assert!(checker.check_write(1000, "/test", 500).await.is_ok());
        assert!(checker.check_write(1000, "/test", 1500).await.is_err());
    }

    #[test]
    fn test_quota_scope() {
        let user = QuotaScope::user(1000);
        assert!(matches!(user, QuotaScope::User(1000)));

        let path = QuotaScope::path("/data");
        assert!(matches!(path, QuotaScope::Path(_)));

        let tenant = QuotaScope::tenant("acme");
        assert!(matches!(tenant, QuotaScope::Tenant(_)));
    }

    #[tokio::test]
    async fn test_ensure_user_quota() {
        let manager = QuotaManager::new()
            .with_default_user_quota(QuotaConfig::gb_1());

        manager.ensure_user_quota(1000).await;

        let quota = manager.get_quota(&QuotaScope::User(1000)).await;
        assert!(quota.is_some());
    }

    #[test]
    fn test_quota_stats() {
        let config = QuotaConfig::full(1000, 100);
        let mut entry = QuotaEntry::new(config);
        entry.usage.bytes_used = 500;
        entry.usage.files_used = 50;

        let stats = entry.get_stats();
        assert_eq!(stats.bytes_used, 500);
        assert_eq!(stats.bytes_limit, Some(1000));
        assert_eq!(stats.bytes_percent, Some(50));
        assert_eq!(stats.files_used, 50);
        assert_eq!(stats.files_percent, Some(50));
    }
}
