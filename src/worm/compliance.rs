// Compliance management for WORM storage

use crate::error::{Result, StrataError};
use super::retention::{RetentionPolicy, RetentionMode, RetentionPeriod, ObjectRetention, BucketRetentionConfig};
use super::legal_hold::LegalHoldManager;
use super::audit::WormAuditLog;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::info;

/// Compliance standard
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ComplianceStandard {
    /// SEC Rule 17a-4 (broker-dealer records)
    Sec17a4,
    /// FINRA (financial industry)
    Finra,
    /// HIPAA (healthcare)
    Hipaa,
    /// SOX (Sarbanes-Oxley)
    Sox,
    /// GDPR (European data protection)
    Gdpr,
    /// PCI DSS (payment card industry)
    PciDss,
    /// FedRAMP (federal government)
    FedRamp,
    /// Custom
    Custom,
}

/// Compliance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceConfig {
    /// Enabled compliance standards
    pub standards: Vec<ComplianceStandard>,
    /// Default retention mode
    pub default_mode: RetentionMode,
    /// Require retention on all objects
    pub require_retention: bool,
    /// Allow extending retention
    pub allow_extend: bool,
    /// Allow shortening retention (governance mode only)
    pub allow_shorten: bool,
    /// Enable audit logging
    pub audit_enabled: bool,
    /// Audit log retention period
    pub audit_retention_days: u32,
    /// Clock skew tolerance for timestamps
    pub clock_skew_tolerance_secs: u32,
    /// Require legal hold approval for deletion
    pub require_hold_approval: bool,
}

impl Default for ComplianceConfig {
    fn default() -> Self {
        Self {
            standards: Vec::new(),
            default_mode: RetentionMode::Governance,
            require_retention: false,
            allow_extend: true,
            allow_shorten: false,
            audit_enabled: true,
            audit_retention_days: 365 * 7, // 7 years
            clock_skew_tolerance_secs: 300, // 5 minutes
            require_hold_approval: false,
        }
    }
}

impl ComplianceConfig {
    /// Creates SEC 17a-4 compliant configuration
    pub fn sec_17a4() -> Self {
        Self {
            standards: vec![ComplianceStandard::Sec17a4],
            default_mode: RetentionMode::Compliance,
            require_retention: true,
            allow_extend: true,
            allow_shorten: false,
            audit_enabled: true,
            audit_retention_days: 365 * 6 + 1, // 6 years + 1 day
            clock_skew_tolerance_secs: 60,
            require_hold_approval: true,
        }
    }

    /// Creates HIPAA compliant configuration
    pub fn hipaa() -> Self {
        Self {
            standards: vec![ComplianceStandard::Hipaa],
            default_mode: RetentionMode::Compliance,
            require_retention: true,
            allow_extend: true,
            allow_shorten: false,
            audit_enabled: true,
            audit_retention_days: 365 * 6 + 1,
            clock_skew_tolerance_secs: 60,
            require_hold_approval: true,
        }
    }
}

/// Access type for compliance checks
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessType {
    Read,
    Write,
    Delete,
    Modify,
    ListVersions,
    GetRetention,
    SetRetention,
    ExtendRetention,
    PlaceLegalHold,
    ReleaseLegalHold,
}

/// Access check result
#[derive(Debug, Clone)]
pub struct AccessCheckResult {
    pub allowed: bool,
    pub reason: Option<String>,
    pub requires_audit: bool,
    pub warnings: Vec<String>,
}

impl AccessCheckResult {
    pub fn allowed() -> Self {
        Self {
            allowed: true,
            reason: None,
            requires_audit: false,
            warnings: Vec::new(),
        }
    }

    pub fn denied(reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            reason: Some(reason.into()),
            requires_audit: true,
            warnings: Vec::new(),
        }
    }

    pub fn with_audit(mut self) -> Self {
        self.requires_audit = true;
        self
    }

    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }
}

/// Compliance manager
pub struct ComplianceManager {
    config: ComplianceConfig,
    policies: Arc<RwLock<HashMap<String, RetentionPolicy>>>,
    bucket_configs: Arc<RwLock<HashMap<String, BucketRetentionConfig>>>,
    object_retentions: Arc<RwLock<HashMap<String, ObjectRetention>>>,
    legal_holds: Arc<LegalHoldManager>,
    audit_log: Arc<WormAuditLog>,
}

impl ComplianceManager {
    /// Creates a new compliance manager
    pub fn new(config: ComplianceConfig) -> Self {
        Self {
            config,
            policies: Arc::new(RwLock::new(HashMap::new())),
            bucket_configs: Arc::new(RwLock::new(HashMap::new())),
            object_retentions: Arc::new(RwLock::new(HashMap::new())),
            legal_holds: Arc::new(LegalHoldManager::new()),
            audit_log: Arc::new(WormAuditLog::new()),
        }
    }

    /// Gets the legal hold manager
    pub fn legal_holds(&self) -> Arc<LegalHoldManager> {
        Arc::clone(&self.legal_holds)
    }

    /// Gets the audit log
    pub fn audit_log(&self) -> Arc<WormAuditLog> {
        Arc::clone(&self.audit_log)
    }

    /// Registers a retention policy
    pub async fn register_policy(&self, policy: RetentionPolicy) -> Result<()> {
        let mut policies = self.policies.write().await;

        info!(policy_id = %policy.id, name = %policy.name, "Registered retention policy");
        policies.insert(policy.id.clone(), policy);

        Ok(())
    }

    /// Gets a policy by ID
    pub async fn get_policy(&self, policy_id: &str) -> Option<RetentionPolicy> {
        let policies = self.policies.read().await;
        policies.get(policy_id).cloned()
    }

    /// Configures WORM for a bucket
    pub async fn configure_bucket(&self, config: BucketRetentionConfig) -> Result<()> {
        let mut configs = self.bucket_configs.write().await;

        // If bucket already configured and locked, cannot reconfigure
        if let Some(existing) = configs.get(&config.bucket) {
            if existing.locked && existing.enabled {
                return Err(StrataError::InvalidOperation(
                    "Cannot modify locked WORM bucket configuration".to_string()
                ));
            }
        }

        info!(
            bucket = %config.bucket,
            enabled = config.enabled,
            mode = ?config.default_mode,
            "Configured bucket WORM settings"
        );

        configs.insert(config.bucket.clone(), config);
        Ok(())
    }

    /// Gets bucket configuration
    pub async fn get_bucket_config(&self, bucket: &str) -> Option<BucketRetentionConfig> {
        let configs = self.bucket_configs.read().await;
        configs.get(bucket).cloned()
    }

    /// Sets object retention
    pub async fn set_object_retention(
        &self,
        bucket: &str,
        object_key: &str,
        mode: RetentionMode,
        until: Option<SystemTime>,
        user: &str,
    ) -> Result<()> {
        let key = format!("{}/{}", bucket, object_key);

        // Check if bucket allows this
        let bucket_config = {
            let configs = self.bucket_configs.read().await;
            configs.get(bucket).cloned()
        };

        // Validate against bucket config
        if let Some(bc) = &bucket_config {
            if !bc.enabled {
                return Err(StrataError::InvalidOperation(
                    "WORM is not enabled for this bucket".to_string()
                ));
            }

            // Cannot set governance mode on compliance bucket
            if bc.default_mode == RetentionMode::Compliance && mode == RetentionMode::Governance {
                return Err(StrataError::InvalidOperation(
                    "Bucket requires compliance mode retention".to_string()
                ));
            }
        }

        // Check existing retention
        let mut retentions = self.object_retentions.write().await;
        if let Some(existing) = retentions.get(&key) {
            // Cannot downgrade from compliance to governance
            if existing.mode == RetentionMode::Compliance && mode == RetentionMode::Governance {
                return Err(StrataError::InvalidOperation(
                    "Cannot downgrade from compliance to governance mode".to_string()
                ));
            }

            // Cannot shorten retention in compliance mode
            if existing.mode == RetentionMode::Compliance {
                if let (Some(new_until), Some(existing_until)) = (until, existing.retain_until) {
                    if new_until < existing_until {
                        return Err(StrataError::InvalidOperation(
                            "Cannot shorten retention in compliance mode".to_string()
                        ));
                    }
                }
            }
        }

        let retention = ObjectRetention::new(bucket, object_key, mode, until);

        // Audit the change
        self.audit_log.log_retention_change(
            bucket,
            object_key,
            user,
            mode,
            until,
        ).await;

        info!(
            bucket = %bucket,
            object_key = %object_key,
            mode = ?mode,
            until = ?until,
            user = %user,
            "Set object retention"
        );

        retentions.insert(key, retention);
        Ok(())
    }

    /// Gets object retention
    pub async fn get_object_retention(&self, bucket: &str, object_key: &str) -> Option<ObjectRetention> {
        let key = format!("{}/{}", bucket, object_key);
        let retentions = self.object_retentions.read().await;
        retentions.get(&key).cloned()
    }

    /// Checks if an access is allowed
    pub async fn check_access(
        &self,
        bucket: &str,
        object_key: &str,
        access_type: AccessType,
        #[allow(unused_variables)] user: &str,
        is_privileged: bool,
    ) -> AccessCheckResult {
        let key = format!("{}/{}", bucket, object_key);

        // Check legal hold first
        if self.legal_holds.is_object_held(bucket, object_key).await {
            match access_type {
                AccessType::Delete | AccessType::Modify => {
                    return AccessCheckResult::denied("Object is under legal hold");
                }
                _ => {}
            }
        }

        // Check retention
        let retention = {
            let retentions = self.object_retentions.read().await;
            retentions.get(&key).cloned()
        };

        if let Some(ret) = retention {
            match access_type {
                AccessType::Delete => {
                    if !ret.can_delete(is_privileged) {
                        return AccessCheckResult::denied(format!(
                            "Object under {} retention until {:?}",
                            if ret.mode == RetentionMode::Compliance { "compliance" } else { "governance" },
                            ret.retain_until
                        ));
                    }

                    // Governance mode bypass warning
                    if ret.mode == RetentionMode::Governance && is_privileged && !ret.is_expired() {
                        return AccessCheckResult::allowed()
                            .with_audit()
                            .with_warning("Privileged delete bypassing governance retention");
                    }
                }
                AccessType::Modify => {
                    return AccessCheckResult::denied(
                        "WORM objects cannot be modified in place"
                    );
                }
                AccessType::SetRetention | AccessType::ExtendRetention => {
                    // Already handled above
                }
                _ => {}
            }
        }

        // Read access is always allowed
        AccessCheckResult::allowed()
    }

    /// Validates write before allowing
    pub async fn validate_write(
        &self,
        bucket: &str,
        object_key: &str,
        retention_period: Option<RetentionPeriod>,
        _user: &str,
    ) -> Result<()> {
        // Check bucket config
        let bucket_config = {
            let configs = self.bucket_configs.read().await;
            configs.get(bucket).cloned()
        };

        if let Some(bc) = bucket_config {
            // If bucket requires retention, validate
            if bc.force_retention && retention_period.is_none() {
                return Err(StrataError::InvalidOperation(
                    "Bucket requires retention period on all objects".to_string()
                ));
            }

            // If policy specified, validate period
            if let (Some(policy_id), Some(period)) = (&bc.policy_id, &retention_period) {
                let policies = self.policies.read().await;
                if let Some(policy) = policies.get(policy_id) {
                    policy.validate_period(period)?;
                }
            }
        }

        // Check if object already exists with retention
        let key = format!("{}/{}", bucket, object_key);
        let retentions = self.object_retentions.read().await;

        if let Some(existing) = retentions.get(&key) {
            if !existing.is_expired() {
                return Err(StrataError::InvalidOperation(
                    "Cannot overwrite object with active retention".to_string()
                ));
            }
        }

        Ok(())
    }

    /// Generates compliance report
    pub async fn generate_report(&self) -> ComplianceReport {
        let bucket_configs = self.bucket_configs.read().await;
        let retentions = self.object_retentions.read().await;
        let hold_stats = self.legal_holds.get_stats().await;
        let policies = self.policies.read().await;

        let mut objects_by_mode = HashMap::new();
        let mut expiring_soon = 0;
        let now = SystemTime::now();

        for ret in retentions.values() {
            *objects_by_mode.entry(ret.mode).or_insert(0u64) += 1;

            // Check if expiring within 30 days
            if let Some(until) = ret.retain_until {
                if let Ok(remaining) = until.duration_since(now) {
                    if remaining.as_secs() < 30 * 24 * 3600 {
                        expiring_soon += 1;
                    }
                }
            }
        }

        ComplianceReport {
            generated_at: SystemTime::now(),
            standards: self.config.standards.clone(),
            total_buckets: bucket_configs.len() as u64,
            worm_enabled_buckets: bucket_configs.values().filter(|b| b.enabled).count() as u64,
            locked_buckets: bucket_configs.values().filter(|b| b.locked).count() as u64,
            total_retained_objects: retentions.len() as u64,
            objects_by_mode,
            expiring_soon_30_days: expiring_soon,
            active_legal_holds: hold_stats.active_holds,
            objects_under_hold: hold_stats.total_objects_held,
            total_policies: policies.len() as u64,
            audit_enabled: self.config.audit_enabled,
        }
    }

    /// Validates overall compliance status
    pub async fn validate_compliance(&self) -> Vec<ComplianceIssue> {
        let mut issues = Vec::new();
        let bucket_configs = self.bucket_configs.read().await;

        // Check standard-specific requirements
        for standard in &self.config.standards {
            match standard {
                ComplianceStandard::Sec17a4 | ComplianceStandard::Finra => {
                    // Require at least one locked bucket
                    if !bucket_configs.values().any(|b| b.locked) {
                        issues.push(ComplianceIssue {
                            severity: IssueSeverity::High,
                            standard: Some(*standard),
                            message: "No locked WORM buckets found - required for SEC 17a-4".to_string(),
                            recommendation: "Lock at least one WORM bucket".to_string(),
                        });
                    }

                    // Require audit logging
                    if !self.config.audit_enabled {
                        issues.push(ComplianceIssue {
                            severity: IssueSeverity::Critical,
                            standard: Some(*standard),
                            message: "Audit logging is disabled".to_string(),
                            recommendation: "Enable audit logging for compliance".to_string(),
                        });
                    }
                }
                ComplianceStandard::Hipaa => {
                    // Similar checks for HIPAA
                }
                _ => {}
            }
        }

        issues
    }
}

/// Compliance report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceReport {
    pub generated_at: SystemTime,
    pub standards: Vec<ComplianceStandard>,
    pub total_buckets: u64,
    pub worm_enabled_buckets: u64,
    pub locked_buckets: u64,
    pub total_retained_objects: u64,
    pub objects_by_mode: HashMap<RetentionMode, u64>,
    pub expiring_soon_30_days: u64,
    pub active_legal_holds: u64,
    pub objects_under_hold: u64,
    pub total_policies: u64,
    pub audit_enabled: bool,
}

/// Compliance issue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceIssue {
    pub severity: IssueSeverity,
    pub standard: Option<ComplianceStandard>,
    pub message: String,
    pub recommendation: String,
}

/// Issue severity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueSeverity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_compliance_manager() {
        let config = ComplianceConfig::sec_17a4();
        let manager = ComplianceManager::new(config);

        // Configure bucket
        let mut bucket_config = BucketRetentionConfig::new("compliance-bucket");
        bucket_config.enable().unwrap();
        bucket_config.default_mode = RetentionMode::Compliance;
        bucket_config.lock().unwrap();

        manager.configure_bucket(bucket_config).await.unwrap();

        // Set retention
        let until = SystemTime::now() + std::time::Duration::from_secs(86400 * 365);
        manager.set_object_retention(
            "compliance-bucket",
            "record.pdf",
            RetentionMode::Compliance,
            Some(until),
            "admin",
        ).await.unwrap();

        // Check delete is blocked
        let result = manager.check_access(
            "compliance-bucket",
            "record.pdf",
            AccessType::Delete,
            "user",
            false,
        ).await;

        assert!(!result.allowed);
    }

    #[tokio::test]
    async fn test_governance_bypass() {
        let config = ComplianceConfig::default();
        let manager = ComplianceManager::new(config);

        // Configure bucket
        let mut bucket_config = BucketRetentionConfig::new("gov-bucket");
        bucket_config.enable().unwrap();
        bucket_config.default_mode = RetentionMode::Governance;

        manager.configure_bucket(bucket_config).await.unwrap();

        let until = SystemTime::now() + std::time::Duration::from_secs(86400 * 30);
        manager.set_object_retention(
            "gov-bucket",
            "file.txt",
            RetentionMode::Governance,
            Some(until),
            "admin",
        ).await.unwrap();

        // Regular user cannot delete
        let result = manager.check_access(
            "gov-bucket",
            "file.txt",
            AccessType::Delete,
            "user",
            false,
        ).await;
        assert!(!result.allowed);

        // Privileged user can delete
        let result = manager.check_access(
            "gov-bucket",
            "file.txt",
            AccessType::Delete,
            "admin",
            true,
        ).await;
        assert!(result.allowed);
        assert!(!result.warnings.is_empty()); // Should have bypass warning
    }

    #[tokio::test]
    async fn test_compliance_report() {
        let config = ComplianceConfig::sec_17a4();
        let manager = ComplianceManager::new(config);

        let report = manager.generate_report().await;
        assert!(report.audit_enabled);
        assert!(report.standards.contains(&ComplianceStandard::Sec17a4));
    }

    #[tokio::test]
    async fn test_validation() {
        let config = ComplianceConfig::sec_17a4();
        let manager = ComplianceManager::new(config);

        let issues = manager.validate_compliance().await;

        // Should have issues since no locked buckets
        assert!(!issues.is_empty());
    }
}
