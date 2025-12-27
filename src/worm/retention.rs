// Retention policy management for WORM storage

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// Retention mode determining modification rules
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RetentionMode {
    /// Governance mode - privileged users can modify/delete
    /// Use for soft compliance requirements
    #[default]
    Governance,
    /// Compliance mode - no one can modify/delete until retention expires
    /// Use for regulatory compliance (SEC 17a-4, FINRA, HIPAA)
    Compliance,
}

/// Retention period specification
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionPeriod {
    /// Retention for a specific duration
    Duration {
        #[serde(with = "crate::config::humantime_serde")]
        duration: Duration,
    },
    /// Retention until a specific date
    UntilDate { date: SystemTime },
    /// Indefinite retention (only manual release)
    Indefinite,
    /// No retention (can be deleted anytime)
    #[default]
    None,
}

impl RetentionPeriod {
    /// Creates a duration-based retention
    pub fn days(days: u64) -> Self {
        RetentionPeriod::Duration {
            duration: Duration::from_secs(days * 24 * 3600),
        }
    }

    /// Creates a retention until specific date
    pub fn until(date: SystemTime) -> Self {
        RetentionPeriod::UntilDate { date }
    }

    /// Checks if retention has expired
    pub fn is_expired(&self, created_at: SystemTime) -> bool {
        match self {
            RetentionPeriod::Duration { duration } => {
                let expiry = created_at + *duration;
                SystemTime::now() >= expiry
            }
            RetentionPeriod::UntilDate { date } => {
                SystemTime::now() >= *date
            }
            RetentionPeriod::Indefinite => false,
            RetentionPeriod::None => true,
        }
    }

    /// Gets the expiration time
    pub fn expiration(&self, created_at: SystemTime) -> Option<SystemTime> {
        match self {
            RetentionPeriod::Duration { duration } => Some(created_at + *duration),
            RetentionPeriod::UntilDate { date } => Some(*date),
            RetentionPeriod::Indefinite => None,
            RetentionPeriod::None => None,
        }
    }

    /// Returns remaining time until expiration
    pub fn remaining(&self, created_at: SystemTime) -> Option<Duration> {
        let expiry = self.expiration(created_at)?;
        expiry.duration_since(SystemTime::now()).ok()
    }
}

/// Retention policy for objects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Unique policy ID
    pub id: String,
    /// Policy name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Retention mode
    pub mode: RetentionMode,
    /// Default retention period for new objects
    pub default_period: RetentionPeriod,
    /// Minimum retention period (cannot set shorter)
    pub min_period: Option<Duration>,
    /// Maximum retention period (cannot set longer)
    pub max_period: Option<Duration>,
    /// Whether to extend retention on update
    pub extend_on_update: bool,
    /// Whether the policy is locked (cannot be deleted)
    pub locked: bool,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Last modification timestamp
    pub updated_at: SystemTime,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: "default".to_string(),
            description: None,
            mode: RetentionMode::Governance,
            default_period: RetentionPeriod::None,
            min_period: None,
            max_period: None,
            extend_on_update: false,
            locked: false,
            created_at: now,
            updated_at: now,
        }
    }
}

impl RetentionPolicy {
    /// Creates a new retention policy
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Creates a SEC 17a-4 compliant policy
    pub fn sec_17a4() -> Self {
        Self {
            name: "SEC-17a4".to_string(),
            description: Some("SEC Rule 17a-4 compliance for broker-dealer records".to_string()),
            mode: RetentionMode::Compliance,
            default_period: RetentionPeriod::days(365 * 6), // 6 years
            min_period: Some(Duration::from_secs(365 * 6 * 24 * 3600)),
            max_period: None,
            extend_on_update: true,
            locked: true,
            ..Default::default()
        }
    }

    /// Creates a HIPAA compliant policy
    pub fn hipaa() -> Self {
        Self {
            name: "HIPAA".to_string(),
            description: Some("HIPAA compliance for healthcare records".to_string()),
            mode: RetentionMode::Compliance,
            default_period: RetentionPeriod::days(365 * 6), // 6 years
            min_period: Some(Duration::from_secs(365 * 6 * 24 * 3600)),
            max_period: None,
            extend_on_update: true,
            locked: true,
            ..Default::default()
        }
    }

    /// Creates a GDPR compliant policy (allows deletion)
    pub fn gdpr() -> Self {
        Self {
            name: "GDPR".to_string(),
            description: Some("GDPR compliance with right to erasure".to_string()),
            mode: RetentionMode::Governance, // Allows deletion by privileged users
            default_period: RetentionPeriod::days(365 * 3), // 3 years default
            min_period: None,
            max_period: Some(Duration::from_secs(365 * 10 * 24 * 3600)), // Max 10 years
            extend_on_update: false,
            locked: false,
            ..Default::default()
        }
    }

    /// Creates a financial records policy (SOX)
    pub fn sox() -> Self {
        Self {
            name: "SOX".to_string(),
            description: Some("Sarbanes-Oxley compliance for financial records".to_string()),
            mode: RetentionMode::Compliance,
            default_period: RetentionPeriod::days(365 * 7), // 7 years
            min_period: Some(Duration::from_secs(365 * 7 * 24 * 3600)),
            max_period: None,
            extend_on_update: true,
            locked: true,
            ..Default::default()
        }
    }

    /// Sets retention mode
    pub fn with_mode(mut self, mode: RetentionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Sets default period
    pub fn with_default_period(mut self, period: RetentionPeriod) -> Self {
        self.default_period = period;
        self
    }

    /// Validates a requested retention period against policy
    pub fn validate_period(&self, requested: &RetentionPeriod) -> Result<()> {
        // Extract duration for validation
        let requested_duration = match requested {
            RetentionPeriod::Duration { duration } => Some(*duration),
            RetentionPeriod::UntilDate { date } => {
                date.duration_since(SystemTime::now()).ok()
            }
            RetentionPeriod::Indefinite => None, // Always valid
            RetentionPeriod::None => {
                if self.min_period.is_some() {
                    return Err(StrataError::InvalidOperation(
                        "Retention period required by policy".to_string()
                    ));
                }
                return Ok(());
            }
        };

        // Check against min period
        if let (Some(requested), Some(min)) = (requested_duration, self.min_period) {
            if requested < min {
                return Err(StrataError::InvalidOperation(format!(
                    "Retention period {:?} is less than minimum {:?}",
                    requested, min
                )));
            }
        }

        // Check against max period
        if let (Some(requested), Some(max)) = (requested_duration, self.max_period) {
            if requested > max {
                return Err(StrataError::InvalidOperation(format!(
                    "Retention period {:?} exceeds maximum {:?}",
                    requested, max
                )));
            }
        }

        Ok(())
    }
}

/// Object retention settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRetention {
    /// Object path/key
    pub object_key: String,
    /// Bucket name
    pub bucket: String,
    /// Applied retention mode
    pub mode: RetentionMode,
    /// Retention expiration
    pub retain_until: Option<SystemTime>,
    /// Policy ID that applied this retention
    pub policy_id: Option<String>,
    /// Whether retention is locked (cannot be shortened)
    pub locked: bool,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Last modification timestamp
    pub updated_at: SystemTime,
}

impl ObjectRetention {
    /// Creates new object retention
    pub fn new(bucket: &str, key: &str, mode: RetentionMode, until: Option<SystemTime>) -> Self {
        let now = SystemTime::now();
        Self {
            object_key: key.to_string(),
            bucket: bucket.to_string(),
            mode,
            retain_until: until,
            policy_id: None,
            locked: mode == RetentionMode::Compliance,
            created_at: now,
            updated_at: now,
        }
    }

    /// Checks if retention has expired
    pub fn is_expired(&self) -> bool {
        match self.retain_until {
            Some(until) => SystemTime::now() >= until,
            None => false, // Indefinite
        }
    }

    /// Checks if object can be deleted
    pub fn can_delete(&self, is_privileged: bool) -> bool {
        // If retention has expired, allow deletion
        if self.is_expired() {
            return true;
        }

        // In compliance mode, no one can delete
        if self.mode == RetentionMode::Compliance {
            return false;
        }

        // In governance mode, privileged users can delete
        is_privileged
    }

    /// Checks if object can be modified
    pub fn can_modify(&self, _is_privileged: bool) -> bool {
        // WORM objects can never be modified in-place
        // Only version can be created
        false
    }

    /// Extends retention period
    pub fn extend(&mut self, new_until: SystemTime) -> Result<()> {
        if let Some(current_until) = self.retain_until {
            if new_until < current_until && self.locked {
                return Err(StrataError::InvalidOperation(
                    "Cannot shorten locked retention period".to_string()
                ));
            }
        }

        self.retain_until = Some(new_until);
        self.updated_at = SystemTime::now();
        Ok(())
    }

    /// Remaining time until retention expires
    pub fn remaining(&self) -> Option<Duration> {
        let until = self.retain_until?;
        until.duration_since(SystemTime::now()).ok()
    }
}

/// Default retention settings for a bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketRetentionConfig {
    /// Bucket name
    pub bucket: String,
    /// Whether WORM is enabled for bucket
    pub enabled: bool,
    /// Default retention mode for new objects
    pub default_mode: RetentionMode,
    /// Default retention period for new objects
    pub default_period: RetentionPeriod,
    /// Whether to apply retention to all objects (even if not specified)
    pub force_retention: bool,
    /// Associated policy ID
    pub policy_id: Option<String>,
    /// Bucket lock status (cannot disable WORM once locked)
    pub locked: bool,
    /// Lock date
    pub locked_at: Option<SystemTime>,
}

impl Default for BucketRetentionConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            enabled: false,
            default_mode: RetentionMode::Governance,
            default_period: RetentionPeriod::None,
            force_retention: false,
            policy_id: None,
            locked: false,
            locked_at: None,
        }
    }
}

impl BucketRetentionConfig {
    /// Creates new bucket retention config
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            ..Default::default()
        }
    }

    /// Enables WORM for the bucket
    pub fn enable(&mut self) -> Result<()> {
        self.enabled = true;
        Ok(())
    }

    /// Disables WORM for the bucket (if not locked)
    pub fn disable(&mut self) -> Result<()> {
        if self.locked {
            return Err(StrataError::InvalidOperation(
                "Cannot disable WORM on locked bucket".to_string()
            ));
        }
        self.enabled = false;
        Ok(())
    }

    /// Locks the bucket (cannot disable WORM afterward)
    pub fn lock(&mut self) -> Result<()> {
        if !self.enabled {
            return Err(StrataError::InvalidOperation(
                "Cannot lock bucket without enabling WORM first".to_string()
            ));
        }
        self.locked = true;
        self.locked_at = Some(SystemTime::now());
        Ok(())
    }

    /// Gets default retention for a new object
    pub fn get_default_retention(&self, key: &str) -> ObjectRetention {
        let until = self.default_period.expiration(SystemTime::now());
        ObjectRetention::new(&self.bucket, key, self.default_mode, until)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_period_expiration() {
        let created = SystemTime::now() - Duration::from_secs(7200); // 2 hours ago

        let period = RetentionPeriod::Duration {
            duration: Duration::from_secs(3600), // 1 hour
        };
        assert!(period.is_expired(created));

        let period = RetentionPeriod::Duration {
            duration: Duration::from_secs(86400), // 1 day
        };
        assert!(!period.is_expired(created));
    }

    #[test]
    fn test_retention_mode() {
        let retention = ObjectRetention::new(
            "bucket",
            "key",
            RetentionMode::Compliance,
            Some(SystemTime::now() + Duration::from_secs(86400)),
        );

        assert!(!retention.can_delete(false)); // Regular user
        assert!(!retention.can_delete(true));  // Even privileged user

        let retention = ObjectRetention::new(
            "bucket",
            "key",
            RetentionMode::Governance,
            Some(SystemTime::now() + Duration::from_secs(86400)),
        );

        assert!(!retention.can_delete(false)); // Regular user
        assert!(retention.can_delete(true));   // Privileged user can bypass
    }

    #[test]
    fn test_retention_policy_validation() {
        let policy = RetentionPolicy {
            min_period: Some(Duration::from_secs(86400 * 7)), // 7 days
            max_period: Some(Duration::from_secs(86400 * 365)), // 1 year
            ..Default::default()
        };

        // Too short
        let short = RetentionPeriod::Duration {
            duration: Duration::from_secs(86400), // 1 day
        };
        assert!(policy.validate_period(&short).is_err());

        // Too long
        let long = RetentionPeriod::Duration {
            duration: Duration::from_secs(86400 * 400), // 400 days
        };
        assert!(policy.validate_period(&long).is_err());

        // Just right
        let ok = RetentionPeriod::Duration {
            duration: Duration::from_secs(86400 * 30), // 30 days
        };
        assert!(policy.validate_period(&ok).is_ok());
    }

    #[test]
    fn test_bucket_lock() {
        let mut config = BucketRetentionConfig::new("test-bucket");

        // Cannot lock without enabling first
        assert!(config.lock().is_err());

        config.enable().unwrap();
        config.lock().unwrap();

        // Cannot disable after locking
        assert!(config.disable().is_err());
    }

    #[test]
    fn test_extend_retention() {
        let mut retention = ObjectRetention::new(
            "bucket",
            "key",
            RetentionMode::Compliance,
            Some(SystemTime::now() + Duration::from_secs(86400)),
        );

        // Can extend
        let new_until = SystemTime::now() + Duration::from_secs(86400 * 7);
        assert!(retention.extend(new_until).is_ok());

        // Cannot shorten when locked
        let shorter = SystemTime::now() + Duration::from_secs(3600);
        assert!(retention.extend(shorter).is_err());
    }
}
