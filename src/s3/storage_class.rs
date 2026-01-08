//! S3 storage class management and transition execution.
//!
//! Implements storage class tracking and automatic transitions based on lifecycle rules.

use super::lifecycle::{LifecycleState, StorageClass, Transition};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Metadata for an object's storage class.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStorageInfo {
    /// Current storage class.
    pub storage_class: StorageClass,
    /// When the object was created.
    pub created_at: u64,
    /// When the storage class was last changed.
    pub last_transitioned_at: Option<u64>,
    /// Object size in bytes.
    pub size: u64,
    /// Object key.
    pub key: String,
}

impl ObjectStorageInfo {
    /// Create new object storage info with STANDARD class.
    pub fn new(key: impl Into<String>, size: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            storage_class: StorageClass::Standard,
            created_at: now,
            last_transitioned_at: None,
            size,
            key: key.into(),
        }
    }

    /// Create with a specific storage class.
    pub fn with_storage_class(mut self, class: StorageClass) -> Self {
        self.storage_class = class;
        self
    }

    /// Get age in days since creation.
    pub fn age_days(&self) -> u32 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        ((now.saturating_sub(self.created_at)) / 86400) as u32
    }

    /// Check if object matches a prefix filter.
    pub fn matches_prefix(&self, prefix: &str) -> bool {
        self.key.starts_with(prefix)
    }
}

/// Result of a transition check.
#[derive(Debug, Clone)]
pub struct TransitionResult {
    /// Object key.
    pub key: String,
    /// Bucket name.
    pub bucket: String,
    /// Previous storage class.
    pub from_class: StorageClass,
    /// New storage class.
    pub to_class: StorageClass,
    /// Rule ID that triggered the transition.
    pub rule_id: Option<String>,
}

/// Storage class transition errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum StorageClassError {
    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Invalid transition from {from} to {to}")]
    InvalidTransition { from: String, to: String },

    #[error("Storage class not supported: {0}")]
    UnsupportedClass(String),

    #[error("Transition in progress")]
    TransitionInProgress,
}

/// Store for tracking object storage classes.
#[derive(Clone)]
pub struct StorageClassStore {
    /// Map of (bucket, key) -> storage info.
    objects: Arc<RwLock<HashMap<(String, String), ObjectStorageInfo>>>,
}

impl Default for StorageClassStore {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageClassStore {
    /// Create a new storage class store.
    pub fn new() -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get storage info for an object.
    pub fn get(&self, bucket: &str, key: &str) -> Option<ObjectStorageInfo> {
        self.objects
            .read()
            .ok()?
            .get(&(bucket.to_string(), key.to_string()))
            .cloned()
    }

    /// Set storage info for an object.
    pub fn set(&self, bucket: &str, key: &str, info: ObjectStorageInfo) {
        if let Ok(mut objects) = self.objects.write() {
            objects.insert((bucket.to_string(), key.to_string()), info);
        }
    }

    /// Remove storage info for an object.
    pub fn remove(&self, bucket: &str, key: &str) -> Option<ObjectStorageInfo> {
        self.objects
            .write()
            .ok()?
            .remove(&(bucket.to_string(), key.to_string()))
    }

    /// List all objects in a bucket.
    pub fn list_bucket(&self, bucket: &str) -> Vec<ObjectStorageInfo> {
        self.objects
            .read()
            .ok()
            .map(|objects| {
                objects
                    .iter()
                    .filter(|((b, _), _)| b == bucket)
                    .map(|(_, info)| info.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List objects with a specific storage class.
    pub fn list_by_class(&self, bucket: &str, class: StorageClass) -> Vec<ObjectStorageInfo> {
        self.objects
            .read()
            .ok()
            .map(|objects| {
                objects
                    .iter()
                    .filter(|((b, _), info)| b == bucket && info.storage_class == class)
                    .map(|(_, info)| info.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Update storage class for an object.
    pub fn transition(
        &self,
        bucket: &str,
        key: &str,
        new_class: StorageClass,
    ) -> Result<TransitionResult, StorageClassError> {
        let mut objects = self
            .objects
            .write()
            .map_err(|_| StorageClassError::TransitionInProgress)?;

        let bucket_key = (bucket.to_string(), key.to_string());
        let info = objects
            .get_mut(&bucket_key)
            .ok_or_else(|| StorageClassError::ObjectNotFound(key.to_string()))?;

        // Validate transition is allowed
        if !is_valid_transition(info.storage_class, new_class) {
            return Err(StorageClassError::InvalidTransition {
                from: info.storage_class.as_str().to_string(),
                to: new_class.as_str().to_string(),
            });
        }

        let from_class = info.storage_class;
        info.storage_class = new_class;
        info.last_transitioned_at = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        Ok(TransitionResult {
            key: key.to_string(),
            bucket: bucket.to_string(),
            from_class,
            to_class: new_class,
            rule_id: None,
        })
    }
}

/// Check if a storage class transition is valid.
///
/// S3 only allows transitions to colder storage classes.
pub fn is_valid_transition(from: StorageClass, to: StorageClass) -> bool {
    let tier = |class: StorageClass| -> u8 {
        match class {
            StorageClass::Standard => 0,
            StorageClass::IntelligentTiering => 1,
            StorageClass::StandardIa => 2,
            StorageClass::OnezoneIa => 2,
            StorageClass::GlacierIr => 3,
            StorageClass::Glacier => 4,
            StorageClass::DeepArchive => 5,
        }
    };

    // Can only transition to colder tiers (higher numbers)
    // Or same tier for IA variants
    tier(to) >= tier(from)
}

/// State for storage class operations.
#[derive(Clone)]
pub struct StorageClassState {
    /// Storage class store.
    pub store: StorageClassStore,
}

impl Default for StorageClassState {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageClassState {
    /// Create new storage class state.
    pub fn new() -> Self {
        Self {
            store: StorageClassStore::new(),
        }
    }
}

/// Transition executor that applies lifecycle rules.
pub struct TransitionExecutor {
    /// Storage class state.
    storage_state: StorageClassState,
    /// Lifecycle state for rules.
    lifecycle_state: LifecycleState,
    /// Check interval.
    check_interval: Duration,
}

impl TransitionExecutor {
    /// Create a new transition executor.
    pub fn new(storage_state: StorageClassState, lifecycle_state: LifecycleState) -> Self {
        Self {
            storage_state,
            lifecycle_state,
            check_interval: Duration::from_secs(3600), // Check hourly by default
        }
    }

    /// Set the check interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    /// Evaluate lifecycle rules and identify objects to transition.
    pub fn evaluate_transitions(&self, bucket: &str) -> Vec<TransitionResult> {
        let mut results = Vec::new();

        // Get lifecycle configuration for bucket
        let config = match self.lifecycle_state.store.get(bucket) {
            Some(c) => c,
            None => return results,
        };

        // Get all objects in bucket
        let objects = self.storage_state.store.list_bucket(bucket);

        for rule in &config.rules {
            // Skip disabled rules
            if rule.status != super::lifecycle::RuleStatus::Enabled {
                continue;
            }

            // Process each transition in the rule
            for transition in &rule.transitions {
                for obj in &objects {
                    if self.should_transition(obj, &rule.filter, transition) {
                        match self.storage_state.store.transition(
                            bucket,
                            &obj.key,
                            transition.storage_class,
                        ) {
                            Ok(mut result) => {
                                result.rule_id = rule.id.clone();
                                debug!(
                                    bucket = %bucket,
                                    key = %obj.key,
                                    from = %result.from_class.as_str(),
                                    to = %result.to_class.as_str(),
                                    "Transitioned object storage class"
                                );
                                results.push(result);
                            }
                            Err(e) => {
                                warn!(
                                    bucket = %bucket,
                                    key = %obj.key,
                                    error = %e,
                                    "Failed to transition object"
                                );
                            }
                        }
                    }
                }
            }
        }

        if !results.is_empty() {
            info!(
                bucket = %bucket,
                count = results.len(),
                "Completed storage class transitions"
            );
        }

        results
    }

    /// Check if an object should be transitioned based on a rule.
    fn should_transition(
        &self,
        obj: &ObjectStorageInfo,
        filter: &super::lifecycle::LifecycleFilter,
        transition: &Transition,
    ) -> bool {
        // Check prefix filter
        if let Some(ref prefix) = filter.prefix {
            if !obj.matches_prefix(prefix) {
                return false;
            }
        }

        // Check size filters
        if let Some(min_size) = filter.object_size_greater_than {
            if obj.size <= min_size {
                return false;
            }
        }

        if let Some(max_size) = filter.object_size_less_than {
            if obj.size >= max_size {
                return false;
            }
        }

        // Check if already at or past target class
        if !is_valid_transition(obj.storage_class, transition.storage_class) {
            return false;
        }

        // Already at target class
        if obj.storage_class == transition.storage_class {
            return false;
        }

        // Check age requirement
        if let Some(days) = transition.days {
            if obj.age_days() < days {
                return false;
            }
        }

        // TODO: Handle date-based transitions

        true
    }

    /// Run transition check for all buckets with lifecycle rules.
    pub async fn run_once(&self, buckets: &[String]) -> Vec<TransitionResult> {
        let mut all_results = Vec::new();

        for bucket in buckets {
            let results = self.evaluate_transitions(bucket);
            all_results.extend(results);
        }

        all_results
    }
}

/// Restore request for archived objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreRequest {
    /// Number of days to keep the restored copy.
    pub days: u32,
    /// Restore tier (speed).
    pub tier: RestoreTier,
}

impl Default for RestoreRequest {
    fn default() -> Self {
        Self {
            days: 1,
            tier: RestoreTier::Standard,
        }
    }
}

/// Restore tier (retrieval speed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum RestoreTier {
    /// 3-5 hours for Glacier, not available for Deep Archive.
    Expedited,
    /// 3-5 hours for Glacier, 12 hours for Deep Archive.
    Standard,
    /// 5-12 hours for Glacier, 48 hours for Deep Archive.
    Bulk,
}

impl RestoreTier {
    /// Parse from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "expedited" => Some(Self::Expedited),
            "standard" => Some(Self::Standard),
            "bulk" => Some(Self::Bulk),
            _ => None,
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Expedited => "Expedited",
            Self::Standard => "Standard",
            Self::Bulk => "Bulk",
        }
    }
}

/// Status of an object restore operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreStatus {
    /// Whether the restore is in progress.
    pub in_progress: bool,
    /// When the restored copy expires.
    pub expiry_date: Option<u64>,
}

/// Parse restore request from XML.
pub fn parse_restore_xml(xml: &str) -> Result<RestoreRequest, StorageClassError> {
    let mut request = RestoreRequest::default();

    // Extract Days
    if let Some(days_start) = xml.find("<Days>") {
        if let Some(days_end) = xml[days_start..].find("</Days>") {
            let days_str = &xml[days_start + 6..days_start + days_end];
            if let Ok(days) = days_str.trim().parse::<u32>() {
                request.days = days;
            }
        }
    }

    // Extract Tier
    if let Some(tier_start) = xml.find("<Tier>") {
        if let Some(tier_end) = xml[tier_start..].find("</Tier>") {
            let tier_str = &xml[tier_start + 6..tier_start + tier_end];
            if let Some(tier) = RestoreTier::from_str(tier_str.trim()) {
                request.tier = tier;
            }
        }
    }

    Ok(request)
}

/// Generate XML for restore status in response headers.
pub fn restore_status_header(status: &RestoreStatus) -> String {
    if status.in_progress {
        "ongoing-request=\"true\"".to_string()
    } else if let Some(expiry) = status.expiry_date {
        // Format as HTTP date
        format!("ongoing-request=\"false\", expiry-date=\"{}\"", expiry)
    } else {
        "ongoing-request=\"false\"".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_storage_info_new() {
        let info = ObjectStorageInfo::new("test/key.txt", 1024);
        assert_eq!(info.storage_class, StorageClass::Standard);
        assert_eq!(info.key, "test/key.txt");
        assert_eq!(info.size, 1024);
    }

    #[test]
    fn test_object_storage_info_with_class() {
        let info = ObjectStorageInfo::new("key", 100).with_storage_class(StorageClass::Glacier);
        assert_eq!(info.storage_class, StorageClass::Glacier);
    }

    #[test]
    fn test_matches_prefix() {
        let info = ObjectStorageInfo::new("logs/2024/data.log", 100);
        assert!(info.matches_prefix("logs/"));
        assert!(info.matches_prefix("logs/2024/"));
        assert!(!info.matches_prefix("data/"));
    }

    #[test]
    fn test_is_valid_transition() {
        // Valid transitions (to colder storage)
        assert!(is_valid_transition(
            StorageClass::Standard,
            StorageClass::StandardIa
        ));
        assert!(is_valid_transition(
            StorageClass::Standard,
            StorageClass::Glacier
        ));
        assert!(is_valid_transition(
            StorageClass::StandardIa,
            StorageClass::Glacier
        ));
        assert!(is_valid_transition(
            StorageClass::Glacier,
            StorageClass::DeepArchive
        ));

        // Invalid transitions (to warmer storage)
        assert!(!is_valid_transition(
            StorageClass::Glacier,
            StorageClass::Standard
        ));
        assert!(!is_valid_transition(
            StorageClass::DeepArchive,
            StorageClass::Glacier
        ));

        // Same class is valid
        assert!(is_valid_transition(
            StorageClass::Standard,
            StorageClass::Standard
        ));
    }

    #[test]
    fn test_storage_class_store_basic() {
        let store = StorageClassStore::new();

        let info = ObjectStorageInfo::new("key1", 100);
        store.set("bucket1", "key1", info);

        let retrieved = store.get("bucket1", "key1").unwrap();
        assert_eq!(retrieved.key, "key1");
        assert_eq!(retrieved.size, 100);
    }

    #[test]
    fn test_storage_class_store_transition() {
        let store = StorageClassStore::new();

        let info = ObjectStorageInfo::new("key1", 100);
        store.set("bucket1", "key1", info);

        let result = store
            .transition("bucket1", "key1", StorageClass::StandardIa)
            .unwrap();
        assert_eq!(result.from_class, StorageClass::Standard);
        assert_eq!(result.to_class, StorageClass::StandardIa);

        let updated = store.get("bucket1", "key1").unwrap();
        assert_eq!(updated.storage_class, StorageClass::StandardIa);
        assert!(updated.last_transitioned_at.is_some());
    }

    #[test]
    fn test_storage_class_store_invalid_transition() {
        let store = StorageClassStore::new();

        let info = ObjectStorageInfo::new("key1", 100).with_storage_class(StorageClass::Glacier);
        store.set("bucket1", "key1", info);

        // Trying to transition to warmer storage should fail
        let result = store.transition("bucket1", "key1", StorageClass::Standard);
        assert!(matches!(result, Err(StorageClassError::InvalidTransition { .. })));
    }

    #[test]
    fn test_storage_class_store_list_bucket() {
        let store = StorageClassStore::new();

        store.set("bucket1", "key1", ObjectStorageInfo::new("key1", 100));
        store.set("bucket1", "key2", ObjectStorageInfo::new("key2", 200));
        store.set("bucket2", "key3", ObjectStorageInfo::new("key3", 300));

        let bucket1_objects = store.list_bucket("bucket1");
        assert_eq!(bucket1_objects.len(), 2);

        let bucket2_objects = store.list_bucket("bucket2");
        assert_eq!(bucket2_objects.len(), 1);
    }

    #[test]
    fn test_storage_class_store_list_by_class() {
        let store = StorageClassStore::new();

        store.set("bucket1", "key1", ObjectStorageInfo::new("key1", 100));
        store.set(
            "bucket1",
            "key2",
            ObjectStorageInfo::new("key2", 200).with_storage_class(StorageClass::Glacier),
        );

        let standard_objects = store.list_by_class("bucket1", StorageClass::Standard);
        assert_eq!(standard_objects.len(), 1);

        let glacier_objects = store.list_by_class("bucket1", StorageClass::Glacier);
        assert_eq!(glacier_objects.len(), 1);
    }

    #[test]
    fn test_restore_tier() {
        assert_eq!(
            RestoreTier::from_str("Expedited"),
            Some(RestoreTier::Expedited)
        );
        assert_eq!(
            RestoreTier::from_str("standard"),
            Some(RestoreTier::Standard)
        );
        assert_eq!(RestoreTier::from_str("BULK"), Some(RestoreTier::Bulk));
        assert_eq!(RestoreTier::from_str("invalid"), None);
    }

    #[test]
    fn test_parse_restore_xml() {
        let xml = r#"
            <RestoreRequest>
                <Days>7</Days>
                <GlacierJobParameters>
                    <Tier>Expedited</Tier>
                </GlacierJobParameters>
            </RestoreRequest>
        "#;

        let request = parse_restore_xml(xml).unwrap();
        assert_eq!(request.days, 7);
        assert_eq!(request.tier, RestoreTier::Expedited);
    }

    #[test]
    fn test_restore_status_header() {
        let in_progress = RestoreStatus {
            in_progress: true,
            expiry_date: None,
        };
        assert_eq!(
            restore_status_header(&in_progress),
            "ongoing-request=\"true\""
        );

        let completed = RestoreStatus {
            in_progress: false,
            expiry_date: Some(1234567890),
        };
        let header = restore_status_header(&completed);
        assert!(header.contains("ongoing-request=\"false\""));
        assert!(header.contains("expiry-date"));
    }

    #[test]
    fn test_storage_class_state_thread_safe() {
        use std::thread;

        let state = StorageClassState::new();
        let state1 = state.clone();
        let state2 = state.clone();

        let h1 = thread::spawn(move || {
            state1
                .store
                .set("bucket", "key1", ObjectStorageInfo::new("key1", 100));
        });

        let h2 = thread::spawn(move || {
            state2
                .store
                .set("bucket", "key2", ObjectStorageInfo::new("key2", 200));
        });

        h1.join().unwrap();
        h2.join().unwrap();

        assert!(state.store.get("bucket", "key1").is_some());
        assert!(state.store.get("bucket", "key2").is_some());
    }
}
