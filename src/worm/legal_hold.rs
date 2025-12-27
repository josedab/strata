// Legal hold management for WORM storage

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Legal hold status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HoldStatus {
    /// Hold is active
    Active,
    /// Hold has been released
    Released,
    /// Hold has expired
    Expired,
}

/// Legal hold on an object or set of objects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegalHold {
    /// Unique hold ID
    pub id: String,
    /// Hold name/identifier (e.g., case number)
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Current status
    pub status: HoldStatus,
    /// Who placed the hold
    pub placed_by: String,
    /// When the hold was placed
    pub placed_at: SystemTime,
    /// When the hold was released (if released)
    pub released_at: Option<SystemTime>,
    /// Who released the hold
    pub released_by: Option<String>,
    /// Release reason
    pub release_reason: Option<String>,
    /// Expiration date (optional)
    pub expires_at: Option<SystemTime>,
    /// Tags/metadata
    pub tags: HashMap<String, String>,
}

impl LegalHold {
    /// Creates a new legal hold
    pub fn new(name: impl Into<String>, placed_by: impl Into<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            description: None,
            status: HoldStatus::Active,
            placed_by: placed_by.into(),
            placed_at: SystemTime::now(),
            released_at: None,
            released_by: None,
            release_reason: None,
            expires_at: None,
            tags: HashMap::new(),
        }
    }

    /// Sets description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets expiration
    pub fn with_expiration(mut self, expires_at: SystemTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Adds a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Checks if hold is currently active
    pub fn is_active(&self) -> bool {
        if self.status != HoldStatus::Active {
            return false;
        }

        // Check expiration
        if let Some(expires) = self.expires_at {
            if SystemTime::now() >= expires {
                return false;
            }
        }

        true
    }

    /// Releases the hold
    pub fn release(&mut self, released_by: impl Into<String>, reason: Option<String>) {
        self.status = HoldStatus::Released;
        self.released_at = Some(SystemTime::now());
        self.released_by = Some(released_by.into());
        self.release_reason = reason;
    }
}

/// Object legal hold association
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectHold {
    /// Object key
    pub object_key: String,
    /// Bucket name
    pub bucket: String,
    /// Version ID (if versioned)
    pub version_id: Option<String>,
    /// Hold ID
    pub hold_id: String,
    /// When object was added to hold
    pub added_at: SystemTime,
    /// Who added object to hold
    pub added_by: String,
}

/// Legal hold manager
pub struct LegalHoldManager {
    /// All holds
    holds: Arc<RwLock<HashMap<String, LegalHold>>>,
    /// Object to holds mapping (object_key -> set of hold_ids)
    object_holds: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    /// Hold to objects mapping (hold_id -> set of object_keys)
    hold_objects: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl LegalHoldManager {
    /// Creates a new legal hold manager
    pub fn new() -> Self {
        Self {
            holds: Arc::new(RwLock::new(HashMap::new())),
            object_holds: Arc::new(RwLock::new(HashMap::new())),
            hold_objects: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a new legal hold
    pub async fn create_hold(&self, hold: LegalHold) -> Result<String> {
        let hold_id = hold.id.clone();

        let mut holds = self.holds.write().await;
        if holds.contains_key(&hold_id) {
            return Err(StrataError::AlreadyExists(format!(
                "Legal hold {} already exists",
                hold_id
            )));
        }

        info!(
            hold_id = %hold_id,
            name = %hold.name,
            placed_by = %hold.placed_by,
            "Created legal hold"
        );

        holds.insert(hold_id.clone(), hold);

        // Initialize empty object set
        let mut hold_objects = self.hold_objects.write().await;
        hold_objects.insert(hold_id.clone(), HashSet::new());

        Ok(hold_id)
    }

    /// Gets a hold by ID
    pub async fn get_hold(&self, hold_id: &str) -> Option<LegalHold> {
        let holds = self.holds.read().await;
        holds.get(hold_id).cloned()
    }

    /// Lists all holds
    pub async fn list_holds(&self, include_released: bool) -> Vec<LegalHold> {
        let holds = self.holds.read().await;
        holds.values()
            .filter(|h| include_released || h.is_active())
            .cloned()
            .collect()
    }

    /// Releases a hold
    pub async fn release_hold(
        &self,
        hold_id: &str,
        released_by: &str,
        reason: Option<String>,
    ) -> Result<()> {
        let mut holds = self.holds.write().await;
        let hold = holds.get_mut(hold_id)
            .ok_or_else(|| StrataError::NotFound(format!("Legal hold {}", hold_id)))?;

        if !hold.is_active() {
            return Err(StrataError::InvalidOperation(
                "Hold is not active".to_string()
            ));
        }

        hold.release(released_by, reason.clone());

        info!(
            hold_id = %hold_id,
            released_by = %released_by,
            reason = ?reason,
            "Released legal hold"
        );

        // Note: Objects remain associated but hold is no longer active
        Ok(())
    }

    /// Adds an object to a hold
    pub async fn add_object(
        &self,
        hold_id: &str,
        bucket: &str,
        object_key: &str,
        added_by: &str,
    ) -> Result<()> {
        // Verify hold exists and is active
        {
            let holds = self.holds.read().await;
            let hold = holds.get(hold_id)
                .ok_or_else(|| StrataError::NotFound(format!("Legal hold {}", hold_id)))?;

            if !hold.is_active() {
                return Err(StrataError::InvalidOperation(
                    "Cannot add objects to inactive hold".to_string()
                ));
            }
        }

        let full_key = format!("{}/{}", bucket, object_key);

        // Add to object_holds mapping
        {
            let mut object_holds = self.object_holds.write().await;
            object_holds
                .entry(full_key.clone())
                .or_insert_with(HashSet::new)
                .insert(hold_id.to_string());
        }

        // Add to hold_objects mapping
        {
            let mut hold_objects = self.hold_objects.write().await;
            hold_objects
                .entry(hold_id.to_string())
                .or_insert_with(HashSet::new)
                .insert(full_key.clone());
        }

        info!(
            hold_id = %hold_id,
            bucket = %bucket,
            object_key = %object_key,
            added_by = %added_by,
            "Added object to legal hold"
        );

        Ok(())
    }

    /// Removes an object from a hold (when hold is released)
    pub async fn remove_object(
        &self,
        hold_id: &str,
        bucket: &str,
        object_key: &str,
    ) -> Result<()> {
        let full_key = format!("{}/{}", bucket, object_key);

        // Remove from object_holds
        {
            let mut object_holds = self.object_holds.write().await;
            if let Some(holds) = object_holds.get_mut(&full_key) {
                holds.remove(hold_id);
                if holds.is_empty() {
                    object_holds.remove(&full_key);
                }
            }
        }

        // Remove from hold_objects
        {
            let mut hold_objects = self.hold_objects.write().await;
            if let Some(objects) = hold_objects.get_mut(hold_id) {
                objects.remove(&full_key);
            }
        }

        debug!(
            hold_id = %hold_id,
            bucket = %bucket,
            object_key = %object_key,
            "Removed object from legal hold"
        );

        Ok(())
    }

    /// Checks if an object is under any active legal hold
    pub async fn is_object_held(&self, bucket: &str, object_key: &str) -> bool {
        let full_key = format!("{}/{}", bucket, object_key);

        let object_holds = self.object_holds.read().await;
        let Some(hold_ids) = object_holds.get(&full_key) else {
            return false;
        };

        if hold_ids.is_empty() {
            return false;
        }

        // Check if any of the holds are active
        let holds = self.holds.read().await;
        for hold_id in hold_ids {
            if let Some(hold) = holds.get(hold_id) {
                if hold.is_active() {
                    return true;
                }
            }
        }

        false
    }

    /// Gets all active holds for an object
    pub async fn get_object_holds(&self, bucket: &str, object_key: &str) -> Vec<LegalHold> {
        let full_key = format!("{}/{}", bucket, object_key);

        let object_holds = self.object_holds.read().await;
        let Some(hold_ids) = object_holds.get(&full_key) else {
            return Vec::new();
        };

        let holds = self.holds.read().await;
        hold_ids.iter()
            .filter_map(|id| holds.get(id))
            .filter(|h| h.is_active())
            .cloned()
            .collect()
    }

    /// Gets all objects under a hold
    pub async fn get_hold_objects(&self, hold_id: &str) -> Vec<String> {
        let hold_objects = self.hold_objects.read().await;
        hold_objects.get(hold_id)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Counts objects under a hold
    pub async fn count_hold_objects(&self, hold_id: &str) -> usize {
        let hold_objects = self.hold_objects.read().await;
        hold_objects.get(hold_id).map(|s| s.len()).unwrap_or(0)
    }

    /// Checks if deletion is allowed for an object
    pub async fn can_delete(&self, bucket: &str, object_key: &str) -> Result<()> {
        if self.is_object_held(bucket, object_key).await {
            let holds = self.get_object_holds(bucket, object_key).await;
            let hold_names: Vec<_> = holds.iter().map(|h| h.name.as_str()).collect();
            return Err(StrataError::InvalidOperation(format!(
                "Object is under legal hold: {}",
                hold_names.join(", ")
            )));
        }
        Ok(())
    }

    /// Expires outdated holds
    pub async fn expire_holds(&self) {
        let mut holds = self.holds.write().await;
        let now = SystemTime::now();

        for hold in holds.values_mut() {
            if hold.status == HoldStatus::Active {
                if let Some(expires) = hold.expires_at {
                    if now >= expires {
                        hold.status = HoldStatus::Expired;
                        info!(hold_id = %hold.id, "Legal hold expired");
                    }
                }
            }
        }
    }

    /// Gets hold statistics
    pub async fn get_stats(&self) -> HoldStats {
        let holds = self.holds.read().await;
        let hold_objects = self.hold_objects.read().await;

        let mut stats = HoldStats::default();

        for hold in holds.values() {
            match hold.status {
                HoldStatus::Active => {
                    if hold.is_active() {
                        stats.active_holds += 1;
                    } else {
                        stats.expired_holds += 1;
                    }
                }
                HoldStatus::Released => stats.released_holds += 1,
                HoldStatus::Expired => stats.expired_holds += 1,
            }
        }

        stats.total_holds = holds.len() as u64;
        stats.total_objects_held = hold_objects.len() as u64;

        stats
    }
}

impl Default for LegalHoldManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Hold statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HoldStats {
    pub total_holds: u64,
    pub active_holds: u64,
    pub released_holds: u64,
    pub expired_holds: u64,
    pub total_objects_held: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_legal_hold_lifecycle() {
        let manager = LegalHoldManager::new();

        // Create hold
        let hold = LegalHold::new("Case-2024-001", "legal@company.com")
            .with_description("Litigation hold for Q1 2024");

        let hold_id = manager.create_hold(hold).await.unwrap();

        // Add object
        manager.add_object(&hold_id, "documents", "contract.pdf", "admin").await.unwrap();

        // Check object is held
        assert!(manager.is_object_held("documents", "contract.pdf").await);

        // Cannot delete
        assert!(manager.can_delete("documents", "contract.pdf").await.is_err());

        // Release hold
        manager.release_hold(&hold_id, "legal@company.com", Some("Case settled".to_string())).await.unwrap();

        // Object no longer actively held
        assert!(!manager.is_object_held("documents", "contract.pdf").await);
    }

    #[tokio::test]
    async fn test_multiple_holds() {
        let manager = LegalHoldManager::new();

        let hold1 = LegalHold::new("Case-A", "user1");
        let hold2 = LegalHold::new("Case-B", "user2");

        let id1 = manager.create_hold(hold1).await.unwrap();
        let id2 = manager.create_hold(hold2).await.unwrap();

        // Add same object to both holds
        manager.add_object(&id1, "bucket", "file.txt", "admin").await.unwrap();
        manager.add_object(&id2, "bucket", "file.txt", "admin").await.unwrap();

        // Release one hold
        manager.release_hold(&id1, "admin", None).await.unwrap();

        // Object still held by second hold
        assert!(manager.is_object_held("bucket", "file.txt").await);

        // Release second hold
        manager.release_hold(&id2, "admin", None).await.unwrap();

        // Object no longer held
        assert!(!manager.is_object_held("bucket", "file.txt").await);
    }

    #[test]
    fn test_hold_expiration() {
        let mut hold = LegalHold::new("Test", "user")
            .with_expiration(SystemTime::now() - Duration::from_secs(3600));

        assert!(!hold.is_active()); // Already expired

        let hold2 = LegalHold::new("Test2", "user")
            .with_expiration(SystemTime::now() + Duration::from_secs(86400));

        assert!(hold2.is_active()); // Not yet expired
    }

    #[tokio::test]
    async fn test_hold_stats() {
        let manager = LegalHoldManager::new();

        let hold1 = LegalHold::new("Active", "user");
        let hold2 = LegalHold::new("ToRelease", "user");

        let id1 = manager.create_hold(hold1).await.unwrap();
        let id2 = manager.create_hold(hold2).await.unwrap();

        manager.add_object(&id1, "bucket", "file1.txt", "admin").await.unwrap();
        manager.add_object(&id1, "bucket", "file2.txt", "admin").await.unwrap();

        manager.release_hold(&id2, "admin", None).await.unwrap();

        let stats = manager.get_stats().await;
        assert_eq!(stats.total_holds, 2);
        assert_eq!(stats.active_holds, 1);
        assert_eq!(stats.released_holds, 1);
        assert_eq!(stats.total_objects_held, 2);
    }
}
