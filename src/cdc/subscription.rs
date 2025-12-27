// CDC Subscriptions

use super::event::{ChangeEvent, ChangeEventType};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Subscription for CDC events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    /// Subscription ID
    pub id: String,
    /// Subscription name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Filter criteria
    pub filter: SubscriptionFilter,
    /// Target connector
    pub connector: String,
    /// Enabled status
    pub enabled: bool,
    /// Creation timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub updated_at: u64,
    /// Owner/creator
    pub owner: Option<String>,
    /// Labels for organization
    pub labels: HashMap<String, String>,
}

impl Subscription {
    /// Creates a new subscription
    pub fn new(name: impl Into<String>, filter: SubscriptionFilter) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.into(),
            description: None,
            filter,
            connector: String::new(),
            enabled: true,
            created_at: now,
            updated_at: now,
            owner: None,
            labels: HashMap::new(),
        }
    }

    /// Sets the connector
    pub fn with_connector(mut self, connector: impl Into<String>) -> Self {
        self.connector = connector.into();
        self
    }

    /// Sets the description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Sets the owner
    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = Some(owner.into());
        self
    }

    /// Adds a label
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Checks if an event matches this subscription
    pub fn matches(&self, event: &ChangeEvent) -> bool {
        if !self.enabled {
            return false;
        }
        self.filter.matches(event)
    }
}

/// Filter for subscription matching
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubscriptionFilter {
    /// Event types to include
    pub event_types: Option<Vec<ChangeEventType>>,
    /// Buckets to include (exact match or glob)
    pub buckets: Option<Vec<String>>,
    /// Key prefixes to include
    pub key_prefixes: Option<Vec<String>>,
    /// Key patterns (regex)
    pub key_patterns: Option<Vec<String>>,
    /// Minimum object size
    pub min_size: Option<u64>,
    /// Maximum object size
    pub max_size: Option<u64>,
    /// Content types to include
    pub content_types: Option<Vec<String>>,
    /// Storage classes to include
    pub storage_classes: Option<Vec<String>>,
    /// Tags required (key=value)
    pub required_tags: Option<HashMap<String, String>>,
    /// Exclude patterns
    pub exclude: Option<Box<SubscriptionFilter>>,
}

impl SubscriptionFilter {
    /// Creates a filter for all events
    pub fn all() -> Self {
        Self::default()
    }

    /// Creates a filter for specific event types
    pub fn for_events(types: Vec<ChangeEventType>) -> Self {
        Self {
            event_types: Some(types),
            ..Default::default()
        }
    }

    /// Creates a filter for a specific bucket
    pub fn for_bucket(bucket: impl Into<String>) -> Self {
        Self {
            buckets: Some(vec![bucket.into()]),
            ..Default::default()
        }
    }

    /// Creates a filter for a key prefix
    pub fn for_prefix(prefix: impl Into<String>) -> Self {
        Self {
            key_prefixes: Some(vec![prefix.into()]),
            ..Default::default()
        }
    }

    /// Adds event types
    pub fn with_event_types(mut self, types: Vec<ChangeEventType>) -> Self {
        self.event_types = Some(types);
        self
    }

    /// Adds buckets
    pub fn with_buckets(mut self, buckets: Vec<String>) -> Self {
        self.buckets = Some(buckets);
        self
    }

    /// Adds key prefixes
    pub fn with_prefixes(mut self, prefixes: Vec<String>) -> Self {
        self.key_prefixes = Some(prefixes);
        self
    }

    /// Sets size range
    pub fn with_size_range(mut self, min: Option<u64>, max: Option<u64>) -> Self {
        self.min_size = min;
        self.max_size = max;
        self
    }

    /// Adds exclusion filter
    pub fn excluding(mut self, filter: SubscriptionFilter) -> Self {
        self.exclude = Some(Box::new(filter));
        self
    }

    /// Checks if an event matches this filter
    pub fn matches(&self, event: &ChangeEvent) -> bool {
        // Check exclusions first
        if let Some(ref exclude) = self.exclude {
            if exclude.matches_positive(event) {
                return false;
            }
        }

        self.matches_positive(event)
    }

    /// Matches without checking exclusions
    fn matches_positive(&self, event: &ChangeEvent) -> bool {
        // Check event types
        if let Some(ref types) = self.event_types {
            if !types.contains(&event.event_type) {
                return false;
            }
        }

        // Check buckets
        if let Some(ref buckets) = self.buckets {
            let bucket_matches = buckets.iter().any(|b| {
                if b.contains('*') {
                    glob_match(b, &event.bucket)
                } else {
                    b == &event.bucket
                }
            });
            if !bucket_matches {
                return false;
            }
        }

        // Check key prefixes
        if let Some(ref prefixes) = self.key_prefixes {
            let prefix_matches = prefixes.iter().any(|p| event.key.starts_with(p));
            if !prefix_matches {
                return false;
            }
        }

        // Check key patterns
        if let Some(ref patterns) = self.key_patterns {
            let pattern_matches = patterns.iter().any(|p| {
                regex::Regex::new(p)
                    .map(|re| re.is_match(&event.key))
                    .unwrap_or(false)
            });
            if !pattern_matches {
                return false;
            }
        }

        // Check size constraints
        if let Some(size) = event.payload.size {
            if let Some(min) = self.min_size {
                if size < min {
                    return false;
                }
            }
            if let Some(max) = self.max_size {
                if size > max {
                    return false;
                }
            }
        }

        // Check content types
        if let Some(ref types) = self.content_types {
            if let Some(ref ct) = event.payload.content_type {
                if !types.iter().any(|t| t == ct || ct.starts_with(t)) {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check storage classes
        if let Some(ref classes) = self.storage_classes {
            if let Some(ref sc) = event.payload.storage_class {
                if !classes.contains(sc) {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }
}

/// Simple glob matching
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.ends_with('*') {
        let prefix = &pattern[..pattern.len() - 1];
        return text.starts_with(prefix);
    }
    if pattern.starts_with('*') {
        let suffix = &pattern[1..];
        return text.ends_with(suffix);
    }
    pattern == text
}

/// Subscription manager
pub struct SubscriptionManager {
    /// Active subscriptions
    subscriptions: Arc<RwLock<HashMap<String, Subscription>>>,
    /// Subscriptions by connector
    by_connector: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl SubscriptionManager {
    /// Creates a new subscription manager
    pub fn new() -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            by_connector: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Adds a subscription
    pub async fn add(&self, subscription: Subscription) -> Result<String> {
        let id = subscription.id.clone();
        let connector = subscription.connector.clone();

        let mut subs = self.subscriptions.write().await;
        subs.insert(id.clone(), subscription);

        // Index by connector
        if !connector.is_empty() {
            let mut by_conn = self.by_connector.write().await;
            by_conn
                .entry(connector)
                .or_insert_with(Vec::new)
                .push(id.clone());
        }

        Ok(id)
    }

    /// Removes a subscription
    pub async fn remove(&self, subscription_id: &str) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        let sub = subs.remove(subscription_id).ok_or_else(|| {
            StrataError::NotFound(format!("Subscription {} not found", subscription_id))
        })?;

        // Remove from connector index
        if !sub.connector.is_empty() {
            let mut by_conn = self.by_connector.write().await;
            if let Some(ids) = by_conn.get_mut(&sub.connector) {
                ids.retain(|id| id != subscription_id);
            }
        }

        Ok(())
    }

    /// Gets a subscription by ID
    pub async fn get(&self, subscription_id: &str) -> Option<Subscription> {
        let subs = self.subscriptions.read().await;
        subs.get(subscription_id).cloned()
    }

    /// Lists all subscriptions
    pub async fn list(&self) -> Vec<Subscription> {
        let subs = self.subscriptions.read().await;
        subs.values().cloned().collect()
    }

    /// Lists subscriptions for a connector
    pub async fn list_for_connector(&self, connector: &str) -> Vec<Subscription> {
        let by_conn = self.by_connector.read().await;
        let subs = self.subscriptions.read().await;

        by_conn
            .get(connector)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| subs.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Updates a subscription
    pub async fn update(&self, subscription: Subscription) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        let id = &subscription.id;

        if !subs.contains_key(id) {
            return Err(StrataError::NotFound(format!("Subscription {} not found", id)));
        }

        subs.insert(id.clone(), subscription);
        Ok(())
    }

    /// Enables a subscription
    pub async fn enable(&self, subscription_id: &str) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        let sub = subs.get_mut(subscription_id).ok_or_else(|| {
            StrataError::NotFound(format!("Subscription {} not found", subscription_id))
        })?;
        sub.enabled = true;
        sub.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Ok(())
    }

    /// Disables a subscription
    pub async fn disable(&self, subscription_id: &str) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        let sub = subs.get_mut(subscription_id).ok_or_else(|| {
            StrataError::NotFound(format!("Subscription {} not found", subscription_id))
        })?;
        sub.enabled = false;
        sub.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Ok(())
    }

    /// Finds matching subscriptions for an event
    pub async fn find_matching(&self, event: &ChangeEvent) -> Vec<Subscription> {
        let subs = self.subscriptions.read().await;
        subs.values()
            .filter(|s| s.matches(event))
            .cloned()
            .collect()
    }

    /// Gets subscription count
    pub async fn count(&self) -> usize {
        self.subscriptions.read().await.len()
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::event::EventPayload;

    #[test]
    fn test_subscription_filter_all() {
        let filter = SubscriptionFilter::all();
        let event = ChangeEvent::new(ChangeEventType::Create, "bucket", "key", 1);
        assert!(filter.matches(&event));
    }

    #[test]
    fn test_subscription_filter_event_types() {
        let filter = SubscriptionFilter::for_events(vec![ChangeEventType::Delete]);

        let create = ChangeEvent::new(ChangeEventType::Create, "bucket", "key", 1);
        let delete = ChangeEvent::new(ChangeEventType::Delete, "bucket", "key", 2);

        assert!(!filter.matches(&create));
        assert!(filter.matches(&delete));
    }

    #[test]
    fn test_subscription_filter_bucket() {
        let filter = SubscriptionFilter::for_bucket("my-bucket");

        let matching = ChangeEvent::new(ChangeEventType::Create, "my-bucket", "key", 1);
        let non_matching = ChangeEvent::new(ChangeEventType::Create, "other-bucket", "key", 2);

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }

    #[test]
    fn test_subscription_filter_prefix() {
        let filter = SubscriptionFilter::for_prefix("logs/");

        let matching = ChangeEvent::new(ChangeEventType::Create, "bucket", "logs/app.log", 1);
        let non_matching = ChangeEvent::new(ChangeEventType::Create, "bucket", "data/file.txt", 2);

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }

    #[test]
    fn test_subscription_filter_size() {
        let filter = SubscriptionFilter::all().with_size_range(Some(1000), Some(10000));

        let mut small = ChangeEvent::new(ChangeEventType::Create, "b", "k", 1);
        small.payload = EventPayload {
            size: Some(500),
            ..Default::default()
        };

        let mut medium = ChangeEvent::new(ChangeEventType::Create, "b", "k", 2);
        medium.payload = EventPayload {
            size: Some(5000),
            ..Default::default()
        };

        let mut large = ChangeEvent::new(ChangeEventType::Create, "b", "k", 3);
        large.payload = EventPayload {
            size: Some(50000),
            ..Default::default()
        };

        assert!(!filter.matches(&small));
        assert!(filter.matches(&medium));
        assert!(!filter.matches(&large));
    }

    #[test]
    fn test_subscription_filter_exclusion() {
        let filter = SubscriptionFilter::for_bucket("bucket")
            .excluding(SubscriptionFilter::for_prefix("tmp/"));

        let matching = ChangeEvent::new(ChangeEventType::Create, "bucket", "data/file.txt", 1);
        let excluded = ChangeEvent::new(ChangeEventType::Create, "bucket", "tmp/file.txt", 2);

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&excluded));
    }

    #[tokio::test]
    async fn test_subscription_manager() {
        let manager = SubscriptionManager::new();

        let sub = Subscription::new("test-sub", SubscriptionFilter::all())
            .with_connector("kafka");

        let id = manager.add(sub).await.unwrap();
        assert_eq!(manager.count().await, 1);

        let retrieved = manager.get(&id).await.unwrap();
        assert_eq!(retrieved.name, "test-sub");

        manager.disable(&id).await.unwrap();
        let disabled = manager.get(&id).await.unwrap();
        assert!(!disabled.enabled);

        manager.remove(&id).await.unwrap();
        assert_eq!(manager.count().await, 0);
    }

    #[tokio::test]
    async fn test_find_matching_subscriptions() {
        let manager = SubscriptionManager::new();

        let sub1 = Subscription::new("creates", SubscriptionFilter::for_events(vec![ChangeEventType::Create]));
        let sub2 = Subscription::new("deletes", SubscriptionFilter::for_events(vec![ChangeEventType::Delete]));

        manager.add(sub1).await.unwrap();
        manager.add(sub2).await.unwrap();

        let create_event = ChangeEvent::new(ChangeEventType::Create, "b", "k", 1);
        let matching = manager.find_matching(&create_event).await;

        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].name, "creates");
    }
}
