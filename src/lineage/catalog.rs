// Data Catalog for Lineage

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Catalog configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogConfig {
    /// Enable automatic asset discovery
    pub auto_discovery: bool,
    /// Discovery interval in seconds
    pub discovery_interval_secs: u64,
    /// Enable schema inference
    pub infer_schema: bool,
    /// Maximum metadata size
    pub max_metadata_size: usize,
    /// Enable versioning
    pub versioning: bool,
    /// Retention for deleted assets in days
    pub deleted_retention_days: u32,
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            auto_discovery: true,
            discovery_interval_secs: 3600,
            infer_schema: true,
            max_metadata_size: 1024 * 1024,
            versioning: true,
            deleted_retention_days: 30,
        }
    }
}

/// Asset type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetType {
    /// File
    File,
    /// Directory
    Directory,
    /// Bucket
    Bucket,
    /// Table
    Table,
    /// View
    View,
    /// Dataset
    Dataset,
    /// Pipeline
    Pipeline,
    /// Model
    Model,
    /// Dashboard
    Dashboard,
    /// Report
    Report,
    /// Api
    Api,
    /// Stream
    Stream,
    /// Job
    Job,
    /// Unknown
    Unknown,
}

/// Asset status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetStatus {
    /// Active and available
    Active,
    /// Deprecated but available
    Deprecated,
    /// Archived (read-only)
    Archived,
    /// Deleted
    Deleted,
    /// Draft (not published)
    Draft,
    /// Processing
    Processing,
}

/// Data asset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Asset {
    /// Unique asset ID
    pub id: String,
    /// Asset type
    pub asset_type: AssetType,
    /// Fully qualified name
    pub qualified_name: String,
    /// Display name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Owner
    pub owner: Option<String>,
    /// Status
    pub status: AssetStatus,
    /// Tags
    pub tags: Vec<String>,
    /// Labels (key-value pairs)
    pub labels: HashMap<String, String>,
    /// Classification/sensitivity
    pub classification: Option<Classification>,
    /// Schema reference
    pub schema_id: Option<String>,
    /// Location/path
    pub location: String,
    /// Format
    pub format: Option<String>,
    /// Size in bytes
    pub size_bytes: Option<u64>,
    /// Row count (for tabular data)
    pub row_count: Option<u64>,
    /// Column count
    pub column_count: Option<u32>,
    /// Custom properties
    pub properties: HashMap<String, serde_json::Value>,
    /// Creation time
    pub created_at: u64,
    /// Last modified time
    pub updated_at: u64,
    /// Created by
    pub created_by: Option<String>,
    /// Updated by
    pub updated_by: Option<String>,
    /// Version
    pub version: u64,
}

impl Asset {
    /// Creates a new asset
    pub fn new(id: impl Into<String>, asset_type: AssetType, name: impl Into<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id: id.into(),
            asset_type,
            qualified_name: String::new(),
            name: name.into(),
            description: None,
            owner: None,
            status: AssetStatus::Active,
            tags: Vec::new(),
            labels: HashMap::new(),
            classification: None,
            schema_id: None,
            location: String::new(),
            format: None,
            size_bytes: None,
            row_count: None,
            column_count: None,
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
            created_by: None,
            updated_by: None,
            version: 1,
        }
    }

    /// Sets description
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Sets owner
    pub fn with_owner(mut self, owner: &str) -> Self {
        self.owner = Some(owner.to_string());
        self
    }

    /// Sets location
    pub fn with_location(mut self, location: &str) -> Self {
        self.location = location.to_string();
        self
    }

    /// Adds a tag
    pub fn with_tag(mut self, tag: &str) -> Self {
        self.tags.push(tag.to_string());
        self
    }

    /// Adds a label
    pub fn with_label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.to_string(), value.to_string());
        self
    }

    /// Sets classification
    pub fn with_classification(mut self, classification: Classification) -> Self {
        self.classification = Some(classification);
        self
    }

    /// Adds a custom property
    pub fn with_property(mut self, key: &str, value: serde_json::Value) -> Self {
        self.properties.insert(key.to_string(), value);
        self
    }

    /// Generates qualified name
    pub fn generate_qualified_name(&mut self, namespace: &str) {
        self.qualified_name = format!("{}.{}.{}", namespace, self.asset_type_str(), self.name);
    }

    fn asset_type_str(&self) -> &str {
        match self.asset_type {
            AssetType::File => "file",
            AssetType::Directory => "directory",
            AssetType::Bucket => "bucket",
            AssetType::Table => "table",
            AssetType::View => "view",
            AssetType::Dataset => "dataset",
            AssetType::Pipeline => "pipeline",
            AssetType::Model => "model",
            AssetType::Dashboard => "dashboard",
            AssetType::Report => "report",
            AssetType::Api => "api",
            AssetType::Stream => "stream",
            AssetType::Job => "job",
            AssetType::Unknown => "unknown",
        }
    }
}

/// Data classification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Classification {
    /// Classification level
    pub level: ClassificationLevel,
    /// Data categories
    pub categories: Vec<DataCategory>,
    /// Compliance requirements
    pub compliance: Vec<String>,
    /// Retention policy
    pub retention_days: Option<u32>,
}

/// Classification level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClassificationLevel {
    Public,
    Internal,
    Confidential,
    Restricted,
    TopSecret,
}

/// Data category for classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataCategory {
    /// Personal Identifiable Information
    Pii,
    /// Protected Health Information
    Phi,
    /// Payment Card Industry
    Pci,
    /// Financial data
    Financial,
    /// Credentials/secrets
    Credentials,
    /// Intellectual property
    Ip,
    /// Business sensitive
    BusinessSensitive,
    /// Public data
    Public,
}

/// Data catalog
pub struct Catalog {
    /// Configuration
    config: CatalogConfig,
    /// Assets by ID
    assets: Arc<RwLock<HashMap<String, Asset>>>,
    /// Assets by qualified name
    by_name: Arc<RwLock<HashMap<String, String>>>,
    /// Assets by type
    by_type: Arc<RwLock<HashMap<AssetType, Vec<String>>>>,
    /// Assets by tag
    by_tag: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Asset versions
    versions: Arc<RwLock<HashMap<String, Vec<Asset>>>>,
    /// Statistics
    stats: Arc<CatalogStats>,
}

/// Catalog statistics
pub struct CatalogStats {
    /// Total assets
    pub total_assets: AtomicU64,
    /// Assets by type counts
    pub assets_created: AtomicU64,
    /// Assets updated
    pub assets_updated: AtomicU64,
    /// Assets deleted
    pub assets_deleted: AtomicU64,
    /// Searches performed
    pub searches: AtomicU64,
}

impl Default for CatalogStats {
    fn default() -> Self {
        Self {
            total_assets: AtomicU64::new(0),
            assets_created: AtomicU64::new(0),
            assets_updated: AtomicU64::new(0),
            assets_deleted: AtomicU64::new(0),
            searches: AtomicU64::new(0),
        }
    }
}

impl Catalog {
    /// Creates a new catalog
    pub fn new(config: CatalogConfig) -> Self {
        Self {
            config,
            assets: Arc::new(RwLock::new(HashMap::new())),
            by_name: Arc::new(RwLock::new(HashMap::new())),
            by_type: Arc::new(RwLock::new(HashMap::new())),
            by_tag: Arc::new(RwLock::new(HashMap::new())),
            versions: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(CatalogStats::default()),
        }
    }

    /// Registers a new asset
    pub async fn register(&self, asset: Asset) -> Result<String> {
        let id = asset.id.clone();

        // Check for existing asset
        {
            let assets = self.assets.read().await;
            if assets.contains_key(&id) {
                return Err(StrataError::InvalidOperation(
                    format!("Asset {} already exists", id),
                ));
            }
        }

        // Index by qualified name
        if !asset.qualified_name.is_empty() {
            let mut by_name = self.by_name.write().await;
            by_name.insert(asset.qualified_name.clone(), id.clone());
        }

        // Index by type
        {
            let mut by_type = self.by_type.write().await;
            by_type
                .entry(asset.asset_type)
                .or_insert_with(Vec::new)
                .push(id.clone());
        }

        // Index by tags
        {
            let mut by_tag = self.by_tag.write().await;
            for tag in &asset.tags {
                by_tag
                    .entry(tag.clone())
                    .or_insert_with(Vec::new)
                    .push(id.clone());
            }
        }

        // Store asset
        {
            let mut assets = self.assets.write().await;
            assets.insert(id.clone(), asset);
        }

        self.stats.total_assets.fetch_add(1, Ordering::Relaxed);
        self.stats.assets_created.fetch_add(1, Ordering::Relaxed);

        Ok(id)
    }

    /// Updates an existing asset
    pub async fn update(&self, asset: Asset) -> Result<()> {
        let id = asset.id.clone();

        let old_asset = {
            let assets = self.assets.read().await;
            assets.get(&id).cloned()
        };

        let old_asset = old_asset.ok_or_else(|| {
            StrataError::NotFound(format!("Asset {} not found", id))
        })?;

        // Save version if versioning enabled
        if self.config.versioning {
            let mut versions = self.versions.write().await;
            versions
                .entry(id.clone())
                .or_insert_with(Vec::new)
                .push(old_asset.clone());
        }

        // Update tag index
        {
            let mut by_tag = self.by_tag.write().await;
            // Remove old tags
            for tag in &old_asset.tags {
                if let Some(ids) = by_tag.get_mut(tag) {
                    ids.retain(|i| i != &id);
                }
            }
            // Add new tags
            for tag in &asset.tags {
                by_tag
                    .entry(tag.clone())
                    .or_insert_with(Vec::new)
                    .push(id.clone());
            }
        }

        // Update asset
        {
            let mut assets = self.assets.write().await;
            assets.insert(id, asset);
        }

        self.stats.assets_updated.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Deletes an asset
    pub async fn delete(&self, id: &str) -> Result<bool> {
        let asset = {
            let mut assets = self.assets.write().await;
            assets.remove(id)
        };

        let asset = match asset {
            Some(a) => a,
            None => return Ok(false),
        };

        // Remove from qualified name index
        if !asset.qualified_name.is_empty() {
            let mut by_name = self.by_name.write().await;
            by_name.remove(&asset.qualified_name);
        }

        // Remove from type index
        {
            let mut by_type = self.by_type.write().await;
            if let Some(ids) = by_type.get_mut(&asset.asset_type) {
                ids.retain(|i| i != id);
            }
        }

        // Remove from tag index
        {
            let mut by_tag = self.by_tag.write().await;
            for tag in &asset.tags {
                if let Some(ids) = by_tag.get_mut(tag) {
                    ids.retain(|i| i != id);
                }
            }
        }

        self.stats.total_assets.fetch_sub(1, Ordering::Relaxed);
        self.stats.assets_deleted.fetch_add(1, Ordering::Relaxed);

        Ok(true)
    }

    /// Gets an asset by ID
    pub async fn get(&self, id: &str) -> Option<Asset> {
        let assets = self.assets.read().await;
        assets.get(id).cloned()
    }

    /// Gets an asset by qualified name
    pub async fn get_by_name(&self, name: &str) -> Option<Asset> {
        let by_name = self.by_name.read().await;
        if let Some(id) = by_name.get(name) {
            let assets = self.assets.read().await;
            return assets.get(id).cloned();
        }
        None
    }

    /// Lists assets by type
    pub async fn list_by_type(&self, asset_type: AssetType) -> Vec<Asset> {
        let by_type = self.by_type.read().await;
        let assets = self.assets.read().await;

        by_type
            .get(&asset_type)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| assets.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Lists assets by tag
    pub async fn list_by_tag(&self, tag: &str) -> Vec<Asset> {
        let by_tag = self.by_tag.read().await;
        let assets = self.assets.read().await;

        by_tag
            .get(tag)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| assets.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Searches assets
    pub async fn search(&self, query: &SearchQuery) -> Vec<Asset> {
        self.stats.searches.fetch_add(1, Ordering::Relaxed);

        let assets = self.assets.read().await;
        let mut results: Vec<Asset> = assets
            .values()
            .filter(|a| self.matches_query(a, query))
            .cloned()
            .collect();

        // Sort by relevance or specified field
        match &query.sort_by {
            Some(field) => {
                results.sort_by(|a, b| {
                    let a_val = a.properties.get(field).map(|v| v.to_string());
                    let b_val = b.properties.get(field).map(|v| v.to_string());
                    match query.sort_order {
                        SortOrder::Asc => a_val.cmp(&b_val),
                        SortOrder::Desc => b_val.cmp(&a_val),
                    }
                });
            }
            None => {
                results.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
            }
        }

        // Apply pagination
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(100);

        results.into_iter().skip(offset).take(limit).collect()
    }

    fn matches_query(&self, asset: &Asset, query: &SearchQuery) -> bool {
        // Text search
        if let Some(ref text) = query.text {
            let text_lower = text.to_lowercase();
            let matches = asset.name.to_lowercase().contains(&text_lower)
                || asset.description.as_ref().map(|d| d.to_lowercase().contains(&text_lower)).unwrap_or(false)
                || asset.qualified_name.to_lowercase().contains(&text_lower);
            if !matches {
                return false;
            }
        }

        // Type filter
        if let Some(ref types) = query.types {
            if !types.contains(&asset.asset_type) {
                return false;
            }
        }

        // Tag filter
        if let Some(ref tags) = query.tags {
            if !tags.iter().any(|t| asset.tags.contains(t)) {
                return false;
            }
        }

        // Owner filter
        if let Some(ref owner) = query.owner {
            if asset.owner.as_ref() != Some(owner) {
                return false;
            }
        }

        // Status filter
        if let Some(ref statuses) = query.statuses {
            if !statuses.contains(&asset.status) {
                return false;
            }
        }

        // Classification filter
        if let Some(ref level) = query.min_classification {
            if let Some(ref class) = asset.classification {
                if class.level < *level {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    /// Gets asset versions
    pub async fn get_versions(&self, id: &str) -> Vec<Asset> {
        let versions = self.versions.read().await;
        versions.get(id).cloned().unwrap_or_default()
    }

    /// Gets statistics
    pub fn stats(&self) -> CatalogStatsSnapshot {
        CatalogStatsSnapshot {
            total_assets: self.stats.total_assets.load(Ordering::Relaxed),
            assets_created: self.stats.assets_created.load(Ordering::Relaxed),
            assets_updated: self.stats.assets_updated.load(Ordering::Relaxed),
            assets_deleted: self.stats.assets_deleted.load(Ordering::Relaxed),
            searches: self.stats.searches.load(Ordering::Relaxed),
        }
    }

    /// Gets asset count
    pub async fn count(&self) -> usize {
        self.assets.read().await.len()
    }

    /// Lists all tags
    pub async fn list_tags(&self) -> Vec<(String, usize)> {
        let by_tag = self.by_tag.read().await;
        by_tag
            .iter()
            .map(|(tag, ids)| (tag.clone(), ids.len()))
            .collect()
    }
}

/// Search query
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchQuery {
    /// Text search
    pub text: Option<String>,
    /// Filter by types
    pub types: Option<Vec<AssetType>>,
    /// Filter by tags
    pub tags: Option<Vec<String>>,
    /// Filter by owner
    pub owner: Option<String>,
    /// Filter by statuses
    pub statuses: Option<Vec<AssetStatus>>,
    /// Minimum classification level
    pub min_classification: Option<ClassificationLevel>,
    /// Sort by field
    pub sort_by: Option<String>,
    /// Sort order
    #[serde(default)]
    pub sort_order: SortOrder,
    /// Offset
    pub offset: Option<usize>,
    /// Limit
    pub limit: Option<usize>,
}

/// Sort order
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogStatsSnapshot {
    pub total_assets: u64,
    pub assets_created: u64,
    pub assets_updated: u64,
    pub assets_deleted: u64,
    pub searches: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_catalog_crud() {
        let catalog = Catalog::new(CatalogConfig::default());

        // Create asset
        let asset = Asset::new("asset-1", AssetType::File, "test.csv")
            .with_description("Test file")
            .with_owner("alice")
            .with_tag("test")
            .with_location("/data/test.csv");

        let id = catalog.register(asset).await.unwrap();
        assert_eq!(id, "asset-1");

        // Get asset
        let retrieved = catalog.get("asset-1").await.unwrap();
        assert_eq!(retrieved.name, "test.csv");

        // Update asset
        let mut updated = retrieved.clone();
        updated.description = Some("Updated description".to_string());
        catalog.update(updated).await.unwrap();

        let retrieved = catalog.get("asset-1").await.unwrap();
        assert_eq!(retrieved.description, Some("Updated description".to_string()));

        // Delete asset
        let deleted = catalog.delete("asset-1").await.unwrap();
        assert!(deleted);

        assert!(catalog.get("asset-1").await.is_none());
    }

    #[tokio::test]
    async fn test_catalog_search() {
        let catalog = Catalog::new(CatalogConfig::default());

        catalog.register(
            Asset::new("1", AssetType::File, "users.csv")
                .with_tag("production")
                .with_owner("alice")
        ).await.unwrap();

        catalog.register(
            Asset::new("2", AssetType::Table, "users_table")
                .with_tag("production")
                .with_owner("bob")
        ).await.unwrap();

        catalog.register(
            Asset::new("3", AssetType::File, "orders.csv")
                .with_tag("staging")
                .with_owner("alice")
        ).await.unwrap();

        // Search by text
        let results = catalog.search(&SearchQuery {
            text: Some("users".to_string()),
            ..Default::default()
        }).await;
        assert_eq!(results.len(), 2);

        // Search by type
        let results = catalog.search(&SearchQuery {
            types: Some(vec![AssetType::File]),
            ..Default::default()
        }).await;
        assert_eq!(results.len(), 2);

        // Search by tag
        let results = catalog.search(&SearchQuery {
            tags: Some(vec!["production".to_string()]),
            ..Default::default()
        }).await;
        assert_eq!(results.len(), 2);

        // Search by owner
        let results = catalog.search(&SearchQuery {
            owner: Some("alice".to_string()),
            ..Default::default()
        }).await;
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_catalog_versioning() {
        let catalog = Catalog::new(CatalogConfig {
            versioning: true,
            ..Default::default()
        });

        let asset = Asset::new("1", AssetType::File, "test.csv");
        catalog.register(asset).await.unwrap();

        // Update multiple times
        for i in 1..=3 {
            let mut asset = catalog.get("1").await.unwrap();
            asset.description = Some(format!("Version {}", i));
            asset.version = i as u64 + 1;
            catalog.update(asset).await.unwrap();
        }

        let versions = catalog.get_versions("1").await;
        assert_eq!(versions.len(), 3);
    }
}
