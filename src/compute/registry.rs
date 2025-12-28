// Model Registry for ML Model Serving

use super::model::{Model, ModelConfig, ModelFormat, ModelMetadata};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Registry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    /// Storage path for models
    pub storage_path: String,
    /// Maximum model size in bytes
    pub max_model_size: u64,
    /// Enable versioning
    pub versioning: bool,
    /// Maximum versions to keep
    pub max_versions: usize,
    /// Enable model validation
    pub validate_on_upload: bool,
    /// Enable model signing
    pub sign_models: bool,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            storage_path: "/data/models".to_string(),
            max_model_size: 10 * 1024 * 1024 * 1024, // 10GB
            versioning: true,
            max_versions: 10,
            validate_on_upload: true,
            sign_models: false,
        }
    }
}

/// Model version in registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelVersion {
    /// Version number
    pub version: u32,
    /// Model configuration
    pub config: ModelConfig,
    /// Model metadata
    pub metadata: ModelMetadata,
    /// Storage path
    pub storage_path: String,
    /// Status
    pub status: ModelStatus,
    /// Created timestamp
    pub created_at: u64,
    /// Deployed timestamp
    pub deployed_at: Option<u64>,
    /// Deprecated timestamp
    pub deprecated_at: Option<u64>,
}

/// Model status in registry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelStatus {
    /// Uploading
    Uploading,
    /// Validating
    Validating,
    /// Ready for deployment
    Ready,
    /// Currently deployed
    Deployed,
    /// Deprecated
    Deprecated,
    /// Archived
    Archived,
    /// Failed validation
    Failed,
}

/// Model registry
pub struct ModelRegistry {
    /// Configuration
    config: RegistryConfig,
    /// Models by name
    models: Arc<RwLock<HashMap<String, Vec<ModelVersion>>>>,
    /// Latest version mapping
    latest: Arc<RwLock<HashMap<String, u32>>>,
    /// Deployed versions
    deployed: Arc<RwLock<HashMap<String, u32>>>,
    /// Statistics
    stats: Arc<RegistryStats>,
}

/// Registry statistics
pub struct RegistryStats {
    /// Total models registered
    pub models_registered: AtomicU64,
    /// Total versions
    pub total_versions: AtomicU64,
    /// Models deployed
    pub models_deployed: AtomicU64,
    /// Total storage used
    pub storage_bytes: AtomicU64,
    /// Downloads
    pub downloads: AtomicU64,
}

impl Default for RegistryStats {
    fn default() -> Self {
        Self {
            models_registered: AtomicU64::new(0),
            total_versions: AtomicU64::new(0),
            models_deployed: AtomicU64::new(0),
            storage_bytes: AtomicU64::new(0),
            downloads: AtomicU64::new(0),
        }
    }
}

impl ModelRegistry {
    /// Creates a new registry
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            config,
            models: Arc::new(RwLock::new(HashMap::new())),
            latest: Arc::new(RwLock::new(HashMap::new())),
            deployed: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RegistryStats::default()),
        }
    }

    /// Registers a new model version
    pub async fn register(&self, model: Model) -> Result<u32> {
        let name = model.metadata.name.clone();
        let size = model.metadata.size_bytes;

        // Check size limit
        if size > self.config.max_model_size {
            return Err(StrataError::InvalidOperation(
                format!("Model exceeds maximum size: {} > {}", size, self.config.max_model_size)
            ));
        }

        let mut models = self.models.write().await;
        let versions = models.entry(name.clone()).or_insert_with(Vec::new);

        // Determine version number
        let version_num = versions.len() as u32 + 1;

        // Create storage path
        let storage_path = format!(
            "{}/{}/v{}",
            self.config.storage_path, name, version_num
        );

        let version = ModelVersion {
            version: version_num,
            config: model.config,
            metadata: model.metadata,
            storage_path,
            status: ModelStatus::Ready,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            deployed_at: None,
            deprecated_at: None,
        };

        versions.push(version);

        // Enforce max versions
        if versions.len() > self.config.max_versions {
            let to_archive = versions.len() - self.config.max_versions;
            for i in 0..to_archive {
                versions[i].status = ModelStatus::Archived;
            }
        }

        drop(models);

        // Update latest
        {
            let mut latest = self.latest.write().await;
            latest.insert(name, version_num);
        }

        self.stats.total_versions.fetch_add(1, Ordering::Relaxed);
        if version_num == 1 {
            self.stats.models_registered.fetch_add(1, Ordering::Relaxed);
        }
        self.stats.storage_bytes.fetch_add(size, Ordering::Relaxed);

        Ok(version_num)
    }

    /// Gets a model version
    pub async fn get(&self, name: &str, version: Option<u32>) -> Option<ModelVersion> {
        let models = self.models.read().await;

        if let Some(versions) = models.get(name) {
            let target_version = match version {
                Some(v) => v,
                None => {
                    let latest = self.latest.read().await;
                    *latest.get(name)?
                }
            };

            return versions
                .iter()
                .find(|v| v.version == target_version)
                .cloned();
        }

        None
    }

    /// Gets latest version
    pub async fn get_latest(&self, name: &str) -> Option<ModelVersion> {
        self.get(name, None).await
    }

    /// Lists all model names
    pub async fn list_models(&self) -> Vec<String> {
        let models = self.models.read().await;
        models.keys().cloned().collect()
    }

    /// Lists versions for a model
    pub async fn list_versions(&self, name: &str) -> Vec<ModelVersion> {
        let models = self.models.read().await;
        models.get(name).cloned().unwrap_or_default()
    }

    /// Deploys a model version
    pub async fn deploy(&self, name: &str, version: u32) -> Result<()> {
        let mut models = self.models.write().await;

        let versions = models.get_mut(name).ok_or_else(|| {
            StrataError::NotFound(format!("Model {} not found", name))
        })?;

        // Find the version index and validate status
        let version_idx = versions.iter().position(|v| v.version == version).ok_or_else(|| {
            StrataError::NotFound(format!("Version {} not found", version))
        })?;

        let status = versions[version_idx].status;
        if status != ModelStatus::Ready && status != ModelStatus::Deployed {
            return Err(StrataError::InvalidOperation(
                format!("Cannot deploy model with status {:?}", status)
            ));
        }

        // Undeploy current version
        let deployed = self.deployed.read().await;
        let current_deployed = deployed.get(name).copied();
        drop(deployed);

        if let Some(current) = current_deployed {
            if let Some(current_version) = versions.iter_mut().find(|v| v.version == current) {
                current_version.status = ModelStatus::Ready;
                current_version.deployed_at = None;
            }
        }

        // Deploy new version
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        versions[version_idx].status = ModelStatus::Deployed;
        versions[version_idx].deployed_at = Some(now);

        drop(models);

        // Update deployed mapping
        {
            let mut deployed = self.deployed.write().await;
            deployed.insert(name.to_string(), version);
        }

        self.stats.models_deployed.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Undeploys a model
    pub async fn undeploy(&self, name: &str) -> Result<bool> {
        let mut deployed = self.deployed.write().await;

        if let Some(version) = deployed.remove(name) {
            let mut models = self.models.write().await;
            if let Some(versions) = models.get_mut(name) {
                if let Some(v) = versions.iter_mut().find(|v| v.version == version) {
                    v.status = ModelStatus::Ready;
                    v.deployed_at = None;
                }
            }
            self.stats.models_deployed.fetch_sub(1, Ordering::Relaxed);
            return Ok(true);
        }

        Ok(false)
    }

    /// Deprecates a model version
    pub async fn deprecate(&self, name: &str, version: u32) -> Result<()> {
        let mut models = self.models.write().await;

        let versions = models.get_mut(name).ok_or_else(|| {
            StrataError::NotFound(format!("Model {} not found", name))
        })?;

        let version_entry = versions.iter_mut().find(|v| v.version == version).ok_or_else(|| {
            StrataError::NotFound(format!("Version {} not found", version))
        })?;

        version_entry.status = ModelStatus::Deprecated;
        version_entry.deprecated_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        );

        Ok(())
    }

    /// Deletes a model version
    pub async fn delete(&self, name: &str, version: u32) -> Result<bool> {
        // Check if deployed
        {
            let deployed = self.deployed.read().await;
            if deployed.get(name) == Some(&version) {
                return Err(StrataError::InvalidOperation(
                    "Cannot delete deployed model".to_string()
                ));
            }
        }

        let mut models = self.models.write().await;

        if let Some(versions) = models.get_mut(name) {
            if let Some(pos) = versions.iter().position(|v| v.version == version) {
                let removed = versions.remove(pos);
                self.stats.storage_bytes.fetch_sub(removed.metadata.size_bytes, Ordering::Relaxed);
                self.stats.total_versions.fetch_sub(1, Ordering::Relaxed);

                // Update latest if needed
                if versions.is_empty() {
                    models.remove(name);
                    let mut latest = self.latest.write().await;
                    latest.remove(name);
                    self.stats.models_registered.fetch_sub(1, Ordering::Relaxed);
                } else {
                    let max_version = versions.iter().map(|v| v.version).max().unwrap_or(0);
                    let mut latest = self.latest.write().await;
                    latest.insert(name.to_string(), max_version);
                }

                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Gets deployed version for a model
    pub async fn get_deployed(&self, name: &str) -> Option<ModelVersion> {
        let deployed = self.deployed.read().await;
        if let Some(&version) = deployed.get(name) {
            return self.get(name, Some(version)).await;
        }
        None
    }

    /// Lists all deployed models
    pub async fn list_deployed(&self) -> Vec<(String, ModelVersion)> {
        let deployed = self.deployed.read().await;
        let mut result = Vec::new();

        for (name, version) in deployed.iter() {
            if let Some(v) = self.get(name, Some(*version)).await {
                result.push((name.clone(), v));
            }
        }

        result
    }

    /// Searches models
    pub async fn search(&self, query: &ModelSearchQuery) -> Vec<ModelVersion> {
        let models = self.models.read().await;
        let mut results = Vec::new();

        for versions in models.values() {
            for version in versions {
                if self.matches_query(version, query) {
                    results.push(version.clone());
                }
            }
        }

        // Sort and limit
        results.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        results.truncate(query.limit.unwrap_or(100));

        results
    }

    fn matches_query(&self, version: &ModelVersion, query: &ModelSearchQuery) -> bool {
        if let Some(ref name) = query.name {
            if !version.metadata.name.contains(name) {
                return false;
            }
        }

        if let Some(ref format) = query.format {
            if version.config.format != *format {
                return false;
            }
        }

        if let Some(ref tags) = query.tags {
            if !tags.iter().any(|t| version.metadata.tags.contains(t)) {
                return false;
            }
        }

        if let Some(ref status) = query.status {
            if version.status != *status {
                return false;
            }
        }

        true
    }

    /// Gets statistics
    pub fn stats(&self) -> RegistryStatsSnapshot {
        RegistryStatsSnapshot {
            models_registered: self.stats.models_registered.load(Ordering::Relaxed),
            total_versions: self.stats.total_versions.load(Ordering::Relaxed),
            models_deployed: self.stats.models_deployed.load(Ordering::Relaxed),
            storage_bytes: self.stats.storage_bytes.load(Ordering::Relaxed),
            downloads: self.stats.downloads.load(Ordering::Relaxed),
        }
    }
}

/// Model search query
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ModelSearchQuery {
    /// Name filter
    pub name: Option<String>,
    /// Format filter
    pub format: Option<ModelFormat>,
    /// Tags filter
    pub tags: Option<Vec<String>>,
    /// Status filter
    pub status: Option<ModelStatus>,
    /// Result limit
    pub limit: Option<usize>,
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryStatsSnapshot {
    pub models_registered: u64,
    pub total_versions: u64,
    pub models_deployed: u64,
    pub storage_bytes: u64,
    pub downloads: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_model(name: &str) -> Model {
        use super::super::model::{ModelBuilder, TensorSpec, DataType};

        ModelBuilder::new(name)
            .format(ModelFormat::Onnx)
            .version("1.0")
            .input(TensorSpec::new("input", DataType::Float32, vec![-1, 3, 224, 224]))
            .output(TensorSpec::new("output", DataType::Float32, vec![-1, 1000]))
            .build()
    }

    #[tokio::test]
    async fn test_registry_register() {
        let registry = ModelRegistry::new(RegistryConfig::default());

        let version = registry.register(test_model("model1")).await.unwrap();
        assert_eq!(version, 1);

        let version = registry.register(test_model("model1")).await.unwrap();
        assert_eq!(version, 2);

        let models = registry.list_models().await;
        assert_eq!(models.len(), 1);
    }

    #[tokio::test]
    async fn test_registry_deploy() {
        let registry = ModelRegistry::new(RegistryConfig::default());

        registry.register(test_model("model1")).await.unwrap();

        registry.deploy("model1", 1).await.unwrap();

        let deployed = registry.get_deployed("model1").await.unwrap();
        assert_eq!(deployed.status, ModelStatus::Deployed);
    }

    #[tokio::test]
    async fn test_registry_versioning() {
        let registry = ModelRegistry::new(RegistryConfig::default());

        for i in 0..5 {
            registry.register(test_model("model1")).await.unwrap();
        }

        let versions = registry.list_versions("model1").await;
        assert_eq!(versions.len(), 5);

        let latest = registry.get_latest("model1").await.unwrap();
        assert_eq!(latest.version, 5);
    }
}
