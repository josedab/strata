// Tiering engine - orchestrates automatic data placement

use crate::error::{Result, StrataError};
use crate::types::ChunkId;
use super::migration::{MigrationJob, MigrationManager, MigrationConfig, TierStorage};
use super::policy::{TieringPolicy, TierType, DataStats};
use super::tracker::{AccessTracker, TrackerConfig, ObjectAccessStats};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Tiering engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringEngineConfig {
    /// Enable automatic tiering
    pub enabled: bool,
    /// Evaluation interval
    #[serde(with = "crate::config::humantime_serde")]
    pub evaluation_interval: Duration,
    /// Maximum objects to evaluate per cycle
    pub max_objects_per_cycle: usize,
    /// Minimum confidence for migration
    pub min_confidence: f64,
    /// Enable ML-based predictions
    pub enable_ml_predictions: bool,
    /// Migration configuration
    pub migration: MigrationConfig,
    /// Tracker configuration
    pub tracker: TrackerConfig,
    /// Tier capacity thresholds (trigger migrations when exceeded)
    pub capacity_thresholds: HashMap<TierType, f64>,
    /// Dry run mode (evaluate but don't migrate)
    pub dry_run: bool,
}

impl Default for TieringEngineConfig {
    fn default() -> Self {
        let mut thresholds = HashMap::new();
        thresholds.insert(TierType::Hot, 0.85);
        thresholds.insert(TierType::Warm, 0.90);
        thresholds.insert(TierType::Cold, 0.95);
        thresholds.insert(TierType::Archive, 0.99);

        Self {
            enabled: true,
            evaluation_interval: Duration::from_secs(3600),
            max_objects_per_cycle: 10000,
            min_confidence: 0.6,
            enable_ml_predictions: false,
            migration: MigrationConfig::default(),
            tracker: TrackerConfig::default(),
            capacity_thresholds: thresholds,
            dry_run: false,
        }
    }
}

/// Object tier assignment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectTierInfo {
    pub path: String,
    pub current_tier: TierType,
    pub size: u64,
    pub chunk_ids: Vec<ChunkId>,
    pub created_at: SystemTime,
    pub last_access: SystemTime,
    pub last_evaluated: Option<SystemTime>,
    pub locked: bool, // Prevents automatic tiering
}

/// Tiering engine statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TieringStats {
    pub objects_evaluated: u64,
    pub objects_migrated: u64,
    pub bytes_migrated: u64,
    pub promotions: u64,
    pub demotions: u64,
    pub skipped_low_confidence: u64,
    pub skipped_locked: u64,
    pub evaluation_time_ms: u64,
    pub last_evaluation: Option<SystemTime>,
    pub tier_distribution: HashMap<TierType, TierStats>,
}

/// Per-tier statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TierStats {
    pub object_count: u64,
    pub total_bytes: u64,
    pub capacity_used_percent: f64,
}

/// Tiering engine
pub struct TieringEngine {
    config: TieringEngineConfig,
    policies: Arc<RwLock<HashMap<String, TieringPolicy>>>,
    tracker: Arc<AccessTracker>,
    migration_manager: Arc<MigrationManager>,
    objects: Arc<RwLock<HashMap<String, ObjectTierInfo>>>,
    stats: Arc<RwLock<TieringStats>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl TieringEngine {
    /// Creates a new tiering engine
    pub fn new(config: TieringEngineConfig) -> Self {
        let tracker = Arc::new(AccessTracker::new(config.tracker.clone()));
        let migration_manager = Arc::new(MigrationManager::new(config.migration.clone()));
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);

        let mut policies = HashMap::new();
        policies.insert("default".to_string(), TieringPolicy::default());

        Self {
            config,
            policies: Arc::new(RwLock::new(policies)),
            tracker,
            migration_manager,
            objects: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(TieringStats::default())),
            shutdown,
            shutdown_rx,
        }
    }

    /// Gets the access tracker
    pub fn tracker(&self) -> Arc<AccessTracker> {
        Arc::clone(&self.tracker)
    }

    /// Gets the migration manager
    pub fn migration_manager(&self) -> Arc<MigrationManager> {
        Arc::clone(&self.migration_manager)
    }

    /// Registers a tiering policy
    pub async fn register_policy(&self, name: &str, policy: TieringPolicy) {
        let mut policies = self.policies.write().await;
        policies.insert(name.to_string(), policy);
        info!(name = %name, "Registered tiering policy");
    }

    /// Registers a tier storage backend
    pub async fn register_tier(&self, storage: Arc<dyn TierStorage>) {
        self.migration_manager.register_tier(storage).await;
    }

    /// Registers an object for tiering
    pub async fn register_object(&self, info: ObjectTierInfo) {
        let mut objects = self.objects.write().await;
        objects.insert(info.path.clone(), info);
    }

    /// Updates object location after migration
    pub async fn update_object_tier(&self, path: &str, new_tier: TierType) -> Result<()> {
        let mut objects = self.objects.write().await;
        let obj = objects.get_mut(path)
            .ok_or_else(|| StrataError::NotFound(path.to_string()))?;
        obj.current_tier = new_tier;
        obj.last_evaluated = Some(SystemTime::now());
        Ok(())
    }

    /// Locks an object to prevent automatic tiering
    pub async fn lock_object(&self, path: &str) -> Result<()> {
        let mut objects = self.objects.write().await;
        let obj = objects.get_mut(path)
            .ok_or_else(|| StrataError::NotFound(path.to_string()))?;
        obj.locked = true;
        Ok(())
    }

    /// Unlocks an object for automatic tiering
    pub async fn unlock_object(&self, path: &str) -> Result<()> {
        let mut objects = self.objects.write().await;
        let obj = objects.get_mut(path)
            .ok_or_else(|| StrataError::NotFound(path.to_string()))?;
        obj.locked = false;
        Ok(())
    }

    /// Manually triggers migration
    pub async fn migrate_object(
        &self,
        path: &str,
        target_tier: TierType,
    ) -> Result<String> {
        let objects = self.objects.read().await;
        let obj = objects.get(path)
            .ok_or_else(|| StrataError::NotFound(path.to_string()))?;

        if obj.current_tier == target_tier {
            return Err(StrataError::InvalidOperation(
                "Object already in target tier".to_string()
            ));
        }

        let job = MigrationJob::new(
            path.to_string(),
            obj.current_tier,
            target_tier,
            obj.size,
            obj.chunk_ids.clone(),
        );

        self.migration_manager.queue_migration(job).await
    }

    /// Evaluates tiering for all objects
    pub async fn evaluate_all(&self) -> Result<EvaluationResult> {
        let start = std::time::Instant::now();
        let mut result = EvaluationResult::default();

        let objects = self.objects.read().await;
        let policies = self.policies.read().await;
        let default_policy = policies.get("default").cloned()
            .unwrap_or_else(TieringPolicy::default);

        for (path, obj) in objects.iter().take(self.config.max_objects_per_cycle) {
            if obj.locked {
                result.skipped_locked += 1;
                continue;
            }

            // Get access stats
            let access_stats = self.tracker.get_stats(path).await;

            // Build data stats
            let data_stats = self.build_data_stats(obj, access_stats.as_ref());

            // Evaluate policy
            let evaluation = default_policy.evaluate(&data_stats);
            result.evaluated += 1;

            if evaluation.should_migrate(obj.current_tier) {
                if evaluation.confidence < self.config.min_confidence {
                    result.skipped_low_confidence += 1;
                    continue;
                }

                result.recommendations.push(MigrationRecommendation {
                    path: path.clone(),
                    current_tier: obj.current_tier,
                    recommended_tier: evaluation.recommended_tier,
                    reason: evaluation.reason,
                    confidence: evaluation.confidence,
                    size: obj.size,
                });
            }
        }

        result.duration_ms = start.elapsed().as_millis() as u64;

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.objects_evaluated += result.evaluated;
            stats.skipped_low_confidence += result.skipped_low_confidence;
            stats.skipped_locked += result.skipped_locked;
            stats.evaluation_time_ms = result.duration_ms;
            stats.last_evaluation = Some(SystemTime::now());
        }

        info!(
            evaluated = result.evaluated,
            recommendations = result.recommendations.len(),
            duration_ms = result.duration_ms,
            "Tiering evaluation complete"
        );

        Ok(result)
    }

    /// Builds data stats from object info and access stats
    fn build_data_stats(&self, obj: &ObjectTierInfo, access: Option<&ObjectAccessStats>) -> DataStats {
        let now = SystemTime::now();
        let age = now.duration_since(obj.created_at).unwrap_or(Duration::from_secs(0));
        let time_since_last_access = now.duration_since(obj.last_access).unwrap_or(Duration::from_secs(0));

        DataStats {
            path: obj.path.clone(),
            size: obj.size,
            current_tier: obj.current_tier,
            age,
            time_since_last_access,
            access_history: access.map(|a| {
                a.recent_accesses.iter().map(|r| r.timestamp).collect()
            }).unwrap_or_default(),
            content_type: None,
            tags: HashMap::new(),
            version_count: 1,
            is_compressed: false,
            is_encrypted: false,
        }
    }

    /// Executes recommended migrations
    pub async fn execute_recommendations(&self, recommendations: Vec<MigrationRecommendation>) -> Result<Vec<String>> {
        if self.config.dry_run {
            info!(count = recommendations.len(), "Dry run - skipping migrations");
            return Ok(vec![]);
        }

        let mut job_ids = Vec::new();
        let objects = self.objects.read().await;

        for rec in recommendations {
            if let Some(obj) = objects.get(&rec.path) {
                let job = MigrationJob::new(
                    rec.path.clone(),
                    rec.current_tier,
                    rec.recommended_tier,
                    rec.size,
                    obj.chunk_ids.clone(),
                );

                match self.migration_manager.queue_migration(job).await {
                    Ok(id) => job_ids.push(id),
                    Err(e) => warn!(path = %rec.path, error = %e, "Failed to queue migration"),
                }
            }
        }

        Ok(job_ids)
    }

    /// Runs the tiering engine background loop
    pub async fn run(&self) {
        if !self.config.enabled {
            info!("Tiering engine disabled");
            return;
        }

        info!("Starting tiering engine");

        let mut interval = tokio::time::interval(self.config.evaluation_interval);
        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Tiering engine shutting down");
                        break;
                    }
                }
                _ = interval.tick() => {
                    match self.run_cycle().await {
                        Ok(()) => debug!("Tiering cycle completed"),
                        Err(e) => error!(error = %e, "Tiering cycle failed"),
                    }
                }
            }
        }
    }

    /// Runs a single tiering cycle
    async fn run_cycle(&self) -> Result<()> {
        // Check tier capacities and force demotions if needed
        self.check_tier_capacities().await?;

        // Evaluate all objects
        let result = self.evaluate_all().await?;

        // Execute migrations
        if !result.recommendations.is_empty() {
            self.execute_recommendations(result.recommendations).await?;
        }

        Ok(())
    }

    /// Checks tier capacities and triggers demotions if needed
    async fn check_tier_capacities(&self) -> Result<()> {
        // This would integrate with actual tier storage backends
        // For now, just a placeholder
        Ok(())
    }

    /// Gets current statistics
    pub async fn get_stats(&self) -> TieringStats {
        self.stats.read().await.clone()
    }

    /// Gets tier distribution
    pub async fn get_tier_distribution(&self) -> HashMap<TierType, TierStats> {
        let objects = self.objects.read().await;
        let mut distribution: HashMap<TierType, TierStats> = HashMap::new();

        for obj in objects.values() {
            let entry = distribution.entry(obj.current_tier).or_default();
            entry.object_count += 1;
            entry.total_bytes += obj.size;
        }

        distribution
    }

    /// Shuts down the engine
    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        self.migration_manager.shutdown();
    }

    /// Estimates cost savings from tiering
    pub async fn estimate_savings(&self) -> CostEstimate {
        let objects = self.objects.read().await;

        let mut current_cost = 0.0;
        let mut optimal_cost = 0.0;

        for obj in objects.values() {
            let size_gb = obj.size as f64 / (1024.0 * 1024.0 * 1024.0);
            current_cost += size_gb * obj.current_tier.cost_factor();

            // Estimate optimal tier based on access patterns
            // For now, use simplified heuristic
            if let Some(access) = self.tracker.get_stats(&obj.path).await {
                let optimal_tier = if access.heat_score > 10.0 {
                    TierType::Hot
                } else if access.heat_score > 1.0 {
                    TierType::Warm
                } else if access.heat_score > 0.1 {
                    TierType::Cold
                } else {
                    TierType::Archive
                };
                optimal_cost += size_gb * optimal_tier.cost_factor();
            } else {
                optimal_cost += size_gb * TierType::Cold.cost_factor();
            }
        }

        CostEstimate {
            current_monthly_cost: current_cost,
            optimal_monthly_cost: optimal_cost,
            potential_savings: current_cost - optimal_cost,
            savings_percent: if current_cost > 0.0 {
                ((current_cost - optimal_cost) / current_cost) * 100.0
            } else {
                0.0
            },
        }
    }
}

/// Result of tiering evaluation
#[derive(Debug, Clone, Default)]
pub struct EvaluationResult {
    pub evaluated: u64,
    pub skipped_locked: u64,
    pub skipped_low_confidence: u64,
    pub recommendations: Vec<MigrationRecommendation>,
    pub duration_ms: u64,
}

/// Migration recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRecommendation {
    pub path: String,
    pub current_tier: TierType,
    pub recommended_tier: TierType,
    pub reason: String,
    pub confidence: f64,
    pub size: u64,
}

/// Cost estimate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    pub current_monthly_cost: f64,
    pub optimal_monthly_cost: f64,
    pub potential_savings: f64,
    pub savings_percent: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_creation() {
        let config = TieringEngineConfig::default();
        let engine = TieringEngine::new(config);

        let stats = engine.get_stats().await;
        assert_eq!(stats.objects_evaluated, 0);
    }

    #[tokio::test]
    async fn test_object_registration() {
        let engine = TieringEngine::new(TieringEngineConfig::default());

        let obj = ObjectTierInfo {
            path: "/test/file.txt".to_string(),
            current_tier: TierType::Hot,
            size: 1024,
            chunk_ids: vec![ChunkId::new()],
            created_at: SystemTime::now(),
            last_access: SystemTime::now(),
            last_evaluated: None,
            locked: false,
        };

        engine.register_object(obj).await;

        let distribution = engine.get_tier_distribution().await;
        assert_eq!(distribution.get(&TierType::Hot).unwrap().object_count, 1);
    }

    #[tokio::test]
    async fn test_policy_registration() {
        let engine = TieringEngine::new(TieringEngineConfig::default());

        engine.register_policy("cost", TieringPolicy::cost_optimized()).await;
        engine.register_policy("perf", TieringPolicy::performance_optimized()).await;

        // Policies should be registered
        let policies = engine.policies.read().await;
        assert!(policies.contains_key("cost"));
        assert!(policies.contains_key("perf"));
    }

    #[tokio::test]
    async fn test_object_locking() {
        let engine = TieringEngine::new(TieringEngineConfig::default());

        let obj = ObjectTierInfo {
            path: "/locked/file.txt".to_string(),
            current_tier: TierType::Hot,
            size: 1024,
            chunk_ids: vec![],
            created_at: SystemTime::now(),
            last_access: SystemTime::now(),
            last_evaluated: None,
            locked: false,
        };

        engine.register_object(obj).await;
        engine.lock_object("/locked/file.txt").await.unwrap();

        let result = engine.evaluate_all().await.unwrap();
        assert_eq!(result.skipped_locked, 1);
    }

    #[tokio::test]
    async fn test_cost_estimation() {
        let engine = TieringEngine::new(TieringEngineConfig::default());

        // Register objects in different tiers
        for i in 0..10 {
            let obj = ObjectTierInfo {
                path: format!("/file{}.txt", i),
                current_tier: TierType::Hot, // All on expensive tier
                size: 1024 * 1024 * 1024, // 1 GB each
                chunk_ids: vec![],
                created_at: SystemTime::now() - Duration::from_secs(90 * 24 * 3600),
                last_access: SystemTime::now() - Duration::from_secs(60 * 24 * 3600),
                last_evaluated: None,
                locked: false,
            };
            engine.register_object(obj).await;
        }

        let estimate = engine.estimate_savings().await;
        assert!(estimate.potential_savings > 0.0);
        assert!(estimate.savings_percent > 0.0);
    }
}
