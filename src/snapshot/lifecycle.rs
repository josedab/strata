//! Snapshot lifecycle management.
//!
//! Provides automated snapshot scheduling, retention policy enforcement,
//! and cleanup of expired snapshots using the scheduling module.

use crate::error::{Result, StrataError};
use crate::scheduling::{CronExpr, JobExecutor, JobScheduler, ScheduledJob};
use crate::snapshot::{
    Snapshot, SnapshotBuilder, SnapshotConfig, SnapshotManager, SnapshotPolicy, SnapshotScope,
    SnapshotStatus,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

/// Lifecycle event types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEvent {
    /// Snapshot created by policy.
    SnapshotCreated {
        policy_id: String,
        snapshot_id: String,
    },
    /// Snapshot deleted due to retention.
    SnapshotDeleted {
        policy_id: String,
        snapshot_id: String,
        reason: RetentionReason,
    },
    /// Policy execution failed.
    PolicyFailed { policy_id: String, error: String },
    /// Retention cleanup completed.
    RetentionCleanup {
        policy_id: String,
        deleted_count: usize,
    },
    /// Expired snapshots cleaned up.
    ExpiredCleanup { deleted_count: usize },
}

/// Reason for retention-based deletion.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetentionReason {
    /// Exceeded retention count.
    CountExceeded { current: usize, max: usize },
    /// Exceeded retention days.
    AgeExceeded { age_days: i64, max_days: u64 },
    /// Manually expired.
    Expired,
}

/// Retention policy configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Maximum number of snapshots to keep per policy.
    pub max_count: Option<usize>,
    /// Maximum age in days.
    pub max_age_days: Option<u64>,
    /// Whether to delete oldest first when count exceeded.
    pub delete_oldest_first: bool,
    /// Minimum snapshots to always keep regardless of age.
    pub min_keep: usize,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_count: Some(10),
            max_age_days: Some(30),
            delete_oldest_first: true,
            min_keep: 1,
        }
    }
}

impl RetentionConfig {
    /// Create a retention config keeping N snapshots.
    pub fn keep_count(count: usize) -> Self {
        Self {
            max_count: Some(count),
            max_age_days: None,
            delete_oldest_first: true,
            min_keep: 1,
        }
    }

    /// Create a retention config keeping snapshots for N days.
    pub fn keep_days(days: u64) -> Self {
        Self {
            max_count: None,
            max_age_days: Some(days),
            delete_oldest_first: true,
            min_keep: 1,
        }
    }

    /// Hourly retention: keep 24 hourly snapshots.
    pub fn hourly() -> Self {
        Self {
            max_count: Some(24),
            max_age_days: Some(2),
            delete_oldest_first: true,
            min_keep: 1,
        }
    }

    /// Daily retention: keep 7 daily snapshots.
    pub fn daily() -> Self {
        Self {
            max_count: Some(7),
            max_age_days: Some(14),
            delete_oldest_first: true,
            min_keep: 1,
        }
    }

    /// Weekly retention: keep 4 weekly snapshots.
    pub fn weekly() -> Self {
        Self {
            max_count: Some(4),
            max_age_days: Some(35),
            delete_oldest_first: true,
            min_keep: 1,
        }
    }

    /// Monthly retention: keep 12 monthly snapshots.
    pub fn monthly() -> Self {
        Self {
            max_count: Some(12),
            max_age_days: Some(400),
            delete_oldest_first: true,
            min_keep: 1,
        }
    }
}

/// Lifecycle manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleConfig {
    /// Whether lifecycle management is enabled.
    pub enabled: bool,
    /// How often to check for expired snapshots (minutes).
    pub cleanup_interval_minutes: u64,
    /// Default retention config for policies without explicit settings.
    pub default_retention: RetentionConfig,
    /// Maximum concurrent snapshot operations.
    pub max_concurrent_snapshots: usize,
    /// Whether to send events to event system.
    pub emit_events: bool,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cleanup_interval_minutes: 60,
            default_retention: RetentionConfig::default(),
            max_concurrent_snapshots: 2,
            emit_events: true,
        }
    }
}

/// Policy execution state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyState {
    /// Policy ID.
    pub policy_id: String,
    /// Last execution time.
    pub last_run: Option<DateTime<Utc>>,
    /// Last execution result.
    pub last_result: Option<PolicyResult>,
    /// Next scheduled run.
    pub next_run: Option<DateTime<Utc>>,
    /// Total successful runs.
    pub success_count: u64,
    /// Total failed runs.
    pub failure_count: u64,
    /// Snapshot IDs created by this policy.
    pub snapshot_ids: Vec<String>,
}

impl PolicyState {
    fn new(policy_id: &str) -> Self {
        Self {
            policy_id: policy_id.to_string(),
            last_run: None,
            last_result: None,
            next_run: None,
            success_count: 0,
            failure_count: 0,
            snapshot_ids: Vec::new(),
        }
    }
}

/// Result of policy execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyResult {
    /// Snapshot created successfully.
    Success { snapshot_id: String },
    /// Snapshot creation failed.
    Failed { error: String },
    /// Policy was skipped (e.g., disabled).
    Skipped { reason: String },
}

/// Job executor for snapshot policies.
struct SnapshotPolicyExecutor {
    policy: SnapshotPolicy,
    manager: Arc<SnapshotManager>,
    state: Arc<RwLock<HashMap<String, PolicyState>>>,
    retention: RetentionConfig,
    event_tx: Option<tokio::sync::mpsc::Sender<LifecycleEvent>>,
}

#[async_trait]
impl JobExecutor for SnapshotPolicyExecutor {
    async fn execute(&self, _job_id: &str) -> std::result::Result<String, String> {
        if !self.policy.enabled {
            return Err("Policy is disabled".to_string());
        }

        // Create snapshot name with timestamp
        let timestamp = Utc::now().format("%Y%m%d-%H%M%S");
        let snapshot_name = format!("{}{}", self.policy.name_prefix, timestamp);

        // Build the snapshot
        let mut builder = SnapshotBuilder::new(snapshot_name, self.policy.scope.clone());

        // Set expiration based on retention days
        if let Some(days) = self.policy.retention_days {
            builder = builder.expires_in_days(days as i64);
        }

        // Add policy tags
        for (key, value) in &self.policy.tags {
            builder = builder.tag(key.clone(), value.clone());
        }
        builder = builder.tag("policy_id", self.policy.id.clone());
        builder = builder.tag("automated", "true");

        // Create the snapshot
        match self.manager.create_snapshot(builder).await {
            Ok(snapshot) => {
                let snapshot_id = snapshot.id.clone();

                // Update state
                {
                    let mut states = self.state.write().await;
                    if let Some(state) = states.get_mut(&self.policy.id) {
                        state.last_run = Some(Utc::now());
                        state.last_result = Some(PolicyResult::Success {
                            snapshot_id: snapshot_id.clone(),
                        });
                        state.success_count += 1;
                        state.snapshot_ids.push(snapshot_id.clone());
                    }
                }

                // Send event
                if let Some(tx) = &self.event_tx {
                    let _ = tx
                        .send(LifecycleEvent::SnapshotCreated {
                            policy_id: self.policy.id.clone(),
                            snapshot_id: snapshot_id.clone(),
                        })
                        .await;
                }

                // Enforce retention
                if let Err(e) = self.enforce_retention().await {
                    warn!(
                        policy_id = %self.policy.id,
                        error = %e,
                        "Failed to enforce retention"
                    );
                }

                info!(
                    policy_id = %self.policy.id,
                    snapshot_id = %snapshot_id,
                    "Policy snapshot created"
                );

                Ok(format!("Created snapshot: {}", snapshot_id))
            }
            Err(e) => {
                // Update state
                {
                    let mut states = self.state.write().await;
                    if let Some(state) = states.get_mut(&self.policy.id) {
                        state.last_run = Some(Utc::now());
                        state.last_result = Some(PolicyResult::Failed {
                            error: e.to_string(),
                        });
                        state.failure_count += 1;
                    }
                }

                // Send event
                if let Some(tx) = &self.event_tx {
                    let _ = tx
                        .send(LifecycleEvent::PolicyFailed {
                            policy_id: self.policy.id.clone(),
                            error: e.to_string(),
                        })
                        .await;
                }

                error!(
                    policy_id = %self.policy.id,
                    error = %e,
                    "Policy snapshot failed"
                );

                Err(e.to_string())
            }
        }
    }

    fn name(&self) -> &str {
        &self.policy.name
    }
}

impl SnapshotPolicyExecutor {
    async fn enforce_retention(&self) -> Result<usize> {
        // Get all snapshots for this policy's scope
        let mut snapshots = self.manager.list_by_scope(&self.policy.scope).await?;

        // Filter to snapshots created by this policy
        snapshots.retain(|s| {
            s.tags
                .get("policy_id")
                .map(|id| id == &self.policy.id)
                .unwrap_or(false)
        });

        // Sort by creation time (oldest first)
        snapshots.sort_by_key(|s| s.created_at);

        let mut to_delete = Vec::new();
        let now = Utc::now();

        // Check age-based retention
        if let Some(max_days) = self.retention.max_age_days {
            let cutoff = now - Duration::days(max_days as i64);
            for snapshot in &snapshots {
                if snapshot.created_at < cutoff {
                    // Keep minimum number
                    if snapshots.len() - to_delete.len() > self.retention.min_keep {
                        to_delete.push((
                            snapshot.id.clone(),
                            RetentionReason::AgeExceeded {
                                age_days: (now - snapshot.created_at).num_days(),
                                max_days,
                            },
                        ));
                    }
                }
            }
        }

        // Check count-based retention
        if let Some(max_count) = self.retention.max_count {
            let remaining: Vec<_> = snapshots
                .iter()
                .filter(|s| !to_delete.iter().any(|(id, _)| id == &s.id))
                .collect();

            if remaining.len() > max_count {
                let excess = remaining.len() - max_count;
                let candidates = if self.retention.delete_oldest_first {
                    remaining.iter().take(excess).collect::<Vec<_>>()
                } else {
                    remaining.iter().rev().take(excess).collect::<Vec<_>>()
                };

                for snapshot in candidates {
                    // Keep minimum number
                    if remaining.len() - to_delete.len() > self.retention.min_keep {
                        to_delete.push((
                            snapshot.id.clone(),
                            RetentionReason::CountExceeded {
                                current: remaining.len(),
                                max: max_count,
                            },
                        ));
                    }
                }
            }
        }

        // Delete snapshots
        let deleted_count = to_delete.len();
        for (snapshot_id, reason) in to_delete {
            if let Err(e) = self.manager.delete_snapshot(&snapshot_id).await {
                warn!(
                    snapshot_id = %snapshot_id,
                    error = %e,
                    "Failed to delete snapshot during retention"
                );
                continue;
            }

            // Remove from state tracking
            {
                let mut states = self.state.write().await;
                if let Some(state) = states.get_mut(&self.policy.id) {
                    state.snapshot_ids.retain(|id| id != &snapshot_id);
                }
            }

            // Send event
            if let Some(tx) = &self.event_tx {
                let _ = tx
                    .send(LifecycleEvent::SnapshotDeleted {
                        policy_id: self.policy.id.clone(),
                        snapshot_id,
                        reason,
                    })
                    .await;
            }
        }

        if deleted_count > 0 {
            // Send cleanup event
            if let Some(tx) = &self.event_tx {
                let _ = tx
                    .send(LifecycleEvent::RetentionCleanup {
                        policy_id: self.policy.id.clone(),
                        deleted_count,
                    })
                    .await;
            }

            info!(
                policy_id = %self.policy.id,
                deleted = deleted_count,
                "Retention cleanup completed"
            );
        }

        Ok(deleted_count)
    }
}

/// Snapshot lifecycle manager.
///
/// Manages automated snapshot creation and retention based on policies.
pub struct SnapshotLifecycleManager {
    config: LifecycleConfig,
    manager: Arc<SnapshotManager>,
    scheduler: JobScheduler,
    states: Arc<RwLock<HashMap<String, PolicyState>>>,
    policies: Arc<RwLock<HashMap<String, SnapshotPolicy>>>,
    event_tx: Option<tokio::sync::mpsc::Sender<LifecycleEvent>>,
}

impl SnapshotLifecycleManager {
    /// Create a new lifecycle manager.
    pub fn new(config: LifecycleConfig, manager: Arc<SnapshotManager>) -> Self {
        Self {
            config,
            manager,
            scheduler: JobScheduler::new(),
            states: Arc::new(RwLock::new(HashMap::new())),
            policies: Arc::new(RwLock::new(HashMap::new())),
            event_tx: None,
        }
    }

    /// Set event channel for lifecycle events.
    pub fn with_event_channel(
        mut self,
        tx: tokio::sync::mpsc::Sender<LifecycleEvent>,
    ) -> Self {
        self.event_tx = Some(tx);
        self
    }

    /// Register a snapshot policy.
    pub async fn register_policy(&self, policy: SnapshotPolicy) -> Result<()> {
        let policy_id = policy.id.clone();

        // Parse cron expression
        let cron = CronExpr::parse(&policy.schedule).map_err(|e| {
            StrataError::Internal(format!("Invalid cron expression: {}", e))
        })?;

        // Create retention config from policy
        let retention = RetentionConfig {
            max_count: Some(policy.retention_count),
            max_age_days: policy.retention_days,
            delete_oldest_first: true,
            min_keep: 1,
        };

        // Create job executor
        let executor = SnapshotPolicyExecutor {
            policy: policy.clone(),
            manager: self.manager.clone(),
            state: self.states.clone(),
            retention,
            event_tx: self.event_tx.clone(),
        };

        // Create scheduled job
        let job_name = format!("snapshot-policy-{}", policy.name);
        let scheduled = ScheduledJob::new(
            policy_id.clone(),
            job_name,
            cron,
            Arc::new(executor),
        )
        .with_timeout(std::time::Duration::from_secs(3600)); // 1 hour timeout

        // Register with scheduler
        self.scheduler.register(scheduled).await;

        // Initialize state
        {
            let mut states = self.states.write().await;
            states.insert(policy_id.clone(), PolicyState::new(&policy_id));
        }

        // Store policy
        {
            let mut policies = self.policies.write().await;
            policies.insert(policy_id.clone(), policy);
        }

        info!(
            policy_id = %policy_id,
            "Registered snapshot policy"
        );

        Ok(())
    }

    /// Unregister a snapshot policy.
    pub async fn unregister_policy(&self, policy_id: &str) -> Result<()> {
        self.scheduler.remove(policy_id).await;

        {
            let mut policies = self.policies.write().await;
            policies.remove(policy_id);
        }

        {
            let mut states = self.states.write().await;
            states.remove(policy_id);
        }

        info!(policy_id = %policy_id, "Unregistered snapshot policy");
        Ok(())
    }

    /// Enable a policy.
    pub async fn enable_policy(&self, policy_id: &str) -> Result<()> {
        self.scheduler.resume(policy_id).await;

        let mut policies = self.policies.write().await;
        if let Some(policy) = policies.get_mut(policy_id) {
            policy.enabled = true;
        }

        Ok(())
    }

    /// Disable a policy.
    pub async fn disable_policy(&self, policy_id: &str) -> Result<()> {
        self.scheduler.pause(policy_id).await;

        let mut policies = self.policies.write().await;
        if let Some(policy) = policies.get_mut(policy_id) {
            policy.enabled = false;
        }

        Ok(())
    }

    /// Get policy state.
    pub async fn get_policy_state(&self, policy_id: &str) -> Option<PolicyState> {
        let states = self.states.read().await;
        states.get(policy_id).cloned()
    }

    /// List all policy states.
    pub async fn list_policy_states(&self) -> Vec<PolicyState> {
        let states = self.states.read().await;
        states.values().cloned().collect()
    }

    /// Trigger a policy execution immediately.
    pub async fn trigger_policy(&self, policy_id: &str) -> Result<()> {
        self.scheduler
            .trigger(policy_id)
            .await
            .map_err(|e| StrataError::Internal(e))
    }

    /// Run cleanup of all expired snapshots.
    pub async fn cleanup_expired(&self) -> Result<usize> {
        let deleted = self.manager.cleanup_expired().await?;

        if deleted > 0 && self.config.emit_events {
            if let Some(tx) = &self.event_tx {
                let _ = tx
                    .send(LifecycleEvent::ExpiredCleanup {
                        deleted_count: deleted,
                    })
                    .await;
            }
        }

        Ok(deleted)
    }

    /// Start the lifecycle manager.
    ///
    /// This starts the scheduler and periodic cleanup tasks.
    pub async fn start(&self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        if !self.config.enabled {
            info!("Snapshot lifecycle manager is disabled");
            return Ok(());
        }

        info!("Starting snapshot lifecycle manager");

        // Start cleanup task
        let manager = self.manager.clone();
        let interval = self.config.cleanup_interval_minutes;
        let event_tx = self.event_tx.clone();
        let emit_events = self.config.emit_events;
        let mut cleanup_shutdown = shutdown.resubscribe();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(
                std::time::Duration::from_secs(interval * 60),
            );

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        debug!("Running periodic snapshot cleanup");

                        match manager.cleanup_expired().await {
                            Ok(deleted) => {
                                if deleted > 0 {
                                    info!(count = deleted, "Periodic cleanup deleted expired snapshots");

                                    if emit_events {
                                        if let Some(tx) = &event_tx {
                                            let _ = tx
                                                .send(LifecycleEvent::ExpiredCleanup {
                                                    deleted_count: deleted,
                                                })
                                                .await;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Periodic cleanup failed");
                            }
                        }
                    }
                    _ = cleanup_shutdown.recv() => {
                        info!("Cleanup task shutting down");
                        break;
                    }
                }
            }
        });

        // Run the scheduler
        self.scheduler.run(shutdown).await;

        Ok(())
    }

    /// Get statistics.
    pub async fn stats(&self) -> LifecycleStats {
        let policies = self.policies.read().await;
        let states = self.states.read().await;

        let enabled_policies = policies.values().filter(|p| p.enabled).count();
        let total_policies = policies.len();

        let mut total_success = 0u64;
        let mut total_failures = 0u64;
        let mut total_snapshots = 0usize;

        for state in states.values() {
            total_success += state.success_count;
            total_failures += state.failure_count;
            total_snapshots += state.snapshot_ids.len();
        }

        LifecycleStats {
            total_policies,
            enabled_policies,
            total_executions: total_success + total_failures,
            successful_executions: total_success,
            failed_executions: total_failures,
            total_managed_snapshots: total_snapshots,
        }
    }
}

/// Lifecycle statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleStats {
    /// Total registered policies.
    pub total_policies: usize,
    /// Enabled policies.
    pub enabled_policies: usize,
    /// Total policy executions.
    pub total_executions: u64,
    /// Successful executions.
    pub successful_executions: u64,
    /// Failed executions.
    pub failed_executions: u64,
    /// Total snapshots managed by policies.
    pub total_managed_snapshots: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{MemorySnapshotStore, MockInodeCapture};

    fn create_test_manager() -> Arc<SnapshotManager> {
        let store = Arc::new(MemorySnapshotStore::new());
        let capture = Arc::new(MockInodeCapture::new());
        Arc::new(SnapshotManager::new(
            SnapshotConfig::default(),
            store,
            capture,
        ))
    }

    #[test]
    fn test_retention_config_presets() {
        let hourly = RetentionConfig::hourly();
        assert_eq!(hourly.max_count, Some(24));
        assert_eq!(hourly.max_age_days, Some(2));

        let daily = RetentionConfig::daily();
        assert_eq!(daily.max_count, Some(7));

        let weekly = RetentionConfig::weekly();
        assert_eq!(weekly.max_count, Some(4));

        let monthly = RetentionConfig::monthly();
        assert_eq!(monthly.max_count, Some(12));
    }

    #[test]
    fn test_lifecycle_config_default() {
        let config = LifecycleConfig::default();
        assert!(config.enabled);
        assert_eq!(config.cleanup_interval_minutes, 60);
        assert_eq!(config.max_concurrent_snapshots, 2);
    }

    #[test]
    fn test_policy_state() {
        let state = PolicyState::new("test-policy");
        assert_eq!(state.policy_id, "test-policy");
        assert!(state.last_run.is_none());
        assert_eq!(state.success_count, 0);
        assert_eq!(state.failure_count, 0);
    }

    #[tokio::test]
    async fn test_lifecycle_manager_creation() {
        let manager = create_test_manager();
        let lifecycle = SnapshotLifecycleManager::new(
            LifecycleConfig::default(),
            manager,
        );

        let stats = lifecycle.stats().await;
        assert_eq!(stats.total_policies, 0);
    }

    #[tokio::test]
    async fn test_register_policy() {
        let manager = create_test_manager();
        let lifecycle = SnapshotLifecycleManager::new(
            LifecycleConfig::default(),
            manager,
        );

        let policy = SnapshotPolicy::daily(
            "test-daily",
            SnapshotScope::Volume {
                volume_id: "vol1".to_string(),
            },
        );

        lifecycle.register_policy(policy).await.unwrap();

        let stats = lifecycle.stats().await;
        assert_eq!(stats.total_policies, 1);
        assert_eq!(stats.enabled_policies, 1);
    }

    #[tokio::test]
    async fn test_unregister_policy() {
        let manager = create_test_manager();
        let lifecycle = SnapshotLifecycleManager::new(
            LifecycleConfig::default(),
            manager,
        );

        let policy = SnapshotPolicy::weekly(
            "test-weekly",
            SnapshotScope::Volume {
                volume_id: "vol1".to_string(),
            },
        );
        let policy_id = policy.id.clone();

        lifecycle.register_policy(policy).await.unwrap();
        assert_eq!(lifecycle.stats().await.total_policies, 1);

        lifecycle.unregister_policy(&policy_id).await.unwrap();
        assert_eq!(lifecycle.stats().await.total_policies, 0);
    }

    #[test]
    fn test_lifecycle_events() {
        let event = LifecycleEvent::SnapshotCreated {
            policy_id: "p1".to_string(),
            snapshot_id: "s1".to_string(),
        };

        // Just ensure serialization works
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("snapshot_created"));
    }

    #[test]
    fn test_retention_reason() {
        let reason = RetentionReason::CountExceeded {
            current: 15,
            max: 10,
        };

        let json = serde_json::to_string(&reason).unwrap();
        assert!(json.contains("count_exceeded"));

        let reason = RetentionReason::AgeExceeded {
            age_days: 45,
            max_days: 30,
        };

        let json = serde_json::to_string(&reason).unwrap();
        assert!(json.contains("age_exceeded"));
    }
}
