//! Cluster management module for Strata.
//!
//! This module handles cluster-wide operations including:
//! - Placement engine for chunk distribution
//! - Self-healing and recovery
//! - Rebalancing
//! - Failure detection
//! - Distributed coordination

mod balancer;
mod coordinator;
mod executor;
mod failure_detector;
mod placement;
mod recovery;

pub use balancer::{BalanceStats, Balancer, MoveOperation};
pub use coordinator::{
    ClusterCoordinator, CoordinatorConfig, CoordinatorStats, DegradationLevel, DegradationPolicy,
    DegradedModeStatus, HealthEvaluator, LockGuard, LockResult, LockType,
};
pub use executor::{ClusterState, RecoveryExecutor, RecoveryResult, RecoveryStats};
pub use failure_detector::{
    FailureDetector, FailureDetectorConfig, FailureDetectorStatus, HeartbeatMonitor,
};
pub use placement::PlacementEngine;
pub use recovery::{RecoveryManager, RecoveryTask};

/// Cluster state summary.
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    /// Number of online data servers.
    pub online_servers: usize,
    /// Number of offline data servers.
    pub offline_servers: usize,
    /// Total storage capacity in bytes.
    pub total_capacity: u64,
    /// Used storage in bytes.
    pub used_capacity: u64,
    /// Number of under-replicated chunks.
    pub under_replicated_chunks: usize,
    /// Cluster health status.
    pub health: ClusterHealth,
}

/// Cluster health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterHealth {
    /// All systems operational.
    Healthy,
    /// Minor issues, non-critical.
    Degraded,
    /// Critical issues, data at risk.
    Critical,
    /// Not enough nodes to maintain redundancy.
    Emergency,
}

impl ClusterHealth {
    pub fn is_healthy(&self) -> bool {
        matches!(self, ClusterHealth::Healthy)
    }
}

impl Default for ClusterStatus {
    fn default() -> Self {
        Self {
            online_servers: 0,
            offline_servers: 0,
            total_capacity: 0,
            used_capacity: 0,
            under_replicated_chunks: 0,
            health: ClusterHealth::Healthy,
        }
    }
}
