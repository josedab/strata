//! Cluster Registry for Global Namespace Federation
//!
//! This module provides cluster discovery and registration for creating
//! a federated global namespace across multiple Strata clusters.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Cluster Registry                              │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Discovery Service │ Health Monitor │ Heartbeat Protocol        │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Cluster Metadata │ Peer Authentication │ Status Tracking       │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, StrataError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

/// Unique identifier for a Strata cluster in the federation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(pub String);

impl ClusterId {
    pub fn new(name: &str) -> Self {
        Self(name.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ClusterId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Information about a cluster in the federation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterInfo {
    /// Unique cluster identifier.
    pub id: ClusterId,
    /// Human-readable cluster name.
    pub name: String,
    /// Cluster region/location for routing decisions.
    pub region: String,
    /// Geographic coordinates for latency-aware routing.
    pub location: Option<GeoCoordinates>,
    /// Metadata service endpoints.
    pub metadata_endpoints: Vec<String>,
    /// S3 gateway endpoints.
    pub s3_endpoints: Vec<String>,
    /// Data service endpoints.
    pub data_endpoints: Vec<String>,
    /// Cluster version for compatibility checking.
    pub version: String,
    /// Cluster capabilities and features.
    pub capabilities: ClusterCapabilities,
    /// TLS certificate fingerprint for authentication.
    pub tls_fingerprint: Option<String>,
    /// When this cluster joined the federation.
    pub joined_at: DateTime<Utc>,
    /// Last heartbeat received.
    pub last_heartbeat: DateTime<Utc>,
    /// Custom metadata/labels.
    pub labels: HashMap<String, String>,
}

/// Geographic coordinates for a cluster.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct GeoCoordinates {
    pub latitude: f64,
    pub longitude: f64,
}

impl GeoCoordinates {
    /// Calculate distance to another location in kilometers using Haversine formula.
    pub fn distance_to(&self, other: &GeoCoordinates) -> f64 {
        let r = 6371.0; // Earth's radius in km
        let d_lat = (other.latitude - self.latitude).to_radians();
        let d_lon = (other.longitude - self.longitude).to_radians();
        let lat1 = self.latitude.to_radians();
        let lat2 = other.latitude.to_radians();

        let a = (d_lat / 2.0).sin().powi(2)
            + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().asin();
        r * c
    }
}

/// Capabilities advertised by a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterCapabilities {
    /// Supports S3 protocol.
    pub s3_enabled: bool,
    /// Supports FUSE mount.
    pub fuse_enabled: bool,
    /// Supports NFS gateway.
    pub nfs_enabled: bool,
    /// Maximum object size supported.
    pub max_object_size: u64,
    /// Erasure coding configuration.
    pub erasure_config: String,
    /// Supports cross-cluster replication.
    pub supports_replication: bool,
    /// Supports serving as replication target.
    pub accepts_replicas: bool,
    /// Storage capacity in bytes.
    pub total_capacity: u64,
    /// Available storage in bytes.
    pub available_capacity: u64,
}

impl Default for ClusterCapabilities {
    fn default() -> Self {
        Self {
            s3_enabled: true,
            fuse_enabled: true,
            nfs_enabled: false,
            max_object_size: 5 * 1024 * 1024 * 1024 * 1024, // 5TB
            erasure_config: "4+2".to_string(),
            supports_replication: true,
            accepts_replicas: true,
            total_capacity: 0,
            available_capacity: 0,
        }
    }
}

/// Health status of a cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterHealth {
    /// Cluster is fully operational.
    Healthy,
    /// Cluster is operational but degraded.
    Degraded,
    /// Cluster is unreachable or unhealthy.
    Unhealthy,
    /// Cluster health is unknown (no recent heartbeat).
    Unknown,
}

/// Cluster registration request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterClusterRequest {
    pub cluster_info: ClusterInfo,
    /// Authentication token for registration.
    pub auth_token: String,
}

/// Cluster heartbeat message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHeartbeat {
    pub cluster_id: ClusterId,
    pub timestamp: DateTime<Utc>,
    pub health: ClusterHealth,
    pub load_metrics: LoadMetrics,
    /// Current leader node ID (for Raft clusters).
    pub leader_id: Option<u64>,
}

/// Load metrics for routing decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadMetrics {
    /// CPU utilization percentage (0-100).
    pub cpu_percent: f32,
    /// Memory utilization percentage (0-100).
    pub memory_percent: f32,
    /// Disk I/O utilization percentage (0-100).
    pub disk_io_percent: f32,
    /// Network I/O utilization percentage (0-100).
    pub network_io_percent: f32,
    /// Pending operations queue depth.
    pub pending_operations: u64,
    /// Active connections count.
    pub active_connections: u64,
}

impl Default for LoadMetrics {
    fn default() -> Self {
        Self {
            cpu_percent: 0.0,
            memory_percent: 0.0,
            disk_io_percent: 0.0,
            network_io_percent: 0.0,
            pending_operations: 0,
            active_connections: 0,
        }
    }
}

/// Configuration for the cluster registry.
#[derive(Debug, Clone)]
pub struct ClusterRegistryConfig {
    /// This cluster's ID.
    pub local_cluster_id: ClusterId,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Timeout before marking cluster as unhealthy.
    pub heartbeat_timeout: Duration,
    /// Timeout before removing cluster from registry.
    pub removal_timeout: Duration,
    /// Enable automatic discovery via DNS/multicast.
    pub auto_discovery: bool,
    /// Bootstrap peers for initial discovery.
    pub bootstrap_peers: Vec<String>,
}

impl Default for ClusterRegistryConfig {
    fn default() -> Self {
        Self {
            local_cluster_id: ClusterId::new("local"),
            heartbeat_interval: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(30),
            removal_timeout: Duration::from_secs(300),
            auto_discovery: false,
            bootstrap_peers: vec![],
        }
    }
}

/// Internal state for a registered cluster.
struct ClusterState {
    info: ClusterInfo,
    health: ClusterHealth,
    last_heartbeat_received: Instant,
    consecutive_failures: u32,
    load_metrics: LoadMetrics,
}

/// Events emitted by the cluster registry.
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    /// A new cluster joined the federation.
    ClusterJoined(ClusterId),
    /// A cluster left the federation.
    ClusterLeft(ClusterId),
    /// A cluster's health status changed.
    HealthChanged {
        cluster_id: ClusterId,
        old_health: ClusterHealth,
        new_health: ClusterHealth,
    },
    /// A cluster's capabilities were updated.
    CapabilitiesUpdated(ClusterId),
}

/// Cluster Registry - manages federation membership.
pub struct ClusterRegistry {
    config: ClusterRegistryConfig,
    /// Registered clusters.
    clusters: Arc<RwLock<HashMap<ClusterId, ClusterState>>>,
    /// Event subscribers.
    event_tx: mpsc::Sender<ClusterEvent>,
    /// Shutdown signal.
    shutdown_tx: mpsc::Sender<()>,
}

impl ClusterRegistry {
    /// Create a new cluster registry.
    pub fn new(config: ClusterRegistryConfig) -> (Self, mpsc::Receiver<ClusterEvent>) {
        let (event_tx, event_rx) = mpsc::channel(1000);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let registry = Self {
            config: config.clone(),
            clusters: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            shutdown_tx,
        };

        // Start background health check task
        let clusters = Arc::clone(&registry.clusters);
        let heartbeat_timeout = config.heartbeat_timeout;
        let removal_timeout = config.removal_timeout;
        let event_tx = registry.event_tx.clone();
        tokio::spawn(Self::health_check_loop(
            clusters,
            heartbeat_timeout,
            removal_timeout,
            event_tx,
            shutdown_rx,
        ));

        (registry, event_rx)
    }

    /// Register a new cluster in the federation.
    pub async fn register(&self, info: ClusterInfo) -> Result<()> {
        let cluster_id = info.id.clone();

        let mut clusters = self.clusters.write().await;

        if clusters.contains_key(&cluster_id) {
            return Err(StrataError::AlreadyExists(format!(
                "Cluster {} already registered",
                cluster_id
            )));
        }

        let state = ClusterState {
            info: info.clone(),
            health: ClusterHealth::Unknown,
            last_heartbeat_received: Instant::now(),
            consecutive_failures: 0,
            load_metrics: LoadMetrics::default(),
        };

        clusters.insert(cluster_id.clone(), state);

        info!(cluster_id = %cluster_id, region = %info.region, "Cluster registered");

        // Emit event
        let _ = self.event_tx.send(ClusterEvent::ClusterJoined(cluster_id)).await;

        Ok(())
    }

    /// Unregister a cluster from the federation.
    pub async fn unregister(&self, cluster_id: &ClusterId) -> Result<()> {
        let mut clusters = self.clusters.write().await;

        if clusters.remove(cluster_id).is_some() {
            info!(cluster_id = %cluster_id, "Cluster unregistered");
            let _ = self
                .event_tx
                .send(ClusterEvent::ClusterLeft(cluster_id.clone()))
                .await;
            Ok(())
        } else {
            Err(StrataError::NotFound(format!(
                "Cluster {} not found",
                cluster_id
            )))
        }
    }

    /// Process a heartbeat from a cluster.
    pub async fn heartbeat(&self, heartbeat: ClusterHeartbeat) -> Result<()> {
        let mut clusters = self.clusters.write().await;

        if let Some(state) = clusters.get_mut(&heartbeat.cluster_id) {
            let old_health = state.health;
            let new_health = heartbeat.health;

            state.last_heartbeat_received = Instant::now();
            state.health = new_health;
            state.load_metrics = heartbeat.load_metrics;
            state.consecutive_failures = 0;
            state.info.last_heartbeat = heartbeat.timestamp;

            if old_health != new_health {
                let cluster_id = heartbeat.cluster_id.clone();
                drop(clusters);

                let _ = self
                    .event_tx
                    .send(ClusterEvent::HealthChanged {
                        cluster_id,
                        old_health,
                        new_health,
                    })
                    .await;
            }

            debug!(
                cluster_id = %heartbeat.cluster_id,
                health = ?new_health,
                "Heartbeat received"
            );

            Ok(())
        } else {
            Err(StrataError::NotFound(format!(
                "Cluster {} not registered",
                heartbeat.cluster_id
            )))
        }
    }

    /// Get information about a specific cluster.
    pub async fn get_cluster(&self, cluster_id: &ClusterId) -> Option<ClusterInfo> {
        self.clusters
            .read()
            .await
            .get(cluster_id)
            .map(|s| s.info.clone())
    }

    /// Get the health status of a cluster.
    pub async fn get_health(&self, cluster_id: &ClusterId) -> Option<ClusterHealth> {
        self.clusters
            .read()
            .await
            .get(cluster_id)
            .map(|s| s.health)
    }

    /// Get load metrics for a cluster.
    pub async fn get_load(&self, cluster_id: &ClusterId) -> Option<LoadMetrics> {
        self.clusters
            .read()
            .await
            .get(cluster_id)
            .map(|s| s.load_metrics.clone())
    }

    /// List all registered clusters.
    pub async fn list_clusters(&self) -> Vec<ClusterInfo> {
        self.clusters
            .read()
            .await
            .values()
            .map(|s| s.info.clone())
            .collect()
    }

    /// List healthy clusters only.
    pub async fn list_healthy_clusters(&self) -> Vec<ClusterInfo> {
        self.clusters
            .read()
            .await
            .values()
            .filter(|s| s.health == ClusterHealth::Healthy)
            .map(|s| s.info.clone())
            .collect()
    }

    /// Find clusters by region.
    pub async fn find_by_region(&self, region: &str) -> Vec<ClusterInfo> {
        self.clusters
            .read()
            .await
            .values()
            .filter(|s| s.info.region == region)
            .map(|s| s.info.clone())
            .collect()
    }

    /// Find clusters with specific capability.
    pub async fn find_with_capability<F>(&self, predicate: F) -> Vec<ClusterInfo>
    where
        F: Fn(&ClusterCapabilities) -> bool,
    {
        self.clusters
            .read()
            .await
            .values()
            .filter(|s| predicate(&s.info.capabilities))
            .map(|s| s.info.clone())
            .collect()
    }

    /// Find the nearest cluster by geographic distance.
    pub async fn find_nearest(&self, location: &GeoCoordinates) -> Option<ClusterInfo> {
        self.clusters
            .read()
            .await
            .values()
            .filter(|s| s.health == ClusterHealth::Healthy)
            .filter_map(|s| {
                s.info.location.map(|loc| {
                    let distance = location.distance_to(&loc);
                    (distance, s.info.clone())
                })
            })
            .min_by(|(d1, _), (d2, _)| d1.partial_cmp(d2).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(_, info)| info)
    }

    /// Find the least loaded cluster.
    pub async fn find_least_loaded(&self) -> Option<ClusterInfo> {
        self.clusters
            .read()
            .await
            .values()
            .filter(|s| s.health == ClusterHealth::Healthy)
            .min_by(|a, b| {
                let load_a = a.load_metrics.cpu_percent
                    + a.load_metrics.memory_percent
                    + a.load_metrics.disk_io_percent;
                let load_b = b.load_metrics.cpu_percent
                    + b.load_metrics.memory_percent
                    + b.load_metrics.disk_io_percent;
                load_a
                    .partial_cmp(&load_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|s| s.info.clone())
    }

    /// Get federation statistics.
    pub async fn get_stats(&self) -> FederationStats {
        let clusters = self.clusters.read().await;

        let total_clusters = clusters.len();
        let healthy_clusters = clusters
            .values()
            .filter(|s| s.health == ClusterHealth::Healthy)
            .count();
        let degraded_clusters = clusters
            .values()
            .filter(|s| s.health == ClusterHealth::Degraded)
            .count();
        let unhealthy_clusters = clusters
            .values()
            .filter(|s| s.health == ClusterHealth::Unhealthy)
            .count();

        let total_capacity: u64 = clusters
            .values()
            .map(|s| s.info.capabilities.total_capacity)
            .sum();
        let available_capacity: u64 = clusters
            .values()
            .map(|s| s.info.capabilities.available_capacity)
            .sum();

        let regions: Vec<String> = clusters
            .values()
            .map(|s| s.info.region.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        FederationStats {
            total_clusters,
            healthy_clusters,
            degraded_clusters,
            unhealthy_clusters,
            total_capacity,
            available_capacity,
            regions,
        }
    }

    /// Update cluster capabilities.
    pub async fn update_capabilities(
        &self,
        cluster_id: &ClusterId,
        capabilities: ClusterCapabilities,
    ) -> Result<()> {
        let mut clusters = self.clusters.write().await;

        if let Some(state) = clusters.get_mut(cluster_id) {
            state.info.capabilities = capabilities;

            info!(cluster_id = %cluster_id, "Cluster capabilities updated");

            let cluster_id = cluster_id.clone();
            drop(clusters);

            let _ = self
                .event_tx
                .send(ClusterEvent::CapabilitiesUpdated(cluster_id))
                .await;

            Ok(())
        } else {
            Err(StrataError::NotFound(format!(
                "Cluster {} not found",
                cluster_id
            )))
        }
    }

    /// Shutdown the registry.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(()).await;
        info!("Cluster registry shutting down");
    }

    /// Background task to check cluster health.
    async fn health_check_loop(
        clusters: Arc<RwLock<HashMap<ClusterId, ClusterState>>>,
        heartbeat_timeout: Duration,
        removal_timeout: Duration,
        event_tx: mpsc::Sender<ClusterEvent>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Instant::now();
                    let mut to_remove = Vec::new();
                    let mut health_changes = Vec::new();

                    {
                        let mut clusters = clusters.write().await;
                        for (cluster_id, state) in clusters.iter_mut() {
                            let elapsed = now.duration_since(state.last_heartbeat_received);

                            if elapsed > removal_timeout {
                                to_remove.push(cluster_id.clone());
                            } else if elapsed > heartbeat_timeout {
                                if state.health != ClusterHealth::Unhealthy {
                                    let old_health = state.health;
                                    state.health = ClusterHealth::Unhealthy;
                                    state.consecutive_failures += 1;

                                    health_changes.push((
                                        cluster_id.clone(),
                                        old_health,
                                        ClusterHealth::Unhealthy,
                                    ));

                                    warn!(
                                        cluster_id = %cluster_id,
                                        elapsed_secs = elapsed.as_secs(),
                                        "Cluster marked unhealthy due to missed heartbeats"
                                    );
                                }
                            }
                        }

                        for cluster_id in &to_remove {
                            clusters.remove(cluster_id);
                            error!(cluster_id = %cluster_id, "Cluster removed due to prolonged unavailability");
                        }
                    }

                    // Emit events outside of lock
                    for cluster_id in to_remove {
                        let _ = event_tx.send(ClusterEvent::ClusterLeft(cluster_id)).await;
                    }
                    for (cluster_id, old_health, new_health) in health_changes {
                        let _ = event_tx.send(ClusterEvent::HealthChanged {
                            cluster_id,
                            old_health,
                            new_health,
                        }).await;
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Health check loop shutting down");
                    break;
                }
            }
        }
    }
}

/// Federation statistics.
#[derive(Debug, Clone)]
pub struct FederationStats {
    pub total_clusters: usize,
    pub healthy_clusters: usize,
    pub degraded_clusters: usize,
    pub unhealthy_clusters: usize,
    pub total_capacity: u64,
    pub available_capacity: u64,
    pub regions: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_cluster_info(id: &str, region: &str) -> ClusterInfo {
        ClusterInfo {
            id: ClusterId::new(id),
            name: format!("Test Cluster {}", id),
            region: region.to_string(),
            location: Some(GeoCoordinates {
                latitude: 37.7749,
                longitude: -122.4194,
            }),
            metadata_endpoints: vec!["localhost:9090".to_string()],
            s3_endpoints: vec!["localhost:8080".to_string()],
            data_endpoints: vec!["localhost:9091".to_string()],
            version: "0.1.0".to_string(),
            capabilities: ClusterCapabilities::default(),
            tls_fingerprint: None,
            joined_at: Utc::now(),
            last_heartbeat: Utc::now(),
            labels: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_register_and_list() {
        let config = ClusterRegistryConfig::default();
        let (registry, _event_rx) = ClusterRegistry::new(config);

        let cluster1 = create_test_cluster_info("cluster-1", "us-east-1");
        let cluster2 = create_test_cluster_info("cluster-2", "eu-west-1");

        registry.register(cluster1.clone()).await.unwrap();
        registry.register(cluster2.clone()).await.unwrap();

        let clusters = registry.list_clusters().await;
        assert_eq!(clusters.len(), 2);

        let info = registry.get_cluster(&ClusterId::new("cluster-1")).await;
        assert!(info.is_some());
        assert_eq!(info.unwrap().name, "Test Cluster cluster-1");
    }

    #[tokio::test]
    async fn test_duplicate_registration() {
        let config = ClusterRegistryConfig::default();
        let (registry, _event_rx) = ClusterRegistry::new(config);

        let cluster = create_test_cluster_info("cluster-1", "us-east-1");

        registry.register(cluster.clone()).await.unwrap();
        let result = registry.register(cluster).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let config = ClusterRegistryConfig::default();
        let (registry, _event_rx) = ClusterRegistry::new(config);

        let cluster = create_test_cluster_info("cluster-1", "us-east-1");
        registry.register(cluster).await.unwrap();

        let heartbeat = ClusterHeartbeat {
            cluster_id: ClusterId::new("cluster-1"),
            timestamp: Utc::now(),
            health: ClusterHealth::Healthy,
            load_metrics: LoadMetrics::default(),
            leader_id: Some(1),
        };

        registry.heartbeat(heartbeat).await.unwrap();

        let health = registry.get_health(&ClusterId::new("cluster-1")).await;
        assert_eq!(health, Some(ClusterHealth::Healthy));
    }

    #[tokio::test]
    async fn test_find_by_region() {
        let config = ClusterRegistryConfig::default();
        let (registry, _event_rx) = ClusterRegistry::new(config);

        registry
            .register(create_test_cluster_info("cluster-1", "us-east-1"))
            .await
            .unwrap();
        registry
            .register(create_test_cluster_info("cluster-2", "us-east-1"))
            .await
            .unwrap();
        registry
            .register(create_test_cluster_info("cluster-3", "eu-west-1"))
            .await
            .unwrap();

        let us_clusters = registry.find_by_region("us-east-1").await;
        assert_eq!(us_clusters.len(), 2);

        let eu_clusters = registry.find_by_region("eu-west-1").await;
        assert_eq!(eu_clusters.len(), 1);
    }

    #[tokio::test]
    async fn test_unregister() {
        let config = ClusterRegistryConfig::default();
        let (registry, _event_rx) = ClusterRegistry::new(config);

        let cluster = create_test_cluster_info("cluster-1", "us-east-1");
        registry.register(cluster).await.unwrap();

        assert!(registry.get_cluster(&ClusterId::new("cluster-1")).await.is_some());

        registry
            .unregister(&ClusterId::new("cluster-1"))
            .await
            .unwrap();

        assert!(registry.get_cluster(&ClusterId::new("cluster-1")).await.is_none());
    }

    #[test]
    fn test_geo_distance() {
        let sf = GeoCoordinates {
            latitude: 37.7749,
            longitude: -122.4194,
        };
        let ny = GeoCoordinates {
            latitude: 40.7128,
            longitude: -74.0060,
        };

        let distance = sf.distance_to(&ny);
        // Approximately 4000 km
        assert!(distance > 3500.0 && distance < 5000.0);
    }

    #[tokio::test]
    async fn test_stats() {
        let config = ClusterRegistryConfig::default();
        let (registry, _event_rx) = ClusterRegistry::new(config);

        registry
            .register(create_test_cluster_info("cluster-1", "us-east-1"))
            .await
            .unwrap();
        registry
            .register(create_test_cluster_info("cluster-2", "eu-west-1"))
            .await
            .unwrap();

        let stats = registry.get_stats().await;
        assert_eq!(stats.total_clusters, 2);
        assert_eq!(stats.regions.len(), 2);
    }
}
