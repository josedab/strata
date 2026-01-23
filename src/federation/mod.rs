//! Federation Module for Strata
//!
//! This module provides both multi-cloud federation (unified access to multiple
//! cloud storage providers) and cluster-to-cluster federation (global namespace
//! across multiple Strata clusters).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    Global Federation                             │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Cluster Registry │ Namespace Router │ Cross-Cluster Replication│
//! ├─────────────────────────────────────────────────────────────────┤
//! │  Placement Engine │ Cost Optimizer │ Cloud Adapters             │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! ## Cluster-to-Cluster Federation
//! - Cluster discovery and registration
//! - Health monitoring and heartbeat protocol
//! - Path-based routing rules
//! - Location-aware and load-based routing
//!
//! ## Multi-Cloud Federation
//! - Unified namespace across AWS, GCP, Azure
//! - Intelligent data placement based on cost/latency
//! - Cross-cloud replication
//! - Cost optimization

// Multi-cloud federation (original implementation)
mod cloud;

// Cluster-to-cluster federation (Phase 1: Discovery)
mod cluster_registry;
mod namespace_router;

// Re-export multi-cloud types
pub use cloud::*;

// Re-export cluster federation types
pub use cluster_registry::{
    ClusterCapabilities, ClusterEvent, ClusterHealth, ClusterHeartbeat, ClusterId, ClusterInfo,
    ClusterRegistry, ClusterRegistryConfig, FederationStats, GeoCoordinates, LoadMetrics,
    RegisterClusterRequest,
};

pub use namespace_router::{
    NamespaceRouter, NamespaceRouterConfig, RouterStats, RoutingDecision, RoutingRule,
    RoutingStrategy,
};

use crate::error::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

/// Global Federation Controller
///
/// Combines cluster-to-cluster federation with multi-cloud storage federation.
pub struct GlobalFederation {
    /// Cluster registry for tracking federated clusters.
    pub cluster_registry: Arc<ClusterRegistry>,
    /// Namespace router for cross-cluster routing.
    pub namespace_router: Arc<NamespaceRouter>,
    /// Multi-cloud federation controller.
    pub cloud_federation: FederationController,
    /// Event receiver for cluster events.
    event_rx: Option<mpsc::Receiver<ClusterEvent>>,
}

impl GlobalFederation {
    /// Create a new global federation controller.
    pub fn new(
        cluster_config: ClusterRegistryConfig,
        router_config: NamespaceRouterConfig,
    ) -> Self {
        let (cluster_registry, event_rx) = ClusterRegistry::new(cluster_config);
        let cluster_registry = Arc::new(cluster_registry);

        let namespace_router = Arc::new(NamespaceRouter::new(
            router_config,
            Arc::clone(&cluster_registry),
        ));

        let cloud_federation = FederationController::new();

        Self {
            cluster_registry,
            namespace_router,
            cloud_federation,
            event_rx: Some(event_rx),
        }
    }

    /// Take the event receiver (can only be called once).
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<ClusterEvent>> {
        self.event_rx.take()
    }

    /// Register the local cluster.
    pub async fn register_local(&self, info: ClusterInfo) -> Result<()> {
        self.cluster_registry.register(info).await
    }

    /// Join a remote cluster to the federation.
    pub async fn join_cluster(&self, info: ClusterInfo) -> Result<()> {
        self.cluster_registry.register(info).await
    }

    /// Route a request to the appropriate cluster.
    pub async fn route(&self, path: &str) -> Result<RoutingDecision> {
        self.namespace_router.route(path).await
    }

    /// Add a routing rule.
    pub async fn add_routing_rule(&self, rule: RoutingRule) {
        self.namespace_router.add_rule(rule).await
    }

    /// Get federation statistics.
    pub async fn stats(&self) -> GlobalFederationStats {
        let cluster_stats = self.cluster_registry.get_stats().await;
        let router_stats = self.namespace_router.get_stats().await;

        GlobalFederationStats {
            cluster_stats,
            router_stats,
        }
    }

    /// Shutdown the federation controller.
    pub async fn shutdown(&self) {
        self.cluster_registry.shutdown().await;
        info!("Global federation shutdown complete");
    }
}

/// Combined federation statistics.
#[derive(Debug, Clone)]
pub struct GlobalFederationStats {
    pub cluster_stats: FederationStats,
    pub router_stats: RouterStats,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashMap;

    fn create_test_cluster(id: &str) -> ClusterInfo {
        ClusterInfo {
            id: ClusterId::new(id),
            name: format!("Test {}", id),
            region: "us-east-1".to_string(),
            location: None,
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
    async fn test_global_federation() {
        let cluster_config = ClusterRegistryConfig {
            local_cluster_id: ClusterId::new("local"),
            ..Default::default()
        };
        let router_config = NamespaceRouterConfig {
            local_cluster_id: ClusterId::new("local"),
            ..Default::default()
        };

        let mut federation = GlobalFederation::new(cluster_config, router_config);
        let _event_rx = federation.take_event_receiver();

        // Register local cluster
        federation
            .register_local(create_test_cluster("local"))
            .await
            .unwrap();

        // Get stats
        let stats = federation.stats().await;
        assert_eq!(stats.cluster_stats.total_clusters, 1);

        // Shutdown
        federation.shutdown().await;
    }
}
