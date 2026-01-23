//! Global Namespace Router for Federation
//!
//! Routes requests to the appropriate cluster based on path patterns,
//! policies, and cluster health status.

use super::cluster_registry::{ClusterHealth, ClusterId, ClusterInfo, ClusterRegistry, GeoCoordinates};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Routing rule for path-based routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    /// Rule identifier.
    pub id: String,
    /// Path pattern (supports wildcards: * for segment, ** for recursive).
    pub path_pattern: String,
    /// Target cluster ID (None means local cluster).
    pub target_cluster: Option<ClusterId>,
    /// Priority (higher = evaluated first).
    pub priority: i32,
    /// Whether this rule is enabled.
    pub enabled: bool,
    /// Rule description.
    pub description: String,
}

impl RoutingRule {
    /// Check if a path matches this rule's pattern.
    pub fn matches(&self, path: &str) -> bool {
        if !self.enabled {
            return false;
        }
        pattern_matches(&self.path_pattern, path)
    }
}

/// Check if a path matches a pattern with wildcards.
fn pattern_matches(pattern: &str, path: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('/').filter(|s| !s.is_empty()).collect();
    let path_parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    let mut pi = 0; // pattern index
    let mut ti = 0; // target (path) index

    while pi < pattern_parts.len() && ti < path_parts.len() {
        let pp = pattern_parts[pi];
        let tp = path_parts[ti];

        if pp == "**" {
            // Match rest of path (including empty rest)
            return true;
        } else if pp == "*" {
            // Match single segment
            pi += 1;
            ti += 1;
        } else if pp == tp {
            // Exact match
            pi += 1;
            ti += 1;
        } else {
            return false;
        }
    }

    // Check if we consumed path and remaining pattern is just "**"
    if ti == path_parts.len() && pi < pattern_parts.len() && pattern_parts[pi] == "**" {
        return true;
    }

    // Check if we consumed both completely
    pi == pattern_parts.len() && ti == path_parts.len()
}

/// Routing strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingStrategy {
    /// Route to the nearest cluster by geographic distance.
    NearestCluster,
    /// Route to the least loaded cluster.
    LeastLoaded,
    /// Round-robin across healthy clusters.
    RoundRobin,
    /// Route based on consistent hashing of path.
    ConsistentHash,
    /// Always route to a specific cluster.
    Pinned,
    /// Route based on explicit rules only.
    RulesOnly,
}

/// Configuration for the namespace router.
#[derive(Debug, Clone)]
pub struct NamespaceRouterConfig {
    /// Local cluster ID.
    pub local_cluster_id: ClusterId,
    /// Default routing strategy.
    pub default_strategy: RoutingStrategy,
    /// Client location for distance-based routing.
    pub client_location: Option<GeoCoordinates>,
    /// Whether to fallback to local on routing failure.
    pub fallback_to_local: bool,
    /// Whether to cache routing decisions.
    pub enable_caching: bool,
    /// Cache TTL in seconds.
    pub cache_ttl_secs: u64,
}

impl Default for NamespaceRouterConfig {
    fn default() -> Self {
        Self {
            local_cluster_id: ClusterId::new("local"),
            default_strategy: RoutingStrategy::RulesOnly,
            client_location: None,
            fallback_to_local: true,
            enable_caching: true,
            cache_ttl_secs: 60,
        }
    }
}

/// Routing decision result.
#[derive(Debug, Clone)]
pub struct RoutingDecision {
    /// Target cluster.
    pub target: ClusterInfo,
    /// Whether this is the local cluster.
    pub is_local: bool,
    /// Matched rule (if any).
    pub matched_rule: Option<String>,
    /// Reason for routing decision.
    pub reason: String,
}

/// Namespace Router - routes requests across federated clusters.
pub struct NamespaceRouter {
    config: NamespaceRouterConfig,
    registry: Arc<ClusterRegistry>,
    rules: Arc<RwLock<Vec<RoutingRule>>>,
    round_robin_counter: Arc<RwLock<usize>>,
    route_cache: Arc<RwLock<HashMap<String, CachedRoute>>>,
}

struct CachedRoute {
    cluster_id: ClusterId,
    cached_at: std::time::Instant,
}

impl NamespaceRouter {
    /// Create a new namespace router.
    pub fn new(config: NamespaceRouterConfig, registry: Arc<ClusterRegistry>) -> Self {
        Self {
            config,
            registry,
            rules: Arc::new(RwLock::new(Vec::new())),
            round_robin_counter: Arc::new(RwLock::new(0)),
            route_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a routing rule.
    pub async fn add_rule(&self, rule: RoutingRule) {
        let mut rules = self.rules.write().await;
        rules.push(rule.clone());
        // Sort by priority (descending)
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
        info!(rule_id = %rule.id, pattern = %rule.path_pattern, "Routing rule added");
    }

    /// Remove a routing rule.
    pub async fn remove_rule(&self, rule_id: &str) -> bool {
        let mut rules = self.rules.write().await;
        let len_before = rules.len();
        rules.retain(|r| r.id != rule_id);
        let removed = rules.len() < len_before;
        if removed {
            info!(rule_id = %rule_id, "Routing rule removed");
        }
        removed
    }

    /// List all routing rules.
    pub async fn list_rules(&self) -> Vec<RoutingRule> {
        self.rules.read().await.clone()
    }

    /// Route a request for a given path.
    pub async fn route(&self, path: &str) -> Result<RoutingDecision> {
        // Check cache first
        if self.config.enable_caching {
            if let Some(cached) = self.check_cache(path).await {
                if let Some(info) = self.registry.get_cluster(&cached).await {
                    return Ok(RoutingDecision {
                        is_local: cached == self.config.local_cluster_id,
                        target: info,
                        matched_rule: None,
                        reason: "Cached route".to_string(),
                    });
                }
            }
        }

        // Check explicit rules first
        let rules = self.rules.read().await;
        for rule in rules.iter() {
            if rule.matches(path) {
                if let Some(target_id) = &rule.target_cluster {
                    if let Some(health) = self.registry.get_health(target_id).await {
                        if health == ClusterHealth::Healthy || health == ClusterHealth::Degraded {
                            if let Some(info) = self.registry.get_cluster(target_id).await {
                                self.update_cache(path, target_id.clone()).await;
                                return Ok(RoutingDecision {
                                    is_local: *target_id == self.config.local_cluster_id,
                                    target: info,
                                    matched_rule: Some(rule.id.clone()),
                                    reason: format!("Matched rule: {}", rule.description),
                                });
                            }
                        } else {
                            warn!(
                                rule_id = %rule.id,
                                target = %target_id,
                                health = ?health,
                                "Target cluster unhealthy, skipping rule"
                            );
                        }
                    }
                }
            }
        }
        drop(rules);

        // Apply default strategy
        let decision = match self.config.default_strategy {
            RoutingStrategy::NearestCluster => self.route_nearest(path).await,
            RoutingStrategy::LeastLoaded => self.route_least_loaded(path).await,
            RoutingStrategy::RoundRobin => self.route_round_robin(path).await,
            RoutingStrategy::ConsistentHash => self.route_consistent_hash(path).await,
            RoutingStrategy::Pinned | RoutingStrategy::RulesOnly => self.route_local(path).await,
        };

        match decision {
            Ok(d) => {
                self.update_cache(path, d.target.id.clone()).await;
                Ok(d)
            }
            Err(e) if self.config.fallback_to_local => {
                warn!(path = %path, error = %e, "Routing failed, falling back to local");
                self.route_local(path).await
            }
            Err(e) => Err(e),
        }
    }

    /// Route to nearest cluster by geographic distance.
    async fn route_nearest(&self, path: &str) -> Result<RoutingDecision> {
        let location = self.config.client_location.ok_or_else(|| {
            StrataError::InvalidConfig {
                field: "client_location".to_string(),
                reason: "Required for nearest cluster routing".to_string(),
            }
        })?;

        let nearest = self.registry.find_nearest(&location).await.ok_or_else(|| {
            StrataError::Internal("No healthy clusters available".to_string())
        })?;

        debug!(path = %path, target = %nearest.id, "Routed to nearest cluster");

        Ok(RoutingDecision {
            is_local: nearest.id == self.config.local_cluster_id,
            target: nearest,
            matched_rule: None,
            reason: "Nearest cluster by geographic distance".to_string(),
        })
    }

    /// Route to least loaded cluster.
    async fn route_least_loaded(&self, path: &str) -> Result<RoutingDecision> {
        let least_loaded = self
            .registry
            .find_least_loaded()
            .await
            .ok_or_else(|| StrataError::Internal("No healthy clusters available".to_string()))?;

        debug!(path = %path, target = %least_loaded.id, "Routed to least loaded cluster");

        Ok(RoutingDecision {
            is_local: least_loaded.id == self.config.local_cluster_id,
            target: least_loaded,
            matched_rule: None,
            reason: "Least loaded cluster".to_string(),
        })
    }

    /// Route using round-robin strategy.
    async fn route_round_robin(&self, path: &str) -> Result<RoutingDecision> {
        let clusters = self.registry.list_healthy_clusters().await;
        if clusters.is_empty() {
            return Err(StrataError::Internal("No healthy clusters available".to_string()));
        }

        let mut counter = self.round_robin_counter.write().await;
        let index = *counter % clusters.len();
        *counter = counter.wrapping_add(1);
        let target = clusters[index].clone();

        debug!(path = %path, target = %target.id, index = index, "Routed via round-robin");

        Ok(RoutingDecision {
            is_local: target.id == self.config.local_cluster_id,
            target,
            matched_rule: None,
            reason: format!("Round-robin (index {})", index),
        })
    }

    /// Route using consistent hashing.
    async fn route_consistent_hash(&self, path: &str) -> Result<RoutingDecision> {
        let clusters = self.registry.list_healthy_clusters().await;
        if clusters.is_empty() {
            return Err(StrataError::Internal("No healthy clusters available".to_string()));
        }

        // Simple hash-based selection
        let hash = self.hash_path(path);
        let index = (hash as usize) % clusters.len();
        let target = clusters[index].clone();

        debug!(path = %path, target = %target.id, hash = hash, "Routed via consistent hash");

        Ok(RoutingDecision {
            is_local: target.id == self.config.local_cluster_id,
            target,
            matched_rule: None,
            reason: "Consistent hash".to_string(),
        })
    }

    /// Route to local cluster.
    async fn route_local(&self, path: &str) -> Result<RoutingDecision> {
        let local = self
            .registry
            .get_cluster(&self.config.local_cluster_id)
            .await
            .ok_or_else(|| {
                StrataError::Internal("Local cluster not registered".to_string())
            })?;

        debug!(path = %path, "Routed to local cluster");

        Ok(RoutingDecision {
            is_local: true,
            target: local,
            matched_rule: None,
            reason: "Local cluster".to_string(),
        })
    }

    /// Simple path hashing for consistent hash routing.
    fn hash_path(&self, path: &str) -> u64 {
        let mut hash: u64 = 5381;
        for byte in path.bytes() {
            hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
        }
        hash
    }

    /// Check route cache.
    async fn check_cache(&self, path: &str) -> Option<ClusterId> {
        let cache = self.route_cache.read().await;
        if let Some(cached) = cache.get(path) {
            if cached.cached_at.elapsed().as_secs() < self.config.cache_ttl_secs {
                return Some(cached.cluster_id.clone());
            }
        }
        None
    }

    /// Update route cache.
    async fn update_cache(&self, path: &str, cluster_id: ClusterId) {
        if self.config.enable_caching {
            let mut cache = self.route_cache.write().await;
            cache.insert(
                path.to_string(),
                CachedRoute {
                    cluster_id,
                    cached_at: std::time::Instant::now(),
                },
            );
        }
    }

    /// Clear the route cache.
    pub async fn clear_cache(&self) {
        self.route_cache.write().await.clear();
        info!("Route cache cleared");
    }

    /// Get routing statistics.
    pub async fn get_stats(&self) -> RouterStats {
        let rules = self.rules.read().await;
        let cache = self.route_cache.read().await;

        RouterStats {
            total_rules: rules.len(),
            enabled_rules: rules.iter().filter(|r| r.enabled).count(),
            cached_routes: cache.len(),
            strategy: self.config.default_strategy,
        }
    }
}

/// Router statistics.
#[derive(Debug, Clone)]
pub struct RouterStats {
    pub total_rules: usize,
    pub enabled_rules: usize,
    pub cached_routes: usize,
    pub strategy: RoutingStrategy,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::federation::cluster_registry::{
        ClusterCapabilities, ClusterRegistryConfig,
    };
    use chrono::Utc;

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

    #[test]
    fn test_pattern_matching() {
        // Exact match
        assert!(pattern_matches("/data/files", "/data/files"));
        assert!(!pattern_matches("/data/files", "/data/other"));

        // Single wildcard
        assert!(pattern_matches("/data/*", "/data/files"));
        assert!(pattern_matches("/data/*/info", "/data/files/info"));
        assert!(!pattern_matches("/data/*/info", "/data/files/other"));

        // Recursive wildcard
        assert!(pattern_matches("/data/**", "/data/files"));
        assert!(pattern_matches("/data/**", "/data/files/nested/deep"));
        assert!(!pattern_matches("/other/**", "/data/files"));

        // Root pattern
        assert!(pattern_matches("/**", "/anything/at/all"));
    }

    #[tokio::test]
    async fn test_routing_rule_matching() {
        let rule = RoutingRule {
            id: "test".to_string(),
            path_pattern: "/europe/**".to_string(),
            target_cluster: Some(ClusterId::new("eu-cluster")),
            priority: 10,
            enabled: true,
            description: "Route Europe data".to_string(),
        };

        assert!(rule.matches("/europe/data/files"));
        assert!(rule.matches("/europe"));
        assert!(!rule.matches("/america/data"));
    }

    #[tokio::test]
    async fn test_disabled_rule() {
        let rule = RoutingRule {
            id: "test".to_string(),
            path_pattern: "/data/**".to_string(),
            target_cluster: Some(ClusterId::new("target")),
            priority: 10,
            enabled: false,
            description: "Disabled rule".to_string(),
        };

        assert!(!rule.matches("/data/files"));
    }

    #[tokio::test]
    async fn test_router_add_remove_rules() {
        let reg_config = ClusterRegistryConfig::default();
        let (registry, _) = ClusterRegistry::new(reg_config);
        let registry = Arc::new(registry);

        let config = NamespaceRouterConfig::default();
        let router = NamespaceRouter::new(config, registry);

        let rule = RoutingRule {
            id: "rule1".to_string(),
            path_pattern: "/test/**".to_string(),
            target_cluster: None,
            priority: 10,
            enabled: true,
            description: "Test rule".to_string(),
        };

        router.add_rule(rule).await;
        let rules = router.list_rules().await;
        assert_eq!(rules.len(), 1);

        router.remove_rule("rule1").await;
        let rules = router.list_rules().await;
        assert!(rules.is_empty());
    }

    #[tokio::test]
    async fn test_consistent_hash_stability() {
        let reg_config = ClusterRegistryConfig::default();
        let (registry, _) = ClusterRegistry::new(reg_config);
        let registry = Arc::new(registry);

        let config = NamespaceRouterConfig {
            default_strategy: RoutingStrategy::ConsistentHash,
            ..Default::default()
        };
        let router = NamespaceRouter::new(config, Arc::clone(&registry));

        // Register clusters
        registry
            .register(create_test_cluster_info("cluster-1", "us-east-1"))
            .await
            .unwrap();
        registry
            .register(create_test_cluster_info("cluster-2", "eu-west-1"))
            .await
            .unwrap();

        // Same path should route to same cluster
        let hash1 = router.hash_path("/data/file.txt");
        let hash2 = router.hash_path("/data/file.txt");
        assert_eq!(hash1, hash2);

        // Different paths should (usually) have different hashes
        let hash3 = router.hash_path("/other/path.txt");
        assert_ne!(hash1, hash3);
    }
}
