//! Global Namespace Federation
//!
//! Provides unified namespace across geographically distributed storage
//! clusters with automatic routing, conflict resolution, and consistency.
//!
//! # Features
//!
//! - Single global namespace across clusters
//! - Intelligent request routing
//! - Conflict resolution strategies
//! - Read/write affinity policies
//! - Cross-region replication
//!
//! # Example
//!
//! ```rust,ignore
//! use strata::global::{GlobalNamespace, FederationConfig, Region};
//!
//! let ns = GlobalNamespace::new(FederationConfig::default())
//!     .join_region(Region::new("us-east-1"))
//!     .join_region(Region::new("eu-west-1"))
//!     .join_region(Region::new("ap-south-1"));
//!
//! // Access files transparently
//! let data = ns.read("/shared/data/file.txt").await?;
//!
//! // Write with automatic placement
//! ns.write("/shared/data/new_file.txt", data).await?;
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Global namespace manager
pub struct GlobalNamespace {
    /// Federation configuration
    config: FederationConfig,
    /// Registered regions
    regions: Arc<RwLock<HashMap<String, Region>>>,
    /// Namespace tree
    namespace: Arc<RwLock<NamespaceTree>>,
    /// Routing table
    router: Arc<RwLock<Router>>,
    /// Conflict resolver
    conflict_resolver: ConflictResolver,
    /// Replication manager
    replication: ReplicationManager,
    /// Metrics
    metrics: Arc<RwLock<FederationMetrics>>,
}

/// Federation configuration
#[derive(Debug, Clone)]
pub struct FederationConfig {
    /// Namespace prefix
    pub namespace_prefix: String,
    /// Default consistency level
    pub consistency: ConsistencyLevel,
    /// Default replication factor
    pub replication_factor: u32,
    /// Routing policy
    pub routing_policy: RoutingPolicy,
    /// Conflict resolution strategy
    pub conflict_strategy: ConflictStrategy,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Maximum latency for region
    pub max_latency: Duration,
}

/// Consistency levels
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConsistencyLevel {
    /// Read from any region
    Eventual,
    /// Read from quorum
    Quorum,
    /// Read from all regions
    Strong,
    /// Read from local region only
    Local,
    /// Read your writes consistency
    Session,
}

/// Routing policies
#[derive(Debug, Clone)]
pub enum RoutingPolicy {
    /// Route to nearest region
    LatencyBased,
    /// Route based on data location
    DataLocality,
    /// Round robin
    RoundRobin,
    /// Route based on load
    LoadBased,
    /// Custom routing function
    Custom(String),
}

/// Conflict resolution strategies
#[derive(Debug, Clone)]
pub enum ConflictStrategy {
    /// Last write wins
    LastWriteWins,
    /// First write wins
    FirstWriteWins,
    /// Merge changes
    Merge,
    /// Keep all versions
    MultiVersion,
    /// Custom resolver
    Custom(String),
}

/// Region in the federation
#[derive(Debug, Clone)]
pub struct Region {
    /// Region ID
    pub id: String,
    /// Region name
    pub name: String,
    /// Region location
    pub location: GeoLocation,
    /// Endpoints
    pub endpoints: Vec<String>,
    /// Status
    pub status: RegionStatus,
    /// Capabilities
    pub capabilities: RegionCapabilities,
    /// Current metrics
    pub metrics: RegionMetrics,
    /// Last health check
    pub last_health_check: Option<Instant>,
}

/// Geographic location
#[derive(Debug, Clone)]
pub struct GeoLocation {
    pub latitude: f64,
    pub longitude: f64,
    pub cloud_provider: Option<String>,
    pub availability_zone: Option<String>,
}

/// Region status
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RegionStatus {
    Online,
    Degraded,
    Offline,
    Syncing,
    Draining,
}

/// Region capabilities
#[derive(Debug, Clone)]
pub struct RegionCapabilities {
    pub read_enabled: bool,
    pub write_enabled: bool,
    pub storage_capacity_gb: u64,
    pub max_throughput_mbps: u64,
    pub supported_storage_classes: Vec<String>,
}

/// Region metrics
#[derive(Debug, Clone, Default)]
pub struct RegionMetrics {
    pub latency_ms: f64,
    pub throughput_mbps: f64,
    pub error_rate: f64,
    pub storage_used_gb: u64,
    pub active_connections: u64,
}

/// Namespace entry
#[derive(Debug, Clone)]
pub struct NamespaceEntry {
    /// Entry path
    pub path: String,
    /// Entry type
    pub entry_type: EntryType,
    /// Primary region
    pub primary_region: String,
    /// Replica regions
    pub replica_regions: Vec<String>,
    /// Version vector
    pub version: VersionVector,
    /// Metadata
    pub metadata: EntryMetadata,
    /// Pinned regions (must exist here)
    pub pinned_regions: HashSet<String>,
}

/// Entry type
#[derive(Debug, Clone)]
pub enum EntryType {
    File { size: u64 },
    Directory,
    Symlink { target: String },
    Mount { source: String },
}

/// Entry metadata
#[derive(Debug, Clone)]
pub struct EntryMetadata {
    pub created_at: u64,
    pub modified_at: u64,
    pub created_by: String,
    pub permissions: u32,
    pub content_type: Option<String>,
    pub checksum: Option<String>,
    pub custom: HashMap<String, String>,
}

/// Version vector for conflict detection
#[derive(Debug, Clone, Default)]
pub struct VersionVector {
    versions: BTreeMap<String, u64>,
}

/// Namespace tree
struct NamespaceTree {
    /// Root entries
    entries: HashMap<String, NamespaceEntry>,
    /// Path index
    path_index: HashMap<String, String>, // path -> entry id
    /// Region index
    region_index: HashMap<String, HashSet<String>>, // region -> entry ids
}

/// Request router
struct Router {
    /// Region latencies
    latencies: HashMap<String, Duration>,
    /// Region loads
    loads: HashMap<String, f64>,
    /// Round robin counter
    rr_counter: usize,
    /// Routing cache
    cache: HashMap<String, String>, // path -> region
}

/// Conflict resolver
struct ConflictResolver {
    strategy: ConflictStrategy,
    pending_conflicts: Vec<Conflict>,
}

/// Conflict record
#[derive(Debug, Clone)]
pub struct Conflict {
    pub path: String,
    pub versions: Vec<ConflictVersion>,
    pub detected_at: Instant,
    pub resolved: bool,
}

/// Conflicting version
#[derive(Debug, Clone)]
pub struct ConflictVersion {
    pub region: String,
    pub version: VersionVector,
    pub timestamp: u64,
    pub checksum: String,
}

/// Replication manager
struct ReplicationManager {
    /// Replication policies
    policies: HashMap<String, ReplicationPolicy>,
    /// Active replications
    active: Vec<ReplicationTask>,
    /// Replication queue
    queue: Vec<ReplicationRequest>,
}

/// Replication policy
#[derive(Debug, Clone)]
pub struct ReplicationPolicy {
    /// Policy name
    pub name: String,
    /// Path pattern
    pub path_pattern: String,
    /// Target regions
    pub target_regions: Vec<String>,
    /// Replication mode
    pub mode: ReplicationMode,
    /// Priority
    pub priority: u32,
}

/// Replication mode
#[derive(Debug, Clone)]
pub enum ReplicationMode {
    /// Replicate synchronously
    Sync,
    /// Replicate asynchronously
    Async,
    /// Replicate on schedule
    Scheduled { cron: String },
    /// Replicate on demand
    OnDemand,
}

/// Replication task
struct ReplicationTask {
    id: String,
    source_region: String,
    target_region: String,
    path: String,
    status: ReplicationStatus,
    started_at: Instant,
    bytes_transferred: u64,
}

/// Replication status
#[derive(Debug, Clone, Copy)]
enum ReplicationStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// Replication request
struct ReplicationRequest {
    path: String,
    source: String,
    targets: Vec<String>,
    priority: u32,
}

/// Federation metrics
#[derive(Debug, Clone, Default)]
pub struct FederationMetrics {
    pub total_entries: u64,
    pub total_regions: usize,
    pub online_regions: usize,
    pub total_replications: u64,
    pub pending_replications: u64,
    pub conflicts_detected: u64,
    pub conflicts_resolved: u64,
    pub cross_region_reads: u64,
    pub cross_region_writes: u64,
}

/// Federation operation result
#[derive(Debug)]
pub struct OperationResult {
    pub success: bool,
    pub region: String,
    pub latency: Duration,
    pub version: VersionVector,
}

impl GlobalNamespace {
    /// Create a new global namespace
    pub fn new(config: FederationConfig) -> Self {
        Self {
            config,
            regions: Arc::new(RwLock::new(HashMap::new())),
            namespace: Arc::new(RwLock::new(NamespaceTree::new())),
            router: Arc::new(RwLock::new(Router::new())),
            conflict_resolver: ConflictResolver::new(ConflictStrategy::LastWriteWins),
            replication: ReplicationManager::new(),
            metrics: Arc::new(RwLock::new(FederationMetrics::default())),
        }
    }

    /// Join a region to the federation
    pub fn join_region(self, region: Region) -> Self {
        {
            let mut regions = self.regions.write().unwrap();
            let mut router = self.router.write().unwrap();

            router.latencies.insert(region.id.clone(), Duration::from_millis(region.metrics.latency_ms as u64));
            router.loads.insert(region.id.clone(), 0.0);

            regions.insert(region.id.clone(), region);
        }

        {
            let mut metrics = self.metrics.write().unwrap();
            metrics.total_regions += 1;
            metrics.online_regions += 1;
        }

        self
    }

    /// Remove a region from federation
    pub fn leave_region(&self, region_id: &str) -> Result<(), FederationError> {
        let mut regions = self.regions.write().unwrap();

        if regions.remove(region_id).is_none() {
            return Err(FederationError::RegionNotFound(region_id.to_string()));
        }

        let mut metrics = self.metrics.write().unwrap();
        metrics.total_regions = metrics.total_regions.saturating_sub(1);
        metrics.online_regions = metrics.online_regions.saturating_sub(1);

        Ok(())
    }

    /// Read a file from the namespace
    pub fn read(&self, path: &str) -> Result<ReadResult, FederationError> {
        let normalized = self.normalize_path(path);
        let start = Instant::now();

        // Find entry
        let namespace = self.namespace.read().unwrap();
        let entry = namespace.entries.values()
            .find(|e| e.path == normalized)
            .cloned()
            .ok_or_else(|| FederationError::NotFound(normalized.clone()))?;
        drop(namespace);

        // Route to best region
        let region = self.route_read(&entry)?;

        // Simulate read from region
        let data = self.read_from_region(&region, &normalized)?;

        let latency = start.elapsed();

        // Update metrics
        if region != self.local_region().unwrap_or_default() {
            let mut metrics = self.metrics.write().unwrap();
            metrics.cross_region_reads += 1;
        }

        Ok(ReadResult {
            path: normalized,
            data,
            region,
            latency,
            version: entry.version,
        })
    }

    /// Write a file to the namespace
    pub fn write(&self, path: &str, data: Vec<u8>) -> Result<WriteResult, FederationError> {
        let normalized = self.normalize_path(path);
        let start = Instant::now();
        let size = data.len() as u64;

        // Determine target regions
        let target_regions = self.determine_write_regions(&normalized)?;

        // Create or update entry
        let version = self.create_or_update_entry(&normalized, size, &target_regions)?;

        // Write to primary region
        let primary = target_regions.first()
            .ok_or_else(|| FederationError::NoAvailableRegions)?;

        self.write_to_region(primary, &normalized, &data)?;

        // Queue replication to other regions
        for region in target_regions.iter().skip(1) {
            self.queue_replication(&normalized, primary, region);
        }

        let latency = start.elapsed();

        // Update metrics
        {
            let mut metrics = self.metrics.write().unwrap();
            if target_regions.len() > 1 {
                metrics.cross_region_writes += 1;
            }
            metrics.pending_replications += (target_regions.len() - 1) as u64;
        }

        Ok(WriteResult {
            path: normalized,
            size,
            primary_region: primary.clone(),
            replica_regions: target_regions[1..].to_vec(),
            latency,
            version,
        })
    }

    /// Delete a file from the namespace
    pub fn delete(&self, path: &str) -> Result<(), FederationError> {
        let normalized = self.normalize_path(path);

        // Find and remove entry
        let mut namespace = self.namespace.write().unwrap();

        let entry = namespace.entries.values()
            .find(|e| e.path == normalized)
            .cloned()
            .ok_or_else(|| FederationError::NotFound(normalized.clone()))?;

        // Remove from all regions
        for region_id in std::iter::once(&entry.primary_region).chain(entry.replica_regions.iter()) {
            self.delete_from_region(region_id, &normalized)?;
        }

        // Remove from namespace
        namespace.entries.retain(|_, e| e.path != normalized);
        namespace.path_index.remove(&normalized);

        Ok(())
    }

    /// List directory contents
    pub fn list(&self, path: &str) -> Result<Vec<DirectoryEntry>, FederationError> {
        let normalized = self.normalize_path(path);

        let namespace = self.namespace.read().unwrap();

        let entries: Vec<DirectoryEntry> = namespace.entries.values()
            .filter(|e| {
                if normalized == "/" {
                    // Root: get top-level entries
                    let parts: Vec<_> = e.path.split('/').filter(|p| !p.is_empty()).collect();
                    parts.len() == 1
                } else {
                    // Get direct children
                    e.path.starts_with(&normalized) && e.path != normalized && {
                        let remaining = &e.path[normalized.len()..];
                        let parts: Vec<_> = remaining.split('/').filter(|p| !p.is_empty()).collect();
                        parts.len() == 1
                    }
                }
            })
            .map(|e| DirectoryEntry {
                name: e.path.rsplit('/').next().unwrap_or("").to_string(),
                path: e.path.clone(),
                entry_type: e.entry_type.clone(),
                size: match &e.entry_type {
                    EntryType::File { size } => *size,
                    _ => 0,
                },
                modified_at: e.metadata.modified_at,
                primary_region: e.primary_region.clone(),
            })
            .collect();

        Ok(entries)
    }

    /// Get entry info
    pub fn stat(&self, path: &str) -> Result<StatResult, FederationError> {
        let normalized = self.normalize_path(path);

        let namespace = self.namespace.read().unwrap();

        let entry = namespace.entries.values()
            .find(|e| e.path == normalized)
            .ok_or_else(|| FederationError::NotFound(normalized.clone()))?;

        Ok(StatResult {
            path: entry.path.clone(),
            entry_type: entry.entry_type.clone(),
            primary_region: entry.primary_region.clone(),
            replica_regions: entry.replica_regions.clone(),
            version: entry.version.clone(),
            metadata: entry.metadata.clone(),
        })
    }

    /// Move/rename entry
    pub fn rename(&self, old_path: &str, new_path: &str) -> Result<(), FederationError> {
        let old_normalized = self.normalize_path(old_path);
        let new_normalized = self.normalize_path(new_path);

        let mut namespace = self.namespace.write().unwrap();

        // Find entry
        let entry_id = namespace.path_index.get(&old_normalized)
            .cloned()
            .ok_or_else(|| FederationError::NotFound(old_normalized.clone()))?;

        // Check new path doesn't exist
        if namespace.path_index.contains_key(&new_normalized) {
            return Err(FederationError::AlreadyExists(new_normalized));
        }

        // Update entry
        if let Some(entry) = namespace.entries.get_mut(&entry_id) {
            entry.path = new_normalized.clone();
            entry.metadata.modified_at = current_timestamp();
            entry.version.increment("local");
        }

        // Update index
        namespace.path_index.remove(&old_normalized);
        namespace.path_index.insert(new_normalized, entry_id);

        Ok(())
    }

    /// Create directory
    pub fn mkdir(&self, path: &str) -> Result<(), FederationError> {
        let normalized = self.normalize_path(path);

        let mut namespace = self.namespace.write().unwrap();

        // Check doesn't exist
        if namespace.path_index.contains_key(&normalized) {
            return Err(FederationError::AlreadyExists(normalized));
        }

        let regions = self.regions.read().unwrap();
        let primary = regions.keys().next()
            .cloned()
            .ok_or(FederationError::NoAvailableRegions)?;
        drop(regions);

        let entry_id = uuid_v4();
        let entry = NamespaceEntry {
            path: normalized.clone(),
            entry_type: EntryType::Directory,
            primary_region: primary,
            replica_regions: Vec::new(),
            version: VersionVector::new(),
            metadata: EntryMetadata {
                created_at: current_timestamp(),
                modified_at: current_timestamp(),
                created_by: "system".to_string(),
                permissions: 0o755,
                content_type: None,
                checksum: None,
                custom: HashMap::new(),
            },
            pinned_regions: HashSet::new(),
        };

        namespace.entries.insert(entry_id.clone(), entry);
        namespace.path_index.insert(normalized, entry_id);

        Ok(())
    }

    /// Get region info
    pub fn get_region(&self, region_id: &str) -> Option<Region> {
        let regions = self.regions.read().unwrap();
        regions.get(region_id).cloned()
    }

    /// List all regions
    pub fn list_regions(&self) -> Vec<Region> {
        let regions = self.regions.read().unwrap();
        regions.values().cloned().collect()
    }

    /// Update region status
    pub fn update_region_status(&self, region_id: &str, status: RegionStatus) -> Result<(), FederationError> {
        let mut regions = self.regions.write().unwrap();

        let region = regions.get_mut(region_id)
            .ok_or_else(|| FederationError::RegionNotFound(region_id.to_string()))?;

        let old_status = region.status;
        region.status = status;

        // Update metrics
        let mut metrics = self.metrics.write().unwrap();
        if old_status == RegionStatus::Online && status != RegionStatus::Online {
            metrics.online_regions = metrics.online_regions.saturating_sub(1);
        } else if old_status != RegionStatus::Online && status == RegionStatus::Online {
            metrics.online_regions += 1;
        }

        Ok(())
    }

    /// Check for conflicts
    pub fn check_conflicts(&self) -> Vec<Conflict> {
        self.conflict_resolver.pending_conflicts.clone()
    }

    /// Resolve a conflict
    pub fn resolve_conflict(&self, path: &str, resolution: ConflictResolution) -> Result<(), FederationError> {
        let mut namespace = self.namespace.write().unwrap();

        let entry = namespace.entries.values_mut()
            .find(|e| e.path == path)
            .ok_or_else(|| FederationError::NotFound(path.to_string()))?;

        match resolution {
            ConflictResolution::UseVersion(region) => {
                entry.primary_region = region;
                entry.version.increment("local");
            }
            ConflictResolution::Merge => {
                entry.version.increment("local");
            }
            ConflictResolution::KeepBoth => {
                // Would create a copy with different name
            }
        }

        entry.metadata.modified_at = current_timestamp();

        let mut metrics = self.metrics.write().unwrap();
        metrics.conflicts_resolved += 1;

        Ok(())
    }

    /// Add replication policy
    pub fn add_replication_policy(&mut self, policy: ReplicationPolicy) {
        self.replication.policies.insert(policy.name.clone(), policy);
    }

    /// Get metrics
    pub fn metrics(&self) -> FederationMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Process pending replications
    pub fn process_replications(&self) -> usize {
        // Would process queued replications
        0
    }

    fn normalize_path(&self, path: &str) -> String {
        let path = path.trim();
        let path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}", path)
        };

        // Remove trailing slash unless root
        if path.len() > 1 && path.ends_with('/') {
            path[..path.len()-1].to_string()
        } else {
            path
        }
    }

    fn local_region(&self) -> Option<String> {
        let regions = self.regions.read().unwrap();
        regions.keys().next().cloned()
    }

    fn route_read(&self, entry: &NamespaceEntry) -> Result<String, FederationError> {
        let router = self.router.read().unwrap();
        let regions = self.regions.read().unwrap();

        // Get available regions for this entry
        let available: Vec<_> = std::iter::once(&entry.primary_region)
            .chain(entry.replica_regions.iter())
            .filter(|r| {
                regions.get(*r)
                    .map(|region| region.status == RegionStatus::Online)
                    .unwrap_or(false)
            })
            .collect();

        if available.is_empty() {
            return Err(FederationError::NoAvailableRegions);
        }

        // Route based on policy
        match &self.config.routing_policy {
            RoutingPolicy::LatencyBased => {
                available.iter()
                    .min_by_key(|r| router.latencies.get(**r).cloned().unwrap_or(Duration::MAX))
                    .map(|r| (*r).clone())
                    .ok_or(FederationError::NoAvailableRegions)
            }
            RoutingPolicy::DataLocality => {
                // Prefer primary region
                Ok(entry.primary_region.clone())
            }
            RoutingPolicy::LoadBased => {
                available.iter()
                    .min_by(|a, b| {
                        let load_a = router.loads.get(**a).unwrap_or(&1.0);
                        let load_b = router.loads.get(**b).unwrap_or(&1.0);
                        load_a.partial_cmp(load_b).unwrap()
                    })
                    .map(|r| (*r).clone())
                    .ok_or(FederationError::NoAvailableRegions)
            }
            RoutingPolicy::RoundRobin => {
                let idx = router.rr_counter % available.len();
                Ok(available[idx].clone())
            }
            RoutingPolicy::Custom(_) => {
                Ok(entry.primary_region.clone())
            }
        }
    }

    fn determine_write_regions(&self, _path: &str) -> Result<Vec<String>, FederationError> {
        let regions = self.regions.read().unwrap();

        let available: Vec<_> = regions.values()
            .filter(|r| r.status == RegionStatus::Online && r.capabilities.write_enabled)
            .map(|r| r.id.clone())
            .take(self.config.replication_factor as usize)
            .collect();

        if available.is_empty() {
            return Err(FederationError::NoAvailableRegions);
        }

        Ok(available)
    }

    fn create_or_update_entry(
        &self,
        path: &str,
        size: u64,
        regions: &[String],
    ) -> Result<VersionVector, FederationError> {
        let mut namespace = self.namespace.write().unwrap();

        let primary = regions.first().cloned().ok_or(FederationError::NoAvailableRegions)?;
        let replicas = regions[1..].to_vec();

        if let Some(entry) = namespace.entries.values_mut().find(|e| e.path == path) {
            // Update existing
            entry.entry_type = EntryType::File { size };
            entry.version.increment(&primary);
            entry.metadata.modified_at = current_timestamp();
            Ok(entry.version.clone())
        } else {
            // Create new
            let entry_id = uuid_v4();
            let mut version = VersionVector::new();
            version.increment(&primary);

            let entry = NamespaceEntry {
                path: path.to_string(),
                entry_type: EntryType::File { size },
                primary_region: primary,
                replica_regions: replicas,
                version: version.clone(),
                metadata: EntryMetadata {
                    created_at: current_timestamp(),
                    modified_at: current_timestamp(),
                    created_by: "system".to_string(),
                    permissions: 0o644,
                    content_type: None,
                    checksum: None,
                    custom: HashMap::new(),
                },
                pinned_regions: HashSet::new(),
            };

            namespace.entries.insert(entry_id.clone(), entry);
            namespace.path_index.insert(path.to_string(), entry_id);

            let mut metrics = self.metrics.write().unwrap();
            metrics.total_entries += 1;

            Ok(version)
        }
    }

    fn read_from_region(&self, _region: &str, _path: &str) -> Result<Vec<u8>, FederationError> {
        // Simulated read
        Ok(vec![0u8; 100])
    }

    fn write_to_region(&self, _region: &str, _path: &str, _data: &[u8]) -> Result<(), FederationError> {
        // Simulated write
        Ok(())
    }

    fn delete_from_region(&self, _region: &str, _path: &str) -> Result<(), FederationError> {
        // Simulated delete
        Ok(())
    }

    fn queue_replication(&self, _path: &str, _source: &str, _target: &str) {
        // Would queue replication
    }
}

impl Default for FederationConfig {
    fn default() -> Self {
        Self {
            namespace_prefix: "/global".to_string(),
            consistency: ConsistencyLevel::Eventual,
            replication_factor: 3,
            routing_policy: RoutingPolicy::LatencyBased,
            conflict_strategy: ConflictStrategy::LastWriteWins,
            health_check_interval: Duration::from_secs(10),
            max_latency: Duration::from_millis(500),
        }
    }
}

impl Region {
    /// Create a new region
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        Self {
            id: id.clone(),
            name: id,
            location: GeoLocation {
                latitude: 0.0,
                longitude: 0.0,
                cloud_provider: None,
                availability_zone: None,
            },
            endpoints: Vec::new(),
            status: RegionStatus::Online,
            capabilities: RegionCapabilities {
                read_enabled: true,
                write_enabled: true,
                storage_capacity_gb: 1000,
                max_throughput_mbps: 1000,
                supported_storage_classes: vec!["standard".to_string()],
            },
            metrics: RegionMetrics::default(),
            last_health_check: None,
        }
    }

    /// Set location
    pub fn with_location(mut self, lat: f64, lon: f64) -> Self {
        self.location.latitude = lat;
        self.location.longitude = lon;
        self
    }

    /// Add endpoint
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoints.push(endpoint.into());
        self
    }

    /// Set cloud provider
    pub fn with_cloud_provider(mut self, provider: impl Into<String>) -> Self {
        self.location.cloud_provider = Some(provider.into());
        self
    }
}

impl VersionVector {
    fn new() -> Self {
        Self {
            versions: BTreeMap::new(),
        }
    }

    fn increment(&mut self, region: &str) {
        let counter = self.versions.entry(region.to_string()).or_insert(0);
        *counter += 1;
    }

    fn merge(&mut self, other: &VersionVector) {
        for (region, &version) in &other.versions {
            let current = self.versions.entry(region.clone()).or_insert(0);
            *current = (*current).max(version);
        }
    }

    fn compare(&self, other: &VersionVector) -> VersionComparison {
        let mut dominated = true;
        let mut dominates = true;

        for (region, &v1) in &self.versions {
            if let Some(&v2) = other.versions.get(region) {
                if v1 < v2 {
                    dominates = false;
                } else if v1 > v2 {
                    dominated = false;
                }
            } else if v1 > 0 {
                dominated = false;
            }
        }

        for (region, &v2) in &other.versions {
            if !self.versions.contains_key(region) && v2 > 0 {
                dominates = false;
            }
        }

        if dominated && dominates {
            VersionComparison::Equal
        } else if dominated {
            VersionComparison::Before
        } else if dominates {
            VersionComparison::After
        } else {
            VersionComparison::Concurrent
        }
    }
}

/// Version comparison result
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VersionComparison {
    Before,
    After,
    Equal,
    Concurrent,
}

impl NamespaceTree {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            path_index: HashMap::new(),
            region_index: HashMap::new(),
        }
    }
}

impl Router {
    fn new() -> Self {
        Self {
            latencies: HashMap::new(),
            loads: HashMap::new(),
            rr_counter: 0,
            cache: HashMap::new(),
        }
    }
}

impl ConflictResolver {
    fn new(strategy: ConflictStrategy) -> Self {
        Self {
            strategy,
            pending_conflicts: Vec::new(),
        }
    }
}

impl ReplicationManager {
    fn new() -> Self {
        Self {
            policies: HashMap::new(),
            active: Vec::new(),
            queue: Vec::new(),
        }
    }
}

/// Read result
#[derive(Debug)]
pub struct ReadResult {
    pub path: String,
    pub data: Vec<u8>,
    pub region: String,
    pub latency: Duration,
    pub version: VersionVector,
}

/// Write result
#[derive(Debug)]
pub struct WriteResult {
    pub path: String,
    pub size: u64,
    pub primary_region: String,
    pub replica_regions: Vec<String>,
    pub latency: Duration,
    pub version: VersionVector,
}

/// Directory entry
#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    pub name: String,
    pub path: String,
    pub entry_type: EntryType,
    pub size: u64,
    pub modified_at: u64,
    pub primary_region: String,
}

/// Stat result
#[derive(Debug)]
pub struct StatResult {
    pub path: String,
    pub entry_type: EntryType,
    pub primary_region: String,
    pub replica_regions: Vec<String>,
    pub version: VersionVector,
    pub metadata: EntryMetadata,
}

/// Conflict resolution
#[derive(Debug)]
pub enum ConflictResolution {
    UseVersion(String),
    Merge,
    KeepBoth,
}

/// Federation error
#[derive(Debug)]
pub enum FederationError {
    NotFound(String),
    AlreadyExists(String),
    RegionNotFound(String),
    NoAvailableRegions,
    ReplicationFailed(String),
    ConflictDetected(String),
    PermissionDenied,
    Other(String),
}

impl std::fmt::Display for FederationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(p) => write!(f, "Not found: {}", p),
            Self::AlreadyExists(p) => write!(f, "Already exists: {}", p),
            Self::RegionNotFound(r) => write!(f, "Region not found: {}", r),
            Self::NoAvailableRegions => write!(f, "No available regions"),
            Self::ReplicationFailed(msg) => write!(f, "Replication failed: {}", msg),
            Self::ConflictDetected(p) => write!(f, "Conflict detected: {}", p),
            Self::PermissionDenied => write!(f, "Permission denied"),
            Self::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for FederationError {}

fn uuid_v4() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:032x}", now)
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_creation() {
        let ns = GlobalNamespace::new(FederationConfig::default())
            .join_region(Region::new("us-east-1"))
            .join_region(Region::new("eu-west-1"));

        let regions = ns.list_regions();
        assert_eq!(regions.len(), 2);
    }

    #[test]
    fn test_write_and_read() {
        let ns = GlobalNamespace::new(FederationConfig::default())
            .join_region(Region::new("us-east-1"));

        // Write
        let data = b"hello world".to_vec();
        let write_result = ns.write("/test/file.txt", data.clone()).unwrap();
        assert!(!write_result.primary_region.is_empty());

        // Read
        let read_result = ns.read("/test/file.txt").unwrap();
        assert_eq!(read_result.path, "/test/file.txt");
    }

    #[test]
    fn test_mkdir_and_list() {
        let ns = GlobalNamespace::new(FederationConfig::default())
            .join_region(Region::new("us-east-1"));

        ns.mkdir("/docs").unwrap();
        ns.write("/docs/readme.txt", vec![1, 2, 3]).unwrap();

        let entries = ns.list("/docs").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "readme.txt");
    }

    #[test]
    fn test_stat() {
        let ns = GlobalNamespace::new(FederationConfig::default())
            .join_region(Region::new("us-east-1"));

        ns.write("/test.txt", vec![1, 2, 3, 4, 5]).unwrap();

        let stat = ns.stat("/test.txt").unwrap();
        assert_eq!(stat.path, "/test.txt");
        assert!(matches!(stat.entry_type, EntryType::File { size: 5 }));
    }

    #[test]
    fn test_rename() {
        let ns = GlobalNamespace::new(FederationConfig::default())
            .join_region(Region::new("us-east-1"));

        ns.write("/old.txt", vec![1, 2, 3]).unwrap();
        ns.rename("/old.txt", "/new.txt").unwrap();

        assert!(ns.read("/old.txt").is_err());
        assert!(ns.read("/new.txt").is_ok());
    }

    #[test]
    fn test_delete() {
        let ns = GlobalNamespace::new(FederationConfig::default())
            .join_region(Region::new("us-east-1"));

        ns.write("/to_delete.txt", vec![1]).unwrap();
        ns.delete("/to_delete.txt").unwrap();

        assert!(ns.read("/to_delete.txt").is_err());
    }

    #[test]
    fn test_version_vector() {
        let mut v1 = VersionVector::new();
        v1.increment("us-east-1");
        v1.increment("us-east-1");

        let mut v2 = VersionVector::new();
        v2.increment("eu-west-1");

        assert_eq!(v1.compare(&v2), VersionComparison::Concurrent);

        v2.merge(&v1);
        assert_eq!(v1.compare(&v2), VersionComparison::Before);
    }

    #[test]
    fn test_region_status() {
        let ns = GlobalNamespace::new(FederationConfig::default())
            .join_region(Region::new("us-east-1"));

        ns.update_region_status("us-east-1", RegionStatus::Degraded).unwrap();

        let region = ns.get_region("us-east-1").unwrap();
        assert_eq!(region.status, RegionStatus::Degraded);
    }

    #[test]
    fn test_replication_config() {
        let config = FederationConfig {
            replication_factor: 5,
            consistency: ConsistencyLevel::Strong,
            ..Default::default()
        };

        let ns = GlobalNamespace::new(config)
            .join_region(Region::new("r1"))
            .join_region(Region::new("r2"))
            .join_region(Region::new("r3"));

        let metrics = ns.metrics();
        assert_eq!(metrics.total_regions, 3);
    }

    #[test]
    fn test_path_normalization() {
        let ns = GlobalNamespace::new(FederationConfig::default())
            .join_region(Region::new("us-east-1"));

        // These should all work the same
        ns.write("test.txt", vec![1]).unwrap();
        assert!(ns.read("/test.txt").is_ok());

        ns.write("/another.txt", vec![2]).unwrap();
        assert!(ns.read("another.txt").is_ok());
    }

    #[test]
    fn test_leave_region() {
        let ns = GlobalNamespace::new(FederationConfig::default())
            .join_region(Region::new("us-east-1"))
            .join_region(Region::new("eu-west-1"));

        assert_eq!(ns.list_regions().len(), 2);

        ns.leave_region("eu-west-1").unwrap();

        assert_eq!(ns.list_regions().len(), 1);
    }
}
