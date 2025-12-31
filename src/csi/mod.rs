//! Kubernetes Container Storage Interface (CSI) driver for Strata.
//!
//! Implements the CSI specification to provide Strata volumes to Kubernetes pods.
//! This enables dynamic provisioning, snapshots, and volume expansion.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Kubernetes Cluster                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │  CSI Controller (Deployment)                                │
//! │  ├── CreateVolume / DeleteVolume                            │
//! │  ├── ControllerPublishVolume / ControllerUnpublishVolume    │
//! │  ├── CreateSnapshot / DeleteSnapshot                        │
//! │  └── ControllerExpandVolume                                 │
//! ├─────────────────────────────────────────────────────────────┤
//! │  CSI Node (DaemonSet)                                       │
//! │  ├── NodeStageVolume / NodeUnstageVolume                    │
//! │  ├── NodePublishVolume / NodeUnpublishVolume                │
//! │  └── NodeExpandVolume                                       │
//! ├─────────────────────────────────────────────────────────────┤
//! │  CSI Identity (Both)                                        │
//! │  ├── GetPluginInfo                                          │
//! │  ├── GetPluginCapabilities                                  │
//! │  └── Probe                                                  │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```yaml
//! # StorageClass
//! apiVersion: storage.k8s.io/v1
//! kind: StorageClass
//! metadata:
//!   name: strata-sc
//! provisioner: strata.storage.io
//! parameters:
//!   erasure_profile: "default"
//!   encryption: "true"
//! reclaimPolicy: Delete
//! allowVolumeExpansion: true
//! volumeBindingMode: Immediate
//! ```

pub mod controller;
pub mod identity;
pub mod node;

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;

/// CSI driver name.
pub const DRIVER_NAME: &str = "strata.storage.io";

/// CSI driver version.
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Volume capability access mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AccessMode {
    /// Single node writer.
    SingleNodeWriter,
    /// Single node reader only.
    SingleNodeReaderOnly,
    /// Multi-node reader only.
    MultiNodeReaderOnly,
    /// Multi-node single writer.
    MultiNodeSingleWriter,
    /// Multi-node multi writer.
    MultiNodeMultiWriter,
}

impl AccessMode {
    /// Check if this mode allows writing.
    pub fn is_writable(&self) -> bool {
        !matches!(
            self,
            Self::SingleNodeReaderOnly | Self::MultiNodeReaderOnly
        )
    }

    /// Check if this mode allows multiple nodes.
    pub fn is_multi_node(&self) -> bool {
        matches!(
            self,
            Self::MultiNodeReaderOnly | Self::MultiNodeSingleWriter | Self::MultiNodeMultiWriter
        )
    }
}

/// Volume capability access type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessType {
    /// Block device.
    Block,
    /// Mounted filesystem.
    Mount {
        /// Filesystem type.
        fs_type: String,
        /// Mount flags.
        mount_flags: Vec<String>,
    },
}

/// Volume capability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeCapability {
    /// Access mode.
    pub access_mode: AccessMode,
    /// Access type.
    pub access_type: AccessType,
}

impl VolumeCapability {
    /// Create a mount capability.
    pub fn mount(access_mode: AccessMode, fs_type: &str, mount_flags: Vec<String>) -> Self {
        Self {
            access_mode,
            access_type: AccessType::Mount {
                fs_type: fs_type.to_string(),
                mount_flags,
            },
        }
    }

    /// Create a block capability.
    pub fn block(access_mode: AccessMode) -> Self {
        Self {
            access_mode,
            access_type: AccessType::Block,
        }
    }
}

/// CSI Volume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Volume {
    /// Unique volume ID.
    pub volume_id: String,
    /// Volume name.
    pub name: String,
    /// Capacity in bytes.
    pub capacity_bytes: i64,
    /// Volume parameters from storage class.
    pub parameters: HashMap<String, String>,
    /// Volume context passed to node.
    pub volume_context: HashMap<String, String>,
    /// Content source (snapshot or volume).
    pub content_source: Option<VolumeContentSource>,
    /// Topology accessibility.
    pub accessible_topology: Vec<Topology>,
    /// Current state.
    pub state: VolumeState,
    /// Creation time.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Volume content source for cloning/restoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeContentSource {
    /// Clone from another volume.
    Volume { volume_id: String },
    /// Restore from snapshot.
    Snapshot { snapshot_id: String },
}

/// Volume state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeState {
    /// Volume is being created.
    Creating,
    /// Volume is ready.
    Ready,
    /// Volume is being deleted.
    Deleting,
    /// Volume is in error state.
    Error,
}

/// Topology segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    /// Topology segments (key-value pairs).
    pub segments: HashMap<String, String>,
}

impl Topology {
    /// Create topology from zone.
    pub fn zone(zone: &str) -> Self {
        let mut segments = HashMap::new();
        segments.insert("topology.kubernetes.io/zone".to_string(), zone.to_string());
        Self { segments }
    }

    /// Create topology from region and zone.
    pub fn region_zone(region: &str, zone: &str) -> Self {
        let mut segments = HashMap::new();
        segments.insert(
            "topology.kubernetes.io/region".to_string(),
            region.to_string(),
        );
        segments.insert("topology.kubernetes.io/zone".to_string(), zone.to_string());
        Self { segments }
    }
}

/// CSI Snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Unique snapshot ID.
    pub snapshot_id: String,
    /// Source volume ID.
    pub source_volume_id: String,
    /// Snapshot name.
    pub name: String,
    /// Creation time.
    pub creation_time: chrono::DateTime<chrono::Utc>,
    /// Ready to use.
    pub ready_to_use: bool,
    /// Size in bytes.
    pub size_bytes: i64,
}

/// Published volume information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedVolume {
    /// Volume ID.
    pub volume_id: String,
    /// Node ID.
    pub node_id: String,
    /// Staging path.
    pub staging_path: Option<PathBuf>,
    /// Target paths (multiple mounts possible).
    pub target_paths: Vec<PathBuf>,
    /// Read-only.
    pub read_only: bool,
}

/// CSI driver capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverCapabilities {
    /// Controller service capabilities.
    pub controller: Vec<ControllerCapability>,
    /// Node service capabilities.
    pub node: Vec<NodeCapability>,
    /// Volume expansion mode.
    pub volume_expansion: VolumeExpansionMode,
}

/// Controller capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ControllerCapability {
    CreateDeleteVolume,
    PublishUnpublishVolume,
    ListVolumes,
    GetCapacity,
    CreateDeleteSnapshot,
    ListSnapshots,
    CloneVolume,
    ExpandVolume,
    GetVolume,
    SingleNodeMultiWriter,
}

/// Node capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeCapability {
    StageUnstageVolume,
    GetVolumeStats,
    ExpandVolume,
    VolumeCondition,
    SingleNodeMultiWriter,
}

/// Volume expansion mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum VolumeExpansionMode {
    Unknown,
    Online,
    Offline,
}

impl Default for DriverCapabilities {
    fn default() -> Self {
        Self {
            controller: vec![
                ControllerCapability::CreateDeleteVolume,
                ControllerCapability::PublishUnpublishVolume,
                ControllerCapability::ListVolumes,
                ControllerCapability::CreateDeleteSnapshot,
                ControllerCapability::ListSnapshots,
                ControllerCapability::CloneVolume,
                ControllerCapability::ExpandVolume,
                ControllerCapability::GetVolume,
            ],
            node: vec![
                NodeCapability::StageUnstageVolume,
                NodeCapability::GetVolumeStats,
                NodeCapability::ExpandVolume,
                NodeCapability::VolumeCondition,
            ],
            volume_expansion: VolumeExpansionMode::Online,
        }
    }
}

/// CSI driver configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsiConfig {
    /// Driver name.
    pub driver_name: String,
    /// Node ID (hostname or unique identifier).
    pub node_id: String,
    /// CSI socket path.
    pub socket_path: PathBuf,
    /// Strata metadata server address.
    pub metadata_addr: String,
    /// Staging directory for volumes.
    pub staging_dir: PathBuf,
    /// Default filesystem type.
    pub default_fs_type: String,
    /// Enable topology awareness.
    pub topology_enabled: bool,
    /// Zone label.
    pub zone: Option<String>,
    /// Region label.
    pub region: Option<String>,
}

impl Default for CsiConfig {
    fn default() -> Self {
        Self {
            driver_name: DRIVER_NAME.to_string(),
            node_id: hostname::get()
                .map(|h| h.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
            socket_path: PathBuf::from("/csi/csi.sock"),
            metadata_addr: "localhost:9000".to_string(),
            staging_dir: PathBuf::from("/var/lib/strata/staging"),
            default_fs_type: "ext4".to_string(),
            topology_enabled: true,
            zone: None,
            region: None,
        }
    }
}

/// Volume store trait for persisting volume state.
#[async_trait::async_trait]
pub trait VolumeStore: Send + Sync {
    /// Get a volume by ID.
    async fn get_volume(&self, volume_id: &str) -> Result<Option<Volume>>;

    /// List all volumes.
    async fn list_volumes(&self) -> Result<Vec<Volume>>;

    /// Create a volume.
    async fn create_volume(&self, volume: Volume) -> Result<()>;

    /// Update a volume.
    async fn update_volume(&self, volume: &Volume) -> Result<()>;

    /// Delete a volume.
    async fn delete_volume(&self, volume_id: &str) -> Result<()>;

    /// Get a snapshot by ID.
    async fn get_snapshot(&self, snapshot_id: &str) -> Result<Option<Snapshot>>;

    /// List all snapshots.
    async fn list_snapshots(&self, source_volume_id: Option<&str>) -> Result<Vec<Snapshot>>;

    /// Create a snapshot.
    async fn create_snapshot(&self, snapshot: Snapshot) -> Result<()>;

    /// Delete a snapshot.
    async fn delete_snapshot(&self, snapshot_id: &str) -> Result<()>;

    /// Get published volume info.
    async fn get_published(&self, volume_id: &str, node_id: &str)
        -> Result<Option<PublishedVolume>>;

    /// Save published volume info.
    async fn set_published(&self, published: PublishedVolume) -> Result<()>;

    /// Remove published volume info.
    async fn remove_published(&self, volume_id: &str, node_id: &str) -> Result<()>;
}

/// In-memory volume store.
pub struct MemoryVolumeStore {
    volumes: RwLock<HashMap<String, Volume>>,
    snapshots: RwLock<HashMap<String, Snapshot>>,
    published: RwLock<HashMap<(String, String), PublishedVolume>>,
}

impl MemoryVolumeStore {
    pub fn new() -> Self {
        Self {
            volumes: RwLock::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
            published: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryVolumeStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl VolumeStore for MemoryVolumeStore {
    async fn get_volume(&self, volume_id: &str) -> Result<Option<Volume>> {
        Ok(self.volumes.read().await.get(volume_id).cloned())
    }

    async fn list_volumes(&self) -> Result<Vec<Volume>> {
        Ok(self.volumes.read().await.values().cloned().collect())
    }

    async fn create_volume(&self, volume: Volume) -> Result<()> {
        self.volumes
            .write()
            .await
            .insert(volume.volume_id.clone(), volume);
        Ok(())
    }

    async fn update_volume(&self, volume: &Volume) -> Result<()> {
        self.volumes
            .write()
            .await
            .insert(volume.volume_id.clone(), volume.clone());
        Ok(())
    }

    async fn delete_volume(&self, volume_id: &str) -> Result<()> {
        self.volumes.write().await.remove(volume_id);
        Ok(())
    }

    async fn get_snapshot(&self, snapshot_id: &str) -> Result<Option<Snapshot>> {
        Ok(self.snapshots.read().await.get(snapshot_id).cloned())
    }

    async fn list_snapshots(&self, source_volume_id: Option<&str>) -> Result<Vec<Snapshot>> {
        let snapshots = self.snapshots.read().await;
        let result: Vec<Snapshot> = match source_volume_id {
            Some(vol_id) => snapshots
                .values()
                .filter(|s| s.source_volume_id == vol_id)
                .cloned()
                .collect(),
            None => snapshots.values().cloned().collect(),
        };
        Ok(result)
    }

    async fn create_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        self.snapshots
            .write()
            .await
            .insert(snapshot.snapshot_id.clone(), snapshot);
        Ok(())
    }

    async fn delete_snapshot(&self, snapshot_id: &str) -> Result<()> {
        self.snapshots.write().await.remove(snapshot_id);
        Ok(())
    }

    async fn get_published(
        &self,
        volume_id: &str,
        node_id: &str,
    ) -> Result<Option<PublishedVolume>> {
        Ok(self
            .published
            .read()
            .await
            .get(&(volume_id.to_string(), node_id.to_string()))
            .cloned())
    }

    async fn set_published(&self, published: PublishedVolume) -> Result<()> {
        self.published.write().await.insert(
            (published.volume_id.clone(), published.node_id.clone()),
            published,
        );
        Ok(())
    }

    async fn remove_published(&self, volume_id: &str, node_id: &str) -> Result<()> {
        self.published
            .write()
            .await
            .remove(&(volume_id.to_string(), node_id.to_string()));
        Ok(())
    }
}

/// The main CSI driver.
pub struct CsiDriver<S: VolumeStore> {
    config: CsiConfig,
    store: Arc<S>,
    capabilities: DriverCapabilities,
}

impl<S: VolumeStore> CsiDriver<S> {
    /// Create a new CSI driver.
    pub fn new(config: CsiConfig, store: Arc<S>) -> Self {
        Self {
            config,
            store,
            capabilities: DriverCapabilities::default(),
        }
    }

    /// Get the driver configuration.
    pub fn config(&self) -> &CsiConfig {
        &self.config
    }

    /// Get the volume store.
    pub fn store(&self) -> Arc<S> {
        Arc::clone(&self.store)
    }

    /// Get driver capabilities.
    pub fn capabilities(&self) -> &DriverCapabilities {
        &self.capabilities
    }

    /// Validate volume capabilities.
    pub fn validate_capabilities(&self, capabilities: &[VolumeCapability]) -> Result<()> {
        for cap in capabilities {
            // Check access mode
            match cap.access_mode {
                AccessMode::MultiNodeMultiWriter => {
                    // Requires special support
                    if !self.capabilities.controller.contains(&ControllerCapability::SingleNodeMultiWriter) {
                        return Err(StrataError::InvalidData(
                            "Multi-node multi-writer not supported".into(),
                        ));
                    }
                }
                _ => {} // Other modes supported
            }

            // Check access type
            match &cap.access_type {
                AccessType::Block => {
                    // Block volumes supported
                }
                AccessType::Mount { fs_type, .. } => {
                    // Validate filesystem type
                    let supported = ["ext4", "xfs", "btrfs"];
                    if !supported.contains(&fs_type.as_str()) {
                        warn!(fs_type, "Unsupported filesystem type, using default");
                    }
                }
            }
        }
        Ok(())
    }

    /// Get node topology.
    pub fn get_topology(&self) -> Option<Topology> {
        if !self.config.topology_enabled {
            return None;
        }

        match (&self.config.region, &self.config.zone) {
            (Some(region), Some(zone)) => Some(Topology::region_zone(region, zone)),
            (None, Some(zone)) => Some(Topology::zone(zone)),
            _ => None,
        }
    }
}

// Re-export submodules
pub use controller::ControllerService;
pub use identity::IdentityService;
pub use node::NodeService;

// Hostname helper
mod hostname {
    pub fn get() -> std::io::Result<std::ffi::OsString> {
        #[cfg(unix)]
        {
            use std::ffi::OsString;
            use std::os::unix::ffi::OsStringExt;

            let mut buf = vec![0u8; 256];
            let result = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut i8, buf.len()) };
            if result != 0 {
                return Err(std::io::Error::last_os_error());
            }
            let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            buf.truncate(len);
            Ok(OsString::from_vec(buf))
        }

        #[cfg(not(unix))]
        {
            Ok(std::ffi::OsString::from("unknown"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_mode() {
        assert!(AccessMode::SingleNodeWriter.is_writable());
        assert!(!AccessMode::SingleNodeReaderOnly.is_writable());
        assert!(AccessMode::MultiNodeMultiWriter.is_multi_node());
        assert!(!AccessMode::SingleNodeWriter.is_multi_node());
    }

    #[test]
    fn test_volume_capability() {
        let cap = VolumeCapability::mount(
            AccessMode::SingleNodeWriter,
            "ext4",
            vec!["-o".to_string(), "noatime".to_string()],
        );

        assert_eq!(cap.access_mode, AccessMode::SingleNodeWriter);
        match cap.access_type {
            AccessType::Mount { fs_type, mount_flags } => {
                assert_eq!(fs_type, "ext4");
                assert_eq!(mount_flags.len(), 2);
            }
            _ => panic!("Expected mount access type"),
        }
    }

    #[test]
    fn test_topology() {
        let topo = Topology::zone("us-west-2a");
        assert!(topo.segments.contains_key("topology.kubernetes.io/zone"));

        let topo = Topology::region_zone("us-west-2", "us-west-2a");
        assert!(topo.segments.contains_key("topology.kubernetes.io/region"));
        assert!(topo.segments.contains_key("topology.kubernetes.io/zone"));
    }

    #[test]
    fn test_driver_capabilities() {
        let caps = DriverCapabilities::default();
        assert!(caps
            .controller
            .contains(&ControllerCapability::CreateDeleteVolume));
        assert!(caps.node.contains(&NodeCapability::StageUnstageVolume));
        assert_eq!(caps.volume_expansion, VolumeExpansionMode::Online);
    }

    #[tokio::test]
    async fn test_memory_volume_store() {
        let store = MemoryVolumeStore::new();

        let volume = Volume {
            volume_id: "vol-123".to_string(),
            name: "test-volume".to_string(),
            capacity_bytes: 1024 * 1024 * 1024,
            parameters: HashMap::new(),
            volume_context: HashMap::new(),
            content_source: None,
            accessible_topology: vec![],
            state: VolumeState::Ready,
            created_at: chrono::Utc::now(),
        };

        // Create
        store.create_volume(volume.clone()).await.unwrap();

        // Get
        let retrieved = store.get_volume("vol-123").await.unwrap().unwrap();
        assert_eq!(retrieved.volume_id, "vol-123");

        // List
        let volumes = store.list_volumes().await.unwrap();
        assert_eq!(volumes.len(), 1);

        // Delete
        store.delete_volume("vol-123").await.unwrap();
        assert!(store.get_volume("vol-123").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_snapshot_store() {
        let store = MemoryVolumeStore::new();

        let snapshot = Snapshot {
            snapshot_id: "snap-123".to_string(),
            source_volume_id: "vol-123".to_string(),
            name: "test-snapshot".to_string(),
            creation_time: chrono::Utc::now(),
            ready_to_use: true,
            size_bytes: 1024 * 1024,
        };

        store.create_snapshot(snapshot).await.unwrap();

        let retrieved = store.get_snapshot("snap-123").await.unwrap().unwrap();
        assert_eq!(retrieved.snapshot_id, "snap-123");

        let snapshots = store.list_snapshots(Some("vol-123")).await.unwrap();
        assert_eq!(snapshots.len(), 1);

        let snapshots = store.list_snapshots(Some("vol-999")).await.unwrap();
        assert_eq!(snapshots.len(), 0);
    }

    #[tokio::test]
    async fn test_csi_driver() {
        let config = CsiConfig::default();
        let store = Arc::new(MemoryVolumeStore::new());
        let driver = CsiDriver::new(config, store);

        // Validate capabilities
        let caps = vec![VolumeCapability::mount(
            AccessMode::SingleNodeWriter,
            "ext4",
            vec![],
        )];
        driver.validate_capabilities(&caps).unwrap();
    }
}
