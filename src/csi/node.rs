//! CSI Node Service implementation.
//!
//! The Node service handles volume operations on individual nodes,
//! including staging, mounting, and gathering node information.

use super::{
    AccessType, NodeCapability, PublishedVolume, VolumeCapability, VolumeStore,
};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Node stage volume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStageVolumeRequest {
    /// Volume ID.
    pub volume_id: String,
    /// Publish context from controller.
    pub publish_context: HashMap<String, String>,
    /// Staging target path.
    pub staging_target_path: PathBuf,
    /// Volume capability.
    pub capability: VolumeCapability,
    /// Secrets.
    #[serde(skip)]
    pub secrets: HashMap<String, String>,
    /// Volume context.
    pub volume_context: HashMap<String, String>,
}

/// Node unstage volume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeUnstageVolumeRequest {
    /// Volume ID.
    pub volume_id: String,
    /// Staging target path.
    pub staging_target_path: PathBuf,
}

/// Node publish volume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePublishVolumeRequest {
    /// Volume ID.
    pub volume_id: String,
    /// Publish context from controller.
    pub publish_context: HashMap<String, String>,
    /// Staging target path.
    pub staging_target_path: Option<PathBuf>,
    /// Target path.
    pub target_path: PathBuf,
    /// Volume capability.
    pub capability: VolumeCapability,
    /// Read-only.
    pub readonly: bool,
    /// Secrets.
    #[serde(skip)]
    pub secrets: HashMap<String, String>,
    /// Volume context.
    pub volume_context: HashMap<String, String>,
}

/// Node unpublish volume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeUnpublishVolumeRequest {
    /// Volume ID.
    pub volume_id: String,
    /// Target path.
    pub target_path: PathBuf,
}

/// Node get volume stats request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeGetVolumeStatsRequest {
    /// Volume ID.
    pub volume_id: String,
    /// Volume path.
    pub volume_path: PathBuf,
    /// Staging target path.
    pub staging_target_path: Option<PathBuf>,
}

/// Node get volume stats response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeGetVolumeStatsResponse {
    /// Usage statistics.
    pub usage: Vec<VolumeUsage>,
    /// Volume condition.
    pub condition: Option<VolumeCondition>,
}

/// Volume usage statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeUsage {
    /// Available capacity.
    pub available: i64,
    /// Total capacity.
    pub total: i64,
    /// Used capacity.
    pub used: i64,
    /// Unit.
    pub unit: UsageUnit,
}

/// Usage unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UsageUnit {
    Bytes,
    Inodes,
}

/// Volume condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeCondition {
    /// Abnormal.
    pub abnormal: bool,
    /// Message.
    pub message: String,
}

/// Node expand volume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExpandVolumeRequest {
    /// Volume ID.
    pub volume_id: String,
    /// Volume path.
    pub volume_path: PathBuf,
    /// Required capacity.
    pub capacity_bytes: i64,
    /// Staging target path.
    pub staging_target_path: Option<PathBuf>,
    /// Volume capability.
    pub capability: VolumeCapability,
    /// Secrets.
    #[serde(skip)]
    pub secrets: HashMap<String, String>,
}

/// Node expand volume response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExpandVolumeResponse {
    /// New capacity.
    pub capacity_bytes: i64,
}

/// Node get info response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeGetInfoResponse {
    /// Node ID.
    pub node_id: String,
    /// Maximum volumes per node.
    pub max_volumes_per_node: i64,
    /// Accessible topology.
    pub accessible_topology: Option<super::Topology>,
}

/// Node get capabilities response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeGetCapabilitiesResponse {
    /// Node capabilities.
    pub capabilities: Vec<NodeCapability>,
}

/// Staged volume information.
#[derive(Debug, Clone)]
struct StagedVolume {
    volume_id: String,
    staging_path: PathBuf,
    device_path: Option<PathBuf>,
    fs_type: String,
}

/// CSI Node Service.
pub struct NodeService<S: VolumeStore> {
    node_id: String,
    store: Arc<S>,
    capabilities: Vec<NodeCapability>,
    staging_dir: PathBuf,
    /// Staged volumes on this node.
    staged: RwLock<HashMap<String, StagedVolume>>,
    /// Published mount points.
    mounts: RwLock<HashMap<String, Vec<PathBuf>>>,
    /// Accessible topology.
    topology: Option<super::Topology>,
    /// Maximum volumes per node.
    max_volumes: i64,
}

impl<S: VolumeStore> NodeService<S> {
    /// Create a new node service.
    pub fn new(
        node_id: String,
        store: Arc<S>,
        capabilities: Vec<NodeCapability>,
        staging_dir: PathBuf,
        topology: Option<super::Topology>,
    ) -> Self {
        Self {
            node_id,
            store,
            capabilities,
            staging_dir,
            staged: RwLock::new(HashMap::new()),
            mounts: RwLock::new(HashMap::new()),
            topology,
            max_volumes: 256,
        }
    }

    /// Stage a volume (prepare for mounting).
    ///
    /// CSI RPC: NodeStageVolume
    pub async fn stage_volume(&self, req: NodeStageVolumeRequest) -> Result<()> {
        info!(
            volume_id = %req.volume_id,
            staging_path = %req.staging_target_path.display(),
            "Staging volume"
        );

        // Check if already staged
        if self.staged.read().await.contains_key(&req.volume_id) {
            debug!(volume_id = %req.volume_id, "Volume already staged");
            return Ok(());
        }

        // Ensure staging directory exists
        tokio::fs::create_dir_all(&req.staging_target_path)
            .await
            .map_err(|e| StrataError::Internal(format!("Failed to create staging dir: {}", e)))?;

        let fs_type = match &req.capability.access_type {
            AccessType::Mount { fs_type, .. } => fs_type.clone(),
            AccessType::Block => "block".to_string(),
        };

        // In a real implementation, this would:
        // 1. Connect to Strata metadata server
        // 2. Create a virtual block device or FUSE mount point
        // 3. Mount the volume at the staging path

        let staged = StagedVolume {
            volume_id: req.volume_id.clone(),
            staging_path: req.staging_target_path.clone(),
            device_path: None, // Would be the actual device
            fs_type,
        };

        self.staged
            .write()
            .await
            .insert(req.volume_id.clone(), staged);

        info!(
            volume_id = %req.volume_id,
            staging_path = %req.staging_target_path.display(),
            "Volume staged"
        );

        Ok(())
    }

    /// Unstage a volume.
    ///
    /// CSI RPC: NodeUnstageVolume
    pub async fn unstage_volume(&self, req: NodeUnstageVolumeRequest) -> Result<()> {
        info!(
            volume_id = %req.volume_id,
            staging_path = %req.staging_target_path.display(),
            "Unstaging volume"
        );

        // Check if there are still mounts
        let mounts = self.mounts.read().await;
        if let Some(mount_points) = mounts.get(&req.volume_id) {
            if !mount_points.is_empty() {
                return Err(StrataError::Internal(format!(
                    "Volume {} still has {} mount points",
                    req.volume_id,
                    mount_points.len()
                )));
            }
        }
        drop(mounts);

        // In a real implementation, this would:
        // 1. Unmount the staging path
        // 2. Disconnect from Strata
        // 3. Clean up the staging directory

        self.staged.write().await.remove(&req.volume_id);

        // Clean up staging directory
        if req.staging_target_path.exists() {
            let _ = tokio::fs::remove_dir_all(&req.staging_target_path).await;
        }

        info!(
            volume_id = %req.volume_id,
            "Volume unstaged"
        );

        Ok(())
    }

    /// Publish a volume (mount it for use).
    ///
    /// CSI RPC: NodePublishVolume
    pub async fn publish_volume(&self, req: NodePublishVolumeRequest) -> Result<()> {
        info!(
            volume_id = %req.volume_id,
            target_path = %req.target_path.display(),
            readonly = req.readonly,
            "Publishing volume"
        );

        // Verify volume is staged (if staging is required)
        if self.capabilities.contains(&NodeCapability::StageUnstageVolume) {
            if !self.staged.read().await.contains_key(&req.volume_id) {
                return Err(StrataError::Internal(format!(
                    "Volume {} not staged",
                    req.volume_id
                )));
            }
        }

        // Create target directory
        if let Some(parent) = req.target_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| StrataError::Internal(format!("Failed to create target dir: {}", e)))?;
        }

        // In a real implementation, this would:
        // 1. Bind mount from staging path to target path
        // 2. Apply mount options (readonly, etc.)

        // For block volumes, create a symlink or device node
        match &req.capability.access_type {
            AccessType::Block => {
                // Create symbolic link to block device
                debug!(
                    volume_id = %req.volume_id,
                    target_path = %req.target_path.display(),
                    "Creating block device link"
                );
            }
            AccessType::Mount { fs_type, mount_flags } => {
                debug!(
                    volume_id = %req.volume_id,
                    target_path = %req.target_path.display(),
                    fs_type,
                    flags = ?mount_flags,
                    "Mounting filesystem"
                );
            }
        }

        // Track the mount
        self.mounts
            .write()
            .await
            .entry(req.volume_id.clone())
            .or_insert_with(Vec::new)
            .push(req.target_path.clone());

        // Update store
        let published = PublishedVolume {
            volume_id: req.volume_id.clone(),
            node_id: self.node_id.clone(),
            staging_path: req.staging_target_path,
            target_paths: vec![req.target_path.clone()],
            read_only: req.readonly,
        };
        self.store.set_published(published).await?;

        info!(
            volume_id = %req.volume_id,
            target_path = %req.target_path.display(),
            "Volume published"
        );

        Ok(())
    }

    /// Unpublish a volume (unmount it).
    ///
    /// CSI RPC: NodeUnpublishVolume
    pub async fn unpublish_volume(&self, req: NodeUnpublishVolumeRequest) -> Result<()> {
        info!(
            volume_id = %req.volume_id,
            target_path = %req.target_path.display(),
            "Unpublishing volume"
        );

        // In a real implementation, this would unmount the target path

        // Remove from mount tracking
        let mut mounts = self.mounts.write().await;
        if let Some(mount_points) = mounts.get_mut(&req.volume_id) {
            mount_points.retain(|p| p != &req.target_path);
            if mount_points.is_empty() {
                mounts.remove(&req.volume_id);
            }
        }
        drop(mounts);

        // Clean up target
        if req.target_path.exists() {
            let _ = tokio::fs::remove_dir_all(&req.target_path).await;
        }

        info!(
            volume_id = %req.volume_id,
            target_path = %req.target_path.display(),
            "Volume unpublished"
        );

        Ok(())
    }

    /// Get volume statistics.
    ///
    /// CSI RPC: NodeGetVolumeStats
    pub async fn get_volume_stats(
        &self,
        req: NodeGetVolumeStatsRequest,
    ) -> Result<NodeGetVolumeStatsResponse> {
        debug!(
            volume_id = %req.volume_id,
            path = %req.volume_path.display(),
            "Getting volume stats"
        );

        // In a real implementation, this would use statfs or similar
        // to get actual filesystem statistics

        // Mock stats for now
        let usage = vec![
            VolumeUsage {
                available: 10 * 1024 * 1024 * 1024, // 10GB
                total: 20 * 1024 * 1024 * 1024,     // 20GB
                used: 10 * 1024 * 1024 * 1024,      // 10GB
                unit: UsageUnit::Bytes,
            },
            VolumeUsage {
                available: 1_000_000,
                total: 2_000_000,
                used: 1_000_000,
                unit: UsageUnit::Inodes,
            },
        ];

        let condition = if self.staged.read().await.contains_key(&req.volume_id) {
            Some(VolumeCondition {
                abnormal: false,
                message: "Volume is healthy".to_string(),
            })
        } else {
            Some(VolumeCondition {
                abnormal: true,
                message: "Volume not staged".to_string(),
            })
        };

        Ok(NodeGetVolumeStatsResponse { usage, condition })
    }

    /// Expand a volume on the node.
    ///
    /// CSI RPC: NodeExpandVolume
    pub async fn expand_volume(
        &self,
        req: NodeExpandVolumeRequest,
    ) -> Result<NodeExpandVolumeResponse> {
        info!(
            volume_id = %req.volume_id,
            path = %req.volume_path.display(),
            new_capacity = req.capacity_bytes,
            "Expanding volume on node"
        );

        // In a real implementation, this would:
        // 1. Resize the filesystem (resize2fs for ext4, xfs_growfs for xfs)
        // 2. Update any cached size information

        match &req.capability.access_type {
            AccessType::Block => {
                // Block devices don't need node-side expansion
                debug!(volume_id = %req.volume_id, "Block device expansion complete");
            }
            AccessType::Mount { fs_type, .. } => {
                debug!(
                    volume_id = %req.volume_id,
                    fs_type,
                    "Expanding filesystem"
                );
                // Would run resize2fs or xfs_growfs here
            }
        }

        info!(
            volume_id = %req.volume_id,
            new_capacity = req.capacity_bytes,
            "Volume expanded on node"
        );

        Ok(NodeExpandVolumeResponse {
            capacity_bytes: req.capacity_bytes,
        })
    }

    /// Get node information.
    ///
    /// CSI RPC: NodeGetInfo
    pub fn get_info(&self) -> NodeGetInfoResponse {
        NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            max_volumes_per_node: self.max_volumes,
            accessible_topology: self.topology.clone(),
        }
    }

    /// Get node capabilities.
    ///
    /// CSI RPC: NodeGetCapabilities
    pub fn get_capabilities(&self) -> NodeGetCapabilitiesResponse {
        NodeGetCapabilitiesResponse {
            capabilities: self.capabilities.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csi::{AccessMode, MemoryVolumeStore};
    use tempfile::TempDir;

    fn create_service(
        temp_dir: &TempDir,
    ) -> NodeService<MemoryVolumeStore> {
        let store = Arc::new(MemoryVolumeStore::new());
        NodeService::new(
            "test-node".to_string(),
            store,
            vec![
                NodeCapability::StageUnstageVolume,
                NodeCapability::GetVolumeStats,
                NodeCapability::ExpandVolume,
            ],
            temp_dir.path().to_path_buf(),
            None,
        )
    }

    #[tokio::test]
    async fn test_stage_volume() {
        let temp_dir = TempDir::new().unwrap();
        let service = create_service(&temp_dir);

        let staging_path = temp_dir.path().join("staging").join("vol-123");

        let req = NodeStageVolumeRequest {
            volume_id: "vol-123".to_string(),
            publish_context: HashMap::new(),
            staging_target_path: staging_path.clone(),
            capability: VolumeCapability::mount(AccessMode::SingleNodeWriter, "ext4", vec![]),
            secrets: HashMap::new(),
            volume_context: HashMap::new(),
        };

        service.stage_volume(req).await.unwrap();

        // Verify staged
        assert!(service.staged.read().await.contains_key("vol-123"));
    }

    #[tokio::test]
    async fn test_publish_volume() {
        let temp_dir = TempDir::new().unwrap();
        let service = create_service(&temp_dir);

        // Stage first
        let staging_path = temp_dir.path().join("staging").join("vol-123");
        let stage_req = NodeStageVolumeRequest {
            volume_id: "vol-123".to_string(),
            publish_context: HashMap::new(),
            staging_target_path: staging_path.clone(),
            capability: VolumeCapability::mount(AccessMode::SingleNodeWriter, "ext4", vec![]),
            secrets: HashMap::new(),
            volume_context: HashMap::new(),
        };
        service.stage_volume(stage_req).await.unwrap();

        // Publish
        let target_path = temp_dir.path().join("target").join("vol-123");
        let pub_req = NodePublishVolumeRequest {
            volume_id: "vol-123".to_string(),
            publish_context: HashMap::new(),
            staging_target_path: Some(staging_path),
            target_path: target_path.clone(),
            capability: VolumeCapability::mount(AccessMode::SingleNodeWriter, "ext4", vec![]),
            readonly: false,
            secrets: HashMap::new(),
            volume_context: HashMap::new(),
        };

        service.publish_volume(pub_req).await.unwrap();

        // Verify mount tracked
        let mounts = service.mounts.read().await;
        assert!(mounts.get("vol-123").unwrap().contains(&target_path));
    }

    #[tokio::test]
    async fn test_get_info() {
        let temp_dir = TempDir::new().unwrap();
        let service = create_service(&temp_dir);

        let info = service.get_info();
        assert_eq!(info.node_id, "test-node");
        assert_eq!(info.max_volumes_per_node, 256);
    }

    #[tokio::test]
    async fn test_get_capabilities() {
        let temp_dir = TempDir::new().unwrap();
        let service = create_service(&temp_dir);

        let caps = service.get_capabilities();
        assert!(caps.capabilities.contains(&NodeCapability::StageUnstageVolume));
        assert!(caps.capabilities.contains(&NodeCapability::GetVolumeStats));
    }
}
