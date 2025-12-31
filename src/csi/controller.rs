//! CSI Controller Service implementation.
//!
//! The Controller service handles volume lifecycle operations like
//! create, delete, snapshot, and expand.

#[allow(unused_imports)]
use super::{
    AccessMode, ControllerCapability, Snapshot, Topology, Volume,
    VolumeCapability, VolumeContentSource, VolumeState, VolumeStore,
};
use crate::error::{Result, StrataError};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};
use uuid::Uuid;

/// Create volume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVolumeRequest {
    /// Volume name.
    pub name: String,
    /// Required capacity in bytes.
    pub capacity_bytes: i64,
    /// Volume capabilities.
    pub capabilities: Vec<VolumeCapability>,
    /// Parameters from StorageClass.
    pub parameters: HashMap<String, String>,
    /// Secrets (not persisted).
    #[serde(skip)]
    pub secrets: HashMap<String, String>,
    /// Volume content source.
    pub content_source: Option<VolumeContentSource>,
    /// Accessibility requirements.
    pub accessibility_requirements: Option<TopologyRequirement>,
}

/// Topology requirement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyRequirement {
    /// Required topologies.
    pub requisite: Vec<Topology>,
    /// Preferred topologies.
    pub preferred: Vec<Topology>,
}

/// Create volume response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVolumeResponse {
    /// Created volume.
    pub volume: Volume,
}

/// Delete volume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteVolumeRequest {
    /// Volume ID.
    pub volume_id: String,
    /// Secrets (not persisted).
    #[serde(skip)]
    pub secrets: HashMap<String, String>,
}

/// Controller publish volume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerPublishVolumeRequest {
    /// Volume ID.
    pub volume_id: String,
    /// Node ID.
    pub node_id: String,
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

/// Controller publish volume response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerPublishVolumeResponse {
    /// Publish context passed to NodeStageVolume.
    pub publish_context: HashMap<String, String>,
}

/// Controller unpublish volume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerUnpublishVolumeRequest {
    /// Volume ID.
    pub volume_id: String,
    /// Node ID.
    pub node_id: String,
    /// Secrets.
    #[serde(skip)]
    pub secrets: HashMap<String, String>,
}

/// Create snapshot request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotRequest {
    /// Source volume ID.
    pub source_volume_id: String,
    /// Snapshot name.
    pub name: String,
    /// Secrets.
    #[serde(skip)]
    pub secrets: HashMap<String, String>,
    /// Parameters.
    pub parameters: HashMap<String, String>,
}

/// Create snapshot response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateSnapshotResponse {
    /// Created snapshot.
    pub snapshot: Snapshot,
}

/// Delete snapshot request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteSnapshotRequest {
    /// Snapshot ID.
    pub snapshot_id: String,
    /// Secrets.
    #[serde(skip)]
    pub secrets: HashMap<String, String>,
}

/// List volumes request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListVolumesRequest {
    /// Maximum entries to return.
    pub max_entries: i32,
    /// Starting token for pagination.
    pub starting_token: String,
}

/// List volumes response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListVolumesResponse {
    /// Volume entries.
    pub entries: Vec<VolumeEntry>,
    /// Next token for pagination.
    pub next_token: String,
}

/// Volume entry in list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeEntry {
    /// Volume.
    pub volume: Volume,
    /// Volume status.
    pub status: VolumeStatus,
}

/// Volume status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeStatus {
    /// Published node IDs.
    pub published_node_ids: Vec<String>,
    /// Abnormal condition.
    pub abnormal: bool,
    /// Condition message.
    pub message: String,
}

/// Expand volume request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerExpandVolumeRequest {
    /// Volume ID.
    pub volume_id: String,
    /// Required new capacity.
    pub capacity_bytes: i64,
    /// Secrets.
    #[serde(skip)]
    pub secrets: HashMap<String, String>,
    /// Volume capability.
    pub capability: VolumeCapability,
}

/// Expand volume response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerExpandVolumeResponse {
    /// New capacity.
    pub capacity_bytes: i64,
    /// Whether node expansion is required.
    pub node_expansion_required: bool,
}

/// CSI Controller Service.
pub struct ControllerService<S: VolumeStore> {
    store: Arc<S>,
    capabilities: Vec<ControllerCapability>,
}

impl<S: VolumeStore> ControllerService<S> {
    /// Create a new controller service.
    pub fn new(store: Arc<S>, capabilities: Vec<ControllerCapability>) -> Self {
        Self {
            store,
            capabilities,
        }
    }

    /// Get controller capabilities.
    pub fn get_capabilities(&self) -> &[ControllerCapability] {
        &self.capabilities
    }

    /// Create a volume.
    ///
    /// CSI RPC: CreateVolume
    pub async fn create_volume(&self, req: CreateVolumeRequest) -> Result<CreateVolumeResponse> {
        info!(name = %req.name, capacity = req.capacity_bytes, "Creating volume");

        // Check for existing volume with same name (idempotency)
        let volumes = self.store.list_volumes().await?;
        if let Some(existing) = volumes.iter().find(|v| v.name == req.name) {
            // Verify parameters match
            if existing.capacity_bytes >= req.capacity_bytes {
                info!(volume_id = %existing.volume_id, "Returning existing volume");
                return Ok(CreateVolumeResponse {
                    volume: existing.clone(),
                });
            } else {
                return Err(StrataError::FileExists(format!(
                    "Volume {} exists with smaller capacity",
                    req.name
                )));
            }
        }

        // Generate volume ID
        let volume_id = format!("vol-{}", Uuid::new_v4());

        // Handle content source
        if let Some(ref source) = req.content_source {
            match source {
                VolumeContentSource::Snapshot { snapshot_id } => {
                    let snapshot = self
                        .store
                        .get_snapshot(snapshot_id)
                        .await?
                        .ok_or_else(|| StrataError::NotFound(format!("Snapshot {}", snapshot_id)))?;

                    if req.capacity_bytes < snapshot.size_bytes {
                        return Err(StrataError::InvalidData(
                            "Requested capacity is less than snapshot size".into(),
                        ));
                    }
                }
                VolumeContentSource::Volume { volume_id: src_id } => {
                    let src_volume = self
                        .store
                        .get_volume(src_id)
                        .await?
                        .ok_or_else(|| StrataError::NotFound(format!("Volume {}", src_id)))?;

                    if req.capacity_bytes < src_volume.capacity_bytes {
                        return Err(StrataError::InvalidData(
                            "Requested capacity is less than source volume size".into(),
                        ));
                    }
                }
            }
        }

        // Prepare volume context
        let mut volume_context = HashMap::new();
        volume_context.insert("strata.storage.io/created-by".to_string(), "csi".to_string());

        // Extract Strata-specific parameters
        if let Some(erasure) = req.parameters.get("erasure_profile") {
            volume_context.insert("strata.storage.io/erasure-profile".to_string(), erasure.clone());
        }
        if let Some(encrypt) = req.parameters.get("encryption") {
            volume_context.insert("strata.storage.io/encryption".to_string(), encrypt.clone());
        }

        let volume = Volume {
            volume_id: volume_id.clone(),
            name: req.name.clone(),
            capacity_bytes: req.capacity_bytes,
            parameters: req.parameters.clone(),
            volume_context,
            content_source: req.content_source,
            accessible_topology: vec![], // Would be set based on accessibility_requirements
            state: VolumeState::Ready,
            created_at: Utc::now(),
        };

        self.store.create_volume(volume.clone()).await?;

        info!(volume_id = %volume_id, name = %req.name, "Volume created");

        Ok(CreateVolumeResponse { volume })
    }

    /// Delete a volume.
    ///
    /// CSI RPC: DeleteVolume
    pub async fn delete_volume(&self, req: DeleteVolumeRequest) -> Result<()> {
        info!(volume_id = %req.volume_id, "Deleting volume");

        // Check if volume exists
        let volume = self.store.get_volume(&req.volume_id).await?;
        if volume.is_none() {
            // Idempotent - already deleted
            debug!(volume_id = %req.volume_id, "Volume already deleted");
            return Ok(());
        }

        // Check for snapshots
        let snapshots = self.store.list_snapshots(Some(&req.volume_id)).await?;
        if !snapshots.is_empty() {
            return Err(StrataError::Internal(format!(
                "Cannot delete volume {}: has {} snapshots",
                req.volume_id,
                snapshots.len()
            )));
        }

        // Delete the volume
        self.store.delete_volume(&req.volume_id).await?;

        info!(volume_id = %req.volume_id, "Volume deleted");
        Ok(())
    }

    /// Publish a volume to a node (make it available).
    ///
    /// CSI RPC: ControllerPublishVolume
    pub async fn publish_volume(
        &self,
        req: ControllerPublishVolumeRequest,
    ) -> Result<ControllerPublishVolumeResponse> {
        info!(
            volume_id = %req.volume_id,
            node_id = %req.node_id,
            "Publishing volume"
        );

        // Verify volume exists
        let volume = self
            .store
            .get_volume(&req.volume_id)
            .await?
            .ok_or_else(|| StrataError::NotFound(format!("Volume {}", req.volume_id)))?;

        if volume.state != VolumeState::Ready {
            return Err(StrataError::Internal(format!(
                "Volume {} is not ready",
                req.volume_id
            )));
        }

        // Check access mode compatibility
        if !req.capability.access_mode.is_multi_node() {
            // Single node - check if already published elsewhere
            if let Some(existing) = self
                .store
                .get_published(&req.volume_id, &req.node_id)
                .await?
            {
                if existing.node_id != req.node_id {
                    return Err(StrataError::Internal(format!(
                        "Volume {} already published to node {}",
                        req.volume_id, existing.node_id
                    )));
                }
            }
        }

        // Prepare publish context
        let mut publish_context = HashMap::new();
        publish_context.insert(
            "strata.storage.io/metadata-addr".to_string(),
            "localhost:9000".to_string(), // Would come from config
        );
        publish_context.insert(
            "strata.storage.io/volume-id".to_string(),
            req.volume_id.clone(),
        );

        info!(
            volume_id = %req.volume_id,
            node_id = %req.node_id,
            "Volume published"
        );

        Ok(ControllerPublishVolumeResponse { publish_context })
    }

    /// Unpublish a volume from a node.
    ///
    /// CSI RPC: ControllerUnpublishVolume
    pub async fn unpublish_volume(&self, req: ControllerUnpublishVolumeRequest) -> Result<()> {
        info!(
            volume_id = %req.volume_id,
            node_id = %req.node_id,
            "Unpublishing volume"
        );

        // Remove publish record if exists
        self.store
            .remove_published(&req.volume_id, &req.node_id)
            .await?;

        info!(
            volume_id = %req.volume_id,
            node_id = %req.node_id,
            "Volume unpublished"
        );

        Ok(())
    }

    /// Create a snapshot.
    ///
    /// CSI RPC: CreateSnapshot
    pub async fn create_snapshot(
        &self,
        req: CreateSnapshotRequest,
    ) -> Result<CreateSnapshotResponse> {
        info!(
            source_volume_id = %req.source_volume_id,
            name = %req.name,
            "Creating snapshot"
        );

        // Check for existing snapshot with same name (idempotency)
        let snapshots = self.store.list_snapshots(Some(&req.source_volume_id)).await?;
        if let Some(existing) = snapshots.iter().find(|s| s.name == req.name) {
            info!(snapshot_id = %existing.snapshot_id, "Returning existing snapshot");
            return Ok(CreateSnapshotResponse {
                snapshot: existing.clone(),
            });
        }

        // Verify source volume exists
        let volume = self
            .store
            .get_volume(&req.source_volume_id)
            .await?
            .ok_or_else(|| StrataError::NotFound(format!("Volume {}", req.source_volume_id)))?;

        // Generate snapshot ID
        let snapshot_id = format!("snap-{}", Uuid::new_v4());

        let snapshot = Snapshot {
            snapshot_id: snapshot_id.clone(),
            source_volume_id: req.source_volume_id.clone(),
            name: req.name.clone(),
            creation_time: Utc::now(),
            ready_to_use: true, // Would be false during async creation
            size_bytes: volume.capacity_bytes,
        };

        self.store.create_snapshot(snapshot.clone()).await?;

        info!(
            snapshot_id = %snapshot_id,
            source_volume_id = %req.source_volume_id,
            "Snapshot created"
        );

        Ok(CreateSnapshotResponse { snapshot })
    }

    /// Delete a snapshot.
    ///
    /// CSI RPC: DeleteSnapshot
    pub async fn delete_snapshot(&self, req: DeleteSnapshotRequest) -> Result<()> {
        info!(snapshot_id = %req.snapshot_id, "Deleting snapshot");

        // Idempotent
        if self.store.get_snapshot(&req.snapshot_id).await?.is_none() {
            debug!(snapshot_id = %req.snapshot_id, "Snapshot already deleted");
            return Ok(());
        }

        self.store.delete_snapshot(&req.snapshot_id).await?;

        info!(snapshot_id = %req.snapshot_id, "Snapshot deleted");
        Ok(())
    }

    /// List volumes.
    ///
    /// CSI RPC: ListVolumes
    pub async fn list_volumes(&self, req: ListVolumesRequest) -> Result<ListVolumesResponse> {
        let volumes = self.store.list_volumes().await?;

        // Simple pagination
        let start = if req.starting_token.is_empty() {
            0
        } else {
            req.starting_token.parse().unwrap_or(0)
        };

        let max = if req.max_entries > 0 {
            req.max_entries as usize
        } else {
            volumes.len()
        };

        let entries: Vec<VolumeEntry> = volumes
            .into_iter()
            .skip(start)
            .take(max)
            .map(|v| VolumeEntry {
                volume: v,
                status: VolumeStatus {
                    published_node_ids: vec![],
                    abnormal: false,
                    message: String::new(),
                },
            })
            .collect();

        let next_token = if entries.len() == max {
            (start + max).to_string()
        } else {
            String::new()
        };

        Ok(ListVolumesResponse {
            entries,
            next_token,
        })
    }

    /// Expand a volume.
    ///
    /// CSI RPC: ControllerExpandVolume
    pub async fn expand_volume(
        &self,
        req: ControllerExpandVolumeRequest,
    ) -> Result<ControllerExpandVolumeResponse> {
        info!(
            volume_id = %req.volume_id,
            new_capacity = req.capacity_bytes,
            "Expanding volume"
        );

        let mut volume = self
            .store
            .get_volume(&req.volume_id)
            .await?
            .ok_or_else(|| StrataError::NotFound(format!("Volume {}", req.volume_id)))?;

        if req.capacity_bytes <= volume.capacity_bytes {
            // Already at or above requested size
            return Ok(ControllerExpandVolumeResponse {
                capacity_bytes: volume.capacity_bytes,
                node_expansion_required: false,
            });
        }

        // Update capacity
        volume.capacity_bytes = req.capacity_bytes;
        self.store.update_volume(&volume).await?;

        // Determine if node expansion is needed
        let node_expansion_required = matches!(
            req.capability.access_type,
            super::AccessType::Mount { .. }
        );

        info!(
            volume_id = %req.volume_id,
            new_capacity = req.capacity_bytes,
            node_expansion_required,
            "Volume expanded"
        );

        Ok(ControllerExpandVolumeResponse {
            capacity_bytes: req.capacity_bytes,
            node_expansion_required,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::csi::MemoryVolumeStore;

    fn create_service() -> ControllerService<MemoryVolumeStore> {
        let store = Arc::new(MemoryVolumeStore::new());
        ControllerService::new(
            store,
            vec![
                ControllerCapability::CreateDeleteVolume,
                ControllerCapability::CreateDeleteSnapshot,
                ControllerCapability::ExpandVolume,
            ],
        )
    }

    #[tokio::test]
    async fn test_create_volume() {
        let service = create_service();

        let req = CreateVolumeRequest {
            name: "test-vol".to_string(),
            capacity_bytes: 1024 * 1024 * 1024,
            capabilities: vec![VolumeCapability::mount(
                AccessMode::SingleNodeWriter,
                "ext4",
                vec![],
            )],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            content_source: None,
            accessibility_requirements: None,
        };

        let resp = service.create_volume(req).await.unwrap();
        assert_eq!(resp.volume.name, "test-vol");
        assert_eq!(resp.volume.capacity_bytes, 1024 * 1024 * 1024);
        assert_eq!(resp.volume.state, VolumeState::Ready);
    }

    #[tokio::test]
    async fn test_create_volume_idempotent() {
        let service = create_service();

        let req = CreateVolumeRequest {
            name: "test-vol".to_string(),
            capacity_bytes: 1024 * 1024 * 1024,
            capabilities: vec![],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            content_source: None,
            accessibility_requirements: None,
        };

        let resp1 = service.create_volume(req.clone()).await.unwrap();
        let resp2 = service.create_volume(req).await.unwrap();

        assert_eq!(resp1.volume.volume_id, resp2.volume.volume_id);
    }

    #[tokio::test]
    async fn test_delete_volume() {
        let service = create_service();

        // Create volume
        let req = CreateVolumeRequest {
            name: "test-vol".to_string(),
            capacity_bytes: 1024 * 1024 * 1024,
            capabilities: vec![],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            content_source: None,
            accessibility_requirements: None,
        };

        let resp = service.create_volume(req).await.unwrap();

        // Delete volume
        let del_req = DeleteVolumeRequest {
            volume_id: resp.volume.volume_id.clone(),
            secrets: HashMap::new(),
        };

        service.delete_volume(del_req).await.unwrap();

        // Verify deleted
        let list_resp = service
            .list_volumes(ListVolumesRequest {
                max_entries: 0,
                starting_token: String::new(),
            })
            .await
            .unwrap();

        assert!(list_resp.entries.is_empty());
    }

    #[tokio::test]
    async fn test_create_snapshot() {
        let service = create_service();

        // Create volume first
        let vol_req = CreateVolumeRequest {
            name: "test-vol".to_string(),
            capacity_bytes: 1024 * 1024 * 1024,
            capabilities: vec![],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            content_source: None,
            accessibility_requirements: None,
        };

        let vol_resp = service.create_volume(vol_req).await.unwrap();

        // Create snapshot
        let snap_req = CreateSnapshotRequest {
            source_volume_id: vol_resp.volume.volume_id.clone(),
            name: "test-snap".to_string(),
            secrets: HashMap::new(),
            parameters: HashMap::new(),
        };

        let snap_resp = service.create_snapshot(snap_req).await.unwrap();
        assert_eq!(snap_resp.snapshot.name, "test-snap");
        assert!(snap_resp.snapshot.ready_to_use);
    }

    #[tokio::test]
    async fn test_expand_volume() {
        let service = create_service();

        // Create volume
        let req = CreateVolumeRequest {
            name: "test-vol".to_string(),
            capacity_bytes: 1024 * 1024 * 1024,
            capabilities: vec![],
            parameters: HashMap::new(),
            secrets: HashMap::new(),
            content_source: None,
            accessibility_requirements: None,
        };

        let vol_resp = service.create_volume(req).await.unwrap();

        // Expand volume
        let expand_req = ControllerExpandVolumeRequest {
            volume_id: vol_resp.volume.volume_id,
            capacity_bytes: 2 * 1024 * 1024 * 1024,
            secrets: HashMap::new(),
            capability: VolumeCapability::mount(AccessMode::SingleNodeWriter, "ext4", vec![]),
        };

        let expand_resp = service.expand_volume(expand_req).await.unwrap();
        assert_eq!(expand_resp.capacity_bytes, 2 * 1024 * 1024 * 1024);
        assert!(expand_resp.node_expansion_required);
    }
}
