//! Background recovery executor for self-healing chunk replication.

use super::{PlacementEngine, RecoveryManager, RecoveryTask};
use crate::client::DataClient;
use crate::error::{Result, StrataError};
use crate::types::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Background recovery executor that monitors and repairs under-replicated chunks.
pub struct RecoveryExecutor {
    /// Recovery manager for planning.
    recovery_manager: RecoveryManager,
    /// Check interval for recovery scans.
    check_interval: Duration,
    /// Maximum concurrent recovery operations.
    max_concurrent: usize,
    /// Shutdown signal receiver.
    shutdown_rx: Option<broadcast::Receiver<()>>,
}

/// Shared cluster state for the executor.
#[derive(Clone)]
pub struct ClusterState {
    /// Chunk metadata.
    pub chunks: Arc<RwLock<HashMap<ChunkId, ChunkMeta>>>,
    /// Data server information.
    pub servers: Arc<RwLock<HashMap<DataServerId, DataServerInfo>>>,
}

impl ClusterState {
    pub fn new() -> Self {
        Self {
            chunks: Arc::new(RwLock::new(HashMap::new())),
            servers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_server(&self, info: DataServerInfo) {
        let mut servers = self.servers.write().await;
        servers.insert(info.id, info);
    }

    pub async fn update_server_status(&self, id: DataServerId, status: ServerStatus) {
        let mut servers = self.servers.write().await;
        if let Some(server) = servers.get_mut(&id) {
            server.status = status;
            server.last_heartbeat = std::time::SystemTime::now();
        }
    }

    pub async fn add_chunk(&self, meta: ChunkMeta) {
        let mut chunks = self.chunks.write().await;
        chunks.insert(meta.id, meta);
    }

    pub async fn update_chunk_locations(&self, chunk_id: ChunkId, locations: Vec<DataServerId>) {
        let mut chunks = self.chunks.write().await;
        if let Some(chunk) = chunks.get_mut(&chunk_id) {
            chunk.locations = locations;
        }
    }
}

impl Default for ClusterState {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a recovery operation.
#[derive(Debug)]
pub struct RecoveryResult {
    pub chunk_id: ChunkId,
    pub success: bool,
    pub target_server: DataServerId,
    pub error: Option<String>,
}

impl RecoveryExecutor {
    /// Create a new recovery executor.
    pub fn new(
        placement: PlacementEngine,
        erasure_config: ErasureCodingConfig,
        shutdown_rx: Option<broadcast::Receiver<()>>,
    ) -> Self {
        Self {
            recovery_manager: RecoveryManager::new(placement, erasure_config),
            check_interval: Duration::from_secs(60),
            max_concurrent: 10,
            shutdown_rx,
        }
    }

    /// Set the check interval.
    pub fn with_check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    /// Set maximum concurrent recoveries.
    pub fn with_max_concurrent(mut self, max: usize) -> Self {
        self.max_concurrent = max;
        self
    }

    /// Run the recovery executor as a background task.
    pub async fn run(mut self, state: ClusterState) {
        info!("Recovery executor starting");

        let mut check_timer = interval(self.check_interval);

        loop {
            tokio::select! {
                _ = check_timer.tick() => {
                    if let Err(e) = self.run_recovery_cycle(&state).await {
                        error!(error = %e, "Recovery cycle failed");
                    }
                }
                _ = async {
                    if let Some(ref mut rx) = self.shutdown_rx {
                        rx.recv().await.ok()
                    } else {
                        std::future::pending::<Option<()>>().await
                    }
                } => {
                    info!("Recovery executor shutting down");
                    break;
                }
            }
        }
    }

    /// Run a single recovery cycle.
    async fn run_recovery_cycle(&self, state: &ClusterState) -> Result<()> {
        let chunks = state.chunks.read().await;
        let servers = state.servers.read().await;

        // Find under-replicated chunks
        let under_replicated = self.recovery_manager.find_under_replicated(&chunks, &servers);

        if under_replicated.is_empty() {
            debug!("No under-replicated chunks found");
            return Ok(());
        }

        info!(count = under_replicated.len(), "Found under-replicated chunks");

        // Create recovery plan
        let server_list: Vec<_> = servers.values().cloned().collect();
        let tasks = self.recovery_manager.create_recovery_plan(
            &under_replicated,
            &chunks,
            &server_list,
        );

        drop(chunks);
        drop(servers);

        // Execute recovery tasks (up to max_concurrent)
        let tasks_to_run: Vec<_> = tasks.into_iter().take(self.max_concurrent).collect();

        for task in tasks_to_run {
            let result = self.execute_recovery(&task, state).await;
            match result {
                Ok(r) if r.success => {
                    info!(
                        chunk_id = %r.chunk_id,
                        target = r.target_server,
                        "Recovery completed successfully"
                    );
                }
                Ok(r) => {
                    warn!(
                        chunk_id = %r.chunk_id,
                        error = ?r.error,
                        "Recovery failed"
                    );
                }
                Err(e) => {
                    error!(chunk_id = %task.chunk_id, error = %e, "Recovery execution error");
                }
            }
        }

        Ok(())
    }

    /// Execute a single recovery task.
    async fn execute_recovery(
        &self,
        task: &RecoveryTask,
        state: &ClusterState,
    ) -> Result<RecoveryResult> {
        let servers = state.servers.read().await;

        // Find a source server that has the chunk
        let source_server = task
            .source_locations
            .iter()
            .filter_map(|id| servers.get(id))
            .find(|s| s.status == ServerStatus::Online);

        let source = match source_server {
            Some(s) => s.clone(),
            None => {
                return Ok(RecoveryResult {
                    chunk_id: task.chunk_id,
                    success: false,
                    target_server: task.target_server,
                    error: Some("No online source server found".to_string()),
                });
            }
        };

        let target = match servers.get(&task.target_server) {
            Some(s) => s.clone(),
            None => {
                return Ok(RecoveryResult {
                    chunk_id: task.chunk_id,
                    success: false,
                    target_server: task.target_server,
                    error: Some("Target server not found".to_string()),
                });
            }
        };

        drop(servers);

        // Copy chunk from source to target
        match self.copy_chunk(&source, &target, task.chunk_id).await {
            Ok(()) => {
                // Update chunk locations
                let mut new_locations = task.source_locations.clone();
                if !new_locations.contains(&task.target_server) {
                    new_locations.push(task.target_server);
                }
                state.update_chunk_locations(task.chunk_id, new_locations).await;

                Ok(RecoveryResult {
                    chunk_id: task.chunk_id,
                    success: true,
                    target_server: task.target_server,
                    error: None,
                })
            }
            Err(e) => Ok(RecoveryResult {
                chunk_id: task.chunk_id,
                success: false,
                target_server: task.target_server,
                error: Some(e.to_string()),
            }),
        }
    }

    /// Copy a chunk from source server to target server.
    async fn copy_chunk(
        &self,
        source: &DataServerInfo,
        target: &DataServerInfo,
        chunk_id: ChunkId,
    ) -> Result<()> {
        let source_addr: SocketAddr = source
            .address
            .parse()
            .map_err(|e| StrataError::Internal(format!("Invalid source address: {}", e)))?;

        let target_addr: SocketAddr = target
            .address
            .parse()
            .map_err(|e| StrataError::Internal(format!("Invalid target address: {}", e)))?;

        let source_client = DataClient::new(source_addr);
        let target_client = DataClient::new(target_addr);

        // Read chunk from source
        // Note: This is a simplified implementation. In production, you'd want to:
        // 1. Read individual shards
        // 2. Use erasure coding to reconstruct if needed
        // 3. Write shards to target
        let chunk_data = source_client
            .read_chunk(chunk_id, DEFAULT_CHUNK_SIZE)
            .await?;

        // Write chunk to target
        target_client.write_chunk(chunk_id, &chunk_data).await?;

        debug!(
            chunk_id = %chunk_id,
            source = %source.address,
            target = %target.address,
            "Copied chunk"
        );

        Ok(())
    }
}

/// Statistics about recovery operations.
#[derive(Debug, Default, Clone)]
pub struct RecoveryStats {
    pub total_recoveries: u64,
    pub successful_recoveries: u64,
    pub failed_recoveries: u64,
    pub chunks_under_replicated: usize,
    pub critical_chunks: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_state_new() {
        let state = ClusterState::new();
        // Just verify it can be created
        assert!(Arc::strong_count(&state.chunks) == 1);
    }

    #[tokio::test]
    async fn test_cluster_state_register_server() {
        let state = ClusterState::new();

        let info = DataServerInfo {
            id: 1,
            address: "127.0.0.1:9000".to_string(),
            capacity: 1_000_000_000,
            used: 0,
            status: ServerStatus::Online,
            last_heartbeat: std::time::SystemTime::now(),
        };

        state.register_server(info).await;

        let servers = state.servers.read().await;
        assert_eq!(servers.len(), 1);
        assert!(servers.contains_key(&1));
    }

    #[tokio::test]
    async fn test_cluster_state_update_status() {
        let state = ClusterState::new();

        let info = DataServerInfo {
            id: 1,
            address: "127.0.0.1:9000".to_string(),
            capacity: 1_000_000_000,
            used: 0,
            status: ServerStatus::Online,
            last_heartbeat: std::time::SystemTime::now(),
        };

        state.register_server(info).await;
        state.update_server_status(1, ServerStatus::Offline).await;

        let servers = state.servers.read().await;
        assert_eq!(servers.get(&1).unwrap().status, ServerStatus::Offline);
    }

    #[tokio::test]
    async fn test_cluster_state_add_chunk() {
        let state = ClusterState::new();

        let chunk_id = ChunkId::new();
        let meta = ChunkMeta {
            id: chunk_id,
            size: 1024,
            checksum: 0,
            locations: vec![1, 2, 3],
            version: 1,
        };

        state.add_chunk(meta).await;

        let chunks = state.chunks.read().await;
        assert!(chunks.contains_key(&chunk_id));
    }
}
