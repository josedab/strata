//! Data server implementation.

use super::ChunkStorage;
use crate::config::{DataConfig, StorageConfig};
use crate::erasure::ErasureCoder;
use crate::error::StrataError;
use crate::types::{ChunkId, DataServerId, ErasureCodingConfig, NodeId};
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{delete, get, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

/// Data server state.
#[derive(Clone)]
pub struct DataServer {
    server_id: DataServerId,
    storage: Arc<ChunkStorage>,
    erasure_config: ErasureCodingConfig,
}

impl DataServer {
    pub fn new(
        server_id: DataServerId,
        storage: ChunkStorage,
        erasure_config: ErasureCodingConfig,
    ) -> Self {
        Self {
            server_id,
            storage: Arc::new(storage),
            erasure_config,
        }
    }

    /// Write a full chunk (with erasure encoding).
    pub fn write_chunk(&self, chunk_id: ChunkId, data: &[u8]) -> crate::error::Result<Vec<usize>> {
        let coder = ErasureCoder::new(self.erasure_config)?;
        let shards = coder.encode(data)?;

        let mut stored_indices = Vec::with_capacity(shards.len());
        for (i, shard_data) in shards.iter().enumerate() {
            self.storage.write_shard(chunk_id, i, shard_data)?;
            stored_indices.push(i);
        }

        Ok(stored_indices)
    }

    /// Write a single shard.
    pub fn write_shard(&self, chunk_id: ChunkId, shard_index: usize, data: &[u8]) -> crate::error::Result<()> {
        self.storage.write_shard(chunk_id, shard_index, data)?;
        Ok(())
    }

    /// Read a full chunk (with erasure decoding).
    pub fn read_chunk(&self, chunk_id: ChunkId, original_size: usize) -> crate::error::Result<Vec<u8>> {
        let coder = ErasureCoder::new(self.erasure_config)?;
        let total_shards = self.erasure_config.total_shards();

        // Try to read all shards
        let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(total_shards);
        let mut available = 0;

        for i in 0..total_shards {
            match self.storage.read_shard(chunk_id, i) {
                Ok(data) => {
                    shards.push(Some(data));
                    available += 1;
                }
                Err(_) => {
                    shards.push(None);
                }
            }
        }

        if available < self.erasure_config.data_shards {
            return Err(StrataError::InsufficientShards {
                available,
                required: self.erasure_config.data_shards,
            });
        }

        coder.decode(&mut shards, original_size)
    }

    /// Read a single shard.
    pub fn read_shard(&self, chunk_id: ChunkId, shard_index: usize) -> crate::error::Result<Vec<u8>> {
        self.storage.read_shard(chunk_id, shard_index)
    }

    /// Delete a chunk (all shards).
    pub fn delete_chunk(&self, chunk_id: ChunkId) -> crate::error::Result<()> {
        let shards = self.storage.list_shards(chunk_id)?;
        for shard_index in shards {
            self.storage.delete_shard(chunk_id, shard_index)?;
        }
        Ok(())
    }

    /// Get storage usage.
    pub fn get_usage(&self) -> crate::error::Result<u64> {
        self.storage.calculate_usage()
    }
}

/// Run the data server.
pub async fn run_data_server(
    node_id: NodeId,
    config: DataConfig,
    storage_config: StorageConfig,
) -> crate::error::Result<()> {
    info!(node_id, "Starting data server");

    let storage = ChunkStorage::new(&storage_config.data_dir, config.cache_size)?;
    let server = DataServer::new(node_id, storage, storage_config.erasure_config);

    let app = Router::new()
        // Chunk operations
        .route("/chunk/:chunk_id", put(handle_write_chunk))
        .route("/chunk/:chunk_id", get(handle_read_chunk))
        .route("/chunk/:chunk_id", delete(handle_delete_chunk))
        // Shard operations
        .route("/shard/:chunk_id/:shard_index", put(handle_write_shard))
        .route("/shard/:chunk_id/:shard_index", get(handle_read_shard))
        // Status
        .route("/health", get(health_check))
        .route("/status", get(get_status))
        .with_state(server);

    let listener = TcpListener::bind(config.bind_addr).await?;
    info!(addr = %config.bind_addr, "Data server listening");

    axum::serve(listener, app)
        .await
        .map_err(|e| StrataError::Network(e.to_string()))?;

    Ok(())
}

// Request/Response types

#[derive(Deserialize)]
struct ReadQuery {
    size: usize,
}

#[derive(Serialize)]
struct WriteResponse {
    success: bool,
    shards: Vec<usize>,
    error: Option<String>,
}

#[derive(Serialize)]
struct StatusResponse {
    server_id: u64,
    usage_bytes: u64,
    healthy: bool,
}

// Handlers

async fn health_check() -> &'static str {
    "OK"
}

async fn get_status(State(server): State<DataServer>) -> Json<StatusResponse> {
    let usage = server.get_usage().unwrap_or(0);
    Json(StatusResponse {
        server_id: server.server_id,
        usage_bytes: usage,
        healthy: true,
    })
}

async fn handle_write_chunk(
    State(server): State<DataServer>,
    Path(chunk_id_str): Path<String>,
    body: Bytes,
) -> Result<Json<WriteResponse>, (StatusCode, String)> {
    let chunk_id = parse_chunk_id(&chunk_id_str)?;

    match server.write_chunk(chunk_id, &body) {
        Ok(shards) => Ok(Json(WriteResponse {
            success: true,
            shards,
            error: None,
        })),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn handle_read_chunk(
    State(server): State<DataServer>,
    Path(chunk_id_str): Path<String>,
    Query(query): Query<ReadQuery>,
) -> Result<Bytes, (StatusCode, String)> {
    let chunk_id = parse_chunk_id(&chunk_id_str)?;

    match server.read_chunk(chunk_id, query.size) {
        Ok(data) => Ok(Bytes::from(data)),
        Err(e) => Err((StatusCode::NOT_FOUND, e.to_string())),
    }
}

async fn handle_delete_chunk(
    State(server): State<DataServer>,
    Path(chunk_id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let chunk_id = parse_chunk_id(&chunk_id_str)?;

    match server.delete_chunk(chunk_id) {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn handle_write_shard(
    State(server): State<DataServer>,
    Path((chunk_id_str, shard_index)): Path<(String, usize)>,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    let chunk_id = parse_chunk_id(&chunk_id_str)?;

    match server.write_shard(chunk_id, shard_index, &body) {
        Ok(_) => Ok(StatusCode::CREATED),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn handle_read_shard(
    State(server): State<DataServer>,
    Path((chunk_id_str, shard_index)): Path<(String, usize)>,
) -> Result<Bytes, (StatusCode, String)> {
    let chunk_id = parse_chunk_id(&chunk_id_str)?;

    match server.read_shard(chunk_id, shard_index) {
        Ok(data) => Ok(Bytes::from(data)),
        Err(e) => Err((StatusCode::NOT_FOUND, e.to_string())),
    }
}

fn parse_chunk_id(s: &str) -> Result<ChunkId, (StatusCode, String)> {
    uuid::Uuid::parse_str(s)
        .map(|u| ChunkId(u))
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid chunk ID: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_data_server_write_read() {
        let dir = tempdir().unwrap();
        let config = ErasureCodingConfig::SMALL_CLUSTER; // 2+1
        let storage = ChunkStorage::new(dir.path(), 1024 * 1024).unwrap();
        let server = DataServer::new(1, storage, config);

        let chunk_id = ChunkId::new();
        let data = b"Hello, distributed file system!";

        // Write
        let shards = server.write_chunk(chunk_id, data).unwrap();
        assert_eq!(shards.len(), 3);

        // Read
        let read_data = server.read_chunk(chunk_id, data.len()).unwrap();
        assert_eq!(&read_data, data);
    }

    #[test]
    fn test_data_server_delete() {
        let dir = tempdir().unwrap();
        let config = ErasureCodingConfig::SMALL_CLUSTER;
        let storage = ChunkStorage::new(dir.path(), 1024 * 1024).unwrap();
        let server = DataServer::new(1, storage, config);

        let chunk_id = ChunkId::new();
        server.write_chunk(chunk_id, b"test").unwrap();

        server.delete_chunk(chunk_id).unwrap();

        // Should fail to read
        assert!(server.read_chunk(chunk_id, 4).is_err());
    }
}
