//! Strata client for FUSE operations.

use crate::error::{Result, StrataError};
use crate::erasure::ErasureCoder;
use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Client for communicating with Strata metadata and data servers.
pub struct StrataClient {
    metadata_addr: String,
    data_servers: Vec<String>,
    http_client: reqwest::blocking::Client,
    // Cache for file handles
    handles: HashMap<FileHandle, HandleInfo>,
    next_handle: FileHandle,
    erasure_config: ErasureCodingConfig,
}

#[derive(Clone)]
struct HandleInfo {
    inode: InodeId,
    flags: i32,
}

impl StrataClient {
    /// Connect to a Strata metadata server.
    pub fn connect(metadata_addr: &str) -> Result<Self> {
        Self::connect_with_data_servers(metadata_addr, Vec::new())
    }

    /// Connect to Strata with explicit data server addresses.
    pub fn connect_with_data_servers(metadata_addr: &str, data_servers: Vec<String>) -> Result<Self> {
        let http_client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| StrataError::Network(e.to_string()))?;

        Ok(Self {
            metadata_addr: metadata_addr.to_string(),
            data_servers,
            http_client,
            handles: HashMap::new(),
            next_handle: 1,
            erasure_config: ErasureCodingConfig::default(),
        })
    }

    /// Register a data server.
    pub fn add_data_server(&mut self, addr: String) {
        if !self.data_servers.contains(&addr) {
            self.data_servers.push(addr);
        }
    }

    /// Look up a file in a directory.
    pub fn lookup(&self, parent: InodeId, name: &str) -> Result<Option<Inode>> {
        #[derive(Serialize)]
        struct LookupRequest {
            parent: InodeId,
            name: String,
        }

        #[derive(Deserialize)]
        struct LookupResponse {
            found: bool,
            inode: Option<Inode>,
        }

        let url = format!("http://{}/metadata/lookup", self.metadata_addr);
        let request = LookupRequest {
            parent,
            name: name.to_string(),
        };

        let response: LookupResponse = self.http_client
            .post(&url)
            .json(&request)
            .send()
            .map_err(|e| StrataError::Network(e.to_string()))?
            .json()
            .map_err(|e| StrataError::Deserialization(e.to_string()))?;

        Ok(response.inode)
    }

    /// Get attributes of an inode.
    pub fn getattr(&self, inode: InodeId) -> Result<Option<Inode>> {
        #[derive(Serialize)]
        struct GetattrRequest {
            inode: InodeId,
        }

        #[derive(Deserialize)]
        struct GetattrResponse {
            found: bool,
            inode: Option<Inode>,
        }

        let url = format!("http://{}/metadata/getattr", self.metadata_addr);
        let request = GetattrRequest { inode };

        let response: GetattrResponse = self.http_client
            .post(&url)
            .json(&request)
            .send()
            .map_err(|e| StrataError::Network(e.to_string()))?
            .json()
            .map_err(|e| StrataError::Deserialization(e.to_string()))?;

        Ok(response.inode)
    }

    /// Create a file.
    pub fn create_file(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<InodeId> {
        #[derive(Serialize)]
        struct CreateFileRequest {
            parent: InodeId,
            name: String,
            mode: u32,
            uid: u32,
            gid: u32,
        }

        #[derive(Deserialize)]
        struct CreateResponse {
            success: bool,
            inode: Option<u64>,
            error: Option<String>,
        }

        let url = format!("http://{}/metadata/create_file", self.metadata_addr);
        let request = CreateFileRequest {
            parent,
            name: name.to_string(),
            mode,
            uid,
            gid,
        };

        let response: CreateResponse = self.http_client
            .post(&url)
            .json(&request)
            .send()
            .map_err(|e| StrataError::Network(e.to_string()))?
            .json()
            .map_err(|e| StrataError::Deserialization(e.to_string()))?;

        if response.success {
            // Lookup to get the created inode
            if let Some(inode) = self.lookup(parent, name)? {
                return Ok(inode.id);
            }
        }

        Err(StrataError::Internal(
            response.error.unwrap_or_else(|| "Create failed".to_string()),
        ))
    }

    /// Create a directory.
    pub fn mkdir(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<InodeId> {
        #[derive(Serialize)]
        struct CreateDirRequest {
            parent: InodeId,
            name: String,
            mode: u32,
            uid: u32,
            gid: u32,
        }

        #[derive(Deserialize)]
        struct CreateResponse {
            success: bool,
            inode: Option<u64>,
            error: Option<String>,
        }

        let url = format!("http://{}/metadata/create_directory", self.metadata_addr);
        let request = CreateDirRequest {
            parent,
            name: name.to_string(),
            mode,
            uid,
            gid,
        };

        let response: CreateResponse = self.http_client
            .post(&url)
            .json(&request)
            .send()
            .map_err(|e| StrataError::Network(e.to_string()))?
            .json()
            .map_err(|e| StrataError::Deserialization(e.to_string()))?;

        if response.success {
            if let Some(inode) = self.lookup(parent, name)? {
                return Ok(inode.id);
            }
        }

        Err(StrataError::Internal(
            response.error.unwrap_or_else(|| "Mkdir failed".to_string()),
        ))
    }

    /// Delete a file or directory.
    pub fn unlink(&self, parent: InodeId, name: &str) -> Result<()> {
        #[derive(Serialize)]
        struct DeleteRequest {
            parent: InodeId,
            name: String,
        }

        #[derive(Deserialize)]
        struct DeleteResponse {
            success: bool,
            error: Option<String>,
        }

        let url = format!("http://{}/metadata/delete", self.metadata_addr);
        let request = DeleteRequest {
            parent,
            name: name.to_string(),
        };

        let response: DeleteResponse = self.http_client
            .post(&url)
            .json(&request)
            .send()
            .map_err(|e| StrataError::Network(e.to_string()))?
            .json()
            .map_err(|e| StrataError::Deserialization(e.to_string()))?;

        if response.success {
            Ok(())
        } else {
            Err(StrataError::Internal(
                response.error.unwrap_or_else(|| "Delete failed".to_string()),
            ))
        }
    }

    /// Rename a file or directory.
    pub fn rename(
        &self,
        src_parent: InodeId,
        src_name: &str,
        dst_parent: InodeId,
        dst_name: &str,
    ) -> Result<()> {
        #[derive(Serialize)]
        struct RenameRequest {
            src_parent: InodeId,
            src_name: String,
            dst_parent: InodeId,
            dst_name: String,
        }

        #[derive(Deserialize)]
        struct RenameResponse {
            success: bool,
            error: Option<String>,
        }

        let url = format!("http://{}/metadata/rename", self.metadata_addr);
        let request = RenameRequest {
            src_parent,
            src_name: src_name.to_string(),
            dst_parent,
            dst_name: dst_name.to_string(),
        };

        let response: RenameResponse = self.http_client
            .post(&url)
            .json(&request)
            .send()
            .map_err(|e| StrataError::Network(e.to_string()))?
            .json()
            .map_err(|e| StrataError::Deserialization(e.to_string()))?;

        if response.success {
            Ok(())
        } else {
            Err(StrataError::Internal(
                response.error.unwrap_or_else(|| "Rename failed".to_string()),
            ))
        }
    }

    /// Open a file and return a file handle.
    pub fn open(&mut self, inode: InodeId, flags: i32) -> Result<FileHandle> {
        let handle = self.next_handle;
        self.next_handle += 1;

        self.handles.insert(handle, HandleInfo { inode, flags });

        Ok(handle)
    }

    /// Close a file handle.
    pub fn release(&mut self, handle: FileHandle) -> Result<()> {
        self.handles.remove(&handle);
        Ok(())
    }

    /// Read data from a file.
    pub fn read(
        &self,
        _handle: FileHandle,
        inode: InodeId,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        // Get file metadata to find chunk locations
        let file_inode = self.getattr(inode)?
            .ok_or_else(|| StrataError::FileNotFound(format!("inode {}", inode)))?;

        if file_inode.chunks.is_empty() {
            return Ok(Vec::new());
        }

        // Calculate which chunks we need based on offset and size
        let chunk_size = crate::types::DEFAULT_CHUNK_SIZE as u64;
        let start_chunk = (offset / chunk_size) as usize;
        let end_chunk = ((offset + size as u64 - 1) / chunk_size) as usize;

        let mut result = Vec::with_capacity(size);

        for chunk_idx in start_chunk..=end_chunk {
            if chunk_idx >= file_inode.chunks.len() {
                break;
            }

            let chunk_id = file_inode.chunks[chunk_idx];

            // Try to read from data servers
            let chunk_data = self.read_chunk(chunk_id)?;

            // Calculate how much of this chunk we need
            let chunk_start = if chunk_idx == start_chunk {
                (offset % chunk_size) as usize
            } else {
                0
            };

            let chunk_end = if chunk_idx == end_chunk {
                let end_in_chunk = ((offset + size as u64 - 1) % chunk_size) as usize + 1;
                end_in_chunk.min(chunk_data.len())
            } else {
                chunk_data.len()
            };

            if chunk_start < chunk_data.len() {
                result.extend_from_slice(&chunk_data[chunk_start..chunk_end]);
            }
        }

        Ok(result)
    }

    /// Read a chunk from data servers.
    fn read_chunk(&self, chunk_id: ChunkId) -> Result<Vec<u8>> {
        if self.data_servers.is_empty() {
            return Err(StrataError::DataServerUnavailable("No data servers configured".into()));
        }

        // Try to read shards from data servers and decode using erasure coding
        let coder = ErasureCoder::new(self.erasure_config)?;
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; coder.total_shards()];

        for (shard_idx, server_addr) in self.data_servers.iter().enumerate() {
            if shard_idx >= coder.total_shards() {
                break;
            }

            let url = format!("http://{}/shard/{}/{}", server_addr, chunk_id.0, shard_idx);
            match self.http_client.get(&url).send() {
                Ok(response) if response.status().is_success() => {
                    if let Ok(data) = response.bytes() {
                        shards[shard_idx] = Some(data.to_vec());
                    }
                }
                _ => continue,
            }
        }

        // Count available shards
        let available = shards.iter().filter(|s| s.is_some()).count();
        if available < coder.data_shards() {
            return Err(StrataError::InsufficientShards {
                available,
                required: coder.data_shards(),
            });
        }

        // Decode the data
        coder.decode(&shards)
    }

    /// Write data to a file.
    pub fn write(
        &self,
        _handle: FileHandle,
        inode: InodeId,
        offset: u64,
        data: &[u8],
    ) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }

        if self.data_servers.is_empty() {
            return Err(StrataError::DataServerUnavailable("No data servers configured".into()));
        }

        // For now, we only support appending at offset 0 for simplicity
        // A full implementation would handle partial writes and chunking
        if offset != 0 {
            return Err(StrataError::Internal("Only offset 0 writes supported currently".into()));
        }

        // Create a new chunk for the data
        let chunk_id = ChunkId::new();

        // Encode with erasure coding
        let coder = ErasureCoder::new(self.erasure_config)?;
        let shards = coder.encode(data)?;

        // Write shards to data servers
        for (shard_idx, shard) in shards.iter().enumerate() {
            if shard_idx >= self.data_servers.len() {
                break;
            }

            let server_addr = &self.data_servers[shard_idx];
            let url = format!("http://{}/shard/{}/{}", server_addr, chunk_id.0, shard_idx);

            self.http_client
                .put(&url)
                .body(shard.clone())
                .send()
                .map_err(|e| StrataError::Network(e.to_string()))?;
        }

        // Update file metadata to add the chunk
        self.add_chunk_to_file(inode, chunk_id)?;

        Ok(data.len())
    }

    /// Add a chunk to a file's chunk list (metadata operation).
    fn add_chunk_to_file(&self, inode: InodeId, chunk_id: ChunkId) -> Result<()> {
        #[derive(Serialize)]
        struct AddChunkRequest {
            inode: InodeId,
            chunk_id: ChunkId,
        }

        #[derive(Deserialize)]
        struct AddChunkResponse {
            success: bool,
            error: Option<String>,
        }

        let url = format!("http://{}/metadata/add_chunk", self.metadata_addr);
        let request = AddChunkRequest { inode, chunk_id };

        let response: AddChunkResponse = self.http_client
            .post(&url)
            .json(&request)
            .send()
            .map_err(|e| StrataError::Network(e.to_string()))?
            .json()
            .map_err(|e| StrataError::Deserialization(e.to_string()))?;

        if response.success {
            Ok(())
        } else {
            Err(StrataError::Internal(
                response.error.unwrap_or_else(|| "Add chunk failed".to_string()),
            ))
        }
    }
}
