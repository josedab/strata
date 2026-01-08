//! Client library for communicating with Strata servers.

use crate::error::{Result, StrataError};
use crate::types::{ChunkId, FileType, Inode, InodeId};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

/// Default connection timeout for client requests.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default request timeout for client operations.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Client for communicating with the metadata server.
#[derive(Clone)]
pub struct MetadataClient {
    base_url: String,
    client: Client,
}

impl MetadataClient {
    /// Create a new metadata client with default timeouts.
    pub fn new(addr: SocketAddr) -> Self {
        Self::with_timeouts(addr, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT)
    }

    /// Create a new metadata client with custom timeouts.
    pub fn with_timeouts(
        addr: SocketAddr,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Self {
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            base_url: format!("http://{}", addr),
            client,
        }
    }

    /// Create from address string.
    pub fn from_addr(addr: &str) -> Result<Self> {
        let addr: SocketAddr = addr
            .parse()
            .map_err(|e| StrataError::Internal(format!("Invalid address: {}", e)))?;
        Ok(Self::new(addr))
    }

    /// Check server health.
    pub async fn health(&self) -> Result<bool> {
        let url = format!("{}/health", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;
        Ok(response.status().is_success())
    }

    /// Lookup an inode by parent and name.
    pub async fn lookup(&self, parent: InodeId, name: &str) -> Result<Option<Inode>> {
        let url = format!("{}/metadata/lookup", self.base_url);
        let request = LookupRequest {
            parent,
            name: name.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        let result: LookupResponse = response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))?;

        Ok(result.inode)
    }

    /// Create a file.
    pub async fn create_file(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<CreateResponse> {
        let url = format!("{}/metadata/create_file", self.base_url);
        let request = CreateFileRequest {
            parent,
            name: name.to_string(),
            mode,
            uid,
            gid,
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    /// Create a directory.
    pub async fn create_directory(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<CreateResponse> {
        let url = format!("{}/metadata/create_directory", self.base_url);
        let request = CreateDirectoryRequest {
            parent,
            name: name.to_string(),
            mode,
            uid,
            gid,
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    /// Delete a file or directory.
    pub async fn delete(&self, parent: InodeId, name: &str) -> Result<DeleteResponse> {
        let url = format!("{}/metadata/delete", self.base_url);
        let request = DeleteRequest {
            parent,
            name: name.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    /// Rename a file or directory.
    pub async fn rename(
        &self,
        src_parent: InodeId,
        src_name: &str,
        dst_parent: InodeId,
        dst_name: &str,
    ) -> Result<DeleteResponse> {
        let url = format!("{}/metadata/rename", self.base_url);
        let request = RenameRequest {
            src_parent,
            src_name: src_name.to_string(),
            dst_parent,
            dst_name: dst_name.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    /// List directory contents.
    pub async fn readdir(&self, inode: InodeId) -> Result<ReaddirResponse> {
        let url = format!("{}/metadata/readdir", self.base_url);
        let request = ReaddirRequest { inode };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    /// Get inode attributes.
    pub async fn getattr(&self, inode: InodeId) -> Result<Option<Inode>> {
        let url = format!("{}/metadata/getattr", self.base_url);
        let request = GetattrRequest { inode };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        let result: GetattrResponse = response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))?;

        Ok(result.inode)
    }

    /// Add a chunk to an inode.
    pub async fn add_chunk(&self, inode: InodeId, chunk_id: ChunkId) -> Result<()> {
        let url = format!("{}/metadata/add_chunk", self.base_url);
        let request = AddChunkRequest { inode, chunk_id };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(StrataError::Internal("Failed to add chunk".into()))
        }
    }

    /// Set the size of a file.
    pub async fn set_size(&self, inode: InodeId, size: u64) -> Result<()> {
        let url = format!("{}/metadata/set_size", self.base_url);
        let request = SetSizeRequest { inode, size };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(StrataError::Internal("Failed to set size".into()))
        }
    }

    // Multipart upload operations

    /// Initiate a multipart upload.
    pub async fn initiate_multipart_upload(
        &self,
        bucket_inode: InodeId,
        key: &str,
        upload_id: &str,
    ) -> Result<()> {
        let url = format!("{}/metadata/multipart/initiate", self.base_url);
        let request = InitiateMultipartRequest {
            bucket_inode,
            key: key.to_string(),
            upload_id: upload_id.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            let error: ErrorResponse = response
                .json()
                .await
                .unwrap_or_else(|_| ErrorResponse {
                    error: "Unknown error".to_string(),
                });
            Err(StrataError::Internal(error.error))
        }
    }

    /// Upload a part of a multipart upload.
    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: u32,
        chunk_id: ChunkId,
        size: u64,
        etag: &str,
    ) -> Result<()> {
        let url = format!("{}/metadata/multipart/upload_part", self.base_url);
        let request = UploadPartRequest {
            upload_id: upload_id.to_string(),
            part_number,
            chunk_id,
            size,
            etag: etag.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            let error: ErrorResponse = response
                .json()
                .await
                .unwrap_or_else(|_| ErrorResponse {
                    error: "Unknown error".to_string(),
                });
            Err(StrataError::Internal(error.error))
        }
    }

    /// Complete a multipart upload.
    pub async fn complete_multipart_upload(
        &self,
        upload_id: &str,
        parts: Vec<CompletedPartRequest>,
    ) -> Result<CompleteMultipartResponse> {
        let url = format!("{}/metadata/multipart/complete", self.base_url);
        let request = CompleteMultipartRequest {
            upload_id: upload_id.to_string(),
            parts,
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            response
                .json()
                .await
                .map_err(|e| StrataError::Deserialization(e.to_string()))
        } else {
            let error: ErrorResponse = response
                .json()
                .await
                .unwrap_or_else(|_| ErrorResponse {
                    error: "Unknown error".to_string(),
                });
            Err(StrataError::Internal(error.error))
        }
    }

    /// Abort a multipart upload.
    pub async fn abort_multipart_upload(&self, upload_id: &str) -> Result<()> {
        let url = format!("{}/metadata/multipart/abort", self.base_url);
        let request = AbortMultipartRequest {
            upload_id: upload_id.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            let error: ErrorResponse = response
                .json()
                .await
                .unwrap_or_else(|_| ErrorResponse {
                    error: "Unknown error".to_string(),
                });
            Err(StrataError::Internal(error.error))
        }
    }

    /// List parts for a multipart upload.
    pub async fn list_parts(&self, upload_id: &str) -> Result<ListPartsResponse> {
        let url = format!("{}/metadata/multipart/list_parts", self.base_url);
        let request = ListPartsRequest {
            upload_id: upload_id.to_string(),
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            response
                .json()
                .await
                .map_err(|e| StrataError::Internal(format!("Failed to parse response: {}", e)))
        } else {
            let error: ErrorResponse = response
                .json()
                .await
                .unwrap_or_else(|_| ErrorResponse {
                    error: "Unknown error".to_string(),
                });
            Err(StrataError::Internal(error.error))
        }
    }

    /// List all multipart uploads for a bucket.
    pub async fn list_multipart_uploads(&self, bucket_inode: InodeId) -> Result<ListMultipartUploadsResponse> {
        let url = format!("{}/metadata/multipart/list_uploads", self.base_url);
        let request = ListMultipartUploadsRequest {
            bucket_inode,
        };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            response
                .json()
                .await
                .map_err(|e| StrataError::Internal(format!("Failed to parse response: {}", e)))
        } else {
            let error: ErrorResponse = response
                .json()
                .await
                .unwrap_or_else(|_| ErrorResponse {
                    error: "Unknown error".to_string(),
                });
            Err(StrataError::Internal(error.error))
        }
    }
}

/// Client for communicating with data servers.
#[derive(Clone)]
pub struct DataClient {
    base_url: String,
    client: Client,
}

impl DataClient {
    /// Create a new data client with default timeouts.
    pub fn new(addr: SocketAddr) -> Self {
        Self::with_timeouts(addr, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT)
    }

    /// Create a new data client with custom timeouts.
    pub fn with_timeouts(
        addr: SocketAddr,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Self {
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .unwrap_or_else(|_| Client::new());

        Self {
            base_url: format!("http://{}", addr),
            client,
        }
    }

    /// Create from address string.
    pub fn from_addr(addr: &str) -> Result<Self> {
        let addr: SocketAddr = addr
            .parse()
            .map_err(|e| StrataError::Internal(format!("Invalid address: {}", e)))?;
        Ok(Self::new(addr))
    }

    /// Check server health.
    pub async fn health(&self) -> Result<bool> {
        let url = format!("{}/health", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;
        Ok(response.status().is_success())
    }

    /// Get server status.
    pub async fn status(&self) -> Result<DataServerStatus> {
        let url = format!("{}/status", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    /// Write a chunk.
    pub async fn write_chunk(&self, chunk_id: ChunkId, data: &[u8]) -> Result<WriteChunkResponse> {
        let url = format!("{}/chunk/{}", self.base_url, chunk_id.0);
        let response = self
            .client
            .put(&url)
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        response
            .json()
            .await
            .map_err(|e| StrataError::Deserialization(e.to_string()))
    }

    /// Read a chunk.
    pub async fn read_chunk(&self, chunk_id: ChunkId, size: usize) -> Result<Vec<u8>> {
        let url = format!("{}/chunk/{}?size={}", self.base_url, chunk_id.0, size);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(StrataError::NotFound(format!("Chunk {}", chunk_id.0)));
        }

        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| StrataError::Network(e.to_string()))
    }

    /// Delete a chunk.
    pub async fn delete_chunk(&self, chunk_id: ChunkId) -> Result<()> {
        let url = format!("{}/chunk/{}", self.base_url, chunk_id.0);
        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(StrataError::Internal("Failed to delete chunk".into()))
        }
    }

    /// Write a shard.
    pub async fn write_shard(
        &self,
        chunk_id: ChunkId,
        shard_index: usize,
        data: &[u8],
    ) -> Result<()> {
        let url = format!("{}/shard/{}/{}", self.base_url, chunk_id.0, shard_index);
        let response = self
            .client
            .put(&url)
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(StrataError::Internal("Failed to write shard".into()))
        }
    }

    /// Read a shard.
    pub async fn read_shard(&self, chunk_id: ChunkId, shard_index: usize) -> Result<Vec<u8>> {
        let url = format!("{}/shard/{}/{}", self.base_url, chunk_id.0, shard_index);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if !response.status().is_success() {
            return Err(StrataError::NotFound(format!(
                "Shard {}:{}",
                chunk_id.0, shard_index
            )));
        }

        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| StrataError::Network(e.to_string()))
    }
}

// Request/Response types

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

#[derive(Serialize)]
struct CreateFileRequest {
    parent: InodeId,
    name: String,
    mode: u32,
    uid: u32,
    gid: u32,
}

#[derive(Serialize)]
struct CreateDirectoryRequest {
    parent: InodeId,
    name: String,
    mode: u32,
    uid: u32,
    gid: u32,
}

#[derive(Deserialize)]
pub struct CreateResponse {
    pub success: bool,
    pub inode: Option<InodeId>,
    pub error: Option<String>,
}

#[derive(Serialize)]
struct DeleteRequest {
    parent: InodeId,
    name: String,
}

#[derive(Deserialize)]
pub struct DeleteResponse {
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Serialize)]
struct RenameRequest {
    src_parent: InodeId,
    src_name: String,
    dst_parent: InodeId,
    dst_name: String,
}

#[derive(Serialize)]
struct ReaddirRequest {
    inode: InodeId,
}

#[derive(Deserialize)]
pub struct ReaddirResponse {
    pub success: bool,
    pub entries: Vec<DirEntry>,
    pub error: Option<String>,
}

#[derive(Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub inode: InodeId,
    pub file_type: FileType,
}

#[derive(Serialize)]
struct GetattrRequest {
    inode: InodeId,
}

#[derive(Deserialize)]
struct GetattrResponse {
    found: bool,
    inode: Option<Inode>,
}

#[derive(Serialize)]
struct AddChunkRequest {
    inode: InodeId,
    chunk_id: ChunkId,
}

#[derive(Serialize)]
struct SetSizeRequest {
    inode: InodeId,
    size: u64,
}

#[derive(Deserialize)]
pub struct DataServerStatus {
    pub server_id: u64,
    pub usage_bytes: u64,
    pub healthy: bool,
}

#[derive(Deserialize)]
pub struct WriteChunkResponse {
    pub success: bool,
    pub shards: Vec<usize>,
    pub error: Option<String>,
}

// Multipart upload request/response types

#[derive(Serialize)]
struct InitiateMultipartRequest {
    bucket_inode: InodeId,
    key: String,
    upload_id: String,
}

#[derive(Serialize)]
struct UploadPartRequest {
    upload_id: String,
    part_number: u32,
    chunk_id: ChunkId,
    size: u64,
    etag: String,
}

#[derive(Serialize)]
struct CompleteMultipartRequest {
    upload_id: String,
    parts: Vec<CompletedPartRequest>,
}

/// A completed part for multipart upload completion request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedPartRequest {
    pub part_number: u32,
    pub etag: String,
}

#[derive(Deserialize)]
pub struct CompleteMultipartResponse {
    pub success: bool,
    pub inode: Option<InodeId>,
    pub error: Option<String>,
}

#[derive(Serialize)]
struct AbortMultipartRequest {
    upload_id: String,
}

#[derive(Serialize)]
struct ListPartsRequest {
    upload_id: String,
}

/// Response for list parts request.
#[derive(Debug, Clone, Deserialize)]
pub struct ListPartsResponse {
    /// The upload ID.
    pub upload_id: String,
    /// The bucket inode.
    pub bucket_inode: InodeId,
    /// The object key.
    pub key: String,
    /// The parts that have been uploaded.
    pub parts: Vec<PartInfoResponse>,
}

/// Part information in list parts response.
#[derive(Debug, Clone, Deserialize)]
pub struct PartInfoResponse {
    /// Part number (1-10000).
    pub part_number: u32,
    /// Size in bytes.
    pub size: u64,
    /// ETag (MD5 hash).
    pub etag: String,
}

#[derive(Serialize)]
struct ListMultipartUploadsRequest {
    bucket_inode: InodeId,
}

/// Response for list multipart uploads request.
#[derive(Debug, Clone, Deserialize)]
pub struct ListMultipartUploadsResponse {
    /// The uploads in progress.
    pub uploads: Vec<MultipartUploadInfo>,
}

/// Information about an in-progress multipart upload.
#[derive(Debug, Clone, Deserialize)]
pub struct MultipartUploadInfo {
    /// Upload ID.
    pub upload_id: String,
    /// Object key.
    pub key: String,
    /// When the upload was initiated (ISO 8601).
    pub initiated: String,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_client_creation() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let client = MetadataClient::new(addr);
        assert_eq!(client.base_url, "http://127.0.0.1:8080");
    }

    #[test]
    fn test_data_client_creation() {
        let addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        let client = DataClient::new(addr);
        assert_eq!(client.base_url, "http://127.0.0.1:9090");
    }
}
