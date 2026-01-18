//! Metadata state machine for Raft replication.
//!
//! Uses snapshot caching for efficient repeated snapshot operations.
//! The serialized snapshot is cached and invalidated on mutations.

use super::operations::{CompletedPart, MetadataOp, OpResult};
use crate::error::Result;
use crate::raft::StateMachine;
use crate::types::*;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, SystemTime};
use tracing::{debug, error, warn};

/// Default expiration time for multipart uploads (24 hours).
const MULTIPART_UPLOAD_EXPIRY: Duration = Duration::from_secs(24 * 60 * 60);

/// Snapshot of the metadata state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    pub inodes: BTreeMap<InodeId, Inode>,
    pub directories: BTreeMap<InodeId, Directory>,
    pub chunk_map: BTreeMap<ChunkId, ChunkMeta>,
    pub data_servers: HashMap<DataServerId, DataServerInfo>,
    pub multipart_uploads: HashMap<String, MultipartUpload>,
    pub next_inode_id: u64,
}

/// Tracks an in-progress multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    /// Unique upload identifier.
    pub upload_id: String,
    /// Inode of the bucket containing the upload.
    pub bucket_inode: InodeId,
    /// Object key being uploaded.
    pub key: String,
    /// Uploaded parts, keyed by part number.
    pub parts: BTreeMap<u32, UploadPart>,
    /// When the upload was initiated.
    pub initiated_at: SystemTime,
    /// When the upload expires (and can be garbage collected).
    pub expires_at: SystemTime,
}

/// A single uploaded part in a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadPart {
    /// Part number (1-10000).
    pub part_number: u32,
    /// Chunk ID storing the part data.
    pub chunk_id: ChunkId,
    /// Size of the part in bytes.
    pub size: u64,
    /// ETag (MD5 hash) of the part.
    pub etag: String,
}

/// The metadata state machine, storing all file system metadata.
///
/// Uses snapshot caching to avoid redundant serialization.
/// Mutations invalidate the cache; next snapshot() recomputes it.
pub struct MetadataStateMachine {
    /// All inodes in the system.
    inodes: BTreeMap<InodeId, Inode>,
    /// Directory contents.
    directories: BTreeMap<InodeId, Directory>,
    /// Chunk to data server mapping.
    chunk_map: BTreeMap<ChunkId, ChunkMeta>,
    /// Data server information.
    data_servers: HashMap<DataServerId, DataServerInfo>,
    /// In-progress multipart uploads.
    multipart_uploads: HashMap<String, MultipartUpload>,
    /// Next inode ID to allocate.
    next_inode_id: AtomicU64,
    /// Cached serialized snapshot (invalidated on mutation).
    snapshot_cache: Mutex<Option<Vec<u8>>>,
    /// Whether the cache is valid.
    snapshot_valid: AtomicBool,
}

impl MetadataStateMachine {
    /// Create a new metadata state machine with a root directory.
    pub fn new() -> Self {
        let mut inodes = BTreeMap::new();
        let mut directories = BTreeMap::new();

        // Create root directory (inode 1)
        let root_inode = Inode::new_directory(1, 0o755, 0, 0);
        inodes.insert(1, root_inode);
        directories.insert(1, Directory::new(1, 1)); // Root's parent is itself

        Self {
            inodes,
            directories,
            chunk_map: BTreeMap::new(),
            data_servers: HashMap::new(),
            multipart_uploads: HashMap::new(),
            next_inode_id: AtomicU64::new(2), // 1 is reserved for root
            snapshot_cache: Mutex::new(None),
            snapshot_valid: AtomicBool::new(false),
        }
    }

    /// Invalidate the snapshot cache (call after any mutation).
    #[inline]
    fn invalidate_snapshot_cache(&self) {
        self.snapshot_valid.store(false, Ordering::Release);
    }

    /// Allocate a new inode ID.
    fn allocate_inode_id(&self) -> InodeId {
        self.next_inode_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Apply a metadata operation (takes ownership to avoid cloning).
    /// Invalidates the snapshot cache since state is being modified.
    pub fn apply_op(&mut self, op: MetadataOp) -> OpResult {
        // Invalidate snapshot cache before any mutation
        self.invalidate_snapshot_cache();
        match op {
            MetadataOp::CreateFile { parent, name, mode, uid, gid } => {
                self.create_file(parent, name, mode, uid, gid)
            }
            MetadataOp::CreateDirectory { parent, name, mode, uid, gid } => {
                self.create_directory(parent, name, mode, uid, gid)
            }
            MetadataOp::CreateSymlink { parent, name, target, uid, gid } => {
                self.create_symlink(parent, name, target, uid, gid)
            }
            MetadataOp::Delete { parent, name } => {
                self.delete(parent, name)
            }
            MetadataOp::Rename { src_parent, src_name, dst_parent, dst_name } => {
                self.rename(src_parent, src_name, dst_parent, dst_name)
            }
            MetadataOp::Link { parent, name, inode } => {
                self.link(parent, name, inode)
            }
            MetadataOp::SetAttr { inode, mode, uid, gid, size, atime, mtime } => {
                self.set_attr(inode, mode, uid, gid, size, atime, mtime)
            }
            MetadataOp::SetXattr { inode, name, value } => {
                self.set_xattr(inode, name, value)
            }
            MetadataOp::RemoveXattr { inode, name } => {
                self.remove_xattr(inode, name)
            }
            MetadataOp::AllocateChunks { inode, chunks } => {
                self.allocate_chunks(inode, chunks)
            }
            MetadataOp::AddChunk { inode, chunk_id } => {
                self.add_chunk(inode, chunk_id)
            }
            MetadataOp::UpdateChunkLocations { chunk_id, locations } => {
                self.update_chunk_locations(chunk_id, locations)
            }
            MetadataOp::RemoveChunk { chunk_id } => {
                self.remove_chunk(chunk_id)
            }
            MetadataOp::RegisterDataServer { server_id, address, capacity } => {
                self.register_data_server(server_id, address, capacity)
            }
            MetadataOp::UpdateDataServerStatus { server_id, used, online } => {
                self.update_data_server_status(server_id, used, online)
            }
            MetadataOp::DeregisterDataServer { server_id } => {
                self.deregister_data_server(server_id)
            }
            MetadataOp::InitiateMultipartUpload {
                bucket_inode,
                key,
                upload_id,
            } => self.initiate_multipart_upload(bucket_inode, key, upload_id),
            MetadataOp::UploadPart {
                upload_id,
                part_number,
                chunk_id,
                size,
                etag,
            } => self.upload_part(upload_id, part_number, chunk_id, size, etag),
            MetadataOp::CompleteMultipartUpload { upload_id, parts } => {
                self.complete_multipart_upload(upload_id, parts)
            }
            MetadataOp::AbortMultipartUpload { upload_id } => {
                self.abort_multipart_upload(upload_id)
            }
        }
    }

    fn create_file(&mut self, parent: InodeId, name: String, mode: u32, uid: u32, gid: u32) -> OpResult {
        // Check parent exists and name doesn't already exist
        if let Some(parent_dir) = self.directories.get(&parent) {
            if parent_dir.get_entry(&name).is_some() {
                return OpResult::Error {
                    message: format!("File already exists: {}", name),
                };
            }
        } else {
            return OpResult::Error {
                message: format!("Parent directory not found: {}", parent),
            };
        }

        // Allocate inode ID before getting mutable references
        let inode_id = self.allocate_inode_id();
        let inode = Inode::new_file(inode_id, mode, uid, gid);

        self.inodes.insert(inode_id, inode);

        // Now get mutable reference to parent dir and add entry
        if let Some(parent_dir) = self.directories.get_mut(&parent) {
            parent_dir.add_entry(name, inode_id, FileType::RegularFile);
        }

        // Update parent mtime
        if let Some(parent_inode) = self.inodes.get_mut(&parent) {
            parent_inode.touch();
        }

        debug!(inode = inode_id, "Created file");
        OpResult::Created { inode: inode_id }
    }

    fn create_directory(&mut self, parent: InodeId, name: String, mode: u32, uid: u32, gid: u32) -> OpResult {
        // Check parent exists and name doesn't already exist
        if let Some(parent_dir) = self.directories.get(&parent) {
            if parent_dir.get_entry(&name).is_some() {
                return OpResult::Error {
                    message: format!("Directory already exists: {}", name),
                };
            }
        } else {
            return OpResult::Error {
                message: format!("Parent directory not found: {}", parent),
            };
        }

        // Allocate inode ID before getting mutable references
        let inode_id = self.allocate_inode_id();
        let inode = Inode::new_directory(inode_id, mode, uid, gid);

        self.inodes.insert(inode_id, inode);
        self.directories.insert(inode_id, Directory::new(inode_id, parent));

        // Now get mutable reference to parent dir and add entry
        if let Some(parent_dir) = self.directories.get_mut(&parent) {
            parent_dir.add_entry(name, inode_id, FileType::Directory);
        }

        // Increment parent's link count (for ..)
        if let Some(parent_inode) = self.inodes.get_mut(&parent) {
            parent_inode.nlink += 1;
            parent_inode.touch();
        }

        debug!(inode = inode_id, "Created directory");
        OpResult::Created { inode: inode_id }
    }

    fn create_symlink(&mut self, parent: InodeId, name: String, target: String, uid: u32, gid: u32) -> OpResult {
        // Check parent exists and name doesn't already exist
        if let Some(parent_dir) = self.directories.get(&parent) {
            if parent_dir.get_entry(&name).is_some() {
                return OpResult::Error {
                    message: format!("Name already exists: {}", name),
                };
            }
        } else {
            return OpResult::Error {
                message: format!("Parent directory not found: {}", parent),
            };
        }

        // Allocate inode ID before getting mutable references
        let inode_id = self.allocate_inode_id();
        let inode = Inode::new_symlink(inode_id, uid, gid, target);

        self.inodes.insert(inode_id, inode);

        // Now get mutable reference to parent dir and add entry
        if let Some(parent_dir) = self.directories.get_mut(&parent) {
            parent_dir.add_entry(name, inode_id, FileType::Symlink);
        }

        if let Some(parent_inode) = self.inodes.get_mut(&parent) {
            parent_inode.touch();
        }

        debug!(inode = inode_id, "Created symlink");
        OpResult::Created { inode: inode_id }
    }

    fn delete(&mut self, parent: InodeId, name: String) -> OpResult {
        // First, get the entry information (read-only)
        let entry_info = {
            if let Some(parent_dir) = self.directories.get(&parent) {
                parent_dir.get_entry(&name).cloned()
            } else {
                return OpResult::Error {
                    message: format!("Parent directory not found: {}", parent),
                };
            }
        };

        let entry = match entry_info {
            Some(e) => e,
            None => return OpResult::Error {
                message: format!("Entry not found: {}", name),
            },
        };

        let inode_id = entry.inode;

        // Check if it's a non-empty directory
        let is_dir = self.inodes.get(&inode_id).map(|i| i.is_dir()).unwrap_or(false);
        if is_dir {
            if let Some(dir) = self.directories.get(&inode_id) {
                if !dir.is_empty() {
                    return OpResult::Error {
                        message: "Directory not empty".to_string(),
                    };
                }
            }
        }

        // Remove the entry from parent directory
        if let Some(parent_dir) = self.directories.get_mut(&parent) {
            parent_dir.remove_entry(&name);
        }

        // Handle directory-specific cleanup
        if is_dir {
            self.directories.remove(&inode_id);
            // Decrement parent's link count
            if let Some(parent_inode) = self.inodes.get_mut(&parent) {
                parent_inode.nlink = parent_inode.nlink.saturating_sub(1);
            }
        }

        // Decrement link count and maybe remove inode
        let should_remove = if let Some(inode) = self.inodes.get_mut(&inode_id) {
            inode.nlink = inode.nlink.saturating_sub(1);
            inode.nlink == 0
        } else {
            false
        };

        if should_remove {
            let chunks = self.inodes.get(&inode_id).map(|i| i.chunks.clone()).unwrap_or_default();
            self.inodes.remove(&inode_id);
            debug!(inode = inode_id, chunks = ?chunks, "Inode deleted");
        }

        // Update parent mtime
        if let Some(parent_inode) = self.inodes.get_mut(&parent) {
            parent_inode.touch();
        }

        OpResult::Success
    }

    fn rename(&mut self, src_parent: InodeId, src_name: String, dst_parent: InodeId, dst_name: String) -> OpResult {
        // Get the source entry
        let entry = {
            if let Some(src_dir) = self.directories.get(&src_parent) {
                src_dir.get_entry(&src_name).cloned()
            } else {
                return OpResult::Error {
                    message: "Source parent not found".to_string(),
                };
            }
        };

        let entry = match entry {
            Some(e) => e,
            None => return OpResult::Error {
                message: format!("Source not found: {}", src_name),
            },
        };

        // Check destination parent exists
        if !self.directories.contains_key(&dst_parent) {
            return OpResult::Error {
                message: "Destination parent not found".to_string(),
            };
        }

        // Remove from source
        if let Some(src_dir) = self.directories.get_mut(&src_parent) {
            src_dir.remove_entry(&src_name);
        }

        // Handle existing destination (replace)
        if let Some(dst_dir) = self.directories.get_mut(&dst_parent) {
            if let Some(existing) = dst_dir.remove_entry(&dst_name) {
                // Handle replacement
                if let Some(existing_inode) = self.inodes.get_mut(&existing.inode) {
                    existing_inode.nlink = existing_inode.nlink.saturating_sub(1);
                    if existing_inode.nlink == 0 && !existing_inode.is_dir() {
                        self.inodes.remove(&existing.inode);
                    }
                }
            }
            dst_dir.add_entry(dst_name, entry.inode, entry.file_type);
        }

        // Update directory parent if moving a directory
        if entry.file_type == FileType::Directory {
            if let Some(dir) = self.directories.get_mut(&entry.inode) {
                dir.parent = dst_parent;
                // Update .. entry
                if let Some(dotdot) = dir.entries.get_mut("..") {
                    dotdot.inode = dst_parent;
                }
            }

            // Update link counts if parents differ
            if src_parent != dst_parent {
                if let Some(src_inode) = self.inodes.get_mut(&src_parent) {
                    src_inode.nlink = src_inode.nlink.saturating_sub(1);
                }
                if let Some(dst_inode) = self.inodes.get_mut(&dst_parent) {
                    dst_inode.nlink += 1;
                }
            }
        }

        // Update mtimes
        if let Some(inode) = self.inodes.get_mut(&src_parent) {
            inode.touch();
        }
        if src_parent != dst_parent {
            if let Some(inode) = self.inodes.get_mut(&dst_parent) {
                inode.touch();
            }
        }

        OpResult::Success
    }

    fn link(&mut self, parent: InodeId, name: String, inode: InodeId) -> OpResult {
        // Check target inode exists and is not a directory
        if let Some(target) = self.inodes.get(&inode) {
            if target.is_dir() {
                return OpResult::Error {
                    message: "Cannot hard link directories".to_string(),
                };
            }
        } else {
            return OpResult::Error {
                message: "Target inode not found".to_string(),
            };
        }

        // Add entry to parent
        if let Some(parent_dir) = self.directories.get_mut(&parent) {
            if parent_dir.get_entry(&name).is_some() {
                return OpResult::Error {
                    message: format!("Name already exists: {}", name),
                };
            }

            parent_dir.add_entry(name, inode, FileType::RegularFile);

            // Increment link count
            if let Some(target) = self.inodes.get_mut(&inode) {
                target.nlink += 1;
                target.touch();
            }

            if let Some(parent_inode) = self.inodes.get_mut(&parent) {
                parent_inode.touch();
            }

            OpResult::Success
        } else {
            OpResult::Error {
                message: "Parent directory not found".to_string(),
            }
        }
    }

    fn set_attr(
        &mut self,
        inode_id: InodeId,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<u64>,
        mtime: Option<u64>,
    ) -> OpResult {
        if let Some(inode) = self.inodes.get_mut(&inode_id) {
            if let Some(m) = mode {
                inode.mode = (inode.mode & 0o170000) | (m & 0o7777);
            }
            if let Some(u) = uid {
                inode.uid = u;
            }
            if let Some(g) = gid {
                inode.gid = g;
            }
            if let Some(s) = size {
                inode.size = s;
            }
            if let Some(a) = atime {
                inode.atime = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(a);
            }
            if let Some(m) = mtime {
                inode.mtime = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(m);
            }
            inode.ctime = SystemTime::now();

            OpResult::Success
        } else {
            OpResult::Error {
                message: format!("Inode not found: {}", inode_id),
            }
        }
    }

    fn set_xattr(&mut self, inode_id: InodeId, name: String, value: Vec<u8>) -> OpResult {
        if let Some(inode) = self.inodes.get_mut(&inode_id) {
            inode.extended_attrs.insert(name, value);
            inode.ctime = SystemTime::now();
            OpResult::Success
        } else {
            OpResult::Error {
                message: format!("Inode not found: {}", inode_id),
            }
        }
    }

    fn remove_xattr(&mut self, inode_id: InodeId, name: String) -> OpResult {
        if let Some(inode) = self.inodes.get_mut(&inode_id) {
            inode.extended_attrs.remove(&name);
            inode.ctime = SystemTime::now();
            OpResult::Success
        } else {
            OpResult::Error {
                message: format!("Inode not found: {}", inode_id),
            }
        }
    }

    fn allocate_chunks(&mut self, inode_id: InodeId, chunks: Vec<ChunkId>) -> OpResult {
        if let Some(inode) = self.inodes.get_mut(&inode_id) {
            for chunk_id in &chunks {
                if !self.chunk_map.contains_key(chunk_id) {
                    self.chunk_map.insert(*chunk_id, ChunkMeta::new(*chunk_id, 0, 0));
                }
            }
            inode.chunks.extend(chunks);
            inode.touch();
            OpResult::Success
        } else {
            OpResult::Error {
                message: format!("Inode not found: {}", inode_id),
            }
        }
    }

    fn add_chunk(&mut self, inode_id: InodeId, chunk_id: ChunkId) -> OpResult {
        if let Some(inode) = self.inodes.get_mut(&inode_id) {
            // Add chunk metadata if not exists
            if !self.chunk_map.contains_key(&chunk_id) {
                self.chunk_map.insert(chunk_id, ChunkMeta::new(chunk_id, 0, 0));
            }
            inode.chunks.push(chunk_id);
            inode.touch();
            OpResult::Success
        } else {
            OpResult::Error {
                message: format!("Inode not found: {}", inode_id),
            }
        }
    }

    fn update_chunk_locations(&mut self, chunk_id: ChunkId, locations: Vec<DataServerId>) -> OpResult {
        if let Some(chunk) = self.chunk_map.get_mut(&chunk_id) {
            chunk.locations = locations;
            chunk.version += 1;
            OpResult::Success
        } else {
            OpResult::Error {
                message: format!("Chunk not found: {}", chunk_id),
            }
        }
    }

    fn remove_chunk(&mut self, chunk_id: ChunkId) -> OpResult {
        self.chunk_map.remove(&chunk_id);
        OpResult::Success
    }

    fn register_data_server(&mut self, server_id: DataServerId, address: String, capacity: u64) -> OpResult {
        let info = DataServerInfo {
            id: server_id,
            address,
            capacity,
            used: 0,
            status: ServerStatus::Online,
            last_heartbeat: SystemTime::now(),
        };
        self.data_servers.insert(server_id, info);
        OpResult::Success
    }

    fn update_data_server_status(&mut self, server_id: DataServerId, used: u64, online: bool) -> OpResult {
        if let Some(server) = self.data_servers.get_mut(&server_id) {
            server.used = used;
            server.status = if online { ServerStatus::Online } else { ServerStatus::Offline };
            server.last_heartbeat = SystemTime::now();
            OpResult::Success
        } else {
            OpResult::Error {
                message: format!("Data server not found: {}", server_id),
            }
        }
    }

    fn deregister_data_server(&mut self, server_id: DataServerId) -> OpResult {
        self.data_servers.remove(&server_id);
        OpResult::Success
    }

    // Multipart upload operations

    fn initiate_multipart_upload(
        &mut self,
        bucket_inode: InodeId,
        key: String,
        upload_id: String,
    ) -> OpResult {
        // Verify bucket exists and is a directory
        match self.inodes.get(&bucket_inode) {
            Some(inode) if inode.is_dir() => {}
            Some(_) => {
                return OpResult::Error {
                    message: format!("Inode {} is not a directory", bucket_inode),
                };
            }
            None => {
                return OpResult::Error {
                    message: format!("Bucket inode not found: {}", bucket_inode),
                };
            }
        }

        // Check if upload_id already exists
        if self.multipart_uploads.contains_key(&upload_id) {
            return OpResult::Error {
                message: format!("Upload ID already exists: {}", upload_id),
            };
        }

        let now = SystemTime::now();
        let upload = MultipartUpload {
            upload_id: upload_id.clone(),
            bucket_inode,
            key,
            parts: BTreeMap::new(),
            initiated_at: now,
            expires_at: now + MULTIPART_UPLOAD_EXPIRY,
        };

        self.multipart_uploads.insert(upload_id.clone(), upload);
        debug!(upload_id = %upload_id, "Initiated multipart upload");
        OpResult::Success
    }

    fn upload_part(
        &mut self,
        upload_id: String,
        part_number: u32,
        chunk_id: ChunkId,
        size: u64,
        etag: String,
    ) -> OpResult {
        // Validate part number (S3 allows 1-10000)
        if part_number == 0 || part_number > 10000 {
            return OpResult::Error {
                message: format!("Invalid part number: {} (must be 1-10000)", part_number),
            };
        }

        let upload = match self.multipart_uploads.get_mut(&upload_id) {
            Some(u) => u,
            None => {
                return OpResult::Error {
                    message: format!("Upload not found: {}", upload_id),
                };
            }
        };

        // Check if upload has expired
        if SystemTime::now() > upload.expires_at {
            return OpResult::Error {
                message: format!("Upload has expired: {}", upload_id),
            };
        }

        // Add chunk to chunk_map if not exists
        if !self.chunk_map.contains_key(&chunk_id) {
            self.chunk_map.insert(chunk_id, ChunkMeta::new(chunk_id, size, 0));
        }

        // Insert or replace the part
        let part = UploadPart {
            part_number,
            chunk_id,
            size,
            etag,
        };
        upload.parts.insert(part_number, part);

        debug!(upload_id = %upload_id, part_number, "Uploaded part");
        OpResult::Success
    }

    // Allow unwrap here because upload and parts existence are validated above
    // in the same single-threaded state machine context.
    #[allow(clippy::unwrap_used)]
    fn complete_multipart_upload(
        &mut self,
        upload_id: String,
        parts: Vec<CompletedPart>,
    ) -> OpResult {
        // First, validate all parts without removing the upload
        {
            let upload = match self.multipart_uploads.get(&upload_id) {
                Some(u) => u,
                None => {
                    return OpResult::Error {
                        message: format!("Upload not found: {}", upload_id),
                    };
                }
            };

            // Verify all parts exist and match
            for completed_part in &parts {
                match upload.parts.get(&completed_part.part_number) {
                    Some(uploaded_part) => {
                        if uploaded_part.etag != completed_part.etag {
                            return OpResult::Error {
                                message: format!(
                                    "ETag mismatch for part {}: expected {}, got {}",
                                    completed_part.part_number,
                                    uploaded_part.etag,
                                    completed_part.etag
                                ),
                            };
                        }
                    }
                    None => {
                        return OpResult::Error {
                            message: format!("Part not found: {}", completed_part.part_number),
                        };
                    }
                }
            }
        }

        // Now remove the upload and collect chunk info
        let upload = self.multipart_uploads.remove(&upload_id).unwrap();

        let mut ordered_chunks: Vec<ChunkId> = Vec::new();
        let mut total_size: u64 = 0;

        for completed_part in &parts {
            let uploaded_part = upload.parts.get(&completed_part.part_number).unwrap();
            ordered_chunks.push(uploaded_part.chunk_id);
            total_size += uploaded_part.size;
        }

        // Create the final file
        let bucket_inode = upload.bucket_inode;
        let key = upload.key.clone();

        // Check if parent directory exists
        if !self.directories.contains_key(&bucket_inode) {
            return OpResult::Error {
                message: format!("Bucket directory not found: {}", bucket_inode),
            };
        }

        // Allocate new inode for the file
        let inode_id = self.allocate_inode_id();
        let mut inode = Inode::new_file(inode_id, 0o644, 0, 0);
        inode.size = total_size;
        inode.chunks = ordered_chunks;

        self.inodes.insert(inode_id, inode);

        // Add entry to parent directory (may replace existing)
        if let Some(parent_dir) = self.directories.get_mut(&bucket_inode) {
            // Remove existing entry if present
            if parent_dir.get_entry(&key).is_some() {
                if let Some(entry) = parent_dir.remove_entry(&key) {
                    // Decrement link count and potentially remove old inode
                    if let Some(old_inode) = self.inodes.get_mut(&entry.inode) {
                        old_inode.nlink = old_inode.nlink.saturating_sub(1);
                        if old_inode.nlink == 0 {
                            // Mark old chunks for GC (they remain in chunk_map)
                            warn!(inode = entry.inode, "Replaced file via multipart upload");
                        }
                    }
                    if self.inodes.get(&entry.inode).map_or(false, |i| i.nlink == 0) {
                        self.inodes.remove(&entry.inode);
                    }
                }
            }
            parent_dir.add_entry(key, inode_id, FileType::RegularFile);
        }

        // Update parent mtime
        if let Some(parent_inode) = self.inodes.get_mut(&bucket_inode) {
            parent_inode.touch();
        }

        debug!(
            upload_id = %upload_id,
            inode = inode_id,
            size = total_size,
            parts = parts.len(),
            "Completed multipart upload"
        );
        OpResult::Created { inode: inode_id }
    }

    fn abort_multipart_upload(&mut self, upload_id: String) -> OpResult {
        match self.multipart_uploads.remove(&upload_id) {
            Some(upload) => {
                // Mark chunks for garbage collection
                // Note: Actual chunk cleanup should be done by a background GC process
                let chunk_ids: Vec<ChunkId> = upload.parts.values().map(|p| p.chunk_id).collect();
                debug!(
                    upload_id = %upload_id,
                    chunks = ?chunk_ids,
                    "Aborted multipart upload, chunks marked for GC"
                );
                OpResult::Success
            }
            None => OpResult::Error {
                message: format!("Upload not found: {}", upload_id),
            },
        }
    }

    // Read-only query methods

    /// Look up an entry in a directory.
    pub fn lookup(&self, parent: InodeId, name: &str) -> Option<&Inode> {
        self.directories
            .get(&parent)
            .and_then(|dir| dir.get_entry(name))
            .and_then(|entry| self.inodes.get(&entry.inode))
    }

    /// Get an inode by ID.
    pub fn get_inode(&self, inode_id: InodeId) -> Option<&Inode> {
        self.inodes.get(&inode_id)
    }

    /// Get a directory by inode ID.
    pub fn get_directory(&self, inode_id: InodeId) -> Option<&Directory> {
        self.directories.get(&inode_id)
    }

    /// Get chunk metadata.
    pub fn get_chunk(&self, chunk_id: &ChunkId) -> Option<&ChunkMeta> {
        self.chunk_map.get(chunk_id)
    }

    /// Get all data servers.
    pub fn get_data_servers(&self) -> &HashMap<DataServerId, DataServerInfo> {
        &self.data_servers
    }

    /// Get online data servers.
    pub fn get_online_data_servers(&self) -> Vec<&DataServerInfo> {
        self.data_servers
            .values()
            .filter(|s| s.status == ServerStatus::Online)
            .collect()
    }

    /// Read directory entries.
    pub fn readdir(&self, inode_id: InodeId) -> Result<Vec<(String, InodeId, FileType)>> {
        let dir = self.directories.get(&inode_id).ok_or_else(|| {
            crate::error::StrataError::NotFound(format!("Directory {} not found", inode_id))
        })?;

        let entries: Vec<(String, InodeId, FileType)> = dir
            .entries
            .iter()
            .filter_map(|(name, entry)| {
                self.inodes.get(&entry.inode).map(|inode| {
                    (name.clone(), entry.inode, inode.file_type.clone())
                })
            })
            .collect();

        Ok(entries)
    }

    /// Get a multipart upload by ID.
    pub fn get_multipart_upload(&self, upload_id: &str) -> Option<&MultipartUpload> {
        self.multipart_uploads.get(upload_id)
    }

    /// List multipart uploads for a bucket.
    pub fn list_multipart_uploads(&self, bucket_inode: InodeId) -> Vec<&MultipartUpload> {
        self.multipart_uploads
            .values()
            .filter(|u| u.bucket_inode == bucket_inode)
            .collect()
    }

    /// List all multipart uploads.
    pub fn get_all_multipart_uploads(&self) -> &HashMap<String, MultipartUpload> {
        &self.multipart_uploads
    }
}

impl Default for MetadataStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachine for MetadataStateMachine {
    type Result = OpResult;

    fn apply(&mut self, command: &[u8]) -> Self::Result {
        match bincode::deserialize::<MetadataOp>(command) {
            Ok(op) => self.apply_op(op),  // Pass owned value
            Err(e) => {
                error!(error = %e, "Failed to deserialize metadata operation");
                OpResult::Error {
                    message: format!("Deserialization error: {}", e),
                }
            }
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        // Check if we have a valid cached snapshot
        if self.snapshot_valid.load(Ordering::Acquire) {
            let cache = self.snapshot_cache.lock();
            if let Some(ref cached) = *cache {
                return cached.clone();
            }
        }

        // Generate new snapshot
        let snapshot = MetadataSnapshot {
            inodes: self.inodes.clone(),
            directories: self.directories.clone(),
            chunk_map: self.chunk_map.clone(),
            data_servers: self.data_servers.clone(),
            multipart_uploads: self.multipart_uploads.clone(),
            next_inode_id: self.next_inode_id.load(Ordering::SeqCst),
        };

        let serialized = bincode::serialize(&snapshot).unwrap_or_default();

        // Cache the serialized snapshot
        {
            let mut cache = self.snapshot_cache.lock();
            *cache = Some(serialized.clone());
        }
        self.snapshot_valid.store(true, Ordering::Release);

        serialized
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
        let snapshot: MetadataSnapshot = bincode::deserialize(snapshot)?;

        self.inodes = snapshot.inodes;
        self.directories = snapshot.directories;
        self.chunk_map = snapshot.chunk_map;
        self.data_servers = snapshot.data_servers;
        self.multipart_uploads = snapshot.multipart_uploads;
        self.next_inode_id = AtomicU64::new(snapshot.next_inode_id);

        // Invalidate cache since state has changed
        self.invalidate_snapshot_cache();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_file() {
        let mut sm = MetadataStateMachine::new();

        let result = sm.apply_op(MetadataOp::CreateFile {
            parent: 1, // root
            name: "test.txt".to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });

        assert!(result.is_success());
        let inode_id = result.created_inode().unwrap();

        let inode = sm.get_inode(inode_id).unwrap();
        assert!(inode.is_file());
        assert_eq!(inode.uid, 1000);
    }

    #[test]
    fn test_create_directory() {
        let mut sm = MetadataStateMachine::new();

        let result = sm.apply_op(MetadataOp::CreateDirectory {
            parent: 1,
            name: "subdir".to_string(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        });

        assert!(result.is_success());
        let inode_id = result.created_inode().unwrap();

        let inode = sm.get_inode(inode_id).unwrap();
        assert!(inode.is_dir());

        // Check parent's link count increased
        let root = sm.get_inode(1).unwrap();
        assert_eq!(root.nlink, 3); // . and .. from root, plus .. from subdir
    }

    #[test]
    fn test_delete_file() {
        let mut sm = MetadataStateMachine::new();

        // Create file
        let result = sm.apply_op(MetadataOp::CreateFile {
            parent: 1,
            name: "test.txt".to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });
        let inode_id = result.created_inode().unwrap();

        // Delete file
        let result = sm.apply_op(MetadataOp::Delete {
            parent: 1,
            name: "test.txt".to_string(),
        });
        assert!(result.is_success());

        // Verify file is gone
        assert!(sm.get_inode(inode_id).is_none());
    }

    #[test]
    fn test_rename() {
        let mut sm = MetadataStateMachine::new();

        // Create file
        sm.apply_op(MetadataOp::CreateFile {
            parent: 1,
            name: "old.txt".to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });

        // Rename
        let result = sm.apply_op(MetadataOp::Rename {
            src_parent: 1,
            src_name: "old.txt".to_string(),
            dst_parent: 1,
            dst_name: "new.txt".to_string(),
        });
        assert!(result.is_success());

        // Verify
        assert!(sm.lookup(1, "old.txt").is_none());
        assert!(sm.lookup(1, "new.txt").is_some());
    }

    #[test]
    fn test_hard_link() {
        let mut sm = MetadataStateMachine::new();

        // Create file
        let result = sm.apply_op(MetadataOp::CreateFile {
            parent: 1,
            name: "original.txt".to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });
        let inode_id = result.created_inode().unwrap();

        // Create hard link
        let result = sm.apply_op(MetadataOp::Link {
            parent: 1,
            name: "link.txt".to_string(),
            inode: inode_id,
        });
        assert!(result.is_success());

        // Verify link count
        let inode = sm.get_inode(inode_id).unwrap();
        assert_eq!(inode.nlink, 2);
    }

    #[test]
    fn test_snapshot_restore() {
        let mut sm = MetadataStateMachine::new();

        // Create some structure
        sm.apply_op(MetadataOp::CreateFile {
            parent: 1,
            name: "file.txt".to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });

        sm.apply_op(MetadataOp::CreateDirectory {
            parent: 1,
            name: "dir".to_string(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        });

        // Take snapshot
        let snapshot = sm.snapshot();

        // Create new state machine and restore
        let mut sm2 = MetadataStateMachine::new();
        sm2.restore(&snapshot).unwrap();

        // Verify
        assert!(sm2.lookup(1, "file.txt").is_some());
        assert!(sm2.lookup(1, "dir").is_some());
    }

    #[test]
    fn test_multipart_upload_lifecycle() {
        let mut sm = MetadataStateMachine::new();

        // Create a bucket directory
        let bucket_result = sm.apply_op(MetadataOp::CreateDirectory {
            parent: 1,
            name: "mybucket".to_string(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        });
        let bucket_inode = bucket_result.created_inode().unwrap();

        // Initiate multipart upload
        let upload_id = "test-upload-123".to_string();
        let result = sm.apply_op(MetadataOp::InitiateMultipartUpload {
            bucket_inode,
            key: "largefile.bin".to_string(),
            upload_id: upload_id.clone(),
        });
        assert!(result.is_success());

        // Verify upload exists
        assert!(sm.get_multipart_upload(&upload_id).is_some());

        // Upload parts
        let chunk1 = ChunkId::new();
        let chunk2 = ChunkId::new();

        let result = sm.apply_op(MetadataOp::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 1,
            chunk_id: chunk1,
            size: 5 * 1024 * 1024, // 5MB
            etag: "\"etag1\"".to_string(),
        });
        assert!(result.is_success());

        let result = sm.apply_op(MetadataOp::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 2,
            chunk_id: chunk2,
            size: 3 * 1024 * 1024, // 3MB
            etag: "\"etag2\"".to_string(),
        });
        assert!(result.is_success());

        // Verify parts
        let upload = sm.get_multipart_upload(&upload_id).unwrap();
        assert_eq!(upload.parts.len(), 2);

        // Complete multipart upload
        let result = sm.apply_op(MetadataOp::CompleteMultipartUpload {
            upload_id: upload_id.clone(),
            parts: vec![
                super::super::operations::CompletedPart {
                    part_number: 1,
                    etag: "\"etag1\"".to_string(),
                },
                super::super::operations::CompletedPart {
                    part_number: 2,
                    etag: "\"etag2\"".to_string(),
                },
            ],
        });
        assert!(result.is_success());
        let file_inode = result.created_inode().unwrap();

        // Verify file was created
        let file = sm.get_inode(file_inode).unwrap();
        assert!(file.is_file());
        assert_eq!(file.size, 8 * 1024 * 1024); // 5MB + 3MB
        assert_eq!(file.chunks.len(), 2);

        // Verify upload was removed
        assert!(sm.get_multipart_upload(&upload_id).is_none());

        // Verify file is in bucket directory
        assert!(sm.lookup(bucket_inode, "largefile.bin").is_some());
    }

    #[test]
    fn test_multipart_upload_abort() {
        let mut sm = MetadataStateMachine::new();

        // Initiate multipart upload (using root as bucket for simplicity)
        let upload_id = "abort-test-456".to_string();
        let result = sm.apply_op(MetadataOp::InitiateMultipartUpload {
            bucket_inode: 1,
            key: "aborted.bin".to_string(),
            upload_id: upload_id.clone(),
        });
        assert!(result.is_success());

        // Upload a part
        let chunk = ChunkId::new();
        sm.apply_op(MetadataOp::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 1,
            chunk_id: chunk,
            size: 1024,
            etag: "\"etag\"".to_string(),
        });

        // Abort
        let result = sm.apply_op(MetadataOp::AbortMultipartUpload {
            upload_id: upload_id.clone(),
        });
        assert!(result.is_success());

        // Verify upload was removed
        assert!(sm.get_multipart_upload(&upload_id).is_none());

        // Verify no file was created
        assert!(sm.lookup(1, "aborted.bin").is_none());
    }

    #[test]
    fn test_multipart_upload_invalid_part_number() {
        let mut sm = MetadataStateMachine::new();

        let upload_id = "part-test".to_string();
        sm.apply_op(MetadataOp::InitiateMultipartUpload {
            bucket_inode: 1,
            key: "test.bin".to_string(),
            upload_id: upload_id.clone(),
        });

        // Part number 0 is invalid
        let result = sm.apply_op(MetadataOp::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 0,
            chunk_id: ChunkId::new(),
            size: 1024,
            etag: "\"etag\"".to_string(),
        });
        assert!(!result.is_success());

        // Part number > 10000 is invalid
        let result = sm.apply_op(MetadataOp::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 10001,
            chunk_id: ChunkId::new(),
            size: 1024,
            etag: "\"etag\"".to_string(),
        });
        assert!(!result.is_success());
    }
}
