//! Metadata state machine for Raft replication.

use super::operations::{MetadataOp, OpResult};
use crate::error::Result;
use crate::raft::StateMachine;
use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use tracing::{debug, error};

/// Snapshot of the metadata state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    pub inodes: BTreeMap<InodeId, Inode>,
    pub directories: BTreeMap<InodeId, Directory>,
    pub chunk_map: BTreeMap<ChunkId, ChunkMeta>,
    pub data_servers: HashMap<DataServerId, DataServerInfo>,
    pub next_inode_id: u64,
}

/// The metadata state machine, storing all file system metadata.
pub struct MetadataStateMachine {
    /// All inodes in the system.
    inodes: BTreeMap<InodeId, Inode>,
    /// Directory contents.
    directories: BTreeMap<InodeId, Directory>,
    /// Chunk to data server mapping.
    chunk_map: BTreeMap<ChunkId, ChunkMeta>,
    /// Data server information.
    data_servers: HashMap<DataServerId, DataServerInfo>,
    /// Next inode ID to allocate.
    next_inode_id: AtomicU64,
}

impl MetadataStateMachine {
    /// Create a new metadata state machine with a root directory.
    pub fn new() -> Self {
        let mut sm = Self {
            inodes: BTreeMap::new(),
            directories: BTreeMap::new(),
            chunk_map: BTreeMap::new(),
            data_servers: HashMap::new(),
            next_inode_id: AtomicU64::new(2), // 1 is reserved for root
        };

        // Create root directory (inode 1)
        let root_inode = Inode::new_directory(1, 0o755, 0, 0);
        sm.inodes.insert(1, root_inode);
        sm.directories.insert(1, Directory::new(1, 1)); // Root's parent is itself

        sm
    }

    /// Allocate a new inode ID.
    fn allocate_inode_id(&self) -> InodeId {
        self.next_inode_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Apply a metadata operation.
    pub fn apply_op(&mut self, op: &MetadataOp) -> OpResult {
        match op {
            MetadataOp::CreateFile { parent, name, mode, uid, gid } => {
                self.create_file(*parent, name.clone(), *mode, *uid, *gid)
            }
            MetadataOp::CreateDirectory { parent, name, mode, uid, gid } => {
                self.create_directory(*parent, name.clone(), *mode, *uid, *gid)
            }
            MetadataOp::CreateSymlink { parent, name, target, uid, gid } => {
                self.create_symlink(*parent, name.clone(), target.clone(), *uid, *gid)
            }
            MetadataOp::Delete { parent, name } => {
                self.delete(*parent, name.clone())
            }
            MetadataOp::Rename { src_parent, src_name, dst_parent, dst_name } => {
                self.rename(*src_parent, src_name.clone(), *dst_parent, dst_name.clone())
            }
            MetadataOp::Link { parent, name, inode } => {
                self.link(*parent, name.clone(), *inode)
            }
            MetadataOp::SetAttr { inode, mode, uid, gid, size, atime, mtime } => {
                self.set_attr(*inode, *mode, *uid, *gid, *size, *atime, *mtime)
            }
            MetadataOp::SetXattr { inode, name, value } => {
                self.set_xattr(*inode, name.clone(), value.clone())
            }
            MetadataOp::RemoveXattr { inode, name } => {
                self.remove_xattr(*inode, name.clone())
            }
            MetadataOp::AllocateChunks { inode, chunks } => {
                self.allocate_chunks(*inode, chunks.clone())
            }
            MetadataOp::AddChunk { inode, chunk_id } => {
                self.add_chunk(*inode, *chunk_id)
            }
            MetadataOp::UpdateChunkLocations { chunk_id, locations } => {
                self.update_chunk_locations(*chunk_id, locations.clone())
            }
            MetadataOp::RemoveChunk { chunk_id } => {
                self.remove_chunk(*chunk_id)
            }
            MetadataOp::RegisterDataServer { server_id, address, capacity } => {
                self.register_data_server(*server_id, address.clone(), *capacity)
            }
            MetadataOp::UpdateDataServerStatus { server_id, used, online } => {
                self.update_data_server_status(*server_id, *used, *online)
            }
            MetadataOp::DeregisterDataServer { server_id } => {
                self.deregister_data_server(*server_id)
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
            Ok(op) => self.apply_op(&op),
            Err(e) => {
                error!(error = %e, "Failed to deserialize metadata operation");
                OpResult::Error {
                    message: format!("Deserialization error: {}", e),
                }
            }
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let snapshot = MetadataSnapshot {
            inodes: self.inodes.clone(),
            directories: self.directories.clone(),
            chunk_map: self.chunk_map.clone(),
            data_servers: self.data_servers.clone(),
            next_inode_id: self.next_inode_id.load(Ordering::SeqCst),
        };

        bincode::serialize(&snapshot).unwrap_or_default()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<()> {
        let snapshot: MetadataSnapshot = bincode::deserialize(snapshot)?;

        self.inodes = snapshot.inodes;
        self.directories = snapshot.directories;
        self.chunk_map = snapshot.chunk_map;
        self.data_servers = snapshot.data_servers;
        self.next_inode_id = AtomicU64::new(snapshot.next_inode_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_file() {
        let mut sm = MetadataStateMachine::new();

        let result = sm.apply_op(&MetadataOp::CreateFile {
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

        let result = sm.apply_op(&MetadataOp::CreateDirectory {
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
        let result = sm.apply_op(&MetadataOp::CreateFile {
            parent: 1,
            name: "test.txt".to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });
        let inode_id = result.created_inode().unwrap();

        // Delete file
        let result = sm.apply_op(&MetadataOp::Delete {
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
        sm.apply_op(&MetadataOp::CreateFile {
            parent: 1,
            name: "old.txt".to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });

        // Rename
        let result = sm.apply_op(&MetadataOp::Rename {
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
        let result = sm.apply_op(&MetadataOp::CreateFile {
            parent: 1,
            name: "original.txt".to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });
        let inode_id = result.created_inode().unwrap();

        // Create hard link
        let result = sm.apply_op(&MetadataOp::Link {
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
        sm.apply_op(&MetadataOp::CreateFile {
            parent: 1,
            name: "file.txt".to_string(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        });

        sm.apply_op(&MetadataOp::CreateDirectory {
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
}
