//! Metadata operations that can be replicated through Raft.

use crate::types::{ChunkId, DataServerId, InodeId};
use serde::{Deserialize, Serialize};

/// Metadata operations for the Raft state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataOp {
    // File operations
    CreateFile {
        parent: InodeId,
        name: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateDirectory {
        parent: InodeId,
        name: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateSymlink {
        parent: InodeId,
        name: String,
        target: String,
        uid: u32,
        gid: u32,
    },
    Delete {
        parent: InodeId,
        name: String,
    },
    Rename {
        src_parent: InodeId,
        src_name: String,
        dst_parent: InodeId,
        dst_name: String,
    },
    Link {
        parent: InodeId,
        name: String,
        inode: InodeId,
    },

    // Attribute operations
    SetAttr {
        inode: InodeId,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<u64>,
        mtime: Option<u64>,
    },
    SetXattr {
        inode: InodeId,
        name: String,
        value: Vec<u8>,
    },
    RemoveXattr {
        inode: InodeId,
        name: String,
    },

    // Chunk operations
    AllocateChunks {
        inode: InodeId,
        chunks: Vec<ChunkId>,
    },
    AddChunk {
        inode: InodeId,
        chunk_id: ChunkId,
    },
    UpdateChunkLocations {
        chunk_id: ChunkId,
        locations: Vec<DataServerId>,
    },
    RemoveChunk {
        chunk_id: ChunkId,
    },

    // Data server operations
    RegisterDataServer {
        server_id: DataServerId,
        address: String,
        capacity: u64,
    },
    UpdateDataServerStatus {
        server_id: DataServerId,
        used: u64,
        online: bool,
    },
    DeregisterDataServer {
        server_id: DataServerId,
    },
}

/// Result of a metadata operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpResult {
    /// Operation created a new inode.
    Created { inode: InodeId },
    /// Operation completed successfully.
    Success,
    /// Operation failed with an error.
    Error { message: String },
}

impl OpResult {
    pub fn is_success(&self) -> bool {
        matches!(self, OpResult::Created { .. } | OpResult::Success)
    }

    pub fn created_inode(&self) -> Option<InodeId> {
        match self {
            OpResult::Created { inode } => Some(*inode),
            _ => None,
        }
    }
}
