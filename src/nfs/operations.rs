//! NFS operation implementations.
//!
//! This module implements the actual NFS operations that manipulate
//! the underlying Strata file system.

use super::compound::*;
use super::error::{NfsError, NfsStatus};
use super::filehandle::{FileHandle, FileHandleGenerator};
use super::session::{NfsSession, NfsSessionId, SessionManager};
use super::state::{ClientId, DelegationType, OpenStateFlags, StateManager, StateOwner};
use super::types::*;
use crate::client::{DataClient, MetadataClient};
use crate::types::{FileType, Inode, InodeId};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// NFS operations handler.
pub struct NfsOperations {
    /// Metadata client.
    metadata: MetadataClient,
    /// Data client.
    data: DataClient,
    /// Session manager.
    sessions: Arc<SessionManager>,
    /// State manager.
    state: Arc<StateManager>,
    /// File handle generator.
    filehandles: Arc<FileHandleGenerator>,
}

/// Current operation context.
pub struct OpContext {
    /// Current file handle.
    pub current_fh: Option<FileHandle>,
    /// Saved file handle.
    pub saved_fh: Option<FileHandle>,
    /// Current session (for NFSv4.1+).
    pub session: Option<Arc<NfsSession>>,
    /// Client ID.
    pub client_id: Option<ClientId>,
}

impl OpContext {
    /// Create a new operation context.
    pub fn new() -> Self {
        Self {
            current_fh: None,
            saved_fh: None,
            session: None,
            client_id: None,
        }
    }

    /// Get the current file handle or return an error.
    pub fn require_current_fh(&self) -> Result<&FileHandle, NfsError> {
        self.current_fh
            .as_ref()
            .ok_or(NfsError::protocol(NfsStatus::Badhandle))
    }

    /// Get the saved file handle or return an error.
    pub fn require_saved_fh(&self) -> Result<&FileHandle, NfsError> {
        self.saved_fh
            .as_ref()
            .ok_or(NfsError::protocol(NfsStatus::Badhandle))
    }
}

impl Default for OpContext {
    fn default() -> Self {
        Self::new()
    }
}

impl NfsOperations {
    /// Create new NFS operations handler.
    pub fn new(
        metadata: MetadataClient,
        data: DataClient,
        sessions: Arc<SessionManager>,
        state: Arc<StateManager>,
        filehandles: Arc<FileHandleGenerator>,
    ) -> Self {
        Self {
            metadata,
            data,
            sessions,
            state,
            filehandles,
        }
    }

    /// Process a COMPOUND request.
    pub async fn process_compound(&self, request: CompoundRequest) -> CompoundResult {
        let mut response = CompoundResponse::new(request.tag.clone());
        let mut ctx = OpContext::new();

        for op in request.operations {
            let result = self.process_operation(&mut ctx, op).await;

            // Add result to response
            response.add_result(result.clone());

            // Stop processing on error (except for some operations)
            if result.status != NfsStatus::Ok && !Self::continue_on_error(&result.op) {
                break;
            }
        }

        Ok(response)
    }

    /// Check if we should continue processing after an error.
    fn continue_on_error(op: &NfsOpNum4) -> bool {
        // Some operations should allow processing to continue
        matches!(op, NfsOpNum4::Illegal)
    }

    /// Process a single operation.
    async fn process_operation(&self, ctx: &mut OpContext, op: NfsArgOp4) -> NfsResOp4 {
        match op {
            NfsArgOp4::Access(access) => self.op_access(ctx, access).await,
            NfsArgOp4::Close { seqid, stateid } => self.op_close(ctx, seqid, stateid).await,
            NfsArgOp4::Commit { offset, count } => self.op_commit(ctx, offset, count).await,
            NfsArgOp4::Create {
                objtype,
                name,
                attrs,
            } => self.op_create(ctx, objtype, &name, attrs).await,
            NfsArgOp4::Getattr { attr_request } => self.op_getattr(ctx, attr_request).await,
            NfsArgOp4::Getfh => self.op_getfh(ctx).await,
            NfsArgOp4::Link { new_name } => self.op_link(ctx, &new_name).await,
            NfsArgOp4::Lookup { name } => self.op_lookup(ctx, &name).await,
            NfsArgOp4::Lookupp => self.op_lookupp(ctx).await,
            NfsArgOp4::Open {
                seqid,
                share_access,
                share_deny,
                owner,
                openhow,
                claim,
            } => {
                self.op_open(ctx, seqid, share_access, share_deny, owner, openhow, claim)
                    .await
            }
            NfsArgOp4::Putfh { fh } => self.op_putfh(ctx, fh).await,
            NfsArgOp4::Putpubfh => self.op_putpubfh(ctx).await,
            NfsArgOp4::Putrootfh => self.op_putrootfh(ctx).await,
            NfsArgOp4::Read {
                stateid,
                offset,
                count,
            } => self.op_read(ctx, stateid, offset, count).await,
            NfsArgOp4::Readdir {
                cookie,
                cookieverf,
                dircount,
                maxcount,
                attr_request,
            } => {
                self.op_readdir(ctx, cookie, cookieverf, dircount, maxcount, attr_request)
                    .await
            }
            NfsArgOp4::Readlink => self.op_readlink(ctx).await,
            NfsArgOp4::Remove { target } => self.op_remove(ctx, &target).await,
            NfsArgOp4::Rename { oldname, newname } => self.op_rename(ctx, &oldname, &newname).await,
            NfsArgOp4::Restorefh => self.op_restorefh(ctx).await,
            NfsArgOp4::Savefh => self.op_savefh(ctx).await,
            NfsArgOp4::Setattr { stateid, attrs } => self.op_setattr(ctx, stateid, attrs).await,
            NfsArgOp4::Write {
                stateid,
                offset,
                stable,
                data,
            } => self.op_write(ctx, stateid, offset, stable, data).await,
            NfsArgOp4::Sequence {
                session_id,
                sequence_id,
                slot_id,
                highest_slot_id,
                cachethis,
            } => {
                self.op_sequence(
                    ctx,
                    session_id,
                    sequence_id,
                    slot_id,
                    highest_slot_id,
                    cachethis,
                )
                .await
            }
            NfsArgOp4::ExchangeId {
                client_owner,
                flags,
                state_protect,
                impl_id,
            } => {
                self.op_exchange_id(ctx, client_owner, flags, state_protect, impl_id)
                    .await
            }
            NfsArgOp4::CreateSession {
                client_id,
                sequence,
                flags,
                fore_chan_attrs,
                back_chan_attrs,
                cb_program,
                sec_parms,
            } => {
                self.op_create_session(
                    ctx,
                    client_id,
                    sequence,
                    flags,
                    fore_chan_attrs,
                    back_chan_attrs,
                    cb_program,
                    sec_parms,
                )
                .await
            }
            NfsArgOp4::DestroySession { session_id } => {
                self.op_destroy_session(ctx, session_id).await
            }
            NfsArgOp4::ReclaimComplete { one_fs } => self.op_reclaim_complete(ctx, one_fs).await,
            NfsArgOp4::Illegal => NfsResOp4 {
                op: NfsOpNum4::Illegal,
                status: NfsStatus::Notsupp,
                result: OpResult::None,
            },
            _ => {
                // Unimplemented operations
                warn!("Unimplemented NFS operation");
                NfsResOp4 {
                    op: NfsOpNum4::Illegal,
                    status: NfsStatus::Notsupp,
                    result: OpResult::None,
                }
            }
        }
    }

    // === File handle operations ===

    async fn op_putfh(&self, ctx: &mut OpContext, fh: NfsFh4) -> NfsResOp4 {
        match FileHandle::decode(&fh.0) {
            Some(handle) if handle.is_valid() => {
                ctx.current_fh = Some(handle);
                NfsResOp4 {
                    op: NfsOpNum4::Putfh,
                    status: NfsStatus::Ok,
                    result: OpResult::None,
                }
            }
            _ => NfsResOp4 {
                op: NfsOpNum4::Putfh,
                status: NfsStatus::Badhandle,
                result: OpResult::None,
            },
        }
    }

    async fn op_putpubfh(&self, ctx: &mut OpContext) -> NfsResOp4 {
        // Public file handle is the root
        ctx.current_fh = Some(FileHandle::new(1, 0));
        NfsResOp4 {
            op: NfsOpNum4::Putpubfh,
            status: NfsStatus::Ok,
            result: OpResult::None,
        }
    }

    async fn op_putrootfh(&self, ctx: &mut OpContext) -> NfsResOp4 {
        ctx.current_fh = Some(FileHandle::new(1, 0));
        NfsResOp4 {
            op: NfsOpNum4::Putrootfh,
            status: NfsStatus::Ok,
            result: OpResult::None,
        }
    }

    async fn op_getfh(&self, ctx: &mut OpContext) -> NfsResOp4 {
        match &ctx.current_fh {
            Some(fh) => NfsResOp4 {
                op: NfsOpNum4::Getfh,
                status: NfsStatus::Ok,
                result: OpResult::Getfh {
                    fh: NfsFh4(fh.encode()),
                },
            },
            None => NfsResOp4 {
                op: NfsOpNum4::Getfh,
                status: NfsStatus::Badhandle,
                result: OpResult::None,
            },
        }
    }

    async fn op_savefh(&self, ctx: &mut OpContext) -> NfsResOp4 {
        ctx.saved_fh = ctx.current_fh.clone();
        NfsResOp4 {
            op: NfsOpNum4::Savefh,
            status: NfsStatus::Ok,
            result: OpResult::None,
        }
    }

    async fn op_restorefh(&self, ctx: &mut OpContext) -> NfsResOp4 {
        match &ctx.saved_fh {
            Some(fh) => {
                ctx.current_fh = Some(fh.clone());
                NfsResOp4 {
                    op: NfsOpNum4::Restorefh,
                    status: NfsStatus::Ok,
                    result: OpResult::None,
                }
            }
            None => NfsResOp4 {
                op: NfsOpNum4::Restorefh,
                status: NfsStatus::Badhandle,
                result: OpResult::None,
            },
        }
    }

    // === Attribute operations ===

    async fn op_getattr(&self, ctx: &mut OpContext, attr_request: AttrBitmap) -> NfsResOp4 {
        let fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Getattr,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let inode_id = fh.inode_id();

        // Get inode from metadata server
        match self.metadata.getattr(inode_id).await {
            Ok(Some(inode)) => {
                let attrs = self.build_attrs(&inode, &attr_request);
                NfsResOp4 {
                    op: NfsOpNum4::Getattr,
                    status: NfsStatus::Ok,
                    result: OpResult::Getattr { attrs },
                }
            }
            Ok(None) => NfsResOp4 {
                op: NfsOpNum4::Getattr,
                status: NfsStatus::Stale,
                result: OpResult::None,
            },
            Err(e) => {
                error!("Getattr failed: {}", e);
                NfsResOp4 {
                    op: NfsOpNum4::Getattr,
                    status: NfsStatus::Io,
                    result: OpResult::None,
                }
            }
        }
    }

    async fn op_setattr(
        &self,
        ctx: &mut OpContext,
        _stateid: Stateid4,
        _attrs: Attr4,
    ) -> NfsResOp4 {
        let _fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Setattr,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        // TODO: Implement actual attribute setting
        NfsResOp4 {
            op: NfsOpNum4::Setattr,
            status: NfsStatus::Ok,
            result: OpResult::Setattr {
                attrsset: AttrBitmap::new(),
            },
        }
    }

    async fn op_access(&self, ctx: &mut OpContext, requested: Access4) -> NfsResOp4 {
        let _fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Access,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        // For now, grant all requested access
        // TODO: Implement proper access control
        NfsResOp4 {
            op: NfsOpNum4::Access,
            status: NfsStatus::Ok,
            result: OpResult::Access {
                supported: requested,
                access: requested,
            },
        }
    }

    // === Directory operations ===

    async fn op_lookup(&self, ctx: &mut OpContext, name: &str) -> NfsResOp4 {
        let fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Lookup,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let parent_id = fh.inode_id();

        match self.metadata.lookup(parent_id, name).await {
            Ok(Some(inode)) => {
                let child_fh = self.filehandles.get_or_create(inode.id);
                ctx.current_fh = Some(child_fh);
                NfsResOp4 {
                    op: NfsOpNum4::Lookup,
                    status: NfsStatus::Ok,
                    result: OpResult::None,
                }
            }
            Ok(None) => NfsResOp4 {
                op: NfsOpNum4::Lookup,
                status: NfsStatus::Noent,
                result: OpResult::None,
            },
            Err(e) => {
                error!("Lookup failed: {}", e);
                NfsResOp4 {
                    op: NfsOpNum4::Lookup,
                    status: NfsStatus::Io,
                    result: OpResult::None,
                }
            }
        }
    }

    async fn op_lookupp(&self, ctx: &mut OpContext) -> NfsResOp4 {
        let fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Lookupp,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let inode_id = fh.inode_id();

        // LOOKUPP is used to go to parent directory
        // Since we don't track parent in Inode directly, we return NOTSUPP for now
        // A production implementation would maintain parent tracking in file handles
        // or use a different mechanism to track the directory hierarchy

        // For root directory (inode 1), return NOENT as there's no parent
        if inode_id == 1 {
            return NfsResOp4 {
                op: NfsOpNum4::Lookupp,
                status: NfsStatus::Noent,
                result: OpResult::None,
            };
        }

        // For other inodes, we don't have parent info, so return NOTSUPP
        NfsResOp4 {
            op: NfsOpNum4::Lookupp,
            status: NfsStatus::Notsupp,
            result: OpResult::None,
        }
    }

    async fn op_readdir(
        &self,
        ctx: &mut OpContext,
        _cookie: u64,
        _cookieverf: Verifier4,
        _dircount: u32,
        _maxcount: u32,
        _attr_request: AttrBitmap,
    ) -> NfsResOp4 {
        let fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Readdir,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let dir_id = fh.inode_id();

        match self.metadata.readdir(dir_id).await {
            Ok(response) => {
                let nfs_entries: Vec<DirEntry4> = response
                    .entries
                    .into_iter()
                    .enumerate()
                    .map(|(i, entry)| DirEntry4 {
                        cookie: (i + 1) as u64,
                        name: entry.name,
                        attrs: Attr4::default(),
                    })
                    .collect();

                NfsResOp4 {
                    op: NfsOpNum4::Readdir,
                    status: NfsStatus::Ok,
                    result: OpResult::Readdir {
                        cookieverf: Verifier4::random(),
                        entries: nfs_entries,
                        eof: true,
                    },
                }
            }
            Err(e) => {
                error!("Readdir failed: {}", e);
                NfsResOp4 {
                    op: NfsOpNum4::Readdir,
                    status: NfsStatus::Io,
                    result: OpResult::None,
                }
            }
        }
    }

    async fn op_create(
        &self,
        ctx: &mut OpContext,
        objtype: NfsFtype4,
        name: &str,
        _attrs: Attr4,
    ) -> NfsResOp4 {
        let fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Create,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let parent_id = fh.inode_id();

        // TODO: Extract mode and other attributes from _attrs
        let mode = 0o755;
        let uid = 0; // TODO: Get from request context
        let gid = 0;

        // Use different client methods based on type
        let result = match objtype {
            NfsFtype4::Dir => {
                self.metadata
                    .create_directory(parent_id, name, mode, uid, gid)
                    .await
            }
            NfsFtype4::Reg => {
                self.metadata
                    .create_file(parent_id, name, mode, uid, gid)
                    .await
            }
            NfsFtype4::Lnk => {
                // Symlinks not directly supported by create_file, return unsupported
                return NfsResOp4 {
                    op: NfsOpNum4::Create,
                    status: NfsStatus::Notsupp,
                    result: OpResult::None,
                };
            }
            _ => {
                return NfsResOp4 {
                    op: NfsOpNum4::Create,
                    status: NfsStatus::Notsupp,
                    result: OpResult::None,
                };
            }
        };

        match result {
            Ok(response) => {
                if response.success {
                    if let Some(inode_id) = response.inode {
                        let new_fh = self.filehandles.get_or_create(inode_id);
                        ctx.current_fh = Some(new_fh);

                        NfsResOp4 {
                            op: NfsOpNum4::Create,
                            status: NfsStatus::Ok,
                            result: OpResult::Create {
                                cinfo: ChangeInfo4::new(0, 1, true),
                                attrs: AttrBitmap::new(),
                            },
                        }
                    } else {
                        NfsResOp4 {
                            op: NfsOpNum4::Create,
                            status: NfsStatus::Serverfault,
                            result: OpResult::None,
                        }
                    }
                } else {
                    let status = if response
                        .error
                        .as_ref()
                        .map(|e| e.contains("exists"))
                        .unwrap_or(false)
                    {
                        NfsStatus::Exist
                    } else {
                        NfsStatus::Io
                    };
                    NfsResOp4 {
                        op: NfsOpNum4::Create,
                        status,
                        result: OpResult::None,
                    }
                }
            }
            Err(e) => {
                error!("Create failed: {}", e);
                NfsResOp4 {
                    op: NfsOpNum4::Create,
                    status: NfsStatus::Io,
                    result: OpResult::None,
                }
            }
        }
    }

    async fn op_remove(&self, ctx: &mut OpContext, name: &str) -> NfsResOp4 {
        let fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Remove,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let parent_id = fh.inode_id();

        match self.metadata.delete(parent_id, name).await {
            Ok(response) => {
                if response.success {
                    NfsResOp4 {
                        op: NfsOpNum4::Remove,
                        status: NfsStatus::Ok,
                        result: OpResult::Remove {
                            cinfo: ChangeInfo4::new(0, 1, true),
                        },
                    }
                } else {
                    let status = if response
                        .error
                        .as_ref()
                        .map(|e| e.contains("not found"))
                        .unwrap_or(false)
                    {
                        NfsStatus::Noent
                    } else if response
                        .error
                        .as_ref()
                        .map(|e| e.contains("not empty"))
                        .unwrap_or(false)
                    {
                        NfsStatus::Notempty
                    } else {
                        NfsStatus::Io
                    };
                    NfsResOp4 {
                        op: NfsOpNum4::Remove,
                        status,
                        result: OpResult::None,
                    }
                }
            }
            Err(e) => {
                error!("Remove failed: {}", e);
                NfsResOp4 {
                    op: NfsOpNum4::Remove,
                    status: NfsStatus::Io,
                    result: OpResult::None,
                }
            }
        }
    }

    async fn op_rename(&self, ctx: &mut OpContext, oldname: &str, newname: &str) -> NfsResOp4 {
        let saved_fh = match ctx.require_saved_fh() {
            Ok(fh) => fh.clone(),
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Rename,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let current_fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Rename,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let src_dir = saved_fh.inode_id();
        let dst_dir = current_fh.inode_id();

        match self
            .metadata
            .rename(src_dir, oldname, dst_dir, newname)
            .await
        {
            Ok(_) => NfsResOp4 {
                op: NfsOpNum4::Rename,
                status: NfsStatus::Ok,
                result: OpResult::Rename {
                    source_cinfo: ChangeInfo4::new(0, 1, true),
                    target_cinfo: ChangeInfo4::new(0, 1, true),
                },
            },
            Err(e) => {
                error!("Rename failed: {}", e);
                NfsResOp4 {
                    op: NfsOpNum4::Rename,
                    status: NfsStatus::Io,
                    result: OpResult::None,
                }
            }
        }
    }

    async fn op_link(&self, ctx: &mut OpContext, new_name: &str) -> NfsResOp4 {
        let saved_fh = match ctx.require_saved_fh() {
            Ok(fh) => fh.clone(),
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Link,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let current_fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Link,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        // Hard links are not currently supported by the metadata client
        // Return NOTSUPP to indicate this operation is not available
        NfsResOp4 {
            op: NfsOpNum4::Link,
            status: NfsStatus::Notsupp,
            result: OpResult::None,
        }
    }

    async fn op_readlink(&self, ctx: &mut OpContext) -> NfsResOp4 {
        let fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Readlink,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let inode_id = fh.inode_id();

        // Get inode and read symlink_target field
        match self.metadata.getattr(inode_id).await {
            Ok(Some(inode)) => {
                if inode.file_type != FileType::Symlink {
                    return NfsResOp4 {
                        op: NfsOpNum4::Readlink,
                        status: NfsStatus::Inval,
                        result: OpResult::None,
                    };
                }

                if let Some(target) = inode.symlink_target {
                    NfsResOp4 {
                        op: NfsOpNum4::Readlink,
                        status: NfsStatus::Ok,
                        result: OpResult::Readlink { link: target },
                    }
                } else {
                    NfsResOp4 {
                        op: NfsOpNum4::Readlink,
                        status: NfsStatus::Inval,
                        result: OpResult::None,
                    }
                }
            }
            Ok(None) => NfsResOp4 {
                op: NfsOpNum4::Readlink,
                status: NfsStatus::Stale,
                result: OpResult::None,
            },
            Err(e) => {
                error!("Readlink failed: {}", e);
                NfsResOp4 {
                    op: NfsOpNum4::Readlink,
                    status: NfsStatus::Io,
                    result: OpResult::None,
                }
            }
        }
    }

    // === File I/O operations ===

    async fn op_open(
        &self,
        ctx: &mut OpContext,
        _seqid: u32,
        share_access: ShareAccess4,
        share_deny: ShareDeny4,
        owner: OpenOwner4,
        _openhow: OpenFlags4,
        claim: OpenClaim4,
    ) -> NfsResOp4 {
        let name = match &claim {
            OpenClaim4::Null(name) => name.clone(),
            OpenClaim4::Fh => String::new(),
            _ => {
                return NfsResOp4 {
                    op: NfsOpNum4::Open,
                    status: NfsStatus::Notsupp,
                    result: OpResult::None,
                }
            }
        };

        let fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Open,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let parent_id = fh.inode_id();

        // Look up or create the file
        let inode_result = if name.is_empty() {
            self.metadata.getattr(parent_id).await
        } else {
            self.metadata.lookup(parent_id, &name).await
        };

        match inode_result {
            Ok(Some(inode)) => {
                let file_fh = self.filehandles.get_or_create(inode.id);
                ctx.current_fh = Some(file_fh);

                // Create open state
                let flags = OpenStateFlags {
                    read: share_access.read(),
                    write: share_access.write(),
                    deny_read: share_deny.0 & ShareDeny4::READ != 0,
                    deny_write: share_deny.0 & ShareDeny4::WRITE != 0,
                };

                let state_owner = StateOwner::new(
                    ClientId(owner.client_id),
                    owner.owner.clone(),
                );

                match self.state.open(state_owner, inode.id, flags) {
                    Ok(open_state) => {
                        let stateid = Stateid4::new(
                            open_state.state_id.seqid,
                            open_state.state_id.other,
                        );

                        NfsResOp4 {
                            op: NfsOpNum4::Open,
                            status: NfsStatus::Ok,
                            result: OpResult::Open {
                                stateid,
                                cinfo: ChangeInfo4::new(0, 1, true),
                                rflags: 0,
                                attrset: AttrBitmap::new(),
                                delegation: OpenDelegation4::None,
                            },
                        }
                    }
                    Err(_) => NfsResOp4 {
                        op: NfsOpNum4::Open,
                        status: NfsStatus::ShareDenied,
                        result: OpResult::None,
                    },
                }
            }
            Ok(None) => NfsResOp4 {
                op: NfsOpNum4::Open,
                status: NfsStatus::Noent,
                result: OpResult::None,
            },
            Err(e) => {
                error!("Open failed: {}", e);
                NfsResOp4 {
                    op: NfsOpNum4::Open,
                    status: NfsStatus::Io,
                    result: OpResult::None,
                }
            }
        }
    }

    async fn op_close(&self, ctx: &mut OpContext, _seqid: u32, stateid: Stateid4) -> NfsResOp4 {
        let state_id = super::state::StateId::new(
            ((stateid.other[0] as u64) << 56)
                | ((stateid.other[1] as u64) << 48)
                | ((stateid.other[2] as u64) << 40)
                | ((stateid.other[3] as u64) << 32)
                | ((stateid.other[4] as u64) << 24)
                | ((stateid.other[5] as u64) << 16)
                | ((stateid.other[6] as u64) << 8)
                | (stateid.other[7] as u64),
        );

        if self.state.close(&state_id) {
            let mut new_stateid = stateid;
            new_stateid.seqid = new_stateid.seqid.wrapping_add(1);
            NfsResOp4 {
                op: NfsOpNum4::Close,
                status: NfsStatus::Ok,
                result: OpResult::Close {
                    stateid: new_stateid,
                },
            }
        } else {
            NfsResOp4 {
                op: NfsOpNum4::Close,
                status: NfsStatus::BadStateid,
                result: OpResult::None,
            }
        }
    }

    async fn op_read(
        &self,
        ctx: &mut OpContext,
        _stateid: Stateid4,
        offset: u64,
        count: u32,
    ) -> NfsResOp4 {
        let fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Read,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let inode_id = fh.inode_id();

        // Get the file's chunk list and read from appropriate chunks
        // For now, we implement a simplified version that returns empty for offset > 0
        // A full implementation would:
        // 1. Get inode to find chunk list
        // 2. Calculate which chunk(s) to read based on offset
        // 3. Read chunk data and return appropriate portion

        match self.metadata.getattr(inode_id).await {
            Ok(Some(inode)) => {
                if inode.file_type != FileType::RegularFile {
                    return NfsResOp4 {
                        op: NfsOpNum4::Read,
                        status: NfsStatus::Isdir,
                        result: OpResult::None,
                    };
                }

                // Calculate what we can return
                let file_size = inode.size;
                if offset >= file_size {
                    // Reading past end of file
                    return NfsResOp4 {
                        op: NfsOpNum4::Read,
                        status: NfsStatus::Ok,
                        result: OpResult::Read {
                            eof: true,
                            data: vec![],
                        },
                    };
                }

                // For now, return empty data indicating we need chunk-based read
                // A full implementation would read from chunks
                let bytes_available = (file_size - offset) as usize;
                let to_read = std::cmp::min(count as usize, bytes_available);

                // Read from chunks
                let mut data = Vec::with_capacity(to_read);
                let mut current_offset = offset;

                // Each chunk is up to 64MB by default
                let chunk_size: u64 = 64 * 1024 * 1024;

                for chunk_id in &inode.chunks {
                    let chunk_start = (data.len() as u64 + offset) / chunk_size * chunk_size;
                    let chunk_offset = current_offset - chunk_start;

                    if data.len() >= to_read {
                        break;
                    }

                    let bytes_needed = to_read - data.len();
                    match self.data.read_chunk(*chunk_id, bytes_needed).await {
                        Ok(chunk_data) => {
                            let start = chunk_offset as usize;
                            let end = std::cmp::min(start + bytes_needed, chunk_data.len());
                            if start < chunk_data.len() {
                                data.extend_from_slice(&chunk_data[start..end]);
                            }
                            current_offset += (end - start) as u64;
                        }
                        Err(e) => {
                            warn!("Failed to read chunk {}: {}", chunk_id, e);
                            // Continue with partial data
                            break;
                        }
                    }
                }

                let eof = offset + data.len() as u64 >= file_size;
                NfsResOp4 {
                    op: NfsOpNum4::Read,
                    status: NfsStatus::Ok,
                    result: OpResult::Read { eof, data },
                }
            }
            Ok(None) => NfsResOp4 {
                op: NfsOpNum4::Read,
                status: NfsStatus::Stale,
                result: OpResult::None,
            },
            Err(e) => {
                error!("Read failed: {}", e);
                NfsResOp4 {
                    op: NfsOpNum4::Read,
                    status: NfsStatus::Io,
                    result: OpResult::None,
                }
            }
        }
    }

    async fn op_write(
        &self,
        ctx: &mut OpContext,
        _stateid: Stateid4,
        offset: u64,
        stable: u32,
        data: Vec<u8>,
    ) -> NfsResOp4 {
        let fh = match ctx.require_current_fh() {
            Ok(fh) => fh,
            Err(_) => {
                return NfsResOp4 {
                    op: NfsOpNum4::Write,
                    status: NfsStatus::Badhandle,
                    result: OpResult::None,
                }
            }
        };

        let inode_id = fh.inode_id();
        let data_len = data.len();

        // Get the file's current state
        match self.metadata.getattr(inode_id).await {
            Ok(Some(inode)) => {
                if inode.file_type != FileType::RegularFile {
                    return NfsResOp4 {
                        op: NfsOpNum4::Write,
                        status: NfsStatus::Isdir,
                        result: OpResult::None,
                    };
                }

                // Write data to a new chunk
                let chunk_id = crate::types::ChunkId::new();
                match self.data.write_chunk(chunk_id, &data).await {
                    Ok(response) => {
                        if response.success {
                            // Add chunk to inode
                            if let Err(e) = self.metadata.add_chunk(inode_id, chunk_id).await {
                                error!("Failed to add chunk to inode: {}", e);
                                return NfsResOp4 {
                                    op: NfsOpNum4::Write,
                                    status: NfsStatus::Io,
                                    result: OpResult::None,
                                };
                            }

                            // Update file size if needed
                            let new_size = offset + data_len as u64;
                            if new_size > inode.size {
                                if let Err(e) = self.metadata.set_size(inode_id, new_size).await {
                                    warn!("Failed to update file size: {}", e);
                                }
                            }

                            NfsResOp4 {
                                op: NfsOpNum4::Write,
                                status: NfsStatus::Ok,
                                result: OpResult::Write {
                                    count: data_len as u32,
                                    committed: stable,
                                    verifier: Verifier4::random(),
                                },
                            }
                        } else {
                            NfsResOp4 {
                                op: NfsOpNum4::Write,
                                status: NfsStatus::Nospc,
                                result: OpResult::None,
                            }
                        }
                    }
                    Err(e) => {
                        error!("Write failed: {}", e);
                        NfsResOp4 {
                            op: NfsOpNum4::Write,
                            status: NfsStatus::Io,
                            result: OpResult::None,
                        }
                    }
                }
            }
            Ok(None) => NfsResOp4 {
                op: NfsOpNum4::Write,
                status: NfsStatus::Stale,
                result: OpResult::None,
            },
            Err(e) => {
                error!("Write failed: {}", e);
                NfsResOp4 {
                    op: NfsOpNum4::Write,
                    status: NfsStatus::Io,
                    result: OpResult::None,
                }
            }
        }
    }

    async fn op_commit(&self, ctx: &mut OpContext, _offset: u64, _count: u32) -> NfsResOp4 {
        // For now, we treat all writes as synchronous
        NfsResOp4 {
            op: NfsOpNum4::Commit,
            status: NfsStatus::Ok,
            result: OpResult::Commit {
                verifier: Verifier4::random(),
            },
        }
    }

    // === Session operations (NFSv4.1) ===

    async fn op_exchange_id(
        &self,
        ctx: &mut OpContext,
        client_owner: ClientOwner4,
        flags: u32,
        state_protect: StateProtect4,
        _impl_id: Vec<NfsImplId4>,
    ) -> NfsResOp4 {
        // Generate client ID
        let client_id = {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            client_owner.co_ownerid.hash(&mut hasher);
            client_owner.co_verifier.0.hash(&mut hasher);
            hasher.finish()
        };

        ctx.client_id = Some(ClientId(client_id));

        NfsResOp4 {
            op: NfsOpNum4::ExchangeId,
            status: NfsStatus::Ok,
            result: OpResult::ExchangeId {
                client_id,
                sequence_id: 1,
                flags,
                state_protect,
                server_owner: ServerOwner4 {
                    so_minor_id: 0,
                    so_major_id: b"strata".to_vec(),
                },
                server_scope: b"strata.local".to_vec(),
                server_impl_id: vec![NfsImplId4 {
                    nii_domain: "strata.io".to_string(),
                    nii_name: "Strata NFS Gateway".to_string(),
                    nii_date: NfsTime4::now(),
                }],
            },
        }
    }

    async fn op_create_session(
        &self,
        ctx: &mut OpContext,
        client_id: u64,
        sequence: u32,
        flags: u32,
        fore_chan_attrs: ChannelAttrs4,
        back_chan_attrs: ChannelAttrs4,
        _cb_program: u32,
        _sec_parms: Vec<CallbackSecParms4>,
    ) -> NfsResOp4 {
        match self.sessions.create_session(client_id) {
            Ok(session) => {
                ctx.session = Some(session.clone());

                NfsResOp4 {
                    op: NfsOpNum4::CreateSession,
                    status: NfsStatus::Ok,
                    result: OpResult::CreateSession {
                        session_id: session.id,
                        sequence_id: sequence,
                        flags,
                        fore_chan_attrs,
                        back_chan_attrs,
                    },
                }
            }
            Err(_) => NfsResOp4 {
                op: NfsOpNum4::CreateSession,
                status: NfsStatus::Resource,
                result: OpResult::None,
            },
        }
    }

    async fn op_destroy_session(
        &self,
        _ctx: &mut OpContext,
        session_id: NfsSessionId,
    ) -> NfsResOp4 {
        if self.sessions.destroy_session(&session_id) {
            NfsResOp4 {
                op: NfsOpNum4::DestroySession,
                status: NfsStatus::Ok,
                result: OpResult::None,
            }
        } else {
            NfsResOp4 {
                op: NfsOpNum4::DestroySession,
                status: NfsStatus::BadSession,
                result: OpResult::None,
            }
        }
    }

    async fn op_sequence(
        &self,
        ctx: &mut OpContext,
        session_id: NfsSessionId,
        sequence_id: u32,
        slot_id: u32,
        highest_slot_id: u32,
        _cachethis: bool,
    ) -> NfsResOp4 {
        let session = match self.sessions.get_session(&session_id) {
            Some(s) => s,
            None => {
                return NfsResOp4 {
                    op: NfsOpNum4::Sequence,
                    status: NfsStatus::BadSession,
                    result: OpResult::None,
                }
            }
        };

        // Get or create slot
        let slot = match session.get_or_create_slot(slot_id) {
            Some(s) => s,
            None => {
                return NfsResOp4 {
                    op: NfsOpNum4::Sequence,
                    status: NfsStatus::BadSlot,
                    result: OpResult::None,
                }
            }
        };

        ctx.session = Some(session.clone());
        ctx.client_id = Some(ClientId(session.client_id));

        NfsResOp4 {
            op: NfsOpNum4::Sequence,
            status: NfsStatus::Ok,
            result: OpResult::Sequence {
                session_id,
                sequence_id,
                slot_id,
                highest_slot_id,
                target_highest_slot_id: highest_slot_id,
                status_flags: 0,
            },
        }
    }

    async fn op_reclaim_complete(&self, _ctx: &mut OpContext, _one_fs: bool) -> NfsResOp4 {
        NfsResOp4 {
            op: NfsOpNum4::ReclaimComplete,
            status: NfsStatus::Ok,
            result: OpResult::None,
        }
    }

    // === Helper methods ===

    fn build_attrs(&self, inode: &Inode, requested: &AttrBitmap) -> Attr4 {
        use super::types::attr_bits;

        let mut attrmask = AttrBitmap::new();
        let mut codec = super::protocol::XdrCodec::new();

        // TYPE
        if requested.is_set(attr_bits::TYPE) {
            attrmask.set(attr_bits::TYPE);
            let ftype = match inode.file_type {
                FileType::RegularFile => NfsFtype4::Reg,
                FileType::Directory => NfsFtype4::Dir,
                FileType::Symlink => NfsFtype4::Lnk,
            };
            codec.encode_u32(ftype as u32);
        }

        // SIZE
        if requested.is_set(attr_bits::SIZE) {
            attrmask.set(attr_bits::SIZE);
            codec.encode_u64(inode.size);
        }

        // FILEID
        if requested.is_set(attr_bits::FILEID) {
            attrmask.set(attr_bits::FILEID);
            codec.encode_u64(inode.id);
        }

        // MODE
        if requested.is_set(attr_bits::MODE) {
            attrmask.set(attr_bits::MODE);
            codec.encode_u32(inode.mode);
        }

        // NUMLINKS
        if requested.is_set(attr_bits::NUMLINKS) {
            attrmask.set(attr_bits::NUMLINKS);
            codec.encode_u32(inode.nlink as u32);
        }

        // OWNER (as string)
        if requested.is_set(attr_bits::OWNER) {
            attrmask.set(attr_bits::OWNER);
            codec.encode_string(&inode.uid.to_string());
        }

        // OWNER_GROUP (as string)
        if requested.is_set(attr_bits::OWNER_GROUP) {
            attrmask.set(attr_bits::OWNER_GROUP);
            codec.encode_string(&inode.gid.to_string());
        }

        // TIME_ACCESS
        if requested.is_set(attr_bits::TIME_ACCESS) {
            attrmask.set(attr_bits::TIME_ACCESS);
            let atime = NfsTime4::from_system_time(inode.atime);
            codec.encode_i64(atime.seconds);
            codec.encode_u32(atime.nseconds);
        }

        // TIME_MODIFY
        if requested.is_set(attr_bits::TIME_MODIFY) {
            attrmask.set(attr_bits::TIME_MODIFY);
            let mtime = NfsTime4::from_system_time(inode.mtime);
            codec.encode_i64(mtime.seconds);
            codec.encode_u32(mtime.nseconds);
        }

        // TIME_METADATA (ctime)
        if requested.is_set(attr_bits::TIME_METADATA) {
            attrmask.set(attr_bits::TIME_METADATA);
            let ctime = NfsTime4::from_system_time(inode.ctime);
            codec.encode_i64(ctime.seconds);
            codec.encode_u32(ctime.nseconds);
        }

        Attr4 {
            attrmask,
            attr_vals: codec.into_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_op_context() {
        let ctx = OpContext::new();
        assert!(ctx.current_fh.is_none());
        assert!(ctx.saved_fh.is_none());
        assert!(ctx.require_current_fh().is_err());
    }
}
