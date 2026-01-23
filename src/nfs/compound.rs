//! NFSv4 COMPOUND procedure handling.
//!
//! COMPOUND is the main NFSv4 procedure that allows multiple operations
//! to be batched into a single RPC call for efficiency.

use super::error::{NfsError, NfsStatus};
use super::protocol::XdrCodec;
use super::session::NfsSessionId;
use super::types::*;
use serde::{Deserialize, Serialize};
use std::io;

/// NFSv4 operation codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum NfsOpNum4 {
    Access = 3,
    Close = 4,
    Commit = 5,
    Create = 6,
    DelegPurge = 7,
    DelegReturn = 8,
    Getattr = 9,
    Getfh = 10,
    Link = 11,
    Lock = 12,
    Lockt = 13,
    Locku = 14,
    Lookup = 15,
    Lookupp = 16,
    Nverify = 17,
    Open = 18,
    OpenAttr = 19,
    OpenConfirm = 20,
    OpenDowngrade = 21,
    Putfh = 22,
    Putpubfh = 23,
    Putrootfh = 24,
    Read = 25,
    Readdir = 26,
    Readlink = 27,
    Remove = 28,
    Rename = 29,
    Renew = 30,
    Restorefh = 31,
    Savefh = 32,
    Secinfo = 33,
    Setattr = 34,
    SetClientId = 35,
    SetClientIdConfirm = 36,
    Verify = 37,
    Write = 38,
    ReleaseLockowner = 39,
    // NFSv4.1 operations
    BackchannelCtl = 40,
    BindConnToSession = 41,
    ExchangeId = 42,
    CreateSession = 43,
    DestroySession = 44,
    FreeStateid = 45,
    GetDirDelegation = 46,
    Getdeviceinfo = 47,
    Getdevicelist = 48,
    Layoutcommit = 49,
    Layoutget = 50,
    Layoutreturn = 51,
    SecinfoNoName = 52,
    Sequence = 53,
    SetSsv = 54,
    TestStateid = 55,
    WantDelegation = 56,
    DestroyClientId = 57,
    ReclaimComplete = 58,
    // NFSv4.2 operations
    Allocate = 59,
    Copy = 60,
    CopyNotify = 61,
    Deallocate = 62,
    IoAdvise = 63,
    Layouterror = 64,
    Layoutstats = 65,
    OffloadCancel = 66,
    OffloadStatus = 67,
    ReadPlus = 68,
    Seek = 69,
    WriteSame = 70,
    Clone = 71,
    /// Illegal operation.
    Illegal = 10044,
}

impl NfsOpNum4 {
    /// Convert from u32.
    pub fn from_u32(val: u32) -> Option<Self> {
        match val {
            3 => Some(Self::Access),
            4 => Some(Self::Close),
            5 => Some(Self::Commit),
            6 => Some(Self::Create),
            7 => Some(Self::DelegPurge),
            8 => Some(Self::DelegReturn),
            9 => Some(Self::Getattr),
            10 => Some(Self::Getfh),
            11 => Some(Self::Link),
            12 => Some(Self::Lock),
            13 => Some(Self::Lockt),
            14 => Some(Self::Locku),
            15 => Some(Self::Lookup),
            16 => Some(Self::Lookupp),
            17 => Some(Self::Nverify),
            18 => Some(Self::Open),
            19 => Some(Self::OpenAttr),
            20 => Some(Self::OpenConfirm),
            21 => Some(Self::OpenDowngrade),
            22 => Some(Self::Putfh),
            23 => Some(Self::Putpubfh),
            24 => Some(Self::Putrootfh),
            25 => Some(Self::Read),
            26 => Some(Self::Readdir),
            27 => Some(Self::Readlink),
            28 => Some(Self::Remove),
            29 => Some(Self::Rename),
            30 => Some(Self::Renew),
            31 => Some(Self::Restorefh),
            32 => Some(Self::Savefh),
            33 => Some(Self::Secinfo),
            34 => Some(Self::Setattr),
            35 => Some(Self::SetClientId),
            36 => Some(Self::SetClientIdConfirm),
            37 => Some(Self::Verify),
            38 => Some(Self::Write),
            39 => Some(Self::ReleaseLockowner),
            40 => Some(Self::BackchannelCtl),
            41 => Some(Self::BindConnToSession),
            42 => Some(Self::ExchangeId),
            43 => Some(Self::CreateSession),
            44 => Some(Self::DestroySession),
            45 => Some(Self::FreeStateid),
            46 => Some(Self::GetDirDelegation),
            47 => Some(Self::Getdeviceinfo),
            48 => Some(Self::Getdevicelist),
            49 => Some(Self::Layoutcommit),
            50 => Some(Self::Layoutget),
            51 => Some(Self::Layoutreturn),
            52 => Some(Self::SecinfoNoName),
            53 => Some(Self::Sequence),
            54 => Some(Self::SetSsv),
            55 => Some(Self::TestStateid),
            56 => Some(Self::WantDelegation),
            57 => Some(Self::DestroyClientId),
            58 => Some(Self::ReclaimComplete),
            59 => Some(Self::Allocate),
            60 => Some(Self::Copy),
            61 => Some(Self::CopyNotify),
            62 => Some(Self::Deallocate),
            63 => Some(Self::IoAdvise),
            64 => Some(Self::Layouterror),
            65 => Some(Self::Layoutstats),
            66 => Some(Self::OffloadCancel),
            67 => Some(Self::OffloadStatus),
            68 => Some(Self::ReadPlus),
            69 => Some(Self::Seek),
            70 => Some(Self::WriteSame),
            71 => Some(Self::Clone),
            10044 => Some(Self::Illegal),
            _ => None,
        }
    }
}

/// Individual operation within a COMPOUND request.
#[derive(Debug, Clone)]
pub enum NfsArgOp4 {
    /// ACCESS - Check access permissions.
    Access(Access4),
    /// CLOSE - Close a file.
    Close { seqid: u32, stateid: Stateid4 },
    /// COMMIT - Commit cached writes.
    Commit { offset: u64, count: u32 },
    /// CREATE - Create a file/directory.
    Create {
        objtype: NfsFtype4,
        name: String,
        attrs: Attr4,
    },
    /// GETATTR - Get file attributes.
    Getattr { attr_request: AttrBitmap },
    /// GETFH - Get current file handle.
    Getfh,
    /// LINK - Create hard link.
    Link { new_name: String },
    /// LOCK - Lock a byte range.
    Lock {
        lock_type: u32,
        reclaim: bool,
        offset: u64,
        length: u64,
        locker: LockArgs,
    },
    /// LOCKT - Test lock.
    Lockt {
        lock_type: u32,
        offset: u64,
        length: u64,
        owner: LockOwner4,
    },
    /// LOCKU - Unlock a byte range.
    Locku {
        lock_type: u32,
        seqid: u32,
        stateid: Stateid4,
        offset: u64,
        length: u64,
    },
    /// LOOKUP - Look up filename.
    Lookup { name: String },
    /// LOOKUPP - Look up parent directory.
    Lookupp,
    /// OPEN - Open a file.
    Open {
        seqid: u32,
        share_access: ShareAccess4,
        share_deny: ShareDeny4,
        owner: OpenOwner4,
        openhow: OpenFlags4,
        claim: OpenClaim4,
    },
    /// PUTFH - Set current file handle.
    Putfh { fh: NfsFh4 },
    /// PUTPUBFH - Set public file handle.
    Putpubfh,
    /// PUTROOTFH - Set root file handle.
    Putrootfh,
    /// READ - Read from file.
    Read {
        stateid: Stateid4,
        offset: u64,
        count: u32,
    },
    /// READDIR - Read directory.
    Readdir {
        cookie: u64,
        cookieverf: Verifier4,
        dircount: u32,
        maxcount: u32,
        attr_request: AttrBitmap,
    },
    /// READLINK - Read symbolic link.
    Readlink,
    /// REMOVE - Remove a file/directory.
    Remove { target: String },
    /// RENAME - Rename a file.
    Rename { oldname: String, newname: String },
    /// RESTOREFH - Restore saved file handle.
    Restorefh,
    /// SAVEFH - Save current file handle.
    Savefh,
    /// SETATTR - Set file attributes.
    Setattr { stateid: Stateid4, attrs: Attr4 },
    /// WRITE - Write to file.
    Write {
        stateid: Stateid4,
        offset: u64,
        stable: u32,
        data: Vec<u8>,
    },
    // NFSv4.1 operations
    /// EXCHANGE_ID - Establish client identity.
    ExchangeId {
        client_owner: ClientOwner4,
        flags: u32,
        state_protect: StateProtect4,
        impl_id: Vec<NfsImplId4>,
    },
    /// CREATE_SESSION - Create a session.
    CreateSession {
        client_id: u64,
        sequence: u32,
        flags: u32,
        fore_chan_attrs: ChannelAttrs4,
        back_chan_attrs: ChannelAttrs4,
        cb_program: u32,
        sec_parms: Vec<CallbackSecParms4>,
    },
    /// DESTROY_SESSION - Destroy a session.
    DestroySession { session_id: NfsSessionId },
    /// SEQUENCE - Sequence operation (must be first in NFSv4.1).
    Sequence {
        session_id: NfsSessionId,
        sequence_id: u32,
        slot_id: u32,
        highest_slot_id: u32,
        cachethis: bool,
    },
    /// RECLAIM_COMPLETE - Client reclaim complete.
    ReclaimComplete { one_fs: bool },
    /// Illegal operation.
    Illegal,
}

/// Lock operation arguments.
#[derive(Debug, Clone)]
pub enum LockArgs {
    /// New lock owner.
    NewLocker {
        open_seqid: u32,
        open_stateid: Stateid4,
        lock_seqid: u32,
        lock_owner: LockOwner4,
    },
    /// Existing lock owner.
    Locker { lock_stateid: Stateid4, lock_seqid: u32 },
}

/// Lock owner identifier.
#[derive(Debug, Clone)]
pub struct LockOwner4 {
    /// Client ID.
    pub client_id: u64,
    /// Owner identifier.
    pub owner: Vec<u8>,
}

/// Open owner identifier.
#[derive(Debug, Clone)]
pub struct OpenOwner4 {
    /// Client ID.
    pub client_id: u64,
    /// Owner identifier.
    pub owner: Vec<u8>,
}

/// Client owner for EXCHANGE_ID.
#[derive(Debug, Clone)]
pub struct ClientOwner4 {
    /// Co-owner ID.
    pub co_ownerid: Vec<u8>,
    /// Verifier.
    pub co_verifier: Verifier4,
}

/// State protection for EXCHANGE_ID.
#[derive(Debug, Clone)]
pub struct StateProtect4 {
    /// Protection type.
    pub spa_how: u32,
}

/// NFS implementation ID.
#[derive(Debug, Clone)]
pub struct NfsImplId4 {
    /// Domain.
    pub nii_domain: String,
    /// Name.
    pub nii_name: String,
    /// Date.
    pub nii_date: NfsTime4,
}

/// Channel attributes for CREATE_SESSION.
#[derive(Debug, Clone, Default)]
pub struct ChannelAttrs4 {
    /// Header padding size.
    pub ca_headerpadsize: u32,
    /// Maximum request size.
    pub ca_maxrequestsize: u32,
    /// Maximum response size.
    pub ca_maxresponsesize: u32,
    /// Maximum response size (cached).
    pub ca_maxresponsesize_cached: u32,
    /// Maximum operations.
    pub ca_maxoperations: u32,
    /// Maximum requests.
    pub ca_maxrequests: u32,
    /// RDMA IRD.
    pub ca_rdma_ird: Option<u32>,
}

/// Callback security parameters.
#[derive(Debug, Clone)]
pub struct CallbackSecParms4 {
    /// Security flavor.
    pub cb_secflavor: u32,
    /// System credentials (if AUTH_SYS).
    pub cb_sys_cred: Option<SysCred4>,
}

/// System credentials.
#[derive(Debug, Clone)]
pub struct SysCred4 {
    /// UID.
    pub uid: u32,
    /// GID.
    pub gid: u32,
    /// Auxiliary GIDs.
    pub gids: Vec<u32>,
    /// Machine name.
    pub machinename: String,
}

/// Operation result within a COMPOUND response.
#[derive(Debug, Clone)]
pub struct NfsResOp4 {
    /// Operation code.
    pub op: NfsOpNum4,
    /// Status.
    pub status: NfsStatus,
    /// Result data (varies by operation).
    pub result: OpResult,
}

/// Operation-specific result data.
#[derive(Debug, Clone)]
pub enum OpResult {
    /// No additional result.
    None,
    /// ACCESS result.
    Access { supported: Access4, access: Access4 },
    /// CLOSE result.
    Close { stateid: Stateid4 },
    /// COMMIT result.
    Commit { verifier: Verifier4 },
    /// CREATE result.
    Create { cinfo: ChangeInfo4, attrs: AttrBitmap },
    /// GETATTR result.
    Getattr { attrs: Attr4 },
    /// GETFH result.
    Getfh { fh: NfsFh4 },
    /// LINK result.
    Link { cinfo: ChangeInfo4 },
    /// LOCK result.
    Lock { stateid: Stateid4 },
    /// LOCKT result (denied lock info).
    Lockt { denied: Option<LockDenied4> },
    /// LOCKU result.
    Locku { stateid: Stateid4 },
    /// OPEN result.
    Open {
        stateid: Stateid4,
        cinfo: ChangeInfo4,
        rflags: u32,
        attrset: AttrBitmap,
        delegation: OpenDelegation4,
    },
    /// READ result.
    Read { eof: bool, data: Vec<u8> },
    /// READDIR result.
    Readdir {
        cookieverf: Verifier4,
        entries: Vec<DirEntry4>,
        eof: bool,
    },
    /// READLINK result.
    Readlink { link: String },
    /// REMOVE result.
    Remove { cinfo: ChangeInfo4 },
    /// RENAME result.
    Rename {
        source_cinfo: ChangeInfo4,
        target_cinfo: ChangeInfo4,
    },
    /// SETATTR result.
    Setattr { attrsset: AttrBitmap },
    /// WRITE result.
    Write {
        count: u32,
        committed: u32,
        verifier: Verifier4,
    },
    /// EXCHANGE_ID result.
    ExchangeId {
        client_id: u64,
        sequence_id: u32,
        flags: u32,
        state_protect: StateProtect4,
        server_owner: ServerOwner4,
        server_scope: Vec<u8>,
        server_impl_id: Vec<NfsImplId4>,
    },
    /// CREATE_SESSION result.
    CreateSession {
        session_id: NfsSessionId,
        sequence_id: u32,
        flags: u32,
        fore_chan_attrs: ChannelAttrs4,
        back_chan_attrs: ChannelAttrs4,
    },
    /// SEQUENCE result.
    Sequence {
        session_id: NfsSessionId,
        sequence_id: u32,
        slot_id: u32,
        highest_slot_id: u32,
        target_highest_slot_id: u32,
        status_flags: u32,
    },
}

/// Lock denied information.
#[derive(Debug, Clone)]
pub struct LockDenied4 {
    /// Offset.
    pub offset: u64,
    /// Length.
    pub length: u64,
    /// Lock type.
    pub locktype: u32,
    /// Owner.
    pub owner: LockOwner4,
}

/// Open delegation result.
#[derive(Debug, Clone)]
pub enum OpenDelegation4 {
    /// No delegation.
    None,
    /// Read delegation.
    Read {
        stateid: Stateid4,
        recall: bool,
        permissions: NfsAcl4,
    },
    /// Write delegation.
    Write {
        stateid: Stateid4,
        recall: bool,
        space_limit: Option<u64>,
        permissions: NfsAcl4,
    },
}

/// Directory entry.
#[derive(Debug, Clone)]
pub struct DirEntry4 {
    /// Cookie.
    pub cookie: u64,
    /// Name.
    pub name: String,
    /// Attributes.
    pub attrs: Attr4,
}

/// Server owner for EXCHANGE_ID result.
#[derive(Debug, Clone)]
pub struct ServerOwner4 {
    /// Minor ID.
    pub so_minor_id: u64,
    /// Major ID.
    pub so_major_id: Vec<u8>,
}

/// COMPOUND request.
#[derive(Debug, Clone)]
pub struct CompoundRequest {
    /// Tag for the request.
    pub tag: String,
    /// Minor version (0 for NFSv4.0, 1 for NFSv4.1, 2 for NFSv4.2).
    pub minorversion: u32,
    /// Operations.
    pub operations: Vec<NfsArgOp4>,
}

impl CompoundRequest {
    /// Create a new COMPOUND request.
    pub fn new(minorversion: u32) -> Self {
        Self {
            tag: String::new(),
            minorversion,
            operations: Vec::new(),
        }
    }

    /// Add an operation.
    pub fn add_op(&mut self, op: NfsArgOp4) {
        self.operations.push(op);
    }
}

/// COMPOUND response.
#[derive(Debug, Clone)]
pub struct CompoundResponse {
    /// Status of the compound operation.
    pub status: NfsStatus,
    /// Tag echoed from request.
    pub tag: String,
    /// Results of each operation.
    pub results: Vec<NfsResOp4>,
}

impl CompoundResponse {
    /// Create a new successful response.
    pub fn new(tag: String) -> Self {
        Self {
            status: NfsStatus::Ok,
            tag,
            results: Vec::new(),
        }
    }

    /// Create an error response.
    pub fn error(status: NfsStatus, tag: String) -> Self {
        Self {
            status,
            tag,
            results: Vec::new(),
        }
    }

    /// Add an operation result.
    pub fn add_result(&mut self, result: NfsResOp4) {
        // Update overall status if this operation failed
        if result.status != NfsStatus::Ok && self.status == NfsStatus::Ok {
            self.status = result.status;
        }
        self.results.push(result);
    }
}

/// Result type for compound operations.
pub type CompoundResult = Result<CompoundResponse, NfsError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_code_conversion() {
        assert_eq!(NfsOpNum4::from_u32(9), Some(NfsOpNum4::Getattr));
        assert_eq!(NfsOpNum4::from_u32(53), Some(NfsOpNum4::Sequence));
        assert_eq!(NfsOpNum4::from_u32(99999), None);
    }

    #[test]
    fn test_compound_request() {
        let mut req = CompoundRequest::new(1);
        req.add_op(NfsArgOp4::Putrootfh);
        req.add_op(NfsArgOp4::Getfh);

        assert_eq!(req.operations.len(), 2);
        assert_eq!(req.minorversion, 1);
    }

    #[test]
    fn test_compound_response() {
        let mut resp = CompoundResponse::new("test".to_string());

        resp.add_result(NfsResOp4 {
            op: NfsOpNum4::Putrootfh,
            status: NfsStatus::Ok,
            result: OpResult::None,
        });

        assert_eq!(resp.status, NfsStatus::Ok);
        assert_eq!(resp.results.len(), 1);

        // Add error result
        resp.add_result(NfsResOp4 {
            op: NfsOpNum4::Lookup,
            status: NfsStatus::Noent,
            result: OpResult::None,
        });

        // Overall status should now be the error
        assert_eq!(resp.status, NfsStatus::Noent);
    }
}
