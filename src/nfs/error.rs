//! NFS error types and status codes.
//!
//! Implements NFSv4 status codes as defined in RFC 7530.

use std::fmt;
use thiserror::Error;

/// NFS status codes as defined in RFC 7530.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum NfsStatus {
    /// No error - operation succeeded
    Ok = 0,
    /// Permission denied
    Perm = 1,
    /// No such file or directory
    Noent = 2,
    /// I/O error
    Io = 5,
    /// No such device or address
    Nxio = 6,
    /// Permission denied
    Access = 13,
    /// File exists
    Exist = 17,
    /// Cross-device link
    Xdev = 18,
    /// Not a directory
    Notdir = 20,
    /// Is a directory
    Isdir = 21,
    /// Invalid argument
    Inval = 22,
    /// File too large
    Fbig = 27,
    /// No space left on device
    Nospc = 28,
    /// Read-only file system
    Rofs = 30,
    /// Too many links
    Mlink = 31,
    /// Name too long
    Nametoolong = 63,
    /// Directory not empty
    Notempty = 66,
    /// Disk quota exceeded
    Dquot = 69,
    /// Stale file handle
    Stale = 70,
    /// Remote error
    Remote = 71,
    /// Bad handle
    Badhandle = 10001,
    /// Server not synced
    NotSync = 10002,
    /// Illegal NFS file handle
    BadCookie = 10003,
    /// Operation not supported
    Notsupp = 10004,
    /// Too small
    Toosmall = 10005,
    /// Server fault
    Serverfault = 10006,
    /// Bad type
    Badtype = 10007,
    /// Request delay/retry (same as JUKEBOX)
    Delay = 10008,
    /// NFS4ERR_SAME: same as current
    Same = 10009,
    /// Denied
    Denied = 10010,
    /// Expired
    Expired = 10011,
    /// Locked
    Locked = 10012,
    /// Grace period
    Grace = 10013,
    /// Filehandle expired
    FhExpired = 10014,
    /// Share denied
    ShareDenied = 10015,
    /// Wrong security flavor
    WrongSec = 10016,
    /// Client ID in use
    ClidInuse = 10017,
    /// Resource exhausted
    Resource = 10018,
    /// Move to new server
    Moved = 10019,
    /// No matching layout
    NoGrace = 10020,
    /// Reclaim in conflict
    ReclaimConflict = 10021,
    /// Bad state id
    BadStateid = 10025,
    /// Bad XDR
    BadXdr = 10036,
    /// Locks held
    LocksHeld = 10037,
    /// Open file
    Openmode = 10038,
    /// Bad session digest
    BadSessionDigest = 10039,
    /// Bad session
    BadSession = 10052,
    /// Bad slot
    BadSlot = 10053,
    /// Complete already
    CompleteAlready = 10054,
    /// Connection not bound to session
    ConnNotBoundToSession = 10055,
    /// Delegation already wanted
    DelegAlreadyWanted = 10056,
    /// Back channel busy
    BackChanBusy = 10057,
    /// Layout try later
    LayoutTryLater = 10058,
    /// Layout unavailable
    LayoutUnavailable = 10059,
    /// No matching layout
    NoMatchingLayout = 10060,
    /// Recall conflict
    RecallConflict = 10061,
    /// Unknown layout type
    UnknownLayoutType = 10062,
    /// Sequence misordered
    SeqMisordered = 10063,
    /// Sequence position
    SeqFalseRetry = 10064,
    /// Retry uncached reply
    RetryUncachedRep = 10065,
    /// Bad high slot
    BadHighSlot = 10066,
    /// Dead session
    DeadSession = 10067,
    /// Encryption algorithm unsupported
    EncryptionAlgUnsupported = 10068,
    /// pNFS I/O hole
    PnfsIoHole = 10069,
    /// Sequence false retry
    SeqFalseReply = 10070,
    /// Client ID not found
    ClientIdNotFound = 10071,
    /// Union not supp
    UnionNotSupp = 10072,
    /// Offload canceled
    OffloadCanceled = 10073,
    /// Offload no reqs
    OffloadNoReqs = 10074,
    /// NFS union not supp
    NfsUnionNotSupp = 10075,
    /// Wrong low version
    WrongLfs = 10092,
    /// Bad label
    Badlabel = 10093,
    /// Wrong credential
    WrongCred = 10094,
}

impl NfsStatus {
    /// Convert status to u32 value.
    pub fn to_u32(self) -> u32 {
        self as u32
    }

    /// Create status from u32 value.
    pub fn from_u32(val: u32) -> Option<Self> {
        match val {
            0 => Some(Self::Ok),
            1 => Some(Self::Perm),
            2 => Some(Self::Noent),
            5 => Some(Self::Io),
            6 => Some(Self::Nxio),
            13 => Some(Self::Access),
            17 => Some(Self::Exist),
            18 => Some(Self::Xdev),
            20 => Some(Self::Notdir),
            21 => Some(Self::Isdir),
            22 => Some(Self::Inval),
            27 => Some(Self::Fbig),
            28 => Some(Self::Nospc),
            30 => Some(Self::Rofs),
            31 => Some(Self::Mlink),
            63 => Some(Self::Nametoolong),
            66 => Some(Self::Notempty),
            69 => Some(Self::Dquot),
            70 => Some(Self::Stale),
            71 => Some(Self::Remote),
            10001 => Some(Self::Badhandle),
            10002 => Some(Self::NotSync),
            10003 => Some(Self::BadCookie),
            10004 => Some(Self::Notsupp),
            10005 => Some(Self::Toosmall),
            10006 => Some(Self::Serverfault),
            10007 => Some(Self::Badtype),
            10008 => Some(Self::Delay),
            10009 => Some(Self::Same),
            10010 => Some(Self::Denied),
            10011 => Some(Self::Expired),
            10012 => Some(Self::Locked),
            10013 => Some(Self::Grace),
            10014 => Some(Self::FhExpired),
            10015 => Some(Self::ShareDenied),
            10016 => Some(Self::WrongSec),
            10017 => Some(Self::ClidInuse),
            10018 => Some(Self::Resource),
            10019 => Some(Self::Moved),
            10020 => Some(Self::NoGrace),
            10021 => Some(Self::ReclaimConflict),
            10025 => Some(Self::BadStateid),
            10036 => Some(Self::BadXdr),
            10037 => Some(Self::LocksHeld),
            10038 => Some(Self::Openmode),
            10039 => Some(Self::BadSessionDigest),
            10052 => Some(Self::BadSession),
            10053 => Some(Self::BadSlot),
            10054 => Some(Self::CompleteAlready),
            10055 => Some(Self::ConnNotBoundToSession),
            10056 => Some(Self::DelegAlreadyWanted),
            10057 => Some(Self::BackChanBusy),
            10058 => Some(Self::LayoutTryLater),
            10059 => Some(Self::LayoutUnavailable),
            10060 => Some(Self::NoMatchingLayout),
            10061 => Some(Self::RecallConflict),
            10062 => Some(Self::UnknownLayoutType),
            10063 => Some(Self::SeqMisordered),
            10064 => Some(Self::SeqFalseRetry),
            10065 => Some(Self::RetryUncachedRep),
            10066 => Some(Self::BadHighSlot),
            10067 => Some(Self::DeadSession),
            10068 => Some(Self::EncryptionAlgUnsupported),
            10069 => Some(Self::PnfsIoHole),
            10070 => Some(Self::SeqFalseReply),
            10071 => Some(Self::ClientIdNotFound),
            10072 => Some(Self::UnionNotSupp),
            10073 => Some(Self::OffloadCanceled),
            10074 => Some(Self::OffloadNoReqs),
            10075 => Some(Self::NfsUnionNotSupp),
            10092 => Some(Self::WrongLfs),
            10093 => Some(Self::Badlabel),
            10094 => Some(Self::WrongCred),
            _ => None,
        }
    }

    /// Check if this is a success status.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Ok)
    }

    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Delay
                | Self::Grace
                | Self::Locked
                | Self::LayoutTryLater
                | Self::Resource
        )
    }
}

impl fmt::Display for NfsStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// NFS error type for gateway operations.
#[derive(Error, Debug)]
pub enum NfsError {
    /// Protocol-level error with NFS status.
    #[error("NFS error: {status}")]
    Protocol { status: NfsStatus },

    /// Session-related error.
    #[error("Session error: {0}")]
    Session(String),

    /// State-related error.
    #[error("State error: {0}")]
    State(String),

    /// XDR encoding/decoding error.
    #[error("XDR error: {0}")]
    Xdr(String),

    /// I/O error from the backend.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),

    /// Network error.
    #[error("Network error: {0}")]
    Network(String),
}

impl NfsError {
    /// Create a protocol error with the given status.
    pub fn protocol(status: NfsStatus) -> Self {
        Self::Protocol { status }
    }

    /// Convert to NFS status code.
    pub fn to_status(&self) -> NfsStatus {
        match self {
            Self::Protocol { status } => *status,
            Self::Session(_) => NfsStatus::BadSession,
            Self::State(_) => NfsStatus::Stale,
            Self::Xdr(_) => NfsStatus::BadXdr,
            Self::Io(_) => NfsStatus::Io,
            Self::Internal(_) => NfsStatus::Serverfault,
            Self::Network(_) => NfsStatus::Io,
        }
    }
}

impl From<crate::error::StrataError> for NfsError {
    fn from(err: crate::error::StrataError) -> Self {
        use crate::error::StrataError;
        match err {
            StrataError::FileNotFound(_) | StrataError::InodeNotFound(_) => {
                NfsError::protocol(NfsStatus::Noent)
            }
            StrataError::FileExists(_) => NfsError::protocol(NfsStatus::Exist),
            StrataError::NotADirectory(_) => NfsError::protocol(NfsStatus::Notdir),
            StrataError::NotAFile(_) => NfsError::protocol(NfsStatus::Isdir),
            StrataError::DirectoryNotEmpty(_) => NfsError::protocol(NfsStatus::Notempty),
            StrataError::InvalidPath(_) => NfsError::protocol(NfsStatus::Inval),
            StrataError::PermissionDenied(_) => NfsError::protocol(NfsStatus::Access),
            StrataError::QuotaExceeded { .. } => NfsError::protocol(NfsStatus::Dquot),
            StrataError::Network(_) => NfsError::protocol(NfsStatus::Io),
            _ => NfsError::Internal(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nfs_status_conversion() {
        assert_eq!(NfsStatus::Ok.to_u32(), 0);
        assert_eq!(NfsStatus::Noent.to_u32(), 2);
        assert_eq!(NfsStatus::BadSession.to_u32(), 10052);

        assert_eq!(NfsStatus::from_u32(0), Some(NfsStatus::Ok));
        assert_eq!(NfsStatus::from_u32(2), Some(NfsStatus::Noent));
        assert_eq!(NfsStatus::from_u32(99999), None);
    }

    #[test]
    fn test_nfs_status_checks() {
        assert!(NfsStatus::Ok.is_success());
        assert!(!NfsStatus::Noent.is_success());

        assert!(NfsStatus::Delay.is_retryable());
        assert!(NfsStatus::Grace.is_retryable());
        assert!(!NfsStatus::Noent.is_retryable());
    }

    #[test]
    fn test_nfs_error_to_status() {
        let err = NfsError::protocol(NfsStatus::Noent);
        assert_eq!(err.to_status(), NfsStatus::Noent);

        let err = NfsError::Session("test".into());
        assert_eq!(err.to_status(), NfsStatus::BadSession);
    }
}
