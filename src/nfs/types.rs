//! NFS v4.1/4.2 type definitions.
//!
//! These types follow the XDR definitions from RFC 7530 (NFSv4.0),
//! RFC 5661 (NFSv4.1), and RFC 7862 (NFSv4.2).

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// NFS file type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum NfsFtype4 {
    /// Regular file
    Reg = 1,
    /// Directory
    Dir = 2,
    /// Block device
    Blk = 3,
    /// Character device
    Chr = 4,
    /// Symbolic link
    Lnk = 5,
    /// Socket
    Sock = 6,
    /// FIFO
    Fifo = 7,
    /// Attribute directory
    AttrDir = 8,
    /// Named attribute
    NamedAttr = 9,
}

impl NfsFtype4 {
    /// Convert from POSIX mode.
    pub fn from_mode(mode: u32) -> Self {
        match (mode >> 12) & 0xF {
            0x1 => Self::Fifo,
            0x2 => Self::Chr,
            0x4 => Self::Dir,
            0x6 => Self::Blk,
            0x8 => Self::Reg,
            0xA => Self::Lnk,
            0xC => Self::Sock,
            _ => Self::Reg,
        }
    }
}

/// NFS time value (seconds + nanoseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NfsTime4 {
    /// Seconds since epoch.
    pub seconds: i64,
    /// Nanoseconds.
    pub nseconds: u32,
}

impl NfsTime4 {
    /// Create a new NFS time.
    pub fn new(seconds: i64, nseconds: u32) -> Self {
        Self { seconds, nseconds }
    }

    /// Create from SystemTime.
    pub fn from_system_time(time: SystemTime) -> Self {
        match time.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => Self {
                seconds: duration.as_secs() as i64,
                nseconds: duration.subsec_nanos(),
            },
            Err(_) => Self::default(),
        }
    }

    /// Convert to SystemTime.
    pub fn to_system_time(&self) -> SystemTime {
        if self.seconds >= 0 {
            SystemTime::UNIX_EPOCH
                + std::time::Duration::new(self.seconds as u64, self.nseconds)
        } else {
            SystemTime::UNIX_EPOCH
        }
    }

    /// Get current time.
    pub fn now() -> Self {
        Self::from_system_time(SystemTime::now())
    }
}

/// NFS file handle opaque data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NfsFh4(pub Vec<u8>);

impl NfsFh4 {
    /// Maximum file handle size (128 bytes for NFSv4).
    pub const MAX_SIZE: usize = 128;

    /// Create a new file handle.
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    /// Create an empty file handle.
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    /// Get the file handle data.
    pub fn data(&self) -> &[u8] {
        &self.0
    }

    /// Check if the file handle is valid.
    pub fn is_valid(&self) -> bool {
        !self.0.is_empty() && self.0.len() <= Self::MAX_SIZE
    }
}

/// Verifier for create operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Verifier4(pub [u8; 8]);

impl Verifier4 {
    /// Create a new verifier.
    pub fn new(data: [u8; 8]) -> Self {
        Self(data)
    }

    /// Generate a random verifier.
    pub fn random() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Self(nanos.to_be_bytes())
    }
}

/// State ID for locks, opens, and delegations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Stateid4 {
    /// Sequence ID.
    pub seqid: u32,
    /// Other (opaque identifier).
    pub other: [u8; 12],
}

impl Stateid4 {
    /// Create a new state ID.
    pub fn new(seqid: u32, other: [u8; 12]) -> Self {
        Self { seqid, other }
    }

    /// Special state ID representing anonymous access.
    pub fn anonymous() -> Self {
        Self {
            seqid: 0,
            other: [0; 12],
        }
    }

    /// Special state ID for read bypass.
    pub fn read_bypass() -> Self {
        Self {
            seqid: 0xFFFFFFFF,
            other: [0xFF; 12],
        }
    }

    /// Special state ID for current state.
    pub fn current() -> Self {
        Self {
            seqid: 1,
            other: [0; 12],
        }
    }

    /// Increment the sequence ID.
    pub fn increment(&mut self) {
        self.seqid = self.seqid.wrapping_add(1);
        if self.seqid == 0 {
            self.seqid = 1;
        }
    }
}

/// NFS lease information.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NfsLease4 {
    /// Lease duration in seconds.
    pub duration: u32,
}

/// Bitmap for attribute requests.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttrBitmap(pub Vec<u32>);

impl AttrBitmap {
    /// Create a new empty bitmap.
    pub fn new() -> Self {
        Self(vec![0, 0, 0])
    }

    /// Set a bit.
    pub fn set(&mut self, bit: u32) {
        let word = (bit / 32) as usize;
        let bit_pos = bit % 32;
        while self.0.len() <= word {
            self.0.push(0);
        }
        self.0[word] |= 1 << bit_pos;
    }

    /// Check if a bit is set.
    pub fn is_set(&self, bit: u32) -> bool {
        let word = (bit / 32) as usize;
        let bit_pos = bit % 32;
        if word < self.0.len() {
            (self.0[word] & (1 << bit_pos)) != 0
        } else {
            false
        }
    }

    /// Clear a bit.
    pub fn clear(&mut self, bit: u32) {
        let word = (bit / 32) as usize;
        let bit_pos = bit % 32;
        if word < self.0.len() {
            self.0[word] &= !(1 << bit_pos);
        }
    }

    /// Standard file attributes.
    pub fn standard_file() -> Self {
        let mut bitmap = Self::new();
        // FATTR4_TYPE
        bitmap.set(1);
        // FATTR4_SIZE
        bitmap.set(4);
        // FATTR4_MODE
        bitmap.set(33);
        // FATTR4_NUMLINKS
        bitmap.set(35);
        // FATTR4_OWNER
        bitmap.set(36);
        // FATTR4_OWNER_GROUP
        bitmap.set(37);
        // FATTR4_TIME_ACCESS
        bitmap.set(47);
        // FATTR4_TIME_MODIFY
        bitmap.set(53);
        bitmap
    }
}

/// File attribute bit positions.
pub mod attr_bits {
    pub const SUPPORTED_ATTRS: u32 = 0;
    pub const TYPE: u32 = 1;
    pub const FH_EXPIRE_TYPE: u32 = 2;
    pub const CHANGE: u32 = 3;
    pub const SIZE: u32 = 4;
    pub const LINK_SUPPORT: u32 = 5;
    pub const SYMLINK_SUPPORT: u32 = 6;
    pub const NAMED_ATTR: u32 = 7;
    pub const FSID: u32 = 8;
    pub const UNIQUE_HANDLES: u32 = 9;
    pub const LEASE_TIME: u32 = 10;
    pub const RDATTR_ERROR: u32 = 11;
    pub const ACL: u32 = 12;
    pub const ACLSUPPORT: u32 = 13;
    pub const ARCHIVE: u32 = 14;
    pub const CANSETTIME: u32 = 15;
    pub const CASE_INSENSITIVE: u32 = 16;
    pub const CASE_PRESERVING: u32 = 17;
    pub const CHOWN_RESTRICTED: u32 = 18;
    pub const FILEHANDLE: u32 = 19;
    pub const FILEID: u32 = 20;
    pub const FILES_AVAIL: u32 = 21;
    pub const FILES_FREE: u32 = 22;
    pub const FILES_TOTAL: u32 = 23;
    pub const FS_LOCATIONS: u32 = 24;
    pub const HIDDEN: u32 = 25;
    pub const HOMOGENEOUS: u32 = 26;
    pub const MAXFILESIZE: u32 = 27;
    pub const MAXLINK: u32 = 28;
    pub const MAXNAME: u32 = 29;
    pub const MAXREAD: u32 = 30;
    pub const MAXWRITE: u32 = 31;
    pub const MIMETYPE: u32 = 32;
    pub const MODE: u32 = 33;
    pub const NO_TRUNC: u32 = 34;
    pub const NUMLINKS: u32 = 35;
    pub const OWNER: u32 = 36;
    pub const OWNER_GROUP: u32 = 37;
    pub const QUOTA_AVAIL_HARD: u32 = 38;
    pub const QUOTA_AVAIL_SOFT: u32 = 39;
    pub const QUOTA_USED: u32 = 40;
    pub const RAWDEV: u32 = 41;
    pub const SPACE_AVAIL: u32 = 42;
    pub const SPACE_FREE: u32 = 43;
    pub const SPACE_TOTAL: u32 = 44;
    pub const SPACE_USED: u32 = 45;
    pub const SYSTEM: u32 = 46;
    pub const TIME_ACCESS: u32 = 47;
    pub const TIME_ACCESS_SET: u32 = 48;
    pub const TIME_BACKUP: u32 = 49;
    pub const TIME_CREATE: u32 = 50;
    pub const TIME_DELTA: u32 = 51;
    pub const TIME_METADATA: u32 = 52;
    pub const TIME_MODIFY: u32 = 53;
    pub const TIME_MODIFY_SET: u32 = 54;
    pub const MOUNTED_ON_FILEID: u32 = 55;
}

/// File attributes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Attr4 {
    /// Bitmap of which attributes are present.
    pub attrmask: AttrBitmap,
    /// Encoded attribute values.
    pub attr_vals: Vec<u8>,
}

/// Change info for directory operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ChangeInfo4 {
    /// Atomic change.
    pub atomic: bool,
    /// Before change value.
    pub before: u64,
    /// After change value.
    pub after: u64,
}

impl ChangeInfo4 {
    /// Create new change info.
    pub fn new(before: u64, after: u64, atomic: bool) -> Self {
        Self {
            atomic,
            before,
            after,
        }
    }
}

/// Access rights requested/returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Access4(pub u32);

impl Access4 {
    pub const READ: u32 = 0x0001;
    pub const LOOKUP: u32 = 0x0002;
    pub const MODIFY: u32 = 0x0004;
    pub const EXTEND: u32 = 0x0008;
    pub const DELETE: u32 = 0x0010;
    pub const EXECUTE: u32 = 0x0020;

    /// Check if read access is requested.
    pub fn read(&self) -> bool {
        (self.0 & Self::READ) != 0
    }

    /// Check if lookup access is requested.
    pub fn lookup(&self) -> bool {
        (self.0 & Self::LOOKUP) != 0
    }

    /// Check if modify access is requested.
    pub fn modify(&self) -> bool {
        (self.0 & Self::MODIFY) != 0
    }
}

/// ACE type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum AceType4 {
    AccessAllowed = 0,
    AccessDenied = 1,
    SystemAudit = 2,
    SystemAlarm = 3,
}

/// ACE flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AceFlag4(pub u32);

impl AceFlag4 {
    pub const FILE_INHERIT: u32 = 0x00000001;
    pub const DIRECTORY_INHERIT: u32 = 0x00000002;
    pub const NO_PROPAGATE_INHERIT: u32 = 0x00000004;
    pub const INHERIT_ONLY: u32 = 0x00000008;
    pub const IDENTIFIER_GROUP: u32 = 0x00000040;
}

/// ACE access mask.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AceMask4(pub u32);

impl AceMask4 {
    pub const READ_DATA: u32 = 0x00000001;
    pub const LIST_DIRECTORY: u32 = 0x00000001;
    pub const WRITE_DATA: u32 = 0x00000002;
    pub const ADD_FILE: u32 = 0x00000002;
    pub const APPEND_DATA: u32 = 0x00000004;
    pub const ADD_SUBDIRECTORY: u32 = 0x00000004;
    pub const READ_NAMED_ATTRS: u32 = 0x00000008;
    pub const WRITE_NAMED_ATTRS: u32 = 0x00000010;
    pub const EXECUTE: u32 = 0x00000020;
    pub const DELETE_CHILD: u32 = 0x00000040;
    pub const READ_ATTRIBUTES: u32 = 0x00000080;
    pub const WRITE_ATTRIBUTES: u32 = 0x00000100;
    pub const DELETE: u32 = 0x00010000;
    pub const READ_ACL: u32 = 0x00020000;
    pub const WRITE_ACL: u32 = 0x00040000;
    pub const WRITE_OWNER: u32 = 0x00080000;
    pub const SYNCHRONIZE: u32 = 0x00100000;
}

/// Access Control Entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ace4 {
    /// ACE type.
    pub ace_type: AceType4,
    /// ACE flags.
    pub flag: AceFlag4,
    /// Access mask.
    pub access_mask: AceMask4,
    /// Who (principal).
    pub who: String,
}

/// NFS ACL.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NfsAcl4(pub Vec<Ace4>);

/// Device specifier for special files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Specdata4 {
    /// Major device number.
    pub specdata1: u32,
    /// Minor device number.
    pub specdata2: u32,
}

/// How to create a file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CreateHow4 {
    /// Unchecked - create or truncate.
    Unchecked(Attr4),
    /// Guarded - fail if exists.
    Guarded(Attr4),
    /// Exclusive - use verifier.
    Exclusive(Verifier4),
    /// Exclusive with attributes (NFSv4.1).
    ExclusiveCreate(Verifier4, Attr4),
}

/// Share access modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ShareAccess4(pub u32);

impl ShareAccess4 {
    pub const READ: u32 = 0x00000001;
    pub const WRITE: u32 = 0x00000002;
    pub const BOTH: u32 = 0x00000003;

    pub fn read(&self) -> bool {
        (self.0 & Self::READ) != 0
    }

    pub fn write(&self) -> bool {
        (self.0 & Self::WRITE) != 0
    }
}

/// Share deny modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ShareDeny4(pub u32);

impl ShareDeny4 {
    pub const NONE: u32 = 0x00000000;
    pub const READ: u32 = 0x00000001;
    pub const WRITE: u32 = 0x00000002;
    pub const BOTH: u32 = 0x00000003;
}

/// Open claim type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpenClaim4 {
    /// Open by name.
    Null(String),
    /// Open previous open.
    Previous(OpenDelegationType4),
    /// Open delegate current.
    DelegateCur(Stateid4, String),
    /// Open delegate previous.
    DelegatePrev(String),
    /// Open for filehandle (NFSv4.1).
    Fh,
    /// Open delegate for filehandle (NFSv4.1).
    DelegCurFh(Stateid4),
    /// Open delegate previous for filehandle (NFSv4.1).
    DelegPrevFh,
}

/// Open flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct OpenFlags4(pub u32);

impl OpenFlags4 {
    pub const CREATE: u32 = 0x00000001;
}

/// Open delegation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum OpenDelegationType4 {
    None = 0,
    Read = 1,
    Write = 2,
    NoneExt = 3,
}

/// Layout type for pNFS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum Layouttype4 {
    NfsV41Files = 1,
    Osd2Objects = 2,
    BlockVolume = 3,
    FlexFiles = 4,
}

/// Layout I/O mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum Layoutiomode4 {
    Read = 1,
    Rw = 2,
    Any = 3,
}

/// File system locations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FsLocations4 {
    /// Root path.
    pub fs_root: Vec<String>,
    /// Locations.
    pub locations: Vec<FsLocation4>,
}

/// Single file system location.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsLocation4 {
    /// Server name.
    pub server: Vec<String>,
    /// Root path on server.
    pub rootpath: Vec<String>,
}

/// File system version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FsVer4 {
    /// Major version.
    pub major: u32,
    /// Minor version.
    pub minor: u32,
}

/// File system status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FsStatus4 {
    /// Absent.
    pub absent: bool,
    /// Type.
    pub fs_type: u32,
    /// Source.
    pub source: u32,
    /// Current.
    pub current: bool,
    /// Age.
    pub age: i32,
    /// Version.
    pub version: FsVer4,
}

/// Response operation placeholder.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NfsResop4 {
    /// Placeholder for various operation results.
    Placeholder,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nfs_time() {
        let time = NfsTime4::new(1000, 500);
        assert_eq!(time.seconds, 1000);
        assert_eq!(time.nseconds, 500);

        let now = NfsTime4::now();
        assert!(now.seconds > 0);
    }

    #[test]
    fn test_file_handle() {
        let fh = NfsFh4::new(vec![1, 2, 3, 4]);
        assert!(fh.is_valid());
        assert_eq!(fh.data(), &[1, 2, 3, 4]);

        let empty = NfsFh4::empty();
        assert!(!empty.is_valid());
    }

    #[test]
    fn test_stateid() {
        let mut stateid = Stateid4::new(1, [0; 12]);
        assert_eq!(stateid.seqid, 1);

        stateid.increment();
        assert_eq!(stateid.seqid, 2);

        let anon = Stateid4::anonymous();
        assert_eq!(anon.seqid, 0);
    }

    #[test]
    fn test_attr_bitmap() {
        let mut bitmap = AttrBitmap::new();
        bitmap.set(1);
        bitmap.set(33);

        assert!(bitmap.is_set(1));
        assert!(bitmap.is_set(33));
        assert!(!bitmap.is_set(2));

        bitmap.clear(1);
        assert!(!bitmap.is_set(1));
    }

    #[test]
    fn test_nfs_ftype() {
        // Directory mode
        assert_eq!(NfsFtype4::from_mode(0o40755), NfsFtype4::Dir);
        // Regular file mode
        assert_eq!(NfsFtype4::from_mode(0o100644), NfsFtype4::Reg);
        // Symlink mode
        assert_eq!(NfsFtype4::from_mode(0o120777), NfsFtype4::Lnk);
    }
}
