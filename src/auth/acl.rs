//! Access Control List (ACL) enforcement for Strata.
//!
//! Provides POSIX-compatible permission checking and extended ACL support.

use crate::error::{Result, StrataError};
use crate::Inode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Permission bits for access control.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Permission(u32);

impl Permission {
    /// No permission.
    pub const NONE: Permission = Permission(0);
    /// Execute permission.
    pub const EXECUTE: Permission = Permission(1);
    /// Write permission.
    pub const WRITE: Permission = Permission(2);
    /// Read permission.
    pub const READ: Permission = Permission(4);
    /// Read and execute.
    pub const READ_EXECUTE: Permission = Permission(5);
    /// Read and write.
    pub const READ_WRITE: Permission = Permission(6);
    /// All permissions.
    pub const ALL: Permission = Permission(7);

    /// Create from raw bits.
    pub fn from_bits(bits: u32) -> Self {
        Permission(bits & 0o7)
    }

    /// Get raw bits.
    pub fn bits(&self) -> u32 {
        self.0
    }

    /// Check if permission includes read.
    pub fn can_read(&self) -> bool {
        (self.0 & 4) != 0
    }

    /// Check if permission includes write.
    pub fn can_write(&self) -> bool {
        (self.0 & 2) != 0
    }

    /// Check if permission includes execute.
    pub fn can_execute(&self) -> bool {
        (self.0 & 1) != 0
    }

    /// Check if this permission includes the requested permission.
    pub fn includes(&self, requested: Permission) -> bool {
        (self.0 & requested.0) == requested.0
    }
}

/// User credentials for access checking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserCredentials {
    /// User ID.
    pub uid: u32,
    /// Primary group ID.
    pub gid: u32,
    /// Supplementary group IDs.
    pub groups: Vec<u32>,
}

impl UserCredentials {
    /// Create new user credentials.
    pub fn new(uid: u32, gid: u32) -> Self {
        Self {
            uid,
            gid,
            groups: Vec::new(),
        }
    }

    /// Create root credentials.
    pub fn root() -> Self {
        Self {
            uid: 0,
            gid: 0,
            groups: vec![0],
        }
    }

    /// Add supplementary group.
    pub fn with_group(mut self, gid: u32) -> Self {
        if !self.groups.contains(&gid) {
            self.groups.push(gid);
        }
        self
    }

    /// Check if user is root.
    pub fn is_root(&self) -> bool {
        self.uid == 0
    }

    /// Check if user is in a specific group.
    pub fn in_group(&self, gid: u32) -> bool {
        self.gid == gid || self.groups.contains(&gid)
    }
}

impl Default for UserCredentials {
    fn default() -> Self {
        Self::new(1000, 1000)
    }
}

/// Access control request type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    /// Read access.
    Read,
    /// Write access.
    Write,
    /// Execute (for directories: access/search).
    Execute,
    /// Read and write.
    ReadWrite,
    /// All access types.
    All,
}

impl AccessType {
    /// Convert to permission bits.
    pub fn to_permission(&self) -> Permission {
        match self {
            AccessType::Read => Permission::READ,
            AccessType::Write => Permission::WRITE,
            AccessType::Execute => Permission::EXECUTE,
            AccessType::ReadWrite => Permission::READ_WRITE,
            AccessType::All => Permission::ALL,
        }
    }
}

/// Extended ACL entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclEntry {
    /// Type of ACL entry.
    pub entry_type: AclEntryType,
    /// Permission granted.
    pub permission: Permission,
}

/// Type of ACL entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AclEntryType {
    /// Owner user.
    UserOwner,
    /// Named user.
    User(u32),
    /// Owning group.
    GroupOwner,
    /// Named group.
    Group(u32),
    /// Mask entry (limits named entries).
    Mask,
    /// Others.
    Other,
}

/// Extended ACL for an inode.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExtendedAcl {
    /// ACL entries.
    pub entries: Vec<AclEntry>,
}

impl ExtendedAcl {
    /// Create a new empty ACL.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Add an entry to the ACL.
    pub fn add_entry(&mut self, entry: AclEntry) {
        self.entries.push(entry);
    }

    /// Get permission for a specific entry type.
    pub fn get_permission(&self, entry_type: &AclEntryType) -> Option<Permission> {
        self.entries
            .iter()
            .find(|e| &e.entry_type == entry_type)
            .map(|e| e.permission)
    }

    /// Get the mask permission.
    pub fn get_mask(&self) -> Option<Permission> {
        self.get_permission(&AclEntryType::Mask)
    }
}

/// Access control checker.
#[derive(Debug, Clone)]
pub struct AccessChecker {
    /// Extended ACLs by inode.
    extended_acls: HashMap<u64, ExtendedAcl>,
    /// Enable strict mode (deny on any error).
    strict_mode: bool,
}

impl AccessChecker {
    /// Create a new access checker.
    pub fn new() -> Self {
        Self {
            extended_acls: HashMap::new(),
            strict_mode: false,
        }
    }

    /// Enable strict mode.
    pub fn with_strict_mode(mut self) -> Self {
        self.strict_mode = true;
        self
    }

    /// Set extended ACL for an inode.
    pub fn set_acl(&mut self, inode: u64, acl: ExtendedAcl) {
        self.extended_acls.insert(inode, acl);
    }

    /// Remove extended ACL for an inode.
    pub fn remove_acl(&mut self, inode: u64) {
        self.extended_acls.remove(&inode);
    }

    /// Check access to an inode.
    pub fn check_access(
        &self,
        inode: &Inode,
        credentials: &UserCredentials,
        access_type: AccessType,
    ) -> Result<()> {
        // Root always has access
        if credentials.is_root() {
            return Ok(());
        }

        let requested = access_type.to_permission();

        // Check extended ACL first
        if let Some(acl) = self.extended_acls.get(&inode.id) {
            return self.check_acl_access(inode, acl, credentials, requested);
        }

        // Fall back to POSIX permissions
        self.check_posix_access(inode, credentials, requested)
    }

    /// Check POSIX-style permissions.
    fn check_posix_access(
        &self,
        inode: &Inode,
        credentials: &UserCredentials,
        requested: Permission,
    ) -> Result<()> {
        let mode = inode.mode;

        let permission = if credentials.uid == inode.uid {
            // Owner permissions
            Permission::from_bits((mode >> 6) & 0o7)
        } else if credentials.in_group(inode.gid) {
            // Group permissions
            Permission::from_bits((mode >> 3) & 0o7)
        } else {
            // Other permissions
            Permission::from_bits(mode & 0o7)
        };

        if permission.includes(requested) {
            Ok(())
        } else {
            Err(StrataError::PermissionDenied(format!(
                "Access denied: user {} requesting {:?} on inode {}",
                credentials.uid,
                requested.bits(),
                inode.id
            )))
        }
    }

    /// Check extended ACL permissions.
    fn check_acl_access(
        &self,
        inode: &Inode,
        acl: &ExtendedAcl,
        credentials: &UserCredentials,
        requested: Permission,
    ) -> Result<()> {
        // Step 1: Check if user is the owner
        if credentials.uid == inode.uid {
            if let Some(perm) = acl.get_permission(&AclEntryType::UserOwner) {
                if perm.includes(requested) {
                    return Ok(());
                }
            }
            // Fall back to mode bits for owner
            let owner_perm = Permission::from_bits((inode.mode >> 6) & 0o7);
            if owner_perm.includes(requested) {
                return Ok(());
            }
            return Err(StrataError::PermissionDenied(
                "Owner access denied".to_string(),
            ));
        }

        // Step 2: Check named user entries
        if let Some(perm) = acl.get_permission(&AclEntryType::User(credentials.uid)) {
            let effective = self.apply_mask(acl, perm);
            if effective.includes(requested) {
                return Ok(());
            }
        }

        // Step 3: Check groups
        let mut group_matched = false;
        let mut max_group_perm = Permission::NONE;

        // Check owning group
        if credentials.in_group(inode.gid) {
            group_matched = true;
            if let Some(perm) = acl.get_permission(&AclEntryType::GroupOwner) {
                max_group_perm = Permission::from_bits(max_group_perm.bits() | perm.bits());
            } else {
                let group_perm = Permission::from_bits((inode.mode >> 3) & 0o7);
                max_group_perm = Permission::from_bits(max_group_perm.bits() | group_perm.bits());
            }
        }

        // Check named groups
        for gid in std::iter::once(credentials.gid).chain(credentials.groups.iter().copied()) {
            if let Some(perm) = acl.get_permission(&AclEntryType::Group(gid)) {
                group_matched = true;
                max_group_perm = Permission::from_bits(max_group_perm.bits() | perm.bits());
            }
        }

        if group_matched {
            let effective = self.apply_mask(acl, max_group_perm);
            if effective.includes(requested) {
                return Ok(());
            }
            return Err(StrataError::PermissionDenied(
                "Group access denied".to_string(),
            ));
        }

        // Step 4: Check others
        if let Some(perm) = acl.get_permission(&AclEntryType::Other) {
            if perm.includes(requested) {
                return Ok(());
            }
        } else {
            let other_perm = Permission::from_bits(inode.mode & 0o7);
            if other_perm.includes(requested) {
                return Ok(());
            }
        }

        Err(StrataError::PermissionDenied(
            "Access denied".to_string(),
        ))
    }

    /// Apply mask to permission.
    fn apply_mask(&self, acl: &ExtendedAcl, perm: Permission) -> Permission {
        if let Some(mask) = acl.get_mask() {
            Permission::from_bits(perm.bits() & mask.bits())
        } else {
            perm
        }
    }

    /// Check if user can read an inode.
    pub fn can_read(&self, inode: &Inode, credentials: &UserCredentials) -> bool {
        self.check_access(inode, credentials, AccessType::Read).is_ok()
    }

    /// Check if user can write to an inode.
    pub fn can_write(&self, inode: &Inode, credentials: &UserCredentials) -> bool {
        self.check_access(inode, credentials, AccessType::Write).is_ok()
    }

    /// Check if user can execute/access an inode.
    pub fn can_execute(&self, inode: &Inode, credentials: &UserCredentials) -> bool {
        self.check_access(inode, credentials, AccessType::Execute).is_ok()
    }
}

impl Default for AccessChecker {
    fn default() -> Self {
        Self::new()
    }
}

/// Check directory traversal permissions.
pub fn check_path_access(
    checker: &AccessChecker,
    path_inodes: &[Inode],
    credentials: &UserCredentials,
    final_access: AccessType,
) -> Result<()> {
    // Check execute permission on all parent directories
    for inode in path_inodes.iter().take(path_inodes.len().saturating_sub(1)) {
        checker.check_access(inode, credentials, AccessType::Execute)?;
    }

    // Check requested permission on final component
    if let Some(final_inode) = path_inodes.last() {
        checker.check_access(final_inode, credentials, final_access)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FileType;
    use std::collections::HashMap;
    use std::time::SystemTime;

    fn make_inode(id: u64, uid: u32, gid: u32, mode: u32) -> Inode {
        Inode {
            id,
            uid,
            gid,
            mode,
            file_type: FileType::RegularFile,
            size: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            nlink: 1,
            chunks: Vec::new(),
            symlink_target: None,
            extended_attrs: HashMap::new(),
            generation: 0,
        }
    }

    #[test]
    fn test_permission_bits() {
        assert!(Permission::READ.can_read());
        assert!(!Permission::READ.can_write());
        assert!(!Permission::READ.can_execute());

        assert!(Permission::ALL.can_read());
        assert!(Permission::ALL.can_write());
        assert!(Permission::ALL.can_execute());

        assert!(Permission::ALL.includes(Permission::READ));
        assert!(!Permission::READ.includes(Permission::WRITE));
    }

    #[test]
    fn test_root_access() {
        let checker = AccessChecker::new();
        let inode = make_inode(1, 1000, 1000, 0o000);
        let root = UserCredentials::root();

        assert!(checker.check_access(&inode, &root, AccessType::Read).is_ok());
        assert!(checker.check_access(&inode, &root, AccessType::Write).is_ok());
        assert!(checker.check_access(&inode, &root, AccessType::Execute).is_ok());
    }

    #[test]
    fn test_owner_access() {
        let checker = AccessChecker::new();
        let inode = make_inode(1, 1000, 1000, 0o600);
        let owner = UserCredentials::new(1000, 1000);
        let other = UserCredentials::new(1001, 1001);

        assert!(checker.check_access(&inode, &owner, AccessType::Read).is_ok());
        assert!(checker.check_access(&inode, &owner, AccessType::Write).is_ok());
        assert!(checker.check_access(&inode, &other, AccessType::Read).is_err());
    }

    #[test]
    fn test_group_access() {
        let checker = AccessChecker::new();
        let inode = make_inode(1, 1000, 100, 0o060);
        let group_member = UserCredentials::new(1001, 100);
        let non_member = UserCredentials::new(1002, 200);

        assert!(checker.check_access(&inode, &group_member, AccessType::Read).is_ok());
        assert!(checker.check_access(&inode, &non_member, AccessType::Read).is_err());
    }

    #[test]
    fn test_supplementary_group_access() {
        let checker = AccessChecker::new();
        let inode = make_inode(1, 1000, 100, 0o060);
        let user = UserCredentials::new(1001, 200).with_group(100);

        assert!(checker.check_access(&inode, &user, AccessType::Read).is_ok());
    }

    #[test]
    fn test_other_access() {
        let checker = AccessChecker::new();
        let inode = make_inode(1, 1000, 1000, 0o004);
        let other = UserCredentials::new(2000, 2000);

        assert!(checker.check_access(&inode, &other, AccessType::Read).is_ok());
        assert!(checker.check_access(&inode, &other, AccessType::Write).is_err());
    }

    #[test]
    fn test_extended_acl_user() {
        let mut checker = AccessChecker::new();
        let inode = make_inode(1, 1000, 1000, 0o600);

        let mut acl = ExtendedAcl::new();
        acl.add_entry(AclEntry {
            entry_type: AclEntryType::User(1001),
            permission: Permission::READ,
        });
        acl.add_entry(AclEntry {
            entry_type: AclEntryType::Mask,
            permission: Permission::READ_WRITE,
        });
        checker.set_acl(1, acl);

        let user = UserCredentials::new(1001, 2000);
        assert!(checker.check_access(&inode, &user, AccessType::Read).is_ok());
        assert!(checker.check_access(&inode, &user, AccessType::Write).is_err());
    }

    #[test]
    fn test_acl_mask() {
        let mut checker = AccessChecker::new();
        let inode = make_inode(1, 1000, 1000, 0o600);

        let mut acl = ExtendedAcl::new();
        acl.add_entry(AclEntry {
            entry_type: AclEntryType::User(1001),
            permission: Permission::ALL,
        });
        acl.add_entry(AclEntry {
            entry_type: AclEntryType::Mask,
            permission: Permission::READ,
        });
        checker.set_acl(1, acl);

        let user = UserCredentials::new(1001, 2000);
        // User has ALL but mask limits to READ
        assert!(checker.check_access(&inode, &user, AccessType::Read).is_ok());
        assert!(checker.check_access(&inode, &user, AccessType::Write).is_err());
    }

    #[test]
    fn test_convenience_methods() {
        let checker = AccessChecker::new();
        let inode = make_inode(1, 1000, 1000, 0o754);
        let owner = UserCredentials::new(1000, 1000);
        let group = UserCredentials::new(2000, 1000);
        let other = UserCredentials::new(3000, 3000);

        // Owner: rwx
        assert!(checker.can_read(&inode, &owner));
        assert!(checker.can_write(&inode, &owner));
        assert!(checker.can_execute(&inode, &owner));

        // Group: r-x
        assert!(checker.can_read(&inode, &group));
        assert!(!checker.can_write(&inode, &group));
        assert!(checker.can_execute(&inode, &group));

        // Other: r--
        assert!(checker.can_read(&inode, &other));
        assert!(!checker.can_write(&inode, &other));
        assert!(!checker.can_execute(&inode, &other));
    }

    #[test]
    fn test_path_access() {
        let checker = AccessChecker::new();
        let creds = UserCredentials::new(1000, 1000);

        let dir1 = make_inode(1, 1000, 1000, 0o755);
        let dir2 = make_inode(2, 1000, 1000, 0o755);
        let file = make_inode(3, 1000, 1000, 0o644);

        let path = vec![dir1, dir2, file];
        assert!(check_path_access(&checker, &path, &creds, AccessType::Read).is_ok());
    }

    #[test]
    fn test_path_access_denied_intermediate() {
        let checker = AccessChecker::new();
        let creds = UserCredentials::new(2000, 2000);

        let dir1 = make_inode(1, 1000, 1000, 0o700); // No access for others
        let file = make_inode(2, 2000, 2000, 0o644);

        let path = vec![dir1, file];
        assert!(check_path_access(&checker, &path, &creds, AccessType::Read).is_err());
    }

    #[test]
    fn test_user_credentials() {
        let user = UserCredentials::new(1000, 1000)
            .with_group(100)
            .with_group(200);

        assert!(!user.is_root());
        assert!(user.in_group(1000));
        assert!(user.in_group(100));
        assert!(user.in_group(200));
        assert!(!user.in_group(300));
    }
}
