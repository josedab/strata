//! FUSE filesystem implementation.

use super::StrataClient;
use crate::types::*;
use fuser::{
    FileAttr, FileType as FuseFileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use parking_lot::Mutex;
use std::ffi::OsStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, warn};

const TTL: Duration = Duration::from_secs(1);
const BLOCK_SIZE: u32 = 512;

/// Strata FUSE filesystem.
pub struct StrataFuse {
    client: Mutex<StrataClient>,
}

impl StrataFuse {
    pub fn new(client: StrataClient) -> Self {
        Self {
            client: Mutex::new(client),
        }
    }

    fn inode_to_attr(&self, inode: &Inode) -> FileAttr {
        let kind = match inode.file_type {
            FileType::RegularFile => FuseFileType::RegularFile,
            FileType::Directory => FuseFileType::Directory,
            FileType::Symlink => FuseFileType::Symlink,
        };

        FileAttr {
            ino: inode.id,
            size: inode.size,
            blocks: (inode.size + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64,
            atime: inode.atime,
            mtime: inode.mtime,
            ctime: inode.ctime,
            crtime: inode.ctime,
            kind,
            perm: (inode.mode & 0o7777) as u16,
            nlink: inode.nlink,
            uid: inode.uid,
            gid: inode.gid,
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }
}

impl Filesystem for StrataFuse {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name = name.to_string_lossy().to_string();
        debug!(parent, %name, "lookup");

        let client = self.client.lock();
        match client.lookup(parent, &name) {
            Ok(Some(inode)) => {
                let attr = self.inode_to_attr(&inode);
                reply.entry(&TTL, &attr, inode.generation);
            }
            Ok(None) => {
                reply.error(libc::ENOENT);
            }
            Err(e) => {
                error!(error = %e, "lookup failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        debug!(ino, "getattr");

        let client = self.client.lock();
        match client.getattr(ino) {
            Ok(Some(inode)) => {
                let attr = self.inode_to_attr(&inode);
                reply.attr(&TTL, &attr);
            }
            Ok(None) => {
                // For root, return default attributes
                if ino == 1 {
                    let attr = FileAttr {
                        ino: 1,
                        size: 0,
                        blocks: 0,
                        atime: SystemTime::now(),
                        mtime: SystemTime::now(),
                        ctime: SystemTime::now(),
                        crtime: SystemTime::now(),
                        kind: FuseFileType::Directory,
                        perm: 0o755,
                        nlink: 2,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        blksize: BLOCK_SIZE,
                        flags: 0,
                    };
                    reply.attr(&TTL, &attr);
                } else {
                    reply.error(libc::ENOENT);
                }
            }
            Err(e) => {
                error!(error = %e, "getattr failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        debug!(ino, ?mode, ?uid, ?gid, ?size, "setattr");

        // TODO: Implement setattr through metadata server
        // For now, just return current attributes
        let client = self.client.lock();
        match client.getattr(ino) {
            Ok(Some(inode)) => {
                let attr = self.inode_to_attr(&inode);
                reply.attr(&TTL, &attr);
            }
            Ok(None) => {
                reply.error(libc::ENOENT);
            }
            Err(e) => {
                reply.error(e.to_errno());
            }
        }
    }

    fn mknod(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let name = name.to_string_lossy().to_string();
        debug!(parent, %name, mode, "mknod");

        let client = self.client.lock();
        match client.create_file(parent, &name, mode, req.uid(), req.gid()) {
            Ok(inode_id) => {
                if let Ok(Some(inode)) = client.lookup(parent, &name) {
                    let attr = self.inode_to_attr(&inode);
                    reply.entry(&TTL, &attr, inode.generation);
                } else {
                    reply.error(libc::EIO);
                }
            }
            Err(e) => {
                error!(error = %e, "mknod failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn mkdir(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name = name.to_string_lossy().to_string();
        debug!(parent, %name, mode, "mkdir");

        let client = self.client.lock();
        match client.mkdir(parent, &name, mode, req.uid(), req.gid()) {
            Ok(_) => {
                if let Ok(Some(inode)) = client.lookup(parent, &name) {
                    let attr = self.inode_to_attr(&inode);
                    reply.entry(&TTL, &attr, inode.generation);
                } else {
                    reply.error(libc::EIO);
                }
            }
            Err(e) => {
                error!(error = %e, "mkdir failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy().to_string();
        debug!(parent, %name, "unlink");

        let client = self.client.lock();
        match client.unlink(parent, &name) {
            Ok(_) => reply.ok(),
            Err(e) => {
                error!(error = %e, "unlink failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name = name.to_string_lossy().to_string();
        debug!(parent, %name, "rmdir");

        let client = self.client.lock();
        match client.unlink(parent, &name) {
            Ok(_) => reply.ok(),
            Err(e) => {
                error!(error = %e, "rmdir failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let name = name.to_string_lossy().to_string();
        let newname = newname.to_string_lossy().to_string();
        debug!(parent, %name, newparent, %newname, "rename");

        let client = self.client.lock();
        match client.rename(parent, &name, newparent, &newname) {
            Ok(_) => reply.ok(),
            Err(e) => {
                error!(error = %e, "rename failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        debug!(ino, flags, "open");

        let mut client = self.client.lock();
        match client.open(ino, flags) {
            Ok(fh) => {
                reply.opened(fh, 0);
            }
            Err(e) => {
                error!(error = %e, "open failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        debug!(ino, fh, offset, size, "read");

        let client = self.client.lock();
        match client.read(fh, ino, offset as u64, size as usize) {
            Ok(data) => {
                reply.data(&data);
            }
            Err(e) => {
                error!(error = %e, "read failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyWrite,
    ) {
        debug!(ino, fh, offset, size = data.len(), "write");

        let client = self.client.lock();
        match client.write(fh, ino, offset as u64, data) {
            Ok(written) => {
                reply.written(written as u32);
            }
            Err(e) => {
                error!(error = %e, "write failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        debug!(ino, fh, "release");

        let mut client = self.client.lock();
        match client.release(fh) {
            Ok(_) => reply.ok(),
            Err(e) => {
                error!(error = %e, "release failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!(ino, offset, "readdir");

        // For now, return empty directory
        // TODO: Implement proper directory listing through metadata server
        if offset == 0 {
            let _ = reply.add(ino, 1, FuseFileType::Directory, ".");
            let _ = reply.add(ino, 2, FuseFileType::Directory, "..");
        }

        reply.ok();
    }

    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let name = name.to_string_lossy().to_string();
        debug!(parent, %name, mode, flags, "create");

        let mut client = self.client.lock();

        // Create the file
        match client.create_file(parent, &name, mode, req.uid(), req.gid()) {
            Ok(_) => {
                // Look up the created file
                match client.lookup(parent, &name) {
                    Ok(Some(inode)) => {
                        // Open the file
                        match client.open(inode.id, flags) {
                            Ok(fh) => {
                                let attr = self.inode_to_attr(&inode);
                                reply.created(&TTL, &attr, inode.generation, fh, 0);
                            }
                            Err(e) => {
                                error!(error = %e, "create open failed");
                                reply.error(e.to_errno());
                            }
                        }
                    }
                    Ok(None) => {
                        reply.error(libc::EIO);
                    }
                    Err(e) => {
                        error!(error = %e, "create lookup failed");
                        reply.error(e.to_errno());
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "create failed");
                reply.error(e.to_errno());
            }
        }
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: fuser::ReplyStatfs) {
        debug!("statfs");

        // Return some reasonable defaults
        reply.statfs(
            1_000_000,       // Total blocks
            500_000,         // Free blocks
            500_000,         // Available blocks
            1_000_000,       // Total inodes
            900_000,         // Free inodes
            BLOCK_SIZE,      // Block size
            255,             // Max name length
            BLOCK_SIZE,      // Fragment size
        );
    }
}
