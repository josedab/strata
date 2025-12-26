//! FUSE client for POSIX file system access.
//!
//! This module provides a FUSE (Filesystem in Userspace) interface to Strata,
//! allowing standard POSIX file operations on the distributed file system.

#[cfg(feature = "fuse")]
mod filesystem;
#[cfg(feature = "fuse")]
mod client;

#[cfg(feature = "fuse")]
pub use filesystem::StrataFuse;
#[cfg(feature = "fuse")]
pub use client::StrataClient;

use crate::error::Result;
use std::path::Path;

/// Mount a Strata file system at the given mount point.
#[cfg(feature = "fuse")]
pub fn mount<P: AsRef<Path>>(
    mount_point: P,
    metadata_addr: &str,
) -> Result<()> {
    use fuser::MountOption;
    use tracing::info;

    let client = StrataClient::connect(metadata_addr)?;
    let fs = StrataFuse::new(client);

    let options = vec![
        MountOption::FSName("strata".to_string()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
        MountOption::DefaultPermissions,
    ];

    info!(mount_point = %mount_point.as_ref().display(), "Mounting Strata filesystem");

    fuser::mount2(fs, mount_point, &options)
        .map_err(|e| crate::StrataError::Internal(format!("Mount failed: {}", e)))
}
