//! Command-line interface for Strata.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Strata - A distributed file system combining POSIX compatibility with S3 access.
#[derive(Parser)]
#[command(name = "strata")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Configuration file path
    #[arg(short, long, env = "STRATA_CONFIG")]
    pub config: Option<PathBuf>,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, env = "STRATA_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    #[command(subcommand)]
    pub command: Commands,
}

/// Available commands.
#[derive(Subcommand)]
pub enum Commands {
    /// Start a Strata server node
    Server {
        /// Node ID
        #[arg(short, long, env = "STRATA_NODE_ID")]
        node_id: u64,

        /// Bind address for metadata service
        #[arg(long, default_value = "0.0.0.0:9000")]
        metadata_addr: String,

        /// Bind address for data service
        #[arg(long, default_value = "0.0.0.0:9001")]
        data_addr: String,

        /// Bind address for S3 gateway
        #[arg(long, default_value = "0.0.0.0:9002")]
        s3_addr: String,

        /// Raft peer addresses (format: id=addr,id=addr)
        #[arg(long)]
        peers: Option<String>,

        /// Data directory
        #[arg(long, default_value = "/var/lib/strata")]
        data_dir: PathBuf,
    },

    /// Mount a Strata filesystem
    #[cfg(feature = "fuse")]
    Mount {
        /// Mount point
        mount_point: PathBuf,

        /// Metadata server address
        #[arg(short, long, default_value = "127.0.0.1:9000")]
        metadata_addr: String,

        /// Run in foreground
        #[arg(short, long)]
        foreground: bool,
    },

    /// Cluster management commands
    Cluster {
        #[command(subcommand)]
        command: ClusterCommands,
    },

    /// File operations
    Fs {
        #[command(subcommand)]
        command: FsCommands,
    },

    /// Show version information
    Version,
}

/// Cluster management subcommands.
#[derive(Subcommand)]
pub enum ClusterCommands {
    /// Show cluster status
    Status {
        /// Metadata server address
        #[arg(short, long, default_value = "127.0.0.1:9000")]
        addr: String,
    },

    /// List data servers
    Servers {
        /// Metadata server address
        #[arg(short, long, default_value = "127.0.0.1:9000")]
        addr: String,
    },

    /// Show cluster health
    Health {
        /// Metadata server address
        #[arg(short, long, default_value = "127.0.0.1:9000")]
        addr: String,
    },

    /// Trigger rebalance
    Rebalance {
        /// Metadata server address
        #[arg(short, long, default_value = "127.0.0.1:9000")]
        addr: String,

        /// Dry run (show plan without executing)
        #[arg(long)]
        dry_run: bool,
    },
}

/// File system subcommands.
#[derive(Subcommand)]
pub enum FsCommands {
    /// List directory contents
    Ls {
        /// Path to list
        #[arg(default_value = "/")]
        path: String,

        /// Metadata server address
        #[arg(short, long, default_value = "127.0.0.1:9000")]
        addr: String,

        /// Long format
        #[arg(short, long)]
        long: bool,
    },

    /// Show file/directory info
    Stat {
        /// Path to stat
        path: String,

        /// Metadata server address
        #[arg(short, long, default_value = "127.0.0.1:9000")]
        addr: String,
    },

    /// Create a directory
    Mkdir {
        /// Path to create
        path: String,

        /// Metadata server address
        #[arg(short, long, default_value = "127.0.0.1:9000")]
        addr: String,

        /// Create parent directories
        #[arg(short, long)]
        parents: bool,
    },

    /// Remove a file or directory
    Rm {
        /// Path to remove
        path: String,

        /// Metadata server address
        #[arg(short, long, default_value = "127.0.0.1:9000")]
        addr: String,

        /// Remove directories recursively
        #[arg(short, long)]
        recursive: bool,
    },
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
