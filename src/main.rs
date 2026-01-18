//! Strata CLI - Main entry point.

use strata::cli::{Cli, ClusterCommands, Commands, FsCommands};
use strata::client::MetadataClient;
use strata::config::StrataConfig;
use strata::types::{FileType, InodeId};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse_args();

    match cli.command {
        Commands::Server {
            node_id,
            metadata_addr,
            data_addr,
            s3_addr,
            peers,
            data_dir,
        } => {
            // Build configuration from CLI args
            let mut config = StrataConfig::development();
            config.node.id = node_id;
            config.metadata.bind_addr = metadata_addr.parse()?;
            config.data.bind_addr = data_addr.parse()?;
            config.s3.bind_addr = s3_addr.parse()?;
            config.storage.data_dir = data_dir.join("data");
            config.storage.metadata_dir = data_dir.join("metadata");

            if let Some(peers_str) = peers {
                config.metadata.raft_peers = peers_str
                    .split(',')
                    .map(|s| s.to_string())
                    .collect();
            }

            config.observability.log_level = cli.log_level;

            // Run the server
            strata::run(config).await?;
        }

        #[cfg(feature = "fuse")]
        Commands::Mount {
            mount_point,
            metadata_addr,
            foreground,
        } => {
            println!("Mounting Strata filesystem at {:?}", mount_point);
            println!("Connecting to metadata server: {}", metadata_addr);

            if foreground {
                strata::fuse::mount(&mount_point, &metadata_addr)?;
            } else {
                // Daemonize the process before mounting
                let pid_file = format!("/var/run/strata-fuse-{}.pid", std::process::id());
                let log_dir = std::env::var("STRATA_LOG_DIR")
                    .unwrap_or_else(|_| "/var/log/strata".to_string());

                let config = strata::daemon::DaemonConfig::new()
                    .working_directory("/")
                    .pid_file(&pid_file)
                    .stdout(format!("{}/fuse-stdout.log", log_dir))
                    .stderr(format!("{}/fuse-stderr.log", log_dir));

                // Attempt to create log directory if it doesn't exist
                let _ = std::fs::create_dir_all(&log_dir);

                if let Err(e) = strata::daemon::daemonize(&config) {
                    eprintln!("Failed to daemonize: {}. Running in foreground.", e);
                }

                strata::fuse::mount(&mount_point, &metadata_addr)?;
            }
        }

        Commands::Cluster { command } => match command {
            ClusterCommands::Status { addr } => {
                let client = MetadataClient::from_addr(&addr)?;
                match client.health().await {
                    Ok(healthy) => {
                        println!("Cluster status ({})", addr);
                        println!("Status: {}", if healthy { "Healthy" } else { "Unhealthy" });
                        // Get root directory to show some stats
                        if let Ok(response) = client.readdir(1).await {
                            println!("Root entries: {}", response.entries.len());
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to connect to cluster: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            ClusterCommands::Servers { addr } => {
                let client = MetadataClient::from_addr(&addr)?;
                match client.health().await {
                    Ok(_) => {
                        println!("Data servers ({})", addr);
                        println!("Server status requires direct data server connection");
                    }
                    Err(e) => {
                        eprintln!("Failed to connect: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            ClusterCommands::Health { addr } => {
                let client = MetadataClient::from_addr(&addr)?;
                match client.health().await {
                    Ok(healthy) => {
                        println!("Health: {}", if healthy { "OK" } else { "DEGRADED" });
                    }
                    Err(e) => {
                        eprintln!("Health: UNREACHABLE ({})", e);
                        std::process::exit(1);
                    }
                }
            }
            ClusterCommands::Rebalance { addr, dry_run } => {
                let client = MetadataClient::from_addr(&addr)?;
                match client.health().await {
                    Ok(_) => {
                        if dry_run {
                            println!("Rebalance dry run ({})", addr);
                        } else {
                            println!("Triggering rebalance ({})", addr);
                        }
                        println!("Rebalance not yet implemented in server");
                    }
                    Err(e) => {
                        eprintln!("Failed to connect: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        },

        Commands::Fs { command } => match command {
            FsCommands::Ls { path, addr, long } => {
                let client = MetadataClient::from_addr(&addr)?;
                let inode = resolve_path(&client, &path).await?;

                match client.readdir(inode).await {
                    Ok(response) => {
                        if !response.success {
                            eprintln!("Error: {}", response.error.unwrap_or_default());
                            std::process::exit(1);
                        }

                        for entry in &response.entries {
                            if long {
                                let type_char = match entry.file_type {
                                    FileType::Directory => 'd',
                                    FileType::RegularFile => '-',
                                    FileType::Symlink => 'l',
                                };
                                println!("{}rwxr-xr-x  {:>8}  {}", type_char, entry.inode, entry.name);
                            } else {
                                println!("{}", entry.name);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error listing directory: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            FsCommands::Stat { path, addr } => {
                let client = MetadataClient::from_addr(&addr)?;
                let inode = resolve_path(&client, &path).await?;

                match client.getattr(inode).await {
                    Ok(Some(attrs)) => {
                        let type_str = match attrs.file_type {
                            FileType::Directory => "directory",
                            FileType::RegularFile => "regular file",
                            FileType::Symlink => "symbolic link",
                        };
                        println!("  Path: {}", path);
                        println!("  Inode: {}", attrs.id);
                        println!("  Type: {}", type_str);
                        println!("  Mode: {:04o}", attrs.mode & 0o7777);
                        println!("  Size: {}", attrs.size);
                        println!("  Links: {}", attrs.nlink);
                        println!("  UID: {}", attrs.uid);
                        println!("  GID: {}", attrs.gid);
                    }
                    Ok(None) => {
                        eprintln!("Path not found: {}", path);
                        std::process::exit(1);
                    }
                    Err(e) => {
                        eprintln!("Error getting attributes: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            FsCommands::Mkdir { path, addr, parents: _ } => {
                let client = MetadataClient::from_addr(&addr)?;
                let (parent_path, name) = split_path(&path);
                let parent_inode = resolve_path(&client, parent_path).await?;

                match client.create_directory(parent_inode, name, 0o755, 0, 0).await {
                    Ok(response) => {
                        if response.success {
                            println!("Created directory: {}", path);
                        } else {
                            eprintln!("Error: {}", response.error.unwrap_or_default());
                            std::process::exit(1);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error creating directory: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            FsCommands::Rm { path, addr, recursive: _ } => {
                let client = MetadataClient::from_addr(&addr)?;
                let (parent_path, name) = split_path(&path);
                let parent_inode = resolve_path(&client, parent_path).await?;

                match client.delete(parent_inode, name).await {
                    Ok(response) => {
                        if response.success {
                            println!("Removed: {}", path);
                        } else {
                            eprintln!("Error: {}", response.error.unwrap_or_default());
                            std::process::exit(1);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error removing: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        },

        Commands::Version => {
            println!("Strata v{}", env!("CARGO_PKG_VERSION"));
            println!("A distributed file system combining POSIX compatibility with S3 access");
        }
    }

    Ok(())
}

/// Root inode ID.
const ROOT_INODE: InodeId = 1;

/// Resolve a path to an inode ID.
async fn resolve_path(client: &MetadataClient, path: &str) -> anyhow::Result<InodeId> {
    let path = path.trim_start_matches('/');
    if path.is_empty() || path == "." {
        return Ok(ROOT_INODE);
    }

    let mut current_inode = ROOT_INODE;
    for component in path.split('/').filter(|s| !s.is_empty()) {
        match client.lookup(current_inode, component).await {
            Ok(Some(inode)) => {
                current_inode = inode.id;
            }
            Ok(None) => {
                anyhow::bail!("Path not found: {}", component);
            }
            Err(e) => {
                anyhow::bail!("Error looking up path: {}", e);
            }
        }
    }

    Ok(current_inode)
}

/// Split a path into parent path and name.
fn split_path(path: &str) -> (&str, &str) {
    let path = path.trim_end_matches('/');
    if let Some(pos) = path.rfind('/') {
        let parent = if pos == 0 { "/" } else { &path[..pos] };
        let name = &path[pos + 1..];
        (parent, name)
    } else {
        ("/", path)
    }
}
