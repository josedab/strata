//! Administrative operations

use anyhow::Result;
use clap::{Args, Subcommand};
use serde::Serialize;
use tabled::Tabled;

use crate::client::StrataClient;
use crate::output::{self, OutputFormat};

#[derive(Args)]
pub struct AdminArgs {
    #[command(subcommand)]
    command: AdminCommands,
}

#[derive(Subcommand)]
enum AdminCommands {
    /// User management
    User {
        #[command(subcommand)]
        command: UserCommands,
    },

    /// Access key management
    Keys {
        #[command(subcommand)]
        command: KeyCommands,
    },

    /// Backup operations
    Backup {
        #[command(subcommand)]
        command: BackupCommands,
    },

    /// Maintenance operations
    Maintenance {
        #[command(subcommand)]
        command: MaintenanceCommands,
    },

    /// Audit log access
    Audit {
        /// Number of entries to show
        #[arg(short, long, default_value = "50")]
        limit: u32,

        /// Filter by action
        #[arg(short, long)]
        action: Option<String>,

        /// Filter by resource
        #[arg(short, long)]
        resource: Option<String>,
    },
}

#[derive(Subcommand)]
enum UserCommands {
    /// List users
    List,

    /// Create a user
    Create {
        /// Username
        username: String,

        /// User role (admin, operator, viewer)
        #[arg(short, long, default_value = "viewer")]
        role: String,

        /// Email address
        #[arg(short, long)]
        email: Option<String>,
    },

    /// Delete a user
    Delete {
        /// Username
        username: String,
    },

    /// Update user role
    SetRole {
        /// Username
        username: String,

        /// New role
        role: String,
    },
}

#[derive(Subcommand)]
enum KeyCommands {
    /// List access keys
    List,

    /// Create access key
    Create {
        /// Description
        #[arg(short, long)]
        description: Option<String>,
    },

    /// Delete access key
    Delete {
        /// Access key ID
        key_id: String,
    },

    /// Rotate access key
    Rotate {
        /// Access key ID
        key_id: String,
    },
}

#[derive(Subcommand)]
enum BackupCommands {
    /// List backups
    List,

    /// Create a backup
    Create {
        /// Backup name
        name: String,

        /// Include all buckets
        #[arg(long)]
        all_buckets: bool,

        /// Specific buckets to backup
        #[arg(short, long)]
        buckets: Vec<String>,
    },

    /// Restore from backup
    Restore {
        /// Backup ID
        backup_id: String,

        /// Target bucket (optional)
        #[arg(short, long)]
        target: Option<String>,
    },

    /// Delete a backup
    Delete {
        /// Backup ID
        backup_id: String,
    },
}

#[derive(Subcommand)]
enum MaintenanceCommands {
    /// Run garbage collection
    Gc {
        /// Dry run mode
        #[arg(long)]
        dry_run: bool,
    },

    /// Run data scrubbing
    Scrub {
        /// Bucket to scrub (all if not specified)
        #[arg(short, long)]
        bucket: Option<String>,
    },

    /// Rebalance cluster
    Rebalance {
        /// Dry run mode
        #[arg(long)]
        dry_run: bool,
    },

    /// Compact metadata
    Compact,
}

#[derive(Debug, Serialize, Tabled, serde::Deserialize)]
struct User {
    #[tabled(rename = "Username")]
    username: String,
    #[tabled(rename = "Role")]
    role: String,
    #[tabled(rename = "Email")]
    email: String,
    #[tabled(rename = "Created")]
    created_at: String,
}

#[derive(Debug, Serialize, Tabled, serde::Deserialize)]
struct AccessKey {
    #[tabled(rename = "Key ID")]
    key_id: String,
    #[tabled(rename = "Description")]
    description: String,
    #[tabled(rename = "Created")]
    created_at: String,
    #[tabled(rename = "Last Used")]
    last_used: String,
}

#[derive(Debug, Serialize, Tabled, serde::Deserialize)]
struct Backup {
    #[tabled(rename = "ID")]
    id: String,
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Size")]
    size: String,
    #[tabled(rename = "Buckets")]
    bucket_count: u32,
    #[tabled(rename = "Created")]
    created_at: String,
    #[tabled(rename = "Status")]
    status: String,
}

#[derive(Debug, Serialize, Tabled, serde::Deserialize)]
struct AuditEntry {
    #[tabled(rename = "Timestamp")]
    timestamp: String,
    #[tabled(rename = "Action")]
    action: String,
    #[tabled(rename = "Resource")]
    resource: String,
    #[tabled(rename = "Actor")]
    actor: String,
    #[tabled(rename = "Outcome")]
    outcome: String,
}

pub async fn execute(args: AdminArgs, client: &StrataClient, format: OutputFormat) -> Result<()> {
    match args.command {
        AdminCommands::User { command } => match command {
            UserCommands::List => {
                let users: Vec<User> = client.get("/admin/users").await?;
                output::print_output(&users, format);
            }

            UserCommands::Create { username, role, email } => {
                #[derive(Serialize)]
                struct CreateUser {
                    username: String,
                    role: String,
                    email: Option<String>,
                }

                let _: User = client
                    .post(
                        "/admin/users",
                        &CreateUser { username: username.clone(), role, email },
                    )
                    .await?;

                output::success(format!("User '{}' created", username));
            }

            UserCommands::Delete { username } => {
                if !output::confirm(&format!("Delete user '{}'?", username)) {
                    output::info("Cancelled");
                    return Ok(());
                }

                client.delete(&format!("/admin/users/{}", username)).await?;
                output::success(format!("User '{}' deleted", username));
            }

            UserCommands::SetRole { username, role } => {
                #[derive(Serialize)]
                struct UpdateRole {
                    role: String,
                }

                let _: User = client
                    .put(&format!("/admin/users/{}", username), &UpdateRole { role: role.clone() })
                    .await?;

                output::success(format!("User '{}' role updated to '{}'", username, role));
            }
        },

        AdminCommands::Keys { command } => match command {
            KeyCommands::List => {
                let keys: Vec<AccessKey> = client.get("/admin/keys").await?;
                output::print_output(&keys, format);
            }

            KeyCommands::Create { description } => {
                #[derive(Serialize)]
                struct CreateKey {
                    description: Option<String>,
                }

                #[derive(serde::Deserialize)]
                struct NewKey {
                    access_key: String,
                    secret_key: String,
                }

                let key: NewKey = client
                    .post("/admin/keys", &CreateKey { description })
                    .await?;

                output::success("Access key created");
                output::print_header("Credentials (save these - secret key won't be shown again)");
                output::print_kv("Access Key", &key.access_key);
                output::print_kv("Secret Key", &key.secret_key);
            }

            KeyCommands::Delete { key_id } => {
                client.delete(&format!("/admin/keys/{}", key_id)).await?;
                output::success(format!("Key '{}' deleted", key_id));
            }

            KeyCommands::Rotate { key_id } => {
                #[derive(serde::Deserialize)]
                struct RotatedKey {
                    access_key: String,
                    secret_key: String,
                }

                let key: RotatedKey = client
                    .post(&format!("/admin/keys/{}/rotate", key_id), &())
                    .await?;

                output::success("Access key rotated");
                output::print_header("New Credentials");
                output::print_kv("Access Key", &key.access_key);
                output::print_kv("Secret Key", &key.secret_key);
            }
        },

        AdminCommands::Backup { command } => match command {
            BackupCommands::List => {
                let backups: Vec<Backup> = client.get("/admin/backups").await?;
                output::print_output(&backups, format);
            }

            BackupCommands::Create { name, all_buckets, buckets } => {
                #[derive(Serialize)]
                struct CreateBackup {
                    name: String,
                    all_buckets: bool,
                    buckets: Vec<String>,
                }

                let spinner = output::create_spinner("Creating backup...");

                let backup: Backup = client
                    .post(
                        "/admin/backups",
                        &CreateBackup { name: name.clone(), all_buckets, buckets },
                    )
                    .await?;

                spinner.finish_and_clear();
                output::success(format!("Backup '{}' created (ID: {})", name, backup.id));
            }

            BackupCommands::Restore { backup_id, target } => {
                #[derive(Serialize)]
                struct RestoreBackup {
                    target_bucket: Option<String>,
                }

                let spinner = output::create_spinner("Restoring backup...");

                client
                    .post_empty(
                        &format!("/admin/backups/{}/restore", backup_id),
                        &RestoreBackup { target_bucket: target },
                    )
                    .await?;

                spinner.finish_and_clear();
                output::success("Backup restored successfully");
            }

            BackupCommands::Delete { backup_id } => {
                client.delete(&format!("/admin/backups/{}", backup_id)).await?;
                output::success(format!("Backup '{}' deleted", backup_id));
            }
        },

        AdminCommands::Maintenance { command } => match command {
            MaintenanceCommands::Gc { dry_run } => {
                #[derive(Serialize)]
                struct GcRequest {
                    dry_run: bool,
                }

                #[derive(serde::Deserialize)]
                struct GcResult {
                    objects_deleted: u64,
                    bytes_reclaimed: u64,
                }

                let spinner = output::create_spinner("Running garbage collection...");

                let result: GcResult = client
                    .post("/admin/maintenance/gc", &GcRequest { dry_run })
                    .await?;

                spinner.finish_and_clear();

                if dry_run {
                    output::info(format!(
                        "Dry run: would delete {} objects ({} reclaimable)",
                        result.objects_deleted,
                        output::format_bytes(result.bytes_reclaimed)
                    ));
                } else {
                    output::success(format!(
                        "GC complete: {} objects deleted, {} reclaimed",
                        result.objects_deleted,
                        output::format_bytes(result.bytes_reclaimed)
                    ));
                }
            }

            MaintenanceCommands::Scrub { bucket } => {
                #[derive(Serialize)]
                struct ScrubRequest {
                    bucket: Option<String>,
                }

                let spinner = output::create_spinner("Running data scrub...");

                client
                    .post_empty("/admin/maintenance/scrub", &ScrubRequest { bucket })
                    .await?;

                spinner.finish_and_clear();
                output::success("Data scrub completed");
            }

            MaintenanceCommands::Rebalance { dry_run } => {
                #[derive(Serialize)]
                struct RebalanceRequest {
                    dry_run: bool,
                }

                #[derive(serde::Deserialize)]
                struct RebalanceResult {
                    chunks_moved: u64,
                    bytes_moved: u64,
                }

                let spinner = output::create_spinner("Rebalancing cluster...");

                let result: RebalanceResult = client
                    .post("/admin/maintenance/rebalance", &RebalanceRequest { dry_run })
                    .await?;

                spinner.finish_and_clear();

                if dry_run {
                    output::info(format!(
                        "Dry run: would move {} chunks ({})",
                        result.chunks_moved,
                        output::format_bytes(result.bytes_moved)
                    ));
                } else {
                    output::success(format!(
                        "Rebalance complete: {} chunks moved ({})",
                        result.chunks_moved,
                        output::format_bytes(result.bytes_moved)
                    ));
                }
            }

            MaintenanceCommands::Compact => {
                let spinner = output::create_spinner("Compacting metadata...");
                client.post_empty("/admin/maintenance/compact", &()).await?;
                spinner.finish_and_clear();
                output::success("Metadata compaction completed");
            }
        },

        AdminCommands::Audit { limit, action, resource } => {
            #[derive(Serialize)]
            struct AuditQuery {
                limit: u32,
                action: Option<String>,
                resource: Option<String>,
            }

            let entries: Vec<AuditEntry> = client
                .get_with_query("/admin/audit", &AuditQuery { limit, action, resource })
                .await?;

            output::print_output(&entries, format);
        }
    }

    Ok(())
}
