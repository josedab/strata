//! Object operations

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use serde::Serialize;
use std::path::PathBuf;
use tabled::Tabled;
use tokio::io::AsyncWriteExt;

use crate::client::{ListObjectsResponse, ObjectInfo, StrataClient};
use crate::output::{self, OutputFormat};

#[derive(Args)]
pub struct ObjectArgs {
    #[command(subcommand)]
    command: ObjectCommands,
}

#[derive(Subcommand)]
enum ObjectCommands {
    /// List objects in a bucket
    #[command(alias = "ls")]
    List {
        /// Bucket name
        bucket: String,

        /// Prefix filter
        #[arg(short, long)]
        prefix: Option<String>,

        /// Maximum number of results
        #[arg(short, long, default_value = "100")]
        max_keys: u32,

        /// Delimiter for grouping
        #[arg(short, long)]
        delimiter: Option<String>,

        /// Show all objects recursively
        #[arg(short, long)]
        recursive: bool,
    },

    /// Upload an object
    #[command(alias = "put")]
    Upload {
        /// Source file path
        source: PathBuf,

        /// Destination (bucket/key)
        destination: String,

        /// Content type
        #[arg(short, long)]
        content_type: Option<String>,

        /// Storage class
        #[arg(long)]
        storage_class: Option<String>,
    },

    /// Download an object
    #[command(alias = "get")]
    Download {
        /// Source (bucket/key)
        source: String,

        /// Destination file path
        destination: PathBuf,
    },

    /// Delete an object
    #[command(alias = "rm")]
    Delete {
        /// Object path (bucket/key)
        path: String,

        /// Delete all objects with prefix
        #[arg(short, long)]
        recursive: bool,

        /// Skip confirmation
        #[arg(short, long)]
        force: bool,
    },

    /// Copy an object
    #[command(alias = "cp")]
    Copy {
        /// Source (bucket/key)
        source: String,

        /// Destination (bucket/key)
        destination: String,
    },

    /// Move an object
    #[command(alias = "mv")]
    Move {
        /// Source (bucket/key)
        source: String,

        /// Destination (bucket/key)
        destination: String,
    },

    /// Get object metadata
    Head {
        /// Object path (bucket/key)
        path: String,
    },

    /// Generate presigned URL
    Presign {
        /// Object path (bucket/key)
        path: String,

        /// URL expiration in seconds
        #[arg(short, long, default_value = "3600")]
        expires: u64,

        /// HTTP method (GET or PUT)
        #[arg(short, long, default_value = "GET")]
        method: String,
    },
}

#[derive(Debug, Serialize, Tabled)]
struct ObjectRow {
    #[tabled(rename = "Key")]
    key: String,
    #[tabled(rename = "Size")]
    size: String,
    #[tabled(rename = "Type")]
    content_type: String,
    #[tabled(rename = "Storage")]
    storage_class: String,
    #[tabled(rename = "Modified")]
    last_modified: String,
}

impl From<ObjectInfo> for ObjectRow {
    fn from(o: ObjectInfo) -> Self {
        Self {
            key: o.key,
            size: output::format_bytes(o.size),
            content_type: o.content_type,
            storage_class: o.storage_class,
            last_modified: o.last_modified.format("%Y-%m-%d %H:%M").to_string(),
        }
    }
}

fn parse_path(path: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = path.splitn(2, '/').collect();
    if parts.len() != 2 {
        anyhow::bail!("Invalid path format. Use: bucket/key");
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

fn detect_content_type(path: &PathBuf) -> String {
    let ext = path.extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    match ext.to_lowercase().as_str() {
        "txt" => "text/plain",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" => "application/javascript",
        "json" => "application/json",
        "xml" => "application/xml",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "tar" => "application/x-tar",
        "gz" => "application/gzip",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "mp3" => "audio/mpeg",
        "mp4" => "video/mp4",
        _ => "application/octet-stream",
    }
    .to_string()
}

pub async fn execute(args: ObjectArgs, client: &StrataClient, format: OutputFormat) -> Result<()> {
    match args.command {
        ObjectCommands::List {
            bucket,
            prefix,
            max_keys,
            delimiter,
            recursive,
        } => {
            #[derive(Serialize)]
            struct ListQuery {
                prefix: Option<String>,
                delimiter: Option<String>,
                max_keys: u32,
            }

            let query = ListQuery {
                prefix,
                delimiter: if recursive { None } else { delimiter.or(Some("/".to_string())) },
                max_keys,
            };

            let response: ListObjectsResponse = client
                .get_with_query(&format!("/buckets/{}/objects", bucket), &query)
                .await?;

            // Show common prefixes (directories)
            if !response.common_prefixes.is_empty() {
                for prefix in &response.common_prefixes {
                    println!("📁 {}", prefix);
                }
            }

            let rows: Vec<ObjectRow> = response.objects.into_iter().map(Into::into).collect();
            output::print_output(&rows, format);

            if response.is_truncated {
                output::warning("Results truncated. Use --max-keys to retrieve more.");
            }
        }

        ObjectCommands::Upload {
            source,
            destination,
            content_type,
            storage_class: _,
        } => {
            let (bucket, key) = parse_path(&destination)?;

            // Read file
            let data = tokio::fs::read(&source)
                .await
                .with_context(|| format!("Failed to read file: {:?}", source))?;

            let file_size = data.len() as u64;
            let content_type = content_type.unwrap_or_else(|| detect_content_type(&source));

            let spinner = output::create_spinner(format!("Uploading {} ({})", source.display(), output::format_bytes(file_size)));

            let response = client
                .put_bytes(
                    &format!("/buckets/{}/objects/{}", bucket, key),
                    data,
                    &content_type,
                )
                .await?;

            spinner.finish_and_clear();
            output::success(format!(
                "Uploaded {} to {}/{} (ETag: {})",
                source.display(),
                bucket,
                key,
                response.etag
            ));
        }

        ObjectCommands::Download { source, destination } => {
            let (bucket, key) = parse_path(&source)?;

            let spinner = output::create_spinner(format!("Downloading {}", source));

            let data = client
                .download(&format!("/buckets/{}/objects/{}", bucket, key))
                .await?;

            let mut file = tokio::fs::File::create(&destination)
                .await
                .with_context(|| format!("Failed to create file: {:?}", destination))?;

            file.write_all(&data).await?;

            spinner.finish_and_clear();
            output::success(format!(
                "Downloaded to {} ({})",
                destination.display(),
                output::format_bytes(data.len() as u64)
            ));
        }

        ObjectCommands::Delete {
            path,
            recursive,
            force,
        } => {
            let (bucket, key) = parse_path(&path)?;

            if recursive {
                if !force && !output::confirm(&format!("Delete all objects with prefix '{}'?", key)) {
                    output::info("Cancelled");
                    return Ok(());
                }

                // List and delete all matching objects
                #[derive(Serialize)]
                struct ListQuery {
                    prefix: String,
                }

                let response: ListObjectsResponse = client
                    .get_with_query(
                        &format!("/buckets/{}/objects", bucket),
                        &ListQuery { prefix: key.clone() },
                    )
                    .await?;

                let count = response.objects.len();
                for obj in response.objects {
                    client
                        .delete(&format!("/buckets/{}/objects/{}", bucket, obj.key))
                        .await?;
                }

                output::success(format!("Deleted {} objects", count));
            } else {
                if !force && !output::confirm(&format!("Delete '{}'?", path)) {
                    output::info("Cancelled");
                    return Ok(());
                }

                client
                    .delete(&format!("/buckets/{}/objects/{}", bucket, key))
                    .await?;
                output::success(format!("Deleted {}", path));
            }
        }

        ObjectCommands::Copy { source, destination } => {
            let (src_bucket, src_key) = parse_path(&source)?;
            let (dst_bucket, dst_key) = parse_path(&destination)?;

            #[derive(Serialize)]
            struct CopyRequest {
                source_bucket: String,
                source_key: String,
            }

            client
                .post_empty(
                    &format!("/buckets/{}/objects/{}", dst_bucket, dst_key),
                    &CopyRequest {
                        source_bucket: src_bucket,
                        source_key: src_key,
                    },
                )
                .await?;

            output::success(format!("Copied {} to {}", source, destination));
        }

        ObjectCommands::Move { source, destination } => {
            let (src_bucket, src_key) = parse_path(&source)?;
            let (dst_bucket, dst_key) = parse_path(&destination)?;

            // Copy then delete
            #[derive(Serialize)]
            struct CopyRequest {
                source_bucket: String,
                source_key: String,
            }

            client
                .post_empty(
                    &format!("/buckets/{}/objects/{}", dst_bucket, dst_key),
                    &CopyRequest {
                        source_bucket: src_bucket.clone(),
                        source_key: src_key.clone(),
                    },
                )
                .await?;

            client
                .delete(&format!("/buckets/{}/objects/{}", src_bucket, src_key))
                .await?;

            output::success(format!("Moved {} to {}", source, destination));
        }

        ObjectCommands::Head { path } => {
            let (bucket, key) = parse_path(&path)?;

            let obj: ObjectInfo = client
                .get(&format!("/buckets/{}/objects/{}/metadata", bucket, key))
                .await?;

            output::print_header("Object Metadata");
            output::print_kv("Key", &obj.key);
            output::print_kv("Size", output::format_bytes(obj.size));
            output::print_kv("Content-Type", &obj.content_type);
            output::print_kv("ETag", &obj.etag);
            output::print_kv("Last Modified", obj.last_modified.to_rfc3339());
            output::print_kv("Storage Class", &obj.storage_class);
            if let Some(version) = obj.version_id {
                output::print_kv("Version ID", &version);
            }
        }

        ObjectCommands::Presign {
            path,
            expires,
            method,
        } => {
            let (bucket, key) = parse_path(&path)?;

            #[derive(Serialize)]
            struct PresignQuery {
                expires: u64,
                method: String,
            }

            #[derive(serde::Deserialize)]
            struct PresignResponse {
                url: String,
            }

            let response: PresignResponse = client
                .get_with_query(
                    &format!("/buckets/{}/objects/{}/presign", bucket, key),
                    &PresignQuery { expires, method },
                )
                .await?;

            println!("{}", response.url);
        }
    }

    Ok(())
}
