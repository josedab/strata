//! Bucket operations

use anyhow::Result;
use clap::{Args, Subcommand};
use serde::Serialize;
use tabled::Tabled;

use crate::client::{BucketInfo, StrataClient};
use crate::output::{self, OutputFormat};

#[derive(Args)]
pub struct BucketArgs {
    #[command(subcommand)]
    command: BucketCommands,
}

#[derive(Subcommand)]
enum BucketCommands {
    /// List all buckets
    #[command(alias = "ls")]
    List,

    /// Create a new bucket
    Create {
        /// Bucket name
        name: String,

        /// Region
        #[arg(short, long)]
        region: Option<String>,

        /// Enable versioning
        #[arg(long)]
        versioning: bool,

        /// Enable encryption
        #[arg(long, default_value = "true")]
        encryption: bool,
    },

    /// Delete a bucket
    #[command(alias = "rm")]
    Delete {
        /// Bucket name
        name: String,

        /// Force delete (remove all objects first)
        #[arg(short, long)]
        force: bool,
    },

    /// Get bucket information
    Info {
        /// Bucket name
        name: String,
    },

    /// Get bucket metrics
    Metrics {
        /// Bucket name
        name: String,
    },

    /// Configure bucket settings
    Configure {
        /// Bucket name
        name: String,

        /// Enable versioning
        #[arg(long)]
        versioning: Option<bool>,

        /// Enable encryption
        #[arg(long)]
        encryption: Option<bool>,
    },
}

#[derive(Debug, Serialize, Tabled)]
struct BucketRow {
    #[tabled(rename = "Name")]
    name: String,
    #[tabled(rename = "Region")]
    region: String,
    #[tabled(rename = "Objects")]
    objects: String,
    #[tabled(rename = "Size")]
    size: String,
    #[tabled(rename = "Versioning")]
    versioning: String,
    #[tabled(rename = "Encryption")]
    encryption: String,
    #[tabled(rename = "Created")]
    created: String,
}

impl From<BucketInfo> for BucketRow {
    fn from(b: BucketInfo) -> Self {
        Self {
            name: b.name,
            region: b.region.unwrap_or_else(|| "-".to_string()),
            objects: format_number(b.object_count),
            size: output::format_bytes(b.total_size),
            versioning: if b.versioning { "✓" } else { "-" }.to_string(),
            encryption: if b.encryption { "✓" } else { "-" }.to_string(),
            created: b.created_at.format("%Y-%m-%d").to_string(),
        }
    }
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

pub async fn execute(args: BucketArgs, client: &StrataClient, format: OutputFormat) -> Result<()> {
    match args.command {
        BucketCommands::List => {
            let buckets: Vec<BucketInfo> = client.get("/buckets").await?;
            let rows: Vec<BucketRow> = buckets.into_iter().map(Into::into).collect();
            output::print_output(&rows, format);
        }

        BucketCommands::Create {
            name,
            region,
            versioning,
            encryption,
        } => {
            #[derive(Serialize)]
            struct CreateBucket {
                name: String,
                region: Option<String>,
                versioning: bool,
                encryption: bool,
            }

            let body = CreateBucket {
                name: name.clone(),
                region,
                versioning,
                encryption,
            };

            let _: BucketInfo = client.post("/buckets", &body).await?;
            output::success(format!("Bucket '{}' created successfully", name));
        }

        BucketCommands::Delete { name, force } => {
            if !force {
                if !output::confirm(&format!("Delete bucket '{}'?", name)) {
                    output::info("Cancelled");
                    return Ok(());
                }
            }

            let path = if force {
                format!("/buckets/{}?force=true", name)
            } else {
                format!("/buckets/{}", name)
            };

            client.delete(&path).await?;
            output::success(format!("Bucket '{}' deleted", name));
        }

        BucketCommands::Info { name } => {
            let bucket: BucketInfo = client.get(&format!("/buckets/{}", name)).await?;

            output::print_header("Bucket Information");
            output::print_kv("Name", &bucket.name);
            output::print_kv("Region", bucket.region.as_deref().unwrap_or("-"));
            output::print_kv("Created", bucket.created_at.to_rfc3339());
            output::print_kv("Objects", format_number(bucket.object_count));
            output::print_kv("Size", output::format_bytes(bucket.total_size));
            output::print_kv("Versioning", if bucket.versioning { "Enabled" } else { "Disabled" });
            output::print_kv("Encryption", if bucket.encryption { "Enabled" } else { "Disabled" });
        }

        BucketCommands::Metrics { name } => {
            #[derive(Debug, serde::Deserialize, Serialize)]
            struct BucketMetrics {
                object_count: u64,
                total_size_bytes: u64,
                requests_last_hour: u64,
                bandwidth_last_hour_bytes: u64,
                get_requests: u64,
                put_requests: u64,
                delete_requests: u64,
            }

            let metrics: BucketMetrics = client.get(&format!("/buckets/{}/metrics", name)).await?;

            output::print_header(&format!("Metrics for '{}'", name));
            output::print_kv("Objects", format_number(metrics.object_count));
            output::print_kv("Total Size", output::format_bytes(metrics.total_size_bytes));
            output::print_kv("Requests (last hour)", format_number(metrics.requests_last_hour));
            output::print_kv("Bandwidth (last hour)", output::format_bytes(metrics.bandwidth_last_hour_bytes));
            output::print_kv("GET Requests", format_number(metrics.get_requests));
            output::print_kv("PUT Requests", format_number(metrics.put_requests));
            output::print_kv("DELETE Requests", format_number(metrics.delete_requests));
        }

        BucketCommands::Configure {
            name,
            versioning,
            encryption,
        } => {
            #[derive(Serialize)]
            struct UpdateBucket {
                versioning: Option<bool>,
                encryption: Option<bool>,
            }

            let body = UpdateBucket {
                versioning,
                encryption,
            };

            let _: BucketInfo = client.put(&format!("/buckets/{}", name), &body).await?;
            output::success(format!("Bucket '{}' configuration updated", name));
        }
    }

    Ok(())
}
