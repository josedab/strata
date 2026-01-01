//! Strata CLI - Command-line interface for Strata distributed filesystem

use anyhow::Result;
use clap::{Parser, Subcommand};

mod client;
mod commands;
mod config;
mod output;

use commands::{bucket, cluster, config as config_cmd, object, admin};

/// Strata CLI - Distributed filesystem management
#[derive(Parser)]
#[command(name = "strata")]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Configuration file path
    #[arg(short, long, env = "STRATA_CONFIG")]
    config: Option<String>,

    /// Server endpoint
    #[arg(short, long, env = "STRATA_ENDPOINT")]
    endpoint: Option<String>,

    /// Output format (table, json, yaml)
    #[arg(short, long, default_value = "table")]
    output: output::OutputFormat,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,

    /// Disable colored output
    #[arg(long)]
    no_color: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Bucket operations
    #[command(alias = "b")]
    Bucket(bucket::BucketArgs),

    /// Object operations
    #[command(alias = "o")]
    Object(object::ObjectArgs),

    /// Cluster management
    #[command(alias = "c")]
    Cluster(cluster::ClusterArgs),

    /// Administrative operations
    Admin(admin::AdminArgs),

    /// Configuration management
    Config(config_cmd::ConfigArgs),

    /// Generate shell completions
    Completion {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize console
    if cli.no_color {
        console::set_colors_enabled(false);
    }

    // Load configuration
    let cfg = config::Config::load(cli.config.as_deref())?;
    let endpoint = cli.endpoint.unwrap_or(cfg.endpoint.clone());

    // Create client
    let client = client::StrataClient::new(&endpoint, &cfg)?;

    // Execute command
    let result = match cli.command {
        Commands::Bucket(args) => bucket::execute(args, &client, cli.output).await,
        Commands::Object(args) => object::execute(args, &client, cli.output).await,
        Commands::Cluster(args) => cluster::execute(args, &client, cli.output).await,
        Commands::Admin(args) => admin::execute(args, &client, cli.output).await,
        Commands::Config(args) => config_cmd::execute(args, cli.output).await,
        Commands::Completion { shell } => {
            let mut cmd = <Cli as clap::CommandFactory>::command();
            clap_complete::generate(shell, &mut cmd, "strata", &mut std::io::stdout());
            Ok(())
        }
    };

    if let Err(e) = result {
        if cli.verbose {
            eprintln!("{:?}", e);
        } else {
            eprintln!("Error: {}", e);
        }
        std::process::exit(1);
    }

    Ok(())
}
