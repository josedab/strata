//! Configuration management commands

use anyhow::Result;
use clap::{Args, Subcommand};

use crate::config::{Config, Profiles};
use crate::output::{self, OutputFormat};

#[derive(Args)]
pub struct ConfigArgs {
    #[command(subcommand)]
    command: ConfigCommands,
}

#[derive(Subcommand)]
enum ConfigCommands {
    /// Show current configuration
    Show,

    /// Initialize configuration
    Init {
        /// Server endpoint
        #[arg(short, long)]
        endpoint: Option<String>,

        /// Force overwrite existing config
        #[arg(short, long)]
        force: bool,
    },

    /// Set a configuration value
    Set {
        /// Configuration key
        key: String,

        /// Configuration value
        value: String,
    },

    /// Get a configuration value
    Get {
        /// Configuration key
        key: String,
    },

    /// Manage profiles
    Profile {
        #[command(subcommand)]
        command: ProfileCommands,
    },

    /// Show configuration file path
    Path,
}

#[derive(Subcommand)]
enum ProfileCommands {
    /// List profiles
    List,

    /// Create a new profile
    Create {
        /// Profile name
        name: String,

        /// Server endpoint
        #[arg(short, long)]
        endpoint: String,
    },

    /// Delete a profile
    Delete {
        /// Profile name
        name: String,
    },

    /// Set default profile
    Use {
        /// Profile name
        name: String,
    },
}

pub async fn execute(args: ConfigArgs, _format: OutputFormat) -> Result<()> {
    match args.command {
        ConfigCommands::Show => {
            let config = Config::load(None)?;

            output::print_header("Current Configuration");
            output::print_kv("Endpoint", &config.endpoint);
            output::print_kv("Region", &config.region);
            output::print_kv("Timeout", format!("{}s", config.timeout));
            output::print_kv("TLS Enabled", if config.tls.enabled { "Yes" } else { "No" });
            output::print_kv(
                "Access Key",
                config.access_key.as_deref().map(|_| "****").unwrap_or("(not set)"),
            );
            output::print_kv(
                "Secret Key",
                config.secret_key.as_deref().map(|_| "****").unwrap_or("(not set)"),
            );
        }

        ConfigCommands::Init { endpoint, force } => {
            let config_path = Config::config_dir()
                .map(|p| p.join("config.toml"));

            if let Some(ref path) = config_path {
                if path.exists() && !force {
                    output::warning(format!(
                        "Configuration file already exists at {:?}. Use --force to overwrite.",
                        path
                    ));
                    return Ok(());
                }
            }

            // Interactive setup
            let endpoint = endpoint.unwrap_or_else(|| {
                output::input("Server endpoint")
                    .unwrap_or_else(|| "http://localhost:9000".to_string())
            });

            let access_key = output::input("Access key (optional)");
            let secret_key = if access_key.is_some() {
                output::input("Secret key")
            } else {
                None
            };

            let config = Config {
                endpoint,
                access_key,
                secret_key,
                ..Default::default()
            };

            config.save(None)?;

            output::success(format!(
                "Configuration saved to {:?}",
                config_path.unwrap_or_default()
            ));
        }

        ConfigCommands::Set { key, value } => {
            let mut config = Config::load(None)?;

            match key.as_str() {
                "endpoint" => config.endpoint = value,
                "region" => config.region = value,
                "timeout" => config.timeout = value.parse()?,
                "access_key" => config.access_key = Some(value),
                "secret_key" => config.secret_key = Some(value),
                "token" => config.token = Some(value),
                "tls.enabled" => config.tls.enabled = value.parse()?,
                "tls.skip_verify" => config.tls.skip_verify = value.parse()?,
                "output.format" => config.output.format = value,
                "output.colors" => config.output.colors = value.parse()?,
                _ => anyhow::bail!("Unknown configuration key: {}", key),
            }

            config.save(None)?;
            output::success(format!("Set {} = {}", key, value));
        }

        ConfigCommands::Get { key } => {
            let config = Config::load(None)?;

            let value = match key.as_str() {
                "endpoint" => config.endpoint,
                "region" => config.region,
                "timeout" => config.timeout.to_string(),
                "access_key" => config.access_key.unwrap_or_else(|| "(not set)".to_string()),
                "secret_key" => config.secret_key.map(|_| "****".to_string()).unwrap_or_else(|| "(not set)".to_string()),
                "token" => config.token.map(|_| "****".to_string()).unwrap_or_else(|| "(not set)".to_string()),
                "tls.enabled" => config.tls.enabled.to_string(),
                "tls.skip_verify" => config.tls.skip_verify.to_string(),
                "output.format" => config.output.format,
                "output.colors" => config.output.colors.to_string(),
                _ => anyhow::bail!("Unknown configuration key: {}", key),
            };

            println!("{}", value);
        }

        ConfigCommands::Profile { command } => match command {
            ProfileCommands::List => {
                let profiles = Profiles::load()?;

                output::print_header("Profiles");
                for (name, profile) in &profiles.profiles {
                    let marker = if name == &profiles.default { " (default)" } else { "" };
                    println!("  {} - {}{}", name, profile.endpoint, marker);
                }
            }

            ProfileCommands::Create { name, endpoint } => {
                let mut profiles = Profiles::load()?;

                let config = Config {
                    endpoint,
                    ..Default::default()
                };

                profiles.set(&name, config);
                profiles.save()?;

                output::success(format!("Profile '{}' created", name));
            }

            ProfileCommands::Delete { name } => {
                let mut profiles = Profiles::load()?;

                if name == profiles.default {
                    anyhow::bail!("Cannot delete the default profile");
                }

                profiles.profiles.remove(&name);
                profiles.save()?;

                output::success(format!("Profile '{}' deleted", name));
            }

            ProfileCommands::Use { name } => {
                let mut profiles = Profiles::load()?;

                if !profiles.profiles.contains_key(&name) {
                    anyhow::bail!("Profile '{}' not found", name);
                }

                profiles.default = name.clone();
                profiles.save()?;

                output::success(format!("Default profile set to '{}'", name));
            }
        },

        ConfigCommands::Path => {
            if let Some(path) = Config::config_dir() {
                println!("{}", path.display());
            } else {
                output::error("Could not determine configuration directory");
            }
        }
    }

    Ok(())
}
