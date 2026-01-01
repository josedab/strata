//! CLI Configuration management

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Server endpoint
    #[serde(default = "default_endpoint")]
    pub endpoint: String,

    /// Access key for authentication
    pub access_key: Option<String>,

    /// Secret key for authentication
    pub secret_key: Option<String>,

    /// API token (alternative to access/secret key)
    pub token: Option<String>,

    /// Default region
    #[serde(default = "default_region")]
    pub region: String,

    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// TLS configuration
    #[serde(default)]
    pub tls: TlsConfig,

    /// Output preferences
    #[serde(default)]
    pub output: OutputConfig,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Enable TLS
    #[serde(default)]
    pub enabled: bool,

    /// Skip certificate verification (insecure)
    #[serde(default)]
    pub skip_verify: bool,

    /// CA certificate path
    pub ca_cert: Option<String>,

    /// Client certificate path
    pub client_cert: Option<String>,

    /// Client key path
    pub client_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Default output format
    #[serde(default = "default_format")]
    pub format: String,

    /// Enable colors
    #[serde(default = "default_true")]
    pub colors: bool,

    /// Show progress bars
    #[serde(default = "default_true")]
    pub progress: bool,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            format: default_format(),
            colors: true,
            progress: true,
        }
    }
}

fn default_endpoint() -> String {
    "http://localhost:9000".to_string()
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_timeout() -> u64 {
    30
}

fn default_format() -> String {
    "table".to_string()
}

fn default_true() -> bool {
    true
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: default_endpoint(),
            access_key: None,
            secret_key: None,
            token: None,
            region: default_region(),
            timeout: default_timeout(),
            tls: TlsConfig::default(),
            output: OutputConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from file and environment
    pub fn load(path: Option<&str>) -> Result<Self> {
        let config_path = path
            .map(PathBuf::from)
            .or_else(Self::default_config_path);

        let mut config = if let Some(ref path) = config_path {
            if path.exists() {
                let content = std::fs::read_to_string(path)
                    .with_context(|| format!("Failed to read config file: {:?}", path))?;

                if path.extension().map_or(false, |e| e == "yaml" || e == "yml") {
                    serde_yaml::from_str(&content)?
                } else {
                    toml::from_str(&content)?
                }
            } else {
                Config::default()
            }
        } else {
            Config::default()
        };

        // Override with environment variables
        config.apply_env_overrides();

        Ok(config)
    }

    /// Get default configuration file path
    fn default_config_path() -> Option<PathBuf> {
        dirs::config_dir().map(|p| p.join("strata").join("config.toml"))
    }

    /// Apply environment variable overrides
    fn apply_env_overrides(&mut self) {
        if let Ok(endpoint) = std::env::var("STRATA_ENDPOINT") {
            self.endpoint = endpoint;
        }
        if let Ok(key) = std::env::var("STRATA_ACCESS_KEY") {
            self.access_key = Some(key);
        }
        if let Ok(key) = std::env::var("STRATA_SECRET_KEY") {
            self.secret_key = Some(key);
        }
        if let Ok(token) = std::env::var("STRATA_TOKEN") {
            self.token = Some(token);
        }
        if let Ok(region) = std::env::var("STRATA_REGION") {
            self.region = region;
        }
    }

    /// Save configuration to file
    pub fn save(&self, path: Option<&str>) -> Result<()> {
        let config_path = path
            .map(PathBuf::from)
            .or_else(Self::default_config_path)
            .context("No configuration path available")?;

        // Create parent directory if needed
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let content = toml::to_string_pretty(self)?;
        std::fs::write(&config_path, content)?;

        Ok(())
    }

    /// Get configuration directory
    pub fn config_dir() -> Option<PathBuf> {
        dirs::config_dir().map(|p| p.join("strata"))
    }
}

/// Profile management for multiple environments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profiles {
    /// Default profile name
    pub default: String,

    /// Named profiles
    pub profiles: std::collections::HashMap<String, Config>,
}

impl Default for Profiles {
    fn default() -> Self {
        let mut profiles = std::collections::HashMap::new();
        profiles.insert("default".to_string(), Config::default());

        Self {
            default: "default".to_string(),
            profiles,
        }
    }
}

impl Profiles {
    /// Load profiles from file
    pub fn load() -> Result<Self> {
        let path = dirs::config_dir()
            .map(|p| p.join("strata").join("profiles.toml"));

        if let Some(path) = path {
            if path.exists() {
                let content = std::fs::read_to_string(&path)?;
                return Ok(toml::from_str(&content)?);
            }
        }

        Ok(Self::default())
    }

    /// Get a profile by name
    pub fn get(&self, name: &str) -> Option<&Config> {
        self.profiles.get(name)
    }

    /// Get the default profile
    pub fn get_default(&self) -> Option<&Config> {
        self.profiles.get(&self.default)
    }

    /// Add or update a profile
    pub fn set(&mut self, name: &str, config: Config) {
        self.profiles.insert(name.to_string(), config);
    }

    /// Save profiles to file
    pub fn save(&self) -> Result<()> {
        let path = dirs::config_dir()
            .map(|p| p.join("strata").join("profiles.toml"))
            .context("No configuration directory available")?;

        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let content = toml::to_string_pretty(self)?;
        std::fs::write(&path, content)?;

        Ok(())
    }
}
