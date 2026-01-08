//! Configuration module for Strata.

use crate::error::{Result, StrataError};
use crate::types::ErasureCodingConfig;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Main configuration for a Strata node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StrataConfig {
    /// Node configuration.
    pub node: NodeConfig,
    /// Metadata service configuration.
    pub metadata: MetadataConfig,
    /// Data service configuration.
    pub data: DataConfig,
    /// S3 gateway configuration.
    pub s3: S3Config,
    /// Network configuration.
    pub network: NetworkConfig,
    /// Storage configuration.
    pub storage: StorageConfig,
    /// Observability configuration.
    pub observability: ObservabilityConfig,
}


impl StrataConfig {
    /// Load configuration from a file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            StrataError::Config(format!("Failed to read config file: {}", e))
        })?;

        let config: Self = serde_json::from_str(&content).map_err(|e| {
            StrataError::Config(format!("Failed to parse config: {}", e))
        })?;

        config.validate()?;
        Ok(config)
    }

    /// Validate configuration.
    pub fn validate(&self) -> Result<()> {
        if self.node.id == 0 {
            return Err(StrataError::InvalidConfig {
                field: "node.id".to_string(),
                reason: "Node ID must be non-zero".to_string(),
            });
        }

        if self.metadata.raft_peers.is_empty() && self.node.role.is_metadata() {
            return Err(StrataError::InvalidConfig {
                field: "metadata.raft_peers".to_string(),
                reason: "Metadata nodes require at least one peer".to_string(),
            });
        }

        if self.storage.erasure_config.data_shards == 0 {
            return Err(StrataError::InvalidConfig {
                field: "storage.erasure_config.data_shards".to_string(),
                reason: "Data shards must be non-zero".to_string(),
            });
        }

        Ok(())
    }

    /// Create a minimal development configuration.
    pub fn development() -> Self {
        Self {
            node: NodeConfig {
                id: 1,
                name: "dev-node".to_string(),
                role: NodeRole::Combined,
            },
            metadata: MetadataConfig {
                bind_addr: "127.0.0.1:9000".parse().expect("valid socket address"),
                raft_peers: vec![],
                election_timeout_min: Duration::from_millis(150),
                election_timeout_max: Duration::from_millis(300),
                heartbeat_interval: Duration::from_millis(50),
            },
            data: DataConfig {
                bind_addr: "127.0.0.1:9001".parse().expect("valid socket address"),
                chunk_size: 64 * 1024 * 1024,
                cache_size: 1024 * 1024 * 1024,
            },
            s3: S3Config {
                enabled: true,
                bind_addr: "127.0.0.1:9002".parse().expect("valid socket address"),
                region: "us-east-1".to_string(),
                auth: S3AuthSettings::default(),
            },
            network: NetworkConfig::default(),
            storage: StorageConfig {
                data_dir: PathBuf::from("/tmp/strata/data"),
                metadata_dir: PathBuf::from("/tmp/strata/metadata"),
                erasure_config: ErasureCodingConfig::SMALL_CLUSTER,
            },
            observability: ObservabilityConfig::default(),
        }
    }
}

/// Node-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique node identifier.
    pub id: u64,
    /// Human-readable node name.
    pub name: String,
    /// Node role in the cluster.
    pub role: NodeRole,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            id: 1,
            name: "strata-node".to_string(),
            role: NodeRole::Combined,
        }
    }
}

/// Node role enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Metadata-only node (runs Raft).
    Metadata,
    /// Data-only node (stores chunks).
    Data,
    /// Combined node (both metadata and data).
    Combined,
}

impl NodeRole {
    pub fn is_metadata(&self) -> bool {
        matches!(self, NodeRole::Metadata | NodeRole::Combined)
    }

    pub fn is_data(&self) -> bool {
        matches!(self, NodeRole::Data | NodeRole::Combined)
    }
}

/// Metadata service configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataConfig {
    /// Address to bind the metadata service.
    pub bind_addr: SocketAddr,
    /// Raft peer addresses.
    pub raft_peers: Vec<String>,
    /// Minimum election timeout.
    #[serde(with = "humantime_serde")]
    pub election_timeout_min: Duration,
    /// Maximum election timeout.
    #[serde(with = "humantime_serde")]
    pub election_timeout_max: Duration,
    /// Heartbeat interval.
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:9000".parse().expect("valid socket address"),
            raft_peers: Vec::new(),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
        }
    }
}

/// Data service configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataConfig {
    /// Address to bind the data service.
    pub bind_addr: SocketAddr,
    /// Chunk size in bytes.
    pub chunk_size: usize,
    /// Cache size in bytes.
    pub cache_size: usize,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:9001".parse().expect("valid socket address"),
            chunk_size: 64 * 1024 * 1024, // 64MB
            cache_size: 1024 * 1024 * 1024, // 1GB
        }
    }
}

/// S3 gateway configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// Enable S3 gateway.
    pub enabled: bool,
    /// Address to bind the S3 gateway.
    pub bind_addr: SocketAddr,
    /// S3 region name.
    pub region: String,
    /// Authentication configuration.
    #[serde(default)]
    pub auth: S3AuthSettings,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_addr: "0.0.0.0:9002".parse().expect("valid socket address"),
            region: "us-east-1".to_string(),
            auth: S3AuthSettings::default(),
        }
    }
}

/// S3 authentication settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3AuthSettings {
    /// Whether to allow anonymous (unsigned) requests.
    #[serde(default = "default_allow_anonymous")]
    pub allow_anonymous: bool,
    /// Static access credentials.
    #[serde(default)]
    pub credentials: Vec<S3Credential>,
}

fn default_allow_anonymous() -> bool {
    true // Default to anonymous for backwards compatibility
}

impl Default for S3AuthSettings {
    fn default() -> Self {
        Self {
            allow_anonymous: true,
            credentials: vec![],
        }
    }
}

impl S3AuthSettings {
    /// Create settings requiring authentication.
    pub fn authenticated() -> Self {
        Self {
            allow_anonymous: false,
            credentials: vec![],
        }
    }
}

/// An S3 access credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Credential {
    /// The access key ID.
    pub access_key_id: String,
    /// The secret access key.
    pub secret_key: String,
    /// Associated user ID for internal mapping.
    #[serde(default = "default_user_id")]
    pub user_id: String,
    /// Optional display name.
    pub display_name: Option<String>,
}

fn default_user_id() -> String {
    "default".to_string()
}

/// Network configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Connection timeout.
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,
    /// Request timeout.
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,
    /// Maximum concurrent connections.
    pub max_connections: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_connections: 1000,
        }
    }
}

/// Storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Directory for chunk data.
    pub data_dir: PathBuf,
    /// Directory for metadata.
    pub metadata_dir: PathBuf,
    /// Erasure coding configuration.
    pub erasure_config: ErasureCodingConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/strata/data"),
            metadata_dir: PathBuf::from("/var/lib/strata/metadata"),
            erasure_config: ErasureCodingConfig::default(),
        }
    }
}

/// Observability configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Enable Prometheus metrics.
    pub metrics_enabled: bool,
    /// Metrics bind address.
    pub metrics_addr: SocketAddr,
    /// Log level.
    pub log_level: String,
    /// Enable JSON logging.
    pub json_logs: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: true,
            metrics_addr: "0.0.0.0:9090".parse().expect("valid socket address"),
            log_level: "info".to_string(),
            json_logs: false,
        }
    }
}

/// Serde helper for Duration using humantime format.
pub mod humantime_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}ms", duration.as_millis()))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        parse_duration(&s).map_err(serde::de::Error::custom)
    }

    fn parse_duration(s: &str) -> Result<Duration, String> {
        let s = s.trim();
        if let Some(ms) = s.strip_suffix("ms") {
            ms.parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|e| e.to_string())
        } else if let Some(s_val) = s.strip_suffix('s') {
            s_val
                .parse::<u64>()
                .map(Duration::from_secs)
                .map_err(|e| e.to_string())
        } else if let Some(m) = s.strip_suffix('m') {
            m.parse::<u64>()
                .map(|v| Duration::from_secs(v * 60))
                .map_err(|e| e.to_string())
        } else {
            s.parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|e| e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = StrataConfig::default();
        assert_eq!(config.node.id, 1);
        assert!(config.s3.enabled);
    }

    #[test]
    fn test_development_config() {
        let config = StrataConfig::development();
        assert_eq!(config.storage.erasure_config.data_shards, 2);
        assert_eq!(config.storage.erasure_config.parity_shards, 1);
    }

    #[test]
    fn test_node_role() {
        assert!(NodeRole::Metadata.is_metadata());
        assert!(!NodeRole::Metadata.is_data());
        assert!(NodeRole::Combined.is_metadata());
        assert!(NodeRole::Combined.is_data());
    }
}
