//! Custom Resource Definitions for Strata

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ============================================================================
// StrataCluster CRD
// ============================================================================

/// StrataCluster represents a Strata distributed filesystem cluster
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "strata.io",
    version = "v1alpha1",
    kind = "StrataCluster",
    plural = "strataclusters",
    shortname = "sc",
    status = "StrataClusterStatus",
    namespaced,
    printcolumn = r#"{"name":"Nodes","type":"integer","jsonPath":".status.readyNodes"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct StrataClusterSpec {
    /// Number of metadata nodes
    #[serde(default = "default_metadata_replicas")]
    pub metadata_replicas: i32,

    /// Number of data nodes
    #[serde(default = "default_data_replicas")]
    pub data_replicas: i32,

    /// Strata version to deploy
    #[serde(default = "default_version")]
    pub version: String,

    /// Storage configuration
    #[serde(default)]
    pub storage: StorageConfig,

    /// Resource requirements
    #[serde(default)]
    pub resources: ResourceConfig,

    /// Network configuration
    #[serde(default)]
    pub network: NetworkConfig,

    /// Security configuration
    #[serde(default)]
    pub security: SecurityConfig,

    /// Erasure coding configuration
    #[serde(default)]
    pub erasure: ErasureConfig,

    /// S3 gateway configuration
    #[serde(default)]
    pub s3_gateway: Option<S3GatewayConfig>,

    /// Monitoring configuration
    #[serde(default)]
    pub monitoring: MonitoringConfig,

    /// Additional pod annotations
    #[serde(default)]
    pub pod_annotations: BTreeMap<String, String>,

    /// Additional pod labels
    #[serde(default)]
    pub pod_labels: BTreeMap<String, String>,

    /// Node selector for pod placement
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,

    /// Tolerations for pod scheduling
    #[serde(default)]
    pub tolerations: Vec<Toleration>,

    /// Affinity rules
    #[serde(default)]
    pub affinity: Option<Affinity>,

    /// Enable auto-scaling
    #[serde(default)]
    pub auto_scaling: Option<AutoScalingConfig>,

    /// Backup configuration
    #[serde(default)]
    pub backup: Option<BackupConfig>,
}

fn default_metadata_replicas() -> i32 {
    3
}

fn default_data_replicas() -> i32 {
    3
}

fn default_version() -> String {
    "latest".to_string()
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct StorageConfig {
    /// Storage class for persistent volumes
    #[serde(default = "default_storage_class")]
    pub storage_class: String,

    /// Size of metadata volume
    #[serde(default = "default_metadata_size")]
    pub metadata_size: String,

    /// Size of data volume
    #[serde(default = "default_data_size")]
    pub data_size: String,

    /// Cache size per node
    #[serde(default = "default_cache_size")]
    pub cache_size: String,

    /// Enable local SSDs for caching
    #[serde(default)]
    pub use_local_ssd: bool,
}

fn default_storage_class() -> String {
    "standard".to_string()
}

fn default_metadata_size() -> String {
    "10Gi".to_string()
}

fn default_data_size() -> String {
    "100Gi".to_string()
}

fn default_cache_size() -> String {
    "1Gi".to_string()
}

/// Resource configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceConfig {
    /// Metadata node resources
    #[serde(default)]
    pub metadata: NodeResources,

    /// Data node resources
    #[serde(default)]
    pub data: NodeResources,

    /// S3 gateway resources
    #[serde(default)]
    pub s3_gateway: NodeResources,
}

/// Resource requirements for a node type
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeResources {
    /// CPU request
    #[serde(default = "default_cpu_request")]
    pub cpu_request: String,

    /// CPU limit
    #[serde(default = "default_cpu_limit")]
    pub cpu_limit: String,

    /// Memory request
    #[serde(default = "default_memory_request")]
    pub memory_request: String,

    /// Memory limit
    #[serde(default = "default_memory_limit")]
    pub memory_limit: String,
}

fn default_cpu_request() -> String {
    "500m".to_string()
}

fn default_cpu_limit() -> String {
    "2".to_string()
}

fn default_memory_request() -> String {
    "512Mi".to_string()
}

fn default_memory_limit() -> String {
    "2Gi".to_string()
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NetworkConfig {
    /// Service type (ClusterIP, LoadBalancer, NodePort)
    #[serde(default = "default_service_type")]
    pub service_type: String,

    /// Enable host networking
    #[serde(default)]
    pub host_network: bool,

    /// Metadata service port
    #[serde(default = "default_metadata_port")]
    pub metadata_port: i32,

    /// Data service port
    #[serde(default = "default_data_port")]
    pub data_port: i32,

    /// S3 gateway port
    #[serde(default = "default_s3_port")]
    pub s3_port: i32,

    /// Enable network policies
    #[serde(default)]
    pub enable_network_policy: bool,
}

fn default_service_type() -> String {
    "ClusterIP".to_string()
}

fn default_metadata_port() -> i32 {
    9000
}

fn default_data_port() -> i32 {
    9001
}

fn default_s3_port() -> i32 {
    9002
}

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct SecurityConfig {
    /// Enable TLS
    #[serde(default = "default_tls_enabled")]
    pub tls_enabled: bool,

    /// TLS secret name
    pub tls_secret: Option<String>,

    /// Enable encryption at rest
    #[serde(default)]
    pub encryption_at_rest: bool,

    /// KMS key ID for encryption
    pub kms_key_id: Option<String>,

    /// Enable audit logging
    #[serde(default)]
    pub audit_logging: bool,

    /// Run as non-root
    #[serde(default = "default_true")]
    pub run_as_non_root: bool,

    /// Read-only root filesystem
    #[serde(default = "default_true")]
    pub read_only_root_filesystem: bool,

    /// Service account name
    pub service_account: Option<String>,
}

fn default_tls_enabled() -> bool {
    true
}

fn default_true() -> bool {
    true
}

/// Erasure coding configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ErasureConfig {
    /// Data shards
    #[serde(default = "default_data_shards")]
    pub data_shards: i32,

    /// Parity shards
    #[serde(default = "default_parity_shards")]
    pub parity_shards: i32,

    /// Minimum placement groups
    #[serde(default = "default_placement_groups")]
    pub placement_groups: i32,
}

fn default_data_shards() -> i32 {
    4
}

fn default_parity_shards() -> i32 {
    2
}

fn default_placement_groups() -> i32 {
    128
}

/// S3 gateway configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct S3GatewayConfig {
    /// Number of gateway replicas
    #[serde(default = "default_s3_replicas")]
    pub replicas: i32,

    /// Enable HTTPS
    #[serde(default = "default_true")]
    pub https_enabled: bool,

    /// Virtual host style addressing
    #[serde(default)]
    pub virtual_host_style: bool,

    /// Ingress configuration
    pub ingress: Option<IngressConfig>,
}

fn default_s3_replicas() -> i32 {
    2
}

/// Ingress configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IngressConfig {
    /// Enable ingress
    #[serde(default)]
    pub enabled: bool,

    /// Ingress class name
    pub ingress_class: Option<String>,

    /// Hostname
    pub hostname: Option<String>,

    /// TLS secret name
    pub tls_secret: Option<String>,

    /// Additional annotations
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
}

/// Monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct MonitoringConfig {
    /// Enable Prometheus metrics
    #[serde(default = "default_true")]
    pub prometheus_enabled: bool,

    /// Metrics port
    #[serde(default = "default_metrics_port")]
    pub metrics_port: i32,

    /// ServiceMonitor configuration
    pub service_monitor: Option<ServiceMonitorConfig>,

    /// Grafana dashboard configuration
    #[serde(default)]
    pub grafana_dashboard: bool,

    /// Alert rules
    #[serde(default)]
    pub alerting_rules: bool,
}

fn default_metrics_port() -> i32 {
    9090
}

/// ServiceMonitor configuration for Prometheus Operator
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceMonitorConfig {
    /// Enable ServiceMonitor creation
    #[serde(default)]
    pub enabled: bool,

    /// Scrape interval
    #[serde(default = "default_scrape_interval")]
    pub interval: String,

    /// Additional labels
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

fn default_scrape_interval() -> String {
    "30s".to_string()
}

/// Toleration for pod scheduling
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    pub key: Option<String>,
    pub operator: Option<String>,
    pub value: Option<String>,
    pub effect: Option<String>,
    pub toleration_seconds: Option<i64>,
}

/// Affinity rules
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Affinity {
    /// Node affinity
    pub node_affinity: Option<serde_json::Value>,
    /// Pod affinity
    pub pod_affinity: Option<serde_json::Value>,
    /// Pod anti-affinity
    pub pod_anti_affinity: Option<serde_json::Value>,
}

/// Auto-scaling configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AutoScalingConfig {
    /// Enable auto-scaling
    #[serde(default)]
    pub enabled: bool,

    /// Minimum data nodes
    #[serde(default = "default_min_nodes")]
    pub min_data_nodes: i32,

    /// Maximum data nodes
    #[serde(default = "default_max_nodes")]
    pub max_data_nodes: i32,

    /// Target CPU utilization
    #[serde(default = "default_target_cpu")]
    pub target_cpu_utilization: i32,

    /// Target storage utilization
    #[serde(default = "default_target_storage")]
    pub target_storage_utilization: i32,

    /// Scale down stabilization window
    #[serde(default = "default_stabilization_window")]
    pub stabilization_window_seconds: i32,
}

fn default_min_nodes() -> i32 {
    3
}

fn default_max_nodes() -> i32 {
    10
}

fn default_target_cpu() -> i32 {
    70
}

fn default_target_storage() -> i32 {
    80
}

fn default_stabilization_window() -> i32 {
    300
}

/// Backup configuration
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupConfig {
    /// Enable automatic backups
    #[serde(default)]
    pub enabled: bool,

    /// Backup schedule (cron format)
    #[serde(default = "default_backup_schedule")]
    pub schedule: String,

    /// Backup retention days
    #[serde(default = "default_retention_days")]
    pub retention_days: i32,

    /// Backup storage location (S3 URL)
    pub storage_location: String,

    /// Secret containing storage credentials
    pub credentials_secret: Option<String>,
}

fn default_backup_schedule() -> String {
    "0 2 * * *".to_string()
}

fn default_retention_days() -> i32 {
    30
}

/// Status of a StrataCluster
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct StrataClusterStatus {
    /// Current phase
    pub phase: ClusterPhase,

    /// Number of ready nodes
    #[serde(default)]
    pub ready_nodes: i32,

    /// Number of total nodes
    #[serde(default)]
    pub total_nodes: i32,

    /// Metadata node status
    #[serde(default)]
    pub metadata_nodes: Vec<NodeStatus>,

    /// Data node status
    #[serde(default)]
    pub data_nodes: Vec<NodeStatus>,

    /// S3 gateway status
    #[serde(default)]
    pub s3_gateways: Vec<NodeStatus>,

    /// Current version
    pub current_version: Option<String>,

    /// Target version (during upgrade)
    pub target_version: Option<String>,

    /// Conditions
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,

    /// Observed generation
    #[serde(default)]
    pub observed_generation: i64,

    /// Last backup time
    pub last_backup: Option<String>,

    /// Cluster capacity
    pub capacity: Option<ClusterCapacity>,

    /// Message
    pub message: Option<String>,
}

/// Cluster phase
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
pub enum ClusterPhase {
    #[default]
    Pending,
    Creating,
    Running,
    Updating,
    Upgrading,
    Scaling,
    Degraded,
    Failed,
    Terminating,
}

/// Status of an individual node
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    /// Node name
    pub name: String,

    /// Pod name
    pub pod_name: String,

    /// Node role
    pub role: String,

    /// Is ready
    pub ready: bool,

    /// Node address
    pub address: Option<String>,

    /// Disk usage
    pub disk_usage_percent: Option<i32>,

    /// Last heartbeat
    pub last_heartbeat: Option<String>,
}

/// Cluster condition
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCondition {
    /// Condition type
    #[serde(rename = "type")]
    pub condition_type: String,

    /// Status (True, False, Unknown)
    pub status: String,

    /// Last transition time
    pub last_transition_time: Option<String>,

    /// Reason
    pub reason: Option<String>,

    /// Message
    pub message: Option<String>,
}

/// Cluster capacity information
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCapacity {
    /// Total capacity in bytes
    pub total_bytes: i64,

    /// Used capacity in bytes
    pub used_bytes: i64,

    /// Available capacity in bytes
    pub available_bytes: i64,

    /// Number of files
    pub file_count: i64,

    /// Number of chunks
    pub chunk_count: i64,
}

// ============================================================================
// StrataBackup CRD
// ============================================================================

/// StrataBackup represents a backup of a Strata cluster
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "strata.io",
    version = "v1alpha1",
    kind = "StrataBackup",
    plural = "stratabackups",
    shortname = "sb",
    status = "StrataBackupStatus",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct StrataBackupSpec {
    /// Cluster to backup
    pub cluster_ref: String,

    /// Storage location
    pub storage_location: String,

    /// Include metadata only
    #[serde(default)]
    pub metadata_only: bool,

    /// Paths to include
    #[serde(default)]
    pub include_paths: Vec<String>,

    /// Paths to exclude
    #[serde(default)]
    pub exclude_paths: Vec<String>,

    /// Retention days
    #[serde(default = "default_retention_days")]
    pub retention_days: i32,
}

/// Status of a backup
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct StrataBackupStatus {
    /// Backup phase
    pub phase: BackupPhase,

    /// Start time
    pub start_time: Option<String>,

    /// Completion time
    pub completion_time: Option<String>,

    /// Backup size in bytes
    pub size_bytes: Option<i64>,

    /// Number of files backed up
    pub file_count: Option<i64>,

    /// Backup location
    pub location: Option<String>,

    /// Error message
    pub error: Option<String>,
}

/// Backup phase
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
pub enum BackupPhase {
    #[default]
    Pending,
    InProgress,
    Completed,
    Failed,
}

// ============================================================================
// StrataRestore CRD
// ============================================================================

/// StrataRestore represents a restore operation
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "strata.io",
    version = "v1alpha1",
    kind = "StrataRestore",
    plural = "stratarestores",
    shortname = "sr",
    status = "StrataRestoreStatus",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct StrataRestoreSpec {
    /// Backup to restore from
    pub backup_ref: String,

    /// Target cluster
    pub cluster_ref: String,

    /// Paths to restore
    #[serde(default)]
    pub include_paths: Vec<String>,

    /// Restore mode (overwrite, skip, rename)
    #[serde(default = "default_restore_mode")]
    pub restore_mode: String,
}

fn default_restore_mode() -> String {
    "skip".to_string()
}

/// Status of a restore operation
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct StrataRestoreStatus {
    /// Restore phase
    pub phase: RestorePhase,

    /// Start time
    pub start_time: Option<String>,

    /// Completion time
    pub completion_time: Option<String>,

    /// Files restored
    pub files_restored: Option<i64>,

    /// Bytes restored
    pub bytes_restored: Option<i64>,

    /// Error message
    pub error: Option<String>,
}

/// Restore phase
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
pub enum RestorePhase {
    #[default]
    Pending,
    InProgress,
    Completed,
    Failed,
}
