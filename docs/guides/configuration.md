# Configuration Reference

Complete reference for all Strata configuration options.

## Configuration Sources

Strata reads configuration from (in order of precedence):

1. Command-line arguments
2. Environment variables (prefixed with `STRATA_`)
3. Configuration file (TOML format)
4. Default values

## Configuration File Format

```toml
# /etc/strata/config.toml

[node]
id = 1
name = "strata-node-1"
role = "combined"

[metadata]
bind_addr = "0.0.0.0:9000"
raft_peers = ["node2:9000", "node3:9000"]
election_timeout_min = "150ms"
election_timeout_max = "300ms"
heartbeat_interval = "50ms"

[data]
bind_addr = "0.0.0.0:9001"
chunk_size = 67108864
cache_size = 1073741824

[s3]
enabled = true
bind_addr = "0.0.0.0:9002"
region = "us-east-1"

[network]
connect_timeout = "5s"
request_timeout = "30s"
max_connections = 1000

[storage]
data_dir = "/var/lib/strata/data"
metadata_dir = "/var/lib/strata/metadata"

[storage.erasure_config]
data_shards = 4
parity_shards = 2

[observability]
metrics_enabled = true
metrics_addr = "0.0.0.0:9090"
log_level = "info"
json_logs = false
```

## Section Reference

### [node]

Node identity and role configuration.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `id` | u64 | *required* | Unique node identifier (must be > 0) |
| `name` | string | `"strata-node"` | Human-readable node name |
| `role` | string | `"combined"` | Node role: `combined`, `metadata`, or `data` |

**Node Roles:**

| Role | Metadata | Data | Use Case |
|------|----------|------|----------|
| `combined` | Yes | Yes | Small clusters, development |
| `metadata` | Yes | No | Dedicated metadata tier |
| `data` | No | Yes | Dedicated storage tier |

### [metadata]

Metadata service and Raft consensus configuration.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bind_addr` | SocketAddr | `0.0.0.0:9000` | Address to bind metadata service |
| `raft_peers` | string[] | `[]` | Addresses of other metadata nodes |
| `election_timeout_min` | duration | `150ms` | Minimum election timeout |
| `election_timeout_max` | duration | `300ms` | Maximum election timeout |
| `heartbeat_interval` | duration | `50ms` | Raft heartbeat interval |

**Tuning Guidelines:**

- **Low-latency networks (< 1ms RTT):**
  ```toml
  election_timeout_min = "100ms"
  election_timeout_max = "200ms"
  heartbeat_interval = "30ms"
  ```

- **High-latency networks (> 5ms RTT):**
  ```toml
  election_timeout_min = "500ms"
  election_timeout_max = "1000ms"
  heartbeat_interval = "150ms"
  ```

### [data]

Data service configuration.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bind_addr` | SocketAddr | `0.0.0.0:9001` | Address to bind data service |
| `chunk_size` | usize | `67108864` (64MB) | Maximum chunk size in bytes |
| `cache_size` | usize | `1073741824` (1GB) | LRU cache size in bytes |

**Chunk Size Considerations:**

| Size | Pros | Cons |
|------|------|------|
| Small (4-16MB) | Lower latency, better dedup | More metadata overhead |
| Medium (32-64MB) | Balanced | Good default |
| Large (128-256MB) | Less metadata, efficient for large files | Higher latency |

### [s3]

S3 gateway configuration.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `true` | Enable S3 gateway |
| `bind_addr` | SocketAddr | `0.0.0.0:9002` | Address to bind S3 gateway |
| `region` | string | `"us-east-1"` | S3 region name |

#### [s3.auth]

S3 authentication settings.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `allow_anonymous` | bool | `true` | Allow unsigned requests |
| `credentials` | list | `[]` | List of access key credentials |

**Credential format:**
```toml
[[s3.auth.credentials]]
access_key_id = "AKIAIOSFODNN7EXAMPLE"
secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
user_id = "admin"
display_name = "Admin User"  # Optional
```

#### [s3.rate_limit]

S3 rate limiting settings.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `true` | Enable rate limiting |
| `requests_per_second` | u32 | `100` | Max requests per second per client |
| `burst_size` | u32 | `200` | Maximum burst capacity |
| `global_requests_per_second` | u32 | `10000` | Max global requests per second |

**Example configuration:**
```toml
[s3.rate_limit]
enabled = true
requests_per_second = 100
burst_size = 200
global_requests_per_second = 10000
```

### [network]

Network settings.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `connect_timeout` | duration | `5s` | Connection establishment timeout |
| `request_timeout` | duration | `30s` | Request timeout |
| `max_connections` | usize | `1000` | Maximum concurrent connections |

### [storage]

Storage paths and erasure coding.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `data_dir` | path | `/var/lib/strata/data` | Directory for chunk data |
| `metadata_dir` | path | `/var/lib/strata/metadata` | Directory for metadata (RocksDB) |

#### [storage.erasure_config]

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `data_shards` | usize | `4` | Number of data shards (k) |
| `parity_shards` | usize | `2` | Number of parity shards (m) |

**Preset Configurations:**

| Preset | k | m | Overhead | Fault Tolerance | Min Nodes |
|--------|---|---|----------|-----------------|-----------|
| Small | 2 | 1 | 1.50x | 1 failure | 3 |
| Default | 4 | 2 | 1.50x | 2 failures | 6 |
| Large | 8 | 4 | 1.50x | 4 failures | 12 |
| Cost-Optimized | 10 | 4 | 1.40x | 4 failures | 14 |

### [observability]

Metrics and logging.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `metrics_enabled` | bool | `true` | Enable Prometheus metrics |
| `metrics_addr` | SocketAddr | `0.0.0.0:9090` | Metrics endpoint address |
| `log_level` | string | `"info"` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `json_logs` | bool | `false` | Output logs in JSON format |

## Environment Variables

All configuration options can be set via environment variables:

```bash
# Pattern: STRATA_<SECTION>_<OPTION>
export STRATA_NODE_ID=1
export STRATA_NODE_NAME="my-node"
export STRATA_METADATA_BIND_ADDR="0.0.0.0:9000"
export STRATA_DATA_BIND_ADDR="0.0.0.0:9001"
export STRATA_S3_BIND_ADDR="0.0.0.0:9002"
export STRATA_STORAGE_DATA_DIR="/data/strata"
export STRATA_OBSERVABILITY_LOG_LEVEL="debug"
```

**Special Variables:**

| Variable | Description |
|----------|-------------|
| `STRATA_CONFIG` | Path to configuration file |
| `STRATA_LOG_LEVEL` | Override log level |
| `RUST_LOG` | Detailed logging filter |

## Command-Line Arguments

```bash
strata server [OPTIONS]

OPTIONS:
    --node-id <ID>              Node identifier [required]
    --config <PATH>             Configuration file path
    --role <ROLE>               Node role: combined, metadata, data
    --metadata-addr <ADDR>      Metadata service bind address
    --data-addr <ADDR>          Data service bind address
    --s3-addr <ADDR>            S3 gateway bind address
    --data-dir <PATH>           Data directory
    --metadata-dir <PATH>       Metadata directory
    --bootstrap                 Bootstrap new cluster
    --join <ADDR>               Join existing cluster
    --log-level <LEVEL>         Log level
    -h, --help                  Print help
    -V, --version               Print version
```

## Duration Format

Durations support the following formats:

| Format | Example | Description |
|--------|---------|-------------|
| Milliseconds | `150ms` | 150 milliseconds |
| Seconds | `5s` | 5 seconds |
| Minutes | `5m` | 5 minutes |
| Plain number | `150` | Interpreted as milliseconds |

## Configuration Validation

Strata validates configuration on startup:

```bash
# Validate configuration without starting
strata config validate --config /etc/strata/config.toml
```

**Validation Rules:**

1. `node.id` must be > 0
2. `metadata.raft_peers` required for metadata nodes (unless bootstrapping)
3. `storage.erasure_config.data_shards` must be > 0
4. All bind addresses must be valid socket addresses
5. Directories must be writable

## Example Configurations

### Development (Single Node)

```toml
[node]
id = 1
role = "combined"

[storage]
data_dir = "/tmp/strata/data"
metadata_dir = "/tmp/strata/metadata"

[storage.erasure_config]
data_shards = 2
parity_shards = 1
```

### Production (3-Node Cluster)

```toml
# Node 1
[node]
id = 1
name = "strata-prod-1"
role = "combined"

[metadata]
bind_addr = "0.0.0.0:9000"
raft_peers = ["strata-prod-2:9000", "strata-prod-3:9000"]

[storage]
data_dir = "/var/lib/strata/data"
metadata_dir = "/var/lib/strata/metadata"

[storage.erasure_config]
data_shards = 4
parity_shards = 2

[observability]
log_level = "info"
json_logs = true
```

### High-Performance

```toml
[data]
chunk_size = 134217728  # 128MB
cache_size = 8589934592  # 8GB

[network]
max_connections = 5000
request_timeout = "60s"

[observability]
log_level = "warn"  # Reduce logging overhead
```

## See Also

- [Deployment Guide](deployment.md) - Deployment instructions
- [Operator's Guide](operations.md) - Operations and monitoring
