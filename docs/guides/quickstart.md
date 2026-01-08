# Quick Start Guide

Get Strata running in minutes.

## Prerequisites

- Rust 1.70+ (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- Linux or macOS
- For FUSE: `libfuse3-dev` (Linux) or `macfuse` (macOS)

## Build from Source

```bash
# Clone the repository
git clone https://github.com/strata-storage/strata.git
cd strata

# Build with all features
cargo build --release

# Binaries are in target/release/
ls target/release/strata*
```

## Single Node Setup (Development)

Start a single combined node for development:

```bash
# Create data directories
mkdir -p /tmp/strata/{data,metadata}

# Start the server
cargo run --release -- server \
    --node-id 1 \
    --data-dir /tmp/strata/data \
    --metadata-dir /tmp/strata/metadata
```

The server exposes:
- **Port 9000**: Metadata service (gRPC)
- **Port 9001**: Data service (gRPC)
- **Port 9002**: S3 gateway (HTTP)
- **Port 9090**: Metrics (Prometheus)

## Access via S3

Use any S3-compatible tool:

```bash
# List buckets
aws --endpoint-url http://localhost:9002 s3 ls

# Create a bucket
aws --endpoint-url http://localhost:9002 s3 mb s3://mybucket

# Upload a file
aws --endpoint-url http://localhost:9002 s3 cp myfile.txt s3://mybucket/

# Download a file
aws --endpoint-url http://localhost:9002 s3 cp s3://mybucket/myfile.txt ./downloaded.txt

# List bucket contents
aws --endpoint-url http://localhost:9002 s3 ls s3://mybucket/
```

## Access via FUSE (POSIX)

Mount as a local filesystem:

```bash
# Create mount point
sudo mkdir -p /mnt/strata

# Mount (requires FUSE feature)
cargo run --release --features fuse --bin strata-fuse -- \
    --metadata-addr 127.0.0.1:9000 \
    /mnt/strata

# Use like any filesystem
ls /mnt/strata
echo "Hello Strata" > /mnt/strata/hello.txt
cat /mnt/strata/hello.txt

# Unmount
fusermount -u /mnt/strata  # Linux
umount /mnt/strata         # macOS
```

## CLI Operations

```bash
# Check cluster status
cargo run -- cluster status

# List files
cargo run -- fs ls /

# Create a directory
cargo run -- fs mkdir /mydir

# Show version
cargo run -- version
```

## Three-Node Cluster

For a production-like setup with fault tolerance:

### Node 1 (Terminal 1)
```bash
cargo run --release -- server \
    --node-id 1 \
    --metadata-addr 0.0.0.0:9000 \
    --data-addr 0.0.0.0:9001 \
    --data-dir /tmp/strata1/data \
    --metadata-dir /tmp/strata1/metadata \
    --bootstrap
```

### Node 2 (Terminal 2)
```bash
cargo run --release -- server \
    --node-id 2 \
    --metadata-addr 0.0.0.0:9100 \
    --data-addr 0.0.0.0:9101 \
    --data-dir /tmp/strata2/data \
    --metadata-dir /tmp/strata2/metadata \
    --join 127.0.0.1:9000
```

### Node 3 (Terminal 3)
```bash
cargo run --release -- server \
    --node-id 3 \
    --metadata-addr 0.0.0.0:9200 \
    --data-addr 0.0.0.0:9201 \
    --data-dir /tmp/strata3/data \
    --metadata-dir /tmp/strata3/metadata \
    --join 127.0.0.1:9000
```

## Verify the Cluster

```bash
# Check cluster status
cargo run -- cluster status --metadata-addr 127.0.0.1:9000

# Should show 3 nodes, one leader
```

## Monitor with Metrics

View Prometheus metrics:

```bash
curl http://localhost:9090/metrics
```

Key metrics to watch:
- `strata_cluster_nodes_online` - Number of healthy nodes
- `strata_raft_is_leader` - Whether this node is the Raft leader
- `strata_data_bytes_written` - Total bytes written

## Configuration File

For more complex setups, use a configuration file:

```toml
# strata.toml
[node]
id = 1
name = "strata-node-1"
role = "combined"

[metadata]
bind_addr = "0.0.0.0:9000"
raft_peers = []

[data]
bind_addr = "0.0.0.0:9001"
chunk_size = 67108864  # 64MB

[s3]
enabled = true
bind_addr = "0.0.0.0:9002"
region = "us-east-1"

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
```

Run with config:
```bash
cargo run --release -- server --config strata.toml
```

## Next Steps

- [Deployment Guide](deployment.md) - Production deployment
- [Operator's Guide](operations.md) - Monitoring and maintenance
- [Architecture Overview](../architecture/overview.md) - System design
