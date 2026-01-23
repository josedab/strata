# Quick Start Guide

Get Strata running in 5 minutes with Docker, or build from source for development.

## Option 1: Docker (Recommended)

### Prerequisites

- Docker 20.10+
- Docker Compose 2.0+

### Start Strata

```bash
# Clone the repository
git clone https://github.com/strata-storage/strata.git
cd strata

# Start a single node
docker-compose up -d

# Check status
docker-compose ps
```

This starts Strata with:
- **S3 Gateway**: http://localhost:8080
- **Metrics**: http://localhost:9100/metrics

### Test with S3

```bash
# Configure AWS CLI
export AWS_ACCESS_KEY_ID=strata-access-key
export AWS_SECRET_ACCESS_KEY=strata-secret-key
export AWS_DEFAULT_REGION=us-east-1

# Create a bucket
aws --endpoint-url http://localhost:8080 s3 mb s3://my-bucket

# Upload a file
echo "Hello, Strata!" > hello.txt
aws --endpoint-url http://localhost:8080 s3 cp hello.txt s3://my-bucket/

# List bucket contents
aws --endpoint-url http://localhost:8080 s3 ls s3://my-bucket/

# Download the file
aws --endpoint-url http://localhost:8080 s3 cp s3://my-bucket/hello.txt downloaded.txt
cat downloaded.txt
```

### Start a 3-Node Cluster

For production-like setup with Raft consensus:

```bash
docker-compose --profile cluster up -d
```

Cluster endpoints:
- Node 1: S3 on :8081, metrics on :9101
- Node 2: S3 on :8082, metrics on :9102
- Node 3: S3 on :8083, metrics on :9103

```bash
# Data is replicated across all nodes
aws --endpoint-url http://localhost:8081 s3 mb s3://cluster-bucket
aws --endpoint-url http://localhost:8082 s3 ls  # Visible on any node
```

### Start with Monitoring

```bash
docker-compose --profile monitoring up -d
```

Access dashboards:
- **Grafana**: http://localhost:3000 (admin/strata)
- **Prometheus**: http://localhost:9093

### Stop Strata

```bash
# Stop services
docker-compose down

# Stop and delete all data
docker-compose down -v
```

---

## Option 2: Build from Source

### Prerequisites

- Rust 1.70+ (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- Linux or macOS
- For FUSE: `libfuse3-dev` (Linux) or `macfuse` (macOS)

### Build

```bash
# Clone the repository
git clone https://github.com/strata-storage/strata.git
cd strata

# Build with all features
cargo build --release

# Binaries are in target/release/
ls target/release/strata*
```

### Start a Single Node

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

### Access via S3

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

### Access via FUSE (POSIX)

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

### CLI Operations

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

---

## Three-Node Cluster (Source Build)

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

### Verify the Cluster

```bash
# Check cluster status
cargo run -- cluster status --metadata-addr 127.0.0.1:9000

# Should show 3 nodes, one leader
```

---

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

---

## Monitor with Metrics

View Prometheus metrics:

```bash
curl http://localhost:9090/metrics
```

Key metrics to watch:
- `strata_cluster_nodes_online` - Number of healthy nodes
- `strata_raft_is_leader` - Whether this node is the Raft leader
- `strata_data_bytes_written` - Total bytes written

---

## Troubleshooting

### Port Already in Use

```bash
# Check what's using the port
lsof -i :8080

# Use different ports (Docker)
S3_PORT=9080 docker-compose up -d
```

### Connection Refused

```bash
# Docker: Check if container is running
docker-compose ps
docker-compose logs strata

# Source: Check if server started
ps aux | grep strata
```

### Slow Performance

- Docker: Increase memory limits to at least 4GB
- Source: Use `--release` flag for optimized builds

---

## Next Steps

- [Helm Chart for Kubernetes](../../helm/strata/) - Production deployment on Kubernetes
- [Benchmark Suite](../../benchmarks/) - Performance testing
- [Architecture Overview](../architecture/overview.md) - System design
- [Operator's Guide](operations.md) - Monitoring and maintenance
