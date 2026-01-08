# Operator's Guide

This guide covers day-to-day operations, monitoring, and maintenance of a Strata cluster.

## Monitoring

### Metrics Overview

Strata exposes Prometheus metrics at `/metrics` on port 9090.

#### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `strata_cluster_nodes_total` | Gauge | Total nodes in cluster |
| `strata_cluster_nodes_online` | Gauge | Online nodes |
| `strata_raft_term` | Gauge | Current Raft term |
| `strata_raft_is_leader` | Gauge | Whether node is Raft leader |
| `strata_data_reads_total` | Counter | Total read operations |
| `strata_data_writes_total` | Counter | Total write operations |
| `strata_data_bytes_read` | Counter | Total bytes read |
| `strata_data_bytes_written` | Counter | Total bytes written |
| `strata_s3_requests_total` | Counter | S3 API requests (by method) |
| `strata_s3_request_duration_seconds` | Histogram | S3 request latency |
| `strata_chunk_storage_bytes` | Gauge | Stored chunk bytes |
| `strata_chunk_storage_available_bytes` | Gauge | Available storage |
| `strata_recovery_operations_total` | Counter | Recovery operations |
| `strata_scrub_errors_total` | Counter | Scrubbing errors found |

### Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'strata'
    static_configs:
      - targets:
        - 'strata-node-1:9090'
        - 'strata-node-2:9090'
        - 'strata-node-3:9090'
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '([^:]+):\d+'
        replacement: '${1}'
```

### Grafana Dashboard

Import the Strata dashboard (ID: coming soon) or create panels for:

**Cluster Health**
```promql
# Healthy nodes percentage
strata_cluster_nodes_online / strata_cluster_nodes_total * 100

# Leader changes (should be rare)
rate(strata_raft_term[5m])
```

**Throughput**
```promql
# Read throughput
rate(strata_data_bytes_read[5m])

# Write throughput
rate(strata_data_bytes_written[5m])

# IOPS
rate(strata_data_reads_total[5m]) + rate(strata_data_writes_total[5m])
```

**Latency**
```promql
# P99 S3 request latency
histogram_quantile(0.99, rate(strata_s3_request_duration_seconds_bucket[5m]))

# P50 metadata operation latency
histogram_quantile(0.50, rate(strata_metadata_operation_duration_seconds_bucket[5m]))
```

**Storage**
```promql
# Storage utilization per node
strata_chunk_storage_bytes / (strata_chunk_storage_bytes + strata_chunk_storage_available_bytes) * 100

# Storage growth rate
rate(strata_chunk_storage_bytes[1h])
```

### Alerting Rules

```yaml
groups:
  - name: strata
    rules:
      - alert: StrataNodeDown
        expr: up{job="strata"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Strata node {{ $labels.instance }} is down"

      - alert: StrataNoLeader
        expr: sum(strata_raft_is_leader) == 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "Strata cluster has no Raft leader"

      - alert: StrataStorageHigh
        expr: >
          strata_chunk_storage_bytes /
          (strata_chunk_storage_bytes + strata_chunk_storage_available_bytes) > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Strata storage utilization above 85% on {{ $labels.instance }}"

      - alert: StrataHighLatency
        expr: >
          histogram_quantile(0.99, rate(strata_s3_request_duration_seconds_bucket[5m])) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Strata P99 latency above 1s"

      - alert: StrataRecoveryInProgress
        expr: strata_recovery_operations_in_progress > 0
        for: 10m
        labels:
          severity: info
        annotations:
          summary: "Strata data recovery in progress"
```

## Health Checks

### Liveness Probe

```bash
curl -f http://localhost:9090/health/live
```

Returns `200 OK` if the process is running.

### Readiness Probe

```bash
curl -f http://localhost:9090/health/ready
```

Returns `200 OK` when:
- Connected to Raft cluster
- Storage is accessible
- Basic operations succeed

### Kubernetes Probes

```yaml
livenessProbe:
  httpGet:
    path: /health/live
    port: 9090
  initialDelaySeconds: 10
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /health/ready
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 5
```

## Cluster Operations

### View Cluster Status

```bash
strata cluster status
```

### Add a Node

1. Configure the new node with cluster information
2. Start the node with `--join` flag:

```bash
strata-server --node-id 4 --join strata-node-1:9000
```

3. Verify the node joined:

```bash
strata cluster nodes
```

### Remove a Node

1. Drain the node (move data away):

```bash
strata cluster drain node-4
```

2. Wait for drain to complete:

```bash
strata cluster drain-status node-4
```

3. Remove from cluster:

```bash
strata cluster remove node-4
```

### Replace a Failed Node

If a node fails and cannot be recovered:

1. Remove the failed node:

```bash
strata cluster remove --force node-4
```

2. Wait for automatic recovery to complete
3. Add a replacement node

### Rebalance Data

Trigger manual rebalancing:

```bash
strata cluster rebalance
```

Monitor progress:

```bash
strata cluster rebalance-status
```

## Backup and Restore

### Metadata Backup

Backup metadata using the built-in snapshot mechanism:

```bash
strata backup create --output /backup/metadata-$(date +%Y%m%d).snap
```

### Scheduled Backups

Configure automatic backups in config:

```toml
[backup]
enabled = true
schedule = "0 2 * * *"  # Daily at 2 AM
retention_days = 30
destination = "/backup"
```

### Restore from Backup

```bash
# Stop all nodes
systemctl stop strata

# Restore on each metadata node
strata restore --input /backup/metadata-20240101.snap

# Start nodes
systemctl start strata
```

## Data Integrity

### Background Scrubbing

Strata automatically scrubs data to detect corruption. Configure in config:

```toml
[scrub]
enabled = true
interval = "168h"  # Weekly
throttle_mb_per_sec = 100
```

### Manual Scrub

Trigger an immediate scrub:

```bash
strata scrub start
```

Check scrub status:

```bash
strata scrub status
```

### Handling Corruption

If corruption is detected:

1. Check scrub logs for affected chunks
2. Strata will automatically repair using parity shards
3. Monitor recovery progress
4. Investigate root cause (disk errors, etc.)

## Performance Tuning

### Memory Tuning

Adjust cache sizes based on workload:

```toml
[data]
cache_size = 4294967296  # 4GB for read-heavy workloads

[metadata]
# RocksDB block cache
rocksdb_cache_size = 1073741824  # 1GB
```

### I/O Tuning

For SSDs:

```toml
[storage]
sync_writes = false  # SSDs have good write durability
direct_io = true     # Bypass OS cache
```

For HDDs:

```toml
[storage]
sync_writes = true   # Ensure durability
direct_io = false    # Use OS cache
```

### Network Tuning

For high-throughput workloads:

```toml
[network]
max_connections = 5000
tcp_nodelay = true
send_buffer_size = 1048576    # 1MB
receive_buffer_size = 1048576  # 1MB
```

### Raft Tuning

Adjust for network latency:

```toml
[metadata]
# For low-latency networks (< 1ms)
election_timeout_min = "100ms"
election_timeout_max = "200ms"
heartbeat_interval = "30ms"

# For higher-latency networks (> 5ms)
election_timeout_min = "500ms"
election_timeout_max = "1000ms"
heartbeat_interval = "150ms"
```

## Troubleshooting

### Common Issues

#### No Raft Leader

**Symptoms**: Operations fail with "NotLeader" errors

**Diagnosis**:
```bash
strata cluster status
strata logs --level debug --filter raft
```

**Resolution**:
1. Ensure majority of metadata nodes are running
2. Check network connectivity between nodes
3. Verify clocks are synchronized (NTP)
4. Check for split-brain (network partition)

#### Slow Operations

**Symptoms**: High latency, timeouts

**Diagnosis**:
```bash
# Check metrics
curl localhost:9090/metrics | grep duration

# Check disk I/O
iostat -x 1

# Check network
iperf3 -c other-node
```

**Resolution**:
1. Check disk health and utilization
2. Check network bandwidth and latency
3. Review cache hit rates
4. Consider adding more nodes

#### Storage Full

**Symptoms**: Write failures, "CapacityExceeded" errors

**Diagnosis**:
```bash
strata storage status
df -h /var/lib/strata
```

**Resolution**:
1. Add more data nodes
2. Delete unnecessary data
3. Enable compression
4. Adjust erasure coding for better efficiency

### Log Analysis

View logs with filtering:

```bash
# Recent errors
strata logs --level error --since 1h

# Specific component
strata logs --filter metadata --level debug

# Follow logs
strata logs --follow
```

### Debug Mode

Enable verbose logging temporarily:

```bash
STRATA_LOG_LEVEL=debug strata-server ...
```

Or update running node:

```bash
strata config set log_level debug
```

## Maintenance Windows

### Rolling Restart

Restart nodes one at a time:

```bash
for node in strata-node-{1..3}; do
    echo "Restarting $node..."
    ssh $node systemctl restart strata
    sleep 30  # Wait for node to rejoin
    strata cluster status
done
```

### Upgrading

1. Download new version
2. Drain and stop one node at a time
3. Replace binary
4. Start node
5. Wait for node to rejoin and resync
6. Repeat for remaining nodes

```bash
for node in strata-node-{1..3}; do
    echo "Upgrading $node..."
    strata cluster drain $node
    ssh $node "systemctl stop strata && cp /tmp/strata-new /usr/local/bin/strata-server && systemctl start strata"
    strata cluster undrain $node
    sleep 60
done
```

## See Also

- [Deployment Guide](deployment.md) - Initial setup
- [Troubleshooting](troubleshooting.md) - Detailed troubleshooting
- [Configuration Reference](configuration.md) - All options
