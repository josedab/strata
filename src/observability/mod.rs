//! Observability module for Strata.
//!
//! Provides logging, metrics, tracing, and monitoring capabilities.

pub mod distributed_tracing;

pub use self::distributed_tracing::{
    Span, SpanEvent, SpanId, SpanStatus, SpanValue, TraceContext, TraceId, Traceable,
};

use crate::config::ObservabilityConfig;
use crate::error::{Result, StrataError};
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::net::TcpListener;
use ::tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Initialize observability (logging and metrics).
pub fn init(config: &ObservabilityConfig) -> Result<()> {
    // Initialize tracing
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.log_level));

    let subscriber = tracing_subscriber::registry().with(filter);

    if config.json_logs {
        subscriber
            .with(fmt::layer().json())
            .try_init()
            .map_err(|e| StrataError::Internal(format!("Failed to init logging: {}", e)))?;
    } else {
        subscriber
            .with(fmt::layer())
            .try_init()
            .map_err(|e| StrataError::Internal(format!("Failed to init logging: {}", e)))?;
    }

    info!("Observability initialized");
    Ok(())
}

/// Run the Prometheus metrics server.
pub async fn run_metrics_server(config: ObservabilityConfig) -> Result<()> {
    let builder = PrometheusBuilder::new();

    let handle = builder
        .install_recorder()
        .map_err(|e| StrataError::Internal(format!("Failed to install metrics recorder: {}", e)))?;

    // Register some basic metrics
    register_metrics();

    // Start HTTP server for metrics
    let app = axum::Router::new()
        .route("/metrics", axum::routing::get(move || async move {
            handle.render()
        }))
        .route("/health", axum::routing::get(|| async { "OK" }));

    let listener = TcpListener::bind(config.metrics_addr).await?;
    info!(addr = %config.metrics_addr, "Metrics server listening");

    axum::serve(listener, app)
        .await
        .map_err(|e| StrataError::Network(e.to_string()))?;

    Ok(())
}

/// Register standard metrics.
fn register_metrics() {
    // Cluster metrics
    gauge!("strata_cluster_nodes_total").set(0.0);
    gauge!("strata_cluster_nodes_online").set(0.0);
    gauge!("strata_cluster_capacity_bytes").set(0.0);
    gauge!("strata_cluster_used_bytes").set(0.0);

    // Raft metrics
    gauge!("strata_raft_term").set(0.0);
    gauge!("strata_raft_commit_index").set(0.0);
    counter!("strata_raft_elections_total").absolute(0);
    counter!("strata_raft_proposals_total").absolute(0);

    // Metadata metrics
    gauge!("strata_metadata_inodes_total").set(0.0);
    gauge!("strata_metadata_chunks_total").set(0.0);
    counter!("strata_metadata_ops_total").absolute(0);

    // Data server metrics
    gauge!("strata_data_shards_total").set(0.0);
    counter!("strata_data_reads_total").absolute(0);
    counter!("strata_data_writes_total").absolute(0);
    counter!("strata_data_bytes_read").absolute(0);
    counter!("strata_data_bytes_written").absolute(0);

    // S3 gateway metrics
    counter!("strata_s3_requests_total").absolute(0);
    counter!("strata_s3_errors_total").absolute(0);
}

/// Record a metadata operation.
pub fn record_metadata_op(op_type: &str) {
    counter!("strata_metadata_ops_total", "type" => op_type.to_string()).increment(1);
}

/// Record a data read.
pub fn record_data_read(bytes: u64) {
    counter!("strata_data_reads_total").increment(1);
    counter!("strata_data_bytes_read").increment(bytes);
}

/// Record a data write.
pub fn record_data_write(bytes: u64) {
    counter!("strata_data_writes_total").increment(1);
    counter!("strata_data_bytes_written").increment(bytes);
}

/// Record an S3 request.
pub fn record_s3_request(method: &str, status: u16) {
    counter!(
        "strata_s3_requests_total",
        "method" => method.to_string(),
        "status" => status.to_string()
    ).increment(1);

    if status >= 400 {
        counter!("strata_s3_errors_total").increment(1);
    }
}

/// Update cluster metrics.
pub fn update_cluster_metrics(nodes_total: usize, nodes_online: usize, capacity: u64, used: u64) {
    gauge!("strata_cluster_nodes_total").set(nodes_total as f64);
    gauge!("strata_cluster_nodes_online").set(nodes_online as f64);
    gauge!("strata_cluster_capacity_bytes").set(capacity as f64);
    gauge!("strata_cluster_used_bytes").set(used as f64);
}

/// Update Raft metrics.
pub fn update_raft_metrics(term: u64, commit_index: u64) {
    gauge!("strata_raft_term").set(term as f64);
    gauge!("strata_raft_commit_index").set(commit_index as f64);
}
