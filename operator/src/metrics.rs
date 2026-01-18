//! Prometheus metrics for the Strata operator

use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, CounterVec, GaugeVec,
    HistogramVec, TextEncoder,
};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::info;

lazy_static::lazy_static! {
    /// Total reconciliations
    pub static ref RECONCILIATIONS: CounterVec = register_counter_vec!(
        "strata_operator_reconciliations_total",
        "Total number of reconciliations",
        &["cluster", "result"]
    ).unwrap();

    /// Reconciliation duration
    pub static ref RECONCILIATION_DURATION: HistogramVec = register_histogram_vec!(
        "strata_operator_reconciliation_duration_seconds",
        "Duration of reconciliations",
        &["cluster"]
    ).unwrap();

    /// Number of clusters
    pub static ref CLUSTER_COUNT: GaugeVec = register_gauge_vec!(
        "strata_operator_clusters",
        "Number of Strata clusters",
        &["namespace", "phase"]
    ).unwrap();

    /// Number of nodes
    pub static ref NODE_COUNT: GaugeVec = register_gauge_vec!(
        "strata_operator_nodes",
        "Number of nodes in clusters",
        &["cluster", "role", "ready"]
    ).unwrap();

    /// Operator errors
    pub static ref ERRORS: CounterVec = register_counter_vec!(
        "strata_operator_errors_total",
        "Total number of errors",
        &["type"]
    ).unwrap();
}

/// Run the metrics server
pub async fn run_metrics_server(port: u16) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await.unwrap();
    info!(port = %port, "Metrics server started");

    loop {
        if let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;

                // Encode metrics
                let encoder = TextEncoder::new();
                let metric_families = prometheus::gather();
                let metrics = encoder.encode_to_string(&metric_families).unwrap_or_default();

                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
                    metrics.len(),
                    metrics
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    }
}

/// Record a successful reconciliation
pub fn record_reconciliation_success(cluster: &str, duration_secs: f64) {
    RECONCILIATIONS.with_label_values(&[cluster, "success"]).inc();
    RECONCILIATION_DURATION
        .with_label_values(&[cluster])
        .observe(duration_secs);
}

/// Record a failed reconciliation
pub fn record_reconciliation_failure(cluster: &str, duration_secs: f64) {
    RECONCILIATIONS.with_label_values(&[cluster, "failure"]).inc();
    RECONCILIATION_DURATION
        .with_label_values(&[cluster])
        .observe(duration_secs);
}

/// Update cluster count
pub fn set_cluster_count(namespace: &str, phase: &str, count: f64) {
    CLUSTER_COUNT.with_label_values(&[namespace, phase]).set(count);
}

/// Update node count
pub fn set_node_count(cluster: &str, role: &str, ready: bool, count: f64) {
    let ready_str = if ready { "true" } else { "false" };
    NODE_COUNT.with_label_values(&[cluster, role, ready_str]).set(count);
}

/// Record an error
pub fn record_error(error_type: &str) {
    ERRORS.with_label_values(&[error_type]).inc();
}
