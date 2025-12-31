// Trace exporters for different backends

use super::spans::Span;
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, warn};

/// Exporter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExporterConfig {
    /// Exporter type
    pub exporter_type: ExporterType,
    /// Endpoint URL
    pub endpoint: String,
    /// Export batch size
    pub batch_size: usize,
    /// Export interval
    #[serde(with = "crate::config::humantime_serde")]
    pub export_interval: Duration,
    /// Export timeout
    #[serde(with = "crate::config::humantime_serde")]
    pub export_timeout: Duration,
    /// Maximum queue size
    pub max_queue_size: usize,
    /// Enable compression
    pub compression: bool,
    /// Headers to include
    pub headers: Vec<(String, String)>,
    /// TLS configuration
    pub tls: Option<TlsConfig>,
    /// Retry configuration
    pub retry: RetryConfig,
}

impl Default for ExporterConfig {
    fn default() -> Self {
        Self {
            exporter_type: ExporterType::Otlp,
            endpoint: "http://localhost:4317".to_string(),
            batch_size: 512,
            export_interval: Duration::from_secs(5),
            export_timeout: Duration::from_secs(30),
            max_queue_size: 2048,
            compression: true,
            headers: Vec::new(),
            tls: None,
            retry: RetryConfig::default(),
        }
    }
}

/// Exporter type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExporterType {
    /// OTLP (OpenTelemetry Protocol)
    Otlp,
    /// Jaeger
    Jaeger,
    /// Zipkin
    Zipkin,
    /// Console (for debugging)
    Console,
    /// None (disabled)
    None,
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub ca_cert: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
    pub insecure_skip_verify: bool,
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    pub enabled: bool,
    pub max_retries: u32,
    #[serde(with = "crate::config::humantime_serde")]
    pub initial_delay: Duration,
    #[serde(with = "crate::config::humantime_serde")]
    pub max_delay: Duration,
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
        }
    }
}

/// Export result
#[derive(Debug)]
pub struct ExportResult {
    pub success: bool,
    pub spans_exported: usize,
    pub spans_failed: usize,
    pub duration: Duration,
    pub error: Option<String>,
}

/// Trait for span exporters
#[async_trait::async_trait]
pub trait SpanExporter: Send + Sync {
    /// Exports a batch of spans
    async fn export(&self, spans: Vec<Span>) -> Result<ExportResult>;

    /// Shuts down the exporter
    async fn shutdown(&self) -> Result<()>;

    /// Forces a flush
    async fn force_flush(&self) -> Result<()>;
}

/// OTLP exporter (OpenTelemetry Protocol)
pub struct OtlpExporter {
    config: ExporterConfig,
    client: reqwest::Client,
}

impl OtlpExporter {
    pub fn new(config: ExporterConfig) -> Result<Self> {
        let client_builder = reqwest::Client::builder()
            .timeout(config.export_timeout);

        // Note: gzip compression requires the "gzip" feature on reqwest
        // For now, compression is handled at the request level if needed
        let _ = config.compression; // Silence unused warning

        let client = client_builder.build()
            .map_err(|e| StrataError::Internal(e.to_string()))?;

        Ok(Self { config, client })
    }
}

#[async_trait::async_trait]
impl SpanExporter for OtlpExporter {
    async fn export(&self, spans: Vec<Span>) -> Result<ExportResult> {
        let start = std::time::Instant::now();
        let count = spans.len();

        // Convert to OTLP format
        let payload = convert_to_otlp(spans);

        let mut request = self.client.post(&self.config.endpoint)
            .header("Content-Type", "application/json");

        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }

        let response = request.json(&payload).send().await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(ExportResult {
                success: true,
                spans_exported: count,
                spans_failed: 0,
                duration: start.elapsed(),
                error: None,
            })
        } else {
            let status = response.status();
            let error = response.text().await.unwrap_or_default();
            Ok(ExportResult {
                success: false,
                spans_exported: 0,
                spans_failed: count,
                duration: start.elapsed(),
                error: Some(format!("{}: {}", status, error)),
            })
        }
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn force_flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Jaeger exporter
pub struct JaegerExporter {
    config: ExporterConfig,
    client: reqwest::Client,
}

impl JaegerExporter {
    pub fn new(config: ExporterConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(config.export_timeout)
            .build()
            .map_err(|e| StrataError::Internal(e.to_string()))?;

        Ok(Self { config, client })
    }
}

#[async_trait::async_trait]
impl SpanExporter for JaegerExporter {
    async fn export(&self, spans: Vec<Span>) -> Result<ExportResult> {
        let start = std::time::Instant::now();
        let count = spans.len();

        // Convert to Jaeger Thrift format
        let payload = convert_to_jaeger(spans);

        let response = self.client.post(&self.config.endpoint)
            .header("Content-Type", "application/x-thrift")
            .body(payload)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(ExportResult {
                success: true,
                spans_exported: count,
                spans_failed: 0,
                duration: start.elapsed(),
                error: None,
            })
        } else {
            Ok(ExportResult {
                success: false,
                spans_exported: 0,
                spans_failed: count,
                duration: start.elapsed(),
                error: Some(response.status().to_string()),
            })
        }
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn force_flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Zipkin exporter
pub struct ZipkinExporter {
    config: ExporterConfig,
    client: reqwest::Client,
}

impl ZipkinExporter {
    pub fn new(config: ExporterConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(config.export_timeout)
            .build()
            .map_err(|e| StrataError::Internal(e.to_string()))?;

        Ok(Self { config, client })
    }
}

#[async_trait::async_trait]
impl SpanExporter for ZipkinExporter {
    async fn export(&self, spans: Vec<Span>) -> Result<ExportResult> {
        let start = std::time::Instant::now();
        let count = spans.len();

        // Convert to Zipkin format
        let payload = convert_to_zipkin(spans);

        let response = self.client.post(&self.config.endpoint)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .map_err(|e| StrataError::Network(e.to_string()))?;

        if response.status().is_success() {
            Ok(ExportResult {
                success: true,
                spans_exported: count,
                spans_failed: 0,
                duration: start.elapsed(),
                error: None,
            })
        } else {
            Ok(ExportResult {
                success: false,
                spans_exported: 0,
                spans_failed: count,
                duration: start.elapsed(),
                error: Some(response.status().to_string()),
            })
        }
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn force_flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Console exporter (for debugging)
pub struct ConsoleExporter;

#[async_trait::async_trait]
impl SpanExporter for ConsoleExporter {
    async fn export(&self, spans: Vec<Span>) -> Result<ExportResult> {
        let start = std::time::Instant::now();
        let count = spans.len();

        for span in spans {
            println!("Span: {} [{}]", span.name, span.context.trace_id);
            println!("  Kind: {:?}", span.kind);
            println!("  Duration: {:?}", span.duration());
            println!("  Status: {:?}", span.status);
            for (key, value) in &span.attributes {
                println!("  {}: {:?}", key, value);
            }
            println!();
        }

        Ok(ExportResult {
            success: true,
            spans_exported: count,
            spans_failed: 0,
            duration: start.elapsed(),
            error: None,
        })
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn force_flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Batch span processor
pub struct BatchSpanProcessor {
    sender: mpsc::Sender<Span>,
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl BatchSpanProcessor {
    pub fn new(exporter: Arc<dyn SpanExporter>, config: ExporterConfig) -> Self {
        let (sender, receiver) = mpsc::channel(config.max_queue_size);
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);

        tokio::spawn(Self::run_processor(
            receiver,
            shutdown_rx,
            exporter,
            config,
        ));

        Self { sender, shutdown }
    }

    async fn run_processor(
        mut receiver: mpsc::Receiver<Span>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        exporter: Arc<dyn SpanExporter>,
        config: ExporterConfig,
    ) {
        let mut batch = Vec::with_capacity(config.batch_size);
        let mut interval = tokio::time::interval(config.export_interval);

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        // Export remaining spans
                        while let Ok(span) = receiver.try_recv() {
                            batch.push(span);
                        }
                        if !batch.is_empty() {
                            let _ = exporter.export(std::mem::take(&mut batch)).await;
                        }
                        let _ = exporter.shutdown().await;
                        break;
                    }
                }
                Some(span) = receiver.recv() => {
                    batch.push(span);
                    if batch.len() >= config.batch_size {
                        let spans = std::mem::take(&mut batch);
                        if let Err(e) = exporter.export(spans).await {
                            error!(error = %e, "Failed to export spans");
                        }
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        let spans = std::mem::take(&mut batch);
                        if let Err(e) = exporter.export(spans).await {
                            error!(error = %e, "Failed to export spans");
                        }
                    }
                }
            }
        }
    }

    /// Submits a span for export
    pub fn submit(&self, span: Span) {
        if let Err(e) = self.sender.try_send(span) {
            warn!("Failed to queue span for export: {}", e);
        }
    }

    /// Shuts down the processor
    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }
}

/// Main tracing exporter
pub struct TracingExporter {
    processor: BatchSpanProcessor,
}

impl TracingExporter {
    /// Creates a new tracing exporter
    pub fn new(config: ExporterConfig) -> Result<Self> {
        let exporter: Arc<dyn SpanExporter> = match config.exporter_type {
            ExporterType::Otlp => Arc::new(OtlpExporter::new(config.clone())?),
            ExporterType::Jaeger => Arc::new(JaegerExporter::new(config.clone())?),
            ExporterType::Zipkin => Arc::new(ZipkinExporter::new(config.clone())?),
            ExporterType::Console => Arc::new(ConsoleExporter),
            ExporterType::None => return Err(StrataError::InvalidOperation(
                "Exporter type is None".to_string()
            )),
        };

        let processor = BatchSpanProcessor::new(exporter, config);

        Ok(Self { processor })
    }

    /// Submits a span for export
    pub fn export(&self, span: Span) {
        self.processor.submit(span);
    }

    /// Shuts down the exporter
    pub fn shutdown(&self) {
        self.processor.shutdown();
    }
}

// Format conversion functions

fn convert_to_otlp(spans: Vec<Span>) -> serde_json::Value {
    let resource_spans = spans.into_iter().map(|span| {
        serde_json::json!({
            "resource": {
                "attributes": []
            },
            "scopeSpans": [{
                "scope": {
                    "name": "strata"
                },
                "spans": [{
                    "traceId": span.context.trace_id.to_hex(),
                    "spanId": span.context.span_id.to_hex(),
                    "parentSpanId": span.parent_span_id.map(|id| id.to_hex()),
                    "name": span.name,
                    "kind": match span.kind {
                        super::spans::SpanKind::Internal => 1,
                        super::spans::SpanKind::Server => 2,
                        super::spans::SpanKind::Client => 3,
                        super::spans::SpanKind::Producer => 4,
                        super::spans::SpanKind::Consumer => 5,
                    },
                    "startTimeUnixNano": span.start_time.duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default().as_nanos(),
                    "endTimeUnixNano": span.end_time.unwrap_or(span.start_time)
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default().as_nanos(),
                    "attributes": span.attributes.iter().map(|(k, v)| {
                        serde_json::json!({
                            "key": k,
                            "value": v
                        })
                    }).collect::<Vec<_>>(),
                    "events": span.events.iter().map(|e| {
                        serde_json::json!({
                            "name": e.name,
                            "timeUnixNano": e.timestamp.duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default().as_nanos(),
                            "attributes": e.attributes.iter().map(|(k, v)| {
                                serde_json::json!({
                                    "key": k,
                                    "value": v
                                })
                            }).collect::<Vec<_>>()
                        })
                    }).collect::<Vec<_>>(),
                    "status": match &span.status {
                        super::spans::SpanStatus::Unset => serde_json::json!({}),
                        super::spans::SpanStatus::Ok => serde_json::json!({"code": 1}),
                        super::spans::SpanStatus::Error { message } => {
                            serde_json::json!({"code": 2, "message": message})
                        }
                    }
                }]
            }]
        })
    }).collect::<Vec<_>>();

    serde_json::json!({
        "resourceSpans": resource_spans
    })
}

fn convert_to_jaeger(spans: Vec<Span>) -> Vec<u8> {
    // Simplified - would use Thrift serialization
    serde_json::to_vec(&spans).unwrap_or_default()
}

fn convert_to_zipkin(spans: Vec<Span>) -> Vec<serde_json::Value> {
    spans.into_iter().map(|span| {
        serde_json::json!({
            "traceId": span.context.trace_id.to_hex(),
            "id": span.context.span_id.to_hex(),
            "parentId": span.parent_span_id.map(|id| id.to_hex()),
            "name": span.name,
            "kind": match span.kind {
                super::spans::SpanKind::Server => "SERVER",
                super::spans::SpanKind::Client => "CLIENT",
                super::spans::SpanKind::Producer => "PRODUCER",
                super::spans::SpanKind::Consumer => "CONSUMER",
                _ => "INTERNAL",
            },
            "timestamp": span.start_time.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default().as_micros(),
            "duration": span.duration().unwrap_or_default().as_micros(),
            "tags": span.attributes,
        })
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::context::SpanContext;

    #[tokio::test]
    async fn test_console_exporter() {
        let exporter = ConsoleExporter;

        let span = Span::new("test-span", SpanContext::root());
        let result = exporter.export(vec![span]).await.unwrap();

        assert!(result.success);
        assert_eq!(result.spans_exported, 1);
    }

    #[test]
    fn test_otlp_conversion() {
        let span = Span::new("test", SpanContext::root());
        let payload = convert_to_otlp(vec![span]);

        assert!(payload.get("resourceSpans").is_some());
    }

    #[test]
    fn test_zipkin_conversion() {
        let span = Span::new("test", SpanContext::root());
        let payload = convert_to_zipkin(vec![span]);

        assert_eq!(payload.len(), 1);
        assert!(payload[0].get("traceId").is_some());
    }
}
