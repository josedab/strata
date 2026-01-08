// Tracing middleware for automatic span creation

use super::context::{SpanContext, TraceContext, Baggage};
use super::exporter::TracingExporter;
use super::sampler::{TracingSampler, SamplingParams};
use super::spans::{Span, SpanBuilder, SpanEvent, SpanKind, SpanStatus, semantic};
use std::sync::Arc;
use std::time::Instant;

/// Tracing middleware for HTTP/gRPC requests
pub struct TracingMiddleware {
    sampler: Arc<TracingSampler>,
    exporter: Arc<TracingExporter>,
    service_name: String,
    service_version: String,
}

impl TracingMiddleware {
    /// Creates a new tracing middleware
    pub fn new(
        sampler: TracingSampler,
        exporter: TracingExporter,
        service_name: impl Into<String>,
        service_version: impl Into<String>,
    ) -> Self {
        Self {
            sampler: Arc::new(sampler),
            exporter: Arc::new(exporter),
            service_name: service_name.into(),
            service_version: service_version.into(),
        }
    }

    /// Extracts trace context from HTTP headers
    pub fn extract_from_headers(&self, headers: &[(String, String)]) -> Option<TraceContext> {
        // Look for W3C traceparent header
        let traceparent = headers.iter()
            .find(|(k, _)| k.to_lowercase() == "traceparent")
            .map(|(_, v)| v.as_str())?;

        let span_context = SpanContext::from_traceparent(traceparent)?;

        // Look for baggage
        let baggage = headers.iter()
            .find(|(k, _)| k.to_lowercase() == "baggage")
            .map(|(_, v)| Baggage::from_header(v))
            .unwrap_or_default();

        Some(TraceContext {
            span: span_context,
            parent_span_id: None,
            baggage,
        })
    }

    /// Injects trace context into HTTP headers
    pub fn inject_into_headers(&self, ctx: &TraceContext, headers: &mut Vec<(String, String)>) {
        headers.push(("traceparent".to_string(), ctx.span.to_traceparent()));

        let baggage_header = ctx.baggage.to_header();
        if !baggage_header.is_empty() {
            headers.push(("baggage".to_string(), baggage_header));
        }

        // Also inject tracestate if present
        let tracestate = ctx.span.trace_state.to_header();
        if !tracestate.is_empty() {
            headers.push(("tracestate".to_string(), tracestate));
        }
    }

    /// Starts a server span from incoming request
    pub fn start_server_span(
        &self,
        operation: &str,
        parent: Option<&TraceContext>,
        attributes: &[(String, String)],
    ) -> Option<TracedRequest> {
        let parent_context = parent.map(|p| &p.span);

        // Create new span context
        let span_context = if let Some(parent) = parent_context {
            parent.child()
        } else {
            SpanContext::root()
        };

        // Make sampling decision
        let params = SamplingParams {
            parent_context,
            trace_id: &span_context.trace_id.0,
            span_name: operation,
            span_kind: SpanKind::Server,
            attributes,
        };

        let sampling = self.sampler.should_sample(params);
        if !sampling.decision.should_sample() {
            return None;
        }

        // Create span
        let mut span = SpanBuilder::new(operation)
            .server()
            .build();

        span.context = span_context.with_sampled(true);
        span.parent_span_id = parent_context.map(|p| p.span_id);

        // Add service attributes
        span.set_attribute(semantic::SERVICE_NAME, self.service_name.clone());
        span.set_attribute(semantic::SERVICE_VERSION, self.service_version.clone());

        // Add provided attributes
        for (key, value) in attributes {
            span.set_attribute(key.clone(), value.clone());
        }

        // Add sampling attributes
        for (key, value) in sampling.attributes {
            span.set_attribute(key, value);
        }

        Some(TracedRequest {
            span,
            exporter: Arc::clone(&self.exporter),
            sampler: Arc::clone(&self.sampler),
            start: Instant::now(),
        })
    }

    /// Starts a client span for outgoing request
    pub fn start_client_span(
        &self,
        operation: &str,
        parent: &TraceContext,
        attributes: &[(String, String)],
    ) -> Option<TracedRequest> {
        // Make sampling decision based on parent
        let params = SamplingParams {
            parent_context: Some(&parent.span),
            trace_id: &parent.span.trace_id.0,
            span_name: operation,
            span_kind: SpanKind::Client,
            attributes,
        };

        let sampling = self.sampler.should_sample(params);
        if !sampling.decision.should_sample() {
            return None;
        }

        // Create span as child of parent
        let mut span = SpanBuilder::new(operation)
            .client()
            .parent(parent.span.clone())
            .build();

        // Add attributes
        for (key, value) in attributes {
            span.set_attribute(key.clone(), value.clone());
        }

        Some(TracedRequest {
            span,
            exporter: Arc::clone(&self.exporter),
            sampler: Arc::clone(&self.sampler),
            start: Instant::now(),
        })
    }

    /// Creates an internal span
    pub fn start_internal_span(
        &self,
        operation: &str,
        parent: Option<&TraceContext>,
    ) -> Option<TracedRequest> {
        let parent_context = parent.map(|p| &p.span);

        // For internal spans, follow parent sampling decision
        if let Some(parent) = parent_context {
            if !parent.is_sampled() {
                return None;
            }
        }

        let span_context = if let Some(parent) = parent_context {
            parent.child()
        } else {
            SpanContext::root()
        };

        let mut span = Span::new(operation, span_context.with_sampled(true));
        span.parent_span_id = parent_context.map(|p| p.span_id);

        Some(TracedRequest {
            span,
            exporter: Arc::clone(&self.exporter),
            sampler: Arc::clone(&self.sampler),
            start: Instant::now(),
        })
    }
}

/// A traced request that automatically reports span on drop
pub struct TracedRequest {
    span: Span,
    exporter: Arc<TracingExporter>,
    sampler: Arc<TracingSampler>,
    start: Instant,
}

impl TracedRequest {
    /// Gets the span context for propagation
    pub fn context(&self) -> SpanContext {
        self.span.context.clone()
    }

    /// Gets a full trace context
    pub fn trace_context(&self) -> TraceContext {
        TraceContext {
            span: self.span.context.clone(),
            parent_span_id: self.span.parent_span_id,
            baggage: Baggage::new(),
        }
    }

    /// Sets an attribute
    pub fn set_attribute(&mut self, key: impl Into<String>, value: impl Into<super::spans::AttributeValue>) {
        self.span.set_attribute(key, value);
    }

    /// Adds an event
    pub fn add_event(&mut self, name: impl Into<String>) {
        self.span.add_event(SpanEvent::new(name));
    }

    /// Records an exception
    pub fn record_exception(&mut self, error: &dyn std::error::Error) {
        let event = SpanEvent::new("exception")
            .with_attribute("exception.type", std::any::type_name_of_val(error))
            .with_attribute("exception.message", error.to_string());

        self.span.add_event(event);
        self.span.set_status(SpanStatus::Error {
            message: error.to_string(),
        });
    }

    /// Sets status to OK
    pub fn set_ok(&mut self) {
        self.span.set_status(SpanStatus::Ok);
    }

    /// Sets status to error
    pub fn set_error(&mut self, message: impl Into<String>) {
        self.span.set_status(SpanStatus::Error {
            message: message.into(),
        });
    }

    /// Sets HTTP-specific attributes
    pub fn set_http_attributes(
        &mut self,
        method: &str,
        url: &str,
        status_code: Option<u16>,
    ) {
        self.span.set_attribute(semantic::HTTP_METHOD, method.to_string());
        self.span.set_attribute(semantic::HTTP_URL, url.to_string());
        if let Some(code) = status_code {
            self.span.set_attribute(semantic::HTTP_STATUS_CODE, code as i64);
        }
    }

    /// Sets gRPC-specific attributes
    pub fn set_grpc_attributes(
        &mut self,
        service: &str,
        method: &str,
        status_code: Option<i32>,
    ) {
        self.span.set_attribute(semantic::RPC_SYSTEM, "grpc".to_string());
        self.span.set_attribute(semantic::RPC_SERVICE, service.to_string());
        self.span.set_attribute(semantic::RPC_METHOD, method.to_string());
        if let Some(code) = status_code {
            self.span.set_attribute(semantic::RPC_GRPC_STATUS_CODE, code as i64);
        }
    }

    /// Sets Strata-specific attributes
    pub fn set_strata_attributes(
        &mut self,
        operation: &str,
        bucket: Option<&str>,
        key: Option<&str>,
    ) {
        self.span.set_attribute(semantic::STRATA_OPERATION, operation.to_string());
        if let Some(bucket) = bucket {
            self.span.set_attribute(semantic::STRATA_BUCKET, bucket.to_string());
        }
        if let Some(key) = key {
            self.span.set_attribute(semantic::STRATA_OBJECT_KEY, key.to_string());
        }
    }

    /// Finishes the span and reports it
    pub fn finish(mut self) {
        let duration = self.start.elapsed();
        self.span.end();

        // Check for tail-based sampling
        let is_error = matches!(self.span.status, SpanStatus::Error { .. });
        let should_sample_error = is_error && self.sampler.should_sample_error();
        let should_sample_slow = self.sampler.should_sample_slow(duration);

        if should_sample_error || should_sample_slow {
            // Force sample this span
            self.span.context = self.span.context.clone().with_sampled(true);
        }

        // Export the span (clone to allow moving out)
        self.exporter.export(self.span.clone());
    }
}

/// Strata operation tracing helpers
pub struct StrataTracer {
    middleware: Arc<TracingMiddleware>,
}

impl StrataTracer {
    pub fn new(middleware: Arc<TracingMiddleware>) -> Self {
        Self { middleware }
    }

    /// Traces a bucket operation
    pub fn trace_bucket_op(
        &self,
        operation: &str,
        bucket: &str,
        parent: Option<&TraceContext>,
    ) -> Option<TracedRequest> {
        let mut request = self.middleware.start_internal_span(
            &format!("strata.bucket.{}", operation),
            parent,
        )?;

        request.set_strata_attributes(operation, Some(bucket), None);
        Some(request)
    }

    /// Traces an object operation
    pub fn trace_object_op(
        &self,
        operation: &str,
        bucket: &str,
        key: &str,
        parent: Option<&TraceContext>,
    ) -> Option<TracedRequest> {
        let mut request = self.middleware.start_internal_span(
            &format!("strata.object.{}", operation),
            parent,
        )?;

        request.set_strata_attributes(operation, Some(bucket), Some(key));
        Some(request)
    }

    /// Traces a metadata operation
    pub fn trace_metadata_op(
        &self,
        operation: &str,
        parent: Option<&TraceContext>,
    ) -> Option<TracedRequest> {
        self.middleware.start_internal_span(
            &format!("strata.metadata.{}", operation),
            parent,
        )
    }

    /// Traces a data operation
    pub fn trace_data_op(
        &self,
        operation: &str,
        chunk_id: &str,
        parent: Option<&TraceContext>,
    ) -> Option<TracedRequest> {
        let mut request = self.middleware.start_internal_span(
            &format!("strata.data.{}", operation),
            parent,
        )?;

        request.set_attribute(semantic::STRATA_CHUNK_ID, chunk_id.to_string());
        Some(request)
    }

    /// Traces a Raft operation
    pub fn trace_raft_op(
        &self,
        operation: &str,
        node_id: u64,
        parent: Option<&TraceContext>,
    ) -> Option<TracedRequest> {
        let mut request = self.middleware.start_internal_span(
            &format!("strata.raft.{}", operation),
            parent,
        )?;

        request.set_attribute(semantic::STRATA_NODE_ID, node_id as i64);
        Some(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otel::sampler::SamplerConfig;
    use crate::otel::exporter::ExporterConfig;

    fn create_middleware() -> TracingMiddleware {
        let sampler = TracingSampler::always_on();
        let exporter = TracingExporter::new(ExporterConfig {
            exporter_type: super::super::exporter::ExporterType::Console,
            ..Default::default()
        }).unwrap();

        TracingMiddleware::new(sampler, exporter, "test-service", "1.0.0")
    }

    #[tokio::test]
    async fn test_extract_traceparent() {
        let middleware = create_middleware();

        let headers = vec![
            ("traceparent".to_string(), "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
        ];

        let ctx = middleware.extract_from_headers(&headers).unwrap();
        assert!(ctx.span.is_sampled());
        assert!(ctx.span.is_remote);
    }

    #[tokio::test]
    async fn test_inject_headers() {
        let middleware = create_middleware();
        let ctx = TraceContext::root();

        let mut headers = Vec::new();
        middleware.inject_into_headers(&ctx, &mut headers);

        assert!(headers.iter().any(|(k, _)| k == "traceparent"));
    }

    #[tokio::test]
    async fn test_start_server_span() {
        let middleware = create_middleware();

        let request = middleware.start_server_span(
            "GET /api/buckets",
            None,
            &[],
        );

        assert!(request.is_some());
        let request = request.unwrap();
        assert!(request.context().is_valid());
    }

    #[tokio::test]
    async fn test_start_client_span() {
        let middleware = create_middleware();
        // Create a sampled parent context - client spans respect parent sampling
        let parent = TraceContext {
            span: SpanContext::root().with_sampled(true),
            parent_span_id: None,
            baggage: Baggage::new(),
        };

        let request = middleware.start_client_span(
            "rpc.GetMetadata",
            &parent,
            &[],
        );

        assert!(request.is_some());
        let request = request.unwrap();
        assert_eq!(request.context().trace_id, parent.span.trace_id);
    }

    #[tokio::test]
    async fn test_traced_request_attributes() {
        let middleware = create_middleware();

        let mut request = middleware.start_server_span("test", None, &[]).unwrap();

        request.set_http_attributes("GET", "http://example.com", Some(200));
        request.set_ok();

        // Finish and export
        request.finish();
    }
}
