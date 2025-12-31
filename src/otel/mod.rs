// Distributed Tracing with OpenTelemetry
//
// Provides comprehensive distributed tracing for Strata operations.
// Integrates with OpenTelemetry for vendor-neutral observability.

pub mod context;
pub mod exporter;
pub mod middleware;
pub mod sampler;
pub mod spans;

pub use context::{TraceContext, SpanContext, Baggage};
pub use exporter::{TracingExporter, ExporterConfig};
pub use middleware::TracingMiddleware;
pub use sampler::{TracingSampler, SamplerConfig};
pub use spans::{SpanBuilder, SpanKind, SpanStatus};
