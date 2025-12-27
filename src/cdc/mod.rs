//! Change Data Capture (CDC) for Strata
//!
//! Captures and streams data changes in real-time to external systems.
//! Supports Kafka, Pulsar, webhooks, and custom connectors.

pub mod capture;
pub mod connector;
pub mod event;
pub mod stream;
pub mod subscription;

pub use capture::{CaptureMode, ChangeCapture, CaptureConfig};
pub use connector::{Connector, ConnectorConfig, ConnectorType};
pub use event::{ChangeEvent, ChangeEventType, EventPayload};
pub use stream::{EventStream, StreamConfig};
pub use subscription::{Subscription, SubscriptionFilter, SubscriptionManager};
