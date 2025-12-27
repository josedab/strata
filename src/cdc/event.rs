// CDC Event Types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Change event type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeEventType {
    /// Object created
    Create,
    /// Object updated
    Update,
    /// Object deleted
    Delete,
    /// Object renamed/moved
    Rename,
    /// Metadata changed
    Metadata,
    /// Access control changed
    Acl,
    /// Bucket created
    BucketCreate,
    /// Bucket deleted
    BucketDelete,
    /// Lifecycle event
    Lifecycle,
    /// Replication event
    Replication,
}

impl std::fmt::Display for ChangeEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeEventType::Create => write!(f, "create"),
            ChangeEventType::Update => write!(f, "update"),
            ChangeEventType::Delete => write!(f, "delete"),
            ChangeEventType::Rename => write!(f, "rename"),
            ChangeEventType::Metadata => write!(f, "metadata"),
            ChangeEventType::Acl => write!(f, "acl"),
            ChangeEventType::BucketCreate => write!(f, "bucket_create"),
            ChangeEventType::BucketDelete => write!(f, "bucket_delete"),
            ChangeEventType::Lifecycle => write!(f, "lifecycle"),
            ChangeEventType::Replication => write!(f, "replication"),
        }
    }
}

/// Change event representing a data change
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Event ID (unique)
    pub id: String,
    /// Event type
    pub event_type: ChangeEventType,
    /// Event timestamp (millis since epoch)
    pub timestamp: u64,
    /// Source bucket
    pub bucket: String,
    /// Object key
    pub key: String,
    /// Version ID (if versioning enabled)
    pub version_id: Option<String>,
    /// Previous version ID (for updates)
    pub previous_version_id: Option<String>,
    /// Event payload
    pub payload: EventPayload,
    /// Principal who triggered the change
    pub principal: Option<String>,
    /// Source IP address
    pub source_ip: Option<String>,
    /// Request ID
    pub request_id: Option<String>,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

impl ChangeEvent {
    /// Creates a new change event
    pub fn new(
        event_type: ChangeEventType,
        bucket: impl Into<String>,
        key: impl Into<String>,
        sequence: u64,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            event_type,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            bucket: bucket.into(),
            key: key.into(),
            version_id: None,
            previous_version_id: None,
            payload: EventPayload::default(),
            principal: None,
            source_ip: None,
            request_id: None,
            sequence,
            metadata: HashMap::new(),
        }
    }

    /// Sets version ID
    pub fn with_version(mut self, version_id: impl Into<String>) -> Self {
        self.version_id = Some(version_id.into());
        self
    }

    /// Sets principal
    pub fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Sets payload
    pub fn with_payload(mut self, payload: EventPayload) -> Self {
        self.payload = payload;
        self
    }

    /// Adds metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Serializes to JSON
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    /// Serializes to JSON bytes
    pub fn to_json_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    /// Deserializes from JSON
    pub fn from_json(json: &str) -> serde_json::Result<Self> {
        serde_json::from_str(json)
    }
}

/// Event payload with change details
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventPayload {
    /// Object size (for creates/updates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Content type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// ETag/checksum
    #[serde(skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// Storage class
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Old key (for renames)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_key: Option<String>,
    /// New key (for renames)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_key: Option<String>,
    /// Changed fields (for metadata updates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_fields: Option<Vec<String>>,
    /// Old values (for updates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_values: Option<HashMap<String, String>>,
    /// New values (for updates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_values: Option<HashMap<String, String>>,
    /// Encryption info
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption: Option<EncryptionInfo>,
    /// Replication status
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_status: Option<String>,
}

/// Encryption information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionInfo {
    /// Encryption algorithm
    pub algorithm: String,
    /// Key ID
    pub key_id: Option<String>,
}

/// Event format for serialization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventFormat {
    /// JSON format
    Json,
    /// Avro format
    Avro,
    /// Protobuf format
    Protobuf,
    /// CloudEvents format
    CloudEvents,
}

impl Default for EventFormat {
    fn default() -> Self {
        EventFormat::Json
    }
}

/// CloudEvents wrapper for change events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudEvent {
    /// CloudEvents spec version
    #[serde(rename = "specversion")]
    pub spec_version: String,
    /// Event type
    #[serde(rename = "type")]
    pub event_type: String,
    /// Event source
    pub source: String,
    /// Event ID
    pub id: String,
    /// Event time (RFC3339)
    pub time: String,
    /// Data content type
    #[serde(rename = "datacontenttype")]
    pub data_content_type: String,
    /// Subject
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Event data
    pub data: ChangeEvent,
}

impl CloudEvent {
    /// Creates a CloudEvent from a ChangeEvent
    pub fn from_change_event(event: ChangeEvent, source: impl Into<String>) -> Self {
        let time = chrono::DateTime::from_timestamp_millis(event.timestamp as i64)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        Self {
            spec_version: "1.0".to_string(),
            event_type: format!("com.strata.{}", event.event_type),
            source: source.into(),
            id: event.id.clone(),
            time,
            data_content_type: "application/json".to_string(),
            subject: Some(format!("{}/{}", event.bucket, event.key)),
            data: event,
        }
    }
}

/// Event batch for bulk operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBatch {
    /// Batch ID
    pub id: String,
    /// Events in this batch
    pub events: Vec<ChangeEvent>,
    /// First sequence in batch
    pub first_sequence: u64,
    /// Last sequence in batch
    pub last_sequence: u64,
    /// Batch creation timestamp
    pub timestamp: u64,
}

impl EventBatch {
    /// Creates a new event batch
    pub fn new(events: Vec<ChangeEvent>) -> Self {
        let first_sequence = events.first().map(|e| e.sequence).unwrap_or(0);
        let last_sequence = events.last().map(|e| e.sequence).unwrap_or(0);

        Self {
            id: uuid::Uuid::new_v4().to_string(),
            events,
            first_sequence,
            last_sequence,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Returns the number of events
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Checks if batch is empty
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_change_event_creation() {
        let event = ChangeEvent::new(ChangeEventType::Create, "my-bucket", "path/to/file.txt", 1)
            .with_version("v1")
            .with_principal("user@example.com")
            .with_metadata("custom", "value");

        assert_eq!(event.event_type, ChangeEventType::Create);
        assert_eq!(event.bucket, "my-bucket");
        assert_eq!(event.key, "path/to/file.txt");
        assert_eq!(event.version_id, Some("v1".to_string()));
        assert_eq!(event.metadata.get("custom"), Some(&"value".to_string()));
    }

    #[test]
    fn test_event_serialization() {
        let event = ChangeEvent::new(ChangeEventType::Update, "bucket", "key", 42);
        let json = event.to_json().unwrap();
        let restored = ChangeEvent::from_json(&json).unwrap();

        assert_eq!(restored.event_type, event.event_type);
        assert_eq!(restored.bucket, event.bucket);
        assert_eq!(restored.sequence, event.sequence);
    }

    #[test]
    fn test_cloud_event() {
        let event = ChangeEvent::new(ChangeEventType::Delete, "bucket", "key", 1);
        let cloud_event = CloudEvent::from_change_event(event, "urn:strata:cluster:prod");

        assert_eq!(cloud_event.spec_version, "1.0");
        assert_eq!(cloud_event.event_type, "com.strata.delete");
        assert_eq!(cloud_event.subject, Some("bucket/key".to_string()));
    }

    #[test]
    fn test_event_batch() {
        let events = vec![
            ChangeEvent::new(ChangeEventType::Create, "b", "k1", 1),
            ChangeEvent::new(ChangeEventType::Create, "b", "k2", 2),
            ChangeEvent::new(ChangeEventType::Create, "b", "k3", 3),
        ];

        let batch = EventBatch::new(events);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch.first_sequence, 1);
        assert_eq!(batch.last_sequence, 3);
    }
}
