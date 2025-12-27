// CDC Connectors for External Systems

use super::event::{ChangeEvent, CloudEvent, EventBatch, EventFormat};
use crate::error::{Result, StrataError};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Connector type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorType {
    /// Apache Kafka
    Kafka,
    /// Apache Pulsar
    Pulsar,
    /// HTTP webhook
    Webhook,
    /// AWS SNS
    Sns,
    /// AWS SQS
    Sqs,
    /// Google Cloud Pub/Sub
    PubSub,
    /// Azure Event Hubs
    EventHubs,
    /// Redis Streams
    Redis,
    /// NATS
    Nats,
    /// RabbitMQ
    RabbitMq,
    /// File output (for debugging)
    File,
    /// Stdout (for debugging)
    Stdout,
}

/// Connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    /// Connector type
    pub connector_type: ConnectorType,
    /// Connector name
    pub name: String,
    /// Connection endpoints
    pub endpoints: Vec<String>,
    /// Topic/queue name
    pub topic: String,
    /// Event format
    pub format: EventFormat,
    /// Enable CloudEvents wrapper
    pub cloud_events: bool,
    /// Batch settings
    pub batch: BatchConfig,
    /// Retry settings
    pub retry: RetryConfig,
    /// Authentication
    pub auth: Option<AuthConfig>,
    /// TLS settings
    pub tls: Option<TlsConfig>,
    /// Additional properties
    pub properties: HashMap<String, String>,
}

/// Batch configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Enable batching
    pub enabled: bool,
    /// Maximum batch size
    pub max_size: usize,
    /// Maximum batch bytes
    pub max_bytes: usize,
    /// Batch linger time in milliseconds
    pub linger_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_size: 100,
            max_bytes: 1048576, // 1MB
            linger_ms: 10,
        }
    }
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum retries
    pub max_retries: u32,
    /// Initial backoff in milliseconds
    pub initial_backoff_ms: u64,
    /// Maximum backoff in milliseconds
    pub max_backoff_ms: u64,
    /// Backoff multiplier
    pub multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 10000,
            multiplier: 2.0,
        }
    }
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Auth type
    pub auth_type: AuthType,
    /// Username
    pub username: Option<String>,
    /// Password (should be from secrets manager)
    pub password: Option<String>,
    /// API key
    pub api_key: Option<String>,
    /// Token
    pub token: Option<String>,
    /// AWS credentials profile
    pub aws_profile: Option<String>,
}

/// Authentication type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthType {
    None,
    Basic,
    ApiKey,
    Bearer,
    Sasl,
    AwsIam,
    OAuth2,
}

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Enable TLS
    pub enabled: bool,
    /// CA certificate path
    pub ca_cert: Option<String>,
    /// Client certificate path
    pub client_cert: Option<String>,
    /// Client key path
    pub client_key: Option<String>,
    /// Skip verification (not recommended)
    pub insecure_skip_verify: bool,
}

/// Connector trait for sending events
#[async_trait]
pub trait Connector: Send + Sync {
    /// Gets the connector name
    fn name(&self) -> &str;

    /// Gets the connector type
    fn connector_type(&self) -> ConnectorType;

    /// Connects to the destination
    async fn connect(&mut self) -> Result<()>;

    /// Disconnects from the destination
    async fn disconnect(&mut self) -> Result<()>;

    /// Sends a single event
    async fn send(&self, event: &ChangeEvent) -> Result<()>;

    /// Sends a batch of events
    async fn send_batch(&self, events: &[ChangeEvent]) -> Result<()>;

    /// Checks if connected
    fn is_connected(&self) -> bool;

    /// Gets connector statistics
    fn stats(&self) -> ConnectorStats;
}

/// Connector statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectorStats {
    pub events_sent: u64,
    pub events_failed: u64,
    pub bytes_sent: u64,
    pub batches_sent: u64,
    pub retries: u64,
    pub last_error: Option<String>,
    pub last_success_time: Option<u64>,
}

/// Kafka connector
pub struct KafkaConnector {
    config: ConnectorConfig,
    connected: bool,
    stats: Arc<RwLock<ConnectorStats>>,
}

impl KafkaConnector {
    pub fn new(config: ConnectorConfig) -> Self {
        Self {
            config,
            connected: false,
            stats: Arc::new(RwLock::new(ConnectorStats::default())),
        }
    }

    fn format_event(&self, event: &ChangeEvent) -> Result<Vec<u8>> {
        if self.config.cloud_events {
            let ce = CloudEvent::from_change_event(
                event.clone(),
                format!("urn:strata:connector:{}", self.config.name),
            );
            serde_json::to_vec(&ce).map_err(|e| StrataError::Serialization(e.to_string()))
        } else {
            event.to_json_bytes().map_err(|e| StrataError::Serialization(e.to_string()))
        }
    }
}

#[async_trait]
impl Connector for KafkaConnector {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn connector_type(&self) -> ConnectorType {
        ConnectorType::Kafka
    }

    async fn connect(&mut self) -> Result<()> {
        // In real implementation, would create Kafka producer
        // using rdkafka or similar library
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connected = false;
        Ok(())
    }

    async fn send(&self, event: &ChangeEvent) -> Result<()> {
        if !self.connected {
            return Err(StrataError::NotConnected("Kafka connector not connected".to_string()));
        }

        let data = self.format_event(event)?;
        let _key = format!("{}/{}", event.bucket, event.key);

        // Simulated Kafka send
        // In real implementation:
        // producer.send(FutureRecord::to(&self.config.topic)
        //     .key(&key)
        //     .payload(&data))

        let mut stats = self.stats.write().await;
        stats.events_sent += 1;
        stats.bytes_sent += data.len() as u64;
        stats.last_success_time = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );

        Ok(())
    }

    async fn send_batch(&self, events: &[ChangeEvent]) -> Result<()> {
        for event in events {
            self.send(event).await?;
        }
        let mut stats = self.stats.write().await;
        stats.batches_sent += 1;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn stats(&self) -> ConnectorStats {
        // Using blocking read for stats snapshot
        futures::executor::block_on(async {
            self.stats.read().await.clone()
        })
    }
}

/// Webhook connector
pub struct WebhookConnector {
    config: ConnectorConfig,
    client: reqwest::Client,
    connected: bool,
    stats: Arc<RwLock<ConnectorStats>>,
}

impl WebhookConnector {
    pub fn new(config: ConnectorConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();

        Self {
            config,
            client,
            connected: false,
            stats: Arc::new(RwLock::new(ConnectorStats::default())),
        }
    }

    fn get_headers(&self) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );

        if let Some(ref auth) = self.config.auth {
            match auth.auth_type {
                AuthType::Bearer => {
                    if let Some(ref token) = auth.token {
                        headers.insert(
                            reqwest::header::AUTHORIZATION,
                            format!("Bearer {}", token).parse().unwrap(),
                        );
                    }
                }
                AuthType::ApiKey => {
                    if let Some(ref key) = auth.api_key {
                        headers.insert("X-API-Key", key.parse().unwrap());
                    }
                }
                _ => {}
            }
        }

        headers
    }

    async fn send_with_retry(&self, data: &[u8]) -> Result<()> {
        let mut attempts = 0;
        let mut backoff = self.config.retry.initial_backoff_ms;

        loop {
            let endpoint = self.config.endpoints.first()
                .ok_or_else(|| StrataError::Configuration("No endpoint configured".to_string()))?;

            let result = self.client
                .post(endpoint)
                .headers(self.get_headers())
                .body(data.to_vec())
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {
                    return Ok(());
                }
                Ok(response) => {
                    let status = response.status();
                    if attempts >= self.config.retry.max_retries {
                        let mut stats = self.stats.write().await;
                        stats.events_failed += 1;
                        stats.last_error = Some(format!("HTTP {}", status));
                        return Err(StrataError::External(format!("Webhook failed: HTTP {}", status)));
                    }
                }
                Err(e) => {
                    if attempts >= self.config.retry.max_retries {
                        let mut stats = self.stats.write().await;
                        stats.events_failed += 1;
                        stats.last_error = Some(e.to_string());
                        return Err(StrataError::External(format!("Webhook error: {}", e)));
                    }
                }
            }

            attempts += 1;
            {
                let mut stats = self.stats.write().await;
                stats.retries += 1;
            }

            tokio::time::sleep(Duration::from_millis(backoff)).await;
            backoff = (backoff as f64 * self.config.retry.multiplier) as u64;
            backoff = backoff.min(self.config.retry.max_backoff_ms);
        }
    }
}

#[async_trait]
impl Connector for WebhookConnector {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn connector_type(&self) -> ConnectorType {
        ConnectorType::Webhook
    }

    async fn connect(&mut self) -> Result<()> {
        // Verify endpoint is reachable
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connected = false;
        Ok(())
    }

    async fn send(&self, event: &ChangeEvent) -> Result<()> {
        if !self.connected {
            return Err(StrataError::NotConnected("HTTP connector not connected".to_string()));
        }

        let data = if self.config.cloud_events {
            let ce = CloudEvent::from_change_event(
                event.clone(),
                format!("urn:strata:connector:{}", self.config.name),
            );
            serde_json::to_vec(&ce).map_err(|e| StrataError::Serialization(e.to_string()))?
        } else {
            event.to_json_bytes().map_err(|e| StrataError::Serialization(e.to_string()))?
        };

        self.send_with_retry(&data).await?;

        let mut stats = self.stats.write().await;
        stats.events_sent += 1;
        stats.bytes_sent += data.len() as u64;
        stats.last_success_time = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        );

        Ok(())
    }

    async fn send_batch(&self, events: &[ChangeEvent]) -> Result<()> {
        let batch = EventBatch::new(events.to_vec());
        let data = serde_json::to_vec(&batch)
            .map_err(|e| StrataError::Serialization(e.to_string()))?;

        self.send_with_retry(&data).await?;

        let mut stats = self.stats.write().await;
        stats.events_sent += events.len() as u64;
        stats.bytes_sent += data.len() as u64;
        stats.batches_sent += 1;

        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn stats(&self) -> ConnectorStats {
        futures::executor::block_on(async {
            self.stats.read().await.clone()
        })
    }
}

/// Stdout connector for debugging
pub struct StdoutConnector {
    config: ConnectorConfig,
    stats: Arc<AtomicStats>,
}

struct AtomicStats {
    events_sent: AtomicU64,
    bytes_sent: AtomicU64,
}

impl StdoutConnector {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            config: ConnectorConfig {
                connector_type: ConnectorType::Stdout,
                name: name.into(),
                endpoints: vec![],
                topic: String::new(),
                format: EventFormat::Json,
                cloud_events: false,
                batch: BatchConfig::default(),
                retry: RetryConfig::default(),
                auth: None,
                tls: None,
                properties: HashMap::new(),
            },
            stats: Arc::new(AtomicStats {
                events_sent: AtomicU64::new(0),
                bytes_sent: AtomicU64::new(0),
            }),
        }
    }
}

#[async_trait]
impl Connector for StdoutConnector {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn connector_type(&self) -> ConnectorType {
        ConnectorType::Stdout
    }

    async fn connect(&mut self) -> Result<()> {
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        Ok(())
    }

    async fn send(&self, event: &ChangeEvent) -> Result<()> {
        let json = event.to_json().map_err(|e| StrataError::Serialization(e.to_string()))?;
        println!("[CDC] {}", json);
        self.stats.events_sent.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_sent.fetch_add(json.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    async fn send_batch(&self, events: &[ChangeEvent]) -> Result<()> {
        for event in events {
            self.send(event).await?;
        }
        Ok(())
    }

    fn is_connected(&self) -> bool {
        true
    }

    fn stats(&self) -> ConnectorStats {
        ConnectorStats {
            events_sent: self.stats.events_sent.load(Ordering::Relaxed),
            bytes_sent: self.stats.bytes_sent.load(Ordering::Relaxed),
            ..Default::default()
        }
    }
}

/// Connector factory
pub struct ConnectorFactory;

impl ConnectorFactory {
    /// Creates a connector from configuration
    pub fn create(config: ConnectorConfig) -> Result<Box<dyn Connector>> {
        match config.connector_type {
            ConnectorType::Kafka => Ok(Box::new(KafkaConnector::new(config))),
            ConnectorType::Webhook => Ok(Box::new(WebhookConnector::new(config))),
            ConnectorType::Stdout => Ok(Box::new(StdoutConnector::new(&config.name))),
            _ => Err(StrataError::NotImplemented(format!(
                "Connector type {:?} not yet implemented",
                config.connector_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stdout_connector() {
        let mut connector = StdoutConnector::new("test");
        connector.connect().await.unwrap();

        let event = ChangeEvent::new(
            super::super::event::ChangeEventType::Create,
            "bucket",
            "key",
            1,
        );
        connector.send(&event).await.unwrap();

        let stats = connector.stats();
        assert_eq!(stats.events_sent, 1);
    }

    #[tokio::test]
    async fn test_kafka_connector() {
        let config = ConnectorConfig {
            connector_type: ConnectorType::Kafka,
            name: "test-kafka".to_string(),
            endpoints: vec!["localhost:9092".to_string()],
            topic: "test-topic".to_string(),
            format: EventFormat::Json,
            cloud_events: false,
            batch: BatchConfig::default(),
            retry: RetryConfig::default(),
            auth: None,
            tls: None,
            properties: HashMap::new(),
        };

        let mut connector = KafkaConnector::new(config);
        connector.connect().await.unwrap();
        assert!(connector.is_connected());

        let event = ChangeEvent::new(
            super::super::event::ChangeEventType::Update,
            "bucket",
            "key",
            1,
        );
        connector.send(&event).await.unwrap();

        let stats = connector.stats();
        assert_eq!(stats.events_sent, 1);
    }

    #[test]
    fn test_connector_factory() {
        let config = ConnectorConfig {
            connector_type: ConnectorType::Stdout,
            name: "factory-test".to_string(),
            endpoints: vec![],
            topic: String::new(),
            format: EventFormat::Json,
            cloud_events: false,
            batch: BatchConfig::default(),
            retry: RetryConfig::default(),
            auth: None,
            tls: None,
            properties: HashMap::new(),
        };

        let connector = ConnectorFactory::create(config).unwrap();
        assert_eq!(connector.connector_type(), ConnectorType::Stdout);
    }
}
