//! Common test utilities for integration tests.

pub mod assertions;
pub mod cluster_sim;
pub mod fixtures;

use std::net::TcpListener;
use std::path::PathBuf;
use tempfile::TempDir;

// Re-export common types
pub use assertions::*;
pub use cluster_sim::*;
pub use fixtures::*;

/// Test error type
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// Find an available port for testing.
pub fn find_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to port");
    listener.local_addr().unwrap().port()
}

/// Find multiple available ports
pub fn find_available_ports(count: usize) -> Vec<u16> {
    (0..count).map(|_| find_available_port()).collect()
}

/// Test environment that manages temporary directories and cleanup.
pub struct TestEnv {
    pub temp_dir: TempDir,
    pub data_dir: PathBuf,
    pub metadata_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub log_dir: PathBuf,
}

impl TestEnv {
    pub fn new() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let data_dir = temp_dir.path().join("data");
        let metadata_dir = temp_dir.path().join("metadata");
        let cache_dir = temp_dir.path().join("cache");
        let log_dir = temp_dir.path().join("logs");

        std::fs::create_dir_all(&data_dir).expect("Failed to create data dir");
        std::fs::create_dir_all(&metadata_dir).expect("Failed to create metadata dir");
        std::fs::create_dir_all(&cache_dir).expect("Failed to create cache dir");
        std::fs::create_dir_all(&log_dir).expect("Failed to create log dir");

        Self {
            temp_dir,
            data_dir,
            metadata_dir,
            cache_dir,
            log_dir,
        }
    }

    /// Creates a sub-directory in the temp dir
    pub fn subdir(&self, name: &str) -> PathBuf {
        let path = self.temp_dir.path().join(name);
        std::fs::create_dir_all(&path).expect("Failed to create subdir");
        path
    }

    /// Creates a test file with random content
    pub fn create_test_file(&self, name: &str, size: usize) -> (PathBuf, Vec<u8>) {
        use rand::Rng;
        let path = self.temp_dir.path().join(name);
        let mut content = vec![0u8; size];
        rand::thread_rng().fill(&mut content[..]);
        std::fs::write(&path, &content).expect("Failed to write test file");
        (path, content)
    }
}

impl Default for TestEnv {
    fn default() -> Self {
        Self::new()
    }
}

/// Wait for a server to be ready by polling a health endpoint.
pub async fn wait_for_server(addr: &str, timeout_secs: u64) -> bool {
    let client = reqwest::Client::new();
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout {
        if let Ok(response) = client.get(&format!("http://{}/health", addr)).send().await {
            if response.status().is_success() {
                return true;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    false
}

/// Wait for a TCP port to be available
pub async fn wait_for_port(port: u16, timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout {
        if tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .is_ok()
        {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    false
}

/// A test cluster with configurable number of nodes.
pub struct TestCluster {
    pub env: TestEnv,
    pub metadata_addr: String,
    pub data_addrs: Vec<String>,
    pub s3_addr: Option<String>,
    shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
}

impl TestCluster {
    /// Create a new test cluster configuration.
    pub fn new(num_data_servers: usize) -> Self {
        let env = TestEnv::new();
        let metadata_port = find_available_port();
        let metadata_addr = format!("127.0.0.1:{}", metadata_port);

        let data_addrs: Vec<String> = (0..num_data_servers)
            .map(|_| format!("127.0.0.1:{}", find_available_port()))
            .collect();

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        Self {
            env,
            metadata_addr,
            data_addrs,
            s3_addr: None,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Enables S3 gateway
    pub fn with_s3(mut self) -> Self {
        let port = find_available_port();
        self.s3_addr = Some(format!("127.0.0.1:{}", port));
        self
    }

    /// Get a shutdown receiver for graceful termination.
    pub fn shutdown_receiver(&self) -> tokio::sync::broadcast::Receiver<()> {
        self.shutdown_tx.as_ref().unwrap().subscribe()
    }

    /// Signal all servers to shutdown.
    pub fn shutdown(&self) {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(());
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Test timing utilities
pub mod timing {
    use std::time::{Duration, Instant};

    /// Measures execution time of a closure
    pub async fn measure<F, Fut, T>(f: F) -> (T, Duration)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let start = Instant::now();
        let result = f().await;
        (result, start.elapsed())
    }

    /// Runs a function with a timeout
    pub async fn with_timeout<F, Fut, T>(
        f: F,
        timeout: Duration,
    ) -> std::result::Result<T, tokio::time::error::Elapsed>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        tokio::time::timeout(timeout, f()).await
    }
}

/// Test retry utilities
pub mod retry {
    use std::time::Duration;

    /// Retries a fallible operation with exponential backoff
    pub async fn with_backoff<F, Fut, T, E>(
        mut f: F,
        max_attempts: usize,
        initial_delay: Duration,
    ) -> std::result::Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, E>>,
    {
        let mut delay = initial_delay;
        let mut last_err = None;

        for attempt in 0..max_attempts {
            match f().await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    last_err = Some(e);
                    if attempt < max_attempts - 1 {
                        tokio::time::sleep(delay).await;
                        delay *= 2;
                    }
                }
            }
        }

        Err(last_err.unwrap())
    }
}

/// Test concurrency utilities
pub mod concurrency {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Counter for tracking concurrent operations
    #[derive(Clone)]
    pub struct ConcurrencyTracker {
        current: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        total: Arc<AtomicUsize>,
    }

    impl ConcurrencyTracker {
        pub fn new() -> Self {
            Self {
                current: Arc::new(AtomicUsize::new(0)),
                peak: Arc::new(AtomicUsize::new(0)),
                total: Arc::new(AtomicUsize::new(0)),
            }
        }

        pub fn enter(&self) -> ConcurrencyGuard {
            let current = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(current, Ordering::SeqCst);
            self.total.fetch_add(1, Ordering::SeqCst);
            ConcurrencyGuard {
                tracker: self.clone(),
            }
        }

        pub fn current(&self) -> usize {
            self.current.load(Ordering::SeqCst)
        }

        pub fn peak(&self) -> usize {
            self.peak.load(Ordering::SeqCst)
        }

        pub fn total(&self) -> usize {
            self.total.load(Ordering::SeqCst)
        }
    }

    impl Default for ConcurrencyTracker {
        fn default() -> Self {
            Self::new()
        }
    }

    pub struct ConcurrencyGuard {
        tracker: ConcurrencyTracker,
    }

    impl Drop for ConcurrencyGuard {
        fn drop(&mut self) {
            self.tracker.current.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_creation() {
        let env = TestEnv::new();
        assert!(env.data_dir.exists());
        assert!(env.metadata_dir.exists());
    }

    #[test]
    fn test_find_ports() {
        let ports = find_available_ports(5);
        assert_eq!(ports.len(), 5);
        // All ports should be unique
        let unique: std::collections::HashSet<_> = ports.iter().collect();
        assert_eq!(unique.len(), 5);
    }

    #[tokio::test]
    async fn test_concurrency_tracker() {
        use concurrency::ConcurrencyTracker;

        let tracker = ConcurrencyTracker::new();
        assert_eq!(tracker.current(), 0);

        {
            let _g1 = tracker.enter();
            assert_eq!(tracker.current(), 1);

            let _g2 = tracker.enter();
            assert_eq!(tracker.current(), 2);
            assert_eq!(tracker.peak(), 2);
        }

        assert_eq!(tracker.current(), 0);
        assert_eq!(tracker.peak(), 2);
        assert_eq!(tracker.total(), 2);
    }
}
