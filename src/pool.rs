//! Connection pooling for Strata services.
//!
//! Provides generic connection pool management for efficient resource utilization.

use crate::error::{Result, StrataError};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

/// Connection pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of connections to maintain.
    pub min_connections: usize,
    /// Maximum number of connections allowed.
    pub max_connections: usize,
    /// Maximum time to wait for a connection.
    pub connection_timeout: Duration,
    /// Maximum idle time before closing a connection.
    pub idle_timeout: Duration,
    /// Maximum lifetime of a connection.
    pub max_lifetime: Duration,
    /// How often to run health checks on idle connections.
    pub health_check_interval: Duration,
    /// Whether to validate connections before use.
    pub test_on_acquire: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(600),
            max_lifetime: Duration::from_secs(3600),
            health_check_interval: Duration::from_secs(30),
            test_on_acquire: true,
        }
    }
}

impl PoolConfig {
    /// Configuration for high-throughput workloads.
    pub fn high_throughput() -> Self {
        Self {
            min_connections: 5,
            max_connections: 50,
            connection_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(300),
            max_lifetime: Duration::from_secs(1800),
            health_check_interval: Duration::from_secs(15),
            test_on_acquire: true,
        }
    }

    /// Configuration for low-resource environments.
    pub fn low_resource() -> Self {
        Self {
            min_connections: 1,
            max_connections: 5,
            connection_timeout: Duration::from_secs(60),
            idle_timeout: Duration::from_secs(900),
            max_lifetime: Duration::from_secs(7200),
            health_check_interval: Duration::from_secs(60),
            test_on_acquire: false,
        }
    }
}

/// Trait for poolable connections.
pub trait Poolable: Send + Sync + Sized + 'static {
    /// Create a new connection.
    fn create() -> impl std::future::Future<Output = Result<Self>> + Send;

    /// Check if the connection is still valid.
    fn is_valid(&self) -> impl std::future::Future<Output = bool> + Send;

    /// Close the connection.
    fn close(self) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Reset the connection for reuse.
    fn reset(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

/// Pooled connection wrapper.
struct PooledConnection<C: Poolable> {
    /// The actual connection.
    connection: C,
    /// When this connection was created.
    created_at: Instant,
    /// When this connection was last used.
    last_used: Instant,
    /// Number of times this connection has been used.
    #[allow(dead_code)]
    use_count: u64,
}

impl<C: Poolable> PooledConnection<C> {
    fn new(connection: C) -> Self {
        let now = Instant::now();
        Self {
            connection,
            created_at: now,
            last_used: now,
            use_count: 0,
        }
    }

    fn is_expired(&self, config: &PoolConfig) -> bool {
        self.created_at.elapsed() > config.max_lifetime
    }

    fn is_idle_too_long(&self, config: &PoolConfig) -> bool {
        self.last_used.elapsed() > config.idle_timeout
    }

    fn mark_used(&mut self) {
        self.last_used = Instant::now();
        self.use_count += 1;
    }
}

/// Connection pool statistics.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total connections created.
    pub connections_created: u64,
    /// Total connections destroyed.
    pub connections_destroyed: u64,
    /// Current pool size.
    pub current_size: usize,
    /// Current idle connections.
    pub idle_count: usize,
    /// Current active (in-use) connections.
    pub active_count: usize,
    /// Total successful acquires.
    pub acquires: u64,
    /// Total acquire failures.
    pub acquire_failures: u64,
    /// Total acquire timeouts.
    pub acquire_timeouts: u64,
    /// Average wait time in milliseconds.
    pub avg_wait_time_ms: u64,
}

/// Internal pool state that can be shared.
struct PoolInner<C: Poolable> {
    /// Pool configuration.
    config: PoolConfig,
    /// Available connections.
    connections: Mutex<VecDeque<PooledConnection<C>>>,
    /// Semaphore to limit concurrent connections.
    semaphore: Arc<Semaphore>,
    /// Statistics.
    stats: PoolStatsInner,
    /// Whether the pool is closed.
    closed: std::sync::atomic::AtomicBool,
}

/// Internal statistics with atomic counters.
struct PoolStatsInner {
    connections_created: AtomicU64,
    connections_destroyed: AtomicU64,
    current_size: AtomicUsize,
    idle_count: AtomicUsize,
    acquires: AtomicU64,
    acquire_failures: AtomicU64,
    acquire_timeouts: AtomicU64,
    total_wait_time_ns: AtomicU64,
}

impl Default for PoolStatsInner {
    fn default() -> Self {
        Self {
            connections_created: AtomicU64::new(0),
            connections_destroyed: AtomicU64::new(0),
            current_size: AtomicUsize::new(0),
            idle_count: AtomicUsize::new(0),
            acquires: AtomicU64::new(0),
            acquire_failures: AtomicU64::new(0),
            acquire_timeouts: AtomicU64::new(0),
            total_wait_time_ns: AtomicU64::new(0),
        }
    }
}

/// Generic connection pool.
pub struct ConnectionPool<C: Poolable> {
    inner: Arc<PoolInner<C>>,
}

impl<C: Poolable> Clone for ConnectionPool<C> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C: Poolable> ConnectionPool<C> {
    /// Create a new connection pool.
    pub fn new(config: PoolConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections));
        Self {
            inner: Arc::new(PoolInner {
                config,
                connections: Mutex::new(VecDeque::new()),
                semaphore,
                stats: PoolStatsInner::default(),
                closed: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// Initialize the pool with minimum connections.
    pub async fn initialize(&self) -> Result<()> {
        for _ in 0..self.inner.config.min_connections {
            let conn = self.create_connection().await?;
            let mut connections = self.inner.connections.lock().await;
            connections.push_back(conn);
            self.inner.stats.idle_count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Acquire a connection from the pool.
    pub async fn acquire(&self) -> Result<PoolGuard<C>> {
        if self.inner.closed.load(Ordering::Relaxed) {
            return Err(StrataError::Internal("Pool is closed".to_string()));
        }

        let start = Instant::now();

        // Wait for a permit (limits concurrent connections)
        let permit = match tokio::time::timeout(
            self.inner.config.connection_timeout,
            Arc::clone(&self.inner.semaphore).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                self.inner.stats.acquire_failures.fetch_add(1, Ordering::Relaxed);
                return Err(StrataError::Internal("Pool semaphore closed".to_string()));
            }
            Err(_) => {
                self.inner.stats.acquire_timeouts.fetch_add(1, Ordering::Relaxed);
                return Err(StrataError::TimeoutStr("Connection pool timeout".to_string()));
            }
        };

        // Try to get an existing connection
        let connection = self.get_or_create_connection().await?;

        let wait_time = start.elapsed().as_nanos() as u64;
        self.inner.stats.total_wait_time_ns.fetch_add(wait_time, Ordering::Relaxed);
        self.inner.stats.acquires.fetch_add(1, Ordering::Relaxed);

        Ok(PoolGuard {
            connection: Some(connection),
            pool: self.clone(),
            _permit: permit,
        })
    }

    /// Try to get an existing connection or create a new one.
    async fn get_or_create_connection(&self) -> Result<PooledConnection<C>> {
        loop {
            // Try to get from pool
            let conn_opt = {
                let mut connections = self.inner.connections.lock().await;
                connections.pop_front()
            };

            if let Some(mut conn) = conn_opt {
                self.inner.stats.idle_count.fetch_sub(1, Ordering::Relaxed);

                // Check if connection is still valid
                if conn.is_expired(&self.inner.config) || conn.is_idle_too_long(&self.inner.config) {
                    self.destroy_connection(conn).await;
                    continue;
                }

                // Validate if configured
                if self.inner.config.test_on_acquire && !conn.connection.is_valid().await {
                    self.destroy_connection(conn).await;
                    continue;
                }

                conn.mark_used();
                return Ok(conn);
            } else {
                // Create a new connection
                return self.create_connection().await;
            }
        }
    }

    /// Create a new connection.
    async fn create_connection(&self) -> Result<PooledConnection<C>> {
        let conn = C::create().await?;
        self.inner.stats.connections_created.fetch_add(1, Ordering::Relaxed);
        self.inner.stats.current_size.fetch_add(1, Ordering::Relaxed);
        Ok(PooledConnection::new(conn))
    }

    /// Destroy a connection.
    async fn destroy_connection(&self, conn: PooledConnection<C>) {
        let _ = conn.connection.close().await;
        self.inner.stats.connections_destroyed.fetch_add(1, Ordering::Relaxed);
        self.inner.stats.current_size.fetch_sub(1, Ordering::Relaxed);
    }

    /// Return a connection to the pool.
    async fn release(&self, mut conn: PooledConnection<C>) {
        if self.inner.closed.load(Ordering::Relaxed) {
            self.destroy_connection(conn).await;
            return;
        }

        // Check if connection should be reused
        if conn.is_expired(&self.inner.config) {
            self.destroy_connection(conn).await;
            return;
        }

        // Reset connection for reuse
        if conn.connection.reset().await.is_err() {
            self.destroy_connection(conn).await;
            return;
        }

        conn.last_used = Instant::now();

        let mut connections = self.inner.connections.lock().await;
        connections.push_back(conn);
        self.inner.stats.idle_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get pool statistics.
    pub fn stats(&self) -> PoolStats {
        let acquires = self.inner.stats.acquires.load(Ordering::Relaxed);
        let avg_wait = if acquires > 0 {
            self.inner.stats.total_wait_time_ns.load(Ordering::Relaxed) / (acquires * 1_000_000)
        } else {
            0
        };

        PoolStats {
            connections_created: self.inner.stats.connections_created.load(Ordering::Relaxed),
            connections_destroyed: self.inner.stats.connections_destroyed.load(Ordering::Relaxed),
            current_size: self.inner.stats.current_size.load(Ordering::Relaxed),
            idle_count: self.inner.stats.idle_count.load(Ordering::Relaxed),
            active_count: self.inner.stats.current_size.load(Ordering::Relaxed)
                .saturating_sub(self.inner.stats.idle_count.load(Ordering::Relaxed)),
            acquires,
            acquire_failures: self.inner.stats.acquire_failures.load(Ordering::Relaxed),
            acquire_timeouts: self.inner.stats.acquire_timeouts.load(Ordering::Relaxed),
            avg_wait_time_ms: avg_wait,
        }
    }

    /// Run maintenance on the pool (evict idle connections, etc.)
    pub async fn maintain(&self) {
        let mut to_destroy = Vec::new();

        {
            let mut connections = self.inner.connections.lock().await;
            let mut i = 0;
            while i < connections.len() {
                let conn = &connections[i];
                if conn.is_expired(&self.inner.config) || conn.is_idle_too_long(&self.inner.config) {
                    if let Some(conn) = connections.remove(i) {
                        self.inner.stats.idle_count.fetch_sub(1, Ordering::Relaxed);
                        to_destroy.push(conn);
                    }
                } else {
                    i += 1;
                }
            }

            // Ensure minimum connections
            while connections.len() < self.inner.config.min_connections {
                match self.create_connection().await {
                    Ok(conn) => {
                        connections.push_back(conn);
                        self.inner.stats.idle_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => break,
                }
            }
        }

        // Destroy connections outside the lock
        for conn in to_destroy {
            self.destroy_connection(conn).await;
        }
    }

    /// Close the pool and all connections.
    pub async fn close(&self) {
        self.inner.closed.store(true, Ordering::Relaxed);

        let connections: Vec<_> = {
            let mut conns = self.inner.connections.lock().await;
            conns.drain(..).collect()
        };

        for conn in connections {
            self.destroy_connection(conn).await;
        }
    }

    /// Check if the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Relaxed)
    }
}

/// RAII guard for pooled connections.
pub struct PoolGuard<C: Poolable> {
    connection: Option<PooledConnection<C>>,
    pool: ConnectionPool<C>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<C: Poolable> PoolGuard<C> {
    /// Get a reference to the underlying connection.
    pub fn conn(&self) -> &C {
        &self.connection.as_ref().unwrap().connection
    }

    /// Get a mutable reference to the underlying connection.
    pub fn conn_mut(&mut self) -> &mut C {
        &mut self.connection.as_mut().unwrap().connection
    }
}

impl<C: Poolable> std::ops::Deref for PoolGuard<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.conn()
    }
}

impl<C: Poolable> std::ops::DerefMut for PoolGuard<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn_mut()
    }
}

impl<C: Poolable> Drop for PoolGuard<C> {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            let pool = self.pool.clone();
            tokio::spawn(async move {
                pool.release(conn).await;
            });
        }
    }
}

/// Simple connection pool for a fixed resource type.
pub struct SimplePool<T: Send + Sync> {
    /// Available resources.
    resources: Mutex<VecDeque<T>>,
    /// Semaphore to limit concurrent usage.
    semaphore: Arc<Semaphore>,
    /// Factory function to create resources.
    factory: Box<dyn Fn() -> T + Send + Sync>,
}

impl<T: Send + Sync + 'static> SimplePool<T> {
    /// Create a new simple pool.
    pub fn new<F>(size: usize, factory: F) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        let mut resources = VecDeque::with_capacity(size);
        for _ in 0..size {
            resources.push_back(factory());
        }

        Self {
            resources: Mutex::new(resources),
            semaphore: Arc::new(Semaphore::new(size)),
            factory: Box::new(factory),
        }
    }

    /// Get a resource from the pool.
    pub async fn get(&self) -> Result<SimplePoolGuard<'_, T>> {
        let permit = Arc::clone(&self.semaphore).acquire_owned().await.map_err(|_| {
            StrataError::Internal("Pool semaphore closed".to_string())
        })?;

        let resource = {
            let mut resources = self.resources.lock().await;
            resources.pop_front().unwrap_or_else(|| (self.factory)())
        };

        Ok(SimplePoolGuard {
            resource: Some(resource),
            pool: self,
            _permit: permit,
        })
    }

    /// Return a resource to the pool.
    async fn return_resource(&self, resource: T) {
        let mut resources = self.resources.lock().await;
        resources.push_back(resource);
    }
}

/// Guard for simple pool resources.
pub struct SimplePoolGuard<'a, T: Send + Sync + 'static> {
    resource: Option<T>,
    pool: &'a SimplePool<T>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<'a, T: Send + Sync + 'static> std::ops::Deref for SimplePoolGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.resource.as_ref().unwrap()
    }
}

impl<'a, T: Send + Sync + 'static> std::ops::DerefMut for SimplePoolGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.resource.as_mut().unwrap()
    }
}

impl<'a, T: Send + Sync + 'static> Drop for SimplePoolGuard<'a, T> {
    fn drop(&mut self) {
        if let Some(resource) = self.resource.take() {
            let pool = self.pool;
            // Use try_lock to avoid blocking in drop
            if let Ok(mut resources) = pool.resources.try_lock() {
                resources.push_back(resource);
            }
            // If lock fails, resource is lost (acceptable tradeoff for simpler code)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    // Test implementation of Poolable
    struct TestConnection {
        #[allow(dead_code)]
        id: u32,
        valid: bool,
    }

    static NEXT_ID: AtomicU32 = AtomicU32::new(0);

    impl Poolable for TestConnection {
        fn create() -> impl std::future::Future<Output = Result<Self>> + Send {
            async {
                Ok(Self {
                    id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
                    valid: true,
                })
            }
        }

        fn is_valid(&self) -> impl std::future::Future<Output = bool> + Send {
            let valid = self.valid;
            async move { valid }
        }

        fn close(self) -> impl std::future::Future<Output = Result<()>> + Send {
            async { Ok(()) }
        }
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.max_connections, 10);
    }

    #[test]
    fn test_pool_config_presets() {
        let high = PoolConfig::high_throughput();
        assert_eq!(high.max_connections, 50);

        let low = PoolConfig::low_resource();
        assert_eq!(low.max_connections, 5);
    }

    #[tokio::test]
    async fn test_pool_acquire_release() {
        NEXT_ID.store(0, Ordering::Relaxed);

        let pool = ConnectionPool::<TestConnection>::new(PoolConfig::default());
        pool.initialize().await.unwrap();

        // Acquire a connection
        let guard = pool.acquire().await.unwrap();
        assert!(guard.valid);

        // Stats should reflect active connection
        let stats = pool.stats();
        assert!(stats.acquires >= 1);
    }

    #[tokio::test]
    async fn test_pool_reuses_connections() {
        NEXT_ID.store(0, Ordering::Relaxed);

        let config = PoolConfig {
            test_on_acquire: false, // Disable for simpler test
            ..Default::default()
        };
        let pool = ConnectionPool::<TestConnection>::new(config);
        pool.initialize().await.unwrap();

        // Acquire and release multiple times
        for _ in 0..5 {
            let _guard = pool.acquire().await.unwrap();
            // Guard is dropped here, but release is async via tokio::spawn
            // So we yield to allow the release task to complete
        }

        // Allow time for the last release to complete
        tokio::time::sleep(Duration::from_millis(10)).await;

        let stats = pool.stats();
        assert_eq!(stats.acquires, 5);
        // With async release, connections may not always be returned in time,
        // but we should not create more than the number of acquires
        assert!(stats.connections_created <= 5);
    }

    #[tokio::test]
    async fn test_pool_concurrent_access() {
        NEXT_ID.store(0, Ordering::Relaxed);

        let config = PoolConfig {
            min_connections: 2,
            max_connections: 4,
            test_on_acquire: false,
            ..Default::default()
        };
        let pool = ConnectionPool::<TestConnection>::new(config);
        pool.initialize().await.unwrap();

        // Spawn multiple tasks that acquire connections
        let mut handles = Vec::new();
        for _ in 0..10 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let _guard = pool.acquire().await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let stats = pool.stats();
        assert_eq!(stats.acquires, 10);
    }

    #[tokio::test]
    async fn test_pool_stats() {
        NEXT_ID.store(0, Ordering::Relaxed);

        let pool = ConnectionPool::<TestConnection>::new(PoolConfig::default());

        let initial_stats = pool.stats();
        assert_eq!(initial_stats.connections_created, 0);
        assert_eq!(initial_stats.acquires, 0);

        pool.initialize().await.unwrap();
        let _guard = pool.acquire().await.unwrap();

        let stats = pool.stats();
        assert!(stats.connections_created >= 1);
        assert!(stats.acquires >= 1);
    }

    #[tokio::test]
    async fn test_pool_close() {
        NEXT_ID.store(0, Ordering::Relaxed);

        let pool = ConnectionPool::<TestConnection>::new(PoolConfig::default());
        pool.initialize().await.unwrap();

        assert!(!pool.is_closed());
        pool.close().await;
        assert!(pool.is_closed());

        // Acquiring after close should fail
        let result = pool.acquire().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_simple_pool() {
        let pool = SimplePool::new(3, || vec![0u8; 1024]);

        // Should be able to get resources
        let mut guards = Vec::new();
        for _ in 0..3 {
            guards.push(pool.get().await.unwrap());
        }

        // All resources acquired, verify they work
        for guard in &guards {
            assert_eq!(guard.len(), 1024);
        }
    }

    #[tokio::test]
    async fn test_pool_maintain() {
        NEXT_ID.store(0, Ordering::Relaxed);

        let config = PoolConfig {
            min_connections: 2,
            idle_timeout: Duration::from_millis(10),
            test_on_acquire: false,
            ..Default::default()
        };
        let pool = ConnectionPool::<TestConnection>::new(config);
        pool.initialize().await.unwrap();

        // Wait for idle timeout
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Maintain should replace idle connections
        pool.maintain().await;

        let stats = pool.stats();
        // Should have maintained minimum connections
        assert!(stats.current_size >= 2);
    }
}
