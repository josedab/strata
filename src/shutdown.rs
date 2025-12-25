//! Graceful shutdown handling for Strata services.
//!
//! This module provides utilities for coordinating graceful shutdowns
//! across all service components when receiving OS signals.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tracing::{error, info, warn};

/// Maximum time to wait for graceful shutdown before force exit.
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Shutdown coordinator for managing graceful service termination.
#[derive(Clone)]
pub struct ShutdownCoordinator {
    /// Broadcast channel for shutdown signal.
    shutdown_tx: broadcast::Sender<()>,
    /// Watch channel for checking if shutdown is in progress.
    shutdown_watch: watch::Receiver<bool>,
    /// Internal sender for watch channel.
    shutdown_watch_tx: Arc<watch::Sender<bool>>,
    /// Flag indicating if shutdown has been initiated.
    is_shutting_down: Arc<AtomicBool>,
    /// Shutdown timeout.
    timeout: Duration,
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator.
    pub fn new() -> Self {
        Self::with_timeout(DEFAULT_SHUTDOWN_TIMEOUT)
    }

    /// Create a new shutdown coordinator with custom timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        let (shutdown_watch_tx, shutdown_watch) = watch::channel(false);

        Self {
            shutdown_tx,
            shutdown_watch,
            shutdown_watch_tx: Arc::new(shutdown_watch_tx),
            is_shutting_down: Arc::new(AtomicBool::new(false)),
            timeout,
        }
    }

    /// Subscribe to shutdown signals.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Get a watch receiver for shutdown status.
    pub fn watch(&self) -> watch::Receiver<bool> {
        self.shutdown_watch.clone()
    }

    /// Check if shutdown is in progress.
    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::SeqCst)
    }

    /// Initiate shutdown.
    pub fn shutdown(&self) {
        if self
            .is_shutting_down
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            info!("Initiating graceful shutdown");

            // Update watch channel
            let _ = self.shutdown_watch_tx.send(true);

            // Broadcast shutdown signal
            let _ = self.shutdown_tx.send(());
        }
    }

    /// Wait for shutdown signal (for use in select! macros).
    pub async fn wait_for_shutdown(&self) {
        let mut rx = self.shutdown_watch.clone();
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }

    /// Get shutdown timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// Signal handler for graceful shutdown.
pub struct SignalHandler {
    coordinator: ShutdownCoordinator,
}

impl SignalHandler {
    /// Create a new signal handler.
    pub fn new(coordinator: ShutdownCoordinator) -> Self {
        Self { coordinator }
    }

    /// Install signal handlers and run the handler loop.
    /// Returns when a shutdown signal is received.
    #[cfg(unix)]
    pub async fn run(self) {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
        let mut sigquit = signal(SignalKind::quit()).expect("Failed to install SIGQUIT handler");

        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
            _ = sigint.recv() => {
                info!("Received SIGINT");
            }
            _ = sigquit.recv() => {
                info!("Received SIGQUIT");
            }
        }

        self.coordinator.shutdown();
    }

    /// Install signal handlers (Windows version).
    #[cfg(windows)]
    pub async fn run(self) {
        use tokio::signal::ctrl_c;

        ctrl_c().await.expect("Failed to install Ctrl+C handler");
        info!("Received Ctrl+C");
        self.coordinator.shutdown();
    }
}

/// A handle for a running service that can be gracefully shut down.
pub struct ServiceHandle {
    name: String,
    shutdown_fn: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl ServiceHandle {
    /// Create a new service handle.
    pub fn new<S, F>(name: S, shutdown_fn: F) -> Self
    where
        S: Into<String>,
        F: Future<Output = ()> + Send + 'static,
    {
        Self {
            name: name.into(),
            shutdown_fn: Some(Box::pin(shutdown_fn)),
        }
    }

    /// Create a handle that just logs shutdown.
    pub fn simple<S: Into<String>>(name: S) -> Self {
        let name = name.into();
        let name_clone = name.clone();
        Self {
            name,
            shutdown_fn: Some(Box::pin(async move {
                info!(service = %name_clone, "Service shutdown complete");
            })),
        }
    }

    /// Get the service name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Execute the shutdown function.
    pub async fn shutdown(&mut self) {
        if let Some(shutdown_fn) = self.shutdown_fn.take() {
            info!(service = %self.name, "Shutting down service");
            shutdown_fn.await;
        }
    }
}

/// Manager for coordinating shutdown of multiple services.
pub struct ShutdownManager {
    coordinator: ShutdownCoordinator,
    services: Vec<ServiceHandle>,
}

impl ShutdownManager {
    /// Create a new shutdown manager.
    pub fn new(coordinator: ShutdownCoordinator) -> Self {
        Self {
            coordinator,
            services: Vec::new(),
        }
    }

    /// Register a service for managed shutdown.
    pub fn register(&mut self, handle: ServiceHandle) {
        info!(service = %handle.name(), "Registered service for managed shutdown");
        self.services.push(handle);
    }

    /// Wait for shutdown signal and then shut down all services.
    pub async fn run(mut self) {
        // Wait for shutdown signal
        self.coordinator.wait_for_shutdown().await;

        info!(
            "Shutdown initiated, stopping {} services",
            self.services.len()
        );

        // Shutdown services in reverse order (LIFO)
        let timeout = self.coordinator.timeout();

        let shutdown_future = async {
            while let Some(mut service) = self.services.pop() {
                service.shutdown().await;
            }
        };

        if tokio::time::timeout(timeout, shutdown_future).await.is_err() {
            error!("Shutdown timed out after {:?}", timeout);
        } else {
            info!("All services shut down successfully");
        }
    }
}

/// Guard that signals completion when dropped.
pub struct ShutdownGuard {
    name: String,
    completed: Arc<AtomicBool>,
}

impl ShutdownGuard {
    /// Create a new shutdown guard.
    pub fn new<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            completed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Mark the guarded task as completed.
    pub fn complete(&self) {
        self.completed.store(true, Ordering::SeqCst);
    }

    /// Check if the guarded task completed.
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        if self.is_completed() {
            info!(task = %self.name, "Task completed normally");
        } else {
            warn!(task = %self.name, "Task terminated without completion");
        }
    }
}

/// Extension trait for futures to make them shutdown-aware.
pub trait ShutdownAware: Sized {
    /// Run until completion or shutdown signal.
    fn with_shutdown(
        self,
        coordinator: &ShutdownCoordinator,
    ) -> impl Future<Output = Option<Self::Output>> + Send
    where
        Self: Future + Send,
        Self::Output: Send;
}

impl<F> ShutdownAware for F
where
    F: Future + Send,
    F::Output: Send,
{
    async fn with_shutdown(self, coordinator: &ShutdownCoordinator) -> Option<F::Output> {
        tokio::select! {
            result = self => Some(result),
            _ = coordinator.wait_for_shutdown() => {
                info!("Task cancelled due to shutdown");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shutdown_coordinator_new() {
        let coordinator = ShutdownCoordinator::new();
        assert!(!coordinator.is_shutting_down());
    }

    #[tokio::test]
    async fn test_shutdown_coordinator_shutdown() {
        let coordinator = ShutdownCoordinator::new();

        assert!(!coordinator.is_shutting_down());
        coordinator.shutdown();
        assert!(coordinator.is_shutting_down());
    }

    #[tokio::test]
    async fn test_shutdown_coordinator_subscribe() {
        let coordinator = ShutdownCoordinator::new();
        let mut rx = coordinator.subscribe();

        coordinator.shutdown();

        // Should receive the signal
        let result = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_shutdown_watch() {
        let coordinator = ShutdownCoordinator::new();
        let mut watch = coordinator.watch();

        assert!(!*watch.borrow());

        coordinator.shutdown();

        // Wait for update
        watch.changed().await.unwrap();
        assert!(*watch.borrow());
    }

    #[tokio::test]
    async fn test_shutdown_manager() {
        let coordinator = ShutdownCoordinator::new();
        let mut manager = ShutdownManager::new(coordinator.clone());

        manager.register(ServiceHandle::simple("test-service"));

        // Trigger shutdown
        coordinator.shutdown();

        // Run should complete quickly
        let result = tokio::time::timeout(Duration::from_millis(100), manager.run()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_shutdown_guard_complete() {
        let guard = ShutdownGuard::new("test");
        assert!(!guard.is_completed());

        guard.complete();
        assert!(guard.is_completed());
    }

    #[tokio::test]
    async fn test_shutdown_aware() {
        let coordinator = ShutdownCoordinator::new();

        let task = async { 42 };

        let result = task.with_shutdown(&coordinator).await;
        assert_eq!(result, Some(42));
    }

    #[tokio::test]
    async fn test_shutdown_aware_cancelled() {
        let coordinator = ShutdownCoordinator::new();

        // Shutdown before task completes
        coordinator.shutdown();

        let task = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            42
        };

        let result = task.with_shutdown(&coordinator).await;
        assert_eq!(result, None);
    }
}
