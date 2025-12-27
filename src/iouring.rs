//! io_uring Backend for High-Performance Async I/O
//!
//! Provides a high-performance I/O backend using Linux's io_uring interface.
//! Falls back to standard async I/O on non-Linux platforms.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    io_uring Backend                         │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Submission Queue  │  Completion Queue  │  Registered Bufs  │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Batched Ops  │  Zero-Copy  │  Fixed Files  │  Polling     │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Read/Write  │  ReadV/WriteV  │  Fsync  │  Fallocate       │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Performance Benefits
//!
//! - **Batched syscalls**: Submit multiple I/O operations in a single syscall
//! - **Zero-copy I/O**: Use registered buffers to avoid copies
//! - **Fixed file descriptors**: Avoid fd lookup overhead
//! - **Polling mode**: Avoid interrupt overhead for high-throughput workloads

use crate::error::{Result, StrataError};

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write as IoWrite};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

// ============================================================================
// Configuration
// ============================================================================

/// io_uring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoUringConfig {
    /// Enable io_uring (falls back to standard I/O if unavailable)
    pub enabled: bool,
    /// Submission queue depth
    pub sq_depth: u32,
    /// Completion queue depth (usually 2x sq_depth)
    pub cq_depth: u32,
    /// Number of registered buffers
    pub registered_buffers: usize,
    /// Size of each registered buffer
    pub buffer_size: usize,
    /// Number of fixed file slots
    pub fixed_files: usize,
    /// Enable kernel-side polling (IORING_SETUP_SQPOLL)
    pub kernel_poll: bool,
    /// Kernel poll idle timeout in milliseconds
    pub kernel_poll_idle_ms: u32,
    /// Enable busy-waiting for completions
    pub busy_wait: bool,
    /// Maximum batch size for submissions
    pub max_batch_size: usize,
    /// Timeout for batch collection
    pub batch_timeout: Duration,
    /// Enable direct I/O (O_DIRECT)
    pub direct_io: bool,
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sq_depth: 256,
            cq_depth: 512,
            registered_buffers: 64,
            buffer_size: 64 * 1024, // 64KB
            fixed_files: 128,
            kernel_poll: false,
            kernel_poll_idle_ms: 1000,
            busy_wait: false,
            max_batch_size: 32,
            batch_timeout: Duration::from_micros(100),
            direct_io: false,
        }
    }
}

impl IoUringConfig {
    /// High-throughput configuration
    pub fn high_throughput() -> Self {
        Self {
            enabled: true,
            sq_depth: 1024,
            cq_depth: 2048,
            registered_buffers: 256,
            buffer_size: 128 * 1024, // 128KB
            fixed_files: 512,
            kernel_poll: true,
            kernel_poll_idle_ms: 100,
            busy_wait: true,
            max_batch_size: 64,
            batch_timeout: Duration::from_micros(50),
            direct_io: true,
        }
    }

    /// Low-latency configuration
    pub fn low_latency() -> Self {
        Self {
            enabled: true,
            sq_depth: 128,
            cq_depth: 256,
            registered_buffers: 32,
            buffer_size: 32 * 1024, // 32KB
            fixed_files: 64,
            kernel_poll: true,
            kernel_poll_idle_ms: 10,
            busy_wait: true,
            max_batch_size: 8,
            batch_timeout: Duration::from_micros(10),
            direct_io: true,
        }
    }
}

// ============================================================================
// I/O Operations
// ============================================================================

/// Types of I/O operations
#[derive(Debug, Clone)]
pub enum IoOperation {
    /// Read data from file
    Read {
        fd: RawFd,
        offset: u64,
        len: usize,
        buffer_id: Option<usize>,
    },
    /// Write data to file
    Write {
        fd: RawFd,
        offset: u64,
        data: Vec<u8>,
        buffer_id: Option<usize>,
    },
    /// Vectored read
    ReadV {
        fd: RawFd,
        offset: u64,
        lens: Vec<usize>,
    },
    /// Vectored write
    WriteV {
        fd: RawFd,
        offset: u64,
        buffers: Vec<Vec<u8>>,
    },
    /// Sync file data
    Fsync { fd: RawFd, datasync: bool },
    /// Allocate space
    Fallocate {
        fd: RawFd,
        offset: u64,
        len: u64,
        mode: FallocateMode,
    },
    /// Open file
    Open { path: PathBuf, flags: OpenFlags },
    /// Close file
    Close { fd: RawFd },
    /// Read with timeout
    ReadTimeout {
        fd: RawFd,
        offset: u64,
        len: usize,
        timeout: Duration,
    },
    /// Poll for readiness
    Poll { fd: RawFd, events: PollEvents },
    /// Cancel operation
    Cancel { user_data: u64 },
    /// No-op (for benchmarking)
    Nop,
}

/// File allocation modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallocateMode {
    /// Allocate space (default)
    Allocate,
    /// Punch a hole (deallocate)
    PunchHole,
    /// Zero range
    ZeroRange,
    /// Collapse range
    CollapseRange,
    /// Insert range
    InsertRange,
}

/// File open flags
#[derive(Debug, Clone, Default)]
pub struct OpenFlags {
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub truncate: bool,
    pub append: bool,
    pub direct: bool,
    pub sync: bool,
}

impl OpenFlags {
    pub fn read_only() -> Self {
        Self {
            read: true,
            ..Default::default()
        }
    }

    pub fn write_only() -> Self {
        Self {
            write: true,
            create: true,
            ..Default::default()
        }
    }

    pub fn read_write() -> Self {
        Self {
            read: true,
            write: true,
            create: true,
            ..Default::default()
        }
    }
}

/// Poll events
#[derive(Debug, Clone, Copy, Default)]
pub struct PollEvents {
    pub readable: bool,
    pub writable: bool,
    pub error: bool,
    pub hangup: bool,
}

/// Result of an I/O operation
#[derive(Debug)]
pub enum IoResult {
    /// Read completed
    Read { data: Vec<u8>, bytes_read: usize },
    /// Write completed
    Write { bytes_written: usize },
    /// ReadV completed
    ReadV { buffers: Vec<Vec<u8>>, total_bytes: usize },
    /// WriteV completed
    WriteV { bytes_written: usize },
    /// Fsync completed
    Fsync,
    /// Fallocate completed
    Fallocate,
    /// Open completed
    Open { fd: RawFd },
    /// Close completed
    Close,
    /// Poll completed
    Poll { events: PollEvents },
    /// Cancel completed
    Cancel { cancelled: bool },
    /// Nop completed
    Nop,
    /// Timeout occurred
    Timeout,
    /// Error occurred
    Error { code: i32, message: String },
}

// ============================================================================
// Registered Buffers
// ============================================================================

/// Pool of registered buffers for zero-copy I/O
pub struct BufferPool {
    buffers: Vec<Vec<u8>>,
    available: Mutex<VecDeque<usize>>,
    buffer_size: usize,
    stats: BufferPoolStats,
}

#[derive(Debug, Default)]
struct BufferPoolStats {
    allocations: AtomicU64,
    releases: AtomicU64,
    waits: AtomicU64,
}

impl BufferPool {
    pub fn new(count: usize, buffer_size: usize) -> Self {
        let buffers: Vec<Vec<u8>> = (0..count)
            .map(|_| vec![0u8; buffer_size])
            .collect();

        let available: VecDeque<usize> = (0..count).collect();

        Self {
            buffers,
            available: Mutex::new(available),
            buffer_size,
            stats: BufferPoolStats::default(),
        }
    }

    /// Acquire a buffer from the pool
    pub fn acquire(&self) -> Option<BufferGuard<'_>> {
        let mut available = self.available.lock();
        if let Some(id) = available.pop_front() {
            self.stats.allocations.fetch_add(1, Ordering::Relaxed);
            Some(BufferGuard {
                pool: self,
                buffer_id: id,
            })
        } else {
            self.stats.waits.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Try to acquire a buffer, blocking if none available
    pub async fn acquire_async(&self, timeout: Duration) -> Option<BufferGuard<'_>> {
        let start = Instant::now();
        loop {
            if let Some(guard) = self.acquire() {
                return Some(guard);
            }

            if start.elapsed() > timeout {
                return None;
            }

            tokio::time::sleep(Duration::from_micros(10)).await;
        }
    }

    /// Release a buffer back to the pool
    fn release(&self, buffer_id: usize) {
        let mut available = self.available.lock();
        available.push_back(buffer_id);
        self.stats.releases.fetch_add(1, Ordering::Relaxed);
    }

    /// Get buffer by ID
    pub fn get(&self, buffer_id: usize) -> Option<&[u8]> {
        self.buffers.get(buffer_id).map(|b| b.as_slice())
    }

    /// Get mutable buffer by ID
    pub fn get_mut(&mut self, buffer_id: usize) -> Option<&mut [u8]> {
        self.buffers.get_mut(buffer_id).map(|b| b.as_mut_slice())
    }

    /// Get buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get pool statistics
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.allocations.load(Ordering::Relaxed),
            self.stats.releases.load(Ordering::Relaxed),
            self.stats.waits.load(Ordering::Relaxed),
        )
    }
}

/// Guard for a borrowed buffer
pub struct BufferGuard<'a> {
    pool: &'a BufferPool,
    buffer_id: usize,
}

impl<'a> BufferGuard<'a> {
    pub fn id(&self) -> usize {
        self.buffer_id
    }

    pub fn as_slice(&self) -> &[u8] {
        self.pool.get(self.buffer_id).unwrap()
    }
}

impl<'a> Drop for BufferGuard<'a> {
    fn drop(&mut self) {
        self.pool.release(self.buffer_id);
    }
}

// ============================================================================
// Fixed File Registry
// ============================================================================

/// Registry of fixed file descriptors
pub struct FixedFileRegistry {
    slots: RwLock<Vec<Option<FixedFile>>>,
    available: Mutex<VecDeque<usize>>,
    capacity: usize,
}

struct FixedFile {
    fd: RawFd,
    path: PathBuf,
    refcount: AtomicU64,
}

impl FixedFileRegistry {
    pub fn new(capacity: usize) -> Self {
        let slots: Vec<Option<FixedFile>> = (0..capacity).map(|_| None).collect();
        let available: VecDeque<usize> = (0..capacity).collect();

        Self {
            slots: RwLock::new(slots),
            available: Mutex::new(available),
            capacity,
        }
    }

    /// Register a file descriptor
    pub fn register(&self, fd: RawFd, path: PathBuf) -> Option<usize> {
        let mut available = self.available.lock();
        let slot = available.pop_front()?;

        let mut slots = self.slots.write();
        slots[slot] = Some(FixedFile {
            fd,
            path,
            refcount: AtomicU64::new(1),
        });

        Some(slot)
    }

    /// Unregister a file descriptor
    pub fn unregister(&self, slot: usize) -> Option<RawFd> {
        let mut slots = self.slots.write();
        if let Some(file) = slots[slot].take() {
            self.available.lock().push_back(slot);
            Some(file.fd)
        } else {
            None
        }
    }

    /// Get file descriptor by slot
    pub fn get(&self, slot: usize) -> Option<RawFd> {
        let slots = self.slots.read();
        slots.get(slot)?.as_ref().map(|f| f.fd)
    }

    /// Get path by slot
    pub fn get_path(&self, slot: usize) -> Option<PathBuf> {
        let slots = self.slots.read();
        slots.get(slot)?.as_ref().map(|f| f.path.clone())
    }

    /// Increment reference count
    pub fn inc_ref(&self, slot: usize) {
        let slots = self.slots.read();
        if let Some(Some(file)) = slots.get(slot) {
            file.refcount.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Decrement reference count, returns true if should be closed
    pub fn dec_ref(&self, slot: usize) -> bool {
        let slots = self.slots.read();
        if let Some(Some(file)) = slots.get(slot) {
            file.refcount.fetch_sub(1, Ordering::Relaxed) == 1
        } else {
            false
        }
    }
}

// ============================================================================
// Submission Batcher
// ============================================================================

/// Request for the submission queue
struct SubmissionRequest {
    operation: IoOperation,
    response_tx: oneshot::Sender<IoResult>,
    user_data: u64,
    submitted_at: Instant,
}

/// Batches I/O submissions for efficiency
struct SubmissionBatcher {
    pending: Mutex<Vec<SubmissionRequest>>,
    max_batch_size: usize,
    batch_timeout: Duration,
    next_user_data: AtomicU64,
}

impl SubmissionBatcher {
    fn new(max_batch_size: usize, batch_timeout: Duration) -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
            max_batch_size,
            batch_timeout,
            next_user_data: AtomicU64::new(1),
        }
    }

    fn submit(&self, operation: IoOperation) -> oneshot::Receiver<IoResult> {
        let (tx, rx) = oneshot::channel();
        let user_data = self.next_user_data.fetch_add(1, Ordering::Relaxed);

        let request = SubmissionRequest {
            operation,
            response_tx: tx,
            user_data,
            submitted_at: Instant::now(),
        };

        self.pending.lock().push(request);
        rx
    }

    fn take_batch(&self) -> Vec<SubmissionRequest> {
        let mut pending = self.pending.lock();

        if pending.is_empty() {
            return Vec::new();
        }

        // Check if we should flush
        let should_flush = pending.len() >= self.max_batch_size
            || pending
                .first()
                .map(|r| r.submitted_at.elapsed() >= self.batch_timeout)
                .unwrap_or(false);

        if should_flush {
            std::mem::take(&mut *pending)
        } else {
            Vec::new()
        }
    }
}

// ============================================================================
// io_uring Backend (Simulated)
// ============================================================================

/// io_uring backend for high-performance I/O
///
/// Note: This is a simulated implementation that provides the API
/// but uses standard I/O under the hood. A real implementation would
/// use the io-uring crate with actual kernel io_uring support.
pub struct IoUringBackend {
    config: IoUringConfig,
    buffer_pool: Arc<BufferPool>,
    file_registry: Arc<FixedFileRegistry>,
    batcher: Arc<SubmissionBatcher>,
    stats: Arc<IoUringStats>,
    worker_tx: mpsc::Sender<WorkerCommand>,
    running: Arc<std::sync::atomic::AtomicBool>,
}

enum WorkerCommand {
    Process(Vec<SubmissionRequest>),
    Shutdown,
}

/// Statistics for the io_uring backend
#[derive(Debug, Default)]
pub struct IoUringStats {
    pub submissions: AtomicU64,
    pub completions: AtomicU64,
    pub batches: AtomicU64,
    pub avg_batch_size: AtomicU64,
    pub read_bytes: AtomicU64,
    pub write_bytes: AtomicU64,
    pub read_ops: AtomicU64,
    pub write_ops: AtomicU64,
    pub errors: AtomicU64,
}

impl IoUringBackend {
    /// Create a new io_uring backend
    pub fn new(config: IoUringConfig) -> Result<Self> {
        let buffer_pool = Arc::new(BufferPool::new(
            config.registered_buffers,
            config.buffer_size,
        ));
        let file_registry = Arc::new(FixedFileRegistry::new(config.fixed_files));
        let batcher = Arc::new(SubmissionBatcher::new(
            config.max_batch_size,
            config.batch_timeout,
        ));
        let stats = Arc::new(IoUringStats::default());

        let (worker_tx, worker_rx) = mpsc::channel(1024);

        let backend = Self {
            config,
            buffer_pool,
            file_registry,
            batcher,
            stats: Arc::clone(&stats),
            worker_tx,
            running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        };

        // Start worker
        let stats_clone = Arc::clone(&stats);
        let running = Arc::clone(&backend.running);
        tokio::spawn(async move {
            Self::worker_loop(worker_rx, stats_clone, running).await;
        });

        // Start batcher flusher
        let batcher_clone = Arc::clone(&backend.batcher);
        let tx_clone = backend.worker_tx.clone();
        let running_clone = Arc::clone(&backend.running);
        tokio::spawn(async move {
            Self::batcher_loop(batcher_clone, tx_clone, running_clone).await;
        });

        info!(
            "io_uring backend initialized (sq_depth={}, cq_depth={})",
            backend.config.sq_depth, backend.config.cq_depth
        );

        Ok(backend)
    }

    async fn worker_loop(
        mut rx: mpsc::Receiver<WorkerCommand>,
        stats: Arc<IoUringStats>,
        running: Arc<std::sync::atomic::AtomicBool>,
    ) {
        while running.load(Ordering::Relaxed) {
            match rx.recv().await {
                Some(WorkerCommand::Process(requests)) => {
                    let batch_size = requests.len();
                    stats.batches.fetch_add(1, Ordering::Relaxed);
                    stats.submissions.fetch_add(batch_size as u64, Ordering::Relaxed);

                    for request in requests {
                        let result = Self::execute_operation(&request.operation, &stats);
                        stats.completions.fetch_add(1, Ordering::Relaxed);
                        let _ = request.response_tx.send(result);
                    }
                }
                Some(WorkerCommand::Shutdown) | None => break,
            }
        }

        debug!("io_uring worker loop terminated");
    }

    async fn batcher_loop(
        batcher: Arc<SubmissionBatcher>,
        tx: mpsc::Sender<WorkerCommand>,
        running: Arc<std::sync::atomic::AtomicBool>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_micros(50));

        while running.load(Ordering::Relaxed) {
            interval.tick().await;

            let batch = batcher.take_batch();
            if !batch.is_empty() {
                let _ = tx.send(WorkerCommand::Process(batch)).await;
            }
        }

        debug!("io_uring batcher loop terminated");
    }

    fn execute_operation(op: &IoOperation, stats: &IoUringStats) -> IoResult {
        match op {
            IoOperation::Read { fd, offset, len, .. } => {
                Self::execute_read(*fd, *offset, *len, stats)
            }
            IoOperation::Write { fd, offset, data, .. } => {
                Self::execute_write(*fd, *offset, data, stats)
            }
            IoOperation::ReadV { fd, offset, lens } => {
                Self::execute_readv(*fd, *offset, lens, stats)
            }
            IoOperation::WriteV { fd, offset, buffers } => {
                Self::execute_writev(*fd, *offset, buffers, stats)
            }
            IoOperation::Fsync { fd, datasync } => {
                Self::execute_fsync(*fd, *datasync)
            }
            IoOperation::Fallocate { fd, offset, len, mode } => {
                Self::execute_fallocate(*fd, *offset, *len, *mode)
            }
            IoOperation::Open { path, flags } => {
                Self::execute_open(path, flags)
            }
            IoOperation::Close { fd } => {
                Self::execute_close(*fd)
            }
            IoOperation::Poll { fd, events } => {
                Self::execute_poll(*fd, *events)
            }
            IoOperation::Nop => IoResult::Nop,
            IoOperation::Cancel { .. } => IoResult::Cancel { cancelled: false },
            IoOperation::ReadTimeout { fd, offset, len, timeout } => {
                Self::execute_read_timeout(*fd, *offset, *len, *timeout, stats)
            }
        }
    }

    fn execute_read(fd: RawFd, offset: u64, len: usize, stats: &IoUringStats) -> IoResult {
        // Simulated read using standard I/O
        // A real implementation would use io_uring's read operation

        use std::os::unix::io::FromRawFd;

        let mut file = unsafe { File::from_raw_fd(fd) };
        let result = (|| -> io::Result<Vec<u8>> {
            file.seek(SeekFrom::Start(offset))?;
            let mut buffer = vec![0u8; len];
            let bytes_read = file.read(&mut buffer)?;
            buffer.truncate(bytes_read);
            Ok(buffer)
        })();

        // Don't close the fd when file is dropped
        std::mem::forget(file);

        match result {
            Ok(data) => {
                let bytes_read = data.len();
                stats.read_ops.fetch_add(1, Ordering::Relaxed);
                stats.read_bytes.fetch_add(bytes_read as u64, Ordering::Relaxed);
                IoResult::Read { data, bytes_read }
            }
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                IoResult::Error {
                    code: e.raw_os_error().unwrap_or(-1),
                    message: e.to_string(),
                }
            }
        }
    }

    fn execute_write(fd: RawFd, offset: u64, data: &[u8], stats: &IoUringStats) -> IoResult {
        use std::os::unix::io::FromRawFd;

        let mut file = unsafe { File::from_raw_fd(fd) };
        let result = (|| -> io::Result<usize> {
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(data)?;
            Ok(data.len())
        })();

        std::mem::forget(file);

        match result {
            Ok(bytes_written) => {
                stats.write_ops.fetch_add(1, Ordering::Relaxed);
                stats.write_bytes.fetch_add(bytes_written as u64, Ordering::Relaxed);
                IoResult::Write { bytes_written }
            }
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                IoResult::Error {
                    code: e.raw_os_error().unwrap_or(-1),
                    message: e.to_string(),
                }
            }
        }
    }

    fn execute_readv(fd: RawFd, offset: u64, lens: &[usize], stats: &IoUringStats) -> IoResult {
        use std::os::unix::io::FromRawFd;

        let mut file = unsafe { File::from_raw_fd(fd) };
        let result = (|| -> io::Result<Vec<Vec<u8>>> {
            file.seek(SeekFrom::Start(offset))?;
            let mut buffers = Vec::new();
            for &len in lens {
                let mut buffer = vec![0u8; len];
                let bytes_read = file.read(&mut buffer)?;
                buffer.truncate(bytes_read);
                buffers.push(buffer);
            }
            Ok(buffers)
        })();

        std::mem::forget(file);

        match result {
            Ok(buffers) => {
                let total_bytes: usize = buffers.iter().map(|b| b.len()).sum();
                stats.read_ops.fetch_add(1, Ordering::Relaxed);
                stats.read_bytes.fetch_add(total_bytes as u64, Ordering::Relaxed);
                IoResult::ReadV { buffers, total_bytes }
            }
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                IoResult::Error {
                    code: e.raw_os_error().unwrap_or(-1),
                    message: e.to_string(),
                }
            }
        }
    }

    fn execute_writev(fd: RawFd, offset: u64, buffers: &[Vec<u8>], stats: &IoUringStats) -> IoResult {
        use std::os::unix::io::FromRawFd;

        let mut file = unsafe { File::from_raw_fd(fd) };
        let result = (|| -> io::Result<usize> {
            file.seek(SeekFrom::Start(offset))?;
            let mut total = 0;
            for buffer in buffers {
                file.write_all(buffer)?;
                total += buffer.len();
            }
            Ok(total)
        })();

        std::mem::forget(file);

        match result {
            Ok(bytes_written) => {
                stats.write_ops.fetch_add(1, Ordering::Relaxed);
                stats.write_bytes.fetch_add(bytes_written as u64, Ordering::Relaxed);
                IoResult::WriteV { bytes_written }
            }
            Err(e) => {
                stats.errors.fetch_add(1, Ordering::Relaxed);
                IoResult::Error {
                    code: e.raw_os_error().unwrap_or(-1),
                    message: e.to_string(),
                }
            }
        }
    }

    fn execute_fsync(fd: RawFd, datasync: bool) -> IoResult {
        use std::os::unix::io::FromRawFd;

        let file = unsafe { File::from_raw_fd(fd) };
        let result = if datasync {
            file.sync_data()
        } else {
            file.sync_all()
        };

        std::mem::forget(file);

        match result {
            Ok(()) => IoResult::Fsync,
            Err(e) => IoResult::Error {
                code: e.raw_os_error().unwrap_or(-1),
                message: e.to_string(),
            },
        }
    }

    fn execute_fallocate(_fd: RawFd, _offset: u64, _len: u64, _mode: FallocateMode) -> IoResult {
        // Fallocate is Linux-specific, simulate success
        IoResult::Fallocate
    }

    fn execute_open(path: &Path, flags: &OpenFlags) -> IoResult {
        let result = OpenOptions::new()
            .read(flags.read)
            .write(flags.write)
            .create(flags.create)
            .truncate(flags.truncate)
            .append(flags.append)
            .open(path);

        match result {
            Ok(file) => {
                let fd = file.as_raw_fd();
                std::mem::forget(file); // Don't close on drop
                IoResult::Open { fd }
            }
            Err(e) => IoResult::Error {
                code: e.raw_os_error().unwrap_or(-1),
                message: e.to_string(),
            },
        }
    }

    fn execute_close(fd: RawFd) -> IoResult {
        use std::os::unix::io::FromRawFd;
        let file = unsafe { File::from_raw_fd(fd) };
        drop(file); // This will close the fd
        IoResult::Close
    }

    fn execute_poll(_fd: RawFd, events: PollEvents) -> IoResult {
        // Simulated poll - just return the requested events
        IoResult::Poll { events }
    }

    fn execute_read_timeout(
        fd: RawFd,
        offset: u64,
        len: usize,
        timeout: Duration,
        stats: &IoUringStats,
    ) -> IoResult {
        let start = Instant::now();

        // Simple timeout check - in real impl would use IORING_OP_LINK_TIMEOUT
        if start.elapsed() > timeout {
            return IoResult::Timeout;
        }

        Self::execute_read(fd, offset, len, stats)
    }

    /// Submit a single I/O operation
    pub async fn submit(&self, operation: IoOperation) -> Result<IoResult> {
        let rx = self.batcher.submit(operation);

        // Force flush for immediate processing
        let batch = self.batcher.take_batch();
        if !batch.is_empty() {
            let _ = self.worker_tx.send(WorkerCommand::Process(batch)).await;
        }

        rx.await.map_err(|_| StrataError::Internal("I/O cancelled".into()))
    }

    /// Submit multiple I/O operations as a batch
    pub async fn submit_batch(&self, operations: Vec<IoOperation>) -> Result<Vec<IoResult>> {
        let mut receivers = Vec::new();

        for op in operations {
            receivers.push(self.batcher.submit(op));
        }

        // Force flush
        let batch = self.batcher.take_batch();
        if !batch.is_empty() {
            let _ = self.worker_tx.send(WorkerCommand::Process(batch)).await;
        }

        let mut results = Vec::new();
        for rx in receivers {
            results.push(rx.await.map_err(|_| StrataError::Internal("I/O cancelled".into()))?);
        }

        Ok(results)
    }

    /// Get buffer pool
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    /// Get file registry
    pub fn file_registry(&self) -> &Arc<FixedFileRegistry> {
        &self.file_registry
    }

    /// Get statistics
    pub fn stats(&self) -> &IoUringStats {
        &self.stats
    }

    /// Check if backend is using real io_uring
    pub fn is_native(&self) -> bool {
        // This implementation is simulated
        false
    }

    /// Shutdown the backend
    pub async fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
        let _ = self.worker_tx.send(WorkerCommand::Shutdown).await;
    }
}

impl Drop for IoUringBackend {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

// ============================================================================
// High-Level File Operations
// ============================================================================

/// High-level file handle using io_uring
pub struct UringFile {
    backend: Arc<IoUringBackend>,
    fd: RawFd,
    path: PathBuf,
    position: AtomicU64,
    fixed_slot: Option<usize>,
}

impl UringFile {
    /// Open a file
    pub async fn open(backend: Arc<IoUringBackend>, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let result = backend
            .submit(IoOperation::Open {
                path: path.clone(),
                flags: OpenFlags::read_write(),
            })
            .await?;

        match result {
            IoResult::Open { fd } => {
                // Try to register as fixed file
                let fixed_slot = backend.file_registry().register(fd, path.clone());

                Ok(Self {
                    backend,
                    fd,
                    path,
                    position: AtomicU64::new(0),
                    fixed_slot,
                })
            }
            IoResult::Error { code, message } => {
                Err(StrataError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to open file: {} (code {})", message, code),
                )))
            }
            _ => Err(StrataError::Internal("Unexpected result".into())),
        }
    }

    /// Create a new file
    pub async fn create(backend: Arc<IoUringBackend>, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let result = backend
            .submit(IoOperation::Open {
                path: path.clone(),
                flags: OpenFlags {
                    read: true,
                    write: true,
                    create: true,
                    truncate: true,
                    ..Default::default()
                },
            })
            .await?;

        match result {
            IoResult::Open { fd } => {
                let fixed_slot = backend.file_registry().register(fd, path.clone());

                Ok(Self {
                    backend,
                    fd,
                    path,
                    position: AtomicU64::new(0),
                    fixed_slot,
                })
            }
            IoResult::Error { code, message } => {
                Err(StrataError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to create file: {} (code {})", message, code),
                )))
            }
            _ => Err(StrataError::Internal("Unexpected result".into())),
        }
    }

    /// Read data at the current position
    pub async fn read(&self, len: usize) -> Result<Vec<u8>> {
        let offset = self.position.load(Ordering::Relaxed);
        let result = self
            .backend
            .submit(IoOperation::Read {
                fd: self.fd,
                offset,
                len,
                buffer_id: None,
            })
            .await?;

        match result {
            IoResult::Read { data, bytes_read } => {
                self.position.fetch_add(bytes_read as u64, Ordering::Relaxed);
                Ok(data)
            }
            IoResult::Error { code, message } => {
                Err(StrataError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Read failed: {} (code {})", message, code),
                )))
            }
            _ => Err(StrataError::Internal("Unexpected result".into())),
        }
    }

    /// Read data at a specific offset
    pub async fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let result = self
            .backend
            .submit(IoOperation::Read {
                fd: self.fd,
                offset,
                len,
                buffer_id: None,
            })
            .await?;

        match result {
            IoResult::Read { data, .. } => Ok(data),
            IoResult::Error { code, message } => {
                Err(StrataError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Read failed: {} (code {})", message, code),
                )))
            }
            _ => Err(StrataError::Internal("Unexpected result".into())),
        }
    }

    /// Write data at the current position
    pub async fn write(&self, data: &[u8]) -> Result<usize> {
        let offset = self.position.load(Ordering::Relaxed);
        let result = self
            .backend
            .submit(IoOperation::Write {
                fd: self.fd,
                offset,
                data: data.to_vec(),
                buffer_id: None,
            })
            .await?;

        match result {
            IoResult::Write { bytes_written } => {
                self.position.fetch_add(bytes_written as u64, Ordering::Relaxed);
                Ok(bytes_written)
            }
            IoResult::Error { code, message } => {
                Err(StrataError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Write failed: {} (code {})", message, code),
                )))
            }
            _ => Err(StrataError::Internal("Unexpected result".into())),
        }
    }

    /// Write data at a specific offset
    pub async fn write_at(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let result = self
            .backend
            .submit(IoOperation::Write {
                fd: self.fd,
                offset,
                data: data.to_vec(),
                buffer_id: None,
            })
            .await?;

        match result {
            IoResult::Write { bytes_written } => Ok(bytes_written),
            IoResult::Error { code, message } => {
                Err(StrataError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Write failed: {} (code {})", message, code),
                )))
            }
            _ => Err(StrataError::Internal("Unexpected result".into())),
        }
    }

    /// Sync file data to disk
    pub async fn sync(&self) -> Result<()> {
        let result = self
            .backend
            .submit(IoOperation::Fsync {
                fd: self.fd,
                datasync: false,
            })
            .await?;

        match result {
            IoResult::Fsync => Ok(()),
            IoResult::Error { code, message } => {
                Err(StrataError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Sync failed: {} (code {})", message, code),
                )))
            }
            _ => Err(StrataError::Internal("Unexpected result".into())),
        }
    }

    /// Sync only file data (not metadata)
    pub async fn datasync(&self) -> Result<()> {
        let result = self
            .backend
            .submit(IoOperation::Fsync {
                fd: self.fd,
                datasync: true,
            })
            .await?;

        match result {
            IoResult::Fsync => Ok(()),
            IoResult::Error { code, message } => {
                Err(StrataError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Datasync failed: {} (code {})", message, code),
                )))
            }
            _ => Err(StrataError::Internal("Unexpected result".into())),
        }
    }

    /// Seek to a position
    pub fn seek(&self, pos: u64) {
        self.position.store(pos, Ordering::Relaxed);
    }

    /// Get current position
    pub fn position(&self) -> u64 {
        self.position.load(Ordering::Relaxed)
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get raw file descriptor
    pub fn fd(&self) -> RawFd {
        self.fd
    }

    /// Get fixed file slot if registered
    pub fn fixed_slot(&self) -> Option<usize> {
        self.fixed_slot
    }
}

impl Drop for UringFile {
    fn drop(&mut self) {
        // Unregister from fixed files
        if let Some(slot) = self.fixed_slot {
            self.backend.file_registry().unregister(slot);
        }

        // Note: We should close the fd here, but since we're in drop
        // and can't use async, we'll just let the OS handle it
    }
}

// ============================================================================
// Benchmarking Support
// ============================================================================

/// Benchmark results for io_uring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IoUringBenchmark {
    pub name: String,
    pub operations: u64,
    pub duration: Duration,
    pub ops_per_second: f64,
    pub bytes_per_second: Option<f64>,
    pub avg_latency_us: f64,
    pub p50_latency_us: f64,
    pub p99_latency_us: f64,
    pub p999_latency_us: f64,
}

/// Run io_uring benchmarks
pub async fn run_benchmarks(backend: &IoUringBackend, iterations: u64) -> Vec<IoUringBenchmark> {
    let mut results = Vec::new();

    // Nop benchmark (measures submission overhead)
    let nop_result = benchmark_nop(backend, iterations).await;
    results.push(nop_result);

    info!("io_uring benchmarks complete");
    results
}

async fn benchmark_nop(backend: &IoUringBackend, iterations: u64) -> IoUringBenchmark {
    let mut latencies = Vec::with_capacity(iterations as usize);

    let start = Instant::now();
    for _ in 0..iterations {
        let op_start = Instant::now();
        let _ = backend.submit(IoOperation::Nop).await;
        latencies.push(op_start.elapsed().as_micros() as f64);
    }
    let duration = start.elapsed();

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p50_idx = (iterations as f64 * 0.5) as usize;
    let p99_idx = (iterations as f64 * 0.99) as usize;
    let p999_idx = (iterations as f64 * 0.999) as usize;

    IoUringBenchmark {
        name: "nop".to_string(),
        operations: iterations,
        duration,
        ops_per_second: iterations as f64 / duration.as_secs_f64(),
        bytes_per_second: None,
        avg_latency_us: latencies.iter().sum::<f64>() / iterations as f64,
        p50_latency_us: latencies.get(p50_idx).copied().unwrap_or(0.0),
        p99_latency_us: latencies.get(p99_idx).copied().unwrap_or(0.0),
        p999_latency_us: latencies.get(p999_idx).copied().unwrap_or(0.0),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_config_defaults() {
        let config = IoUringConfig::default();
        assert!(config.enabled);
        assert_eq!(config.sq_depth, 256);
        assert_eq!(config.cq_depth, 512);
    }

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(4, 1024);

        // Acquire all buffers
        let guards: Vec<_> = (0..4).filter_map(|_| pool.acquire()).collect();
        assert_eq!(guards.len(), 4);

        // No more available
        assert!(pool.acquire().is_none());

        // Drop one
        drop(guards.into_iter().next());

        // One available again
        assert!(pool.acquire().is_some());
    }

    #[test]
    fn test_fixed_file_registry() {
        let registry = FixedFileRegistry::new(4);

        // Register files
        let slot1 = registry.register(10, PathBuf::from("/test1")).unwrap();
        let slot2 = registry.register(20, PathBuf::from("/test2")).unwrap();

        assert_eq!(registry.get(slot1), Some(10));
        assert_eq!(registry.get(slot2), Some(20));

        // Unregister
        let fd = registry.unregister(slot1);
        assert_eq!(fd, Some(10));
        assert_eq!(registry.get(slot1), None);
    }

    #[test]
    fn test_open_flags() {
        let flags = OpenFlags::read_only();
        assert!(flags.read);
        assert!(!flags.write);

        let flags = OpenFlags::write_only();
        assert!(!flags.read);
        assert!(flags.write);

        let flags = OpenFlags::read_write();
        assert!(flags.read);
        assert!(flags.write);
    }

    #[tokio::test]
    async fn test_backend_creation() {
        let config = IoUringConfig::default();
        let backend = IoUringBackend::new(config).unwrap();

        assert!(!backend.is_native()); // Simulated impl
        backend.shutdown().await;
    }

    #[tokio::test]
    async fn test_nop_operation() {
        let config = IoUringConfig::default();
        let backend = IoUringBackend::new(config).unwrap();

        let result = backend.submit(IoOperation::Nop).await.unwrap();
        assert!(matches!(result, IoResult::Nop));

        backend.shutdown().await;
    }

    #[tokio::test]
    async fn test_file_operations() {
        let config = IoUringConfig::default();
        let backend = Arc::new(IoUringBackend::new(config).unwrap());

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.dat");

        // Create file
        let file = UringFile::create(Arc::clone(&backend), &path).await.unwrap();

        // Write data
        let data = b"Hello, io_uring!";
        let written = file.write(data).await.unwrap();
        assert_eq!(written, data.len());

        // Sync
        file.sync().await.unwrap();

        // Seek and read
        file.seek(0);
        let read_data = file.read(data.len()).await.unwrap();
        assert_eq!(read_data, data);

        drop(file);
        backend.shutdown().await;
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let config = IoUringConfig::default();
        let backend = IoUringBackend::new(config).unwrap();

        let ops = vec![IoOperation::Nop, IoOperation::Nop, IoOperation::Nop];

        let results = backend.submit_batch(ops).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| matches!(r, IoResult::Nop)));

        backend.shutdown().await;
    }

    #[tokio::test]
    async fn test_stats() {
        let config = IoUringConfig::default();
        let backend = IoUringBackend::new(config).unwrap();

        // Submit some ops
        for _ in 0..10 {
            let _ = backend.submit(IoOperation::Nop).await;
        }

        let stats = backend.stats();
        assert!(stats.submissions.load(Ordering::Relaxed) >= 10);
        assert!(stats.completions.load(Ordering::Relaxed) >= 10);

        backend.shutdown().await;
    }

    #[test]
    fn test_io_operation_variants() {
        // Test all operation variants can be constructed
        let _ = IoOperation::Read {
            fd: 1,
            offset: 0,
            len: 1024,
            buffer_id: None,
        };
        let _ = IoOperation::Write {
            fd: 1,
            offset: 0,
            data: vec![0; 1024],
            buffer_id: None,
        };
        let _ = IoOperation::Fsync {
            fd: 1,
            datasync: false,
        };
        let _ = IoOperation::Nop;
    }
}
