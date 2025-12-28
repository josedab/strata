// RDMA Memory Registration and Management

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Memory region for RDMA operations
#[derive(Debug, Clone)]
pub struct MemoryRegion {
    /// Region ID
    pub id: u64,
    /// Start address
    pub addr: u64,
    /// Length in bytes
    pub length: usize,
    /// Local key (for local access)
    pub lkey: u32,
    /// Remote key (for remote access)
    pub rkey: u32,
    /// Access flags
    pub access: MemoryAccess,
    /// Whether this is hugepage memory
    pub hugepage: bool,
}

/// Memory access flags
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryAccess {
    /// Local read access
    pub local_read: bool,
    /// Local write access
    pub local_write: bool,
    /// Remote read access
    pub remote_read: bool,
    /// Remote write access
    pub remote_write: bool,
    /// Atomic access
    pub atomic: bool,
}

impl Default for MemoryAccess {
    fn default() -> Self {
        Self {
            local_read: true,
            local_write: true,
            remote_read: false,
            remote_write: false,
            atomic: false,
        }
    }
}

impl MemoryAccess {
    /// Full access for RDMA operations
    pub fn full() -> Self {
        Self {
            local_read: true,
            local_write: true,
            remote_read: true,
            remote_write: true,
            atomic: true,
        }
    }

    /// Read-only access
    pub fn read_only() -> Self {
        Self {
            local_read: true,
            local_write: false,
            remote_read: true,
            remote_write: false,
            atomic: false,
        }
    }
}

/// Registered buffer for zero-copy operations
pub struct RegisteredBuffer {
    /// Memory region
    region: MemoryRegion,
    /// Underlying data
    data: Vec<u8>,
    /// Current read position
    read_pos: usize,
    /// Current write position
    write_pos: usize,
}

impl RegisteredBuffer {
    /// Creates a new registered buffer
    pub fn new(size: usize, access: MemoryAccess) -> Result<Self> {
        let data = vec![0u8; size];
        let addr = data.as_ptr() as u64;

        let region = MemoryRegion {
            id: rand::random(),
            addr,
            length: size,
            lkey: rand::random(),
            rkey: rand::random(),
            access,
            hugepage: false,
        };

        Ok(Self {
            region,
            data,
            read_pos: 0,
            write_pos: 0,
        })
    }

    /// Gets the memory region info
    pub fn region(&self) -> &MemoryRegion {
        &self.region
    }

    /// Gets the local key
    pub fn lkey(&self) -> u32 {
        self.region.lkey
    }

    /// Gets the remote key
    pub fn rkey(&self) -> u32 {
        self.region.rkey
    }

    /// Gets the address
    pub fn addr(&self) -> u64 {
        self.region.addr
    }

    /// Gets the length
    pub fn len(&self) -> usize {
        self.region.length
    }

    /// Checks if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.region.length == 0
    }

    /// Gets a slice of the underlying data
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Gets a mutable slice of the underlying data
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Writes data to the buffer
    pub fn write(&mut self, data: &[u8]) -> Result<usize> {
        let available = self.data.len() - self.write_pos;
        let to_write = data.len().min(available);

        if to_write == 0 {
            return Err(StrataError::BufferFull("RDMA buffer full".to_string()));
        }

        self.data[self.write_pos..self.write_pos + to_write].copy_from_slice(&data[..to_write]);
        self.write_pos += to_write;
        Ok(to_write)
    }

    /// Reads data from the buffer
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let available = self.write_pos - self.read_pos;
        let to_read = buf.len().min(available);

        if to_read == 0 {
            return Ok(0);
        }

        buf[..to_read].copy_from_slice(&self.data[self.read_pos..self.read_pos + to_read]);
        self.read_pos += to_read;
        Ok(to_read)
    }

    /// Resets the buffer positions
    pub fn reset(&mut self) {
        self.read_pos = 0;
        self.write_pos = 0;
    }

    /// Gets available space for writing
    pub fn available(&self) -> usize {
        self.data.len() - self.write_pos
    }

    /// Gets readable data length
    pub fn readable(&self) -> usize {
        self.write_pos - self.read_pos
    }
}

/// Memory pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryPoolConfig {
    /// Number of buffers
    pub num_buffers: usize,
    /// Size of each buffer
    pub buffer_size: usize,
    /// Use hugepages
    pub hugepages: bool,
    /// Memory access flags
    pub access: MemoryAccess,
    /// NUMA node affinity
    pub numa_node: Option<u32>,
}

impl Default for MemoryPoolConfig {
    fn default() -> Self {
        Self {
            num_buffers: 1024,
            buffer_size: 4096,
            hugepages: false,
            access: MemoryAccess::full(),
            numa_node: None,
        }
    }
}

/// Pool of registered memory buffers
pub struct MemoryPool {
    /// Configuration
    config: MemoryPoolConfig,
    /// Available buffers
    free_buffers: Arc<RwLock<Vec<RegisteredBuffer>>>,
    /// In-use buffers
    used_buffers: Arc<RwLock<HashMap<u64, RegisteredBuffer>>>,
    /// Statistics
    stats: Arc<MemoryPoolStats>,
}

/// Memory pool statistics
pub struct MemoryPoolStats {
    /// Total allocations
    pub allocations: AtomicU64,
    /// Total deallocations
    pub deallocations: AtomicU64,
    /// Current in-use buffers
    pub in_use: AtomicU64,
    /// Peak usage
    pub peak_usage: AtomicU64,
}

impl MemoryPool {
    /// Creates a new memory pool
    pub fn new(config: MemoryPoolConfig) -> Result<Self> {
        let mut free_buffers = Vec::with_capacity(config.num_buffers);

        // Pre-allocate buffers
        for _ in 0..config.num_buffers {
            let buffer = RegisteredBuffer::new(config.buffer_size, config.access.clone())?;
            free_buffers.push(buffer);
        }

        Ok(Self {
            config,
            free_buffers: Arc::new(RwLock::new(free_buffers)),
            used_buffers: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(MemoryPoolStats {
                allocations: AtomicU64::new(0),
                deallocations: AtomicU64::new(0),
                in_use: AtomicU64::new(0),
                peak_usage: AtomicU64::new(0),
            }),
        })
    }

    /// Allocates a buffer from the pool
    pub async fn allocate(&self) -> Result<RegisteredBuffer> {
        let mut free = self.free_buffers.write().await;

        match free.pop() {
            Some(mut buffer) => {
                buffer.reset();
                let _id = buffer.region.id;

                self.stats.allocations.fetch_add(1, Ordering::Relaxed);
                let in_use = self.stats.in_use.fetch_add(1, Ordering::Relaxed) + 1;

                // Update peak
                let mut peak = self.stats.peak_usage.load(Ordering::Relaxed);
                while in_use > peak {
                    match self.stats.peak_usage.compare_exchange_weak(
                        peak,
                        in_use,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(p) => peak = p,
                    }
                }

                Ok(buffer)
            }
            None => Err(StrataError::ResourceExhausted("Memory pool exhausted".to_string())),
        }
    }

    /// Returns a buffer to the pool
    pub async fn deallocate(&self, buffer: RegisteredBuffer) {
        self.stats.deallocations.fetch_add(1, Ordering::Relaxed);
        self.stats.in_use.fetch_sub(1, Ordering::Relaxed);

        let mut free = self.free_buffers.write().await;
        free.push(buffer);
    }

    /// Gets pool statistics
    pub fn stats(&self) -> PoolStatsSnapshot {
        PoolStatsSnapshot {
            allocations: self.stats.allocations.load(Ordering::Relaxed),
            deallocations: self.stats.deallocations.load(Ordering::Relaxed),
            in_use: self.stats.in_use.load(Ordering::Relaxed),
            peak_usage: self.stats.peak_usage.load(Ordering::Relaxed),
            total_buffers: self.config.num_buffers as u64,
            buffer_size: self.config.buffer_size,
        }
    }

    /// Gets the number of available buffers
    pub async fn available(&self) -> usize {
        self.free_buffers.read().await.len()
    }

    /// Gets pool configuration
    pub fn config(&self) -> &MemoryPoolConfig {
        &self.config
    }
}

/// Snapshot of pool statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatsSnapshot {
    pub allocations: u64,
    pub deallocations: u64,
    pub in_use: u64,
    pub peak_usage: u64,
    pub total_buffers: u64,
    pub buffer_size: usize,
}

/// Scatter-Gather Entry for RDMA operations
#[derive(Debug, Clone)]
pub struct ScatterGatherEntry {
    /// Address
    pub addr: u64,
    /// Length
    pub length: u32,
    /// Local key
    pub lkey: u32,
}

impl ScatterGatherEntry {
    /// Creates from a registered buffer
    pub fn from_buffer(buffer: &RegisteredBuffer) -> Self {
        Self {
            addr: buffer.addr(),
            length: buffer.len() as u32,
            lkey: buffer.lkey(),
        }
    }

    /// Creates for a specific region of a buffer
    pub fn from_buffer_region(buffer: &RegisteredBuffer, offset: usize, length: usize) -> Self {
        Self {
            addr: buffer.addr() + offset as u64,
            length: length as u32,
            lkey: buffer.lkey(),
        }
    }
}

/// Scatter-Gather List
#[derive(Debug, Clone)]
pub struct ScatterGatherList {
    entries: Vec<ScatterGatherEntry>,
}

impl ScatterGatherList {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn push(&mut self, entry: ScatterGatherEntry) {
        self.entries.push(entry);
    }

    pub fn entries(&self) -> &[ScatterGatherEntry] {
        &self.entries
    }

    pub fn total_length(&self) -> u64 {
        self.entries.iter().map(|e| e.length as u64).sum()
    }
}

impl Default for ScatterGatherList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registered_buffer() {
        let mut buffer = RegisteredBuffer::new(1024, MemoryAccess::full()).unwrap();

        assert_eq!(buffer.len(), 1024);
        assert_eq!(buffer.available(), 1024);

        let data = b"hello world";
        buffer.write(data).unwrap();

        assert_eq!(buffer.readable(), 11);
        assert_eq!(buffer.available(), 1024 - 11);

        let mut read_buf = [0u8; 11];
        buffer.read(&mut read_buf).unwrap();

        assert_eq!(&read_buf, data);
    }

    #[tokio::test]
    async fn test_memory_pool() {
        let config = MemoryPoolConfig {
            num_buffers: 10,
            buffer_size: 1024,
            ..Default::default()
        };

        let pool = MemoryPool::new(config).unwrap();

        // Allocate a buffer
        let buffer = pool.allocate().await.unwrap();
        assert_eq!(pool.stats().in_use, 1);

        // Deallocate
        pool.deallocate(buffer).await;
        assert_eq!(pool.stats().in_use, 0);
    }

    #[tokio::test]
    async fn test_pool_exhaustion() {
        let config = MemoryPoolConfig {
            num_buffers: 2,
            buffer_size: 64,
            ..Default::default()
        };

        let pool = MemoryPool::new(config).unwrap();

        let _buf1 = pool.allocate().await.unwrap();
        let _buf2 = pool.allocate().await.unwrap();

        // Third allocation should fail
        assert!(pool.allocate().await.is_err());
    }

    #[test]
    fn test_scatter_gather_list() {
        let buffer = RegisteredBuffer::new(4096, MemoryAccess::full()).unwrap();

        let mut sgl = ScatterGatherList::new();
        sgl.push(ScatterGatherEntry::from_buffer_region(&buffer, 0, 1024));
        sgl.push(ScatterGatherEntry::from_buffer_region(&buffer, 1024, 1024));

        assert_eq!(sgl.entries().len(), 2);
        assert_eq!(sgl.total_length(), 2048);
    }
}
