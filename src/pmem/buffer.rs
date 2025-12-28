// PMEM Persistent Buffers

use super::allocator::{PmemAllocator, PmemAllocation};
use super::device::PmemDevice;
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Persistent buffer in PMEM
pub struct PersistentBuffer {
    /// Allocation info
    allocation: PmemAllocation,
    /// Cached data (for writes)
    write_buffer: Vec<u8>,
    /// Current position
    position: usize,
    /// Whether buffer has unflushed writes
    dirty: bool,
    /// Buffer metadata
    metadata: BufferMetadata,
}

/// Buffer metadata stored in PMEM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferMetadata {
    /// Buffer ID
    pub id: u64,
    /// Creation timestamp
    pub created_at: u64,
    /// Last modified timestamp
    pub modified_at: u64,
    /// Data length (valid bytes)
    pub data_len: usize,
    /// Checksum
    pub checksum: u64,
    /// User tags
    pub tags: HashMap<String, String>,
}

impl PersistentBuffer {
    /// Creates a new persistent buffer
    pub fn new(allocation: PmemAllocation) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            allocation: allocation.clone(),
            write_buffer: Vec::new(),
            position: 0,
            dirty: false,
            metadata: BufferMetadata {
                id: allocation.id,
                created_at: now,
                modified_at: now,
                data_len: 0,
                checksum: 0,
                tags: HashMap::new(),
            },
        }
    }

    /// Gets the allocation
    pub fn allocation(&self) -> &PmemAllocation {
        &self.allocation
    }

    /// Gets the buffer size
    pub fn capacity(&self) -> u64 {
        self.allocation.size
    }

    /// Gets the current data length
    pub fn len(&self) -> usize {
        self.metadata.data_len
    }

    /// Checks if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.metadata.data_len == 0
    }

    /// Gets the current position
    pub fn position(&self) -> usize {
        self.position
    }

    /// Seeks to a position
    pub fn seek(&mut self, position: usize) -> Result<()> {
        if position > self.metadata.data_len {
            return Err(StrataError::InvalidOperation(
                "Seek beyond data length".to_string(),
            ));
        }
        self.position = position;
        Ok(())
    }

    /// Writes data to the buffer
    pub fn write(&mut self, data: &[u8]) -> Result<usize> {
        let available = self.allocation.size as usize - self.position;
        let to_write = data.len().min(available);

        if to_write == 0 {
            return Err(StrataError::BufferFull("PMEM buffer full".to_string()));
        }

        // Extend write buffer if needed
        if self.write_buffer.len() < self.position + to_write {
            self.write_buffer.resize(self.position + to_write, 0);
        }

        self.write_buffer[self.position..self.position + to_write]
            .copy_from_slice(&data[..to_write]);
        self.position += to_write;
        self.metadata.data_len = self.metadata.data_len.max(self.position);
        self.dirty = true;

        Ok(to_write)
    }

    /// Reads data from the buffer
    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let available = self.metadata.data_len - self.position;
        let to_read = buf.len().min(available);

        if to_read == 0 {
            return Ok(0);
        }

        buf[..to_read].copy_from_slice(
            &self.write_buffer[self.position..self.position + to_read],
        );
        self.position += to_read;

        Ok(to_read)
    }

    /// Flushes the buffer to PMEM
    pub fn flush(&mut self, device: &mut PmemDevice) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }

        // Write data
        device.write(self.allocation.offset, &self.write_buffer)?;

        // Ensure persistence
        device.flush(self.allocation.offset as u64, self.write_buffer.len())?;
        device.fence();

        // Update metadata
        self.metadata.modified_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.metadata.checksum = calculate_checksum(&self.write_buffer);

        self.dirty = false;

        Ok(())
    }

    /// Loads buffer data from PMEM
    pub fn load(&mut self, device: &mut PmemDevice) -> Result<()> {
        let data = device.read(self.allocation.offset, self.allocation.size as usize)?;
        self.write_buffer = data;
        self.position = 0;
        self.dirty = false;
        Ok(())
    }

    /// Gets buffer metadata
    pub fn metadata(&self) -> &BufferMetadata {
        &self.metadata
    }

    /// Sets a tag
    pub fn set_tag(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.tags.insert(key.into(), value.into());
    }

    /// Gets a tag
    pub fn get_tag(&self, key: &str) -> Option<&String> {
        self.metadata.tags.get(key)
    }

    /// Checks if buffer is dirty
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Resets the buffer
    pub fn reset(&mut self) {
        self.write_buffer.clear();
        self.position = 0;
        self.metadata.data_len = 0;
        self.dirty = false;
    }
}

/// Simple checksum calculation
fn calculate_checksum(data: &[u8]) -> u64 {
    let mut sum: u64 = 0;
    for chunk in data.chunks(8) {
        let mut bytes = [0u8; 8];
        bytes[..chunk.len()].copy_from_slice(chunk);
        sum = sum.wrapping_add(u64::from_le_bytes(bytes));
    }
    sum
}

/// Buffer pool for managing persistent buffers
pub struct BufferPool {
    /// Allocator
    allocator: Arc<PmemAllocator>,
    /// Buffer size
    buffer_size: u64,
    /// Free buffers
    free_buffers: Arc<RwLock<Vec<PersistentBuffer>>>,
    /// Active buffers
    active_buffers: Arc<RwLock<HashMap<u64, PersistentBuffer>>>,
    /// Statistics
    stats: Arc<BufferPoolStats>,
    /// Pool configuration
    config: BufferPoolConfig,
}

/// Buffer pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferPoolConfig {
    /// Number of pre-allocated buffers
    pub preallocate: usize,
    /// Maximum buffers in pool
    pub max_buffers: usize,
    /// Buffer size
    pub buffer_size: u64,
    /// Enable auto-flush
    pub auto_flush: bool,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            preallocate: 16,
            max_buffers: 1024,
            buffer_size: 64 * 1024, // 64KB
            auto_flush: true,
            flush_interval_ms: 100,
        }
    }
}

/// Buffer pool statistics
pub struct BufferPoolStats {
    /// Allocations
    pub allocations: AtomicU64,
    /// Deallocations
    pub deallocations: AtomicU64,
    /// Pool hits
    pub pool_hits: AtomicU64,
    /// Pool misses
    pub pool_misses: AtomicU64,
    /// Flushes
    pub flushes: AtomicU64,
}

impl Default for BufferPoolStats {
    fn default() -> Self {
        Self {
            allocations: AtomicU64::new(0),
            deallocations: AtomicU64::new(0),
            pool_hits: AtomicU64::new(0),
            pool_misses: AtomicU64::new(0),
            flushes: AtomicU64::new(0),
        }
    }
}

impl BufferPool {
    /// Creates a new buffer pool
    pub async fn new(allocator: Arc<PmemAllocator>, config: BufferPoolConfig) -> Result<Self> {
        let pool = Self {
            allocator,
            buffer_size: config.buffer_size,
            free_buffers: Arc::new(RwLock::new(Vec::with_capacity(config.preallocate))),
            active_buffers: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(BufferPoolStats::default()),
            config,
        };

        // Pre-allocate buffers
        pool.preallocate().await?;

        Ok(pool)
    }

    /// Pre-allocates buffers
    async fn preallocate(&self) -> Result<()> {
        let mut free = self.free_buffers.write().await;

        for _ in 0..self.config.preallocate {
            let allocation = self.allocator.allocate(self.buffer_size).await?;
            let buffer = PersistentBuffer::new(allocation);
            free.push(buffer);
        }

        Ok(())
    }

    /// Acquires a buffer from the pool
    pub async fn acquire(&self) -> Result<u64> {
        let mut free = self.free_buffers.write().await;

        let buffer = if let Some(buffer) = free.pop() {
            self.stats.pool_hits.fetch_add(1, Ordering::Relaxed);
            buffer
        } else {
            self.stats.pool_misses.fetch_add(1, Ordering::Relaxed);

            // Allocate new buffer
            let allocation = self.allocator.allocate(self.buffer_size).await?;
            PersistentBuffer::new(allocation)
        };

        let id = buffer.allocation.id;
        drop(free);

        let mut active = self.active_buffers.write().await;
        active.insert(id, buffer);

        self.stats.allocations.fetch_add(1, Ordering::Relaxed);

        Ok(id)
    }

    /// Releases a buffer back to the pool
    pub async fn release(&self, buffer_id: u64) -> Result<()> {
        let mut active = self.active_buffers.write().await;
        let mut buffer = active.remove(&buffer_id).ok_or_else(|| {
            StrataError::NotFound(format!("Buffer {} not found", buffer_id))
        })?;

        buffer.reset();
        drop(active);

        let mut free = self.free_buffers.write().await;
        if free.len() < self.config.max_buffers {
            free.push(buffer);
        } else {
            // Pool is full, deallocate
            self.allocator.deallocate(buffer.allocation.id).await?;
        }

        self.stats.deallocations.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Gets a buffer by ID
    pub async fn get(&self, buffer_id: u64) -> Option<()> {
        let active = self.active_buffers.read().await;
        if active.contains_key(&buffer_id) {
            Some(())
        } else {
            None
        }
    }

    /// Writes to a buffer
    pub async fn write(&self, buffer_id: u64, data: &[u8]) -> Result<usize> {
        let mut active = self.active_buffers.write().await;
        let buffer = active.get_mut(&buffer_id).ok_or_else(|| {
            StrataError::NotFound(format!("Buffer {} not found", buffer_id))
        })?;
        buffer.write(data)
    }

    /// Reads from a buffer
    pub async fn read(&self, buffer_id: u64, buf: &mut [u8]) -> Result<usize> {
        let mut active = self.active_buffers.write().await;
        let buffer = active.get_mut(&buffer_id).ok_or_else(|| {
            StrataError::NotFound(format!("Buffer {} not found", buffer_id))
        })?;
        buffer.read(buf)
    }

    /// Flushes a buffer to PMEM
    pub async fn flush(&self, buffer_id: u64, device: &mut PmemDevice) -> Result<()> {
        let mut active = self.active_buffers.write().await;
        let buffer = active.get_mut(&buffer_id).ok_or_else(|| {
            StrataError::NotFound(format!("Buffer {} not found", buffer_id))
        })?;
        buffer.flush(device)?;
        self.stats.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Gets pool statistics
    pub fn stats(&self) -> BufferPoolStatsSnapshot {
        BufferPoolStatsSnapshot {
            allocations: self.stats.allocations.load(Ordering::Relaxed),
            deallocations: self.stats.deallocations.load(Ordering::Relaxed),
            pool_hits: self.stats.pool_hits.load(Ordering::Relaxed),
            pool_misses: self.stats.pool_misses.load(Ordering::Relaxed),
            flushes: self.stats.flushes.load(Ordering::Relaxed),
        }
    }

    /// Gets the number of free buffers
    pub async fn free_count(&self) -> usize {
        self.free_buffers.read().await.len()
    }

    /// Gets the number of active buffers
    pub async fn active_count(&self) -> usize {
        self.active_buffers.read().await.len()
    }
}

/// Buffer pool statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferPoolStatsSnapshot {
    pub allocations: u64,
    pub deallocations: u64,
    pub pool_hits: u64,
    pub pool_misses: u64,
    pub flushes: u64,
}

impl BufferPoolStatsSnapshot {
    /// Gets hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let total = self.pool_hits + self.pool_misses;
        if total == 0 {
            0.0
        } else {
            self.pool_hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pmem::allocator::AllocatorConfig;

    #[test]
    fn test_persistent_buffer() {
        let allocation = PmemAllocation {
            id: 1,
            offset: 0,
            size: 1024,
            alignment: 64,
            created_at: 0,
            tags: HashMap::new(),
        };

        let mut buffer = PersistentBuffer::new(allocation);
        assert!(buffer.is_empty());

        let data = b"Hello PMEM!";
        let written = buffer.write(data).unwrap();
        assert_eq!(written, data.len());
        assert_eq!(buffer.len(), data.len());
        assert!(buffer.is_dirty());

        buffer.seek(0).unwrap();
        let mut read_buf = [0u8; 11];
        let read = buffer.read(&mut read_buf).unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&read_buf, data);
    }

    #[tokio::test]
    async fn test_buffer_pool() {
        let allocator = Arc::new(PmemAllocator::new(
            10 * 1024 * 1024,
            AllocatorConfig::default(),
        ));

        let config = BufferPoolConfig {
            preallocate: 4,
            max_buffers: 10,
            buffer_size: 4096,
            ..Default::default()
        };

        let pool = BufferPool::new(allocator, config).await.unwrap();
        assert_eq!(pool.free_count().await, 4);

        let buf_id = pool.acquire().await.unwrap();
        assert_eq!(pool.active_count().await, 1);

        pool.write(buf_id, b"test data").await.unwrap();

        pool.release(buf_id).await.unwrap();
        assert_eq!(pool.active_count().await, 0);

        let stats = pool.stats();
        assert_eq!(stats.allocations, 1);
        assert_eq!(stats.deallocations, 1);
        assert_eq!(stats.pool_hits, 1); // Pre-allocated buffer was used
    }

    #[test]
    fn test_checksum() {
        let data1 = b"Hello World!";
        let data2 = b"Hello World!";
        let data3 = b"Hello World?";

        assert_eq!(calculate_checksum(data1), calculate_checksum(data2));
        assert_ne!(calculate_checksum(data1), calculate_checksum(data3));
    }
}
