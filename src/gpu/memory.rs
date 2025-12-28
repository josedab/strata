// GPU Memory Management

use super::device::{AllocationType, GpuAllocation, GpuDevice};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Data transfer mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferMode {
    /// Synchronous transfer
    Sync,
    /// Asynchronous transfer
    Async,
    /// Pinned memory transfer
    Pinned,
    /// Zero-copy (mapped memory)
    ZeroCopy,
}

/// GPU buffer for data operations
pub struct GpuBuffer {
    /// Underlying allocation
    allocation: GpuAllocation,
    /// Buffer size
    size: usize,
    /// Current data length
    data_len: usize,
    /// Pinned host buffer (for async transfers)
    host_buffer: Option<Vec<u8>>,
    /// Transfer mode
    transfer_mode: TransferMode,
    /// Read position
    read_pos: usize,
    /// Write position
    write_pos: usize,
}

impl GpuBuffer {
    /// Creates a new GPU buffer
    pub async fn new(
        device: &GpuDevice,
        size: usize,
        transfer_mode: TransferMode,
    ) -> Result<Self> {
        let alloc_type = match transfer_mode {
            TransferMode::Sync => AllocationType::Device,
            TransferMode::Async => AllocationType::Device,
            TransferMode::Pinned => AllocationType::Pinned,
            TransferMode::ZeroCopy => AllocationType::HostMapped,
        };

        let allocation = device.allocate(size as u64, alloc_type).await?;

        let host_buffer = if matches!(transfer_mode, TransferMode::Pinned | TransferMode::ZeroCopy)
        {
            Some(vec![0u8; size])
        } else {
            None
        };

        Ok(Self {
            allocation,
            size,
            data_len: 0,
            host_buffer,
            transfer_mode,
            read_pos: 0,
            write_pos: 0,
        })
    }

    /// Creates a buffer with initial data
    pub async fn with_data(
        device: &GpuDevice,
        data: &[u8],
        transfer_mode: TransferMode,
    ) -> Result<Self> {
        let mut buffer = Self::new(device, data.len(), transfer_mode).await?;
        buffer.upload(device, data).await?;
        Ok(buffer)
    }

    /// Uploads data to the GPU
    pub async fn upload(&mut self, device: &GpuDevice, data: &[u8]) -> Result<()> {
        if data.len() > self.size {
            return Err(StrataError::InvalidOperation(
                "Data exceeds buffer size".to_string(),
            ));
        }

        if let Some(ref mut host_buf) = self.host_buffer {
            host_buf[..data.len()].copy_from_slice(data);
        }

        device.copy_to_device(data, &self.allocation).await?;
        self.data_len = data.len();
        self.write_pos = data.len();

        Ok(())
    }

    /// Downloads data from the GPU
    pub async fn download(&self, device: &GpuDevice) -> Result<Vec<u8>> {
        device.copy_from_device(&self.allocation, self.data_len).await
    }

    /// Gets the buffer size
    pub fn size(&self) -> usize {
        self.size
    }

    /// Gets the current data length
    pub fn len(&self) -> usize {
        self.data_len
    }

    /// Checks if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.data_len == 0
    }

    /// Gets the allocation
    pub fn allocation(&self) -> &GpuAllocation {
        &self.allocation
    }

    /// Gets the device pointer
    pub fn device_ptr(&self) -> u64 {
        self.allocation.device_ptr
    }

    /// Gets the transfer mode
    pub fn transfer_mode(&self) -> TransferMode {
        self.transfer_mode
    }

    /// Resets the buffer
    pub fn reset(&mut self) {
        self.data_len = 0;
        self.read_pos = 0;
        self.write_pos = 0;
    }
}

/// GPU memory pool for efficient allocation
pub struct GpuMemoryPool {
    /// Device reference
    device: Arc<RwLock<GpuDevice>>,
    /// Pool configuration
    config: PoolConfig,
    /// Free buffers by size class
    free_buffers: Arc<RwLock<Vec<VecDeque<GpuBuffer>>>>,
    /// Size classes
    size_classes: Vec<usize>,
    /// Statistics
    stats: Arc<PoolStats>,
}

/// Pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Size classes for buffers
    pub size_classes: Vec<usize>,
    /// Maximum buffers per size class
    pub max_per_class: usize,
    /// Pre-allocate buffers
    pub preallocate: bool,
    /// Default transfer mode
    pub transfer_mode: TransferMode,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            size_classes: vec![
                4 * 1024,       // 4KB
                16 * 1024,      // 16KB
                64 * 1024,      // 64KB
                256 * 1024,     // 256KB
                1024 * 1024,    // 1MB
                4 * 1024 * 1024, // 4MB
                16 * 1024 * 1024, // 16MB
            ],
            max_per_class: 16,
            preallocate: false,
            transfer_mode: TransferMode::Async,
        }
    }
}

/// Pool statistics
pub struct PoolStats {
    /// Allocations
    pub allocations: AtomicU64,
    /// Deallocations
    pub deallocations: AtomicU64,
    /// Pool hits
    pub pool_hits: AtomicU64,
    /// Pool misses
    pub pool_misses: AtomicU64,
    /// Bytes allocated
    pub bytes_allocated: AtomicU64,
}

impl Default for PoolStats {
    fn default() -> Self {
        Self {
            allocations: AtomicU64::new(0),
            deallocations: AtomicU64::new(0),
            pool_hits: AtomicU64::new(0),
            pool_misses: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
        }
    }
}

impl GpuMemoryPool {
    /// Creates a new memory pool
    pub fn new(device: Arc<RwLock<GpuDevice>>, config: PoolConfig) -> Self {
        let size_classes = config.size_classes.clone();
        let free_buffers = size_classes.iter().map(|_| VecDeque::new()).collect();

        Self {
            device,
            config,
            free_buffers: Arc::new(RwLock::new(free_buffers)),
            size_classes,
            stats: Arc::new(PoolStats::default()),
        }
    }

    /// Finds the size class for a given size
    fn find_size_class(&self, size: usize) -> Option<(usize, usize)> {
        for (i, &class_size) in self.size_classes.iter().enumerate() {
            if size <= class_size {
                return Some((i, class_size));
            }
        }
        None
    }

    /// Acquires a buffer from the pool
    pub async fn acquire(&self, size: usize) -> Result<GpuBuffer> {
        let (class_idx, class_size) = self.find_size_class(size).ok_or_else(|| {
            StrataError::InvalidOperation(format!(
                "Size {} exceeds maximum size class",
                size
            ))
        })?;

        // Try to get from pool
        {
            let mut free = self.free_buffers.write().await;
            if let Some(mut buffer) = free[class_idx].pop_front() {
                self.stats.pool_hits.fetch_add(1, Ordering::Relaxed);
                buffer.reset();
                return Ok(buffer);
            }
        }

        // Allocate new buffer
        self.stats.pool_misses.fetch_add(1, Ordering::Relaxed);
        self.stats.allocations.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_allocated.fetch_add(class_size as u64, Ordering::Relaxed);

        let device = self.device.read().await;
        GpuBuffer::new(&device, class_size, self.config.transfer_mode).await
    }

    /// Returns a buffer to the pool
    pub async fn release(&self, buffer: GpuBuffer) {
        let size = buffer.size();

        if let Some((class_idx, _)) = self.find_size_class(size) {
            let mut free = self.free_buffers.write().await;
            if free[class_idx].len() < self.config.max_per_class {
                free[class_idx].push_back(buffer);
                self.stats.deallocations.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        // Pool is full or oversized buffer - let it drop
        self.stats.deallocations.fetch_add(1, Ordering::Relaxed);
    }

    /// Gets pool statistics
    pub fn stats(&self) -> PoolStatsSnapshot {
        PoolStatsSnapshot {
            allocations: self.stats.allocations.load(Ordering::Relaxed),
            deallocations: self.stats.deallocations.load(Ordering::Relaxed),
            pool_hits: self.stats.pool_hits.load(Ordering::Relaxed),
            pool_misses: self.stats.pool_misses.load(Ordering::Relaxed),
            bytes_allocated: self.stats.bytes_allocated.load(Ordering::Relaxed),
        }
    }

    /// Gets free buffer counts per size class
    pub async fn free_counts(&self) -> Vec<(usize, usize)> {
        let free = self.free_buffers.read().await;
        self.size_classes
            .iter()
            .zip(free.iter())
            .map(|(&size, buffers)| (size, buffers.len()))
            .collect()
    }
}

/// Pool statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatsSnapshot {
    pub allocations: u64,
    pub deallocations: u64,
    pub pool_hits: u64,
    pub pool_misses: u64,
    pub bytes_allocated: u64,
}

impl PoolStatsSnapshot {
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

/// Staging buffer for CPU-GPU transfers
pub struct StagingBuffer {
    /// Pinned host memory
    data: Vec<u8>,
    /// Current offset
    offset: usize,
    /// Capacity
    capacity: usize,
}

impl StagingBuffer {
    /// Creates a new staging buffer
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity],
            offset: 0,
            capacity,
        }
    }

    /// Writes data to the staging buffer
    pub fn write(&mut self, data: &[u8]) -> Result<usize> {
        let available = self.capacity - self.offset;
        let to_write = data.len().min(available);

        if to_write == 0 {
            return Err(StrataError::BufferFull("GPU buffer full".to_string()));
        }

        self.data[self.offset..self.offset + to_write].copy_from_slice(&data[..to_write]);
        self.offset += to_write;

        Ok(to_write)
    }

    /// Gets the data
    pub fn data(&self) -> &[u8] {
        &self.data[..self.offset]
    }

    /// Gets remaining capacity
    pub fn remaining(&self) -> usize {
        self.capacity - self.offset
    }

    /// Resets the buffer
    pub fn reset(&mut self) {
        self.offset = 0;
    }

    /// Gets current size
    pub fn len(&self) -> usize {
        self.offset
    }

    /// Checks if empty
    pub fn is_empty(&self) -> bool {
        self.offset == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gpu_buffer() {
        let mut device = GpuDevice::simulated();
        device.init().unwrap();

        let mut buffer = GpuBuffer::new(&device, 1024, TransferMode::Sync).await.unwrap();
        assert_eq!(buffer.size(), 1024);
        assert!(buffer.is_empty());

        let data = b"Hello GPU!";
        buffer.upload(&device, data).await.unwrap();
        assert_eq!(buffer.len(), data.len());
    }

    #[tokio::test]
    async fn test_memory_pool() {
        let mut device = GpuDevice::simulated();
        device.init().unwrap();
        let device = Arc::new(RwLock::new(device));

        let pool = GpuMemoryPool::new(device, PoolConfig::default());

        let buf1 = pool.acquire(1000).await.unwrap();
        assert!(buf1.size() >= 1000);

        pool.release(buf1).await;

        let buf2 = pool.acquire(1000).await.unwrap();
        assert!(buf2.size() >= 1000);

        let stats = pool.stats();
        assert_eq!(stats.pool_hits, 1);
    }

    #[test]
    fn test_staging_buffer() {
        let mut staging = StagingBuffer::new(1024);

        staging.write(b"Hello").unwrap();
        staging.write(b" World").unwrap();

        assert_eq!(staging.len(), 11);
        assert_eq!(staging.data(), b"Hello World");

        staging.reset();
        assert!(staging.is_empty());
    }

    use super::super::device::GpuDevice;
}
