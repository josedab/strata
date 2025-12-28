// PMEM Allocator

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Allocator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocatorConfig {
    /// Minimum allocation size
    pub min_alloc_size: usize,
    /// Maximum allocation size
    pub max_alloc_size: usize,
    /// Alignment requirement
    pub alignment: usize,
    /// Enable coalescing of free blocks
    pub coalesce_free: bool,
    /// Enable defragmentation
    pub defragment: bool,
    /// Reserve percentage for metadata
    pub metadata_reserve_pct: f32,
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            min_alloc_size: 64,
            max_alloc_size: 1024 * 1024 * 1024, // 1GB
            alignment: 64,                       // Cache line alignment
            coalesce_free: true,
            defragment: true,
            metadata_reserve_pct: 1.0,
        }
    }
}

/// Allocation handle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PmemAllocation {
    /// Allocation ID
    pub id: u64,
    /// Offset in the device
    pub offset: u64,
    /// Size in bytes
    pub size: u64,
    /// Alignment used
    pub alignment: usize,
    /// Allocation timestamp
    pub created_at: u64,
    /// Tags/metadata
    pub tags: HashMap<String, String>,
}

impl PmemAllocation {
    /// Gets the end offset
    pub fn end_offset(&self) -> u64 {
        self.offset + self.size
    }
}

/// Free block entry
#[derive(Debug, Clone)]
struct FreeBlock {
    offset: u64,
    size: u64,
}

/// PMEM allocator using buddy allocation
pub struct PmemAllocator {
    /// Configuration
    config: AllocatorConfig,
    /// Device capacity
    capacity: u64,
    /// Free blocks by size (sorted)
    free_by_size: Arc<RwLock<BTreeMap<u64, Vec<u64>>>>,
    /// Free blocks by offset
    free_by_offset: Arc<RwLock<BTreeMap<u64, u64>>>,
    /// Active allocations
    allocations: Arc<RwLock<HashMap<u64, PmemAllocation>>>,
    /// Next allocation ID
    next_id: AtomicU64,
    /// Statistics
    stats: Arc<AllocatorStats>,
}

/// Allocator statistics
pub struct AllocatorStats {
    /// Total allocations
    pub total_allocations: AtomicU64,
    /// Total deallocations
    pub total_deallocations: AtomicU64,
    /// Bytes allocated
    pub bytes_allocated: AtomicU64,
    /// Bytes freed
    pub bytes_freed: AtomicU64,
    /// Current allocation count
    pub current_allocations: AtomicU64,
    /// Current bytes in use
    pub current_bytes_used: AtomicU64,
    /// Fragmentation count
    pub fragment_count: AtomicU64,
    /// Coalesce operations
    pub coalesce_ops: AtomicU64,
}

impl Default for AllocatorStats {
    fn default() -> Self {
        Self {
            total_allocations: AtomicU64::new(0),
            total_deallocations: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
            bytes_freed: AtomicU64::new(0),
            current_allocations: AtomicU64::new(0),
            current_bytes_used: AtomicU64::new(0),
            fragment_count: AtomicU64::new(0),
            coalesce_ops: AtomicU64::new(0),
        }
    }
}

impl PmemAllocator {
    /// Creates a new allocator for a device
    pub fn new(capacity: u64, config: AllocatorConfig) -> Self {
        let metadata_reserve = (capacity as f64 * config.metadata_reserve_pct as f64 / 100.0) as u64;
        let usable = capacity - metadata_reserve;

        let mut free_by_size = BTreeMap::new();
        free_by_size.insert(usable, vec![metadata_reserve]);

        let mut free_by_offset = BTreeMap::new();
        free_by_offset.insert(metadata_reserve, usable);

        Self {
            config,
            capacity,
            free_by_size: Arc::new(RwLock::new(free_by_size)),
            free_by_offset: Arc::new(RwLock::new(free_by_offset)),
            allocations: Arc::new(RwLock::new(HashMap::new())),
            next_id: AtomicU64::new(1),
            stats: Arc::new(AllocatorStats::default()),
        }
    }

    /// Allocates memory
    pub async fn allocate(&self, size: u64) -> Result<PmemAllocation> {
        self.allocate_aligned(size, self.config.alignment).await
    }

    /// Allocates aligned memory
    pub async fn allocate_aligned(&self, size: u64, alignment: usize) -> Result<PmemAllocation> {
        let size = size.max(self.config.min_alloc_size as u64);

        if size > self.config.max_alloc_size as u64 {
            return Err(StrataError::InvalidOperation(format!(
                "Allocation size {} exceeds maximum {}",
                size, self.config.max_alloc_size
            )));
        }

        // Align size
        let aligned_size = align_up(size, alignment);

        // Find best fit
        let mut free_by_size = self.free_by_size.write().await;
        let mut free_by_offset = self.free_by_offset.write().await;

        // Find smallest block that fits
        let block_size = {
            let mut found = None;
            for (&s, offsets) in free_by_size.range(aligned_size..) {
                if !offsets.is_empty() {
                    found = Some(s);
                    break;
                }
            }
            found.ok_or_else(|| {
                StrataError::ResourceExhausted("No free block available".to_string())
            })?
        };

        // Get the offset
        let offset = {
            let offsets = free_by_size.get_mut(&block_size).unwrap();
            let offset = offsets.pop().unwrap();
            if offsets.is_empty() {
                free_by_size.remove(&block_size);
            }
            offset
        };

        // Align offset
        let aligned_offset = align_up(offset, alignment);
        let alignment_waste = aligned_offset - offset;

        // Remove from offset map
        free_by_offset.remove(&offset);

        // Create free blocks for leftover space
        if alignment_waste > 0 {
            // Add back the alignment waste as a free block
            free_by_offset.insert(offset, alignment_waste);
            free_by_size.entry(alignment_waste).or_default().push(offset);
            self.stats.fragment_count.fetch_add(1, Ordering::Relaxed);
        }

        let remaining = block_size - alignment_waste - aligned_size;
        if remaining >= self.config.min_alloc_size as u64 {
            // Add remaining space as free block
            let remaining_offset = aligned_offset + aligned_size;
            free_by_offset.insert(remaining_offset, remaining);
            free_by_size.entry(remaining).or_default().push(remaining_offset);
            self.stats.fragment_count.fetch_add(1, Ordering::Relaxed);
        }

        // Create allocation
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let allocation = PmemAllocation {
            id,
            offset: aligned_offset,
            size: aligned_size,
            alignment,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            tags: HashMap::new(),
        };

        drop(free_by_size);
        drop(free_by_offset);

        // Record allocation
        let mut allocations = self.allocations.write().await;
        allocations.insert(id, allocation.clone());

        // Update stats
        self.stats.total_allocations.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_allocated.fetch_add(aligned_size, Ordering::Relaxed);
        self.stats.current_allocations.fetch_add(1, Ordering::Relaxed);
        self.stats.current_bytes_used.fetch_add(aligned_size, Ordering::Relaxed);

        Ok(allocation)
    }

    /// Deallocates memory
    pub async fn deallocate(&self, allocation_id: u64) -> Result<()> {
        let mut allocations = self.allocations.write().await;
        let allocation = allocations.remove(&allocation_id).ok_or_else(|| {
            StrataError::NotFound(format!("Allocation {} not found", allocation_id))
        })?;

        drop(allocations);

        // Add back to free list
        let mut free_by_size = self.free_by_size.write().await;
        let mut free_by_offset = self.free_by_offset.write().await;

        let mut offset = allocation.offset;
        let mut size = allocation.size;

        // Coalesce with previous block
        if self.config.coalesce_free {
            let prev_blocks: Vec<_> = free_by_offset.range(..offset).rev().take(1).map(|(&o, &s)| (o, s)).collect();
            for (prev_offset, prev_size) in prev_blocks {
                if prev_offset + prev_size == offset {
                    // Merge with previous
                    free_by_offset.remove(&prev_offset);
                    if let Some(offsets) = free_by_size.get_mut(&prev_size) {
                        offsets.retain(|&o| o != prev_offset);
                        if offsets.is_empty() {
                            free_by_size.remove(&prev_size);
                        }
                    }
                    offset = prev_offset;
                    size += prev_size;
                    self.stats.coalesce_ops.fetch_add(1, Ordering::Relaxed);
                }
            }

            // Coalesce with next block
            let end = offset + size;
            if let Some(&next_size) = free_by_offset.get(&end) {
                free_by_offset.remove(&end);
                if let Some(offsets) = free_by_size.get_mut(&next_size) {
                    offsets.retain(|&o| o != end);
                    if offsets.is_empty() {
                        free_by_size.remove(&next_size);
                    }
                }
                size += next_size;
                self.stats.coalesce_ops.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Add combined block
        free_by_offset.insert(offset, size);
        free_by_size.entry(size).or_default().push(offset);

        // Update stats
        self.stats.total_deallocations.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_freed.fetch_add(allocation.size, Ordering::Relaxed);
        self.stats.current_allocations.fetch_sub(1, Ordering::Relaxed);
        self.stats.current_bytes_used.fetch_sub(allocation.size, Ordering::Relaxed);

        Ok(())
    }

    /// Gets an allocation by ID
    pub async fn get_allocation(&self, allocation_id: u64) -> Option<PmemAllocation> {
        let allocations = self.allocations.read().await;
        allocations.get(&allocation_id).cloned()
    }

    /// Lists all allocations
    pub async fn list_allocations(&self) -> Vec<PmemAllocation> {
        let allocations = self.allocations.read().await;
        allocations.values().cloned().collect()
    }

    /// Gets allocator statistics
    pub fn stats(&self) -> AllocatorStatsSnapshot {
        AllocatorStatsSnapshot {
            total_allocations: self.stats.total_allocations.load(Ordering::Relaxed),
            total_deallocations: self.stats.total_deallocations.load(Ordering::Relaxed),
            bytes_allocated: self.stats.bytes_allocated.load(Ordering::Relaxed),
            bytes_freed: self.stats.bytes_freed.load(Ordering::Relaxed),
            current_allocations: self.stats.current_allocations.load(Ordering::Relaxed),
            current_bytes_used: self.stats.current_bytes_used.load(Ordering::Relaxed),
            capacity: self.capacity,
            fragment_count: self.stats.fragment_count.load(Ordering::Relaxed),
            coalesce_ops: self.stats.coalesce_ops.load(Ordering::Relaxed),
        }
    }

    /// Gets available space
    pub async fn available(&self) -> u64 {
        self.capacity - self.stats.current_bytes_used.load(Ordering::Relaxed)
    }

    /// Gets fragmentation ratio
    pub async fn fragmentation_ratio(&self) -> f64 {
        let free_by_offset = self.free_by_offset.read().await;
        let total_free = self.available().await;

        if total_free == 0 {
            return 0.0;
        }

        let largest_block = free_by_offset.values().max().copied().unwrap_or(0);
        1.0 - (largest_block as f64 / total_free as f64)
    }
}

/// Aligns a value up to the given alignment
fn align_up(value: u64, alignment: usize) -> u64 {
    let alignment = alignment as u64;
    (value + alignment - 1) & !(alignment - 1)
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocatorStatsSnapshot {
    pub total_allocations: u64,
    pub total_deallocations: u64,
    pub bytes_allocated: u64,
    pub bytes_freed: u64,
    pub current_allocations: u64,
    pub current_bytes_used: u64,
    pub capacity: u64,
    pub fragment_count: u64,
    pub coalesce_ops: u64,
}

impl AllocatorStatsSnapshot {
    /// Gets utilization percentage
    pub fn utilization(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.current_bytes_used as f64 / self.capacity as f64 * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_allocator_basic() {
        let allocator = PmemAllocator::new(1024 * 1024, AllocatorConfig::default());

        let alloc1 = allocator.allocate(1000).await.unwrap();
        assert!(alloc1.size >= 1000);

        let alloc2 = allocator.allocate(2000).await.unwrap();
        assert!(alloc2.size >= 2000);
        assert!(alloc2.offset > alloc1.offset);

        let stats = allocator.stats();
        assert_eq!(stats.current_allocations, 2);
    }

    #[tokio::test]
    async fn test_allocator_deallocate() {
        let allocator = PmemAllocator::new(1024 * 1024, AllocatorConfig::default());

        let alloc = allocator.allocate(1000).await.unwrap();
        allocator.deallocate(alloc.id).await.unwrap();

        let stats = allocator.stats();
        assert_eq!(stats.current_allocations, 0);
        assert_eq!(stats.total_deallocations, 1);
    }

    #[tokio::test]
    async fn test_allocator_coalesce() {
        let config = AllocatorConfig {
            coalesce_free: true,
            ..Default::default()
        };
        let allocator = PmemAllocator::new(1024 * 1024, config);

        let a1 = allocator.allocate(1000).await.unwrap();
        let a2 = allocator.allocate(1000).await.unwrap();
        let a3 = allocator.allocate(1000).await.unwrap();

        // Free middle one
        allocator.deallocate(a2.id).await.unwrap();
        // Free adjacent ones
        allocator.deallocate(a1.id).await.unwrap();
        allocator.deallocate(a3.id).await.unwrap();

        let stats = allocator.stats();
        // Should have coalesced
        assert!(stats.coalesce_ops > 0);
    }

    #[tokio::test]
    async fn test_allocator_alignment() {
        let allocator = PmemAllocator::new(1024 * 1024, AllocatorConfig::default());

        let alloc = allocator.allocate_aligned(100, 256).await.unwrap();
        assert_eq!(alloc.offset % 256, 0);
        assert_eq!(alloc.alignment, 256);
    }

    #[test]
    fn test_align_up() {
        assert_eq!(align_up(0, 64), 0);
        assert_eq!(align_up(1, 64), 64);
        assert_eq!(align_up(63, 64), 64);
        assert_eq!(align_up(64, 64), 64);
        assert_eq!(align_up(65, 64), 128);
    }
}
