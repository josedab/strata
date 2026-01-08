// CXL (Compute Express Link) Support

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// CXL device type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CxlDeviceType {
    /// Type 1: Accelerator (coherent cache only)
    Type1,
    /// Type 2: Accelerator with HDM (coherent cache + host-managed device memory)
    Type2,
    /// Type 3: Memory expander (HDM only)
    Type3,
}

/// Memory type for CXL devices
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryType {
    /// Volatile memory
    Volatile,
    /// Persistent memory
    Persistent,
    /// Volatile with battery backup
    VolatileWithBattery,
}

/// CXL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CxlConfig {
    /// Device path
    pub device_path: PathBuf,
    /// Enable CXL.cache
    pub cache_enabled: bool,
    /// Enable CXL.mem
    pub mem_enabled: bool,
    /// Memory interleave granularity
    pub interleave_granularity: u32,
    /// NUMA affinity
    pub numa_node: Option<u32>,
    /// Memory tier (for tiered memory)
    pub memory_tier: u8,
    /// Enable hardware prefetch
    pub hw_prefetch: bool,
    /// Enable write combining
    pub write_combining: bool,
}

impl Default for CxlConfig {
    fn default() -> Self {
        Self {
            device_path: PathBuf::from("/dev/cxl/mem0"),
            cache_enabled: true,
            mem_enabled: true,
            interleave_granularity: 256,
            numa_node: None,
            memory_tier: 1,
            hw_prefetch: true,
            write_combining: true,
        }
    }
}

/// CXL device information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CxlDeviceInfo {
    /// Device ID
    pub id: String,
    /// Device name
    pub name: String,
    /// Device type
    pub device_type: CxlDeviceType,
    /// Memory type
    pub memory_type: MemoryType,
    /// Total capacity
    pub capacity: u64,
    /// Available capacity
    pub available: u64,
    /// NUMA node
    pub numa_node: u32,
    /// Link speed (GT/s)
    pub link_speed: f32,
    /// Link width
    pub link_width: u8,
    /// Firmware version
    pub firmware_version: Option<String>,
    /// Serial number
    pub serial_number: Option<String>,
    /// Vendor
    pub vendor: Option<String>,
    /// Device health
    pub health: CxlHealth,
}

/// CXL device health
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CxlHealth {
    /// Overall health status
    pub status: HealthStatus,
    /// Temperature in Celsius
    pub temperature: Option<f32>,
    /// Memory error count
    pub memory_errors: u64,
    /// Link errors
    pub link_errors: u64,
    /// Predicted lifetime remaining
    pub lifetime_remaining: Option<u8>,
}

/// Health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
    Failed,
    Unknown,
}

/// CXL device abstraction
pub struct CxlDevice {
    /// Configuration
    config: CxlConfig,
    /// Device info
    info: CxlDeviceInfo,
    /// Mapped memory
    mapped_memory: Option<MappedMemory>,
    /// Statistics
    stats: Arc<CxlStats>,
    /// Active regions
    regions: Arc<RwLock<HashMap<String, CxlRegion>>>,
}

/// Mapped CXL memory
struct MappedMemory {
    addr: *mut u8,
    len: usize,
}

// SAFETY: CxlDevice can be sent between threads because:
// 1. The raw pointer `addr` in MappedMemory points to memory allocated by the global allocator
//    which is thread-safe for send operations.
// 2. All access to the mapped memory is through methods that use &self for reads and ensure
//    external synchronization (via Arc<RwLock<CxlDevice>>) for writes.
// 3. The Arc<RwLock<_>> fields are inherently Send.
unsafe impl Send for CxlDevice {}

// SAFETY: CxlDevice can be shared between threads because:
// 1. The config and info fields are immutable after construction.
// 2. MappedMemory access for write() and read() methods use &self, and the caller is
//    expected to use external synchronization (Arc<RwLock<CxlDevice>>) for concurrent access.
// 3. Stats and regions fields use Arc<AtomicU64> and Arc<RwLock<_>> which are Sync.
unsafe impl Sync for CxlDevice {}

/// CXL memory region
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CxlRegion {
    /// Region ID
    pub id: String,
    /// Region name
    pub name: String,
    /// Start offset
    pub offset: u64,
    /// Size
    pub size: u64,
    /// Interleave set
    pub interleave_set: Option<u32>,
    /// Interleave ways
    pub interleave_ways: u8,
    /// Access mode
    pub access_mode: AccessMode,
}

/// Access mode for CXL memory
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    /// Normal cached access
    Cached,
    /// Uncached access
    Uncached,
    /// Write-combining
    WriteCombining,
    /// Write-through
    WriteThrough,
}

/// CXL statistics
pub struct CxlStats {
    /// Bytes read
    pub bytes_read: AtomicU64,
    /// Bytes written
    pub bytes_written: AtomicU64,
    /// Read operations
    pub reads: AtomicU64,
    /// Write operations
    pub writes: AtomicU64,
    /// Cache hits
    pub cache_hits: AtomicU64,
    /// Cache misses
    pub cache_misses: AtomicU64,
    /// Memory errors corrected
    pub errors_corrected: AtomicU64,
}

impl Default for CxlStats {
    fn default() -> Self {
        Self {
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            errors_corrected: AtomicU64::new(0),
        }
    }
}

impl CxlDevice {
    /// Creates a new CXL device
    pub fn new(config: CxlConfig) -> Result<Self> {
        // Probe device info
        let info = Self::probe_device(&config)?;

        Ok(Self {
            config,
            info,
            mapped_memory: None,
            stats: Arc::new(CxlStats::default()),
            regions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Creates an emulated CXL device for testing
    pub fn emulated(capacity: u64) -> Result<Self> {
        let config = CxlConfig::default();
        let info = CxlDeviceInfo {
            id: "emulated-cxl-0".to_string(),
            name: "Emulated CXL Memory Expander".to_string(),
            device_type: CxlDeviceType::Type3,
            memory_type: MemoryType::Volatile,
            capacity,
            available: capacity,
            numa_node: 1,
            link_speed: 32.0, // PCIe 5.0
            link_width: 16,
            firmware_version: Some("emulated".to_string()),
            serial_number: None,
            vendor: Some("Strata Emulator".to_string()),
            health: CxlHealth {
                status: HealthStatus::Healthy,
                temperature: Some(35.0),
                memory_errors: 0,
                link_errors: 0,
                lifetime_remaining: Some(100),
            },
        };

        Ok(Self {
            config,
            info,
            mapped_memory: None,
            stats: Arc::new(CxlStats::default()),
            regions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Probes device information
    fn probe_device(config: &CxlConfig) -> Result<CxlDeviceInfo> {
        // In real implementation, would read from sysfs/CXL subsystem
        // For now, return emulated info
        Ok(CxlDeviceInfo {
            id: "cxl-0".to_string(),
            name: "CXL Memory Expander".to_string(),
            device_type: CxlDeviceType::Type3,
            memory_type: MemoryType::Volatile,
            capacity: 0,
            available: 0,
            numa_node: config.numa_node.unwrap_or(0),
            link_speed: 32.0,
            link_width: 16,
            firmware_version: None,
            serial_number: None,
            vendor: None,
            health: CxlHealth {
                status: HealthStatus::Unknown,
                temperature: None,
                memory_errors: 0,
                link_errors: 0,
                lifetime_remaining: None,
            },
        })
    }

    /// Opens and maps the device
    pub fn open(&mut self) -> Result<()> {
        if self.mapped_memory.is_some() {
            return Err(StrataError::InvalidOperation(
                "Device already opened".to_string(),
            ));
        }

        let len = self.info.capacity as usize;
        if len == 0 {
            return Err(StrataError::InvalidOperation(
                "Device has no capacity".to_string(),
            ));
        }

        // Allocate memory (simulation)
        let layout = std::alloc::Layout::from_size_align(len, 4096)
            .map_err(|e| StrataError::Internal(e.to_string()))?;

        // SAFETY: Layout is valid (checked above via from_size_align which returns Err on invalid).
        // The allocated memory is zeroed and will be deallocated in close() or Drop.
        let addr = unsafe { std::alloc::alloc_zeroed(layout) };
        if addr.is_null() {
            return Err(StrataError::ResourceExhausted(
                "Failed to allocate CXL simulation".to_string(),
            ));
        }

        self.mapped_memory = Some(MappedMemory { addr, len });

        Ok(())
    }

    /// Closes the device
    pub fn close(&mut self) -> Result<()> {
        if let Some(mem) = self.mapped_memory.take() {
            let layout = std::alloc::Layout::from_size_align(mem.len, 4096)
                .map_err(|e| StrataError::Internal(e.to_string()))?;
            // SAFETY: The pointer `mem.addr` was allocated by alloc_zeroed in open()
            // with the same layout (size and alignment). We take() the memory ensuring
            // this deallocation happens exactly once.
            unsafe {
                std::alloc::dealloc(mem.addr, layout);
            }
        }
        Ok(())
    }

    /// Creates a memory region
    pub async fn create_region(
        &self,
        name: impl Into<String>,
        offset: u64,
        size: u64,
        access_mode: AccessMode,
    ) -> Result<CxlRegion> {
        let name = name.into();

        // Validate bounds
        if offset + size > self.info.capacity {
            return Err(StrataError::InvalidOperation(
                "Region exceeds device capacity".to_string(),
            ));
        }

        let region = CxlRegion {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.clone(),
            offset,
            size,
            interleave_set: None,
            interleave_ways: 1,
            access_mode,
        };

        let mut regions = self.regions.write().await;
        regions.insert(region.id.clone(), region.clone());

        Ok(region)
    }

    /// Removes a memory region
    pub async fn remove_region(&self, region_id: &str) -> Result<()> {
        let mut regions = self.regions.write().await;
        regions.remove(region_id).ok_or_else(|| {
            StrataError::NotFound(format!("Region {} not found", region_id))
        })?;
        Ok(())
    }

    /// Writes data to a region
    pub fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        let mem = self.mapped_memory.as_ref().ok_or_else(|| {
            StrataError::InvalidOperation("Device not opened".to_string())
        })?;

        let offset = offset as usize;
        if offset + data.len() > mem.len {
            return Err(StrataError::InvalidOperation(
                "Write exceeds device capacity".to_string(),
            ));
        }

        // SAFETY: The bounds check above ensures offset + data.len() <= mem.len.
        // The source (data) and destination (mem.addr + offset) do not overlap
        // because data is a separate slice and mem is our allocated buffer.
        // Both pointers are valid and properly aligned for u8.
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), mem.addr.add(offset), data.len());
        }

        self.stats.bytes_written.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.stats.writes.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Reads data from the device
    pub fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let mem = self.mapped_memory.as_ref().ok_or_else(|| {
            StrataError::InvalidOperation("Device not opened".to_string())
        })?;

        let offset = offset as usize;
        if offset + len > mem.len {
            return Err(StrataError::InvalidOperation(
                "Read exceeds device capacity".to_string(),
            ));
        }

        let mut data = vec![0u8; len];
        // SAFETY: The bounds check above ensures offset + len <= mem.len.
        // The source (mem.addr + offset) and destination (data) do not overlap
        // because data is a freshly allocated Vec. Both pointers are valid and
        // properly aligned for u8.
        unsafe {
            std::ptr::copy_nonoverlapping(mem.addr.add(offset), data.as_mut_ptr(), len);
        }

        self.stats.bytes_read.fetch_add(len as u64, Ordering::Relaxed);
        self.stats.reads.fetch_add(1, Ordering::Relaxed);

        Ok(data)
    }

    /// Gets device information
    pub fn info(&self) -> &CxlDeviceInfo {
        &self.info
    }

    /// Gets device statistics
    pub fn stats(&self) -> CxlStatsSnapshot {
        CxlStatsSnapshot {
            bytes_read: self.stats.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.stats.bytes_written.load(Ordering::Relaxed),
            reads: self.stats.reads.load(Ordering::Relaxed),
            writes: self.stats.writes.load(Ordering::Relaxed),
            cache_hits: self.stats.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.stats.cache_misses.load(Ordering::Relaxed),
            errors_corrected: self.stats.errors_corrected.load(Ordering::Relaxed),
        }
    }

    /// Lists all regions
    pub async fn list_regions(&self) -> Vec<CxlRegion> {
        let regions = self.regions.read().await;
        regions.values().cloned().collect()
    }

    /// Gets configuration
    pub fn config(&self) -> &CxlConfig {
        &self.config
    }

    /// Checks if device is open
    pub fn is_open(&self) -> bool {
        self.mapped_memory.is_some()
    }
}

impl Drop for CxlDevice {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CxlStatsSnapshot {
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub reads: u64,
    pub writes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub errors_corrected: u64,
}

/// CXL memory tiering manager
pub struct CxlTieringManager {
    /// CXL devices by tier
    devices: Arc<RwLock<HashMap<u8, Vec<Arc<RwLock<CxlDevice>>>>>>,
    /// Tier policies
    policies: Arc<RwLock<HashMap<u8, TierPolicy>>>,
}

/// Tier policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierPolicy {
    /// Tier level
    pub tier: u8,
    /// Promotion threshold (access count)
    pub promote_threshold: u64,
    /// Demotion threshold (idle time in ms)
    pub demote_threshold: u64,
    /// Target utilization
    pub target_utilization: f64,
}

impl CxlTieringManager {
    /// Creates a new tiering manager
    pub fn new() -> Self {
        Self {
            devices: Arc::new(RwLock::new(HashMap::new())),
            policies: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Adds a device to a tier
    pub async fn add_device(&self, tier: u8, device: Arc<RwLock<CxlDevice>>) {
        let mut devices = self.devices.write().await;
        devices.entry(tier).or_default().push(device);
    }

    /// Sets a tier policy
    pub async fn set_policy(&self, policy: TierPolicy) {
        let mut policies = self.policies.write().await;
        policies.insert(policy.tier, policy);
    }

    /// Gets total capacity for a tier
    pub async fn tier_capacity(&self, tier: u8) -> u64 {
        let devices = self.devices.read().await;
        if let Some(tier_devices) = devices.get(&tier) {
            let mut total = 0u64;
            for device in tier_devices {
                let dev = device.read().await;
                total += dev.info().capacity;
            }
            total
        } else {
            0
        }
    }
}

impl Default for CxlTieringManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cxl_device() {
        let mut device = CxlDevice::emulated(1024 * 1024).unwrap();
        device.open().unwrap();

        // Write and read
        let data = b"Hello CXL!";
        device.write(0, data).unwrap();

        let read = device.read(0, data.len()).unwrap();
        assert_eq!(&read, data);

        let stats = device.stats();
        assert_eq!(stats.writes, 1);
        assert_eq!(stats.reads, 1);
    }

    #[tokio::test]
    async fn test_cxl_regions() {
        let device = CxlDevice::emulated(10 * 1024 * 1024).unwrap();

        let region = device.create_region("data", 0, 1024 * 1024, AccessMode::Cached).await.unwrap();
        assert_eq!(region.size, 1024 * 1024);

        let regions = device.list_regions().await;
        assert_eq!(regions.len(), 1);

        device.remove_region(&region.id).await.unwrap();
        assert!(device.list_regions().await.is_empty());
    }

    #[tokio::test]
    async fn test_tiering_manager() {
        let manager = CxlTieringManager::new();

        let device = Arc::new(RwLock::new(CxlDevice::emulated(1024 * 1024).unwrap()));
        manager.add_device(1, device).await;

        let policy = TierPolicy {
            tier: 1,
            promote_threshold: 100,
            demote_threshold: 60000,
            target_utilization: 0.8,
        };
        manager.set_policy(policy).await;

        let capacity = manager.tier_capacity(1).await;
        assert_eq!(capacity, 1024 * 1024);
    }
}
