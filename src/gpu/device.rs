// GPU Device Management

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// GPU vendor
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GpuVendor {
    Nvidia,
    Amd,
    Intel,
    Apple,
    Unknown,
}

/// GPU compute API
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ComputeApi {
    /// NVIDIA CUDA
    Cuda,
    /// OpenCL
    OpenCl,
    /// Vulkan Compute
    Vulkan,
    /// AMD ROCm
    Rocm,
    /// Intel oneAPI
    OneApi,
    /// Apple Metal
    Metal,
    /// Simulated (for testing)
    Simulated,
}

/// GPU device information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuDeviceInfo {
    /// Device ID
    pub id: String,
    /// Device name
    pub name: String,
    /// Vendor
    pub vendor: GpuVendor,
    /// Compute API
    pub compute_api: ComputeApi,
    /// Total memory in bytes
    pub total_memory: u64,
    /// Free memory in bytes
    pub free_memory: u64,
    /// Compute capability (major.minor)
    pub compute_capability: (u32, u32),
    /// Number of multiprocessors
    pub multiprocessors: u32,
    /// Max threads per block
    pub max_threads_per_block: u32,
    /// Max block dimensions
    pub max_block_dims: (u32, u32, u32),
    /// Max grid dimensions
    pub max_grid_dims: (u32, u32, u32),
    /// Warp size
    pub warp_size: u32,
    /// Clock rate in MHz
    pub clock_rate_mhz: u32,
    /// Memory clock rate in MHz
    pub memory_clock_mhz: u32,
    /// Memory bus width in bits
    pub memory_bus_width: u32,
    /// PCIe bandwidth in GB/s
    pub pcie_bandwidth: f32,
    /// Temperature in Celsius
    pub temperature: Option<f32>,
    /// Power usage in watts
    pub power_usage: Option<f32>,
    /// Driver version
    pub driver_version: Option<String>,
}

/// GPU device abstraction
pub struct GpuDevice {
    /// Device info
    info: GpuDeviceInfo,
    /// Current context
    context: Option<GpuContext>,
    /// Statistics
    stats: Arc<GpuStats>,
    /// Memory allocations
    allocations: Arc<RwLock<HashMap<u64, GpuAllocation>>>,
    /// Next allocation ID
    next_alloc_id: AtomicU64,
}

/// GPU context
struct GpuContext {
    /// Context handle (in real impl, would be CUcontext, cl_context, etc.)
    _handle: u64,
    /// Active streams
    streams: Vec<GpuStream>,
}

/// GPU stream for async operations
struct GpuStream {
    _id: u64,
    _priority: i32,
}

/// GPU memory allocation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuAllocation {
    /// Allocation ID
    pub id: u64,
    /// Size in bytes
    pub size: u64,
    /// Device pointer (simulated)
    pub device_ptr: u64,
    /// Allocation type
    pub alloc_type: AllocationType,
    /// Created timestamp
    pub created_at: u64,
}

/// Allocation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationType {
    /// Regular device memory
    Device,
    /// Pinned host memory
    Pinned,
    /// Managed/unified memory
    Managed,
    /// Host-mapped memory
    HostMapped,
}

/// GPU statistics
pub struct GpuStats {
    /// Kernels launched
    pub kernels_launched: AtomicU64,
    /// Bytes transferred to device
    pub bytes_to_device: AtomicU64,
    /// Bytes transferred from device
    pub bytes_from_device: AtomicU64,
    /// Memory allocated
    pub memory_allocated: AtomicU64,
    /// Memory freed
    pub memory_freed: AtomicU64,
    /// Total compute time in microseconds
    pub compute_time_us: AtomicU64,
    /// Transfer time in microseconds
    pub transfer_time_us: AtomicU64,
}

impl Default for GpuStats {
    fn default() -> Self {
        Self {
            kernels_launched: AtomicU64::new(0),
            bytes_to_device: AtomicU64::new(0),
            bytes_from_device: AtomicU64::new(0),
            memory_allocated: AtomicU64::new(0),
            memory_freed: AtomicU64::new(0),
            compute_time_us: AtomicU64::new(0),
            transfer_time_us: AtomicU64::new(0),
        }
    }
}

impl GpuDevice {
    /// Creates a GPU device from device info
    pub fn new(info: GpuDeviceInfo) -> Self {
        Self {
            info,
            context: None,
            stats: Arc::new(GpuStats::default()),
            allocations: Arc::new(RwLock::new(HashMap::new())),
            next_alloc_id: AtomicU64::new(1),
        }
    }

    /// Creates a simulated GPU device for testing
    pub fn simulated() -> Self {
        let info = GpuDeviceInfo {
            id: "simulated-0".to_string(),
            name: "Simulated GPU".to_string(),
            vendor: GpuVendor::Unknown,
            compute_api: ComputeApi::Simulated,
            total_memory: 8 * 1024 * 1024 * 1024, // 8GB
            free_memory: 8 * 1024 * 1024 * 1024,
            compute_capability: (8, 0),
            multiprocessors: 80,
            max_threads_per_block: 1024,
            max_block_dims: (1024, 1024, 64),
            max_grid_dims: (2147483647, 65535, 65535),
            warp_size: 32,
            clock_rate_mhz: 1500,
            memory_clock_mhz: 9500,
            memory_bus_width: 256,
            pcie_bandwidth: 16.0,
            temperature: Some(45.0),
            power_usage: Some(150.0),
            driver_version: Some("simulated".to_string()),
        };

        Self::new(info)
    }

    /// Initializes the device context
    pub fn init(&mut self) -> Result<()> {
        if self.context.is_some() {
            return Err(StrataError::InvalidOperation(
                "Device already initialized".to_string(),
            ));
        }

        // Create default stream
        let streams = vec![GpuStream {
            _id: 0,
            _priority: 0,
        }];

        self.context = Some(GpuContext {
            _handle: rand::random(),
            streams,
        });

        Ok(())
    }

    /// Shuts down the device context
    pub fn shutdown(&mut self) -> Result<()> {
        self.context = None;
        Ok(())
    }

    /// Allocates device memory
    pub async fn allocate(&self, size: u64, alloc_type: AllocationType) -> Result<GpuAllocation> {
        if self.context.is_none() {
            return Err(StrataError::InvalidOperation(
                "Device not initialized".to_string(),
            ));
        }

        let id = self.next_alloc_id.fetch_add(1, Ordering::SeqCst);
        let allocation = GpuAllocation {
            id,
            size,
            device_ptr: rand::random(), // Simulated pointer
            alloc_type,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        let mut allocations = self.allocations.write().await;
        allocations.insert(id, allocation.clone());

        self.stats.memory_allocated.fetch_add(size, Ordering::Relaxed);

        Ok(allocation)
    }

    /// Frees device memory
    pub async fn free(&self, allocation_id: u64) -> Result<()> {
        let mut allocations = self.allocations.write().await;
        let allocation = allocations.remove(&allocation_id).ok_or_else(|| {
            StrataError::NotFound(format!("Allocation {} not found", allocation_id))
        })?;

        self.stats.memory_freed.fetch_add(allocation.size, Ordering::Relaxed);

        Ok(())
    }

    /// Copies data to device
    pub async fn copy_to_device(&self, data: &[u8], allocation: &GpuAllocation) -> Result<()> {
        if data.len() as u64 > allocation.size {
            return Err(StrataError::InvalidOperation(
                "Data exceeds allocation size".to_string(),
            ));
        }

        // Simulated copy
        self.stats.bytes_to_device.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.stats.transfer_time_us.fetch_add(
            (data.len() as u64 / 1_000_000).max(1), // ~1GB/s simulated
            Ordering::Relaxed,
        );

        Ok(())
    }

    /// Copies data from device
    pub async fn copy_from_device(&self, allocation: &GpuAllocation, len: usize) -> Result<Vec<u8>> {
        if len as u64 > allocation.size {
            return Err(StrataError::InvalidOperation(
                "Requested length exceeds allocation size".to_string(),
            ));
        }

        // Simulated copy - return zeros
        let data = vec![0u8; len];

        self.stats.bytes_from_device.fetch_add(len as u64, Ordering::Relaxed);
        self.stats.transfer_time_us.fetch_add(
            (len as u64 / 1_000_000).max(1),
            Ordering::Relaxed,
        );

        Ok(data)
    }

    /// Synchronizes the device
    pub async fn synchronize(&self) -> Result<()> {
        // In real implementation, would call cuCtxSynchronize or similar
        Ok(())
    }

    /// Gets device info
    pub fn info(&self) -> &GpuDeviceInfo {
        &self.info
    }

    /// Gets device statistics
    pub fn stats(&self) -> GpuStatsSnapshot {
        GpuStatsSnapshot {
            kernels_launched: self.stats.kernels_launched.load(Ordering::Relaxed),
            bytes_to_device: self.stats.bytes_to_device.load(Ordering::Relaxed),
            bytes_from_device: self.stats.bytes_from_device.load(Ordering::Relaxed),
            memory_allocated: self.stats.memory_allocated.load(Ordering::Relaxed),
            memory_freed: self.stats.memory_freed.load(Ordering::Relaxed),
            compute_time_us: self.stats.compute_time_us.load(Ordering::Relaxed),
            transfer_time_us: self.stats.transfer_time_us.load(Ordering::Relaxed),
        }
    }

    /// Checks if device is initialized
    pub fn is_initialized(&self) -> bool {
        self.context.is_some()
    }

    /// Gets current memory usage
    pub async fn memory_usage(&self) -> (u64, u64) {
        let allocated = self.stats.memory_allocated.load(Ordering::Relaxed);
        let freed = self.stats.memory_freed.load(Ordering::Relaxed);
        let in_use = allocated.saturating_sub(freed);
        (in_use, self.info.total_memory)
    }

    /// Records kernel execution time
    pub fn record_kernel(&self, time_us: u64) {
        self.stats.kernels_launched.fetch_add(1, Ordering::Relaxed);
        self.stats.compute_time_us.fetch_add(time_us, Ordering::Relaxed);
    }
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuStatsSnapshot {
    pub kernels_launched: u64,
    pub bytes_to_device: u64,
    pub bytes_from_device: u64,
    pub memory_allocated: u64,
    pub memory_freed: u64,
    pub compute_time_us: u64,
    pub transfer_time_us: u64,
}

impl GpuStatsSnapshot {
    /// Gets memory currently in use
    pub fn memory_in_use(&self) -> u64 {
        self.memory_allocated.saturating_sub(self.memory_freed)
    }

    /// Gets compute efficiency (compute time / total time)
    pub fn compute_efficiency(&self) -> f64 {
        let total = self.compute_time_us + self.transfer_time_us;
        if total == 0 {
            0.0
        } else {
            self.compute_time_us as f64 / total as f64
        }
    }
}

/// Device manager for discovering and managing GPUs
pub struct DeviceManager {
    /// Discovered devices
    devices: Arc<RwLock<HashMap<String, Arc<RwLock<GpuDevice>>>>>,
    /// Default device ID
    default_device: Arc<RwLock<Option<String>>>,
}

impl DeviceManager {
    /// Creates a new device manager
    pub fn new() -> Self {
        Self {
            devices: Arc::new(RwLock::new(HashMap::new())),
            default_device: Arc::new(RwLock::new(None)),
        }
    }

    /// Discovers available GPU devices
    pub async fn discover(&self) -> Result<Vec<GpuDeviceInfo>> {
        let mut devices = Vec::new();

        // In real implementation, would use CUDA/OpenCL/etc. to enumerate
        // For now, create a simulated device
        let simulated = GpuDevice::simulated();
        devices.push(simulated.info.clone());

        // Store the device
        let mut stored = self.devices.write().await;
        stored.insert(
            simulated.info.id.clone(),
            Arc::new(RwLock::new(simulated)),
        );

        // Set as default
        if let Some(first) = devices.first() {
            let mut default = self.default_device.write().await;
            *default = Some(first.id.clone());
        }

        Ok(devices)
    }

    /// Gets a device by ID
    pub async fn get_device(&self, device_id: &str) -> Option<Arc<RwLock<GpuDevice>>> {
        let devices = self.devices.read().await;
        devices.get(device_id).cloned()
    }

    /// Gets the default device
    pub async fn get_default(&self) -> Option<Arc<RwLock<GpuDevice>>> {
        let default = self.default_device.read().await;
        if let Some(ref id) = *default {
            self.get_device(id).await
        } else {
            None
        }
    }

    /// Sets the default device
    pub async fn set_default(&self, device_id: &str) -> Result<()> {
        let devices = self.devices.read().await;
        if !devices.contains_key(device_id) {
            return Err(StrataError::NotFound(format!(
                "Device {} not found",
                device_id
            )));
        }

        let mut default = self.default_device.write().await;
        *default = Some(device_id.to_string());
        Ok(())
    }

    /// Lists all device IDs
    pub async fn list_devices(&self) -> Vec<String> {
        let devices = self.devices.read().await;
        devices.keys().cloned().collect()
    }

    /// Gets device count
    pub async fn device_count(&self) -> usize {
        self.devices.read().await.len()
    }
}

impl Default for DeviceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_device() {
        let mut device = GpuDevice::simulated();
        device.init().unwrap();
        assert!(device.is_initialized());
        device.shutdown().unwrap();
        assert!(!device.is_initialized());
    }

    #[tokio::test]
    async fn test_gpu_allocation() {
        let mut device = GpuDevice::simulated();
        device.init().unwrap();

        let alloc = device.allocate(1024, AllocationType::Device).await.unwrap();
        assert_eq!(alloc.size, 1024);

        device.free(alloc.id).await.unwrap();

        let stats = device.stats();
        assert_eq!(stats.memory_allocated, 1024);
        assert_eq!(stats.memory_freed, 1024);
    }

    #[tokio::test]
    async fn test_device_manager() {
        let manager = DeviceManager::new();
        let devices = manager.discover().await.unwrap();

        assert!(!devices.is_empty());
        assert!(manager.get_default().await.is_some());
    }
}
