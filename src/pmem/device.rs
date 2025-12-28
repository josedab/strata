// PMEM Device Detection and Management

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Persistent memory device type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    /// Intel Optane PMem
    Optane,
    /// CXL Type 3 memory expander
    CxlType3,
    /// NVDIMM (older persistent memory)
    Nvdimm,
    /// Emulated PMEM (for testing)
    Emulated,
    /// Unknown device type
    Unknown,
}

/// Device mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceMode {
    /// App Direct mode (byte-addressable)
    AppDirect,
    /// Memory mode (as volatile memory extension)
    Memory,
    /// Mixed mode
    Mixed,
    /// Device-DAX mode
    DevDax,
    /// File system DAX (fsdax)
    FsDax,
}

/// Persistent memory device information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    /// Device ID
    pub id: String,
    /// Device name
    pub name: String,
    /// Device type
    pub device_type: DeviceType,
    /// Device mode
    pub mode: DeviceMode,
    /// Total capacity in bytes
    pub capacity: u64,
    /// Available capacity in bytes
    pub available: u64,
    /// Device path (e.g., /dev/pmem0)
    pub path: PathBuf,
    /// NUMA node
    pub numa_node: u32,
    /// Interleave set ID
    pub interleave_set: Option<u32>,
    /// Health status
    pub health: DeviceHealth,
    /// Firmware version
    pub firmware_version: Option<String>,
    /// Serial number
    pub serial_number: Option<String>,
    /// Temperature in Celsius
    pub temperature: Option<f32>,
    /// Lifetime remaining (percentage)
    pub lifetime_remaining: Option<u8>,
}

/// Device health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceHealth {
    /// Device is healthy
    Healthy,
    /// Device has warnings
    Warning,
    /// Device is critical
    Critical,
    /// Device has failed
    Failed,
    /// Health unknown
    Unknown,
}

/// PMEM device abstraction
pub struct PmemDevice {
    /// Device information
    info: DeviceInfo,
    /// Mapped memory region
    mapped_region: Option<MappedRegion>,
    /// Statistics
    stats: DeviceStats,
}

/// Mapped memory region
struct MappedRegion {
    /// Base address
    addr: *mut u8,
    /// Length
    len: usize,
    /// File descriptor (if any)
    _fd: Option<std::fs::File>,
}

// Safety: PmemDevice manages raw pointers but ensures safe access
unsafe impl Send for PmemDevice {}
unsafe impl Sync for PmemDevice {}

/// Device statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeviceStats {
    /// Bytes written
    pub bytes_written: u64,
    /// Bytes read
    pub bytes_read: u64,
    /// Write operations
    pub writes: u64,
    /// Read operations
    pub reads: u64,
    /// Flush operations
    pub flushes: u64,
    /// Memory fences
    pub fences: u64,
}

impl PmemDevice {
    /// Creates a new PMEM device from device info
    pub fn new(info: DeviceInfo) -> Self {
        Self {
            info,
            mapped_region: None,
            stats: DeviceStats::default(),
        }
    }

    /// Opens and maps the device
    pub fn open(&mut self) -> Result<()> {
        if self.mapped_region.is_some() {
            return Err(StrataError::InvalidOperation(
                "Device already opened".to_string(),
            ));
        }

        // In a real implementation, would use mmap with MAP_SYNC for pmem
        // For simulation, we allocate regular memory
        let len = self.info.capacity as usize;
        let layout = std::alloc::Layout::from_size_align(len, 4096)
            .map_err(|e| StrataError::Internal(e.to_string()))?;

        let addr = unsafe { std::alloc::alloc_zeroed(layout) };
        if addr.is_null() {
            return Err(StrataError::ResourceExhausted(
                "Failed to allocate PMEM simulation".to_string(),
            ));
        }

        self.mapped_region = Some(MappedRegion {
            addr,
            len,
            _fd: None,
        });

        Ok(())
    }

    /// Closes and unmaps the device
    pub fn close(&mut self) -> Result<()> {
        if let Some(region) = self.mapped_region.take() {
            let layout = std::alloc::Layout::from_size_align(region.len, 4096)
                .map_err(|e| StrataError::Internal(e.to_string()))?;
            unsafe {
                std::alloc::dealloc(region.addr, layout);
            }
        }
        Ok(())
    }

    /// Writes data to a specific offset
    pub fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        let region = self.mapped_region.as_ref().ok_or_else(|| {
            StrataError::InvalidOperation("Device not opened".to_string())
        })?;

        let offset = offset as usize;
        if offset + data.len() > region.len {
            return Err(StrataError::InvalidOperation(
                "Write exceeds device capacity".to_string(),
            ));
        }

        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), region.addr.add(offset), data.len());
        }

        self.stats.bytes_written += data.len() as u64;
        self.stats.writes += 1;

        Ok(())
    }

    /// Reads data from a specific offset
    pub fn read(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let region = self.mapped_region.as_ref().ok_or_else(|| {
            StrataError::InvalidOperation("Device not opened".to_string())
        })?;

        let offset = offset as usize;
        if offset + len > region.len {
            return Err(StrataError::InvalidOperation(
                "Read exceeds device capacity".to_string(),
            ));
        }

        let mut data = vec![0u8; len];
        unsafe {
            std::ptr::copy_nonoverlapping(region.addr.add(offset), data.as_mut_ptr(), len);
        }

        self.stats.bytes_read += len as u64;
        self.stats.reads += 1;

        Ok(data)
    }

    /// Flushes data to persistent storage
    pub fn flush(&mut self, offset: u64, len: usize) -> Result<()> {
        let region = self.mapped_region.as_ref().ok_or_else(|| {
            StrataError::InvalidOperation("Device not opened".to_string())
        })?;

        let offset = offset as usize;
        if offset + len > region.len {
            return Err(StrataError::InvalidOperation(
                "Flush range exceeds device capacity".to_string(),
            ));
        }

        // In real implementation, would use clflush/clwb instructions
        // or pmem_persist() from PMDK
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

        self.stats.flushes += 1;

        Ok(())
    }

    /// Issues a memory fence
    pub fn fence(&mut self) {
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        self.stats.fences += 1;
    }

    /// Gets device information
    pub fn info(&self) -> &DeviceInfo {
        &self.info
    }

    /// Gets device statistics
    pub fn stats(&self) -> &DeviceStats {
        &self.stats
    }

    /// Checks if device is opened
    pub fn is_open(&self) -> bool {
        self.mapped_region.is_some()
    }

    /// Gets the base address (for advanced users)
    pub fn base_addr(&self) -> Option<*mut u8> {
        self.mapped_region.as_ref().map(|r| r.addr)
    }
}

impl Drop for PmemDevice {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Device manager for discovering and managing PMEM devices
pub struct DeviceManager {
    /// Discovered devices
    devices: Arc<RwLock<HashMap<String, Arc<RwLock<PmemDevice>>>>>,
    /// Device discovery sources
    discovery_paths: Vec<PathBuf>,
}

impl DeviceManager {
    /// Creates a new device manager
    pub fn new() -> Self {
        Self {
            devices: Arc::new(RwLock::new(HashMap::new())),
            discovery_paths: vec![
                PathBuf::from("/dev"),
                PathBuf::from("/sys/bus/nd"),
                PathBuf::from("/sys/bus/cxl"),
            ],
        }
    }

    /// Discovers available PMEM devices
    pub async fn discover(&self) -> Result<Vec<DeviceInfo>> {
        let mut devices = Vec::new();

        // Check for /dev/pmem* devices
        if let Ok(entries) = std::fs::read_dir("/dev") {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("pmem") || name.starts_with("dax") {
                    let info = self.probe_device(&entry.path()).await?;
                    devices.push(info);
                }
            }
        }

        // If no real devices found, create an emulated one for testing
        if devices.is_empty() {
            devices.push(DeviceInfo {
                id: "emulated-0".to_string(),
                name: "Emulated PMEM".to_string(),
                device_type: DeviceType::Emulated,
                mode: DeviceMode::AppDirect,
                capacity: 1024 * 1024 * 1024, // 1GB
                available: 1024 * 1024 * 1024,
                path: PathBuf::from("/dev/null"),
                numa_node: 0,
                interleave_set: None,
                health: DeviceHealth::Healthy,
                firmware_version: Some("emulated".to_string()),
                serial_number: None,
                temperature: None,
                lifetime_remaining: Some(100),
            });
        }

        Ok(devices)
    }

    /// Probes a specific device
    async fn probe_device(&self, path: &Path) -> Result<DeviceInfo> {
        // In real implementation, would read from sysfs and ndctl
        let name = path.file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let device_type = if name.starts_with("pmem") {
            DeviceType::Optane
        } else if name.starts_with("dax") {
            DeviceType::Optane
        } else {
            DeviceType::Unknown
        };

        Ok(DeviceInfo {
            id: name.clone(),
            name: name.clone(),
            device_type,
            mode: DeviceMode::AppDirect,
            capacity: 0, // Would read from sysfs
            available: 0,
            path: path.to_path_buf(),
            numa_node: 0,
            interleave_set: None,
            health: DeviceHealth::Unknown,
            firmware_version: None,
            serial_number: None,
            temperature: None,
            lifetime_remaining: None,
        })
    }

    /// Opens a device by ID
    pub async fn open_device(&self, device_id: &str) -> Result<Arc<RwLock<PmemDevice>>> {
        let devices = self.devices.write().await;

        if let Some(device) = devices.get(device_id) {
            return Ok(Arc::clone(device));
        }

        // Discover and find the device
        drop(devices);
        let discovered = self.discover().await?;
        let info = discovered.into_iter()
            .find(|d| d.id == device_id)
            .ok_or_else(|| StrataError::NotFound(format!("Device {} not found", device_id)))?;

        let mut device = PmemDevice::new(info);
        device.open()?;

        let device = Arc::new(RwLock::new(device));
        let mut devices = self.devices.write().await;
        devices.insert(device_id.to_string(), Arc::clone(&device));

        Ok(device)
    }

    /// Closes a device
    pub async fn close_device(&self, device_id: &str) -> Result<()> {
        let mut devices = self.devices.write().await;
        if let Some(device) = devices.remove(device_id) {
            let mut dev = device.write().await;
            dev.close()?;
        }
        Ok(())
    }

    /// Lists all opened devices
    pub async fn list_devices(&self) -> Vec<String> {
        let devices = self.devices.read().await;
        devices.keys().cloned().collect()
    }

    /// Gets device by ID
    pub async fn get_device(&self, device_id: &str) -> Option<Arc<RwLock<PmemDevice>>> {
        let devices = self.devices.read().await;
        devices.get(device_id).cloned()
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
    fn test_pmem_device() {
        let info = DeviceInfo {
            id: "test-0".to_string(),
            name: "Test PMEM".to_string(),
            device_type: DeviceType::Emulated,
            mode: DeviceMode::AppDirect,
            capacity: 1024 * 1024, // 1MB
            available: 1024 * 1024,
            path: PathBuf::from("/dev/null"),
            numa_node: 0,
            interleave_set: None,
            health: DeviceHealth::Healthy,
            firmware_version: None,
            serial_number: None,
            temperature: None,
            lifetime_remaining: None,
        };

        let mut device = PmemDevice::new(info);
        device.open().unwrap();

        // Write and read
        let data = b"Hello PMEM!";
        device.write(0, data).unwrap();
        device.flush(0, data.len()).unwrap();

        let read = device.read(0, data.len()).unwrap();
        assert_eq!(&read, data);

        let stats = device.stats();
        assert_eq!(stats.writes, 1);
        assert_eq!(stats.reads, 1);
        assert_eq!(stats.flushes, 1);
    }

    #[tokio::test]
    async fn test_device_manager() {
        let manager = DeviceManager::new();
        let devices = manager.discover().await.unwrap();

        assert!(!devices.is_empty());
        // Should have at least the emulated device
        assert!(devices.iter().any(|d| d.device_type == DeviceType::Emulated));
    }
}
