//! PMEM (Persistent Memory) and CXL (Compute Express Link) Support
//!
//! Provides support for persistent memory technologies:
//! - Intel Optane PMem
//! - CXL Type 3 memory devices
//! - Persistent memory allocation
//! - Crash-consistent data structures

pub mod allocator;
pub mod buffer;
pub mod cxl;
pub mod device;

pub use allocator::{PmemAllocator, AllocatorConfig, PmemAllocation};
pub use buffer::{PersistentBuffer, BufferPool};
pub use cxl::{CxlDevice, CxlConfig, MemoryType};
pub use device::{PmemDevice, DeviceInfo, DeviceManager};
