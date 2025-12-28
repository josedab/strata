//! GPU Acceleration for Strata
//!
//! Provides GPU-accelerated operations for:
//! - Compression/decompression
//! - Encryption/decryption
//! - Erasure coding
//! - Deduplication hashing
//! - Data processing pipelines

pub mod device;
pub mod kernel;
pub mod memory;
pub mod operations;

pub use device::{GpuDevice, GpuDeviceInfo, DeviceManager};
pub use kernel::{Kernel, KernelConfig, KernelType};
pub use memory::{GpuBuffer, GpuMemoryPool, TransferMode};
pub use operations::{GpuOperations, CompressionOp, EncryptionOp, ErasureOp};
