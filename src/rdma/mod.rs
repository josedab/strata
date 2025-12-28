//! RDMA and DPDK High-Performance Networking
//!
//! Provides kernel-bypass networking for ultra-low latency data transfer:
//! - RDMA (Remote Direct Memory Access) for zero-copy transfers
//! - DPDK (Data Plane Development Kit) for high-throughput packet processing
//! - Memory registration and management
//! - Connection management

pub mod transport;
pub mod memory;
pub mod connection;
pub mod dpdk;

pub use transport::{RdmaTransport, TransportConfig, TransportType, ConnectionState};
pub use memory::{MemoryRegion, MemoryPool, RegisteredBuffer};
pub use connection::{RdmaConnection, QueuePair};
pub use dpdk::{DpdkPort, DpdkConfig, PacketBuffer};
