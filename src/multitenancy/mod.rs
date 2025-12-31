//! Multi-tenancy support for Strata
//!
//! Provides complete tenant isolation including:
//! - Namespace isolation (each tenant has isolated storage)
//! - Resource quotas (storage, IOPS, bandwidth)
//! - Per-tenant billing and metering
//! - Cross-tenant security boundaries
//! - Tenant lifecycle management

pub mod tenant;
pub mod isolation;
pub mod quota;
pub mod billing;
pub mod context;

pub use tenant::{Tenant, TenantId, TenantConfig, TenantStatus, TenantManager};
pub use isolation::{IsolationLevel, IsolationPolicy, TenantNamespace};
pub use quota::{ResourceQuota, QuotaEnforcer, QuotaUsage};
pub use billing::{BillingMetrics, UsageMeter, BillingPlan};
pub use context::{TenantContext, TenantScope};
