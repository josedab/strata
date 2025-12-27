// WORM (Write Once Read Many) / Immutable Storage
//
// Provides compliance-grade immutable storage with:
// - Legal hold capability
// - Retention policies (governance and compliance modes)
// - Audit logging for all access attempts
// - SEC 17a-4, FINRA, HIPAA compliance support

pub mod retention;
pub mod legal_hold;
pub mod compliance;
pub mod audit;

pub use retention::{RetentionPolicy, RetentionMode, RetentionPeriod};
pub use legal_hold::LegalHold;
pub use compliance::{ComplianceManager, ComplianceConfig};
pub use audit::WormAuditLog;
