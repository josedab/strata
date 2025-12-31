// Billing and usage metering for multi-tenancy

use super::tenant::TenantId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// Billing plan type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingPlan {
    /// Free tier
    #[default]
    Free,
    /// Pay-as-you-go
    PayAsYouGo,
    /// Monthly subscription - Basic
    MonthlyBasic,
    /// Monthly subscription - Professional
    MonthlyProfessional,
    /// Annual subscription
    Annual,
    /// Enterprise (custom pricing)
    Enterprise,
}

/// Pricing rates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PricingRates {
    /// Storage cost per GB per month
    pub storage_per_gb_month: f64,
    /// Data transfer out per GB
    pub egress_per_gb: f64,
    /// Data transfer in per GB (usually free)
    pub ingress_per_gb: f64,
    /// API requests per 10,000
    pub requests_per_10k: f64,
    /// Read operations per 10,000
    pub read_ops_per_10k: f64,
    /// Write operations per 10,000
    pub write_ops_per_10k: f64,
    /// Delete operations per 10,000
    pub delete_ops_per_10k: f64,
    /// List operations per 10,000
    pub list_ops_per_10k: f64,
}

impl Default for PricingRates {
    fn default() -> Self {
        Self {
            storage_per_gb_month: 0.023, // Similar to S3 Standard
            egress_per_gb: 0.09,
            ingress_per_gb: 0.0, // Free ingress
            requests_per_10k: 0.005,
            read_ops_per_10k: 0.004,
            write_ops_per_10k: 0.05,
            delete_ops_per_10k: 0.0, // Free deletes
            list_ops_per_10k: 0.05,
        }
    }
}

impl PricingRates {
    /// Free tier (no costs)
    pub fn free() -> Self {
        Self {
            storage_per_gb_month: 0.0,
            egress_per_gb: 0.0,
            ingress_per_gb: 0.0,
            requests_per_10k: 0.0,
            read_ops_per_10k: 0.0,
            write_ops_per_10k: 0.0,
            delete_ops_per_10k: 0.0,
            list_ops_per_10k: 0.0,
        }
    }

    /// Discounted annual rates
    pub fn annual_discount() -> Self {
        let base = Self::default();
        Self {
            storage_per_gb_month: base.storage_per_gb_month * 0.8,
            egress_per_gb: base.egress_per_gb * 0.8,
            ingress_per_gb: base.ingress_per_gb,
            requests_per_10k: base.requests_per_10k * 0.8,
            read_ops_per_10k: base.read_ops_per_10k * 0.8,
            write_ops_per_10k: base.write_ops_per_10k * 0.8,
            delete_ops_per_10k: base.delete_ops_per_10k,
            list_ops_per_10k: base.list_ops_per_10k * 0.8,
        }
    }
}

/// Usage metrics for billing
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BillingMetrics {
    /// Average storage used (bytes)
    pub storage_bytes_avg: u64,
    /// Peak storage used (bytes)
    pub storage_bytes_peak: u64,
    /// Data transferred out (bytes)
    pub egress_bytes: u64,
    /// Data transferred in (bytes)
    pub ingress_bytes: u64,
    /// Total API requests
    pub total_requests: u64,
    /// Read operations
    pub read_operations: u64,
    /// Write operations
    pub write_operations: u64,
    /// Delete operations
    pub delete_operations: u64,
    /// List operations
    pub list_operations: u64,
    /// Number of active objects
    pub object_count: u64,
    /// Number of buckets
    pub bucket_count: u32,
    /// Billing period start
    pub period_start: Option<SystemTime>,
    /// Billing period end
    pub period_end: Option<SystemTime>,
}

impl BillingMetrics {
    /// Creates new metrics for a billing period
    pub fn new_period(start: SystemTime, end: SystemTime) -> Self {
        Self {
            period_start: Some(start),
            period_end: Some(end),
            ..Default::default()
        }
    }

    /// Calculates the bill for these metrics
    pub fn calculate_bill(&self, rates: &PricingRates) -> Bill {
        let storage_gb = self.storage_bytes_avg as f64 / (1024.0 * 1024.0 * 1024.0);
        let egress_gb = self.egress_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let ingress_gb = self.ingress_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

        let storage_cost = storage_gb * rates.storage_per_gb_month;
        let egress_cost = egress_gb * rates.egress_per_gb;
        let ingress_cost = ingress_gb * rates.ingress_per_gb;

        let read_cost = (self.read_operations as f64 / 10_000.0) * rates.read_ops_per_10k;
        let write_cost = (self.write_operations as f64 / 10_000.0) * rates.write_ops_per_10k;
        let delete_cost = (self.delete_operations as f64 / 10_000.0) * rates.delete_ops_per_10k;
        let list_cost = (self.list_operations as f64 / 10_000.0) * rates.list_ops_per_10k;
        let request_cost = (self.total_requests as f64 / 10_000.0) * rates.requests_per_10k;

        let total = storage_cost
            + egress_cost
            + ingress_cost
            + read_cost
            + write_cost
            + delete_cost
            + list_cost
            + request_cost;

        Bill {
            storage_cost,
            egress_cost,
            ingress_cost,
            request_cost: read_cost + write_cost + delete_cost + list_cost + request_cost,
            subtotal: total,
            tax: 0.0, // Tax calculation would depend on jurisdiction
            total,
            currency: "USD".to_string(),
            period_start: self.period_start,
            period_end: self.period_end,
            generated_at: SystemTime::now(),
        }
    }

    /// Merges another metrics instance into this one
    pub fn merge(&mut self, other: &BillingMetrics) {
        self.storage_bytes_avg = (self.storage_bytes_avg + other.storage_bytes_avg) / 2;
        self.storage_bytes_peak = self.storage_bytes_peak.max(other.storage_bytes_peak);
        self.egress_bytes += other.egress_bytes;
        self.ingress_bytes += other.ingress_bytes;
        self.total_requests += other.total_requests;
        self.read_operations += other.read_operations;
        self.write_operations += other.write_operations;
        self.delete_operations += other.delete_operations;
        self.list_operations += other.list_operations;
    }
}

/// Generated bill
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bill {
    /// Storage cost
    pub storage_cost: f64,
    /// Egress cost
    pub egress_cost: f64,
    /// Ingress cost
    pub ingress_cost: f64,
    /// Request cost (all operations)
    pub request_cost: f64,
    /// Subtotal before tax
    pub subtotal: f64,
    /// Tax amount
    pub tax: f64,
    /// Total amount
    pub total: f64,
    /// Currency code
    pub currency: String,
    /// Billing period start
    pub period_start: Option<SystemTime>,
    /// Billing period end
    pub period_end: Option<SystemTime>,
    /// When bill was generated
    pub generated_at: SystemTime,
}

impl Bill {
    /// Formats the bill as a simple text summary
    pub fn summary(&self) -> String {
        format!(
            "Storage: ${:.2}, Egress: ${:.2}, Requests: ${:.2}, Total: ${:.2} {}",
            self.storage_cost, self.egress_cost, self.request_cost, self.total, self.currency
        )
    }
}

/// Usage event types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UsageEvent {
    /// Object uploaded
    ObjectUploaded {
        bucket: String,
        key: String,
        size: u64,
    },
    /// Object downloaded
    ObjectDownloaded {
        bucket: String,
        key: String,
        size: u64,
    },
    /// Object deleted
    ObjectDeleted {
        bucket: String,
        key: String,
        size: u64,
    },
    /// Bucket created
    BucketCreated { name: String },
    /// Bucket deleted
    BucketDeleted { name: String },
    /// List operation
    ListOperation { bucket: String, count: u32 },
    /// API request
    ApiRequest { operation: String },
}

/// Metered usage sample
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UsageSample {
    timestamp: SystemTime,
    storage_bytes: u64,
    event: Option<UsageEvent>,
}

/// Usage meter for tracking tenant usage
pub struct UsageMeter {
    /// Current usage by tenant
    current: Arc<RwLock<HashMap<TenantId, BillingMetrics>>>,
    /// Historical usage samples
    samples: Arc<RwLock<HashMap<TenantId, Vec<UsageSample>>>>,
    /// Billing plans by tenant
    plans: Arc<RwLock<HashMap<TenantId, BillingPlan>>>,
    /// Pricing rates by plan
    rates: Arc<RwLock<HashMap<BillingPlan, PricingRates>>>,
    /// Sampling interval
    sample_interval: Duration,
}

impl UsageMeter {
    /// Creates a new usage meter
    pub fn new() -> Self {
        let mut rates = HashMap::new();
        rates.insert(BillingPlan::Free, PricingRates::free());
        rates.insert(BillingPlan::PayAsYouGo, PricingRates::default());
        rates.insert(BillingPlan::MonthlyBasic, PricingRates::default());
        rates.insert(BillingPlan::MonthlyProfessional, PricingRates::default());
        rates.insert(BillingPlan::Annual, PricingRates::annual_discount());
        rates.insert(BillingPlan::Enterprise, PricingRates::default());

        Self {
            current: Arc::new(RwLock::new(HashMap::new())),
            samples: Arc::new(RwLock::new(HashMap::new())),
            plans: Arc::new(RwLock::new(HashMap::new())),
            rates: Arc::new(RwLock::new(rates)),
            sample_interval: Duration::from_secs(3600), // Hourly samples
        }
    }

    /// Initializes metering for a tenant
    pub async fn initialize_tenant(&self, tenant_id: TenantId, plan: BillingPlan) {
        self.current
            .write()
            .await
            .insert(tenant_id.clone(), BillingMetrics::default());
        self.samples
            .write()
            .await
            .insert(tenant_id.clone(), Vec::new());
        self.plans.write().await.insert(tenant_id, plan);
    }

    /// Updates billing plan for a tenant
    pub async fn set_plan(&self, tenant_id: &TenantId, plan: BillingPlan) {
        self.plans.write().await.insert(tenant_id.clone(), plan);
    }

    /// Gets the billing plan for a tenant
    pub async fn get_plan(&self, tenant_id: &TenantId) -> Option<BillingPlan> {
        self.plans.read().await.get(tenant_id).copied()
    }

    /// Records a usage event
    pub async fn record_event(&self, tenant_id: &TenantId, event: UsageEvent) {
        let mut current = self.current.write().await;
        let metrics = current
            .entry(tenant_id.clone())
            .or_insert_with(BillingMetrics::default);

        match &event {
            UsageEvent::ObjectUploaded { size, .. } => {
                metrics.ingress_bytes += size;
                metrics.write_operations += 1;
                metrics.total_requests += 1;
            }
            UsageEvent::ObjectDownloaded { size, .. } => {
                metrics.egress_bytes += size;
                metrics.read_operations += 1;
                metrics.total_requests += 1;
            }
            UsageEvent::ObjectDeleted { .. } => {
                metrics.delete_operations += 1;
                metrics.total_requests += 1;
            }
            UsageEvent::BucketCreated { .. } => {
                metrics.bucket_count += 1;
                metrics.write_operations += 1;
                metrics.total_requests += 1;
            }
            UsageEvent::BucketDeleted { .. } => {
                metrics.bucket_count = metrics.bucket_count.saturating_sub(1);
                metrics.delete_operations += 1;
                metrics.total_requests += 1;
            }
            UsageEvent::ListOperation { count, .. } => {
                metrics.list_operations += *count as u64;
                metrics.total_requests += 1;
            }
            UsageEvent::ApiRequest { .. } => {
                metrics.total_requests += 1;
            }
        }
    }

    /// Updates storage usage snapshot
    pub async fn update_storage(&self, tenant_id: &TenantId, storage_bytes: u64) {
        let mut current = self.current.write().await;
        if let Some(metrics) = current.get_mut(tenant_id) {
            metrics.storage_bytes_peak = metrics.storage_bytes_peak.max(storage_bytes);
            // Rolling average
            metrics.storage_bytes_avg = (metrics.storage_bytes_avg + storage_bytes) / 2;
        }

        // Record sample
        let mut samples = self.samples.write().await;
        if let Some(tenant_samples) = samples.get_mut(tenant_id) {
            tenant_samples.push(UsageSample {
                timestamp: SystemTime::now(),
                storage_bytes,
                event: None,
            });

            // Keep last 30 days of samples (720 hourly samples)
            if tenant_samples.len() > 720 {
                tenant_samples.remove(0);
            }
        }
    }

    /// Gets current usage metrics for a tenant
    pub async fn get_current_usage(&self, tenant_id: &TenantId) -> Option<BillingMetrics> {
        self.current.read().await.get(tenant_id).cloned()
    }

    /// Generates a bill for the current period
    pub async fn generate_bill(&self, tenant_id: &TenantId) -> Option<Bill> {
        let current = self.current.read().await;
        let plans = self.plans.read().await;
        let rates = self.rates.read().await;

        let metrics = current.get(tenant_id)?;
        let plan = plans.get(tenant_id).copied().unwrap_or(BillingPlan::PayAsYouGo);
        let pricing = rates.get(&plan)?;

        Some(metrics.calculate_bill(pricing))
    }

    /// Resets usage metrics for a new billing period
    pub async fn reset_period(&self, tenant_id: &TenantId) {
        let mut current = self.current.write().await;
        if let Some(metrics) = current.get_mut(tenant_id) {
            let now = SystemTime::now();
            *metrics = BillingMetrics {
                storage_bytes_avg: metrics.storage_bytes_avg,
                storage_bytes_peak: metrics.storage_bytes_peak,
                bucket_count: metrics.bucket_count,
                object_count: metrics.object_count,
                period_start: Some(now),
                ..Default::default()
            };
        }
    }

    /// Gets historical usage for a time range
    pub async fn get_historical_usage(
        &self,
        tenant_id: &TenantId,
        from: SystemTime,
        to: SystemTime,
    ) -> Vec<(SystemTime, u64)> {
        let samples = self.samples.read().await;

        samples
            .get(tenant_id)
            .map(|s| {
                s.iter()
                    .filter(|sample| sample.timestamp >= from && sample.timestamp <= to)
                    .map(|sample| (sample.timestamp, sample.storage_bytes))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Sets custom pricing rates for a plan
    pub async fn set_rates(&self, plan: BillingPlan, rates: PricingRates) {
        self.rates.write().await.insert(plan, rates);
    }

    /// Removes a tenant from metering
    pub async fn remove_tenant(&self, tenant_id: &TenantId) {
        self.current.write().await.remove(tenant_id);
        self.samples.write().await.remove(tenant_id);
        self.plans.write().await.remove(tenant_id);
    }

    /// Gets all tenants with their current bills
    pub async fn get_all_bills(&self) -> HashMap<TenantId, Bill> {
        let current = self.current.read().await;
        let plans = self.plans.read().await;
        let rates = self.rates.read().await;

        let mut bills = HashMap::new();

        for (tenant_id, metrics) in current.iter() {
            let plan = plans.get(tenant_id).copied().unwrap_or(BillingPlan::PayAsYouGo);
            if let Some(pricing) = rates.get(&plan) {
                bills.insert(tenant_id.clone(), metrics.calculate_bill(pricing));
            }
        }

        bills
    }
}

impl Default for UsageMeter {
    fn default() -> Self {
        Self::new()
    }
}

/// Cost allocation tag
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostAllocationTag {
    /// Tag key
    pub key: String,
    /// Tag value
    pub value: String,
}

/// Cost report entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostReportEntry {
    /// Tenant ID
    pub tenant_id: TenantId,
    /// Billing period
    pub period: String,
    /// Service type
    pub service: String,
    /// Usage amount
    pub usage_amount: f64,
    /// Usage unit
    pub usage_unit: String,
    /// Cost amount
    pub cost: f64,
    /// Currency
    pub currency: String,
    /// Cost allocation tags
    pub tags: Vec<CostAllocationTag>,
}

/// Cost report generator
pub struct CostReportGenerator;

impl CostReportGenerator {
    /// Generates a detailed cost report
    pub fn generate_report(
        tenant_id: &TenantId,
        metrics: &BillingMetrics,
        rates: &PricingRates,
    ) -> Vec<CostReportEntry> {
        let period = metrics
            .period_start
            .and_then(|s| {
                s.duration_since(SystemTime::UNIX_EPOCH)
                    .ok()
                    .map(|d| format!("{}", d.as_secs()))
            })
            .unwrap_or_else(|| "current".to_string());

        let mut entries = Vec::new();

        // Storage entry
        let storage_gb = metrics.storage_bytes_avg as f64 / (1024.0 * 1024.0 * 1024.0);
        entries.push(CostReportEntry {
            tenant_id: tenant_id.clone(),
            period: period.clone(),
            service: "Storage".to_string(),
            usage_amount: storage_gb,
            usage_unit: "GB-Month".to_string(),
            cost: storage_gb * rates.storage_per_gb_month,
            currency: "USD".to_string(),
            tags: Vec::new(),
        });

        // Egress entry
        let egress_gb = metrics.egress_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        if egress_gb > 0.0 {
            entries.push(CostReportEntry {
                tenant_id: tenant_id.clone(),
                period: period.clone(),
                service: "Data Transfer Out".to_string(),
                usage_amount: egress_gb,
                usage_unit: "GB".to_string(),
                cost: egress_gb * rates.egress_per_gb,
                currency: "USD".to_string(),
                tags: Vec::new(),
            });
        }

        // Read operations
        if metrics.read_operations > 0 {
            entries.push(CostReportEntry {
                tenant_id: tenant_id.clone(),
                period: period.clone(),
                service: "Read Operations".to_string(),
                usage_amount: metrics.read_operations as f64,
                usage_unit: "Requests".to_string(),
                cost: (metrics.read_operations as f64 / 10_000.0) * rates.read_ops_per_10k,
                currency: "USD".to_string(),
                tags: Vec::new(),
            });
        }

        // Write operations
        if metrics.write_operations > 0 {
            entries.push(CostReportEntry {
                tenant_id: tenant_id.clone(),
                period: period.clone(),
                service: "Write Operations".to_string(),
                usage_amount: metrics.write_operations as f64,
                usage_unit: "Requests".to_string(),
                cost: (metrics.write_operations as f64 / 10_000.0) * rates.write_ops_per_10k,
                currency: "USD".to_string(),
                tags: Vec::new(),
            });
        }

        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_billing_calculation() {
        let metrics = BillingMetrics {
            storage_bytes_avg: 100 * 1024 * 1024 * 1024, // 100 GB
            egress_bytes: 10 * 1024 * 1024 * 1024,       // 10 GB
            read_operations: 100_000,
            write_operations: 10_000,
            ..Default::default()
        };

        let rates = PricingRates::default();
        let bill = metrics.calculate_bill(&rates);

        // Verify calculations
        assert!(bill.storage_cost > 0.0);
        assert!(bill.egress_cost > 0.0);
        assert!(bill.total > 0.0);
    }

    #[tokio::test]
    async fn test_usage_metering() {
        let meter = UsageMeter::new();
        let tenant_id = TenantId::new("test-tenant");

        meter
            .initialize_tenant(tenant_id.clone(), BillingPlan::PayAsYouGo)
            .await;

        // Record some events
        meter
            .record_event(
                &tenant_id,
                UsageEvent::ObjectUploaded {
                    bucket: "test".to_string(),
                    key: "file.txt".to_string(),
                    size: 1024 * 1024,
                },
            )
            .await;

        meter
            .record_event(
                &tenant_id,
                UsageEvent::ObjectDownloaded {
                    bucket: "test".to_string(),
                    key: "file.txt".to_string(),
                    size: 1024 * 1024,
                },
            )
            .await;

        let usage = meter.get_current_usage(&tenant_id).await.unwrap();
        assert_eq!(usage.ingress_bytes, 1024 * 1024);
        assert_eq!(usage.egress_bytes, 1024 * 1024);
        assert_eq!(usage.write_operations, 1);
        assert_eq!(usage.read_operations, 1);
    }

    #[test]
    fn test_free_tier_no_cost() {
        let metrics = BillingMetrics {
            storage_bytes_avg: 1024 * 1024 * 1024, // 1 GB
            egress_bytes: 100 * 1024 * 1024,       // 100 MB
            read_operations: 1000,
            write_operations: 100,
            ..Default::default()
        };

        let rates = PricingRates::free();
        let bill = metrics.calculate_bill(&rates);

        assert_eq!(bill.total, 0.0);
    }

    #[test]
    fn test_annual_discount() {
        let metrics = BillingMetrics {
            storage_bytes_avg: 100 * 1024 * 1024 * 1024,
            ..Default::default()
        };

        let standard = PricingRates::default();
        let annual = PricingRates::annual_discount();

        let standard_bill = metrics.calculate_bill(&standard);
        let annual_bill = metrics.calculate_bill(&annual);

        // Annual should be cheaper
        assert!(annual_bill.total < standard_bill.total);
    }
}
