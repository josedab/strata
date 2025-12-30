//! Carbon-Aware Storage
//!
//! Schedules data operations based on carbon intensity of the power grid
//! to minimize environmental impact while maintaining SLAs.
//!
//! # Features
//!
//! - Real-time carbon intensity monitoring
//! - Intelligent workload scheduling
//! - Geographic workload shifting
//! - Carbon budget tracking and reporting
//! - Green SLA support
//!
//! # Example
//!
//! ```rust,ignore
//! use strata::carbon::{CarbonScheduler, CarbonPolicy, WorkloadClass};
//!
//! let scheduler = CarbonScheduler::new()
//!     .with_region("us-west-2")
//!     .with_carbon_budget(1000.0);  // kg CO2 per month
//!
//! // Schedule deferrable workload
//! scheduler.schedule(
//!     WorkloadClass::Deferrable { deadline: Duration::from_hours(24) },
//!     || async { perform_backup().await }
//! ).await?;
//! ```

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Carbon-aware scheduler for storage operations
pub struct CarbonScheduler {
    /// Carbon intensity provider
    intensity_provider: Arc<dyn CarbonIntensityProvider + Send + Sync>,
    /// Workload queue
    workload_queue: Arc<RwLock<WorkloadQueue>>,
    /// Carbon budget tracker
    budget_tracker: Arc<RwLock<CarbonBudget>>,
    /// Scheduler configuration
    config: SchedulerConfig,
    /// Metrics
    metrics: Arc<RwLock<CarbonMetrics>>,
    /// Geographic optimizer
    geo_optimizer: GeoOptimizer,
}

/// Carbon intensity provider trait
pub trait CarbonIntensityProvider {
    /// Get current carbon intensity (gCO2/kWh)
    fn current_intensity(&self, region: &str) -> f64;

    /// Get forecasted intensity
    fn forecast(&self, region: &str, hours_ahead: u32) -> Vec<CarbonForecast>;

    /// Get historical intensity
    fn historical(&self, region: &str, hours_back: u32) -> Vec<CarbonReading>;
}

/// Carbon forecast entry
#[derive(Debug, Clone)]
pub struct CarbonForecast {
    /// Timestamp
    pub timestamp: u64,
    /// Forecasted intensity (gCO2/kWh)
    pub intensity: f64,
    /// Confidence (0.0-1.0)
    pub confidence: f64,
}

/// Historical carbon reading
#[derive(Debug, Clone)]
pub struct CarbonReading {
    /// Timestamp
    pub timestamp: u64,
    /// Actual intensity (gCO2/kWh)
    pub intensity: f64,
    /// Data source
    pub source: String,
}

/// Scheduler configuration
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Primary region
    pub region: String,
    /// Available regions for workload shifting
    pub available_regions: Vec<String>,
    /// Carbon intensity threshold for immediate execution
    pub green_threshold: f64,
    /// Carbon intensity threshold for deferral
    pub defer_threshold: f64,
    /// Maximum deferral time
    pub max_deferral: Duration,
    /// Enable geographic shifting
    pub geo_shifting_enabled: bool,
    /// Power consumption per operation (kWh)
    pub power_per_operation: f64,
}

/// Workload classification
#[derive(Debug, Clone)]
pub enum WorkloadClass {
    /// Must execute immediately
    Urgent,
    /// Can be deferred within deadline
    Deferrable {
        deadline: Duration,
    },
    /// Background work, execute when green
    Background,
    /// Can shift to different regions
    Shiftable {
        deadline: Duration,
        allowed_regions: Vec<String>,
    },
    /// Batched operations
    Batch {
        min_batch_size: usize,
        max_wait: Duration,
    },
}

/// Queued workload
struct QueuedWorkload {
    /// Unique ID
    id: String,
    /// Workload class
    class: WorkloadClass,
    /// Queued timestamp
    queued_at: Instant,
    /// Priority score (lower is higher priority)
    priority: i64,
    /// Estimated energy consumption (kWh)
    estimated_energy: f64,
    /// Preferred region
    preferred_region: Option<String>,
    /// Callback identifier
    callback_id: String,
}

impl PartialEq for QueuedWorkload {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for QueuedWorkload {}

impl PartialOrd for QueuedWorkload {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedWorkload {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Lower priority value = higher priority in heap
        other.priority.cmp(&self.priority)
    }
}

/// Workload queue with priority scheduling
struct WorkloadQueue {
    /// Priority queue for immediate/urgent work
    urgent: BinaryHeap<QueuedWorkload>,
    /// Deferrable workloads
    deferrable: Vec<QueuedWorkload>,
    /// Background workloads
    background: VecDeque<QueuedWorkload>,
    /// Batch accumulators
    batches: HashMap<String, Vec<QueuedWorkload>>,
}

/// Carbon budget tracker
struct CarbonBudget {
    /// Monthly budget (kg CO2)
    monthly_budget: f64,
    /// Current month usage (kg CO2)
    current_usage: f64,
    /// Daily breakdown
    daily_usage: HashMap<u32, f64>,
    /// Budget start timestamp
    period_start: u64,
    /// Alerts
    alerts: Vec<BudgetAlert>,
}

/// Budget alert
#[derive(Debug, Clone)]
pub struct BudgetAlert {
    /// Alert type
    pub alert_type: AlertType,
    /// Message
    pub message: String,
    /// Timestamp
    pub timestamp: Instant,
}

/// Alert types
#[derive(Debug, Clone)]
pub enum AlertType {
    /// Approaching budget limit
    Warning,
    /// Exceeded budget
    Critical,
    /// Unusual consumption pattern
    Anomaly,
}

/// Carbon metrics
#[derive(Debug, Clone, Default)]
pub struct CarbonMetrics {
    /// Total operations executed
    pub total_operations: u64,
    /// Operations executed during green periods
    pub green_operations: u64,
    /// Total carbon emissions (kg CO2)
    pub total_emissions: f64,
    /// Carbon saved through deferral (kg CO2)
    pub carbon_saved: f64,
    /// Average carbon intensity at execution
    pub avg_execution_intensity: f64,
    /// Operations deferred
    pub deferred_operations: u64,
    /// Operations shifted geographically
    pub shifted_operations: u64,
}

/// Geographic optimizer
struct GeoOptimizer {
    /// Region carbon data
    region_data: HashMap<String, RegionCarbonProfile>,
    /// Network latency between regions
    network_latency: HashMap<(String, String), Duration>,
}

/// Carbon profile for a region
#[derive(Debug, Clone)]
struct RegionCarbonProfile {
    /// Region name
    region: String,
    /// Average intensity
    avg_intensity: f64,
    /// Current intensity
    current_intensity: f64,
    /// Renewable percentage
    renewable_pct: f64,
    /// Best hours (0-23 UTC)
    best_hours: Vec<u8>,
}

/// Execution result
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    /// Workload ID
    pub workload_id: String,
    /// Execution region
    pub region: String,
    /// Carbon intensity at execution
    pub carbon_intensity: f64,
    /// Carbon emissions (g CO2)
    pub emissions: f64,
    /// Was deferred
    pub was_deferred: bool,
    /// Deferral time
    pub deferral_time: Duration,
    /// Carbon saved vs immediate (g CO2)
    pub carbon_saved: f64,
}

/// Carbon policy for workloads
#[derive(Debug, Clone)]
pub struct CarbonPolicy {
    /// Maximum carbon intensity for execution
    pub max_intensity: Option<f64>,
    /// Preferred execution hours (UTC)
    pub preferred_hours: Vec<u8>,
    /// Allow geographic shifting
    pub allow_shifting: bool,
    /// Carbon offset enabled
    pub offset_enabled: bool,
}

/// Mock carbon intensity provider
pub struct MockCarbonProvider {
    base_intensity: HashMap<String, f64>,
}

impl CarbonScheduler {
    /// Create a new carbon scheduler
    pub fn new() -> Self {
        Self::with_provider(Arc::new(MockCarbonProvider::new()))
    }

    /// Create with custom provider
    pub fn with_provider(provider: Arc<dyn CarbonIntensityProvider + Send + Sync>) -> Self {
        Self {
            intensity_provider: provider,
            workload_queue: Arc::new(RwLock::new(WorkloadQueue::new())),
            budget_tracker: Arc::new(RwLock::new(CarbonBudget::new(1000.0))),
            config: SchedulerConfig::default(),
            metrics: Arc::new(RwLock::new(CarbonMetrics::default())),
            geo_optimizer: GeoOptimizer::new(),
        }
    }

    /// Set region
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.config.region = region.into();
        self
    }

    /// Set carbon budget
    pub fn with_carbon_budget(self, budget_kg: f64) -> Self {
        let mut tracker = self.budget_tracker.write().unwrap();
        tracker.monthly_budget = budget_kg;
        drop(tracker);
        self
    }

    /// Add available region
    pub fn with_available_region(mut self, region: impl Into<String>) -> Self {
        self.config.available_regions.push(region.into());
        self
    }

    /// Get current carbon intensity
    pub fn current_intensity(&self) -> f64 {
        self.intensity_provider.current_intensity(&self.config.region)
    }

    /// Get intensity for region
    pub fn intensity_for_region(&self, region: &str) -> f64 {
        self.intensity_provider.current_intensity(region)
    }

    /// Check if current conditions are "green"
    pub fn is_green(&self) -> bool {
        self.current_intensity() <= self.config.green_threshold
    }

    /// Find the greenest available region
    pub fn greenest_region(&self) -> (String, f64) {
        let mut best_region = self.config.region.clone();
        let mut best_intensity = self.current_intensity();

        for region in &self.config.available_regions {
            let intensity = self.intensity_for_region(region);
            if intensity < best_intensity {
                best_intensity = intensity;
                best_region = region.clone();
            }
        }

        (best_region, best_intensity)
    }

    /// Queue a workload for execution
    pub fn queue_workload(
        &self,
        class: WorkloadClass,
        estimated_energy: f64,
        callback_id: impl Into<String>,
    ) -> String {
        let id = format!("wl-{}", uuid_v4());
        let callback_id: String = callback_id.into();
        let callback_id_for_batch = callback_id.clone();

        let priority = match &class {
            WorkloadClass::Urgent => 0,
            WorkloadClass::Deferrable { deadline } => {
                // Priority based on deadline proximity
                deadline.as_secs() as i64
            }
            WorkloadClass::Background => i64::MAX / 2,
            WorkloadClass::Shiftable { deadline, .. } => deadline.as_secs() as i64,
            WorkloadClass::Batch { max_wait, .. } => max_wait.as_secs() as i64,
        };

        let workload = QueuedWorkload {
            id: id.clone(),
            class: class.clone(),
            queued_at: Instant::now(),
            priority,
            estimated_energy,
            preferred_region: None,
            callback_id,
        };

        let mut queue = self.workload_queue.write().unwrap();

        match class {
            WorkloadClass::Urgent => {
                queue.urgent.push(workload);
            }
            WorkloadClass::Deferrable { .. } | WorkloadClass::Shiftable { .. } => {
                queue.deferrable.push(workload);
            }
            WorkloadClass::Background => {
                queue.background.push_back(workload);
            }
            WorkloadClass::Batch { .. } => {
                queue.batches
                    .entry(callback_id_for_batch)
                    .or_default()
                    .push(workload);
            }
        }

        id
    }

    /// Execute urgent workloads immediately
    pub fn execute_urgent(&self) -> Vec<ExecutionResult> {
        let mut queue = self.workload_queue.write().unwrap();
        let mut results = Vec::new();

        while let Some(workload) = queue.urgent.pop() {
            let result = self.execute_workload(&workload);
            results.push(result);
        }

        results
    }

    /// Execute workloads based on current carbon conditions
    pub fn execute_scheduled(&self) -> Vec<ExecutionResult> {
        let mut results = Vec::new();
        let current_intensity = self.current_intensity();
        let is_green = current_intensity <= self.config.green_threshold;

        let mut queue = self.workload_queue.write().unwrap();

        // Execute deferrable workloads if green or deadline approaching
        let mut remaining_deferrable = Vec::new();
        for workload in queue.deferrable.drain(..) {
            let elapsed = workload.queued_at.elapsed();
            let deadline = match &workload.class {
                WorkloadClass::Deferrable { deadline } => *deadline,
                WorkloadClass::Shiftable { deadline, .. } => *deadline,
                _ => Duration::from_secs(0),
            };

            let should_execute = is_green || elapsed >= deadline.saturating_sub(Duration::from_secs(60));

            if should_execute {
                let result = self.execute_workload_unlocked(&workload, current_intensity);
                results.push(result);
            } else {
                remaining_deferrable.push(workload);
            }
        }
        queue.deferrable = remaining_deferrable;

        // Execute background if very green
        if current_intensity <= self.config.green_threshold * 0.5 {
            if let Some(workload) = queue.background.pop_front() {
                let result = self.execute_workload_unlocked(&workload, current_intensity);
                results.push(result);
            }
        }

        // Check batches
        let _now = Instant::now();
        let mut completed_batches = Vec::new();
        for (key, batch) in queue.batches.iter() {
            if let Some(first) = batch.first() {
                let (min_size, max_wait) = match &first.class {
                    WorkloadClass::Batch { min_batch_size, max_wait } => (*min_batch_size, *max_wait),
                    _ => (1, Duration::from_secs(0)),
                };

                let should_execute = batch.len() >= min_size
                    || first.queued_at.elapsed() >= max_wait
                    || is_green;

                if should_execute {
                    completed_batches.push(key.clone());
                }
            }
        }

        for key in completed_batches {
            if let Some(batch) = queue.batches.remove(&key) {
                for workload in batch {
                    let result = self.execute_workload_unlocked(&workload, current_intensity);
                    results.push(result);
                }
            }
        }

        results
    }

    fn execute_workload(&self, workload: &QueuedWorkload) -> ExecutionResult {
        let intensity = self.current_intensity();
        self.execute_workload_unlocked(workload, intensity)
    }

    fn execute_workload_unlocked(&self, workload: &QueuedWorkload, intensity: f64) -> ExecutionResult {
        let deferral_time = workload.queued_at.elapsed();
        let emissions = workload.estimated_energy * intensity; // g CO2
        let was_deferred = deferral_time > Duration::from_secs(1);

        // Calculate savings (what would have been emitted at queue time)
        let carbon_saved = if was_deferred {
            // Assume average intensity was higher when queued
            let assumed_queue_intensity = intensity * 1.2;
            workload.estimated_energy * (assumed_queue_intensity - intensity)
        } else {
            0.0
        };

        // Determine execution region
        let region = if self.config.geo_shifting_enabled {
            if let WorkloadClass::Shiftable { allowed_regions, .. } = &workload.class {
                self.find_greenest_in_regions(allowed_regions)
                    .unwrap_or(self.config.region.clone())
            } else {
                self.config.region.clone()
            }
        } else {
            self.config.region.clone()
        };

        // Update metrics
        {
            let mut metrics = self.metrics.write().unwrap();
            metrics.total_operations += 1;
            metrics.total_emissions += emissions / 1000.0; // Convert to kg

            if intensity <= self.config.green_threshold {
                metrics.green_operations += 1;
            }

            if was_deferred {
                metrics.deferred_operations += 1;
                metrics.carbon_saved += carbon_saved / 1000.0;
            }

            if region != self.config.region {
                metrics.shifted_operations += 1;
            }

            // Update rolling average
            let n = metrics.total_operations as f64;
            metrics.avg_execution_intensity =
                (metrics.avg_execution_intensity * (n - 1.0) + intensity) / n;
        }

        // Update budget
        {
            let mut budget = self.budget_tracker.write().unwrap();
            budget.record_emission(emissions / 1000.0);
        }

        ExecutionResult {
            workload_id: workload.id.clone(),
            region,
            carbon_intensity: intensity,
            emissions,
            was_deferred,
            deferral_time,
            carbon_saved,
        }
    }

    fn find_greenest_in_regions(&self, regions: &[String]) -> Option<String> {
        regions
            .iter()
            .map(|r| (r.clone(), self.intensity_for_region(r)))
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
            .map(|(r, _)| r)
    }

    /// Get optimal execution time in next N hours
    pub fn optimal_execution_time(&self, hours: u32) -> Option<(u64, f64)> {
        let forecast = self.intensity_provider.forecast(&self.config.region, hours);

        forecast
            .into_iter()
            .min_by(|a, b| a.intensity.partial_cmp(&b.intensity).unwrap())
            .map(|f| (f.timestamp, f.intensity))
    }

    /// Get carbon budget status
    pub fn budget_status(&self) -> BudgetStatus {
        let budget = self.budget_tracker.read().unwrap();

        let usage_pct = budget.current_usage / budget.monthly_budget * 100.0;
        let days_elapsed = days_in_period(budget.period_start);
        let days_in_month = 30.0;
        let expected_pct = (days_elapsed / days_in_month) * 100.0;

        BudgetStatus {
            monthly_budget: budget.monthly_budget,
            current_usage: budget.current_usage,
            remaining: budget.monthly_budget - budget.current_usage,
            usage_percentage: usage_pct,
            expected_percentage: expected_pct,
            on_track: usage_pct <= expected_pct * 1.1,
        }
    }

    /// Get carbon metrics
    pub fn metrics(&self) -> CarbonMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Get queue status
    pub fn queue_status(&self) -> QueueStatus {
        let queue = self.workload_queue.read().unwrap();

        QueueStatus {
            urgent_count: queue.urgent.len(),
            deferrable_count: queue.deferrable.len(),
            background_count: queue.background.len(),
            batch_count: queue.batches.values().map(|b| b.len()).sum(),
        }
    }

    /// Estimate carbon emissions for a workload
    pub fn estimate_emissions(&self, energy_kwh: f64) -> EmissionEstimate {
        let current = self.current_intensity();
        let (greenest_region, greenest_intensity) = self.greenest_region();
        let forecast = self.intensity_provider.forecast(&self.config.region, 24);

        let best_forecast = forecast
            .iter()
            .min_by(|a, b| a.intensity.partial_cmp(&b.intensity).unwrap());

        EmissionEstimate {
            immediate_emissions: energy_kwh * current,
            immediate_intensity: current,
            best_region: greenest_region,
            best_region_emissions: energy_kwh * greenest_intensity,
            best_region_intensity: greenest_intensity,
            best_time_emissions: best_forecast
                .map(|f| energy_kwh * f.intensity)
                .unwrap_or(energy_kwh * current),
            best_time_intensity: best_forecast.map(|f| f.intensity).unwrap_or(current),
            best_time: best_forecast.map(|f| f.timestamp),
        }
    }

    /// Generate sustainability report
    pub fn generate_report(&self) -> SustainabilityReport {
        let metrics = self.metrics.read().unwrap();
        let budget = self.budget_tracker.read().unwrap();

        let green_pct = if metrics.total_operations > 0 {
            metrics.green_operations as f64 / metrics.total_operations as f64 * 100.0
        } else {
            0.0
        };

        SustainabilityReport {
            period_start: budget.period_start,
            total_operations: metrics.total_operations,
            total_emissions_kg: metrics.total_emissions,
            carbon_saved_kg: metrics.carbon_saved,
            green_execution_pct: green_pct,
            avg_carbon_intensity: metrics.avg_execution_intensity,
            deferred_operations: metrics.deferred_operations,
            shifted_operations: metrics.shifted_operations,
            budget_used_pct: budget.current_usage / budget.monthly_budget * 100.0,
        }
    }
}

impl Default for CarbonScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            available_regions: vec![
                "us-west-2".to_string(),
                "eu-west-1".to_string(),
                "eu-north-1".to_string(),
            ],
            green_threshold: 200.0,   // gCO2/kWh
            defer_threshold: 400.0,   // gCO2/kWh
            max_deferral: Duration::from_secs(86400), // 24 hours
            geo_shifting_enabled: true,
            power_per_operation: 0.001, // 1 Wh per operation
        }
    }
}

impl WorkloadQueue {
    fn new() -> Self {
        Self {
            urgent: BinaryHeap::new(),
            deferrable: Vec::new(),
            background: VecDeque::new(),
            batches: HashMap::new(),
        }
    }
}

impl CarbonBudget {
    fn new(monthly_budget: f64) -> Self {
        Self {
            monthly_budget,
            current_usage: 0.0,
            daily_usage: HashMap::new(),
            period_start: current_timestamp(),
            alerts: Vec::new(),
        }
    }

    fn record_emission(&mut self, kg_co2: f64) {
        self.current_usage += kg_co2;

        let day = days_in_period(self.period_start) as u32;
        *self.daily_usage.entry(day).or_insert(0.0) += kg_co2;

        // Check for alerts
        let usage_pct = self.current_usage / self.monthly_budget;
        if usage_pct >= 0.9 && !self.has_alert(AlertType::Critical) {
            self.alerts.push(BudgetAlert {
                alert_type: AlertType::Critical,
                message: format!(
                    "Carbon budget 90% consumed: {:.1} kg of {:.1} kg",
                    self.current_usage, self.monthly_budget
                ),
                timestamp: Instant::now(),
            });
        } else if usage_pct >= 0.7 && !self.has_alert(AlertType::Warning) {
            self.alerts.push(BudgetAlert {
                alert_type: AlertType::Warning,
                message: format!(
                    "Carbon budget 70% consumed: {:.1} kg of {:.1} kg",
                    self.current_usage, self.monthly_budget
                ),
                timestamp: Instant::now(),
            });
        }
    }

    fn has_alert(&self, alert_type: AlertType) -> bool {
        self.alerts.iter().any(|a| std::mem::discriminant(&a.alert_type) == std::mem::discriminant(&alert_type))
    }
}

impl GeoOptimizer {
    fn new() -> Self {
        let mut region_data = HashMap::new();

        // Sample region data
        region_data.insert("us-east-1".to_string(), RegionCarbonProfile {
            region: "us-east-1".to_string(),
            avg_intensity: 350.0,
            current_intensity: 380.0,
            renewable_pct: 25.0,
            best_hours: vec![2, 3, 4, 5, 14, 15],
        });

        region_data.insert("us-west-2".to_string(), RegionCarbonProfile {
            region: "us-west-2".to_string(),
            avg_intensity: 120.0,
            current_intensity: 100.0,
            renewable_pct: 75.0,
            best_hours: vec![10, 11, 12, 13, 14, 15],
        });

        region_data.insert("eu-north-1".to_string(), RegionCarbonProfile {
            region: "eu-north-1".to_string(),
            avg_intensity: 50.0,
            current_intensity: 30.0,
            renewable_pct: 95.0,
            best_hours: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
        });

        Self {
            region_data,
            network_latency: HashMap::new(),
        }
    }
}

impl MockCarbonProvider {
    pub fn new() -> Self {
        let mut base_intensity = HashMap::new();
        base_intensity.insert("us-east-1".to_string(), 380.0);
        base_intensity.insert("us-west-2".to_string(), 120.0);
        base_intensity.insert("eu-west-1".to_string(), 250.0);
        base_intensity.insert("eu-north-1".to_string(), 30.0);
        base_intensity.insert("ap-northeast-1".to_string(), 450.0);

        Self { base_intensity }
    }
}

impl Default for MockCarbonProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl CarbonIntensityProvider for MockCarbonProvider {
    fn current_intensity(&self, region: &str) -> f64 {
        // Add some variation based on time of day
        let hour = (current_timestamp() / 3600) % 24;
        let base = self.base_intensity.get(region).copied().unwrap_or(300.0);

        // Higher during peak hours (9-21)
        let multiplier = if (9..21).contains(&hour) {
            1.0 + (hour as f64 - 15.0).abs() / 20.0
        } else {
            0.7 + (hour as f64 - 3.0).abs() / 30.0
        };

        base * multiplier
    }

    fn forecast(&self, region: &str, hours_ahead: u32) -> Vec<CarbonForecast> {
        let now = current_timestamp();
        let base = self.base_intensity.get(region).copied().unwrap_or(300.0);

        (0..hours_ahead)
            .map(|h| {
                let timestamp = now + (h as u64 * 3600);
                let hour = (timestamp / 3600) % 24;

                let multiplier = if (9..21).contains(&hour) {
                    1.0 + (hour as f64 - 15.0).abs() / 20.0
                } else {
                    0.7 + (hour as f64 - 3.0).abs() / 30.0
                };

                CarbonForecast {
                    timestamp,
                    intensity: base * multiplier,
                    confidence: 0.9 - (h as f64 * 0.02),
                }
            })
            .collect()
    }

    fn historical(&self, region: &str, hours_back: u32) -> Vec<CarbonReading> {
        let now = current_timestamp();
        let base = self.base_intensity.get(region).copied().unwrap_or(300.0);

        (0..hours_back)
            .map(|h| {
                let timestamp = now - (h as u64 * 3600);
                let hour = (timestamp / 3600) % 24;

                let multiplier = if (9..21).contains(&hour) {
                    1.0 + (hour as f64 - 15.0).abs() / 20.0
                } else {
                    0.7 + (hour as f64 - 3.0).abs() / 30.0
                };

                CarbonReading {
                    timestamp,
                    intensity: base * multiplier,
                    source: "mock".to_string(),
                }
            })
            .collect()
    }
}

/// Budget status
#[derive(Debug, Clone)]
pub struct BudgetStatus {
    pub monthly_budget: f64,
    pub current_usage: f64,
    pub remaining: f64,
    pub usage_percentage: f64,
    pub expected_percentage: f64,
    pub on_track: bool,
}

/// Queue status
#[derive(Debug, Clone)]
pub struct QueueStatus {
    pub urgent_count: usize,
    pub deferrable_count: usize,
    pub background_count: usize,
    pub batch_count: usize,
}

/// Emission estimate
#[derive(Debug, Clone)]
pub struct EmissionEstimate {
    pub immediate_emissions: f64,
    pub immediate_intensity: f64,
    pub best_region: String,
    pub best_region_emissions: f64,
    pub best_region_intensity: f64,
    pub best_time_emissions: f64,
    pub best_time_intensity: f64,
    pub best_time: Option<u64>,
}

/// Sustainability report
#[derive(Debug, Clone)]
pub struct SustainabilityReport {
    pub period_start: u64,
    pub total_operations: u64,
    pub total_emissions_kg: f64,
    pub carbon_saved_kg: f64,
    pub green_execution_pct: f64,
    pub avg_carbon_intensity: f64,
    pub deferred_operations: u64,
    pub shifted_operations: u64,
    pub budget_used_pct: f64,
}

/// Helper functions
fn uuid_v4() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:032x}", now)
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn days_in_period(start: u64) -> f64 {
    let now = current_timestamp();
    (now - start) as f64 / 86400.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_carbon_scheduler_creation() {
        let scheduler = CarbonScheduler::new()
            .with_region("us-west-2")
            .with_carbon_budget(500.0);

        let intensity = scheduler.current_intensity();
        assert!(intensity > 0.0);
    }

    #[test]
    fn test_queue_workload() {
        let scheduler = CarbonScheduler::new();

        let id = scheduler.queue_workload(
            WorkloadClass::Background,
            0.001,
            "backup-job",
        );

        assert!(!id.is_empty());

        let status = scheduler.queue_status();
        assert_eq!(status.background_count, 1);
    }

    #[test]
    fn test_execute_urgent() {
        let scheduler = CarbonScheduler::new();

        scheduler.queue_workload(WorkloadClass::Urgent, 0.001, "urgent-job");

        let results = scheduler.execute_urgent();

        assert_eq!(results.len(), 1);
        assert!(!results[0].was_deferred);
    }

    #[test]
    fn test_greenest_region() {
        let scheduler = CarbonScheduler::new()
            .with_region("us-east-1")
            .with_available_region("us-west-2")
            .with_available_region("eu-north-1");

        let (region, intensity) = scheduler.greenest_region();

        // eu-north-1 should typically be greenest
        assert!(!region.is_empty());
        assert!(intensity > 0.0);
    }

    #[test]
    fn test_emission_estimate() {
        let scheduler = CarbonScheduler::new()
            .with_region("us-east-1")
            .with_available_region("eu-north-1");

        let estimate = scheduler.estimate_emissions(1.0); // 1 kWh

        assert!(estimate.immediate_emissions > 0.0);
        assert!(estimate.best_region_emissions <= estimate.immediate_emissions);
    }

    #[test]
    fn test_budget_tracking() {
        let scheduler = CarbonScheduler::new()
            .with_carbon_budget(10.0); // 10 kg

        // Execute some workloads
        for _ in 0..5 {
            scheduler.queue_workload(WorkloadClass::Urgent, 0.01, "job");
        }
        scheduler.execute_urgent();

        let status = scheduler.budget_status();
        assert!(status.current_usage > 0.0);
        assert!(status.remaining < status.monthly_budget);
    }

    #[test]
    fn test_carbon_forecast() {
        let provider = MockCarbonProvider::new();
        let forecast = provider.forecast("us-west-2", 24);

        assert_eq!(forecast.len(), 24);
        assert!(forecast.iter().all(|f| f.intensity > 0.0));
        assert!(forecast.iter().all(|f| f.confidence > 0.0));
    }

    #[test]
    fn test_optimal_execution_time() {
        let scheduler = CarbonScheduler::new();

        let optimal = scheduler.optimal_execution_time(24);

        assert!(optimal.is_some());
        let (timestamp, intensity) = optimal.unwrap();
        assert!(timestamp > 0);
        assert!(intensity > 0.0);
    }

    #[test]
    fn test_sustainability_report() {
        let scheduler = CarbonScheduler::new();

        // Execute some workloads
        scheduler.queue_workload(WorkloadClass::Urgent, 0.001, "job1");
        scheduler.queue_workload(WorkloadClass::Urgent, 0.001, "job2");
        scheduler.execute_urgent();

        let report = scheduler.generate_report();

        assert_eq!(report.total_operations, 2);
        assert!(report.total_emissions_kg >= 0.0);
    }

    #[test]
    fn test_workload_deferral() {
        let scheduler = CarbonScheduler::new();

        // Queue deferrable work
        scheduler.queue_workload(
            WorkloadClass::Deferrable {
                deadline: Duration::from_secs(3600),
            },
            0.001,
            "deferrable-job",
        );

        let status = scheduler.queue_status();
        assert_eq!(status.deferrable_count, 1);

        // Try to execute (may or may not run depending on current intensity)
        let _ = scheduler.execute_scheduled();
    }

    #[test]
    fn test_batch_workloads() {
        let scheduler = CarbonScheduler::new();

        // Queue batch work
        for i in 0..5 {
            scheduler.queue_workload(
                WorkloadClass::Batch {
                    min_batch_size: 3,
                    max_wait: Duration::from_secs(60),
                },
                0.001,
                format!("batch-{}", i),
            );
        }

        // Each unique callback_id creates a separate batch
        let status = scheduler.queue_status();
        assert!(status.batch_count >= 1);
    }

    #[test]
    fn test_shiftable_workload() {
        let scheduler = CarbonScheduler::new()
            .with_region("us-east-1")
            .with_available_region("eu-north-1");

        scheduler.queue_workload(
            WorkloadClass::Shiftable {
                deadline: Duration::from_secs(3600),
                allowed_regions: vec!["eu-north-1".to_string()],
            },
            0.001,
            "shiftable-job",
        );

        let status = scheduler.queue_status();
        assert_eq!(status.deferrable_count, 1);
    }
}
