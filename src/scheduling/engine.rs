//! Job scheduling engine for periodic tasks.
//!
//! Provides a background scheduler that executes jobs based on cron expressions.

use super::cron::CronExpr;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

/// Unique identifier for a scheduled job.
pub type JobId = String;

/// Result of a job execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobResult {
    /// Job completed successfully.
    Success {
        message: Option<String>,
        duration_ms: u64,
    },
    /// Job failed with an error.
    Failed {
        error: String,
        duration_ms: u64,
    },
    /// Job was skipped (e.g., previous run still in progress).
    Skipped {
        reason: String,
    },
    /// Job timed out.
    Timeout {
        timeout_ms: u64,
    },
}

/// Current state of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    /// Job is scheduled and waiting.
    Scheduled,
    /// Job is currently running.
    Running,
    /// Job is paused.
    Paused,
    /// Job completed last run successfully.
    Completed,
    /// Job failed last run.
    Failed,
    /// Job is disabled.
    Disabled,
}

/// Statistics for a job.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobStats {
    /// Total number of runs.
    pub total_runs: u64,
    /// Number of successful runs.
    pub successful_runs: u64,
    /// Number of failed runs.
    pub failed_runs: u64,
    /// Number of skipped runs.
    pub skipped_runs: u64,
    /// Number of timeouts.
    pub timeouts: u64,
    /// Last run time.
    pub last_run: Option<DateTime<Utc>>,
    /// Last successful run time.
    pub last_success: Option<DateTime<Utc>>,
    /// Last failure time.
    pub last_failure: Option<DateTime<Utc>>,
    /// Last error message.
    pub last_error: Option<String>,
    /// Average run duration in milliseconds.
    pub avg_duration_ms: f64,
    /// Total run duration in milliseconds.
    total_duration_ms: u64,
}

impl JobStats {
    /// Record a job result.
    pub fn record(&mut self, result: &JobResult) {
        self.total_runs += 1;
        self.last_run = Some(Utc::now());

        match result {
            JobResult::Success { duration_ms, .. } => {
                self.successful_runs += 1;
                self.last_success = Some(Utc::now());
                self.total_duration_ms += duration_ms;
                self.avg_duration_ms =
                    self.total_duration_ms as f64 / self.successful_runs as f64;
            }
            JobResult::Failed { error, duration_ms } => {
                self.failed_runs += 1;
                self.last_failure = Some(Utc::now());
                self.last_error = Some(error.clone());
                self.total_duration_ms += duration_ms;
            }
            JobResult::Skipped { .. } => {
                self.skipped_runs += 1;
            }
            JobResult::Timeout { timeout_ms } => {
                self.timeouts += 1;
                self.last_failure = Some(Utc::now());
                self.last_error = Some(format!("Timeout after {}ms", timeout_ms));
            }
        }
    }

    /// Calculate success rate.
    pub fn success_rate(&self) -> f64 {
        if self.total_runs == 0 {
            return 0.0;
        }
        self.successful_runs as f64 / self.total_runs as f64
    }
}

/// Trait for job executors.
#[async_trait]
pub trait JobExecutor: Send + Sync {
    /// Execute the job.
    async fn execute(&self, job_id: &str) -> Result<String, String>;

    /// Get the job name.
    fn name(&self) -> &str;
}

/// A scheduled job definition.
pub struct ScheduledJob {
    /// Job ID.
    pub id: JobId,
    /// Job name.
    pub name: String,
    /// Cron schedule.
    pub schedule: CronExpr,
    /// Job executor.
    pub executor: Arc<dyn JobExecutor>,
    /// Timeout for the job.
    pub timeout: Option<Duration>,
    /// Whether concurrent runs are allowed.
    pub allow_concurrent: bool,
    /// Job state.
    pub state: JobState,
    /// Next scheduled run.
    pub next_run: Option<DateTime<Utc>>,
    /// Job statistics.
    pub stats: JobStats,
    /// Tags for categorization.
    pub tags: Vec<String>,
    /// Whether the job is enabled.
    pub enabled: bool,
}

impl ScheduledJob {
    /// Create a new scheduled job.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        schedule: CronExpr,
        executor: Arc<dyn JobExecutor>,
    ) -> Self {
        let now = Utc::now();
        let next_run = schedule.next_run(&now);

        Self {
            id: id.into(),
            name: name.into(),
            schedule,
            executor,
            timeout: Some(Duration::from_secs(3600)), // 1 hour default
            allow_concurrent: false,
            state: JobState::Scheduled,
            next_run,
            stats: JobStats::default(),
            tags: Vec::new(),
            enabled: true,
        }
    }

    /// Set the timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Allow concurrent runs.
    pub fn allow_concurrent(mut self) -> Self {
        self.allow_concurrent = true;
        self
    }

    /// Add a tag.
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Disable the job.
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self.state = JobState::Disabled;
        self
    }

    /// Update the next run time.
    pub fn update_next_run(&mut self) {
        self.next_run = self.schedule.next_run(&Utc::now());
    }
}

/// A job event.
#[derive(Debug, Clone)]
pub enum JobEvent {
    /// Job started.
    Started { job_id: JobId, at: DateTime<Utc> },
    /// Job completed.
    Completed {
        job_id: JobId,
        result: JobResult,
        at: DateTime<Utc>,
    },
    /// Job was scheduled.
    Scheduled {
        job_id: JobId,
        next_run: DateTime<Utc>,
    },
    /// Job was paused.
    Paused { job_id: JobId },
    /// Job was resumed.
    Resumed { job_id: JobId },
}

/// Job wrapper for storage (without executor).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Job ID.
    pub id: JobId,
    /// Job name.
    pub name: String,
    /// Cron expression string.
    pub schedule: String,
    /// Timeout in seconds.
    pub timeout_secs: Option<u64>,
    /// Whether concurrent runs are allowed.
    pub allow_concurrent: bool,
    /// Job state.
    pub state: JobState,
    /// Next scheduled run.
    pub next_run: Option<DateTime<Utc>>,
    /// Job statistics.
    pub stats: JobStats,
    /// Tags.
    pub tags: Vec<String>,
    /// Whether enabled.
    pub enabled: bool,
}

impl From<&ScheduledJob> for Job {
    fn from(job: &ScheduledJob) -> Self {
        Self {
            id: job.id.clone(),
            name: job.name.clone(),
            schedule: job.schedule.expr.clone(),
            timeout_secs: job.timeout.map(|d| d.as_secs()),
            allow_concurrent: job.allow_concurrent,
            state: job.state,
            next_run: job.next_run,
            stats: job.stats.clone(),
            tags: job.tags.clone(),
            enabled: job.enabled,
        }
    }
}

/// Job scheduler that manages and executes scheduled jobs.
pub struct JobScheduler {
    /// Registered jobs.
    jobs: Arc<RwLock<HashMap<JobId, ScheduledJob>>>,
    /// Currently running jobs.
    running: Arc<RwLock<HashMap<JobId, tokio::task::JoinHandle<()>>>>,
    /// Event broadcaster.
    event_tx: broadcast::Sender<JobEvent>,
    /// Check interval for scheduler.
    check_interval: Duration,
    /// Counter for job runs.
    run_counter: AtomicU64,
}

impl JobScheduler {
    /// Create a new job scheduler.
    pub fn new() -> Self {
        let (event_tx, _) = broadcast::channel(100);

        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            check_interval: Duration::from_secs(1),
            run_counter: AtomicU64::new(0),
        }
    }

    /// Set the check interval.
    pub fn with_check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    /// Subscribe to job events.
    pub fn subscribe(&self) -> broadcast::Receiver<JobEvent> {
        self.event_tx.subscribe()
    }

    /// Register a job.
    pub async fn register(&self, job: ScheduledJob) {
        info!(
            job_id = %job.id,
            name = %job.name,
            schedule = %job.schedule.expr,
            next_run = ?job.next_run,
            "Registered job"
        );

        if let Some(next) = job.next_run {
            let _ = self.event_tx.send(JobEvent::Scheduled {
                job_id: job.id.clone(),
                next_run: next,
            });
        }

        self.jobs.write().await.insert(job.id.clone(), job);
    }

    /// Remove a job.
    pub async fn remove(&self, job_id: &str) -> Option<Job> {
        let job = self.jobs.write().await.remove(job_id);
        if let Some(ref j) = job {
            info!(job_id, "Removed job");
            return Some(Job::from(j));
        }
        None
    }

    /// Get all jobs.
    pub async fn list_jobs(&self) -> Vec<Job> {
        self.jobs
            .read()
            .await
            .values()
            .map(Job::from)
            .collect()
    }

    /// Get a specific job.
    pub async fn get_job(&self, job_id: &str) -> Option<Job> {
        self.jobs.read().await.get(job_id).map(Job::from)
    }

    /// Pause a job.
    pub async fn pause(&self, job_id: &str) -> bool {
        let mut jobs = self.jobs.write().await;
        if let Some(job) = jobs.get_mut(job_id) {
            job.state = JobState::Paused;
            let _ = self.event_tx.send(JobEvent::Paused {
                job_id: job_id.to_string(),
            });
            info!(job_id, "Paused job");
            return true;
        }
        false
    }

    /// Resume a paused job.
    pub async fn resume(&self, job_id: &str) -> bool {
        let mut jobs = self.jobs.write().await;
        if let Some(job) = jobs.get_mut(job_id) {
            if job.state == JobState::Paused {
                job.state = JobState::Scheduled;
                job.update_next_run();
                let _ = self.event_tx.send(JobEvent::Resumed {
                    job_id: job_id.to_string(),
                });
                info!(job_id, "Resumed job");
                return true;
            }
        }
        false
    }

    /// Trigger a job immediately.
    pub async fn trigger(&self, job_id: &str) -> Result<(), String> {
        let jobs = self.jobs.read().await;
        let job = jobs.get(job_id).ok_or_else(|| "Job not found".to_string())?;

        if !job.enabled {
            return Err("Job is disabled".to_string());
        }

        let executor = job.executor.clone();
        let timeout = job.timeout;
        let job_id_owned = job_id.to_string();

        drop(jobs);

        self.execute_job(&job_id_owned, executor, timeout).await;
        Ok(())
    }

    /// Execute a job.
    async fn execute_job(
        &self,
        job_id: &str,
        executor: Arc<dyn JobExecutor>,
        timeout: Option<Duration>,
    ) {
        let run_id = self.run_counter.fetch_add(1, Ordering::SeqCst);
        let start = std::time::Instant::now();
        let now = Utc::now();

        info!(job_id, run_id, "Starting job execution");

        let _ = self.event_tx.send(JobEvent::Started {
            job_id: job_id.to_string(),
            at: now,
        });

        // Update state to running
        {
            let mut jobs = self.jobs.write().await;
            if let Some(job) = jobs.get_mut(job_id) {
                job.state = JobState::Running;
            }
        }

        // Execute with optional timeout
        let result = if let Some(timeout_duration) = timeout {
            match tokio::time::timeout(timeout_duration, executor.execute(job_id)).await {
                Ok(Ok(msg)) => JobResult::Success {
                    message: Some(msg),
                    duration_ms: start.elapsed().as_millis() as u64,
                },
                Ok(Err(e)) => JobResult::Failed {
                    error: e,
                    duration_ms: start.elapsed().as_millis() as u64,
                },
                Err(_) => JobResult::Timeout {
                    timeout_ms: timeout_duration.as_millis() as u64,
                },
            }
        } else {
            match executor.execute(job_id).await {
                Ok(msg) => JobResult::Success {
                    message: Some(msg),
                    duration_ms: start.elapsed().as_millis() as u64,
                },
                Err(e) => JobResult::Failed {
                    error: e,
                    duration_ms: start.elapsed().as_millis() as u64,
                },
            }
        };

        // Update job state and stats
        {
            let mut jobs = self.jobs.write().await;
            if let Some(job) = jobs.get_mut(job_id) {
                job.stats.record(&result);
                job.state = match &result {
                    JobResult::Success { .. } => JobState::Completed,
                    JobResult::Failed { .. } | JobResult::Timeout { .. } => JobState::Failed,
                    JobResult::Skipped { .. } => JobState::Scheduled,
                };
                job.update_next_run();

                match &result {
                    JobResult::Success { message, duration_ms } => {
                        info!(
                            job_id,
                            run_id,
                            duration_ms,
                            message = message.as_deref().unwrap_or(""),
                            "Job completed successfully"
                        );
                    }
                    JobResult::Failed { error, duration_ms } => {
                        error!(
                            job_id,
                            run_id,
                            duration_ms,
                            error,
                            "Job failed"
                        );
                    }
                    JobResult::Timeout { timeout_ms } => {
                        warn!(
                            job_id,
                            run_id,
                            timeout_ms,
                            "Job timed out"
                        );
                    }
                    JobResult::Skipped { reason } => {
                        debug!(job_id, run_id, reason, "Job skipped");
                    }
                }
            }
        }

        let _ = self.event_tx.send(JobEvent::Completed {
            job_id: job_id.to_string(),
            result,
            at: Utc::now(),
        });
    }

    /// Run the scheduler loop.
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) {
        info!("Job scheduler started");
        let mut interval = tokio::time::interval(self.check_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.check_and_execute().await;
                }
                _ = shutdown.recv() => {
                    info!("Job scheduler shutting down");
                    break;
                }
            }
        }

        // Wait for running jobs to complete
        let running = self.running.write().await;
        for (job_id, handle) in running.iter() {
            debug!(job_id, "Waiting for job to complete");
            let _ = handle;
        }
    }

    /// Check for due jobs and execute them.
    async fn check_and_execute(&self) {
        let now = Utc::now();
        let mut to_execute = Vec::new();

        // Find jobs that are due
        {
            let jobs = self.jobs.read().await;
            for (job_id, job) in jobs.iter() {
                if !job.enabled || job.state == JobState::Paused || job.state == JobState::Disabled {
                    continue;
                }

                if let Some(next_run) = job.next_run {
                    if next_run <= now {
                        // Check if already running
                        if !job.allow_concurrent && job.state == JobState::Running {
                            debug!(job_id, "Job already running, skipping");
                            continue;
                        }

                        to_execute.push((
                            job_id.clone(),
                            job.executor.clone(),
                            job.timeout,
                        ));
                    }
                }
            }
        }

        // Execute due jobs
        for (job_id, executor, timeout) in to_execute {
            self.execute_job(&job_id, executor, timeout).await;
        }
    }
}

impl Default for JobScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple executor that runs a closure.
pub struct ClosureExecutor<F>
where
    F: Fn(&str) -> Result<String, String> + Send + Sync,
{
    name: String,
    func: F,
}

impl<F> ClosureExecutor<F>
where
    F: Fn(&str) -> Result<String, String> + Send + Sync,
{
    /// Create a new closure executor.
    pub fn new(name: impl Into<String>, func: F) -> Self {
        Self {
            name: name.into(),
            func,
        }
    }
}

#[async_trait]
impl<F> JobExecutor for ClosureExecutor<F>
where
    F: Fn(&str) -> Result<String, String> + Send + Sync,
{
    async fn execute(&self, job_id: &str) -> Result<String, String> {
        (self.func)(job_id)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestExecutor {
        should_fail: bool,
    }

    #[async_trait]
    impl JobExecutor for TestExecutor {
        async fn execute(&self, _job_id: &str) -> Result<String, String> {
            if self.should_fail {
                Err("Test failure".to_string())
            } else {
                Ok("Test success".to_string())
            }
        }

        fn name(&self) -> &str {
            "test_executor"
        }
    }

    #[test]
    fn test_job_stats() {
        let mut stats = JobStats::default();

        stats.record(&JobResult::Success {
            message: Some("ok".to_string()),
            duration_ms: 100,
        });

        assert_eq!(stats.total_runs, 1);
        assert_eq!(stats.successful_runs, 1);
        assert_eq!(stats.success_rate(), 1.0);

        stats.record(&JobResult::Failed {
            error: "error".to_string(),
            duration_ms: 50,
        });

        assert_eq!(stats.total_runs, 2);
        assert_eq!(stats.failed_runs, 1);
        assert_eq!(stats.success_rate(), 0.5);
    }

    #[test]
    fn test_job_result() {
        let success = JobResult::Success {
            message: Some("done".to_string()),
            duration_ms: 100,
        };

        match success {
            JobResult::Success { duration_ms, .. } => assert_eq!(duration_ms, 100),
            _ => panic!("Expected success"),
        }
    }

    #[tokio::test]
    async fn test_scheduler_register() {
        let scheduler = JobScheduler::new();
        let executor = Arc::new(TestExecutor { should_fail: false });
        let schedule = super::super::cron::presets::every_minute();

        let job = ScheduledJob::new("test", "Test Job", schedule, executor);
        scheduler.register(job).await;

        let jobs = scheduler.list_jobs().await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, "test");
    }

    #[tokio::test]
    async fn test_scheduler_trigger() {
        let scheduler = JobScheduler::new();
        let executor = Arc::new(TestExecutor { should_fail: false });
        let schedule = super::super::cron::presets::daily(); // Won't run automatically

        let job = ScheduledJob::new("test", "Test Job", schedule, executor);
        scheduler.register(job).await;

        // Trigger manually
        scheduler.trigger("test").await.unwrap();

        // Check stats
        let job = scheduler.get_job("test").await.unwrap();
        assert_eq!(job.stats.total_runs, 1);
        assert_eq!(job.stats.successful_runs, 1);
    }

    #[tokio::test]
    async fn test_scheduler_pause_resume() {
        let scheduler = JobScheduler::new();
        let executor = Arc::new(TestExecutor { should_fail: false });
        let schedule = super::super::cron::presets::every_minute();

        let job = ScheduledJob::new("test", "Test Job", schedule, executor);
        scheduler.register(job).await;

        // Pause
        assert!(scheduler.pause("test").await);
        let job = scheduler.get_job("test").await.unwrap();
        assert_eq!(job.state, JobState::Paused);

        // Resume
        assert!(scheduler.resume("test").await);
        let job = scheduler.get_job("test").await.unwrap();
        assert_eq!(job.state, JobState::Scheduled);
    }
}
