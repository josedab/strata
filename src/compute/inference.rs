// ML Inference Engine

use super::model::{DataType, Model, ModelFormat, PreprocessStep, PostprocessStep};
use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};

/// Inference request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceRequest {
    /// Request ID
    pub id: String,
    /// Model name
    pub model: String,
    /// Model version (None for latest)
    pub version: Option<u32>,
    /// Input tensors
    pub inputs: HashMap<String, TensorData>,
    /// Parameters
    pub parameters: InferenceParameters,
    /// Priority
    pub priority: RequestPriority,
    /// Timestamp
    pub timestamp: u64,
}

impl InferenceRequest {
    /// Creates a new request
    pub fn new(model: &str) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            model: model.to_string(),
            version: None,
            inputs: HashMap::new(),
            parameters: InferenceParameters::default(),
            priority: RequestPriority::Normal,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Sets version
    pub fn with_version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    /// Adds input tensor
    pub fn with_input(mut self, name: &str, data: TensorData) -> Self {
        self.inputs.insert(name.to_string(), data);
        self
    }

    /// Sets priority
    pub fn with_priority(mut self, priority: RequestPriority) -> Self {
        self.priority = priority;
        self
    }
}

/// Tensor data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TensorData {
    /// Data type
    pub dtype: DataType,
    /// Shape
    pub shape: Vec<i64>,
    /// Raw data bytes
    pub data: Vec<u8>,
}

impl TensorData {
    /// Creates from f32 slice
    pub fn from_f32(data: &[f32], shape: Vec<i64>) -> Self {
        let bytes: Vec<u8> = data.iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        Self {
            dtype: DataType::Float32,
            shape,
            data: bytes,
        }
    }

    /// Creates from f64 slice
    pub fn from_f64(data: &[f64], shape: Vec<i64>) -> Self {
        let bytes: Vec<u8> = data.iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        Self {
            dtype: DataType::Float64,
            shape,
            data: bytes,
        }
    }

    /// Creates from i32 slice
    pub fn from_i32(data: &[i32], shape: Vec<i64>) -> Self {
        let bytes: Vec<u8> = data.iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        Self {
            dtype: DataType::Int32,
            shape,
            data: bytes,
        }
    }

    /// Creates from i64 slice
    pub fn from_i64(data: &[i64], shape: Vec<i64>) -> Self {
        let bytes: Vec<u8> = data.iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        Self {
            dtype: DataType::Int64,
            shape,
            data: bytes,
        }
    }

    /// Gets as f32 slice
    pub fn as_f32(&self) -> Option<Vec<f32>> {
        if self.dtype != DataType::Float32 {
            return None;
        }
        Some(
            self.data
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect()
        )
    }

    /// Gets as f64 slice
    pub fn as_f64(&self) -> Option<Vec<f64>> {
        if self.dtype != DataType::Float64 {
            return None;
        }
        Some(
            self.data
                .chunks_exact(8)
                .map(|chunk| {
                    f64::from_le_bytes([
                        chunk[0], chunk[1], chunk[2], chunk[3],
                        chunk[4], chunk[5], chunk[6], chunk[7],
                    ])
                })
                .collect()
        )
    }

    /// Gets element count
    pub fn element_count(&self) -> usize {
        self.shape.iter().map(|&d| d.max(1) as usize).product()
    }
}

/// Inference parameters
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InferenceParameters {
    /// Timeout in milliseconds
    pub timeout_ms: Option<u64>,
    /// Return raw outputs
    pub raw_output: bool,
    /// Output tensor names to return
    pub output_names: Option<Vec<String>>,
    /// Enable profiling
    pub profile: bool,
    /// Custom parameters
    pub custom: HashMap<String, serde_json::Value>,
}

/// Request priority
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestPriority {
    Low,
    Normal,
    High,
    Critical,
}

/// Inference response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResponse {
    /// Request ID
    pub request_id: String,
    /// Model name
    pub model: String,
    /// Model version
    pub version: u32,
    /// Output tensors
    pub outputs: HashMap<String, TensorData>,
    /// Inference time in microseconds
    pub inference_time_us: u64,
    /// Total time including pre/post processing
    pub total_time_us: u64,
    /// Profiling data
    pub profile: Option<ProfileData>,
    /// Status
    pub status: InferenceStatus,
    /// Error message if failed
    pub error: Option<String>,
}

/// Inference status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InferenceStatus {
    Success,
    Failed,
    Timeout,
    Cancelled,
}

/// Profiling data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileData {
    /// Preprocessing time
    pub preprocess_us: u64,
    /// Inference time
    pub inference_us: u64,
    /// Postprocessing time
    pub postprocess_us: u64,
    /// Queue wait time
    pub queue_wait_us: u64,
    /// Memory usage bytes
    pub memory_bytes: u64,
    /// GPU memory bytes
    pub gpu_memory_bytes: Option<u64>,
}

/// Inference engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceConfig {
    /// Maximum concurrent requests
    pub max_concurrent: usize,
    /// Request timeout ms
    pub timeout_ms: u64,
    /// Enable batching
    pub enable_batching: bool,
    /// Batch timeout ms
    pub batch_timeout_ms: u64,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Worker threads
    pub worker_threads: usize,
    /// Enable GPU
    pub enable_gpu: bool,
    /// GPU device IDs
    pub gpu_devices: Vec<u32>,
    /// Memory pool size MB
    pub memory_pool_mb: u64,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 100,
            timeout_ms: 30000,
            enable_batching: true,
            batch_timeout_ms: 50,
            max_batch_size: 32,
            worker_threads: 4,
            enable_gpu: false,
            gpu_devices: Vec::new(),
            memory_pool_mb: 1024,
        }
    }
}

/// Inference engine
pub struct InferenceEngine {
    /// Configuration
    config: InferenceConfig,
    /// Loaded models
    models: Arc<RwLock<HashMap<String, LoadedModel>>>,
    /// Request semaphore
    semaphore: Arc<Semaphore>,
    /// Request queue for batching
    batch_queue: Arc<RwLock<Vec<BatchedRequest>>>,
    /// Statistics
    stats: Arc<InferenceStats>,
    /// Running flag
    running: Arc<RwLock<bool>>,
}

/// Loaded model
struct LoadedModel {
    /// Model info
    model: Model,
    /// Version
    version: u32,
    /// Runtime handle (placeholder for actual runtime)
    runtime: ModelRuntime,
    /// Load time
    loaded_at: u64,
    /// Inference count
    inference_count: AtomicU64,
}

/// Model runtime (placeholder)
enum ModelRuntime {
    /// ONNX Runtime
    Onnx(OnnxRuntime),
    /// TensorFlow
    TensorFlow(TensorFlowRuntime),
    /// PyTorch
    PyTorch(PyTorchRuntime),
    /// Generic/Custom
    Generic(GenericRuntime),
}

/// ONNX Runtime placeholder
struct OnnxRuntime {
    /// Session configuration
    config: HashMap<String, String>,
}

/// TensorFlow runtime placeholder
struct TensorFlowRuntime {
    /// Session options
    options: HashMap<String, String>,
}

/// PyTorch runtime placeholder
struct PyTorchRuntime {
    /// Device
    device: String,
}

/// Generic runtime placeholder
struct GenericRuntime {
    /// Format
    format: ModelFormat,
}

/// Batched request
struct BatchedRequest {
    request: InferenceRequest,
    response_tx: tokio::sync::oneshot::Sender<InferenceResponse>,
    queued_at: std::time::Instant,
}

/// Inference statistics
pub struct InferenceStats {
    /// Total requests
    pub total_requests: AtomicU64,
    /// Successful requests
    pub successful_requests: AtomicU64,
    /// Failed requests
    pub failed_requests: AtomicU64,
    /// Timeout requests
    pub timeout_requests: AtomicU64,
    /// Total inference time us
    pub total_inference_time_us: AtomicU64,
    /// Total preprocessing time us
    pub total_preprocess_time_us: AtomicU64,
    /// Total postprocessing time us
    pub total_postprocess_time_us: AtomicU64,
    /// Batches processed
    pub batches_processed: AtomicU64,
    /// Total batch size
    pub total_batch_size: AtomicU64,
}

impl Default for InferenceStats {
    fn default() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            timeout_requests: AtomicU64::new(0),
            total_inference_time_us: AtomicU64::new(0),
            total_preprocess_time_us: AtomicU64::new(0),
            total_postprocess_time_us: AtomicU64::new(0),
            batches_processed: AtomicU64::new(0),
            total_batch_size: AtomicU64::new(0),
        }
    }
}

impl InferenceEngine {
    /// Creates a new inference engine
    pub fn new(config: InferenceConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent));
        Self {
            config,
            models: Arc::new(RwLock::new(HashMap::new())),
            semaphore,
            batch_queue: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(InferenceStats::default()),
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Starts the engine
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;

        // Start batch processor if batching enabled
        if self.config.enable_batching {
            let batch_queue = Arc::clone(&self.batch_queue);
            let models = Arc::clone(&self.models);
            let stats = Arc::clone(&self.stats);
            let batch_timeout = self.config.batch_timeout_ms;
            let max_batch_size = self.config.max_batch_size;
            let running = Arc::clone(&self.running);

            tokio::spawn(async move {
                Self::batch_processor(
                    batch_queue,
                    models,
                    stats,
                    batch_timeout,
                    max_batch_size,
                    running,
                ).await;
            });
        }

        Ok(())
    }

    /// Stops the engine
    pub async fn stop(&self) -> Result<()> {
        let mut running = self.running.write().await;
        *running = false;
        Ok(())
    }

    /// Loads a model
    pub async fn load_model(&self, model: Model, version: u32) -> Result<()> {
        let name = model.metadata.name.clone();
        let format = model.config.format;

        // Create runtime based on format
        let runtime = match format {
            ModelFormat::Onnx => ModelRuntime::Onnx(OnnxRuntime {
                config: HashMap::new(),
            }),
            ModelFormat::TensorFlow => ModelRuntime::TensorFlow(TensorFlowRuntime {
                options: HashMap::new(),
            }),
            ModelFormat::PyTorch => ModelRuntime::PyTorch(PyTorchRuntime {
                device: if self.config.enable_gpu { "cuda".to_string() } else { "cpu".to_string() },
            }),
            _ => ModelRuntime::Generic(GenericRuntime { format }),
        };

        let loaded = LoadedModel {
            model,
            version,
            runtime,
            loaded_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            inference_count: AtomicU64::new(0),
        };

        let key = format!("{}:v{}", name, version);
        let mut models = self.models.write().await;
        models.insert(key, loaded);

        Ok(())
    }

    /// Unloads a model
    pub async fn unload_model(&self, name: &str, version: u32) -> Result<bool> {
        let key = format!("{}:v{}", name, version);
        let mut models = self.models.write().await;
        Ok(models.remove(&key).is_some())
    }

    /// Runs inference
    pub async fn infer(&self, request: InferenceRequest) -> Result<InferenceResponse> {
        let start = std::time::Instant::now();
        self.stats.total_requests.fetch_add(1, Ordering::Relaxed);

        // Acquire semaphore permit
        let _permit = self.semaphore.acquire().await
            .map_err(|_| StrataError::InvalidOperation("Engine shutting down".to_string()))?;

        // Find model
        let version = request.version.unwrap_or(1);
        let key = format!("{}:v{}", request.model, version);

        let models = self.models.read().await;
        let loaded = models.get(&key).ok_or_else(|| {
            StrataError::NotFound(format!("Model {} version {} not loaded", request.model, version))
        })?;

        // Validate inputs
        self.validate_inputs(&request, &loaded.model)?;

        // Run inference
        let inference_start = std::time::Instant::now();
        let outputs = self.run_inference(&request, loaded).await?;
        let inference_time = inference_start.elapsed();

        loaded.inference_count.fetch_add(1, Ordering::Relaxed);

        let total_time = start.elapsed();

        self.stats.successful_requests.fetch_add(1, Ordering::Relaxed);
        self.stats.total_inference_time_us.fetch_add(
            inference_time.as_micros() as u64,
            Ordering::Relaxed
        );

        Ok(InferenceResponse {
            request_id: request.id,
            model: request.model,
            version,
            outputs,
            inference_time_us: inference_time.as_micros() as u64,
            total_time_us: total_time.as_micros() as u64,
            profile: if request.parameters.profile {
                Some(ProfileData {
                    preprocess_us: 0,
                    inference_us: inference_time.as_micros() as u64,
                    postprocess_us: 0,
                    queue_wait_us: 0,
                    memory_bytes: 0,
                    gpu_memory_bytes: None,
                })
            } else {
                None
            },
            status: InferenceStatus::Success,
            error: None,
        })
    }

    /// Validates request inputs
    fn validate_inputs(&self, request: &InferenceRequest, model: &Model) -> Result<()> {
        for input_spec in &model.config.inputs {
            if !request.inputs.contains_key(&input_spec.name) {
                return Err(StrataError::InvalidOperation(
                    format!("Missing required input: {}", input_spec.name)
                ));
            }

            let tensor = &request.inputs[&input_spec.name];

            // Check data type
            if tensor.dtype != input_spec.dtype {
                return Err(StrataError::InvalidOperation(
                    format!(
                        "Input {} has wrong dtype: expected {:?}, got {:?}",
                        input_spec.name, input_spec.dtype, tensor.dtype
                    )
                ));
            }

            // Check shape (ignoring dynamic dimensions marked with -1)
            if tensor.shape.len() != input_spec.shape.len() {
                return Err(StrataError::InvalidOperation(
                    format!(
                        "Input {} has wrong rank: expected {}, got {}",
                        input_spec.name, input_spec.shape.len(), tensor.shape.len()
                    )
                ));
            }

            for (i, (&expected, &actual)) in input_spec.shape.iter().zip(tensor.shape.iter()).enumerate() {
                if expected != -1 && expected != actual {
                    return Err(StrataError::InvalidOperation(
                        format!(
                            "Input {} dimension {} mismatch: expected {}, got {}",
                            input_spec.name, i, expected, actual
                        )
                    ));
                }
            }
        }

        Ok(())
    }

    /// Runs inference on loaded model
    async fn run_inference(
        &self,
        request: &InferenceRequest,
        loaded: &LoadedModel,
    ) -> Result<HashMap<String, TensorData>> {
        // Apply preprocessing
        let processed_inputs = self.preprocess(&request.inputs, &loaded.model.config.preprocessing)?;

        // Run model inference (placeholder - actual implementation would use runtime)
        let raw_outputs = self.execute_model(loaded, &processed_inputs).await?;

        // Apply postprocessing
        let outputs = self.postprocess(&raw_outputs, &loaded.model.config.postprocessing)?;

        Ok(outputs)
    }

    /// Applies preprocessing steps
    fn preprocess(
        &self,
        inputs: &HashMap<String, TensorData>,
        steps: &[PreprocessStep],
    ) -> Result<HashMap<String, TensorData>> {
        let mut result = inputs.clone();

        for step in steps {
            result = self.apply_preprocess_step(&result, step)?;
        }

        Ok(result)
    }

    /// Applies a single preprocessing step
    fn apply_preprocess_step(
        &self,
        inputs: &HashMap<String, TensorData>,
        step: &PreprocessStep,
    ) -> Result<HashMap<String, TensorData>> {
        match step {
            PreprocessStep::Normalize { mean, std } => {
                let mut result = HashMap::new();
                for (name, tensor) in inputs {
                    if tensor.dtype == DataType::Float32 {
                        if let Some(data) = tensor.as_f32() {
                            let normalized: Vec<f32> = data.iter().enumerate()
                                .map(|(i, &v)| {
                                    let c = i % mean.len();
                                    (v - mean[c]) / std[c]
                                })
                                .collect();
                            result.insert(name.clone(), TensorData::from_f32(&normalized, tensor.shape.clone()));
                        } else {
                            result.insert(name.clone(), tensor.clone());
                        }
                    } else {
                        result.insert(name.clone(), tensor.clone());
                    }
                }
                Ok(result)
            }
            PreprocessStep::ToTensor => {
                // Data is already tensor format
                Ok(inputs.clone())
            }
            _ => {
                // Other steps would need actual image/text processing
                Ok(inputs.clone())
            }
        }
    }

    /// Executes model (placeholder)
    async fn execute_model(
        &self,
        loaded: &LoadedModel,
        _inputs: &HashMap<String, TensorData>,
    ) -> Result<HashMap<String, TensorData>> {
        // In a real implementation, this would:
        // 1. Convert inputs to runtime-specific format
        // 2. Run the actual model inference
        // 3. Convert outputs back to TensorData

        // Placeholder: create dummy outputs based on model spec
        let mut outputs = HashMap::new();
        for output_spec in &loaded.model.config.outputs {
            // Create output with correct shape (replace -1 with 1 for batch)
            let shape: Vec<i64> = output_spec.shape.iter()
                .map(|&d| if d == -1 { 1 } else { d })
                .collect();
            let size = shape.iter().map(|&d| d as usize).product::<usize>();

            let data = match output_spec.dtype {
                DataType::Float32 => TensorData::from_f32(&vec![0.0f32; size], shape),
                DataType::Float64 => TensorData::from_f64(&vec![0.0f64; size], shape),
                DataType::Int32 => TensorData::from_i32(&vec![0i32; size], shape),
                DataType::Int64 => TensorData::from_i64(&vec![0i64; size], shape),
                _ => TensorData::from_f32(&vec![0.0f32; size], shape),
            };

            outputs.insert(output_spec.name.clone(), data);
        }

        Ok(outputs)
    }

    /// Applies postprocessing steps
    fn postprocess(
        &self,
        outputs: &HashMap<String, TensorData>,
        steps: &[PostprocessStep],
    ) -> Result<HashMap<String, TensorData>> {
        let mut result = outputs.clone();

        for step in steps {
            result = self.apply_postprocess_step(&result, step)?;
        }

        Ok(result)
    }

    /// Applies a single postprocessing step
    fn apply_postprocess_step(
        &self,
        outputs: &HashMap<String, TensorData>,
        step: &PostprocessStep,
    ) -> Result<HashMap<String, TensorData>> {
        match step {
            PostprocessStep::Softmax { axis } => {
                let mut result = HashMap::new();
                for (name, tensor) in outputs {
                    if tensor.dtype == DataType::Float32 {
                        if let Some(data) = tensor.as_f32() {
                            let softmax = self.compute_softmax(&data, &tensor.shape, *axis);
                            result.insert(name.clone(), TensorData::from_f32(&softmax, tensor.shape.clone()));
                        } else {
                            result.insert(name.clone(), tensor.clone());
                        }
                    } else {
                        result.insert(name.clone(), tensor.clone());
                    }
                }
                Ok(result)
            }
            PostprocessStep::Argmax { axis: _ } => {
                // Placeholder
                Ok(outputs.clone())
            }
            PostprocessStep::TopK { k: _ } => {
                // Placeholder
                Ok(outputs.clone())
            }
            _ => Ok(outputs.clone()),
        }
    }

    /// Computes softmax
    fn compute_softmax(&self, data: &[f32], shape: &[i64], axis: i32) -> Vec<f32> {
        let axis_idx = if axis < 0 {
            (shape.len() as i32 + axis) as usize
        } else {
            axis as usize
        };

        if axis_idx >= shape.len() {
            return data.to_vec();
        }

        let axis_size = shape[axis_idx] as usize;
        let outer_size: usize = shape[..axis_idx].iter().map(|&d| d as usize).product();
        let inner_size: usize = shape[axis_idx + 1..].iter().map(|&d| d as usize).product();

        let mut result = vec![0.0f32; data.len()];

        for outer in 0..outer_size.max(1) {
            for inner in 0..inner_size.max(1) {
                // Find max for numerical stability
                let mut max_val = f32::NEG_INFINITY;
                for ax in 0..axis_size {
                    let idx = outer * axis_size * inner_size.max(1) + ax * inner_size.max(1) + inner;
                    if idx < data.len() {
                        max_val = max_val.max(data[idx]);
                    }
                }

                // Compute exp and sum
                let mut sum = 0.0f32;
                for ax in 0..axis_size {
                    let idx = outer * axis_size * inner_size.max(1) + ax * inner_size.max(1) + inner;
                    if idx < data.len() {
                        let exp_val = (data[idx] - max_val).exp();
                        result[idx] = exp_val;
                        sum += exp_val;
                    }
                }

                // Normalize
                for ax in 0..axis_size {
                    let idx = outer * axis_size * inner_size.max(1) + ax * inner_size.max(1) + inner;
                    if idx < result.len() {
                        result[idx] /= sum;
                    }
                }
            }
        }

        result
    }

    /// Batch processor loop
    async fn batch_processor(
        batch_queue: Arc<RwLock<Vec<BatchedRequest>>>,
        _models: Arc<RwLock<HashMap<String, LoadedModel>>>,
        stats: Arc<InferenceStats>,
        batch_timeout_ms: u64,
        max_batch_size: usize,
        running: Arc<RwLock<bool>>,
    ) {
        let batch_timeout = std::time::Duration::from_millis(batch_timeout_ms);

        loop {
            // Check if still running
            {
                let running = running.read().await;
                if !*running {
                    break;
                }
            }

            // Collect batch
            let batch = {
                let mut queue = batch_queue.write().await;
                if queue.is_empty() {
                    drop(queue);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }

                // Check if oldest request has timed out or batch is full
                let should_process = queue.len() >= max_batch_size
                    || queue.first().map(|r| r.queued_at.elapsed() >= batch_timeout).unwrap_or(false);

                if should_process {
                    let batch_size = queue.len().min(max_batch_size);
                    queue.drain(..batch_size).collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            };

            if batch.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                continue;
            }

            // Process batch
            stats.batches_processed.fetch_add(1, Ordering::Relaxed);
            stats.total_batch_size.fetch_add(batch.len() as u64, Ordering::Relaxed);

            // Group by model
            let mut by_model: HashMap<String, Vec<BatchedRequest>> = HashMap::new();
            for req in batch {
                let key = format!("{}:v{}", req.request.model, req.request.version.unwrap_or(1));
                by_model.entry(key).or_default().push(req);
            }

            // Process each model group
            for (_model_key, requests) in by_model {
                // In a real implementation, would batch these together
                for batched in requests {
                    let response = InferenceResponse {
                        request_id: batched.request.id.clone(),
                        model: batched.request.model.clone(),
                        version: batched.request.version.unwrap_or(1),
                        outputs: HashMap::new(),
                        inference_time_us: 0,
                        total_time_us: batched.queued_at.elapsed().as_micros() as u64,
                        profile: None,
                        status: InferenceStatus::Success,
                        error: None,
                    };
                    let _ = batched.response_tx.send(response);
                }
            }
        }
    }

    /// Gets statistics snapshot
    pub fn stats(&self) -> InferenceStatsSnapshot {
        let total_requests = self.stats.total_requests.load(Ordering::Relaxed);
        let successful = self.stats.successful_requests.load(Ordering::Relaxed);
        let total_inference_time = self.stats.total_inference_time_us.load(Ordering::Relaxed);
        let batches = self.stats.batches_processed.load(Ordering::Relaxed);
        let total_batch_size = self.stats.total_batch_size.load(Ordering::Relaxed);

        InferenceStatsSnapshot {
            total_requests,
            successful_requests: successful,
            failed_requests: self.stats.failed_requests.load(Ordering::Relaxed),
            timeout_requests: self.stats.timeout_requests.load(Ordering::Relaxed),
            avg_inference_time_us: if successful > 0 { total_inference_time / successful } else { 0 },
            avg_batch_size: if batches > 0 { total_batch_size as f64 / batches as f64 } else { 0.0 },
            batches_processed: batches,
        }
    }

    /// Lists loaded models
    pub async fn list_models(&self) -> Vec<LoadedModelInfo> {
        let models = self.models.read().await;
        models.iter().map(|(key, loaded)| {
            LoadedModelInfo {
                key: key.clone(),
                name: loaded.model.metadata.name.clone(),
                version: loaded.version,
                format: loaded.model.config.format,
                loaded_at: loaded.loaded_at,
                inference_count: loaded.inference_count.load(Ordering::Relaxed),
            }
        }).collect()
    }
}

/// Statistics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceStatsSnapshot {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub timeout_requests: u64,
    pub avg_inference_time_us: u64,
    pub avg_batch_size: f64,
    pub batches_processed: u64,
}

/// Loaded model info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadedModelInfo {
    pub key: String,
    pub name: String,
    pub version: u32,
    pub format: ModelFormat,
    pub loaded_at: u64,
    pub inference_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::model::{ModelBuilder, TensorSpec};

    fn test_model() -> Model {
        ModelBuilder::new("test_model")
            .format(ModelFormat::Onnx)
            .version("1.0")
            .input(TensorSpec::new("input", DataType::Float32, vec![-1, 3, 224, 224]))
            .output(TensorSpec::new("output", DataType::Float32, vec![-1, 1000]))
            .build()
    }

    #[tokio::test]
    async fn test_inference_engine_basic() {
        let engine = InferenceEngine::new(InferenceConfig::default());
        engine.start().await.unwrap();

        engine.load_model(test_model(), 1).await.unwrap();

        let models = engine.list_models().await;
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].name, "test_model");

        engine.stop().await.unwrap();
    }

    #[test]
    fn test_tensor_data() {
        let data = vec![1.0f32, 2.0, 3.0, 4.0];
        let tensor = TensorData::from_f32(&data, vec![2, 2]);

        assert_eq!(tensor.dtype, DataType::Float32);
        assert_eq!(tensor.shape, vec![2, 2]);
        assert_eq!(tensor.element_count(), 4);

        let recovered = tensor.as_f32().unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_inference_request() {
        let request = InferenceRequest::new("model1")
            .with_version(2)
            .with_priority(RequestPriority::High)
            .with_input("input", TensorData::from_f32(&[1.0, 2.0], vec![1, 2]));

        assert_eq!(request.model, "model1");
        assert_eq!(request.version, Some(2));
        assert_eq!(request.priority, RequestPriority::High);
        assert!(request.inputs.contains_key("input"));
    }
}
