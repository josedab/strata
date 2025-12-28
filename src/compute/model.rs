// ML Model Definition and Management

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Model format/framework
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModelFormat {
    /// ONNX Runtime
    Onnx,
    /// TensorFlow SavedModel
    TensorFlow,
    /// PyTorch TorchScript
    PyTorch,
    /// Scikit-learn
    Sklearn,
    /// XGBoost
    XgBoost,
    /// LightGBM
    LightGbm,
    /// TensorRT
    TensorRt,
    /// Custom/other
    Custom,
}

/// Model configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    /// Model name
    pub name: String,
    /// Model format
    pub format: ModelFormat,
    /// Model version
    pub version: String,
    /// Input tensor specifications
    pub inputs: Vec<TensorSpec>,
    /// Output tensor specifications
    pub outputs: Vec<TensorSpec>,
    /// Preprocessing steps
    pub preprocessing: Vec<PreprocessStep>,
    /// Postprocessing steps
    pub postprocessing: Vec<PostprocessStep>,
    /// Batch settings
    pub batch: BatchConfig,
    /// Resource requirements
    pub resources: ResourceConfig,
    /// Custom options
    pub options: HashMap<String, serde_json::Value>,
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            format: ModelFormat::Onnx,
            version: "1".to_string(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            preprocessing: Vec::new(),
            postprocessing: Vec::new(),
            batch: BatchConfig::default(),
            resources: ResourceConfig::default(),
            options: HashMap::new(),
        }
    }
}

/// Tensor specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TensorSpec {
    /// Tensor name
    pub name: String,
    /// Data type
    pub dtype: DataType,
    /// Shape (-1 for dynamic dimensions)
    pub shape: Vec<i64>,
    /// Description
    pub description: Option<String>,
}

impl TensorSpec {
    /// Creates a new tensor spec
    pub fn new(name: &str, dtype: DataType, shape: Vec<i64>) -> Self {
        Self {
            name: name.to_string(),
            dtype,
            shape,
            description: None,
        }
    }
}

/// Data type for tensors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Float16,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Bool,
    String,
    Complex64,
    Complex128,
}

impl DataType {
    /// Gets size in bytes
    pub fn size_bytes(&self) -> usize {
        match self {
            DataType::Bool | DataType::Int8 | DataType::Uint8 => 1,
            DataType::Float16 | DataType::Int16 | DataType::Uint16 => 2,
            DataType::Float32 | DataType::Int32 | DataType::Uint32 => 4,
            DataType::Float64 | DataType::Int64 | DataType::Uint64 | DataType::Complex64 => 8,
            DataType::Complex128 => 16,
            DataType::String => 0, // Variable
        }
    }
}

/// Preprocessing step
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PreprocessStep {
    /// Normalize values
    Normalize {
        mean: Vec<f32>,
        std: Vec<f32>,
    },
    /// Resize image
    Resize {
        width: u32,
        height: u32,
        mode: ResizeMode,
    },
    /// Center crop
    CenterCrop {
        width: u32,
        height: u32,
    },
    /// Convert to tensor
    ToTensor,
    /// Tokenize text
    Tokenize {
        vocab_path: String,
        max_length: usize,
    },
    /// Pad sequence
    Pad {
        max_length: usize,
        pad_value: f32,
    },
    /// Custom preprocessing
    Custom {
        name: String,
        params: HashMap<String, serde_json::Value>,
    },
}

/// Resize mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResizeMode {
    Bilinear,
    Bicubic,
    Nearest,
    Lanczos,
}

/// Postprocessing step
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PostprocessStep {
    /// Softmax activation
    Softmax {
        axis: i32,
    },
    /// Argmax
    Argmax {
        axis: i32,
    },
    /// Top-K selection
    TopK {
        k: usize,
    },
    /// Apply threshold
    Threshold {
        value: f32,
    },
    /// Decode labels
    DecodeLabels {
        labels_path: String,
    },
    /// Non-max suppression
    Nms {
        iou_threshold: f32,
        score_threshold: f32,
    },
    /// Custom postprocessing
    Custom {
        name: String,
        params: HashMap<String, serde_json::Value>,
    },
}

/// Batch configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Preferred batch size
    pub preferred_batch_size: usize,
    /// Maximum queue delay in milliseconds
    pub max_queue_delay_ms: u64,
    /// Enable dynamic batching
    pub dynamic_batching: bool,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 32,
            preferred_batch_size: 8,
            max_queue_delay_ms: 100,
            dynamic_batching: true,
        }
    }
}

/// Resource configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceConfig {
    /// Number of CPU cores
    pub cpu_cores: f32,
    /// Memory limit in MB
    pub memory_mb: u64,
    /// GPU memory limit in MB
    pub gpu_memory_mb: Option<u64>,
    /// Number of GPUs
    pub gpu_count: u32,
    /// GPU device IDs
    pub gpu_devices: Vec<u32>,
    /// Use TensorRT optimization
    pub use_tensorrt: bool,
    /// Number of inference threads
    pub inference_threads: u32,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            cpu_cores: 1.0,
            memory_mb: 512,
            gpu_memory_mb: None,
            gpu_count: 0,
            gpu_devices: Vec::new(),
            use_tensorrt: false,
            inference_threads: 4,
        }
    }
}

/// Model metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMetadata {
    /// Model ID
    pub id: String,
    /// Model name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Author
    pub author: Option<String>,
    /// License
    pub license: Option<String>,
    /// Tags
    pub tags: Vec<String>,
    /// Labels
    pub labels: HashMap<String, String>,
    /// Framework
    pub framework: String,
    /// Framework version
    pub framework_version: Option<String>,
    /// Training dataset
    pub training_dataset: Option<String>,
    /// Metrics
    pub metrics: HashMap<String, f64>,
    /// Creation timestamp
    pub created_at: u64,
    /// Updated timestamp
    pub updated_at: u64,
    /// Signature (hash of model file)
    pub signature: Option<String>,
    /// Size in bytes
    pub size_bytes: u64,
}

impl ModelMetadata {
    /// Creates new metadata
    pub fn new(id: &str, name: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id: id.to_string(),
            name: name.to_string(),
            description: None,
            author: None,
            license: None,
            tags: Vec::new(),
            labels: HashMap::new(),
            framework: String::new(),
            framework_version: None,
            training_dataset: None,
            metrics: HashMap::new(),
            created_at: now,
            updated_at: now,
            signature: None,
            size_bytes: 0,
        }
    }

    /// Sets description
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Sets author
    pub fn with_author(mut self, author: &str) -> Self {
        self.author = Some(author.to_string());
        self
    }

    /// Adds a tag
    pub fn with_tag(mut self, tag: &str) -> Self {
        self.tags.push(tag.to_string());
        self
    }

    /// Adds a metric
    pub fn with_metric(mut self, name: &str, value: f64) -> Self {
        self.metrics.insert(name.to_string(), value);
        self
    }
}

/// ML Model
#[derive(Debug, Clone)]
pub struct Model {
    /// Configuration
    pub config: ModelConfig,
    /// Metadata
    pub metadata: ModelMetadata,
    /// Model data/weights
    pub data: Option<Vec<u8>>,
    /// Model path
    pub path: Option<String>,
    /// Is loaded
    pub loaded: bool,
}

impl Model {
    /// Creates a new model
    pub fn new(config: ModelConfig, metadata: ModelMetadata) -> Self {
        Self {
            config,
            metadata,
            data: None,
            path: None,
            loaded: false,
        }
    }

    /// Creates from file path
    pub fn from_path(path: &str, config: ModelConfig, metadata: ModelMetadata) -> Self {
        Self {
            config,
            metadata,
            data: None,
            path: Some(path.to_string()),
            loaded: false,
        }
    }

    /// Creates from data
    pub fn from_data(data: Vec<u8>, config: ModelConfig, metadata: ModelMetadata) -> Self {
        let mut model = Self::new(config, metadata);
        model.metadata.size_bytes = data.len() as u64;
        model.data = Some(data);
        model
    }

    /// Gets model name
    pub fn name(&self) -> &str {
        &self.metadata.name
    }

    /// Gets model ID
    pub fn id(&self) -> &str {
        &self.metadata.id
    }

    /// Gets model format
    pub fn format(&self) -> ModelFormat {
        self.config.format
    }

    /// Gets model version
    pub fn version(&self) -> &str {
        &self.config.version
    }

    /// Checks if model supports GPU
    pub fn supports_gpu(&self) -> bool {
        matches!(
            self.config.format,
            ModelFormat::Onnx
                | ModelFormat::TensorFlow
                | ModelFormat::PyTorch
                | ModelFormat::TensorRt
        )
    }

    /// Gets input tensor names
    pub fn input_names(&self) -> Vec<&str> {
        self.config.inputs.iter().map(|t| t.name.as_str()).collect()
    }

    /// Gets output tensor names
    pub fn output_names(&self) -> Vec<&str> {
        self.config.outputs.iter().map(|t| t.name.as_str()).collect()
    }
}

/// Model builder
pub struct ModelBuilder {
    config: ModelConfig,
    metadata: ModelMetadata,
}

impl ModelBuilder {
    /// Creates a new builder
    pub fn new(name: &str) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        Self {
            config: ModelConfig {
                name: name.to_string(),
                ..Default::default()
            },
            metadata: ModelMetadata::new(&id, name),
        }
    }

    /// Sets format
    pub fn format(mut self, format: ModelFormat) -> Self {
        self.config.format = format;
        self.metadata.framework = format!("{:?}", format);
        self
    }

    /// Sets version
    pub fn version(mut self, version: &str) -> Self {
        self.config.version = version.to_string();
        self
    }

    /// Adds input tensor
    pub fn input(mut self, spec: TensorSpec) -> Self {
        self.config.inputs.push(spec);
        self
    }

    /// Adds output tensor
    pub fn output(mut self, spec: TensorSpec) -> Self {
        self.config.outputs.push(spec);
        self
    }

    /// Adds preprocessing step
    pub fn preprocess(mut self, step: PreprocessStep) -> Self {
        self.config.preprocessing.push(step);
        self
    }

    /// Adds postprocessing step
    pub fn postprocess(mut self, step: PostprocessStep) -> Self {
        self.config.postprocessing.push(step);
        self
    }

    /// Sets batch config
    pub fn batch(mut self, config: BatchConfig) -> Self {
        self.config.batch = config;
        self
    }

    /// Sets resources
    pub fn resources(mut self, config: ResourceConfig) -> Self {
        self.config.resources = config;
        self
    }

    /// Sets description
    pub fn description(mut self, desc: &str) -> Self {
        self.metadata.description = Some(desc.to_string());
        self
    }

    /// Sets author
    pub fn author(mut self, author: &str) -> Self {
        self.metadata.author = Some(author.to_string());
        self
    }

    /// Adds tag
    pub fn tag(mut self, tag: &str) -> Self {
        self.metadata.tags.push(tag.to_string());
        self
    }

    /// Adds metric
    pub fn metric(mut self, name: &str, value: f64) -> Self {
        self.metadata.metrics.insert(name.to_string(), value);
        self
    }

    /// Builds the model
    pub fn build(self) -> Model {
        Model::new(self.config, self.metadata)
    }

    /// Builds from path
    pub fn build_from_path(self, path: &str) -> Model {
        Model::from_path(path, self.config, self.metadata)
    }

    /// Builds from data
    pub fn build_from_data(self, data: Vec<u8>) -> Model {
        Model::from_data(data, self.config, self.metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_builder() {
        let model = ModelBuilder::new("resnet50")
            .format(ModelFormat::Onnx)
            .version("1.0")
            .input(TensorSpec::new("input", DataType::Float32, vec![-1, 3, 224, 224]))
            .output(TensorSpec::new("output", DataType::Float32, vec![-1, 1000]))
            .preprocess(PreprocessStep::Normalize {
                mean: vec![0.485, 0.456, 0.406],
                std: vec![0.229, 0.224, 0.225],
            })
            .postprocess(PostprocessStep::Softmax { axis: 1 })
            .description("ResNet-50 for image classification")
            .tag("vision")
            .tag("classification")
            .metric("accuracy", 0.76)
            .build();

        assert_eq!(model.name(), "resnet50");
        assert_eq!(model.format(), ModelFormat::Onnx);
        assert_eq!(model.config.inputs.len(), 1);
        assert_eq!(model.config.outputs.len(), 1);
    }

    #[test]
    fn test_tensor_spec() {
        let spec = TensorSpec::new("image", DataType::Float32, vec![-1, 3, 224, 224]);
        assert_eq!(spec.name, "image");
        assert_eq!(spec.dtype, DataType::Float32);
        assert_eq!(spec.shape.len(), 4);
    }

    #[test]
    fn test_data_type_size() {
        assert_eq!(DataType::Float32.size_bytes(), 4);
        assert_eq!(DataType::Float64.size_bytes(), 8);
        assert_eq!(DataType::Int8.size_bytes(), 1);
    }
}
