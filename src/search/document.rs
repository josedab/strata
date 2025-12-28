// Document Model for Full-Text Search

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Field type for indexing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    /// Full-text indexed field
    Text,
    /// Keyword field (exact match)
    Keyword,
    /// Integer field
    Integer,
    /// Long integer field
    Long,
    /// Float field
    Float,
    /// Double field
    Double,
    /// Boolean field
    Boolean,
    /// Date field
    Date,
    /// Binary/blob field (stored, not indexed)
    Binary,
    /// Geo point field
    GeoPoint,
    /// Nested object
    Object,
    /// Array of values
    Array,
}

/// Field value
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldValue {
    /// Text value
    Text(String),
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// Boolean value
    Boolean(bool),
    /// Binary data
    Binary(Vec<u8>),
    /// Null value
    Null,
    /// Array of values
    Array(Vec<FieldValue>),
    /// Nested object
    Object(HashMap<String, FieldValue>),
    /// Geo point (lat, lon)
    GeoPoint { lat: f64, lon: f64 },
}

impl FieldValue {
    /// Gets as string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            FieldValue::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Gets as i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            FieldValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Gets as f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FieldValue::Float(f) => Some(*f),
            FieldValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Gets as bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            FieldValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Converts to string representation
    pub fn to_string_value(&self) -> String {
        match self {
            FieldValue::Text(s) => s.clone(),
            FieldValue::Integer(i) => i.to_string(),
            FieldValue::Float(f) => f.to_string(),
            FieldValue::Boolean(b) => b.to_string(),
            FieldValue::Binary(_) => "[binary]".to_string(),
            FieldValue::Null => "null".to_string(),
            FieldValue::Array(arr) => {
                let values: Vec<String> = arr.iter().map(|v| v.to_string_value()).collect();
                format!("[{}]", values.join(", "))
            }
            FieldValue::Object(_) => "[object]".to_string(),
            FieldValue::GeoPoint { lat, lon } => format!("{},{}", lat, lon),
        }
    }

    /// Checks if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, FieldValue::Null)
    }
}

impl From<String> for FieldValue {
    fn from(s: String) -> Self {
        FieldValue::Text(s)
    }
}

impl From<&str> for FieldValue {
    fn from(s: &str) -> Self {
        FieldValue::Text(s.to_string())
    }
}

impl From<i64> for FieldValue {
    fn from(i: i64) -> Self {
        FieldValue::Integer(i)
    }
}

impl From<i32> for FieldValue {
    fn from(i: i32) -> Self {
        FieldValue::Integer(i as i64)
    }
}

impl From<f64> for FieldValue {
    fn from(f: f64) -> Self {
        FieldValue::Float(f)
    }
}

impl From<bool> for FieldValue {
    fn from(b: bool) -> Self {
        FieldValue::Boolean(b)
    }
}

impl From<Vec<u8>> for FieldValue {
    fn from(b: Vec<u8>) -> Self {
        FieldValue::Binary(b)
    }
}

/// Field definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: FieldType,
    /// Whether field is indexed
    pub indexed: bool,
    /// Whether field is stored
    pub stored: bool,
    /// Analyzer name for text fields
    pub analyzer: Option<String>,
    /// Boost factor for scoring
    pub boost: f32,
    /// Whether field is required
    pub required: bool,
    /// Default value
    pub default: Option<FieldValue>,
    /// Enable position indexing
    pub positions: bool,
    /// Enable term vectors
    pub term_vectors: bool,
}

impl Field {
    /// Creates a text field
    pub fn text(name: &str) -> Self {
        Self {
            name: name.to_string(),
            field_type: FieldType::Text,
            indexed: true,
            stored: true,
            analyzer: Some("default".to_string()),
            boost: 1.0,
            required: false,
            default: None,
            positions: true,
            term_vectors: false,
        }
    }

    /// Creates a keyword field
    pub fn keyword(name: &str) -> Self {
        Self {
            name: name.to_string(),
            field_type: FieldType::Keyword,
            indexed: true,
            stored: true,
            analyzer: None,
            boost: 1.0,
            required: false,
            default: None,
            positions: false,
            term_vectors: false,
        }
    }

    /// Creates an integer field
    pub fn integer(name: &str) -> Self {
        Self {
            name: name.to_string(),
            field_type: FieldType::Integer,
            indexed: true,
            stored: true,
            analyzer: None,
            boost: 1.0,
            required: false,
            default: None,
            positions: false,
            term_vectors: false,
        }
    }

    /// Creates a long field
    pub fn long(name: &str) -> Self {
        Self {
            name: name.to_string(),
            field_type: FieldType::Long,
            indexed: true,
            stored: true,
            analyzer: None,
            boost: 1.0,
            required: false,
            default: None,
            positions: false,
            term_vectors: false,
        }
    }

    /// Creates a float field
    pub fn float(name: &str) -> Self {
        Self {
            name: name.to_string(),
            field_type: FieldType::Float,
            indexed: true,
            stored: true,
            analyzer: None,
            boost: 1.0,
            required: false,
            default: None,
            positions: false,
            term_vectors: false,
        }
    }

    /// Creates a boolean field
    pub fn boolean(name: &str) -> Self {
        Self {
            name: name.to_string(),
            field_type: FieldType::Boolean,
            indexed: true,
            stored: true,
            analyzer: None,
            boost: 1.0,
            required: false,
            default: None,
            positions: false,
            term_vectors: false,
        }
    }

    /// Creates a date field
    pub fn date(name: &str) -> Self {
        Self {
            name: name.to_string(),
            field_type: FieldType::Date,
            indexed: true,
            stored: true,
            analyzer: None,
            boost: 1.0,
            required: false,
            default: None,
            positions: false,
            term_vectors: false,
        }
    }

    /// Creates a stored-only field
    pub fn stored(name: &str) -> Self {
        Self {
            name: name.to_string(),
            field_type: FieldType::Binary,
            indexed: false,
            stored: true,
            analyzer: None,
            boost: 1.0,
            required: false,
            default: None,
            positions: false,
            term_vectors: false,
        }
    }

    /// Sets the boost factor
    pub fn with_boost(mut self, boost: f32) -> Self {
        self.boost = boost;
        self
    }

    /// Marks field as required
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Sets default value
    pub fn with_default(mut self, default: FieldValue) -> Self {
        self.default = Some(default);
        self
    }

    /// Enables term vectors
    pub fn with_term_vectors(mut self) -> Self {
        self.term_vectors = true;
        self
    }

    /// Sets analyzer
    pub fn with_analyzer(mut self, analyzer: &str) -> Self {
        self.analyzer = Some(analyzer.to_string());
        self
    }
}

/// Document for indexing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Document ID
    pub id: String,
    /// Document fields
    pub fields: HashMap<String, FieldValue>,
    /// Document score (for search results)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    /// Highlights (for search results)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlights: Option<HashMap<String, Vec<String>>>,
    /// Document version
    pub version: u64,
    /// Creation timestamp
    pub created_at: u64,
    /// Update timestamp
    pub updated_at: u64,
}

impl Document {
    /// Creates a new document
    pub fn new(id: impl Into<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            id: id.into(),
            fields: HashMap::new(),
            score: None,
            highlights: None,
            version: 1,
            created_at: now,
            updated_at: now,
        }
    }

    /// Adds a field value
    pub fn field(mut self, name: &str, value: impl Into<FieldValue>) -> Self {
        self.fields.insert(name.to_string(), value.into());
        self
    }

    /// Sets a field value
    pub fn set_field(&mut self, name: &str, value: impl Into<FieldValue>) {
        self.fields.insert(name.to_string(), value.into());
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.version += 1;
    }

    /// Gets a field value
    pub fn get_field(&self, name: &str) -> Option<&FieldValue> {
        self.fields.get(name)
    }

    /// Gets field as string
    pub fn get_string(&self, name: &str) -> Option<&str> {
        self.fields.get(name).and_then(|v| v.as_str())
    }

    /// Gets field as i64
    pub fn get_i64(&self, name: &str) -> Option<i64> {
        self.fields.get(name).and_then(|v| v.as_i64())
    }

    /// Gets field as f64
    pub fn get_f64(&self, name: &str) -> Option<f64> {
        self.fields.get(name).and_then(|v| v.as_f64())
    }

    /// Gets field as bool
    pub fn get_bool(&self, name: &str) -> Option<bool> {
        self.fields.get(name).and_then(|v| v.as_bool())
    }

    /// Removes a field
    pub fn remove_field(&mut self, name: &str) -> Option<FieldValue> {
        self.fields.remove(name)
    }

    /// Checks if document has a field
    pub fn has_field(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    /// Gets all field names
    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.fields.keys().map(|s| s.as_str())
    }

    /// Sets score
    pub fn with_score(mut self, score: f32) -> Self {
        self.score = Some(score);
        self
    }

    /// Sets highlights
    pub fn with_highlights(mut self, highlights: HashMap<String, Vec<String>>) -> Self {
        self.highlights = Some(highlights);
        self
    }
}

/// Document builder
pub struct DocumentBuilder {
    id: String,
    fields: HashMap<String, FieldValue>,
}

impl DocumentBuilder {
    /// Creates a new builder
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            fields: HashMap::new(),
        }
    }

    /// Adds a text field
    pub fn text(mut self, name: &str, value: &str) -> Self {
        self.fields
            .insert(name.to_string(), FieldValue::Text(value.to_string()));
        self
    }

    /// Adds an integer field
    pub fn integer(mut self, name: &str, value: i64) -> Self {
        self.fields
            .insert(name.to_string(), FieldValue::Integer(value));
        self
    }

    /// Adds a float field
    pub fn float(mut self, name: &str, value: f64) -> Self {
        self.fields
            .insert(name.to_string(), FieldValue::Float(value));
        self
    }

    /// Adds a boolean field
    pub fn boolean(mut self, name: &str, value: bool) -> Self {
        self.fields
            .insert(name.to_string(), FieldValue::Boolean(value));
        self
    }

    /// Adds a binary field
    pub fn binary(mut self, name: &str, value: Vec<u8>) -> Self {
        self.fields
            .insert(name.to_string(), FieldValue::Binary(value));
        self
    }

    /// Adds a geo point field
    pub fn geo_point(mut self, name: &str, lat: f64, lon: f64) -> Self {
        self.fields
            .insert(name.to_string(), FieldValue::GeoPoint { lat, lon });
        self
    }

    /// Builds the document
    pub fn build(self) -> Document {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Document {
            id: self.id,
            fields: self.fields,
            score: None,
            highlights: None,
            version: 1,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Schema name
    pub name: String,
    /// Field definitions
    pub fields: Vec<Field>,
    /// Primary key field
    pub primary_key: String,
    /// Dynamic field handling
    pub dynamic: DynamicFieldPolicy,
}

/// Dynamic field handling policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DynamicFieldPolicy {
    /// Reject unknown fields
    Strict,
    /// Allow and index unknown fields
    Dynamic,
    /// Allow but don't index unknown fields
    Store,
    /// Ignore unknown fields
    Ignore,
}

impl Schema {
    /// Creates a new schema
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            fields: Vec::new(),
            primary_key: "id".to_string(),
            dynamic: DynamicFieldPolicy::Dynamic,
        }
    }

    /// Adds a field
    pub fn field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    /// Sets primary key
    pub fn primary_key(mut self, field: &str) -> Self {
        self.primary_key = field.to_string();
        self
    }

    /// Sets dynamic policy
    pub fn dynamic(mut self, policy: DynamicFieldPolicy) -> Self {
        self.dynamic = policy;
        self
    }

    /// Gets a field definition
    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Gets indexed fields
    pub fn indexed_fields(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter().filter(|f| f.indexed)
    }

    /// Gets text fields
    pub fn text_fields(&self) -> impl Iterator<Item = &Field> {
        self.fields
            .iter()
            .filter(|f| f.field_type == FieldType::Text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_document_creation() {
        let doc = Document::new("doc-1")
            .field("title", "Test Document")
            .field("count", 42i64)
            .field("active", true);

        assert_eq!(doc.id, "doc-1");
        assert_eq!(doc.get_string("title"), Some("Test Document"));
        assert_eq!(doc.get_i64("count"), Some(42));
        assert_eq!(doc.get_bool("active"), Some(true));
    }

    #[test]
    fn test_document_builder() {
        let doc = DocumentBuilder::new("doc-2")
            .text("name", "Alice")
            .integer("age", 30)
            .float("score", 95.5)
            .boolean("verified", true)
            .build();

        assert_eq!(doc.id, "doc-2");
        assert_eq!(doc.get_string("name"), Some("Alice"));
        assert_eq!(doc.get_i64("age"), Some(30));
        assert_eq!(doc.get_f64("score"), Some(95.5));
    }

    #[test]
    fn test_schema() {
        let schema = Schema::new("files")
            .field(Field::keyword("id").required())
            .field(Field::text("name").with_boost(2.0))
            .field(Field::text("content"))
            .field(Field::long("size"))
            .field(Field::date("created"))
            .primary_key("id");

        assert_eq!(schema.name, "files");
        assert_eq!(schema.fields.len(), 5);
        assert_eq!(schema.primary_key, "id");

        let name_field = schema.get_field("name").unwrap();
        assert_eq!(name_field.boost, 2.0);
    }

    #[test]
    fn test_field_value_conversion() {
        let text: FieldValue = "hello".into();
        let int: FieldValue = 42i64.into();
        let float: FieldValue = 3.14f64.into();
        let boolean: FieldValue = true.into();

        assert_eq!(text.as_str(), Some("hello"));
        assert_eq!(int.as_i64(), Some(42));
        assert_eq!(float.as_f64(), Some(3.14));
        assert_eq!(boolean.as_bool(), Some(true));
    }
}
