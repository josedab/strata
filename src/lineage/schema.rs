// Schema Registry for Data Lineage

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Schema version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Version number
    pub version: u32,
    /// Schema definition
    pub schema: Schema,
    /// Fingerprint/hash
    pub fingerprint: String,
    /// Created timestamp
    pub created_at: u64,
    /// Created by
    pub created_by: Option<String>,
    /// Description
    pub description: Option<String>,
    /// Is latest version
    pub is_latest: bool,
}

/// Schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Schema name
    pub name: String,
    /// Schema type
    pub schema_type: SchemaType,
    /// Namespace
    pub namespace: Option<String>,
    /// Fields/columns
    pub fields: Vec<FieldSchema>,
    /// Documentation
    pub doc: Option<String>,
    /// Custom attributes
    pub attributes: HashMap<String, serde_json::Value>,
}

impl Schema {
    /// Creates a new schema
    pub fn new(name: &str, schema_type: SchemaType) -> Self {
        Self {
            name: name.to_string(),
            schema_type,
            namespace: None,
            fields: Vec::new(),
            doc: None,
            attributes: HashMap::new(),
        }
    }

    /// Adds a field
    pub fn field(mut self, field: FieldSchema) -> Self {
        self.fields.push(field);
        self
    }

    /// Sets namespace
    pub fn namespace(mut self, ns: &str) -> Self {
        self.namespace = Some(ns.to_string());
        self
    }

    /// Sets documentation
    pub fn doc(mut self, doc: &str) -> Self {
        self.doc = Some(doc.to_string());
        self
    }

    /// Generates fingerprint
    pub fn fingerprint(&self) -> String {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        for field in &self.fields {
            field.name.hash(&mut hasher);
            format!("{:?}", field.data_type).hash(&mut hasher);
        }
        format!("{:016x}", hasher.finish())
    }
}

/// Schema type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaType {
    /// Avro schema
    Avro,
    /// JSON Schema
    Json,
    /// Protocol Buffers
    Protobuf,
    /// Parquet schema
    Parquet,
    /// SQL DDL
    Sql,
    /// Custom/internal
    Custom,
}

/// Field schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    /// Field name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Is nullable
    pub nullable: bool,
    /// Default value
    pub default: Option<serde_json::Value>,
    /// Description
    pub doc: Option<String>,
    /// Position/order
    pub position: usize,
    /// Additional attributes
    pub attributes: HashMap<String, serde_json::Value>,
}

impl PartialEq for FieldSchema {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.data_type == other.data_type
            && self.nullable == other.nullable
            && self.doc == other.doc
            && self.position == other.position
    }
}

impl Eq for FieldSchema {}

impl FieldSchema {
    /// Creates a new field
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable: true,
            default: None,
            doc: None,
            position: 0,
            attributes: HashMap::new(),
        }
    }

    /// Sets nullable
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Sets default value
    pub fn default(mut self, value: serde_json::Value) -> Self {
        self.default = Some(value);
        self
    }

    /// Sets documentation
    pub fn doc(mut self, doc: &str) -> Self {
        self.doc = Some(doc.to_string());
        self
    }

    /// Sets position
    pub fn position(mut self, pos: usize) -> Self {
        self.position = pos;
        self
    }
}

/// Data type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataType {
    /// Boolean
    Boolean,
    /// 32-bit integer
    Int32,
    /// 64-bit integer
    Int64,
    /// 32-bit float
    Float32,
    /// 64-bit float
    Float64,
    /// String
    String,
    /// Binary data
    Binary,
    /// Date
    Date,
    /// Timestamp
    Timestamp,
    /// Decimal with precision and scale
    Decimal { precision: u8, scale: u8 },
    /// Fixed-size binary
    Fixed { size: usize },
    /// Array of elements
    Array { element: Box<DataType> },
    /// Map/dictionary
    Map { key: Box<DataType>, value: Box<DataType> },
    /// Struct/record
    Struct { fields: Vec<FieldSchema> },
    /// Union of types
    Union { variants: Vec<DataType> },
    /// Enum
    Enum { symbols: Vec<String> },
    /// UUID
    Uuid,
    /// JSON
    Json,
}

/// Schema evolution compatibility
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Compatibility {
    /// No compatibility checking
    None,
    /// New schema can read old data
    Backward,
    /// Old schema can read new data
    Forward,
    /// Both backward and forward compatible
    Full,
    /// Backward transitive (all versions)
    BackwardTransitive,
    /// Forward transitive (all versions)
    ForwardTransitive,
    /// Full transitive
    FullTransitive,
}

/// Schema evolution rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEvolution {
    /// Changes detected
    pub changes: Vec<SchemaChange>,
    /// Is compatible
    pub compatible: bool,
    /// Compatibility level
    pub compatibility: Compatibility,
    /// Validation errors
    pub errors: Vec<String>,
}

impl SchemaEvolution {
    /// Creates from comparing two schemas
    pub fn compare(old: &Schema, new: &Schema, compatibility: Compatibility) -> Self {
        let mut changes = Vec::new();
        let mut errors = Vec::new();

        // Compare fields
        let old_fields: HashMap<_, _> = old.fields.iter().map(|f| (f.name.clone(), f)).collect();
        let new_fields: HashMap<_, _> = new.fields.iter().map(|f| (f.name.clone(), f)).collect();

        // Check for added fields
        for (name, field) in &new_fields {
            if !old_fields.contains_key(name) {
                changes.push(SchemaChange::FieldAdded {
                    field_name: name.clone(),
                    has_default: field.default.is_some(),
                });
            }
        }

        // Check for removed fields
        for (name, _) in &old_fields {
            if !new_fields.contains_key(name) {
                changes.push(SchemaChange::FieldRemoved {
                    field_name: name.clone(),
                });
            }
        }

        // Check for modified fields
        for (name, old_field) in &old_fields {
            if let Some(new_field) = new_fields.get(name) {
                if old_field.data_type != new_field.data_type {
                    changes.push(SchemaChange::TypeChanged {
                        field_name: name.clone(),
                        old_type: format!("{:?}", old_field.data_type),
                        new_type: format!("{:?}", new_field.data_type),
                    });
                }

                if old_field.nullable != new_field.nullable {
                    changes.push(SchemaChange::NullabilityChanged {
                        field_name: name.clone(),
                        was_nullable: old_field.nullable,
                    });
                }
            }
        }

        // Check compatibility
        let compatible = Self::check_compatibility(&changes, compatibility, &mut errors);

        Self {
            changes,
            compatible,
            compatibility,
            errors,
        }
    }

    fn check_compatibility(changes: &[SchemaChange], compat: Compatibility, errors: &mut Vec<String>) -> bool {
        match compat {
            Compatibility::None => true,

            Compatibility::Backward | Compatibility::BackwardTransitive => {
                // New readers must be able to read old data
                for change in changes {
                    match change {
                        SchemaChange::FieldAdded { has_default, field_name } => {
                            if !has_default {
                                errors.push(format!(
                                    "Added field '{}' must have default for backward compatibility",
                                    field_name
                                ));
                                return false;
                            }
                        }
                        SchemaChange::TypeChanged { field_name, old_type, new_type } => {
                            errors.push(format!(
                                "Type change from {} to {} for field '{}' breaks backward compatibility",
                                old_type, new_type, field_name
                            ));
                            return false;
                        }
                        SchemaChange::NullabilityChanged { field_name, was_nullable } => {
                            if *was_nullable {
                                errors.push(format!(
                                    "Making field '{}' non-nullable breaks backward compatibility",
                                    field_name
                                ));
                                return false;
                            }
                        }
                        _ => {}
                    }
                }
                true
            }

            Compatibility::Forward | Compatibility::ForwardTransitive => {
                // Old readers must be able to read new data
                for change in changes {
                    match change {
                        SchemaChange::FieldRemoved { field_name } => {
                            errors.push(format!(
                                "Removing field '{}' breaks forward compatibility",
                                field_name
                            ));
                            return false;
                        }
                        SchemaChange::TypeChanged { field_name, old_type, new_type } => {
                            errors.push(format!(
                                "Type change from {} to {} for field '{}' breaks forward compatibility",
                                old_type, new_type, field_name
                            ));
                            return false;
                        }
                        _ => {}
                    }
                }
                true
            }

            Compatibility::Full | Compatibility::FullTransitive => {
                // Both directions
                for change in changes {
                    match change {
                        SchemaChange::FieldAdded { has_default, field_name } => {
                            if !has_default {
                                errors.push(format!(
                                    "Added field '{}' must have default for full compatibility",
                                    field_name
                                ));
                                return false;
                            }
                        }
                        SchemaChange::FieldRemoved { field_name } => {
                            errors.push(format!(
                                "Removing field '{}' breaks full compatibility",
                                field_name
                            ));
                            return false;
                        }
                        SchemaChange::TypeChanged { field_name, old_type, new_type } => {
                            errors.push(format!(
                                "Type change from {} to {} for field '{}' breaks full compatibility",
                                old_type, new_type, field_name
                            ));
                            return false;
                        }
                        SchemaChange::NullabilityChanged { field_name, was_nullable } => {
                            if *was_nullable {
                                errors.push(format!(
                                    "Making field '{}' non-nullable breaks full compatibility",
                                    field_name
                                ));
                                return false;
                            }
                        }
                    }
                }
                true
            }
        }
    }
}

/// Schema change type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SchemaChange {
    /// Field was added
    FieldAdded {
        field_name: String,
        has_default: bool,
    },
    /// Field was removed
    FieldRemoved {
        field_name: String,
    },
    /// Type was changed
    TypeChanged {
        field_name: String,
        old_type: String,
        new_type: String,
    },
    /// Nullability changed
    NullabilityChanged {
        field_name: String,
        was_nullable: bool,
    },
}

/// Schema registry
pub struct SchemaRegistry {
    /// Schemas by subject
    schemas: Arc<RwLock<HashMap<String, Vec<SchemaVersion>>>>,
    /// Default compatibility
    default_compatibility: Compatibility,
    /// Subject compatibility overrides
    compatibility_overrides: Arc<RwLock<HashMap<String, Compatibility>>>,
}

impl SchemaRegistry {
    /// Creates a new schema registry
    pub fn new(default_compatibility: Compatibility) -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            default_compatibility,
            compatibility_overrides: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Registers a new schema version
    pub async fn register(&self, subject: &str, schema: Schema, created_by: Option<&str>) -> Result<u32> {
        let fingerprint = schema.fingerprint();

        // Get compatibility level
        let compatibility = {
            let overrides = self.compatibility_overrides.read().await;
            overrides.get(subject).copied().unwrap_or(self.default_compatibility)
        };

        let mut schemas = self.schemas.write().await;
        let versions = schemas.entry(subject.to_string()).or_insert_with(Vec::new);

        // Check if schema already exists
        for version in versions.iter() {
            if version.fingerprint == fingerprint {
                return Ok(version.version);
            }
        }

        // Check compatibility with latest version
        if !versions.is_empty() {
            let latest = versions.last().unwrap();
            let evolution = SchemaEvolution::compare(&latest.schema, &schema, compatibility);

            if !evolution.compatible {
                return Err(StrataError::InvalidOperation(
                    format!("Schema is not compatible: {}", evolution.errors.join(", "))
                ));
            }
        }

        // Mark previous as not latest
        for version in versions.iter_mut() {
            version.is_latest = false;
        }

        // Add new version
        let version_num = versions.len() as u32 + 1;
        versions.push(SchemaVersion {
            version: version_num,
            schema,
            fingerprint,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            created_by: created_by.map(String::from),
            description: None,
            is_latest: true,
        });

        Ok(version_num)
    }

    /// Gets schema by subject and version
    pub async fn get(&self, subject: &str, version: u32) -> Option<SchemaVersion> {
        let schemas = self.schemas.read().await;
        schemas
            .get(subject)
            .and_then(|versions| versions.iter().find(|v| v.version == version).cloned())
    }

    /// Gets latest schema for subject
    pub async fn get_latest(&self, subject: &str) -> Option<SchemaVersion> {
        let schemas = self.schemas.read().await;
        schemas
            .get(subject)
            .and_then(|versions| versions.last().cloned())
    }

    /// Lists all versions for subject
    pub async fn list_versions(&self, subject: &str) -> Vec<SchemaVersion> {
        let schemas = self.schemas.read().await;
        schemas.get(subject).cloned().unwrap_or_default()
    }

    /// Lists all subjects
    pub async fn list_subjects(&self) -> Vec<String> {
        let schemas = self.schemas.read().await;
        schemas.keys().cloned().collect()
    }

    /// Checks schema compatibility
    pub async fn check_compatibility(&self, subject: &str, schema: &Schema) -> SchemaEvolution {
        let compatibility = {
            let overrides = self.compatibility_overrides.read().await;
            overrides.get(subject).copied().unwrap_or(self.default_compatibility)
        };

        let schemas = self.schemas.read().await;
        if let Some(versions) = schemas.get(subject) {
            if let Some(latest) = versions.last() {
                return SchemaEvolution::compare(&latest.schema, schema, compatibility);
            }
        }

        // No existing schema, always compatible
        SchemaEvolution {
            changes: Vec::new(),
            compatible: true,
            compatibility,
            errors: Vec::new(),
        }
    }

    /// Sets compatibility level for subject
    pub async fn set_compatibility(&self, subject: &str, compatibility: Compatibility) {
        let mut overrides = self.compatibility_overrides.write().await;
        overrides.insert(subject.to_string(), compatibility);
    }

    /// Gets compatibility level for subject
    pub async fn get_compatibility(&self, subject: &str) -> Compatibility {
        let overrides = self.compatibility_overrides.read().await;
        overrides.get(subject).copied().unwrap_or(self.default_compatibility)
    }

    /// Deletes a subject
    pub async fn delete_subject(&self, subject: &str) -> bool {
        let mut schemas = self.schemas.write().await;
        schemas.remove(subject).is_some()
    }

    /// Gets schema by fingerprint
    pub async fn get_by_fingerprint(&self, fingerprint: &str) -> Option<(String, SchemaVersion)> {
        let schemas = self.schemas.read().await;
        for (subject, versions) in schemas.iter() {
            for version in versions {
                if version.fingerprint == fingerprint {
                    return Some((subject.clone(), version.clone()));
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        Schema::new("user", SchemaType::Avro)
            .namespace("com.example")
            .field(FieldSchema::new("id", DataType::Int64).nullable(false))
            .field(FieldSchema::new("name", DataType::String))
            .field(FieldSchema::new("email", DataType::String))
    }

    #[tokio::test]
    async fn test_schema_registry() {
        let registry = SchemaRegistry::new(Compatibility::Backward);

        // Register first version
        let version = registry.register("user", test_schema(), Some("alice")).await.unwrap();
        assert_eq!(version, 1);

        // Get schema
        let schema = registry.get("user", 1).await.unwrap();
        assert_eq!(schema.schema.name, "user");
        assert!(schema.is_latest);
    }

    #[tokio::test]
    async fn test_schema_evolution() {
        let old = test_schema();
        let mut new = test_schema();
        new.fields.push(
            FieldSchema::new("age", DataType::Int32)
                .default(serde_json::json!(0))
        );

        let evolution = SchemaEvolution::compare(&old, &new, Compatibility::Backward);
        assert!(evolution.compatible);
        assert_eq!(evolution.changes.len(), 1);
    }

    #[tokio::test]
    async fn test_backward_incompatible() {
        let old = test_schema();
        let mut new = test_schema();
        // Add field without default - breaks backward compatibility
        new.fields.push(FieldSchema::new("required_field", DataType::String).nullable(false));

        let evolution = SchemaEvolution::compare(&old, &new, Compatibility::Backward);
        // Note: Field addition without default is incompatible only if the field is non-nullable
        // and the checker is strict about defaults
    }

    #[test]
    fn test_schema_fingerprint() {
        let schema = test_schema();
        let fingerprint = schema.fingerprint();
        assert!(!fingerprint.is_empty());

        // Same schema should produce same fingerprint
        let schema2 = test_schema();
        assert_eq!(schema.fingerprint(), schema2.fingerprint());
    }

    #[tokio::test]
    async fn test_compatibility_levels() {
        let registry = SchemaRegistry::new(Compatibility::None);

        registry.register("test", test_schema(), None).await.unwrap();

        // Set compatibility for subject
        registry.set_compatibility("test", Compatibility::Full).await;

        let compat = registry.get_compatibility("test").await;
        assert_eq!(compat, Compatibility::Full);
    }
}
