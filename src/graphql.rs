//! GraphQL Metadata API
//!
//! This module provides a flexible GraphQL API for metadata queries and
//! mutations. Features:
//! - Rich metadata queries with filtering and pagination
//! - Real-time subscriptions for file changes
//! - Batch operations
//! - Schema introspection
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    GraphQL API Layer                         │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Schema: Files │ Chunks │ Snapshots │ Lineage │ Stats      │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Queries │ Mutations │ Subscriptions                        │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Resolver Layer │ DataLoader │ Caching                      │
//! └─────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, StrataError};
use crate::types::{ChunkId, InodeId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use chrono::{DateTime, Utc};

/// GraphQL field types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    String,
    Int,
    Float,
    Boolean,
    ID,
    DateTime,
    List(Box<FieldType>),
    NonNull(Box<FieldType>),
    Object(String),
    Enum(String),
    Union(Vec<String>),
}

impl FieldType {
    pub fn required(self) -> Self {
        FieldType::NonNull(Box::new(self))
    }

    pub fn list(self) -> Self {
        FieldType::List(Box::new(self))
    }
}

/// Field definition
#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name: String,
    pub field_type: FieldType,
    pub description: Option<String>,
    pub arguments: Vec<ArgumentDef>,
    pub deprecated: Option<String>,
}

/// Argument definition
#[derive(Debug, Clone)]
pub struct ArgumentDef {
    pub name: String,
    pub arg_type: FieldType,
    pub default_value: Option<GraphQLValue>,
    pub description: Option<String>,
}

/// GraphQL type definition
#[derive(Debug, Clone)]
pub struct TypeDef {
    pub name: String,
    pub description: Option<String>,
    pub fields: Vec<FieldDef>,
    pub interfaces: Vec<String>,
}

/// Enum type definition
#[derive(Debug, Clone)]
pub struct EnumDef {
    pub name: String,
    pub description: Option<String>,
    pub values: Vec<EnumValue>,
}

#[derive(Debug, Clone)]
pub struct EnumValue {
    pub name: String,
    pub description: Option<String>,
    pub deprecated: Option<String>,
}

/// GraphQL schema
#[derive(Debug, Clone)]
pub struct Schema {
    pub types: HashMap<String, TypeDef>,
    pub enums: HashMap<String, EnumDef>,
    pub query_type: String,
    pub mutation_type: Option<String>,
    pub subscription_type: Option<String>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            types: HashMap::new(),
            enums: HashMap::new(),
            query_type: "Query".to_string(),
            mutation_type: Some("Mutation".to_string()),
            subscription_type: Some("Subscription".to_string()),
        }
    }

    pub fn add_type(&mut self, type_def: TypeDef) {
        self.types.insert(type_def.name.clone(), type_def);
    }

    pub fn add_enum(&mut self, enum_def: EnumDef) {
        self.enums.insert(enum_def.name.clone(), enum_def);
    }

    pub fn get_type(&self, name: &str) -> Option<&TypeDef> {
        self.types.get(name)
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

/// GraphQL value types
#[derive(Debug, Clone, PartialEq)]
pub enum GraphQLValue {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Enum(String),
    List(Vec<GraphQLValue>),
    Object(HashMap<String, GraphQLValue>),
}

impl GraphQLValue {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            GraphQLValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            GraphQLValue::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            GraphQLValue::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<GraphQLValue>> {
        match self {
            GraphQLValue::List(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&HashMap<String, GraphQLValue>> {
        match self {
            GraphQLValue::Object(v) => Some(v),
            _ => None,
        }
    }
}

/// GraphQL query operation
#[derive(Debug, Clone)]
pub struct Query {
    pub operation_name: Option<String>,
    pub selections: Vec<Selection>,
    pub variables: HashMap<String, GraphQLValue>,
}

/// Field selection in a query
#[derive(Debug, Clone)]
pub struct Selection {
    pub name: String,
    pub alias: Option<String>,
    pub arguments: HashMap<String, GraphQLValue>,
    pub selections: Vec<Selection>,
}

/// Query result
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub data: Option<GraphQLValue>,
    pub errors: Vec<GraphQLError>,
}

/// GraphQL error
#[derive(Debug, Clone)]
pub struct GraphQLError {
    pub message: String,
    pub locations: Vec<Location>,
    pub path: Vec<PathSegment>,
    pub extensions: HashMap<String, GraphQLValue>,
}

#[derive(Debug, Clone)]
pub struct Location {
    pub line: u32,
    pub column: u32,
}

#[derive(Debug, Clone)]
pub enum PathSegment {
    Field(String),
    Index(usize),
}

/// Pagination input
#[derive(Debug, Clone)]
pub struct PaginationInput {
    pub first: Option<u32>,
    pub after: Option<String>,
    pub last: Option<u32>,
    pub before: Option<String>,
}

/// Connection for paginated results (Relay-style)
#[derive(Debug, Clone)]
pub struct Connection<T> {
    pub edges: Vec<Edge<T>>,
    pub page_info: PageInfo,
    pub total_count: u64,
}

#[derive(Debug, Clone)]
pub struct Edge<T> {
    pub node: T,
    pub cursor: String,
}

#[derive(Debug, Clone)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

/// File metadata for GraphQL
#[derive(Debug, Clone)]
pub struct FileNode {
    pub id: InodeId,
    pub name: String,
    pub path: String,
    pub size: u64,
    pub file_type: FileType,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub owner: String,
    pub permissions: u32,
    pub tags: Vec<String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

/// Chunk metadata for GraphQL
#[derive(Debug, Clone)]
pub struct ChunkNode {
    pub id: ChunkId,
    pub size: u64,
    pub checksum: String,
    pub locations: Vec<String>,
    pub storage_class: String,
}

/// Snapshot metadata for GraphQL
#[derive(Debug, Clone)]
pub struct SnapshotNode {
    pub id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub file_count: u64,
    pub total_size: u64,
    pub tags: Vec<String>,
}

/// Stats for GraphQL
#[derive(Debug, Clone)]
pub struct StatsNode {
    pub total_files: u64,
    pub total_directories: u64,
    pub total_size: u64,
    pub total_chunks: u64,
    pub dedup_ratio: f64,
    pub compression_ratio: f64,
}

/// Subscription event types
#[derive(Debug, Clone)]
pub enum SubscriptionEvent {
    FileCreated(FileNode),
    FileModified(FileNode),
    FileDeleted { id: InodeId, path: String },
    ChunkCreated(ChunkNode),
    SnapshotCreated(SnapshotNode),
}

/// Resolver context with access to data
pub struct ResolverContext {
    pub files: RwLock<HashMap<InodeId, FileNode>>,
    pub chunks: RwLock<HashMap<ChunkId, ChunkNode>>,
    pub snapshots: RwLock<HashMap<String, SnapshotNode>>,
    pub stats: RwLock<StatsNode>,
    pub event_sender: broadcast::Sender<SubscriptionEvent>,
}

impl ResolverContext {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1000);
        Self {
            files: RwLock::new(HashMap::new()),
            chunks: RwLock::new(HashMap::new()),
            snapshots: RwLock::new(HashMap::new()),
            stats: RwLock::new(StatsNode {
                total_files: 0,
                total_directories: 0,
                total_size: 0,
                total_chunks: 0,
                dedup_ratio: 1.0,
                compression_ratio: 1.0,
            }),
            event_sender: tx,
        }
    }

    /// Add a file and emit event
    pub async fn add_file(&self, file: FileNode) {
        let id = file.id;
        {
            let mut files = self.files.write().await;
            files.insert(id, file.clone());
        }
        {
            let mut stats = self.stats.write().await;
            match file.file_type {
                FileType::File => stats.total_files += 1,
                FileType::Directory => stats.total_directories += 1,
                _ => {}
            }
            stats.total_size += file.size;
        }
        let _ = self.event_sender.send(SubscriptionEvent::FileCreated(file));
    }

    /// Update a file and emit event
    pub async fn update_file(&self, file: FileNode) {
        let id = file.id;
        self.files.write().await.insert(id, file.clone());
        let _ = self.event_sender.send(SubscriptionEvent::FileModified(file));
    }

    /// Delete a file and emit event
    pub async fn delete_file(&self, id: InodeId) -> Option<FileNode> {
        let file = self.files.write().await.remove(&id);
        if let Some(ref f) = file {
            let _ = self.event_sender.send(SubscriptionEvent::FileDeleted {
                id,
                path: f.path.clone(),
            });
        }
        file
    }

    /// Subscribe to events
    pub fn subscribe(&self) -> broadcast::Receiver<SubscriptionEvent> {
        self.event_sender.subscribe()
    }
}

impl Default for ResolverContext {
    fn default() -> Self {
        Self::new()
    }
}

/// GraphQL executor
pub struct GraphQLExecutor {
    schema: Schema,
    context: Arc<ResolverContext>,
}

impl GraphQLExecutor {
    pub fn new(context: Arc<ResolverContext>) -> Self {
        Self {
            schema: Self::build_schema(),
            context,
        }
    }

    fn build_schema() -> Schema {
        let mut schema = Schema::new();

        // File type
        schema.add_type(TypeDef {
            name: "File".to_string(),
            description: Some("A file or directory in the filesystem".to_string()),
            fields: vec![
                FieldDef {
                    name: "id".to_string(),
                    field_type: FieldType::ID.required(),
                    description: Some("Unique inode ID".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
                FieldDef {
                    name: "name".to_string(),
                    field_type: FieldType::String.required(),
                    description: Some("File name".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
                FieldDef {
                    name: "path".to_string(),
                    field_type: FieldType::String.required(),
                    description: Some("Full path".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
                FieldDef {
                    name: "size".to_string(),
                    field_type: FieldType::Int.required(),
                    description: Some("File size in bytes".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
                FieldDef {
                    name: "type".to_string(),
                    field_type: FieldType::Enum("FileType".to_string()).required(),
                    description: Some("File type".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
                FieldDef {
                    name: "createdAt".to_string(),
                    field_type: FieldType::DateTime.required(),
                    description: Some("Creation timestamp".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
                FieldDef {
                    name: "modifiedAt".to_string(),
                    field_type: FieldType::DateTime.required(),
                    description: Some("Last modification timestamp".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
                FieldDef {
                    name: "tags".to_string(),
                    field_type: FieldType::String.list(),
                    description: Some("Associated tags".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
                FieldDef {
                    name: "children".to_string(),
                    field_type: FieldType::Object("FileConnection".to_string()),
                    description: Some("Child files (for directories)".to_string()),
                    arguments: vec![
                        ArgumentDef {
                            name: "first".to_string(),
                            arg_type: FieldType::Int,
                            default_value: None,
                            description: Some("Limit results".to_string()),
                        },
                        ArgumentDef {
                            name: "after".to_string(),
                            arg_type: FieldType::String,
                            default_value: None,
                            description: Some("Cursor for pagination".to_string()),
                        },
                    ],
                    deprecated: None,
                },
            ],
            interfaces: vec!["Node".to_string()],
        });

        // FileType enum
        schema.add_enum(EnumDef {
            name: "FileType".to_string(),
            description: Some("Type of file system entry".to_string()),
            values: vec![
                EnumValue {
                    name: "FILE".to_string(),
                    description: Some("Regular file".to_string()),
                    deprecated: None,
                },
                EnumValue {
                    name: "DIRECTORY".to_string(),
                    description: Some("Directory".to_string()),
                    deprecated: None,
                },
                EnumValue {
                    name: "SYMLINK".to_string(),
                    description: Some("Symbolic link".to_string()),
                    deprecated: None,
                },
            ],
        });

        // Query type
        schema.add_type(TypeDef {
            name: "Query".to_string(),
            description: Some("Root query type".to_string()),
            fields: vec![
                FieldDef {
                    name: "file".to_string(),
                    field_type: FieldType::Object("File".to_string()),
                    description: Some("Get a file by ID or path".to_string()),
                    arguments: vec![
                        ArgumentDef {
                            name: "id".to_string(),
                            arg_type: FieldType::ID,
                            default_value: None,
                            description: Some("File ID".to_string()),
                        },
                        ArgumentDef {
                            name: "path".to_string(),
                            arg_type: FieldType::String,
                            default_value: None,
                            description: Some("File path".to_string()),
                        },
                    ],
                    deprecated: None,
                },
                FieldDef {
                    name: "files".to_string(),
                    field_type: FieldType::Object("FileConnection".to_string()).required(),
                    description: Some("List files".to_string()),
                    arguments: vec![
                        ArgumentDef {
                            name: "first".to_string(),
                            arg_type: FieldType::Int,
                            default_value: Some(GraphQLValue::Int(10)),
                            description: None,
                        },
                        ArgumentDef {
                            name: "after".to_string(),
                            arg_type: FieldType::String,
                            default_value: None,
                            description: None,
                        },
                        ArgumentDef {
                            name: "filter".to_string(),
                            arg_type: FieldType::Object("FileFilter".to_string()),
                            default_value: None,
                            description: Some("Filter criteria".to_string()),
                        },
                    ],
                    deprecated: None,
                },
                FieldDef {
                    name: "stats".to_string(),
                    field_type: FieldType::Object("Stats".to_string()).required(),
                    description: Some("System statistics".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
            ],
            interfaces: vec![],
        });

        // Mutation type
        schema.add_type(TypeDef {
            name: "Mutation".to_string(),
            description: Some("Root mutation type".to_string()),
            fields: vec![
                FieldDef {
                    name: "createFile".to_string(),
                    field_type: FieldType::Object("File".to_string()).required(),
                    description: Some("Create a new file".to_string()),
                    arguments: vec![
                        ArgumentDef {
                            name: "input".to_string(),
                            arg_type: FieldType::Object("CreateFileInput".to_string()).required(),
                            default_value: None,
                            description: None,
                        },
                    ],
                    deprecated: None,
                },
                FieldDef {
                    name: "updateFile".to_string(),
                    field_type: FieldType::Object("File".to_string()),
                    description: Some("Update a file".to_string()),
                    arguments: vec![
                        ArgumentDef {
                            name: "id".to_string(),
                            arg_type: FieldType::ID.required(),
                            default_value: None,
                            description: None,
                        },
                        ArgumentDef {
                            name: "input".to_string(),
                            arg_type: FieldType::Object("UpdateFileInput".to_string()).required(),
                            default_value: None,
                            description: None,
                        },
                    ],
                    deprecated: None,
                },
                FieldDef {
                    name: "deleteFile".to_string(),
                    field_type: FieldType::Boolean.required(),
                    description: Some("Delete a file".to_string()),
                    arguments: vec![
                        ArgumentDef {
                            name: "id".to_string(),
                            arg_type: FieldType::ID.required(),
                            default_value: None,
                            description: None,
                        },
                    ],
                    deprecated: None,
                },
                FieldDef {
                    name: "addTag".to_string(),
                    field_type: FieldType::Object("File".to_string()),
                    description: Some("Add a tag to a file".to_string()),
                    arguments: vec![
                        ArgumentDef {
                            name: "id".to_string(),
                            arg_type: FieldType::ID.required(),
                            default_value: None,
                            description: None,
                        },
                        ArgumentDef {
                            name: "tag".to_string(),
                            arg_type: FieldType::String.required(),
                            default_value: None,
                            description: None,
                        },
                    ],
                    deprecated: None,
                },
            ],
            interfaces: vec![],
        });

        // Subscription type
        schema.add_type(TypeDef {
            name: "Subscription".to_string(),
            description: Some("Root subscription type".to_string()),
            fields: vec![
                FieldDef {
                    name: "fileCreated".to_string(),
                    field_type: FieldType::Object("File".to_string()).required(),
                    description: Some("Subscribe to file creations".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
                FieldDef {
                    name: "fileModified".to_string(),
                    field_type: FieldType::Object("File".to_string()).required(),
                    description: Some("Subscribe to file modifications".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
                FieldDef {
                    name: "fileDeleted".to_string(),
                    field_type: FieldType::Object("FileDeleted".to_string()).required(),
                    description: Some("Subscribe to file deletions".to_string()),
                    arguments: vec![],
                    deprecated: None,
                },
            ],
            interfaces: vec![],
        });

        schema
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Execute a query
    pub async fn execute(&self, query: &Query) -> QueryResult {
        let mut data = HashMap::new();
        let mut errors = Vec::new();

        for selection in &query.selections {
            match self.resolve_selection(selection).await {
                Ok(value) => {
                    let key = selection.alias.clone().unwrap_or_else(|| selection.name.clone());
                    data.insert(key, value);
                }
                Err(e) => {
                    errors.push(GraphQLError {
                        message: e.to_string(),
                        locations: vec![],
                        path: vec![PathSegment::Field(selection.name.clone())],
                        extensions: HashMap::new(),
                    });
                }
            }
        }

        QueryResult {
            data: if data.is_empty() { None } else { Some(GraphQLValue::Object(data)) },
            errors,
        }
    }

    async fn resolve_selection(&self, selection: &Selection) -> Result<GraphQLValue> {
        match selection.name.as_str() {
            "file" => self.resolve_file(selection).await,
            "files" => self.resolve_files(selection).await,
            "stats" => self.resolve_stats().await,
            _ => Err(StrataError::InvalidData(format!(
                "Unknown field: {}",
                selection.name
            ))),
        }
    }

    async fn resolve_file(&self, selection: &Selection) -> Result<GraphQLValue> {
        let id = selection.arguments.get("id")
            .and_then(|v| v.as_int())
            .map(|v| v as u64);

        let path = selection.arguments.get("path")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());

        let files = self.context.files.read().await;

        let file = if let Some(id) = id {
            files.get(&id).cloned()
        } else if let Some(path) = path {
            files.values().find(|f| f.path == path).cloned()
        } else {
            None
        };

        match file {
            Some(f) => Ok(self.file_to_graphql(&f, &selection.selections)),
            None => Ok(GraphQLValue::Null),
        }
    }

    async fn resolve_files(&self, selection: &Selection) -> Result<GraphQLValue> {
        let first = selection.arguments.get("first")
            .and_then(|v| v.as_int())
            .map(|v| v as usize)
            .unwrap_or(10);

        let files = self.context.files.read().await;
        let file_list: Vec<_> = files.values().take(first).cloned().collect();

        let edges: Vec<_> = file_list.iter()
            .map(|f| {
                let mut edge = HashMap::new();
                edge.insert("node".to_string(), self.file_to_graphql(f, &selection.selections));
                edge.insert("cursor".to_string(), GraphQLValue::String(f.id.to_string()));
                GraphQLValue::Object(edge)
            })
            .collect();

        let mut page_info = HashMap::new();
        page_info.insert("hasNextPage".to_string(), GraphQLValue::Boolean(file_list.len() >= first));
        page_info.insert("hasPreviousPage".to_string(), GraphQLValue::Boolean(false));

        let mut connection = HashMap::new();
        connection.insert("edges".to_string(), GraphQLValue::List(edges));
        connection.insert("pageInfo".to_string(), GraphQLValue::Object(page_info));
        connection.insert("totalCount".to_string(), GraphQLValue::Int(files.len() as i64));

        Ok(GraphQLValue::Object(connection))
    }

    async fn resolve_stats(&self) -> Result<GraphQLValue> {
        let stats = self.context.stats.read().await;

        let mut obj = HashMap::new();
        obj.insert("totalFiles".to_string(), GraphQLValue::Int(stats.total_files as i64));
        obj.insert("totalDirectories".to_string(), GraphQLValue::Int(stats.total_directories as i64));
        obj.insert("totalSize".to_string(), GraphQLValue::Int(stats.total_size as i64));
        obj.insert("totalChunks".to_string(), GraphQLValue::Int(stats.total_chunks as i64));
        obj.insert("dedupRatio".to_string(), GraphQLValue::Float(stats.dedup_ratio));
        obj.insert("compressionRatio".to_string(), GraphQLValue::Float(stats.compression_ratio));

        Ok(GraphQLValue::Object(obj))
    }

    fn file_to_graphql(&self, file: &FileNode, selections: &[Selection]) -> GraphQLValue {
        let mut obj = HashMap::new();

        for sel in selections {
            let value = match sel.name.as_str() {
                "id" => GraphQLValue::String(file.id.to_string()),
                "name" => GraphQLValue::String(file.name.clone()),
                "path" => GraphQLValue::String(file.path.clone()),
                "size" => GraphQLValue::Int(file.size as i64),
                "type" => GraphQLValue::Enum(match file.file_type {
                    FileType::File => "FILE".to_string(),
                    FileType::Directory => "DIRECTORY".to_string(),
                    FileType::Symlink => "SYMLINK".to_string(),
                }),
                "createdAt" => GraphQLValue::String(file.created_at.to_rfc3339()),
                "modifiedAt" => GraphQLValue::String(file.modified_at.to_rfc3339()),
                "owner" => GraphQLValue::String(file.owner.clone()),
                "permissions" => GraphQLValue::Int(file.permissions as i64),
                "tags" => GraphQLValue::List(
                    file.tags.iter().map(|t| GraphQLValue::String(t.clone())).collect()
                ),
                _ => continue,
            };
            let key = sel.alias.clone().unwrap_or_else(|| sel.name.clone());
            obj.insert(key, value);
        }

        GraphQLValue::Object(obj)
    }

    /// Execute a mutation
    pub async fn execute_mutation(&self, name: &str, args: &HashMap<String, GraphQLValue>) -> Result<GraphQLValue> {
        match name {
            "createFile" => {
                let input = args.get("input")
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| StrataError::InvalidData("Missing input".into()))?;

                let name = input.get("name")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| StrataError::InvalidData("Missing name".into()))?
                    .to_string();

                let path = input.get("path")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| StrataError::InvalidData("Missing path".into()))?
                    .to_string();

                let file = FileNode {
                    id: rand::random(),
                    name,
                    path,
                    size: 0,
                    file_type: FileType::File,
                    created_at: Utc::now(),
                    modified_at: Utc::now(),
                    owner: "root".to_string(),
                    permissions: 0o644,
                    tags: vec![],
                    metadata: HashMap::new(),
                };

                self.context.add_file(file.clone()).await;

                Ok(self.file_to_graphql(&file, &[
                    Selection { name: "id".to_string(), alias: None, arguments: HashMap::new(), selections: vec![] },
                    Selection { name: "name".to_string(), alias: None, arguments: HashMap::new(), selections: vec![] },
                    Selection { name: "path".to_string(), alias: None, arguments: HashMap::new(), selections: vec![] },
                ]))
            }
            "deleteFile" => {
                let id = args.get("id")
                    .and_then(|v| v.as_int())
                    .ok_or_else(|| StrataError::InvalidData("Missing id".into()))?;

                let deleted = self.context.delete_file(id as u64).await.is_some();
                Ok(GraphQLValue::Boolean(deleted))
            }
            "addTag" => {
                let id = args.get("id")
                    .and_then(|v| v.as_int())
                    .ok_or_else(|| StrataError::InvalidData("Missing id".into()))? as u64;

                let tag = args.get("tag")
                    .and_then(|v| v.as_string())
                    .ok_or_else(|| StrataError::InvalidData("Missing tag".into()))?
                    .to_string();

                let mut files = self.context.files.write().await;
                if let Some(file) = files.get_mut(&id) {
                    file.tags.push(tag);
                    return Ok(self.file_to_graphql(file, &[
                        Selection { name: "id".to_string(), alias: None, arguments: HashMap::new(), selections: vec![] },
                        Selection { name: "tags".to_string(), alias: None, arguments: HashMap::new(), selections: vec![] },
                    ]));
                }
                Ok(GraphQLValue::Null)
            }
            _ => Err(StrataError::InvalidData(format!("Unknown mutation: {}", name))),
        }
    }
}

/// Query builder for programmatic query construction
pub struct QueryBuilder {
    operation_name: Option<String>,
    selections: Vec<Selection>,
    variables: HashMap<String, GraphQLValue>,
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self {
            operation_name: None,
            selections: Vec::new(),
            variables: HashMap::new(),
        }
    }

    pub fn operation_name(mut self, name: &str) -> Self {
        self.operation_name = Some(name.to_string());
        self
    }

    pub fn select(mut self, field: &str) -> Self {
        self.selections.push(Selection {
            name: field.to_string(),
            alias: None,
            arguments: HashMap::new(),
            selections: Vec::new(),
        });
        self
    }

    pub fn select_with_args(mut self, field: &str, args: HashMap<String, GraphQLValue>) -> Self {
        self.selections.push(Selection {
            name: field.to_string(),
            alias: None,
            arguments: args,
            selections: Vec::new(),
        });
        self
    }

    pub fn variable(mut self, name: &str, value: GraphQLValue) -> Self {
        self.variables.insert(name.to_string(), value);
        self
    }

    pub fn build(self) -> Query {
        Query {
            operation_name: self.operation_name,
            selections: self.selections,
            variables: self.variables,
        }
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_type() {
        let t = FieldType::String.required();
        assert!(matches!(t, FieldType::NonNull(_)));

        let t = FieldType::String.list();
        assert!(matches!(t, FieldType::List(_)));
    }

    #[test]
    fn test_graphql_value() {
        let v = GraphQLValue::Int(42);
        assert_eq!(v.as_int(), Some(42));
        assert_eq!(v.as_string(), None);

        let v = GraphQLValue::String("hello".to_string());
        assert_eq!(v.as_string(), Some("hello"));
        assert_eq!(v.as_int(), None);

        let v = GraphQLValue::Boolean(true);
        assert_eq!(v.as_bool(), Some(true));
    }

    #[test]
    fn test_schema_building() {
        let schema = GraphQLExecutor::build_schema();

        assert!(schema.get_type("File").is_some());
        assert!(schema.get_type("Query").is_some());
        assert!(schema.get_type("Mutation").is_some());
        assert!(schema.enums.contains_key("FileType"));
    }

    #[tokio::test]
    async fn test_resolver_context() {
        let ctx = ResolverContext::new();

        let file = FileNode {
            id: 1,
            name: "test.txt".to_string(),
            path: "/test.txt".to_string(),
            size: 100,
            file_type: FileType::File,
            created_at: Utc::now(),
            modified_at: Utc::now(),
            owner: "root".to_string(),
            permissions: 0o644,
            tags: vec!["test".to_string()],
            metadata: HashMap::new(),
        };

        ctx.add_file(file.clone()).await;

        let files = ctx.files.read().await;
        assert!(files.contains_key(&1));

        let stats = ctx.stats.read().await;
        assert_eq!(stats.total_files, 1);
    }

    #[tokio::test]
    async fn test_query_execution() {
        let ctx = Arc::new(ResolverContext::new());
        let executor = GraphQLExecutor::new(ctx.clone());

        // Add a test file
        let file = FileNode {
            id: 1,
            name: "test.txt".to_string(),
            path: "/test.txt".to_string(),
            size: 100,
            file_type: FileType::File,
            created_at: Utc::now(),
            modified_at: Utc::now(),
            owner: "root".to_string(),
            permissions: 0o644,
            tags: vec![],
            metadata: HashMap::new(),
        };
        ctx.add_file(file).await;

        // Query stats
        let query = QueryBuilder::new()
            .select("stats")
            .build();

        let result = executor.execute(&query).await;
        assert!(result.errors.is_empty());
        assert!(result.data.is_some());
    }

    #[tokio::test]
    async fn test_mutation_execution() {
        let ctx = Arc::new(ResolverContext::new());
        let executor = GraphQLExecutor::new(ctx.clone());

        // Create file mutation
        let mut input = HashMap::new();
        input.insert("name".to_string(), GraphQLValue::String("new.txt".to_string()));
        input.insert("path".to_string(), GraphQLValue::String("/new.txt".to_string()));

        let mut args = HashMap::new();
        args.insert("input".to_string(), GraphQLValue::Object(input));

        let result = executor.execute_mutation("createFile", &args).await.unwrap();
        assert!(matches!(result, GraphQLValue::Object(_)));

        // Verify file was created
        let files = ctx.files.read().await;
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_subscriptions() {
        let ctx = Arc::new(ResolverContext::new());
        let mut receiver = ctx.subscribe();

        let file = FileNode {
            id: 1,
            name: "test.txt".to_string(),
            path: "/test.txt".to_string(),
            size: 100,
            file_type: FileType::File,
            created_at: Utc::now(),
            modified_at: Utc::now(),
            owner: "root".to_string(),
            permissions: 0o644,
            tags: vec![],
            metadata: HashMap::new(),
        };

        ctx.add_file(file).await;

        // Should receive the event
        let event = receiver.try_recv().unwrap();
        assert!(matches!(event, SubscriptionEvent::FileCreated(_)));
    }

    #[test]
    fn test_query_builder() {
        let query = QueryBuilder::new()
            .operation_name("GetFile")
            .select("file")
            .variable("id", GraphQLValue::Int(1))
            .build();

        assert_eq!(query.operation_name, Some("GetFile".to_string()));
        assert_eq!(query.selections.len(), 1);
        assert!(query.variables.contains_key("id"));
    }

    #[test]
    fn test_pagination() {
        let page_info = PageInfo {
            has_next_page: true,
            has_previous_page: false,
            start_cursor: Some("abc".to_string()),
            end_cursor: Some("xyz".to_string()),
        };

        assert!(page_info.has_next_page);
        assert!(!page_info.has_previous_page);
    }
}
