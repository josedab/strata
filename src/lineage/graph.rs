// Lineage Graph for Data Provenance

use crate::error::{Result, StrataError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Lineage node representing a data asset or process
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageNode {
    /// Node ID
    pub id: String,
    /// Node type
    pub node_type: NodeType,
    /// Asset ID (if this represents an asset)
    pub asset_id: Option<String>,
    /// Display name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Properties
    pub properties: HashMap<String, serde_json::Value>,
    /// Creation timestamp
    pub created_at: u64,
}

impl LineageNode {
    /// Creates a new lineage node
    pub fn new(id: impl Into<String>, node_type: NodeType, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            node_type,
            asset_id: None,
            name: name.into(),
            description: None,
            properties: HashMap::new(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Links to an asset
    pub fn with_asset(mut self, asset_id: &str) -> Self {
        self.asset_id = Some(asset_id.to_string());
        self
    }

    /// Adds description
    pub fn with_description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    /// Adds property
    pub fn with_property(mut self, key: &str, value: serde_json::Value) -> Self {
        self.properties.insert(key.to_string(), value);
        self
    }
}

/// Node type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    /// Data source
    Source,
    /// Data sink/destination
    Sink,
    /// Transformation process
    Transform,
    /// ETL job
    Job,
    /// Query
    Query,
    /// API call
    Api,
    /// Manual action
    Manual,
    /// System process
    System,
    /// External system
    External,
}

/// Lineage edge representing data flow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// Edge ID
    pub id: String,
    /// Source node ID
    pub source: String,
    /// Target node ID
    pub target: String,
    /// Edge type
    pub edge_type: EdgeType,
    /// Transformation details
    pub transformation: Option<String>,
    /// Column mappings
    pub column_mappings: Vec<ColumnMapping>,
    /// Properties
    pub properties: HashMap<String, serde_json::Value>,
    /// Timestamp
    pub timestamp: u64,
}

impl LineageEdge {
    /// Creates a new edge
    pub fn new(source: &str, target: &str, edge_type: EdgeType) -> Self {
        let id = format!("{}_{}", source, target);
        Self {
            id,
            source: source.to_string(),
            target: target.to_string(),
            edge_type,
            transformation: None,
            column_mappings: Vec::new(),
            properties: HashMap::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }

    /// Adds transformation description
    pub fn with_transformation(mut self, transform: &str) -> Self {
        self.transformation = Some(transform.to_string());
        self
    }

    /// Adds column mapping
    pub fn with_column_mapping(mut self, mapping: ColumnMapping) -> Self {
        self.column_mappings.push(mapping);
        self
    }
}

/// Edge type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeType {
    /// Direct data flow
    DataFlow,
    /// Derived/computed
    Derived,
    /// Copied
    Copied,
    /// Aggregated
    Aggregated,
    /// Filtered
    Filtered,
    /// Joined
    Joined,
    /// Read access
    Read,
    /// Write access
    Write,
    /// Schema reference
    Schema,
}

/// Column-level lineage mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMapping {
    /// Source column
    pub source_column: String,
    /// Target column
    pub target_column: String,
    /// Transformation expression
    pub expression: Option<String>,
    /// Confidence score (0-1)
    pub confidence: f32,
}

/// Provenance information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    /// Asset ID
    pub asset_id: String,
    /// Upstream lineage (data sources)
    pub upstream: Vec<LineageNode>,
    /// Downstream lineage (consumers)
    pub downstream: Vec<LineageNode>,
    /// Edges
    pub edges: Vec<LineageEdge>,
    /// Impact analysis results
    pub impact: Option<ImpactAnalysis>,
}

/// Impact analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImpactAnalysis {
    /// Affected downstream assets
    pub affected_assets: Vec<String>,
    /// Affected jobs/pipelines
    pub affected_jobs: Vec<String>,
    /// Risk level
    pub risk_level: RiskLevel,
    /// Summary
    pub summary: String,
}

/// Risk level for impact analysis
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

/// Lineage graph
pub struct LineageGraph {
    /// Nodes
    nodes: Arc<RwLock<HashMap<String, LineageNode>>>,
    /// Edges
    edges: Arc<RwLock<HashMap<String, LineageEdge>>>,
    /// Outgoing edges: node_id -> edge_ids
    outgoing: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Incoming edges: node_id -> edge_ids
    incoming: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl LineageGraph {
    /// Creates a new lineage graph
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            edges: Arc::new(RwLock::new(HashMap::new())),
            outgoing: Arc::new(RwLock::new(HashMap::new())),
            incoming: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Adds a node
    pub async fn add_node(&self, node: LineageNode) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node.id.clone(), node);
        Ok(())
    }

    /// Removes a node
    pub async fn remove_node(&self, id: &str) -> Result<bool> {
        // Get edges to remove
        let edges_to_remove: Vec<String> = {
            let outgoing = self.outgoing.read().await;
            let incoming = self.incoming.read().await;

            let mut edges = Vec::new();
            if let Some(out_edges) = outgoing.get(id) {
                edges.extend(out_edges.clone());
            }
            if let Some(in_edges) = incoming.get(id) {
                edges.extend(in_edges.clone());
            }
            edges
        };

        // Remove edges
        for edge_id in edges_to_remove {
            self.remove_edge(&edge_id).await?;
        }

        // Remove node
        let mut nodes = self.nodes.write().await;
        Ok(nodes.remove(id).is_some())
    }

    /// Gets a node
    pub async fn get_node(&self, id: &str) -> Option<LineageNode> {
        let nodes = self.nodes.read().await;
        nodes.get(id).cloned()
    }

    /// Adds an edge
    pub async fn add_edge(&self, edge: LineageEdge) -> Result<()> {
        // Validate nodes exist
        {
            let nodes = self.nodes.read().await;
            if !nodes.contains_key(&edge.source) {
                return Err(StrataError::NotFound(format!("Source node {} not found", edge.source)));
            }
            if !nodes.contains_key(&edge.target) {
                return Err(StrataError::NotFound(format!("Target node {} not found", edge.target)));
            }
        }

        let edge_id = edge.id.clone();
        let source = edge.source.clone();
        let target = edge.target.clone();

        // Store edge
        {
            let mut edges = self.edges.write().await;
            edges.insert(edge_id.clone(), edge);
        }

        // Update indexes
        {
            let mut outgoing = self.outgoing.write().await;
            outgoing.entry(source).or_insert_with(Vec::new).push(edge_id.clone());
        }
        {
            let mut incoming = self.incoming.write().await;
            incoming.entry(target).or_insert_with(Vec::new).push(edge_id);
        }

        Ok(())
    }

    /// Removes an edge
    pub async fn remove_edge(&self, id: &str) -> Result<bool> {
        let edge = {
            let mut edges = self.edges.write().await;
            edges.remove(id)
        };

        let edge = match edge {
            Some(e) => e,
            None => return Ok(false),
        };

        // Update indexes
        {
            let mut outgoing = self.outgoing.write().await;
            if let Some(edges) = outgoing.get_mut(&edge.source) {
                edges.retain(|e| e != id);
            }
        }
        {
            let mut incoming = self.incoming.write().await;
            if let Some(edges) = incoming.get_mut(&edge.target) {
                edges.retain(|e| e != id);
            }
        }

        Ok(true)
    }

    /// Gets upstream nodes (data sources)
    pub async fn get_upstream(&self, node_id: &str, depth: usize) -> Vec<LineageNode> {
        self.traverse(node_id, depth, TraversalDirection::Upstream).await
    }

    /// Gets downstream nodes (consumers)
    pub async fn get_downstream(&self, node_id: &str, depth: usize) -> Vec<LineageNode> {
        self.traverse(node_id, depth, TraversalDirection::Downstream).await
    }

    /// Traverses the graph
    async fn traverse(&self, start: &str, max_depth: usize, direction: TraversalDirection) -> Vec<LineageNode> {
        let nodes = self.nodes.read().await;
        let edges = self.edges.read().await;
        let outgoing = self.outgoing.read().await;
        let incoming = self.incoming.read().await;

        let mut visited = HashSet::new();
        let mut result = Vec::new();
        let mut queue = VecDeque::new();

        queue.push_back((start.to_string(), 0usize));
        visited.insert(start.to_string());

        while let Some((current, depth)) = queue.pop_front() {
            if depth > max_depth {
                continue;
            }

            // Get adjacent edges based on direction
            let adjacent_edges = match direction {
                TraversalDirection::Upstream => incoming.get(&current),
                TraversalDirection::Downstream => outgoing.get(&current),
            };

            if let Some(edge_ids) = adjacent_edges {
                for edge_id in edge_ids {
                    if let Some(edge) = edges.get(edge_id) {
                        let next_node = match direction {
                            TraversalDirection::Upstream => &edge.source,
                            TraversalDirection::Downstream => &edge.target,
                        };

                        if !visited.contains(next_node) {
                            visited.insert(next_node.clone());
                            if let Some(node) = nodes.get(next_node) {
                                result.push(node.clone());
                            }
                            if depth < max_depth {
                                queue.push_back((next_node.clone(), depth + 1));
                            }
                        }
                    }
                }
            }
        }

        result
    }

    /// Gets edges between nodes
    pub async fn get_edges(&self, source: &str, target: &str) -> Vec<LineageEdge> {
        let edges = self.edges.read().await;
        let outgoing = self.outgoing.read().await;

        let mut result = Vec::new();
        if let Some(edge_ids) = outgoing.get(source) {
            for edge_id in edge_ids {
                if let Some(edge) = edges.get(edge_id) {
                    if edge.target == target {
                        result.push(edge.clone());
                    }
                }
            }
        }
        result
    }

    /// Gets full provenance for an asset
    pub async fn get_provenance(&self, asset_id: &str) -> Option<Provenance> {
        // Find node for asset
        let node_id = {
            let nodes = self.nodes.read().await;
            nodes.values()
                .find(|n| n.asset_id.as_ref() == Some(&asset_id.to_string()))
                .map(|n| n.id.clone())
        };

        let node_id = node_id?;

        let upstream = self.get_upstream(&node_id, 10).await;
        let downstream = self.get_downstream(&node_id, 10).await;

        // Collect edges
        let mut edges = Vec::new();
        let all_edges = self.edges.read().await;
        for edge in all_edges.values() {
            let source_in_path = upstream.iter().any(|n| n.id == edge.source)
                || edge.source == node_id;
            let target_in_path = downstream.iter().any(|n| n.id == edge.target)
                || edge.target == node_id;

            if source_in_path || target_in_path {
                edges.push(edge.clone());
            }
        }

        Some(Provenance {
            asset_id: asset_id.to_string(),
            upstream,
            downstream,
            edges,
            impact: None,
        })
    }

    /// Performs impact analysis
    pub async fn impact_analysis(&self, node_id: &str) -> ImpactAnalysis {
        let downstream = self.get_downstream(node_id, 100).await;

        let affected_assets: Vec<String> = downstream
            .iter()
            .filter_map(|n| n.asset_id.clone())
            .collect();

        let affected_jobs: Vec<String> = downstream
            .iter()
            .filter(|n| n.node_type == NodeType::Job)
            .map(|n| n.id.clone())
            .collect();

        let risk_level = if affected_jobs.len() > 10 || affected_assets.len() > 50 {
            RiskLevel::Critical
        } else if affected_jobs.len() > 5 || affected_assets.len() > 20 {
            RiskLevel::High
        } else if affected_jobs.len() > 2 || affected_assets.len() > 5 {
            RiskLevel::Medium
        } else {
            RiskLevel::Low
        };

        ImpactAnalysis {
            affected_assets,
            affected_jobs,
            risk_level,
            summary: format!(
                "Changes may affect {} downstream assets and {} jobs",
                downstream.iter().filter(|n| n.asset_id.is_some()).count(),
                downstream.iter().filter(|n| n.node_type == NodeType::Job).count()
            ),
        }
    }

    /// Finds path between nodes
    pub async fn find_path(&self, from: &str, to: &str) -> Option<Vec<LineageNode>> {
        let nodes = self.nodes.read().await;
        let outgoing = self.outgoing.read().await;
        let edges = self.edges.read().await;

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut parents: HashMap<String, String> = HashMap::new();

        queue.push_back(from.to_string());
        visited.insert(from.to_string());

        while let Some(current) = queue.pop_front() {
            if current == to {
                // Reconstruct path
                let mut path = Vec::new();
                let mut node = to.to_string();
                while let Some(n) = nodes.get(&node) {
                    path.push(n.clone());
                    if node == from {
                        break;
                    }
                    if let Some(parent) = parents.get(&node) {
                        node = parent.clone();
                    } else {
                        break;
                    }
                }
                path.reverse();
                return Some(path);
            }

            if let Some(edge_ids) = outgoing.get(&current) {
                for edge_id in edge_ids {
                    if let Some(edge) = edges.get(edge_id) {
                        if !visited.contains(&edge.target) {
                            visited.insert(edge.target.clone());
                            parents.insert(edge.target.clone(), current.clone());
                            queue.push_back(edge.target.clone());
                        }
                    }
                }
            }
        }

        None
    }

    /// Gets all nodes
    pub async fn all_nodes(&self) -> Vec<LineageNode> {
        let nodes = self.nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Gets all edges
    pub async fn all_edges(&self) -> Vec<LineageEdge> {
        let edges = self.edges.read().await;
        edges.values().cloned().collect()
    }

    /// Gets node count
    pub async fn node_count(&self) -> usize {
        self.nodes.read().await.len()
    }

    /// Gets edge count
    pub async fn edge_count(&self) -> usize {
        self.edges.read().await.len()
    }
}

impl Default for LineageGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Traversal direction
enum TraversalDirection {
    Upstream,
    Downstream,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_lineage_graph() {
        let graph = LineageGraph::new();

        // Add nodes
        graph.add_node(LineageNode::new("source", NodeType::Source, "Raw Data")).await.unwrap();
        graph.add_node(LineageNode::new("transform", NodeType::Transform, "ETL Job")).await.unwrap();
        graph.add_node(LineageNode::new("sink", NodeType::Sink, "Data Warehouse")).await.unwrap();

        // Add edges
        graph.add_edge(LineageEdge::new("source", "transform", EdgeType::DataFlow)).await.unwrap();
        graph.add_edge(LineageEdge::new("transform", "sink", EdgeType::Derived)).await.unwrap();

        assert_eq!(graph.node_count().await, 3);
        assert_eq!(graph.edge_count().await, 2);
    }

    #[tokio::test]
    async fn test_upstream_downstream() {
        let graph = LineageGraph::new();

        graph.add_node(LineageNode::new("a", NodeType::Source, "A")).await.unwrap();
        graph.add_node(LineageNode::new("b", NodeType::Transform, "B")).await.unwrap();
        graph.add_node(LineageNode::new("c", NodeType::Transform, "C")).await.unwrap();
        graph.add_node(LineageNode::new("d", NodeType::Sink, "D")).await.unwrap();

        graph.add_edge(LineageEdge::new("a", "b", EdgeType::DataFlow)).await.unwrap();
        graph.add_edge(LineageEdge::new("b", "c", EdgeType::DataFlow)).await.unwrap();
        graph.add_edge(LineageEdge::new("c", "d", EdgeType::DataFlow)).await.unwrap();

        // Check upstream
        let upstream = graph.get_upstream("d", 10).await;
        assert_eq!(upstream.len(), 3);

        // Check downstream
        let downstream = graph.get_downstream("a", 10).await;
        assert_eq!(downstream.len(), 3);
    }

    #[tokio::test]
    async fn test_find_path() {
        let graph = LineageGraph::new();

        graph.add_node(LineageNode::new("a", NodeType::Source, "A")).await.unwrap();
        graph.add_node(LineageNode::new("b", NodeType::Transform, "B")).await.unwrap();
        graph.add_node(LineageNode::new("c", NodeType::Sink, "C")).await.unwrap();

        graph.add_edge(LineageEdge::new("a", "b", EdgeType::DataFlow)).await.unwrap();
        graph.add_edge(LineageEdge::new("b", "c", EdgeType::DataFlow)).await.unwrap();

        let path = graph.find_path("a", "c").await.unwrap();
        assert_eq!(path.len(), 3);
        assert_eq!(path[0].id, "a");
        assert_eq!(path[1].id, "b");
        assert_eq!(path[2].id, "c");
    }

    #[tokio::test]
    async fn test_impact_analysis() {
        let graph = LineageGraph::new();

        graph.add_node(LineageNode::new("source", NodeType::Source, "Source").with_asset("asset-1")).await.unwrap();
        graph.add_node(LineageNode::new("job1", NodeType::Job, "Job 1")).await.unwrap();
        graph.add_node(LineageNode::new("job2", NodeType::Job, "Job 2")).await.unwrap();
        graph.add_node(LineageNode::new("sink", NodeType::Sink, "Sink").with_asset("asset-2")).await.unwrap();

        graph.add_edge(LineageEdge::new("source", "job1", EdgeType::DataFlow)).await.unwrap();
        graph.add_edge(LineageEdge::new("source", "job2", EdgeType::DataFlow)).await.unwrap();
        graph.add_edge(LineageEdge::new("job1", "sink", EdgeType::DataFlow)).await.unwrap();
        graph.add_edge(LineageEdge::new("job2", "sink", EdgeType::DataFlow)).await.unwrap();

        let impact = graph.impact_analysis("source").await;
        assert_eq!(impact.affected_jobs.len(), 2);
        assert_eq!(impact.affected_assets.len(), 1);
    }
}
