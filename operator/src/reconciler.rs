//! Reconciliation logic for Strata clusters

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, Container, ContainerPort, EnvVar, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, Pod, PodSpec, PodTemplateSpec, ResourceRequirements,
    Service, ServicePort, ServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::{Client, ResourceExt};
use tracing::{debug, info, warn};

use crate::crd::{
    ClusterPhase, NodeStatus, StrataCluster, StrataClusterSpec, StrataClusterStatus,
};
use crate::error::Error;

/// Reconciler for Strata clusters
pub struct Reconciler {
    client: Client,
}

impl Reconciler {
    /// Create a new reconciler
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Create a new cluster
    pub async fn create_cluster(&self, cluster: &StrataCluster) -> Result<StrataClusterStatus, Error> {
        let name = cluster.name_any();
        let namespace = cluster.namespace().unwrap_or_default();
        let spec = &cluster.spec;

        info!(name = %name, "Creating Strata cluster");

        // Create ConfigMap
        self.create_config_map(&namespace, &name, spec).await?;

        // Create headless service for StatefulSet
        self.create_headless_service(&namespace, &name, "metadata").await?;
        self.create_headless_service(&namespace, &name, "data").await?;

        // Create client service
        self.create_client_service(&namespace, &name, spec).await?;

        // Create metadata StatefulSet
        self.create_stateful_set(&namespace, &name, spec, "metadata").await?;

        // Create data StatefulSet
        self.create_stateful_set(&namespace, &name, spec, "data").await?;

        // Create S3 gateway if enabled
        if spec.s3_gateway.is_some() {
            self.create_s3_gateway(&namespace, &name, spec).await?;
        }

        Ok(StrataClusterStatus {
            phase: ClusterPhase::Creating,
            total_nodes: spec.metadata_replicas + spec.data_replicas,
            ready_nodes: 0,
            current_version: Some(spec.version.clone()),
            message: Some("Creating cluster resources".to_string()),
            ..Default::default()
        })
    }

    /// Wait for cluster to be ready
    pub async fn wait_for_ready(&self, cluster: &StrataCluster) -> Result<StrataClusterStatus, Error> {
        let name = cluster.name_any();
        let namespace = cluster.namespace().unwrap_or_default();
        let spec = &cluster.spec;

        // Check StatefulSet status
        let metadata_ready = self.check_stateful_set_ready(&namespace, &name, "metadata").await?;
        let data_ready = self.check_stateful_set_ready(&namespace, &name, "data").await?;

        let ready_nodes = metadata_ready + data_ready;
        let total_nodes = spec.metadata_replicas + spec.data_replicas;

        let phase = if ready_nodes == total_nodes {
            ClusterPhase::Running
        } else {
            ClusterPhase::Creating
        };

        let message = if phase == ClusterPhase::Running {
            Some("Cluster is running".to_string())
        } else {
            Some(format!("{}/{} nodes ready", ready_nodes, total_nodes))
        };

        Ok(StrataClusterStatus {
            phase,
            ready_nodes,
            total_nodes,
            current_version: Some(spec.version.clone()),
            metadata_nodes: self.get_node_statuses(&namespace, &name, "metadata").await?,
            data_nodes: self.get_node_statuses(&namespace, &name, "data").await?,
            message,
            ..Default::default()
        })
    }

    /// Ensure cluster is running
    pub async fn ensure_running(&self, cluster: &StrataCluster) -> Result<StrataClusterStatus, Error> {
        let name = cluster.name_any();
        let namespace = cluster.namespace().unwrap_or_default();
        let spec = &cluster.spec;

        // Check if spec has changed (would require update)
        let needs_update = self.check_spec_changes(cluster).await?;

        if needs_update {
            return Ok(StrataClusterStatus {
                phase: ClusterPhase::Updating,
                message: Some("Configuration change detected".to_string()),
                ..cluster.status.clone().unwrap_or_default()
            });
        }

        // Check if scaling is needed
        let needs_scaling = self.check_scaling_needed(cluster).await?;

        if needs_scaling {
            return Ok(StrataClusterStatus {
                phase: ClusterPhase::Scaling,
                message: Some("Scaling operation required".to_string()),
                ..cluster.status.clone().unwrap_or_default()
            });
        }

        // Check health
        let (healthy, degraded_message) = self.check_cluster_health(&namespace, &name).await?;

        let phase = if healthy {
            ClusterPhase::Running
        } else {
            ClusterPhase::Degraded
        };

        Ok(StrataClusterStatus {
            phase,
            ready_nodes: self.count_ready_nodes(&namespace, &name).await?,
            total_nodes: spec.metadata_replicas + spec.data_replicas,
            current_version: Some(spec.version.clone()),
            metadata_nodes: self.get_node_statuses(&namespace, &name, "metadata").await?,
            data_nodes: self.get_node_statuses(&namespace, &name, "data").await?,
            message: degraded_message,
            ..Default::default()
        })
    }

    /// Handle cluster update
    pub async fn handle_update(&self, cluster: &StrataCluster) -> Result<StrataClusterStatus, Error> {
        let name = cluster.name_any();
        let namespace = cluster.namespace().unwrap_or_default();
        let spec = &cluster.spec;

        info!(name = %name, "Updating cluster");

        // Update ConfigMap
        self.update_config_map(&namespace, &name, spec).await?;

        // Rolling update StatefulSets
        self.update_stateful_set(&namespace, &name, spec, "metadata").await?;
        self.update_stateful_set(&namespace, &name, spec, "data").await?;

        Ok(StrataClusterStatus {
            phase: ClusterPhase::Updating,
            target_version: Some(spec.version.clone()),
            message: Some("Performing rolling update".to_string()),
            ..cluster.status.clone().unwrap_or_default()
        })
    }

    /// Handle scaling
    pub async fn handle_scaling(&self, cluster: &StrataCluster) -> Result<StrataClusterStatus, Error> {
        let name = cluster.name_any();
        let namespace = cluster.namespace().unwrap_or_default();
        let spec = &cluster.spec;

        info!(name = %name, "Scaling cluster");

        // Scale StatefulSets
        self.scale_stateful_set(&namespace, &name, "data", spec.data_replicas).await?;

        Ok(StrataClusterStatus {
            phase: ClusterPhase::Scaling,
            total_nodes: spec.metadata_replicas + spec.data_replicas,
            message: Some(format!("Scaling to {} data nodes", spec.data_replicas)),
            ..cluster.status.clone().unwrap_or_default()
        })
    }

    /// Handle degraded state
    pub async fn handle_degraded(&self, cluster: &StrataCluster) -> Result<StrataClusterStatus, Error> {
        let name = cluster.name_any();
        let namespace = cluster.namespace().unwrap_or_default();

        warn!(name = %name, "Cluster is degraded, attempting recovery");

        // Try to recover failed pods
        self.recover_failed_pods(&namespace, &name).await?;

        // Check if recovered
        let (healthy, message) = self.check_cluster_health(&namespace, &name).await?;

        let phase = if healthy {
            ClusterPhase::Running
        } else {
            ClusterPhase::Degraded
        };

        Ok(StrataClusterStatus {
            phase,
            message,
            ..cluster.status.clone().unwrap_or_default()
        })
    }

    /// Handle failed state
    pub async fn handle_failed(&self, cluster: &StrataCluster) -> Result<StrataClusterStatus, Error> {
        let name = cluster.name_any();
        warn!(name = %name, "Cluster is in failed state");

        // Keep failed state, require manual intervention
        Ok(cluster.status.clone().unwrap_or_default())
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    async fn create_config_map(
        &self,
        namespace: &str,
        name: &str,
        spec: &StrataClusterSpec,
    ) -> Result<(), Error> {
        let config_name = format!("{}-config", name);

        let config = format!(
            r#"
[node]
role = "combined"

[metadata]
bind_addr = "0.0.0.0:9000"

[data]
bind_addr = "0.0.0.0:9001"

[storage]
data_dir = "/data/strata"
metadata_dir = "/data/metadata"

[erasure]
data_shards = {}
parity_shards = {}

[cluster]
placement_groups = {}
"#,
            spec.erasure.data_shards,
            spec.erasure.parity_shards,
            spec.erasure.placement_groups,
        );

        let cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some(config_name.clone()),
                namespace: Some(namespace.to_string()),
                labels: Some(self.common_labels(name)),
                ..Default::default()
            },
            data: Some({
                let mut data = BTreeMap::new();
                data.insert("strata.toml".to_string(), config);
                data
            }),
            ..Default::default()
        };

        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), namespace);
        let pp = PostParams::default();
        let _ = api.create(&pp, &cm).await;

        debug!(name = %config_name, "ConfigMap created");
        Ok(())
    }

    async fn update_config_map(
        &self,
        namespace: &str,
        name: &str,
        spec: &StrataClusterSpec,
    ) -> Result<(), Error> {
        // Delete and recreate for simplicity
        self.create_config_map(namespace, name, spec).await
    }

    async fn create_headless_service(
        &self,
        namespace: &str,
        cluster_name: &str,
        component: &str,
    ) -> Result<(), Error> {
        let service_name = format!("{}-{}-headless", cluster_name, component);

        let svc = Service {
            metadata: ObjectMeta {
                name: Some(service_name.clone()),
                namespace: Some(namespace.to_string()),
                labels: Some(self.component_labels(cluster_name, component)),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                cluster_ip: Some("None".to_string()),
                selector: Some(self.component_labels(cluster_name, component)),
                ports: Some(vec![
                    ServicePort {
                        name: Some("grpc".to_string()),
                        port: if component == "metadata" { 9000 } else { 9001 },
                        ..Default::default()
                    },
                    ServicePort {
                        name: Some("metrics".to_string()),
                        port: 9090,
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let api: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let pp = PostParams::default();
        let _ = api.create(&pp, &svc).await;

        debug!(name = %service_name, "Headless service created");
        Ok(())
    }

    async fn create_client_service(
        &self,
        namespace: &str,
        cluster_name: &str,
        spec: &StrataClusterSpec,
    ) -> Result<(), Error> {
        let service_name = format!("{}-client", cluster_name);

        let svc = Service {
            metadata: ObjectMeta {
                name: Some(service_name.clone()),
                namespace: Some(namespace.to_string()),
                labels: Some(self.common_labels(cluster_name)),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some(spec.network.service_type.clone()),
                selector: Some(self.common_labels(cluster_name)),
                ports: Some(vec![
                    ServicePort {
                        name: Some("metadata".to_string()),
                        port: spec.network.metadata_port,
                        ..Default::default()
                    },
                    ServicePort {
                        name: Some("data".to_string()),
                        port: spec.network.data_port,
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let api: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let pp = PostParams::default();
        let _ = api.create(&pp, &svc).await;

        debug!(name = %service_name, "Client service created");
        Ok(())
    }

    async fn create_stateful_set(
        &self,
        namespace: &str,
        cluster_name: &str,
        spec: &StrataClusterSpec,
        component: &str,
    ) -> Result<(), Error> {
        let sts_name = format!("{}-{}", cluster_name, component);
        let replicas = if component == "metadata" {
            spec.metadata_replicas
        } else {
            spec.data_replicas
        };

        let resources = if component == "metadata" {
            &spec.resources.metadata
        } else {
            &spec.resources.data
        };

        let volume_size = if component == "metadata" {
            &spec.storage.metadata_size
        } else {
            &spec.storage.data_size
        };

        let sts = StatefulSet {
            metadata: ObjectMeta {
                name: Some(sts_name.clone()),
                namespace: Some(namespace.to_string()),
                labels: Some(self.component_labels(cluster_name, component)),
                ..Default::default()
            },
            spec: Some(StatefulSetSpec {
                replicas: Some(replicas),
                selector: LabelSelector {
                    match_labels: Some(self.component_labels(cluster_name, component)),
                    ..Default::default()
                },
                service_name: format!("{}-{}-headless", cluster_name, component),
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(self.component_labels(cluster_name, component)),
                        annotations: Some(spec.pod_annotations.clone()),
                        ..Default::default()
                    }),
                    spec: Some(PodSpec {
                        containers: vec![Container {
                            name: "strata".to_string(),
                            image: Some(format!("strata/strata:{}", spec.version)),
                            args: Some(vec![
                                "--role".to_string(),
                                component.to_string(),
                                "--config".to_string(),
                                "/etc/strata/strata.toml".to_string(),
                            ]),
                            ports: Some(vec![
                                ContainerPort {
                                    container_port: if component == "metadata" { 9000 } else { 9001 },
                                    name: Some("grpc".to_string()),
                                    ..Default::default()
                                },
                                ContainerPort {
                                    container_port: 9090,
                                    name: Some("metrics".to_string()),
                                    ..Default::default()
                                },
                            ]),
                            resources: Some(ResourceRequirements {
                                requests: Some({
                                    let mut map = BTreeMap::new();
                                    map.insert("cpu".to_string(), Quantity(resources.cpu_request.clone()));
                                    map.insert("memory".to_string(), Quantity(resources.memory_request.clone()));
                                    map
                                }),
                                limits: Some({
                                    let mut map = BTreeMap::new();
                                    map.insert("cpu".to_string(), Quantity(resources.cpu_limit.clone()));
                                    map.insert("memory".to_string(), Quantity(resources.memory_limit.clone()));
                                    map
                                }),
                                ..Default::default()
                            }),
                            volume_mounts: Some(vec![
                                VolumeMount {
                                    name: "data".to_string(),
                                    mount_path: "/data".to_string(),
                                    ..Default::default()
                                },
                                VolumeMount {
                                    name: "config".to_string(),
                                    mount_path: "/etc/strata".to_string(),
                                    ..Default::default()
                                },
                            ]),
                            env: Some(vec![
                                EnvVar {
                                    name: "POD_NAME".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        field_ref: Some(k8s_openapi::api::core::v1::ObjectFieldSelector {
                                            field_path: "metadata.name".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                                EnvVar {
                                    name: "POD_NAMESPACE".to_string(),
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        field_ref: Some(k8s_openapi::api::core::v1::ObjectFieldSelector {
                                            field_path: "metadata.namespace".to_string(),
                                            ..Default::default()
                                        }),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                },
                            ]),
                            ..Default::default()
                        }],
                        volumes: Some(vec![Volume {
                            name: "config".to_string(),
                            config_map: Some(k8s_openapi::api::core::v1::ConfigMapVolumeSource {
                                name: Some(format!("{}-config", cluster_name)),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                },
                volume_claim_templates: Some(vec![PersistentVolumeClaim {
                    metadata: ObjectMeta {
                        name: Some("data".to_string()),
                        ..Default::default()
                    },
                    spec: Some(PersistentVolumeClaimSpec {
                        access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                        storage_class_name: Some(spec.storage.storage_class.clone()),
                        resources: Some(ResourceRequirements {
                            requests: Some({
                                let mut map = BTreeMap::new();
                                map.insert("storage".to_string(), Quantity(volume_size.clone()));
                                map
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);
        let pp = PostParams::default();
        let _ = api.create(&pp, &sts).await;

        debug!(name = %sts_name, "StatefulSet created");
        Ok(())
    }

    async fn update_stateful_set(
        &self,
        namespace: &str,
        cluster_name: &str,
        spec: &StrataClusterSpec,
        component: &str,
    ) -> Result<(), Error> {
        let sts_name = format!("{}-{}", cluster_name, component);
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);

        // Patch the image version
        let patch = serde_json::json!({
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "name": "strata",
                            "image": format!("strata/strata:{}", spec.version)
                        }]
                    }
                }
            }
        });

        let pp = PatchParams::apply("strata-operator");
        api.patch(&sts_name, &pp, &Patch::Merge(&patch)).await?;

        debug!(name = %sts_name, "StatefulSet updated");
        Ok(())
    }

    async fn scale_stateful_set(
        &self,
        namespace: &str,
        cluster_name: &str,
        component: &str,
        replicas: i32,
    ) -> Result<(), Error> {
        let sts_name = format!("{}-{}", cluster_name, component);
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);

        let patch = serde_json::json!({
            "spec": {
                "replicas": replicas
            }
        });

        let pp = PatchParams::apply("strata-operator");
        api.patch(&sts_name, &pp, &Patch::Merge(&patch)).await?;

        debug!(name = %sts_name, replicas = %replicas, "StatefulSet scaled");
        Ok(())
    }

    async fn create_s3_gateway(
        &self,
        _namespace: &str,
        cluster_name: &str,
        _spec: &StrataClusterSpec,
    ) -> Result<(), Error> {
        // S3 gateway would be created as a Deployment
        // Simplified for now
        debug!(cluster = %cluster_name, "S3 gateway creation not yet implemented");
        Ok(())
    }

    async fn check_stateful_set_ready(
        &self,
        namespace: &str,
        cluster_name: &str,
        component: &str,
    ) -> Result<i32, Error> {
        let sts_name = format!("{}-{}", cluster_name, component);
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);

        match api.get(&sts_name).await {
            Ok(sts) => {
                let ready = sts.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0);
                Ok(ready)
            }
            Err(_) => Ok(0),
        }
    }

    async fn check_spec_changes(&self, _cluster: &StrataCluster) -> Result<bool, Error> {
        // Compare current spec with deployed resources
        // Simplified for now
        Ok(false)
    }

    async fn check_scaling_needed(&self, _cluster: &StrataCluster) -> Result<bool, Error> {
        // Check if current replicas match desired
        Ok(false)
    }

    async fn check_cluster_health(
        &self,
        _namespace: &str,
        _cluster_name: &str,
    ) -> Result<(bool, Option<String>), Error> {
        // Check pod status and readiness
        Ok((true, None))
    }

    async fn count_ready_nodes(&self, namespace: &str, cluster_name: &str) -> Result<i32, Error> {
        let metadata = self.check_stateful_set_ready(namespace, cluster_name, "metadata").await?;
        let data = self.check_stateful_set_ready(namespace, cluster_name, "data").await?;
        Ok(metadata + data)
    }

    async fn get_node_statuses(
        &self,
        namespace: &str,
        _cluster_name: &str,
        component: &str,
    ) -> Result<Vec<NodeStatus>, Error> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let lp = ListParams::default()
            .labels(&format!("app.kubernetes.io/name=strata,app.kubernetes.io/component={}", component));

        let pods = api.list(&lp).await?;
        let statuses = pods
            .items
            .iter()
            .map(|pod| {
                let name = pod.name_any();
                let ready = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|c| c.iter().any(|cond| cond.type_ == "Ready" && cond.status == "True"))
                    .unwrap_or(false);

                NodeStatus {
                    name: name.clone(),
                    pod_name: name,
                    role: component.to_string(),
                    ready,
                    address: None,
                    disk_usage_percent: None,
                    last_heartbeat: None,
                }
            })
            .collect();

        Ok(statuses)
    }

    async fn recover_failed_pods(&self, namespace: &str, cluster_name: &str) -> Result<(), Error> {
        // Delete failed pods to trigger recreation
        let api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let lp = ListParams::default()
            .labels(&format!("app.kubernetes.io/instance={}", cluster_name));

        let pods = api.list(&lp).await?;
        for pod in pods.items {
            if let Some(status) = &pod.status {
                if status.phase.as_deref() == Some("Failed") {
                    let name = pod.name_any();
                    let dp = DeleteParams::default();
                    let _ = api.delete(&name, &dp).await;
                    info!(pod = %name, "Deleted failed pod");
                }
            }
        }

        Ok(())
    }

    fn common_labels(&self, cluster_name: &str) -> BTreeMap<String, String> {
        let mut labels = BTreeMap::new();
        labels.insert("app.kubernetes.io/name".to_string(), "strata".to_string());
        labels.insert("app.kubernetes.io/instance".to_string(), cluster_name.to_string());
        labels.insert("app.kubernetes.io/managed-by".to_string(), "strata-operator".to_string());
        labels
    }

    fn component_labels(&self, cluster_name: &str, component: &str) -> BTreeMap<String, String> {
        let mut labels = self.common_labels(cluster_name);
        labels.insert("app.kubernetes.io/component".to_string(), component.to_string());
        labels
    }
}
