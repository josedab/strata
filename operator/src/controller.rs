//! Kubernetes controller for Strata clusters

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::api::{Api, ListParams, Patch, PatchParams, PostParams};
use kube::runtime::controller::{Action, Controller as KubeController};
use kube::runtime::watcher::Config;
use kube::{Client, Resource, ResourceExt};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::crd::{
    ClusterCondition, ClusterPhase, NodeStatus, StrataCluster, StrataClusterStatus,
};
use crate::error::Error;
use crate::reconciler::Reconciler;

/// Controller state
pub struct ControllerState {
    /// Kubernetes client
    pub client: Client,
    /// Reconciler
    pub reconciler: Reconciler,
}

/// Main controller for Strata resources
pub struct Controller {
    client: Client,
    namespace: String,
    state: Arc<RwLock<ControllerState>>,
}

impl Controller {
    /// Create a new controller
    pub async fn new(namespace: String, _leader_election: bool) -> Result<Self, Error> {
        let client = Client::try_default().await?;

        let state = ControllerState {
            client: client.clone(),
            reconciler: Reconciler::new(client.clone()),
        };

        Ok(Self {
            client,
            namespace,
            state: Arc::new(RwLock::new(state)),
        })
    }

    /// Run the controller
    pub async fn run(&self) -> Result<(), Error> {
        info!("Starting Strata controller");

        // Get API for StrataCluster
        let clusters: Api<StrataCluster> = if self.namespace.is_empty() {
            Api::all(self.client.clone())
        } else {
            Api::namespaced(self.client.clone(), &self.namespace)
        };

        // Verify CRD is installed
        let lp = ListParams::default().limit(1);
        if let Err(e) = clusters.list(&lp).await {
            error!("Failed to list StrataClusters. Is the CRD installed? Error: {}", e);
            return Err(Error::CrdNotInstalled);
        }

        info!("CRD verification successful");

        let state = Arc::clone(&self.state);

        // Run the controller
        KubeController::new(clusters.clone(), Config::default())
            .shutdown_on_signal()
            .run(
                |cluster, ctx| async move { reconcile(cluster, ctx).await },
                |cluster, error, ctx| error_policy(cluster, error, ctx),
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(
                            name = %obj.name,
                            ?action,
                            "Reconciliation successful"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "Reconciliation error");
                    }
                }
            })
            .await;

        info!("Controller stopped");
        Ok(())
    }
}

/// Reconcile a StrataCluster resource
async fn reconcile(
    cluster: Arc<StrataCluster>,
    ctx: Arc<RwLock<ControllerState>>,
) -> Result<Action, Error> {
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_default();

    info!(name = %name, namespace = %namespace, "Reconciling StrataCluster");

    let state = ctx.read().await;
    let client = state.client.clone();
    let reconciler = &state.reconciler;

    // Get current status
    let current_phase = cluster
        .status
        .as_ref()
        .map(|s| s.phase.clone())
        .unwrap_or(ClusterPhase::Pending);

    // Perform reconciliation based on current phase
    let result = match current_phase {
        ClusterPhase::Pending => {
            reconciler.create_cluster(&cluster).await
        }
        ClusterPhase::Creating => {
            reconciler.wait_for_ready(&cluster).await
        }
        ClusterPhase::Running => {
            reconciler.ensure_running(&cluster).await
        }
        ClusterPhase::Updating | ClusterPhase::Upgrading => {
            reconciler.handle_update(&cluster).await
        }
        ClusterPhase::Scaling => {
            reconciler.handle_scaling(&cluster).await
        }
        ClusterPhase::Degraded => {
            reconciler.handle_degraded(&cluster).await
        }
        ClusterPhase::Failed => {
            reconciler.handle_failed(&cluster).await
        }
        ClusterPhase::Terminating => {
            // Nothing to do, let Kubernetes handle deletion
            return Ok(Action::await_change());
        }
    };

    match result {
        Ok(new_status) => {
            // Update status
            update_status(&client, &namespace, &name, new_status).await?;
            Ok(Action::requeue(Duration::from_secs(60)))
        }
        Err(e) => {
            error!(name = %name, error = %e, "Reconciliation failed");

            // Update status with error
            let error_status = StrataClusterStatus {
                phase: ClusterPhase::Failed,
                message: Some(e.to_string()),
                ..Default::default()
            };
            update_status(&client, &namespace, &name, error_status).await?;

            Ok(Action::requeue(Duration::from_secs(300)))
        }
    }
}

/// Error policy for reconciliation failures
fn error_policy(
    _cluster: Arc<StrataCluster>,
    error: &Error,
    _ctx: Arc<RwLock<ControllerState>>,
) -> Action {
    warn!(error = %error, "Reconciliation error, will retry");
    Action::requeue(Duration::from_secs(60))
}

/// Update the status of a StrataCluster
async fn update_status(
    client: &Client,
    namespace: &str,
    name: &str,
    status: StrataClusterStatus,
) -> Result<(), Error> {
    let api: Api<StrataCluster> = Api::namespaced(client.clone(), namespace);

    let patch = serde_json::json!({
        "status": status
    });

    let pp = PatchParams::apply("strata-operator");
    api.patch_status(name, &pp, &Patch::Merge(&patch)).await?;

    debug!(name = %name, "Status updated");
    Ok(())
}

/// Create a condition for the cluster
pub fn create_condition(
    condition_type: &str,
    status: &str,
    reason: &str,
    message: &str,
) -> ClusterCondition {
    ClusterCondition {
        condition_type: condition_type.to_string(),
        status: status.to_string(),
        last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
        reason: Some(reason.to_string()),
        message: Some(message.to_string()),
    }
}

/// Create a node status
pub fn create_node_status(
    name: &str,
    pod_name: &str,
    role: &str,
    ready: bool,
    address: Option<String>,
) -> NodeStatus {
    NodeStatus {
        name: name.to_string(),
        pod_name: pod_name.to_string(),
        role: role.to_string(),
        ready,
        address,
        disk_usage_percent: None,
        last_heartbeat: Some(chrono::Utc::now().to_rfc3339()),
    }
}
