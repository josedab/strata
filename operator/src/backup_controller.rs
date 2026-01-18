//! Backup and restore controller for Strata

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller as KubeController};
use kube::runtime::watcher::Config;
use kube::{Client, ResourceExt};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::crd::{
    BackupPhase, RestorePhase, StrataBackup, StrataBackupStatus, StrataCluster,
    StrataRestore, StrataRestoreStatus,
};
use crate::error::Error;

/// Backup controller state
pub struct BackupControllerState {
    pub client: Client,
}

/// Backup controller
pub struct BackupController {
    client: Client,
    namespace: String,
    state: Arc<RwLock<BackupControllerState>>,
}

impl BackupController {
    /// Create a new backup controller
    pub async fn new(namespace: String) -> Result<Self, Error> {
        let client = Client::try_default().await?;
        let state = BackupControllerState {
            client: client.clone(),
        };

        Ok(Self {
            client,
            namespace,
            state: Arc::new(RwLock::new(state)),
        })
    }

    /// Run the backup controller
    pub async fn run_backup_controller(&self) -> Result<(), Error> {
        info!("Starting backup controller");

        let backups: Api<StrataBackup> = if self.namespace.is_empty() {
            Api::all(self.client.clone())
        } else {
            Api::namespaced(self.client.clone(), &self.namespace)
        };

        let state = Arc::clone(&self.state);

        KubeController::new(backups.clone(), Config::default())
            .shutdown_on_signal()
            .run(
                |backup, ctx| async move { reconcile_backup(backup, ctx).await },
                |backup, error, ctx| backup_error_policy(backup, error, ctx),
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(name = %obj.name, ?action, "Backup reconciliation successful");
                    }
                    Err(e) => {
                        error!(error = %e, "Backup reconciliation error");
                    }
                }
            })
            .await;

        Ok(())
    }

    /// Run the restore controller
    pub async fn run_restore_controller(&self) -> Result<(), Error> {
        info!("Starting restore controller");

        let restores: Api<StrataRestore> = if self.namespace.is_empty() {
            Api::all(self.client.clone())
        } else {
            Api::namespaced(self.client.clone(), &self.namespace)
        };

        let state = Arc::clone(&self.state);

        KubeController::new(restores.clone(), Config::default())
            .shutdown_on_signal()
            .run(
                |restore, ctx| async move { reconcile_restore(restore, ctx).await },
                |restore, error, ctx| restore_error_policy(restore, error, ctx),
                state,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, action)) => {
                        debug!(name = %obj.name, ?action, "Restore reconciliation successful");
                    }
                    Err(e) => {
                        error!(error = %e, "Restore reconciliation error");
                    }
                }
            })
            .await;

        Ok(())
    }
}

/// Reconcile a StrataBackup resource
async fn reconcile_backup(
    backup: Arc<StrataBackup>,
    ctx: Arc<RwLock<BackupControllerState>>,
) -> Result<Action, Error> {
    let name = backup.name_any();
    let namespace = backup.namespace().unwrap_or_default();

    info!(name = %name, namespace = %namespace, "Reconciling StrataBackup");

    let state = ctx.read().await;
    let client = state.client.clone();

    let current_phase = backup
        .status
        .as_ref()
        .map(|s| s.phase.clone())
        .unwrap_or(BackupPhase::Pending);

    match current_phase {
        BackupPhase::Pending => {
            // Validate cluster exists
            let clusters: Api<StrataCluster> = Api::namespaced(client.clone(), &namespace);
            if clusters.get(&backup.spec.cluster_ref).await.is_err() {
                let status = StrataBackupStatus {
                    phase: BackupPhase::Failed,
                    error: Some(format!(
                        "Cluster '{}' not found",
                        backup.spec.cluster_ref
                    )),
                    ..Default::default()
                };
                update_backup_status(&client, &namespace, &name, status).await?;
                return Ok(Action::await_change());
            }

            // Start backup
            let status = StrataBackupStatus {
                phase: BackupPhase::InProgress,
                start_time: Some(chrono::Utc::now().to_rfc3339()),
                ..Default::default()
            };
            update_backup_status(&client, &namespace, &name, status).await?;

            info!(name = %name, cluster = %backup.spec.cluster_ref, "Starting backup");
            Ok(Action::requeue(Duration::from_secs(10)))
        }
        BackupPhase::InProgress => {
            // Check backup progress
            // In real implementation, this would:
            // 1. Create a Job to run the backup
            // 2. Monitor the Job status
            // 3. Update backup status based on Job completion

            // Simulate backup completion for now
            let status = StrataBackupStatus {
                phase: BackupPhase::Completed,
                start_time: backup.status.as_ref().and_then(|s| s.start_time.clone()),
                completion_time: Some(chrono::Utc::now().to_rfc3339()),
                location: Some(format!(
                    "{}/{}",
                    backup.spec.storage_location, name
                )),
                size_bytes: Some(1024 * 1024 * 100), // Placeholder
                file_count: Some(1000),              // Placeholder
                ..Default::default()
            };
            update_backup_status(&client, &namespace, &name, status).await?;

            info!(name = %name, "Backup completed");
            Ok(Action::await_change())
        }
        BackupPhase::Completed => {
            // Check retention and cleanup old backups
            Ok(Action::requeue(Duration::from_secs(3600)))
        }
        BackupPhase::Failed => {
            // Backup failed, no action needed
            Ok(Action::await_change())
        }
    }
}

/// Reconcile a StrataRestore resource
async fn reconcile_restore(
    restore: Arc<StrataRestore>,
    ctx: Arc<RwLock<BackupControllerState>>,
) -> Result<Action, Error> {
    let name = restore.name_any();
    let namespace = restore.namespace().unwrap_or_default();

    info!(name = %name, namespace = %namespace, "Reconciling StrataRestore");

    let state = ctx.read().await;
    let client = state.client.clone();

    let current_phase = restore
        .status
        .as_ref()
        .map(|s| s.phase.clone())
        .unwrap_or(RestorePhase::Pending);

    match current_phase {
        RestorePhase::Pending => {
            // Validate backup exists
            let backups: Api<StrataBackup> = Api::namespaced(client.clone(), &namespace);
            let backup = match backups.get(&restore.spec.backup_ref).await {
                Ok(b) => b,
                Err(_) => {
                    let status = StrataRestoreStatus {
                        phase: RestorePhase::Failed,
                        error: Some(format!(
                            "Backup '{}' not found",
                            restore.spec.backup_ref
                        )),
                        ..Default::default()
                    };
                    update_restore_status(&client, &namespace, &name, status).await?;
                    return Ok(Action::await_change());
                }
            };

            // Verify backup is completed
            let backup_phase = backup
                .status
                .as_ref()
                .map(|s| s.phase.clone())
                .unwrap_or(BackupPhase::Pending);

            if backup_phase != BackupPhase::Completed {
                let status = StrataRestoreStatus {
                    phase: RestorePhase::Failed,
                    error: Some("Backup is not completed".to_string()),
                    ..Default::default()
                };
                update_restore_status(&client, &namespace, &name, status).await?;
                return Ok(Action::await_change());
            }

            // Validate target cluster exists
            let clusters: Api<StrataCluster> = Api::namespaced(client.clone(), &namespace);
            if clusters.get(&restore.spec.cluster_ref).await.is_err() {
                let status = StrataRestoreStatus {
                    phase: RestorePhase::Failed,
                    error: Some(format!(
                        "Target cluster '{}' not found",
                        restore.spec.cluster_ref
                    )),
                    ..Default::default()
                };
                update_restore_status(&client, &namespace, &name, status).await?;
                return Ok(Action::await_change());
            }

            // Start restore
            let status = StrataRestoreStatus {
                phase: RestorePhase::InProgress,
                start_time: Some(chrono::Utc::now().to_rfc3339()),
                ..Default::default()
            };
            update_restore_status(&client, &namespace, &name, status).await?;

            info!(
                name = %name,
                backup = %restore.spec.backup_ref,
                cluster = %restore.spec.cluster_ref,
                "Starting restore"
            );
            Ok(Action::requeue(Duration::from_secs(10)))
        }
        RestorePhase::InProgress => {
            // Check restore progress
            // In real implementation, this would:
            // 1. Create a Job to run the restore
            // 2. Monitor the Job status
            // 3. Update restore status based on Job completion

            // Simulate restore completion
            let status = StrataRestoreStatus {
                phase: RestorePhase::Completed,
                start_time: restore.status.as_ref().and_then(|s| s.start_time.clone()),
                completion_time: Some(chrono::Utc::now().to_rfc3339()),
                files_restored: Some(1000),
                bytes_restored: Some(1024 * 1024 * 100),
                ..Default::default()
            };
            update_restore_status(&client, &namespace, &name, status).await?;

            info!(name = %name, "Restore completed");
            Ok(Action::await_change())
        }
        RestorePhase::Completed => {
            // Restore completed, no action needed
            Ok(Action::await_change())
        }
        RestorePhase::Failed => {
            // Restore failed, no action needed
            Ok(Action::await_change())
        }
    }
}

/// Error policy for backup reconciliation
fn backup_error_policy(
    _backup: Arc<StrataBackup>,
    error: &Error,
    _ctx: Arc<RwLock<BackupControllerState>>,
) -> Action {
    warn!(error = %error, "Backup reconciliation error, will retry");
    Action::requeue(Duration::from_secs(60))
}

/// Error policy for restore reconciliation
fn restore_error_policy(
    _restore: Arc<StrataRestore>,
    error: &Error,
    _ctx: Arc<RwLock<BackupControllerState>>,
) -> Action {
    warn!(error = %error, "Restore reconciliation error, will retry");
    Action::requeue(Duration::from_secs(60))
}

/// Update backup status
async fn update_backup_status(
    client: &Client,
    namespace: &str,
    name: &str,
    status: StrataBackupStatus,
) -> Result<(), Error> {
    let api: Api<StrataBackup> = Api::namespaced(client.clone(), namespace);
    let patch = serde_json::json!({ "status": status });
    let pp = PatchParams::apply("strata-operator");
    api.patch_status(name, &pp, &Patch::Merge(&patch)).await?;
    debug!(name = %name, "Backup status updated");
    Ok(())
}

/// Update restore status
async fn update_restore_status(
    client: &Client,
    namespace: &str,
    name: &str,
    status: StrataRestoreStatus,
) -> Result<(), Error> {
    let api: Api<StrataRestore> = Api::namespaced(client.clone(), namespace);
    let patch = serde_json::json!({ "status": status });
    let pp = PatchParams::apply("strata-operator");
    api.patch_status(name, &pp, &Patch::Merge(&patch)).await?;
    debug!(name = %name, "Restore status updated");
    Ok(())
}
