use crate::config::GatewayConfig;
use crate::models::{InstanceStatus, ServiceInstance};
use crate::registry::ServiceRegistry;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::warn;

/// Runs the periodic registry maintenance loop.
///
/// The loop schedules asynchronous health checks for known instances and then
/// prunes stale down instances according to the configured heartbeat timeout.
pub async fn run_maintenance(config: GatewayConfig, registry: Arc<ServiceRegistry>) {
    let client = build_health_check_client();
    let heartbeat_timeout = heartbeat_timeout(&config);
    let health_check_interval = health_check_interval(&config);

    loop {
        schedule_health_checks(Arc::clone(&registry), client.clone());
        registry.remove_stale_instances(heartbeat_timeout);
        sleep(health_check_interval).await;
    }
}

/// Builds the HTTP client used for active health probes.
fn build_health_check_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .danger_accept_invalid_certs(true) // Matching .NET behavior if needed
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// Converts configured heartbeat timeout seconds into a `Duration`.
fn heartbeat_timeout(config: &GatewayConfig) -> Duration {
    Duration::from_secs(config.registry.heartbeat_timeout_seconds)
}

/// Converts configured health check interval seconds into a `Duration`.
fn health_check_interval(config: &GatewayConfig) -> Duration {
    Duration::from_secs(config.registry.health_check_interval_seconds)
}

/// Iterates current service instances and schedules checks for those with probes.
fn schedule_health_checks(registry: Arc<ServiceRegistry>, client: reqwest::Client) {
    for service in registry.get_all_services() {
        for instance in service.instances.values() {
            if instance.health_check.is_empty() {
                continue;
            }

            spawn_health_check(Arc::clone(&registry), client.clone(), instance.clone());
        }
    }
}

/// Spawns an asynchronous health check task for one instance.
fn spawn_health_check(
    registry: Arc<ServiceRegistry>,
    client: reqwest::Client,
    instance: ServiceInstance,
) {
    tokio::spawn(async move {
        let status = run_instance_health_check(&client, &instance).await;
        registry
            .update_instance_status(&instance.service_id, &instance.instance_id, status)
            .await;
    });
}

/// Executes a single instance health probe request.
async fn run_instance_health_check(
    client: &reqwest::Client,
    instance: &ServiceInstance,
) -> InstanceStatus {
    match client.get(&instance.health_check).send().await {
        Ok(resp) if resp.status().is_success() => InstanceStatus::Up,
        Ok(_) => InstanceStatus::Down,
        Err(e) => {
            warn!(
                "Health check failed for {} ({}): {}",
                instance.service_id, instance.instance_id, e
            );
            InstanceStatus::Down
        }
    }
}
