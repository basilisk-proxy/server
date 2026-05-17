use crate::config::GatewayConfig;
use crate::registry::ServiceRegistry;
use crate::service_bus::connection_manager::ConnectionManager;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

/// Runs the periodic registry maintenance loop.
///
/// The loop derives health from metrics heartbeat cadence and then prunes stale
/// down instances according to the configured heartbeat timeout.
pub async fn run_maintenance(
    config: GatewayConfig,
    registry: Arc<ServiceRegistry>,
    _connection_manager: Arc<ConnectionManager>,
) {
    let heartbeat_timeout = heartbeat_timeout(&config);
    let maintenance_interval = Duration::from_secs(1);

    loop {
        if config.service_bus.monitoring_enabled {
            registry.evaluate_metrics_health(heartbeat_timeout);
        }
        registry.remove_stale_instances(heartbeat_timeout);
        sleep(maintenance_interval).await;
    }
}

/// Converts configured heartbeat timeout seconds into a `Duration`.
fn heartbeat_timeout(config: &GatewayConfig) -> Duration {
    Duration::from_secs(config.registry.heartbeat_timeout_seconds)
}
