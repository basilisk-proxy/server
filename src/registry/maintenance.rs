use crate::config::GatewayConfig;
use crate::registry::health::HealthMonitor;
use crate::registry::{ServiceBusHealthMonitor, ServiceRegistry};
use crate::service_bus::connection_manager::ConnectionManager;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

/// Runs the periodic registry maintenance loop.
///
/// Health evaluation is delegated to the monitor trait so service-bus and
/// proxy health are cleanly separated. The service-bus monitor is only
/// evaluated when its `is_enabled` returns true, ensuring proxy recoveries
/// are not overwritten when `connection_health_enabled == false`.
pub async fn run_maintenance(
    config: GatewayConfig,
    registry: Arc<ServiceRegistry>,
    _connection_manager: Arc<ConnectionManager>,
) {
    let heartbeat_timeout = heartbeat_timeout(&config);
    let maintenance_interval = Duration::from_secs(1);
    let bus_monitor = ServiceBusHealthMonitor;

    loop {
        // Service-bus monitor (metrics heartbeat) – only when enabled.
        // When `connection_health_enabled == false`, this is a no-op, so
        // proxy-driven recoveries are not reverted.
        if bus_monitor.is_enabled(&config) {
            bus_monitor.evaluate(&registry, &config, heartbeat_timeout);
        }
        registry.remove_stale_instances(heartbeat_timeout);
        sleep(maintenance_interval).await;
    }
}

/// Converts configured heartbeat timeout seconds into a `Duration`.
fn heartbeat_timeout(config: &GatewayConfig) -> Duration {
    Duration::from_secs(config.registry.heartbeat_timeout_seconds)
}
