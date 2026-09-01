use crate::config::GatewayConfig;
use crate::registry::ServiceRegistry;
use std::time::Duration;

/// Generic health monitoring contract.
///
/// Each health setup (service-bus, proxy) implements this trait so the
/// registry and maintenance loops can drive them uniformly while keeping
/// their state and enablement cleanly separated.
pub trait HealthMonitor: Send + Sync {
    /// Human-readable monitor name for logs.
    fn name(&self) -> &'static str;

    /// Whether this monitor is enabled for the given config.
    fn is_enabled(&self, config: &GatewayConfig) -> bool;

    /// Called when a new instance is registered. Default is no-op.
    fn on_register(&self, _registry: &ServiceRegistry, _service_id: &str, _instance_id: &str) {}

    /// Called after a successful proxy attempt to the instance.
    fn on_proxy_success(&self, _registry: &ServiceRegistry, _service_id: &str, _instance_id: &str) {
    }

    /// Called after an unreachable proxy attempt. Returns `true` if the monitor
    /// transitioned the instance to `Down`.
    fn on_proxy_failure(
        &self,
        _registry: &ServiceRegistry,
        _service_id: &str,
        _instance_id: &str,
        _threshold: u32,
    ) -> bool {
        false
    }

    /// Called when a service-bus connection is established.
    fn on_bus_connect(
        &self,
        _registry: &ServiceRegistry,
        _service_id: &str,
        _instance_id: &str,
        _config: &GatewayConfig,
    ) {
    }

    /// Called when a service-bus connection is torn down.
    fn on_bus_disconnect(
        &self,
        _registry: &ServiceRegistry,
        _service_id: &str,
        _instance_id: &str,
        _config: &GatewayConfig,
    ) {
    }

    /// Called when a metrics heartbeat is received.
    fn on_heartbeat(&self, _registry: &ServiceRegistry, _service_id: &str, _instance_id: &str) {}

    /// Periodic evaluation (e.g. heartbeat timeout, jitter). Called from the
    /// maintenance loop.
    fn evaluate(&self, _registry: &ServiceRegistry, _config: &GatewayConfig, _timeout: Duration) {}
}

/// Proxy-based health: `Up` by default, `Down` after `N` consecutive
/// unreachable proxy attempts, `Up` again on first successful proxy.
///
/// Enabled exactly when `connection_health_enabled == false` (the default
/// since the proxy health migration).
pub struct ProxyHealthMonitor;

impl HealthMonitor for ProxyHealthMonitor {
    fn name(&self) -> &'static str {
        "proxy"
    }

    fn is_enabled(&self, config: &GatewayConfig) -> bool {
        !config.service_bus.connection_health_enabled
    }

    fn on_proxy_success(&self, registry: &ServiceRegistry, service_id: &str, instance_id: &str) {
        registry.record_proxy_success_inner(service_id, instance_id);
    }

    fn on_proxy_failure(
        &self,
        registry: &ServiceRegistry,
        service_id: &str,
        instance_id: &str,
        threshold: u32,
    ) -> bool {
        registry.record_proxy_failure_inner(service_id, instance_id, threshold)
    }

    // Proxy monitor does not react to bus lifecycle or heartbeats.
}

/// Service-bus-based health: bus `connect`/`disconnect` and
/// `basilisk.metrics.distribution` cadence.
///
/// Enabled only when `connection_health_enabled == true`. When disabled,
/// recoveries via proxy are not overwritten by bus state – this is the
/// isolation required by the proxy health migration.
pub struct ServiceBusHealthMonitor;

impl HealthMonitor for ServiceBusHealthMonitor {
    fn name(&self) -> &'static str {
        "service_bus"
    }

    fn is_enabled(&self, config: &GatewayConfig) -> bool {
        config.service_bus.connection_health_enabled
    }

    fn on_bus_connect(
        &self,
        registry: &ServiceRegistry,
        service_id: &str,
        instance_id: &str,
        _config: &GatewayConfig,
    ) {
        registry.set_instance_status_sync(
            service_id,
            instance_id,
            crate::models::InstanceStatus::Up,
        );
    }

    fn on_bus_disconnect(
        &self,
        registry: &ServiceRegistry,
        service_id: &str,
        instance_id: &str,
        config: &GatewayConfig,
    ) {
        // When monitoring is authoritative, disconnect does not force Down;
        // the instance will transition via heartbeat timeout instead.
        if config.service_bus.monitoring_enabled {
            tracing::warn!(
                service_id = %service_id,
                instance_id = %instance_id,
                "service bus disconnect ignored; monitoring is authoritative so instance will transition down only after heartbeat timeout"
            );
            return;
        }
        registry.set_instance_status_sync(
            service_id,
            instance_id,
            crate::models::InstanceStatus::Down,
        );
    }

    fn on_heartbeat(&self, registry: &ServiceRegistry, service_id: &str, instance_id: &str) {
        registry.record_metrics_heartbeat_inner(service_id, instance_id);
    }

    fn evaluate(&self, registry: &ServiceRegistry, config: &GatewayConfig, timeout: Duration) {
        if !config.service_bus.monitoring_enabled {
            return;
        }
        // When this monitor is enabled, monitoring is authoritative.
        registry.evaluate_metrics_health_inner(timeout);
    }
}

/// Registry that holds the two monitors for convenient dispatch.
///
/// The registry keeps boxed trait objects so callers can iterate or select
/// by config without matching on concrete types.
pub struct HealthMonitorRegistry {
    monitors: Vec<Box<dyn HealthMonitor>>,
}

impl HealthMonitorRegistry {
    pub fn new() -> Self {
        Self {
            monitors: vec![
                Box::new(ProxyHealthMonitor),
                Box::new(ServiceBusHealthMonitor),
            ],
        }
    }

    pub fn proxy(&self) -> &dyn HealthMonitor {
        self.monitors[0].as_ref()
    }

    pub fn service_bus(&self) -> &dyn HealthMonitor {
        self.monitors[1].as_ref()
    }

    pub fn all(&self) -> &[Box<dyn HealthMonitor>] {
        &self.monitors
    }
}

impl Default for HealthMonitorRegistry {
    fn default() -> Self {
        Self::new()
    }
}
