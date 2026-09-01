pub mod health;
pub mod maintenance;
pub use health::{
    HealthMonitor, HealthMonitorRegistry, ProxyHealthMonitor, ServiceBusHealthMonitor,
};
pub use maintenance::run_maintenance;

use crate::models::{InstanceStatus, RegistrationRequest, ServiceDefinition, ServiceInstance};
use chrono::Utc;
use dashmap::DashMap;
use std::time::Duration;
use tracing::{debug, info, warn};
use uuid::Uuid;

pub struct ServiceRegistry {
    services: DashMap<String, ServiceDefinition>,
    path_owners: DashMap<String, String>,
    metrics_heartbeat: DashMap<String, MetricsHeartbeatState>,
    proxy_failure_counts: DashMap<String, u32>,
}

#[derive(Clone, Copy)]
struct MetricsHeartbeatState {
    last_seen_utc: chrono::DateTime<Utc>,
    sample_count: u64,
    ema_interval_ms: f64,
    ema_jitter_ms: f64,
}

/// Result returned from a registration attempt.
pub struct RegistrationResult {
    pub success: bool,
    pub instance_id: Option<String>,
    pub token: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceRegistry {
    /// Creates an empty in-memory service registry.
    pub fn new() -> Self {
        Self {
            services: DashMap::new(),
            path_owners: DashMap::new(),
            metrics_heartbeat: DashMap::new(),
            proxy_failure_counts: DashMap::new(),
        }
    }

    /// Registers a service instance and reserves declared route prefixes.
    pub async fn register(&self, request: RegistrationRequest) -> RegistrationResult {
        info!(
            service_id = %request.service_id,
            fingerprint = %request.fingerprint,
            path_prefixes = ?request.path_prefixes,
            instance_id = %request.instance.instance_id,
            instance_host = %request.instance.host,
            instance_port = request.instance.port,
            "registry registration attempt"
        );
        let instance_id = if request.instance.instance_id.trim().is_empty() {
            Uuid::new_v4().to_string()
        } else {
            request.instance.instance_id.clone()
        };

        // 1. Check for prefix collisions
        for prefix in &request.path_prefixes {
            if let Some(owner) = self.path_owners.get(prefix)
                && owner.value() != &request.service_id
            {
                warn!(
                    service_id = %request.service_id,
                    prefix = %prefix,
                    owner_service_id = %owner.value(),
                    "registry registration rejected due to route collision"
                );
                return RegistrationResult {
                    success: false,
                    instance_id: None,
                    token: None,
                    error_code: Some("ROUTE_COLLISION".to_string()),
                    error_message: Some(format!(
                        "Path prefix '{}' already owned by service '{}'",
                        prefix,
                        owner.value()
                    )),
                };
            }
        }

        // 2. Register path owners
        for prefix in &request.path_prefixes {
            self.path_owners
                .insert(prefix.clone(), request.service_id.clone());
        }

        let mut service = self
            .services
            .entry(request.service_id.clone())
            .or_insert_with(|| ServiceDefinition {
                service_id: request.service_id.clone(),
                fingerprint: request.fingerprint.clone(),
                path_prefixes: request.path_prefixes.clone(),
                instances: std::collections::HashMap::new(),
            });

        // Simple fingerprint check (mocking .NET behavior)
        if service.fingerprint != request.fingerprint {
            warn!(
                service_id = %request.service_id,
                expected_fingerprint = %service.fingerprint,
                provided_fingerprint = %request.fingerprint,
                "registry registration rejected due to fingerprint mismatch"
            );
            return RegistrationResult {
                success: false,
                instance_id: None,
                token: None,
                error_code: Some("FINGERPRINT_INVALID".to_string()),
                error_message: Some("Fingerprint mismatch".to_string()),
            };
        }

        let token = Uuid::new_v4().to_string();
        let instance = ServiceInstance {
            instance_id: instance_id.clone(),
            service_id: request.service_id.clone(),
            token: token.clone(),
            scheme: request.instance.scheme,
            host: request.instance.host,
            port: request.instance.port,
            weight: request.instance.weight,
            status: InstanceStatus::Up,
            active_connections: 0,
            last_heartbeat_utc: Utc::now(),
        };

        service.instances.insert(instance_id.clone(), instance);

        info!(
            service_id = %request.service_id,
            instance_id = %instance_id,
            token_issued = true,
            total_instances = service.instances.len(),
            "registry registration succeeded"
        );

        RegistrationResult {
            success: true,
            instance_id: Some(instance_id),
            token: Some(token),
            error_code: None,
            error_message: None,
        }
    }

    /// Removes an instance from a service definition.
    pub async fn deregister(&self, service_id: &str, instance_id: &str) -> bool {
        info!(service_id = %service_id, instance_id = %instance_id, "registry deregister attempt");
        if let Some(mut service) = self.services.get_mut(service_id) {
            let removed = service.instances.remove(instance_id).is_some();
            if removed {
                self.metrics_heartbeat
                    .remove(&metrics_key(service_id, instance_id));
                self.proxy_failure_counts
                    .remove(&metrics_key(service_id, instance_id));
                info!(
                    service_id = %service_id,
                    instance_id = %instance_id,
                    remaining_instances = service.instances.len(),
                    "registry deregister succeeded"
                );
            } else {
                warn!(
                    service_id = %service_id,
                    instance_id = %instance_id,
                    "registry deregister target instance not found"
                );
            }
            return removed;
        }
        warn!(service_id = %service_id, instance_id = %instance_id, "registry deregister target service not found");
        false
    }

    /// Records one metrics heartbeat sample emitted by an instance.
    ///
    /// This sample stream is used to derive liveness and heartbeat regularity.
    pub fn record_metrics_heartbeat(&self, service_id: &str, instance_id: &str) {
        if let Some(mut service) = self.services.get_mut(service_id)
            && let Some(instance) = service.instances.get_mut(instance_id)
        {
            let now = Utc::now();
            instance.last_heartbeat_utc = now;
            debug!(
                service_id = %service_id,
                instance_id = %instance_id,
                "registry metrics heartbeat received"
            );

            let key = metrics_key(service_id, instance_id);
            if let Some(mut hb) = self.metrics_heartbeat.get_mut(&key) {
                let interval_ms = now
                    .signed_duration_since(hb.last_seen_utc)
                    .num_milliseconds()
                    .max(1) as f64;
                hb.sample_count += 1;
                if hb.sample_count == 2 {
                    hb.ema_interval_ms = interval_ms;
                    hb.ema_jitter_ms = 0.0;
                } else {
                    let alpha = 0.25;
                    hb.ema_interval_ms = alpha * interval_ms + (1.0 - alpha) * hb.ema_interval_ms;
                    let jitter = (interval_ms - hb.ema_interval_ms).abs();
                    hb.ema_jitter_ms = alpha * jitter + (1.0 - alpha) * hb.ema_jitter_ms;
                }
                hb.last_seen_utc = now;
            } else {
                self.metrics_heartbeat.insert(
                    key,
                    MetricsHeartbeatState {
                        last_seen_utc: now,
                        sample_count: 1,
                        ema_interval_ms: 0.0,
                        ema_jitter_ms: 0.0,
                    },
                );
            }
        }
    }

    /// Recomputes instance health from metrics, heartbeat recency and regularity.
    ///
    /// - `Down` when heartbeat stops beyond timeout
    /// - `Degraded` when heartbeat becomes irregular (high jitter)
    /// - `Up` when cadence is recent and stable
    pub fn evaluate_metrics_health(&self, timeout: Duration) {
        let now = Utc::now();
        let timeout_ms = timeout.as_millis() as f64;

        for mut service in self.services.iter_mut() {
            for instance in service.instances.values_mut() {
                let key = metrics_key(&instance.service_id, &instance.instance_id);
                let heartbeat = self.metrics_heartbeat.get(&key).map(|hb| *hb.value());
                instance.status =
                    Self::evaluate_instance_metrics_health(now, timeout_ms, heartbeat);
            }
        }
    }

    fn evaluate_instance_metrics_health(
        now: chrono::DateTime<Utc>,
        timeout_ms: f64,
        heartbeat: Option<MetricsHeartbeatState>,
    ) -> InstanceStatus {
        let Some(heartbeat) = heartbeat else {
            // No metrics observed yet for this instance.
            return InstanceStatus::Down;
        };

        let silent_ms = Self::calculate_silent_ms(now, heartbeat);
        if silent_ms > timeout_ms {
            return InstanceStatus::Down;
        }

        if Self::is_irregular_heartbeat(heartbeat, silent_ms) {
            return InstanceStatus::Degraded;
        }

        InstanceStatus::Up
    }

    fn calculate_silent_ms(now: chrono::DateTime<Utc>, heartbeat: MetricsHeartbeatState) -> f64 {
        now.signed_duration_since(heartbeat.last_seen_utc)
            .num_milliseconds()
            .max(0) as f64
    }

    fn is_irregular_heartbeat(heartbeat: MetricsHeartbeatState, silent_ms: f64) -> bool {
        let has_stable_baseline = heartbeat.sample_count >= 5 && heartbeat.ema_interval_ms > 0.0;
        if !has_stable_baseline {
            return false;
        }

        let jitter_ratio = heartbeat.ema_jitter_ms / heartbeat.ema_interval_ms;
        let cadence_gap_ratio = silent_ms / heartbeat.ema_interval_ms;
        jitter_ratio >= 0.4 || cadence_gap_ratio >= 2.5
    }

    /// Returns a snapshot of all known services.
    pub fn get_all_services(&self) -> Vec<ServiceDefinition> {
        self.services.iter().map(|r| r.value().clone()).collect()
    }

    /// Returns a specific service snapshot if present.
    pub fn get_service(&self, service_id: &str) -> Option<ServiceDefinition> {
        self.services.get(service_id).map(|r| r.value().clone())
    }

    /// Validates a token for an exact service instance pair.
    pub fn validate_instance_token(
        &self,
        service_id: &str,
        instance_id: &str,
        token: &str,
    ) -> bool {
        if let Some(service) = self.services.get(service_id)
            && let Some(instance) = service.instances.get(instance_id)
        {
            return instance.token == token;
        }

        false
    }

    /// Validates a token against any instance of the given service.
    pub fn validate_any_instance_token(&self, service_id: &str, token: &str) -> bool {
        if let Some(service) = self.services.get(service_id) {
            return service.instances.values().any(|i| i.token == token);
        }
        false
    }

    /// Binds a path prefix to a service identifier for proxy resolution.
    pub fn bind_path_prefix(&self, path_prefix: String, service_id: String) {
        info!(path_prefix = %path_prefix, service_id = %service_id, "registry path prefix bound");
        self.path_owners.insert(path_prefix, service_id);
    }

    /// Resolves the best-matching service by the longest owned path prefix.
    pub fn resolve_service_by_path(&self, path: &str) -> Option<String> {
        let resolved = self
            .path_owners
            .iter()
            .filter(|r| path.starts_with(r.key()))
            .max_by_key(|r| r.key().len())
            .map(|r| r.value().clone());
        debug!(path = %path, resolved_service_id = ?resolved, "registry path resolution");
        resolved
    }

    /// Removes down instances that exceeded the configured timeout.
    pub fn remove_stale_instances(&self, timeout: Duration) {
        let now = Utc::now();
        for mut service in self.services.iter_mut() {
            service.instances.retain(|_, instance| {
                if instance.status == InstanceStatus::Down {
                    let elapsed = now.signed_duration_since(instance.last_heartbeat_utc);
                    if elapsed.to_std().unwrap_or(Duration::ZERO) > timeout {
                        info!(
                            service_id = %instance.service_id,
                            instance_id = %instance.instance_id,
                            elapsed_secs = elapsed.num_seconds(),
                            timeout_secs = timeout.as_secs(),
                            "registry stale instance removed"
                        );
                        self.metrics_heartbeat
                            .remove(&metrics_key(&instance.service_id, &instance.instance_id));
                        self.proxy_failure_counts
                            .remove(&metrics_key(&instance.service_id, &instance.instance_id));
                        return false;
                    }
                }
                true
            });
        }
    }

    /// Updates an instance health status, refreshing heartbeat when moving to `Up`.
    pub async fn update_instance_status(
        &self,
        service_id: &str,
        instance_id: &str,
        status: InstanceStatus,
    ) {
        if let Some(mut service) = self.services.get_mut(service_id)
            && let Some(instance) = service.instances.get_mut(instance_id)
        {
            instance.status = status;
            info!(
                service_id = %service_id,
                instance_id = %instance_id,
                status = ?status,
                "registry instance status updated"
            );
            if status == InstanceStatus::Up {
                instance.last_heartbeat_utc = Utc::now();
            }
            if status == InstanceStatus::Up || status == InstanceStatus::Down {
                // Reset proxy failure tracking on explicit status transitions.
                self.proxy_failure_counts
                    .remove(&metrics_key(service_id, instance_id));
            }
        }
    }

    /// Records a proxy failure for an instance when connection-health is disabled.
    ///
    /// Increments the consecutive failure counter and marks the instance `Down`
    /// once `threshold` consecutive unreachable attempts are observed.
    /// Returns `true` if this call transitioned the instance to `Down`.
    pub fn record_proxy_failure(
        &self,
        service_id: &str,
        instance_id: &str,
        threshold: u32,
    ) -> bool {
        let threshold = threshold.max(1);
        let key = metrics_key(service_id, instance_id);
        let count = {
            let mut entry = self.proxy_failure_counts.entry(key.clone()).or_insert(0);
            *entry += 1;
            *entry
        };

        if count >= threshold {
            if let Some(mut service) = self.services.get_mut(service_id)
                && let Some(instance) = service.instances.get_mut(instance_id)
                && instance.status != InstanceStatus::Down
            {
                instance.status = InstanceStatus::Down;
                warn!(
                    service_id = %service_id,
                    instance_id = %instance_id,
                    consecutive_failures = count,
                    threshold,
                    "registry instance marked Down after consecutive proxy failures"
                );
            }
            return true;
        }

        debug!(
            service_id = %service_id,
            instance_id = %instance_id,
            consecutive_failures = count,
            threshold,
            "registry proxy failure recorded"
        );
        false
    }

    /// Records a successful proxy attempt, resetting the consecutive failure counter
    /// and ensuring the instance is `Up`.
    pub fn record_proxy_success(&self, service_id: &str, instance_id: &str) {
        let key = metrics_key(service_id, instance_id);
        self.proxy_failure_counts.remove(&key);
        if let Some(mut service) = self.services.get_mut(service_id)
            && let Some(instance) = service.instances.get_mut(instance_id)
            && instance.status != InstanceStatus::Up
        {
            instance.status = InstanceStatus::Up;
            instance.last_heartbeat_utc = Utc::now();
            info!(
                service_id = %service_id,
                instance_id = %instance_id,
                "registry instance marked Up after successful proxy"
            );
        }
    }

    /// Returns the current consecutive proxy failure count for an instance.
    pub fn proxy_failure_count(&self, service_id: &str, instance_id: &str) -> u32 {
        self.proxy_failure_counts
            .get(&metrics_key(service_id, instance_id))
            .map(|v| *v.value())
            .unwrap_or(0)
    }

    /// Synchronous status setter used by `HealthMonitor` implementations.
    ///
    /// Mirrors `update_instance_status` but is `sync` so trait objects can call
    /// it without async.
    pub(crate) fn set_instance_status_sync(
        &self,
        service_id: &str,
        instance_id: &str,
        status: InstanceStatus,
    ) {
        if let Some(mut service) = self.services.get_mut(service_id)
            && let Some(instance) = service.instances.get_mut(instance_id)
        {
            instance.status = status;
            info!(
                service_id = %service_id,
                instance_id = %instance_id,
                status = ?status,
                "registry instance status updated (sync)"
            );
            if status == InstanceStatus::Up {
                instance.last_heartbeat_utc = Utc::now();
            }
            if status == InstanceStatus::Up || status == InstanceStatus::Down {
                self.proxy_failure_counts
                    .remove(&metrics_key(service_id, instance_id));
            }
        }
    }

    pub(crate) fn record_proxy_failure_inner(
        &self,
        service_id: &str,
        instance_id: &str,
        threshold: u32,
    ) -> bool {
        self.record_proxy_failure(service_id, instance_id, threshold)
    }

    pub(crate) fn record_proxy_success_inner(&self, service_id: &str, instance_id: &str) {
        self.record_proxy_success(service_id, instance_id);
    }

    pub(crate) fn record_metrics_heartbeat_inner(&self, service_id: &str, instance_id: &str) {
        self.record_metrics_heartbeat(service_id, instance_id);
    }

    pub(crate) fn evaluate_metrics_health_inner(&self, timeout: Duration) {
        self.evaluate_metrics_health(timeout);
    }
}

fn metrics_key(service_id: &str, instance_id: &str) -> String {
    format!("{}:{}", service_id, instance_id)
}
