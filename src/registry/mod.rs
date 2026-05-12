pub mod maintenance;
pub use maintenance::run_maintenance;

use crate::models::{InstanceStatus, RegistrationRequest, ServiceDefinition, ServiceInstance};
use chrono::Utc;
use dashmap::DashMap;
use std::time::Duration;
use uuid::Uuid;

pub struct ServiceRegistry {
    services: DashMap<String, ServiceDefinition>,
    path_owners: DashMap<String, String>,
    metrics_heartbeat: DashMap<String, MetricsHeartbeatState>,
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
        }
    }

    /// Registers a service instance and reserves declared route prefixes.
    pub async fn register(&self, request: RegistrationRequest) -> RegistrationResult {
        let instance_id = if request.instance.instance_id.trim().is_empty() {
            Uuid::new_v4().to_string()
        } else {
            request.instance.instance_id.clone()
        };

        // 1. Check for prefix collisions
        for prefix in &request.path_prefixes {
            if let Some(owner) = self.path_owners.get(prefix) {
                if owner.value() != &request.service_id {
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
            // Instance is considered healthy only after service-bus/metrics liveness
            // evidence, not immediately at registration time.
            status: InstanceStatus::Down,
            active_connections: 0,
            last_heartbeat_utc: Utc::now(),
        };

        service.instances.insert(instance_id.clone(), instance);

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
        if let Some(mut service) = self.services.get_mut(service_id) {
            return service.instances.remove(instance_id).is_some();
        }
        false
    }

    /// Records one metrics heartbeat sample emitted by an instance.
    ///
    /// This sample stream is used to derive liveness and heartbeat regularity.
    pub fn record_metrics_heartbeat(&self, service_id: &str, instance_id: &str) {
        if let Some(mut service) = self.services.get_mut(service_id) {
            if let Some(instance) = service.instances.get_mut(instance_id) {
                let now = Utc::now();
                instance.last_heartbeat_utc = now;

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
                        hb.ema_interval_ms =
                            alpha * interval_ms + (1.0 - alpha) * hb.ema_interval_ms;
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
    }

    /// Recomputes instance health from metrics heartbeat recency and regularity.
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
                let Some(hb) = self.metrics_heartbeat.get(&key) else {
                    // No metrics observed yet for this instance.
                    instance.status = InstanceStatus::Down;
                    continue;
                };

                let silent_ms = now
                    .signed_duration_since(hb.last_seen_utc)
                    .num_milliseconds()
                    .max(0) as f64;

                if silent_ms > timeout_ms {
                    instance.status = InstanceStatus::Down;
                    continue;
                }

                let has_stable_baseline = hb.sample_count >= 5 && hb.ema_interval_ms > 0.0;
                if has_stable_baseline {
                    let jitter_ratio = hb.ema_jitter_ms / hb.ema_interval_ms;
                    let cadence_gap_ratio = silent_ms / hb.ema_interval_ms;
                    if jitter_ratio >= 0.4 || cadence_gap_ratio >= 2.5 {
                        instance.status = InstanceStatus::Degraded;
                        continue;
                    }
                }

                instance.status = InstanceStatus::Up;
            }
        }
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
        if let Some(service) = self.services.get(service_id) {
            if let Some(instance) = service.instances.get(instance_id) {
                return instance.token == token;
            }
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
        self.path_owners.insert(path_prefix, service_id);
    }

    /// Resolves the best-matching service by the longest owned path prefix.
    pub fn resolve_service_by_path(&self, path: &str) -> Option<String> {
        self.path_owners
            .iter()
            .filter(|r| path.starts_with(r.key()))
            .max_by_key(|r| r.key().len())
            .map(|r| r.value().clone())
    }

    /// Removes down instances that exceeded the configured timeout.
    pub fn remove_stale_instances(&self, timeout: std::time::Duration) {
        let now = Utc::now();
        for mut service in self.services.iter_mut() {
            service.instances.retain(|_, instance| {
                if instance.status == InstanceStatus::Down {
                    let elapsed = now.signed_duration_since(instance.last_heartbeat_utc);
                    if elapsed.to_std().unwrap_or(std::time::Duration::ZERO) > timeout {
                        self.metrics_heartbeat
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
        if let Some(mut service) = self.services.get_mut(service_id) {
            if let Some(instance) = service.instances.get_mut(instance_id) {
                instance.status = status;
                if status == InstanceStatus::Up {
                    instance.last_heartbeat_utc = Utc::now();
                }
            }
        }
    }
}

fn metrics_key(service_id: &str, instance_id: &str) -> String {
    format!("{}:{}", service_id, instance_id)
}
