pub mod maintenance;
pub use maintenance::run_maintenance;

use crate::models::{InstanceStatus, RegistrationRequest, ServiceDefinition, ServiceInstance};
use chrono::Utc;
use dashmap::DashMap;
use uuid::Uuid;

pub struct ServiceRegistry {
    services: DashMap<String, ServiceDefinition>,
    path_owners: DashMap<String, String>,
}

/// Result returned from a registration attempt.
pub struct RegistrationResult {
    pub success: bool,
    pub token: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl ServiceRegistry {
    /// Creates an empty in-memory service registry.
    pub fn new() -> Self {
        Self {
            services: DashMap::new(),
            path_owners: DashMap::new(),
        }
    }

    /// Registers a service instance and reserves declared route prefixes.
    pub async fn register(&self, request: RegistrationRequest) -> RegistrationResult {
        // 1. Check for prefix collisions
        for prefix in &request.path_prefixes {
            if let Some(owner) = self.path_owners.get(prefix) {
                if owner.value() != &request.service_id {
                    return RegistrationResult {
                        success: false,
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
                token: None,
                error_code: Some("FINGERPRINT_INVALID".to_string()),
                error_message: Some("Fingerprint mismatch".to_string()),
            };
        }

        let token = Uuid::new_v4().to_string();
        let instance = ServiceInstance {
            instance_id: request.instance.instance_id.clone(),
            service_id: request.service_id.clone(),
            token: token.clone(),
            scheme: request.instance.scheme,
            host: request.instance.host,
            port: request.instance.port,
            weight: request.instance.weight,
            health_check: request.health_check,
            status: InstanceStatus::Up,
            active_connections: 0,
            last_heartbeat_utc: Utc::now(),
        };

        service
            .instances
            .insert(request.instance.instance_id, instance);

        RegistrationResult {
            success: true,
            token: Some(token),
            error_code: None,
            error_message: None,
        }
    }

    /// Marks an instance as alive and updates its heartbeat timestamp.
    pub async fn heartbeat(&self, service_id: &str, instance_id: &str) -> bool {
        if let Some(mut service) = self.services.get_mut(service_id) {
            if let Some(instance) = service.instances.get_mut(instance_id) {
                instance.last_heartbeat_utc = Utc::now();
                instance.status = InstanceStatus::Up;
                return true;
            }
        }
        false
    }

    /// Removes an instance from a service definition.
    pub async fn deregister(&self, service_id: &str, instance_id: &str) -> bool {
        if let Some(mut service) = self.services.get_mut(service_id) {
            return service.instances.remove(instance_id).is_some();
        }
        false
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
