use serde::{Deserialize, Serialize};

/// Top-level runtime configuration consumed by the gateway, registry and service bus.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GatewayConfig {
    pub server: ServerOptions,
    pub routing: RoutingOptions,
    pub cache: CacheOptions,
    pub registry: RegistryOptions,
    pub security: SecurityOptions,
    pub observability: ObservabilityOptions,
    pub service_bus: ServiceBusOptions,
}

/// HTTP server binding and TLS options.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerOptions {
    pub host: String,
    pub port: u16,
    pub tls: TlsOptions,
}

/// TLS certificate and key material references.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TlsOptions {
    pub enabled: bool,
    pub cert_file: String,
    pub key_file: String,
}

/// Reverse-proxy routing behavior.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RoutingOptions {
    pub default_load_balancing_strategy: String,
    pub strip_prefix: bool,
}

/// Shared gateway cache provider selection and defaults.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CacheOptions {
    pub enabled: bool,
    pub provider: String,
    pub key_prefix: String,
    pub service_resolution_ttl_seconds: u64,
    pub ttl_seconds: u64,
    pub strategy: String,
}

/// Service-registry maintenance timing options.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegistryOptions {
    pub heartbeat_timeout_seconds: u64,
}

/// Security-related gateway policies.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SecurityOptions {
    pub service_registration_auth: String,
    pub registration_token: String,
}

/// Logging and telemetry behavior.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObservabilityOptions {
    pub metrics_enabled: bool,
    pub tracing_enabled: bool,
    pub log_level: String,
}

/// Service bus transport settings.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceBusOptions {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub max_message_chars: usize,
    pub connection_health_enabled: bool,
    pub monitoring_enabled: bool,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            server: ServerOptions {
                host: "0.0.0.0".to_string(),
                port: 443,
                tls: TlsOptions {
                    enabled: false,
                    cert_file: "".to_string(),
                    key_file: "".to_string(),
                },
            },
            routing: RoutingOptions {
                default_load_balancing_strategy: "ROUND_ROBIN".to_string(),
                strip_prefix: true,
            },
            cache: CacheOptions {
                enabled: true,
                provider: "memory".to_string(),
                key_prefix: "runtime".to_string(),
                service_resolution_ttl_seconds: 300,
                ttl_seconds: 300,
                strategy: "lru".to_string(),
            },
            registry: RegistryOptions {
                heartbeat_timeout_seconds: 30,
            },
            security: SecurityOptions {
                service_registration_auth: "TOKEN".to_string(),
                registration_token: "secret-token".to_string(),
            },
            observability: ObservabilityOptions {
                metrics_enabled: true,
                tracing_enabled: true,
                log_level: "info".to_string(),
            },
            service_bus: ServiceBusOptions {
                enabled: true,
                host: "0.0.0.0".to_string(),
                port: 5090,
                max_message_chars: 65536,
                connection_health_enabled: true,
                monitoring_enabled: true,
            },
        }
    }
}
