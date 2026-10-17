use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Health state reported or inferred for a service instance.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum InstanceStatus {
    Up,
    Down,
    Degraded,
}

/// Concrete network endpoint and health metadata for a service instance.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceInstance {
    pub instance_id: String,
    pub service_id: String,
    pub token: String,
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub weight: i32,
    pub status: InstanceStatus,
    pub active_connections: i32,
    pub last_heartbeat_utc: DateTime<Utc>,
}

impl ServiceInstance {
    /// Returns the base URI for this instance.
    ///
    /// When `port == 0`, the URI is emitted without an explicit port, so the
    /// upstream can rely on its default port for the scheme.
    pub fn to_uri(&self) -> String {
        let host = format_uri_host(&self.host);
        if self.port == 0 {
            format!("{}://{}", self.scheme, host)
        } else {
            format!("{}://{}:{}", self.scheme, host, self.port)
        }
    }
}

pub(crate) fn format_uri_host(host: &str) -> String {
    if host.starts_with('[') || !host.contains(':') {
        host.to_string()
    } else {
        format!("[{host}]")
    }
}

/// Service definition containing identity, route ownership, and instances.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceDefinition {
    pub service_id: String,
    pub fingerprint: String,
    pub path_prefixes: Vec<String>,
    pub instances: HashMap<String, ServiceInstance>,
}

/// Registration payload sent by services to the registry endpoint.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegistrationRequest {
    #[serde(rename = "serviceId")]
    pub service_id: String,
    pub fingerprint: String,
    #[serde(rename = "pathPrefixes")]
    pub path_prefixes: Vec<String>,
    pub instance: InstanceInfo,
    pub auth: AuthInfo,
}

/// Network identity of an instance being registered.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InstanceInfo {
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    pub scheme: String,
    pub host: String,
    pub port: u16,
    pub weight: i32,
}

/// Registration authentication information.
///
/// `token` carries the shared registration token for `TOKEN` auth and stays
/// empty for key-pair auth. `signature` carries an Ed25519 signature over the
/// service id, base64-encoded (URL-safe, no padding), for `KEY_PAIR` auth.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthInfo {
    pub r#type: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub signature: Option<String>,
}

/// Supported registration authentication modes.
pub mod auth_modes {
    /// Shared-token registration authentication (default).
    pub const TOKEN: &str = "TOKEN";
    /// Key-pair registration authentication: the service signs its own service id
    /// and the gateway verifies it against the Lua-configured public key.
    pub const KEY_PAIR: &str = "KEY_PAIR";
}

/// Standard API error envelope.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorResponse {
    pub error: ErrorDetail,
}

/// Error payload details.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
}

/// Stable error codes used by HTTP responses.
pub mod error_codes {
    pub const AUTH_FAILED: &str = "AUTH_FAILED";
    pub const INVALID_CONFIG: &str = "INVALID_CONFIG";
    pub const FINGERPRINT_INVALID: &str = "FINGERPRINT_INVALID";
    pub const REGISTRATION_IP_NOT_ALLOWED: &str = "REGISTRATION_IP_NOT_ALLOWED";
    /// A key-pair registration failed verification.
    pub const SIGNATURE_INVALID: &str = "SIGNATURE_INVALID";
}
