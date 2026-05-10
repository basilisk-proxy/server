use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Reserved service identity used by the Lua runtime on the service bus.
/// External clients are prevented from claiming this service_id.
pub const BASILISK_SERVICE_ID: &str = "basilisk";

/// Reserved instance identity used by the Lua runtime on the service bus.
/// External clients are prevented from claiming this instance_id.
pub const BASILISK_INSTANCE_ID: &str = "lua-runtime";

/// Canonical topic for service-emitted distribution metrics consumed by Basilisk.
pub const BASILISK_METRICS_DISTRIBUTION_TOPIC: &str = "basilisk.metrics.distribution";

/// Wire-level `type` values used by the service-bus protocol.
pub mod protocol_types {
    pub const CONNECT: &str = "connect";
    pub const AUTHENTICATE: &str = "authenticate";
    pub const SUBSCRIBE: &str = "subscribe";
    pub const UNSUBSCRIBE: &str = "unsubscribe";
    pub const PUBLISH: &str = "publish";
    pub const FORWARD: &str = "forward";
    pub const FORWARD_RESPONSE: &str = "forward_response";
    pub const EVENT: &str = "event";
    pub const ACK: &str = "ack";
    pub const ERROR: &str = "error";
}

/// Event envelope routed between service-bus publishers and subscribers.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceBusEventEnvelope {
    #[serde(rename = "eventId")]
    pub event_id: String,
    #[serde(rename = "emittedAtUtc")]
    pub emitted_at_utc: DateTime<Utc>,
    #[serde(rename = "serviceId")]
    pub service_id: String,
    #[serde(rename = "instanceId")]
    pub instance_id: String,
    pub topic: String,
    #[serde(rename = "messageType")]
    pub message_type: String,
    #[serde(rename = "correlationId")]
    pub correlation_id: i64,
    #[serde(rename = "causationId")]
    pub causation_id: Option<String>,
    pub payload: HashMap<String, serde_json::Value>,
}

/// Request payload for service-bus-driven forwarding.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceBusForwardRequest {
    #[serde(rename = "targetServiceId")]
    pub target_service_id: String,
    pub path: String,
    pub method: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
    #[serde(rename = "timeoutMs")]
    pub timeout_ms: Option<u64>,
}

/// Response payload for service-bus-driven forwarding.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceBusForwardResponse {
    pub status: u16,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    pub body: String,
}

/// Generic wire message used by service-bus clients.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ServiceBusProtocolMessage {
    pub r#type: String,
    #[serde(rename = "serviceId")]
    pub service_id: Option<String>,
    #[serde(rename = "instanceId")]
    pub instance_id: Option<String>,
    pub token: Option<String>,
    pub topics: Option<Vec<String>>,
    pub event: Option<ServiceBusEventEnvelope>,
    #[serde(rename = "forwardRequest")]
    pub forward_request: Option<ServiceBusForwardRequest>,
    #[serde(rename = "forwardResponse")]
    pub forward_response: Option<ServiceBusForwardResponse>,
    pub message: Option<String>,
    #[serde(rename = "errorCode")]
    pub error_code: Option<String>,
    #[serde(rename = "subscriberCount")]
    pub subscriber_count: Option<i32>,
}
