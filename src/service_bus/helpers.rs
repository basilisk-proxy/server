use crate::service_bus::contracts::ServiceBusEventEnvelope;
use std::collections::HashMap;

pub(crate) fn get_headers_from_event(event: &ServiceBusEventEnvelope) -> HashMap<String, String> {
    event
        .payload
        .get("headers")
        .and_then(|v| serde_json::from_value::<HashMap<String, String>>(v.clone()).ok())
        .unwrap_or_default()
}
