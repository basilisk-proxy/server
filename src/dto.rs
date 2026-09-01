use crate::models::{InstanceStatus, ServiceDefinition};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServiceData {
    pub service_id: String,
    pub path_prefixes: Vec<String>,
    pub instances: HashMap<String, InstanceData>,
}

/// Concrete network endpoint and health metadata for a service instance.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InstanceData {
    pub scheme: String,
    pub host: String,
    pub status: InstanceStatus,
    pub active_connections: i32,
    pub last_heartbeat_utc: DateTime<Utc>,
}

impl ServiceData {
    pub fn from(source: &ServiceDefinition) -> Self {
        let instances: HashMap<String, InstanceData> =
            HashMap::from_iter(source.instances.iter().map(|kv| {
                let (_, instance) = kv;
                (
                    instance.instance_id.clone(),
                    InstanceData {
                        scheme: instance.scheme.clone(),
                        host: instance.host.clone(),
                        status: instance.status,
                        active_connections: instance.active_connections,
                        last_heartbeat_utc: instance.last_heartbeat_utc,
                    },
                )
            }));

        Self {
            service_id: source.service_id.clone(),
            path_prefixes: source.path_prefixes.clone(),
            instances,
        }
    }
}
