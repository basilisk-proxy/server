use crate::service_bus::contracts::{
    ServiceBusEventEnvelope, ServiceBusForwardRequest, ServiceBusForwardResponse,
    ServiceBusProtocolMessage, BASILISK_INSTANCE_ID, BASILISK_SERVICE_ID,
};
use dashmap::DashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tracing::{debug, info, warn};

/// Active service-bus connection metadata tracked by the broker.
pub struct ServiceBusConnection {
    pub service_id: String,
    pub instance_id: String,
    pub tx: mpsc::UnboundedSender<ServiceBusProtocolMessage>,
    pub authenticated: bool,
    pub subscriptions: Vec<String>,
}

/// In-memory connection and subscription registry for service-bus clients.
pub struct ConnectionManager {
    connections: DashMap<String, ServiceBusConnection>,
    next_correlation_id: AtomicI64,
    internal_subscribers: DashMap<String, mpsc::UnboundedSender<ServiceBusEventEnvelope>>,

    /// Receiver half-exposed so the Lua runtime can drain incoming bus messages.
    pub internal_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<ServiceBusProtocolMessage>>,
}

/// Connection key for the built-in reserved identity.
pub fn basilisk_connection_key() -> String {
    format!("{}:{}", BASILISK_SERVICE_ID, BASILISK_INSTANCE_ID)
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionManager {
    /// Creates an empty manager and seeds the reserved basilisk identity as already
    /// connected and authenticated.
    pub fn new() -> Self {
        let (internal_tx, internal_rx) = mpsc::unbounded_channel::<ServiceBusProtocolMessage>();

        let connections: DashMap<String, ServiceBusConnection> = DashMap::new();
        let key = basilisk_connection_key();
        connections.insert(
            key,
            ServiceBusConnection {
                service_id: BASILISK_SERVICE_ID.to_string(),
                instance_id: BASILISK_INSTANCE_ID.to_string(),
                tx: internal_tx,
                authenticated: true,
                subscriptions: Vec::new(),
            },
        );

        Self {
            connections,
            next_correlation_id: AtomicI64::new(1),
            internal_subscribers: DashMap::new(),
            internal_rx: tokio::sync::Mutex::new(internal_rx),
        }
    }

    /// Adds or replaces a connection by key.
    pub fn add_connection(&self, key: String, conn: ServiceBusConnection) {
        info!(
            connection_key = %key,
            service_id = %conn.service_id,
            instance_id = %conn.instance_id,
            authenticated = conn.authenticated,
            "service bus connection added"
        );
        self.connections.insert(key, conn);
    }

    /// Removes a connection and its subscriptions.
    pub fn remove_connection(&self, key: &str) {
        // Never remove the reserved basilisk identity.
        if key == basilisk_connection_key() {
            warn!(connection_key = %key, "attempted to remove reserved basilisk connection");
            return;
        }
        debug!(connection_key = %key, "service bus connection removed");
        self.connections.remove(key);
    }

    /// Marks a connection as authenticated.
    pub fn authenticate(&self, key: &str) {
        if let Some(mut conn) = self.connections.get_mut(key) {
            conn.authenticated = true;
            debug!(
                connection_key = %key,
                service_id = %conn.service_id,
                instance_id = %conn.instance_id,
                "service bus connection authenticated"
            );
        }
    }

    /// Adds topic subscriptions to a connection.
    pub fn add_subscriptions(&self, key: &str, topics: Vec<String>) {
        if let Some(mut conn) = self.connections.get_mut(key) {
            let mut added = Vec::new();
            for topic in topics {
                if !conn.subscriptions.contains(&topic) {
                    added.push(topic.clone());
                    conn.subscriptions.push(topic);
                }
            }
            if !added.is_empty() {
                debug!(
                    connection_key = %key,
                    service_id = %conn.service_id,
                    instance_id = %conn.instance_id,
                    topics = ?added,
                    total_subscriptions = conn.subscriptions.len(),
                    "service bus subscriptions added"
                );
            }
        }
    }

    /// Removes topic subscriptions from a connection.
    pub fn remove_subscriptions(&self, key: &str, topics: Vec<String>) {
        if let Some(mut conn) = self.connections.get_mut(key) {
            let before = conn.subscriptions.len();
            conn.subscriptions.retain(|t| !topics.contains(t));
            let removed = before.saturating_sub(conn.subscriptions.len());
            if removed > 0 {
                debug!(
                    connection_key = %key,
                    service_id = %conn.service_id,
                    instance_id = %conn.instance_id,
                    topics = ?topics,
                    removed_count = removed,
                    total_subscriptions = conn.subscriptions.len(),
                    "service bus subscriptions removed"
                );
            }
        }
    }

    /// Returns authenticated subscribers for a topic, optionally excluding one key.
    pub fn get_subscribers(
        &self,
        topic: &str,
        exclude_key: &str,
    ) -> Vec<(String, mpsc::UnboundedSender<ServiceBusProtocolMessage>)> {
        self.connections
            .iter()
            .filter(|r| {
                r.key() != exclude_key
                    && r.value().authenticated
                    && (r.value().subscriptions.contains(&topic.to_string())
                        || r.value().subscriptions.contains(&String::from("*")))
            })
            .map(|r| (r.key().clone(), r.value().tx.clone()))
            .collect()
    }

    /// Returns the next monotonic correlation id.
    pub fn next_correlation_id(&self) -> i64 {
        self.next_correlation_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Returns whether a connection is authenticated.
    pub fn is_authenticated(&self, key: &str) -> bool {
        self.connections
            .get(key)
            .map(|c| c.authenticated)
            .unwrap_or(false)
    }

    /// Returns `(service_id, instance_id)` for a connection key.
    pub fn get_connection_info(&self, key: &str) -> Option<(String, String)> {
        self.connections
            .get(key)
            .map(|c| (c.service_id.clone(), c.instance_id.clone()))
    }

    /// Publishes an event to matching internal and external subscribers.
    pub fn publish(&self, event: ServiceBusEventEnvelope, exclude_key: Option<&str>) -> i32 {
        let mut delivered_count = 0;
        debug!(
            topic = %event.topic,
            event_id = %event.event_id,
            source_service_id = %event.service_id,
            source_instance_id = %event.instance_id,
            correlation_id = event.correlation_id,
            exclude_connection_key = ?exclude_key,
            "service bus publish start"
        );

        // Internal subscribers
        if let Some(tx) = self.internal_subscribers.get(&event.topic) {
            if tx.send(event.clone()).is_ok() {
                delivered_count += 1;
            }
        }

        let subscribers = self.get_subscribers(&event.topic, exclude_key.unwrap_or(""));

        for (sub_key, sub_tx) in subscribers {
            let sub_msg = ServiceBusProtocolMessage {
                r#type: crate::service_bus::contracts::protocol_types::EVENT.to_string(),
                event: Some(event.clone()),
                ..Default::default()
            };
            if sub_tx.send(sub_msg).is_ok() {
                delivered_count += 1;
            } else {
                warn!(
                    topic = %event.topic,
                    subscriber_connection_key = %sub_key,
                    "service bus publish failed to deliver to subscriber; removing connection"
                );
                self.remove_connection(&sub_key);
            }
        }
        debug!(
            topic = %event.topic,
            event_id = %event.event_id,
            correlation_id = event.correlation_id,
            delivered_count,
            "service bus publish completed"
        );
        delivered_count
    }

    /// Subscribes an internal channel to a topic.
    pub fn subscribe_internal(
        &self,
        topic: String,
        tx: mpsc::UnboundedSender<ServiceBusEventEnvelope>,
    ) {
        debug!(topic = %topic, "service bus internal subscriber added");
        self.internal_subscribers.insert(topic, tx);
    }

    /// Unsubscribes an internal channel from a topic.
    pub fn unsubscribe_internal(&self, topic: &str) {
        debug!(topic = %topic, "service bus internal subscriber removed");
        self.internal_subscribers.remove(topic);
    }

    /// Subscribes the reserved basilisk connection to the given topics so that
    /// events published to those topics reach the Lua runtime via `internal_rx`.
    pub fn subscribe_basilisk(&self, topics: Vec<String>) {
        let key = basilisk_connection_key();
        debug!(topics = ?topics, "service bus basilisk subscription update");
        self.add_subscriptions(&key, topics);
    }

    /// Unsubscribes the reserved basilisk connection from the given topics.
    pub fn unsubscribe_basilisk(&self, topics: Vec<String>) {
        let key = basilisk_connection_key();
        debug!(topics = ?topics, "service bus basilisk unsubscription update");
        self.remove_subscriptions(&key, topics);
    }

    /// Sends a forward request from the basilisk identity and waits for the
    /// response, returning the `ServiceBusForwardResponse` or an error string.
    pub async fn forward_from_basilisk(
        &self,
        req: ServiceBusForwardRequest,
    ) -> Result<ServiceBusForwardResponse, String> {
        use chrono::Utc;
        use uuid::Uuid;

        if req.target_service_id.trim().is_empty() {
            return Err("forwardRequest.targetServiceId is required".to_string());
        }

        let request_id = format!("basilisk-{}", Uuid::now_v7());
        let reply_to_topic = format!("reply-to-{}", request_id);
        debug!(
            request_id = %request_id,
            target_service_id = %req.target_service_id,
            message_type = %req.message_type,
            timeout_ms = req.timeout_ms.unwrap_or(30_000).min(120_000),
            "service bus forward_from_basilisk started"
        );

        let (tx, mut rx) = mpsc::unbounded_channel::<ServiceBusEventEnvelope>();
        self.subscribe_internal(reply_to_topic.clone(), tx);

        let mut payload = req.payload.clone();
        payload.insert(
            "reply_to".to_string(),
            serde_json::Value::String(reply_to_topic.clone()),
        );

        let correlation_id = self.next_correlation_id();
        let event = ServiceBusEventEnvelope {
            event_id: request_id.clone(),
            emitted_at_utc: Utc::now(),
            service_id: BASILISK_SERVICE_ID.to_string(),
            instance_id: BASILISK_INSTANCE_ID.to_string(),
            topic: format!("service-{}", req.target_service_id),
            message_type: req.message_type.clone(),
            correlation_id,
            causation_id: None,
            payload,
        };

        let key = basilisk_connection_key();
        let delivered = self.publish(event, Some(&key));
        if delivered == 0 {
            self.unsubscribe_internal(&reply_to_topic);
            warn!(
                request_id = %request_id,
                target_service_id = %req.target_service_id,
                "service bus forward_from_basilisk failed: target has no subscribers"
            );
            return Err(format!(
                "No subscribers available for target service '{}'",
                req.target_service_id
            ));
        }

        let timeout_ms = req.timeout_ms.unwrap_or(30_000).min(120_000);
        let result = timeout(Duration::from_millis(timeout_ms), rx.recv()).await;
        self.unsubscribe_internal(&reply_to_topic);

        match result {
            Ok(Some(event)) => {
                debug!(
                    request_id = %request_id,
                    target_service_id = %req.target_service_id,
                    response_message_type = %event.message_type,
                    correlation_id = event.correlation_id,
                    "service bus forward_from_basilisk completed"
                );
                Ok(ServiceBusForwardResponse {
                    message_type: event.message_type,
                    payload: event.payload,
                })
            }
            Ok(None) => {
                warn!(request_id = %request_id, target_service_id = %req.target_service_id, "service bus forward_from_basilisk failed: response channel closed");
                Err("Forward response channel closed".to_string())
            }
            Err(_) => {
                warn!(request_id = %request_id, target_service_id = %req.target_service_id, timeout_ms, "service bus forward_from_basilisk timed out");
                Err(format!(
                    "Timeout waiting for forward response from '{}'",
                    req.target_service_id
                ))
            }
        }
    }
}
