use basilisk::service_bus::connection_manager::{ConnectionManager, ServiceBusConnection};
use basilisk::service_bus::contracts::{
    protocol_types, ServiceBusEventEnvelope, ServiceBusForwardRequest, ServiceBusProtocolMessage,
    BASILISK_INSTANCE_ID, BASILISK_SERVICE_ID,
};
use basilisk::{config::GatewayConfig, registry::ServiceRegistry, service_bus::server::run_server};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

fn find_free_local_port() -> u16 {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind ephemeral port");
    let port = listener
        .local_addr()
        .expect("failed to read local addr")
        .port();
    drop(listener);
    port
}

async fn start_test_bus_server(port: u16) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let mut config = GatewayConfig::default();
    config.service_bus.host = "127.0.0.1".to_string();
    config.service_bus.port = port;
    let manager = Arc::new(ConnectionManager::new());
    let registry = Arc::new(ServiceRegistry::new());
    let handle = tokio::spawn(async move { run_server(config, manager, registry).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle
}

async fn send_connect_and_read_reply(
    port: u16,
    service_id: &str,
    instance_id: &str,
) -> ServiceBusProtocolMessage {
    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("failed to connect to service bus");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let connect = ServiceBusProtocolMessage {
        r#type: protocol_types::CONNECT.to_string(),
        service_id: Some(service_id.to_string()),
        instance_id: Some(instance_id.to_string()),
        ..Default::default()
    };

    let mut wire = serde_json::to_string(&connect).expect("failed to serialize connect message");
    wire.push('\n');
    writer
        .write_all(wire.as_bytes())
        .await
        .expect("failed to write connect message");

    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .expect("failed to read server response");
    serde_json::from_str(&line).expect("failed to parse server response")
}

#[tokio::test]
async fn connection_manager_publishes_to_authenticated_subscribers() {
    let manager = ConnectionManager::new();

    let (tx_a, mut rx_a) = mpsc::unbounded_channel::<ServiceBusProtocolMessage>();
    manager.add_connection(
        "svc-a:inst-1".to_string(),
        ServiceBusConnection {
            service_id: "svc-a".to_string(),
            instance_id: "inst-1".to_string(),
            tx: tx_a,
            authenticated: false,
            subscriptions: vec![],
        },
    );
    manager.authenticate("svc-a:inst-1");
    manager.add_subscriptions("svc-a:inst-1", vec!["topic.orders".to_string()]);

    let event = ServiceBusEventEnvelope {
        event_id: "evt-1".to_string(),
        emitted_at_utc: Utc::now(),
        service_id: "producer".to_string(),
        instance_id: "producer-1".to_string(),
        topic: "topic.orders".to_string(),
        message_type: "event".to_string(),
        correlation_id: manager.next_correlation_id(),
        causation_id: None,
        payload: HashMap::new(),
    };

    let delivered = manager.publish(event, None);
    assert_eq!(delivered, 1);

    let msg = rx_a
        .recv()
        .await
        .expect("subscriber should receive published event");
    assert_eq!(msg.r#type, protocol_types::EVENT);
    assert!(msg.event.is_some());
}

#[tokio::test]
async fn connection_manager_internal_subscriptions_are_supported() {
    let manager = ConnectionManager::new();
    let (tx, mut rx) = mpsc::unbounded_channel();

    manager.subscribe_internal("topic.internal".to_string(), tx);

    let event = ServiceBusEventEnvelope {
        event_id: "evt-internal".to_string(),
        emitted_at_utc: Utc::now(),
        service_id: "producer".to_string(),
        instance_id: "producer-1".to_string(),
        topic: "topic.internal".to_string(),
        message_type: "event".to_string(),
        correlation_id: manager.next_correlation_id(),
        causation_id: None,
        payload: HashMap::new(),
    };

    let delivered = manager.publish(event, None);
    assert_eq!(delivered, 1);

    let internal = rx
        .recv()
        .await
        .expect("internal subscriber should receive event");
    assert_eq!(internal.topic, "topic.internal");

    manager.unsubscribe_internal("topic.internal");
}

#[test]
fn protocol_message_serializes_forward_request_fields() {
    let message = ServiceBusProtocolMessage {
        r#type: protocol_types::FORWARD.to_string(),
        forward_request: Some(ServiceBusForwardRequest {
            target_service_id: "orders".to_string(),
            path: "/v1/orders/1".to_string(),
            method: "GET".to_string(),
            headers: HashMap::from([("x-request-id".to_string(), "abc".to_string())]),
            body: None,
            timeout_ms: Some(30_000),
        }),
        ..Default::default()
    };

    let json = serde_json::to_string(&message).expect("serialization should work");
    assert!(json.contains("\"type\":\"forward\""));
    assert!(json.contains("\"forwardRequest\""));
    assert!(json.contains("\"targetServiceId\":\"orders\""));
}

// ──────────────────────────────────────────────────────────────────────────────
// Reserved identity tests
// ──────────────────────────────────────────────────────────────────────────────

#[test]
fn reserved_constants_have_expected_values() {
    assert_eq!(BASILISK_SERVICE_ID, "basilisk");
    assert_eq!(BASILISK_INSTANCE_ID, "lua-runtime");
}

#[tokio::test]
async fn connection_manager_seeds_basilisk_identity_as_authenticated() {
    let manager = ConnectionManager::new();
    let key = format!("{}:{}", BASILISK_SERVICE_ID, BASILISK_INSTANCE_ID);
    assert!(
        manager.is_authenticated(&key),
        "basilisk:lua-runtime must be pre-authenticated"
    );
}

#[tokio::test]
async fn connection_manager_does_not_remove_reserved_identity() {
    let manager = ConnectionManager::new();
    let key = format!("{}:{}", BASILISK_SERVICE_ID, BASILISK_INSTANCE_ID);

    manager.remove_connection(&key);

    // The reserved key must survive remove_connection.
    assert!(
        manager.is_authenticated(&key),
        "reserved identity must not be removable"
    );
}

#[tokio::test]
async fn subscribe_basilisk_adds_topic_to_reserved_connection() {
    let manager = ConnectionManager::new();
    manager.subscribe_basilisk(vec!["topic.alerts".to_string()]);

    // Now publish an event to that topic and confirm the reserved connection is
    // counted (via its tx) as a subscriber.
    let event = ServiceBusEventEnvelope {
        event_id: "evt-b".to_string(),
        emitted_at_utc: Utc::now(),
        service_id: "external-svc".to_string(),
        instance_id: "external-inst".to_string(),
        topic: "topic.alerts".to_string(),
        message_type: "alert".to_string(),
        correlation_id: manager.next_correlation_id(),
        causation_id: None,
        payload: HashMap::new(),
    };

    let delivered = manager.publish(event, None);
    assert!(
        delivered >= 1,
        "basilisk subscription must receive the event"
    );

    // Validate that reserved-connection delivery reaches the loopback channel.
    let mut rx = manager.internal_rx.lock().await;
    let msg = timeout(Duration::from_millis(250), rx.recv())
        .await
        .expect("timed out waiting for reserved loopback message")
        .expect("reserved loopback channel closed unexpectedly");
    assert_eq!(msg.r#type, protocol_types::EVENT);
    assert_eq!(
        msg.event.as_ref().map(|e| e.topic.as_str()),
        Some("topic.alerts")
    );

    manager.unsubscribe_basilisk(vec!["topic.alerts".to_string()]);
}

#[tokio::test]
async fn forward_from_basilisk_errors_when_no_subscriber() {
    let manager = ConnectionManager::new();
    let req = ServiceBusForwardRequest {
        target_service_id: "nonexistent-service".to_string(),
        path: "/ping".to_string(),
        method: "GET".to_string(),
        headers: HashMap::new(),
        body: None,
        timeout_ms: Some(500),
    };

    let result = manager.forward_from_basilisk(req).await;
    assert!(
        result.is_err(),
        "forward to unknown service must return an error"
    );
    let msg = result.unwrap_err();
    assert!(
        msg.contains("nonexistent-service"),
        "error must name the missing target: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn server_rejects_reserved_service_id_on_connect() {
    let port = find_free_local_port();
    let handle = start_test_bus_server(port).await;

    let response = send_connect_and_read_reply(port, BASILISK_SERVICE_ID, "any-instance").await;

    assert_eq!(response.r#type, protocol_types::ERROR);
    assert_eq!(response.error_code.as_deref(), Some("RESERVED_IDENTITY"));

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn server_rejects_reserved_instance_id_on_connect() {
    let port = find_free_local_port();
    let handle = start_test_bus_server(port).await;

    let response =
        send_connect_and_read_reply(port, "external-service", BASILISK_INSTANCE_ID).await;

    assert_eq!(response.r#type, protocol_types::ERROR);
    assert_eq!(response.error_code.as_deref(), Some("RESERVED_IDENTITY"));

    handle.abort();
}
