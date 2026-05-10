use crate::config::GatewayConfig;
use crate::models::InstanceStatus;
use crate::registry::ServiceRegistry;
use crate::service_bus::connection_manager::{ConnectionManager, ServiceBusConnection};
use crate::service_bus::contracts::{
    protocol_types, ServiceBusEventEnvelope, ServiceBusForwardRequest, ServiceBusForwardResponse,
    ServiceBusProtocolMessage, BASILISK_INSTANCE_ID, BASILISK_METRICS_DISTRIBUTION_TOPIC,
    BASILISK_SERVICE_ID,
};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Starts the TCP service-bus server and serves client sessions indefinitely.
pub async fn run_server(
    config: GatewayConfig,
    connection_manager: Arc<ConnectionManager>,
    registry: Arc<ServiceRegistry>,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", config.service_bus.host, config.service_bus.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Service bus TCP server listening on {}", addr);

    loop {
        let (socket, _) = listener.accept().await?;
        let connection_manager = Arc::clone(&connection_manager);
        let registry = Arc::clone(&registry);
        let max_message_chars = config.service_bus.max_message_chars;
        let connection_health_enabled = config.service_bus.connection_health_enabled;
        let monitoring_enabled = config.service_bus.monitoring_enabled;

        tokio::spawn(async move {
            if let Err(e) = handle_client(
                socket,
                connection_manager,
                registry,
                max_message_chars,
                connection_health_enabled,
                monitoring_enabled,
            )
            .await
            {
                error!("Error handling client: {}", e);
            }
        });
    }
}

async fn handle_client(
    socket: TcpStream,
    connection_manager: Arc<ConnectionManager>,
    registry: Arc<ServiceRegistry>,
    max_message_chars: usize,
    connection_health_enabled: bool,
    monitoring_enabled: bool,
) -> anyhow::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let (tx, mut rx) = mpsc::unbounded_channel::<ServiceBusProtocolMessage>();
    let mut connection_key: Option<String> = None;

    let mut line = String::new();

    loop {
        tokio::select! {
            result = reader.read_line(&mut line) => {
                let bytes_read = match result {
                    Ok(b) => b,
                    Err(_) => break,
                };
                if bytes_read == 0 {
                    break;
                }

                if line.len() > max_message_chars {
                    let _ = tx.send(ServiceBusProtocolMessage {
                        r#type: protocol_types::ERROR.to_string(),
                        error_code: Some("MESSAGE_TOO_LARGE".to_string()),
                        message: Some(format!("Message exceeds max allowed size of {} characters", max_message_chars)),
                        ..Default::default()
                    });
                    break;
                }

                let msg: ServiceBusProtocolMessage = match serde_json::from_str(&line) {
                    Ok(m) => m,
                    Err(_) => {
                        let _ = tx.send(ServiceBusProtocolMessage {
                            r#type: protocol_types::ERROR.to_string(),
                            error_code: Some("INVALID_JSON".to_string()),
                            message: Some("Message must be valid JSON".to_string()),
                            ..Default::default()
                        });
                        line.clear();
                        continue;
                    }
                };
                line.clear();

                match msg.r#type.as_str() {
                    protocol_types::CONNECT => {
                        if connection_key.is_some() {
                            let _ = tx.send(ServiceBusProtocolMessage {
                                r#type: protocol_types::ERROR.to_string(),
                                error_code: Some("ALREADY_CONNECTED".to_string()),
                                message: Some("Connection is already established".to_string()),
                                ..Default::default()
                            });
                        } else if let (Some(sid), Some(iid)) =
                            (msg.service_id.clone(), msg.instance_id.clone())
                        {
                            // Reject attempts to impersonate the reserved basilisk identity.
                            if sid == BASILISK_SERVICE_ID || iid == BASILISK_INSTANCE_ID {
                                let _ = tx.send(ServiceBusProtocolMessage {
                                    r#type: protocol_types::ERROR.to_string(),
                                    error_code: Some("RESERVED_IDENTITY".to_string()),
                                    message: Some(format!(
                                        "service_id '{}' and instance_id '{}' are reserved",
                                        BASILISK_SERVICE_ID, BASILISK_INSTANCE_ID
                                    )),
                                    ..Default::default()
                                });
                            } else {
                                let key = format!("{}:{}", sid, iid);
                                connection_key = Some(key.clone());
                                connection_manager.add_connection(
                                    key.clone(),
                                    ServiceBusConnection {
                                        service_id: sid,
                                        instance_id: iid,
                                        tx: tx.clone(),
                                        authenticated: false,
                                        subscriptions: Vec::new(),
                                    },
                                );

                                let _ = tx.send(ServiceBusProtocolMessage {
                                    r#type: protocol_types::ACK.to_string(),
                                    message: Some("Connected to service bus".to_string()),
                                    ..Default::default()
                                });

                                if monitoring_enabled {
                                    connection_manager.subscribe_basilisk(vec![
                                        BASILISK_METRICS_DISTRIBUTION_TOPIC.to_string(),
                                    ]);
                                }
                            }
                        }
                    }
                    protocol_types::AUTHENTICATE => {
                        if let Some(key) = &connection_key {
                            if let Some(token) = msg.token {
                                if let Some((sid, iid)) = connection_manager.get_connection_info(key) {
                                    if registry.validate_instance_token(&sid, &iid, &token) {
                                        connection_manager.authenticate(key);
                                        if connection_health_enabled {
                                            registry
                                                .update_instance_status(&sid, &iid, InstanceStatus::Up)
                                                .await;
                                        }
                                        let _ = tx.send(ServiceBusProtocolMessage {
                                            r#type: protocol_types::ACK.to_string(),
                                            message: Some("Authenticated".to_string()),
                                            ..Default::default()
                                        });
                                    } else {
                                        let _ = tx.send(ServiceBusProtocolMessage {
                                            r#type: protocol_types::ERROR.to_string(),
                                            error_code: Some("AUTH_FAILED".to_string()),
                                            message: Some("Authentication failed".to_string()),
                                            ..Default::default()
                                        });
                                    }
                                }
                            }
                        }
                    }
                    protocol_types::SUBSCRIBE => {
                        if let Some(key) = &connection_key {
                            if connection_manager.is_authenticated(key) {
                                if let Some(topics) = msg.topics {
                                    connection_manager.add_subscriptions(key, topics);
                                    let _ = tx.send(ServiceBusProtocolMessage {
                                        r#type: protocol_types::ACK.to_string(),
                                        message: Some("Subscription updated".to_string()),
                                        ..Default::default()
                                    });
                                }
                            } else {
                                let _ = tx.send(ServiceBusProtocolMessage {
                                    r#type: protocol_types::ERROR.to_string(),
                                    error_code: Some("NOT_AUTHENTICATED".to_string()),
                                    message: Some("Authenticate before subscribing".to_string()),
                                    ..Default::default()
                                });
                            }
                        }
                    }
                    protocol_types::PUBLISH => {
                        if let Some(key) = &connection_key {
                            if connection_manager.is_authenticated(key) {
                                if let Some(mut event) = msg.event {
                                    if event.topic.is_empty() {
                                        let _ = tx.send(ServiceBusProtocolMessage {
                                            r#type: protocol_types::ERROR.to_string(),
                                            error_code: Some("TOPIC_REQUIRED".to_string()),
                                            message: Some("Event topic is required".to_string()),
                                            ..Default::default()
                                        });
                                    } else if let Some((sid, iid)) = connection_manager.get_connection_info(key) {
                                        event.event_id = format!("{}-{}", iid, Uuid::now_v7());
                                        event.emitted_at_utc = Utc::now();
                                        event.service_id = sid;
                                        event.instance_id = iid;
                                        event.correlation_id = if event.correlation_id <= 0 {
                                            connection_manager.next_correlation_id()
                                        } else {
                                            event.correlation_id
                                        };

                                        let delivered_count = connection_manager.publish(event.clone(), Some(key));

                                        let _ = tx.send(ServiceBusProtocolMessage {
                                            r#type: protocol_types::ACK.to_string(),
                                            message: Some("Event published".to_string()),
                                            subscriber_count: Some(delivered_count),
                                            event: Some(ServiceBusEventEnvelope {
                                                payload: HashMap::new(),
                                                ..event
                                            }),
                                            ..Default::default()
                                        });
                                    }
                                }
                            }
                        }
                    }
                    protocol_types::FORWARD => {
                        if let Some(key) = &connection_key {
                            if connection_manager.is_authenticated(key) {
                                let response = handle_forward_request(
                                    Arc::clone(&connection_manager),
                                    key,
                                    msg.forward_request,
                                ).await;
                                let _ = tx.send(response);
                            } else {
                                let _ = tx.send(ServiceBusProtocolMessage {
                                    r#type: protocol_types::ERROR.to_string(),
                                    error_code: Some("NOT_AUTHENTICATED".to_string()),
                                    message: Some("Authenticate before forwarding".to_string()),
                                    ..Default::default()
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
            msg = rx.recv() => {
                if let Some(msg) = msg {
                    let mut json = serde_json::to_string(&msg)?;
                    json.push('\n');
                    writer.write_all(json.as_bytes()).await?;
                    writer.flush().await?;
                } else {
                    break;
                }
            }
        }
    }

    if let Some(key) = connection_key {
        if connection_health_enabled {
            if let Some((sid, iid)) = connection_manager.get_connection_info(&key) {
                registry
                    .update_instance_status(&sid, &iid, InstanceStatus::Down)
                    .await;
            }
        }
        connection_manager.remove_connection(&key);
    }
    Ok(())
}

/// Handles a `forward` protocol request by fan-out and awaited reply routing.
async fn handle_forward_request(
    connection_manager: Arc<ConnectionManager>,
    key: &str,
    forward_request: Option<ServiceBusForwardRequest>,
) -> ServiceBusProtocolMessage {
    let forward_request = match forward_request {
        Some(req) => req,
        None => {
            return ServiceBusProtocolMessage {
                r#type: protocol_types::ERROR.to_string(),
                error_code: Some("INVALID_FORWARD_REQUEST".to_string()),
                message: Some("forwardRequest is required for forward messages".to_string()),
                ..Default::default()
            }
        }
    };

    if forward_request.target_service_id.trim().is_empty() {
        return ServiceBusProtocolMessage {
            r#type: protocol_types::ERROR.to_string(),
            error_code: Some("TARGET_SERVICE_REQUIRED".to_string()),
            message: Some("forwardRequest.targetServiceId is required".to_string()),
            ..Default::default()
        };
    }

    let (sid, iid) = match connection_manager.get_connection_info(key) {
        Some(values) => values,
        None => {
            return ServiceBusProtocolMessage {
                r#type: protocol_types::ERROR.to_string(),
                error_code: Some("CONNECTION_NOT_FOUND".to_string()),
                message: Some("Forwarding connection is not available".to_string()),
                ..Default::default()
            }
        }
    };

    let request_id = format!("basilisk-{}", Uuid::now_v7());
    let reply_to_topic = format!("reply-to-{}", request_id);

    let (tx, mut rx) = mpsc::unbounded_channel::<ServiceBusEventEnvelope>();
    connection_manager.subscribe_internal(reply_to_topic.clone(), tx);

    let mut payload = forward_request.payload.clone();
    payload.insert(
        "reply_to".to_string(),
        serde_json::Value::String(reply_to_topic.clone()),
    );

    let correlation_id = connection_manager.next_correlation_id();
    let event = ServiceBusEventEnvelope {
        event_id: request_id.clone(),
        emitted_at_utc: Utc::now(),
        service_id: sid,
        instance_id: iid,
        topic: format!("service-{}", forward_request.target_service_id),
        message_type: forward_request.message_type.clone(),
        correlation_id,
        causation_id: None,
        payload,
    };

    let delivered = connection_manager.publish(event, Some(key));
    if delivered == 0 {
        connection_manager.unsubscribe_internal(&reply_to_topic);
        return ServiceBusProtocolMessage {
            r#type: protocol_types::ERROR.to_string(),
            error_code: Some("TARGET_NOT_AVAILABLE".to_string()),
            message: Some("No subscribers available for target service".to_string()),
            ..Default::default()
        };
    }

    let timeout_ms = forward_request.timeout_ms.unwrap_or(30_000).min(120_000);
    let response = timeout(Duration::from_millis(timeout_ms), rx.recv()).await;
    connection_manager.unsubscribe_internal(&reply_to_topic);

    match response {
        Ok(Some(event)) => {
            if event.causation_id.as_deref() != Some(request_id.as_str()) {
                warn!(
                    "Forward response causation mismatch: expected={}, got={:?}",
                    request_id, event.causation_id
                );
            }

            ServiceBusProtocolMessage {
                r#type: protocol_types::FORWARD_RESPONSE.to_string(),
                forward_response: Some(ServiceBusForwardResponse {
                    message_type: event.message_type,
                    payload: event.payload,
                }),
                message: Some("Forward request completed".to_string()),
                ..Default::default()
            }
        }
        Ok(None) => ServiceBusProtocolMessage {
            r#type: protocol_types::ERROR.to_string(),
            error_code: Some("FORWARD_CHANNEL_CLOSED".to_string()),
            message: Some("Forward response channel closed".to_string()),
            ..Default::default()
        },
        Err(_) => ServiceBusProtocolMessage {
            r#type: protocol_types::ERROR.to_string(),
            error_code: Some("FORWARD_TIMEOUT".to_string()),
            message: Some("Timeout waiting for forward response".to_string()),
            ..Default::default()
        },
    }
}
