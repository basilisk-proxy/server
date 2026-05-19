use crate::config::GatewayConfig;
use crate::models::InstanceStatus;
use crate::registry::ServiceRegistry;
use crate::service_bus::connection_manager::{ConnectionManager, ServiceBusConnection};
use crate::service_bus::contracts::{
    BASILISK_INSTANCE_ID, BASILISK_SERVICE_ID, ServiceBusEventEnvelope, ServiceBusForwardRequest,
    ServiceBusForwardResponse, ServiceBusProtocolMessage, protocol_types,
};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tracing::{debug, error, info, warn};
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
        let (socket, peer_addr) = listener.accept().await?;
        debug!(peer = %peer_addr, "service bus client accepted");
        let connection_manager = Arc::clone(&connection_manager);
        let registry = Arc::clone(&registry);
        let max_message_chars = config.service_bus.max_message_chars;
        let connection_health_enabled = config.service_bus.connection_health_enabled;
        let monitoring_enabled = config.service_bus.monitoring_enabled;

        // Run this separately.
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
    let peer = socket.peer_addr().ok();
    debug!(peer = ?peer, "service bus client session started");
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let (tx, mut rx) = mpsc::unbounded_channel::<ServiceBusProtocolMessage>();
    let mut connection_key: Option<String> = None;
    let mut connected_identity: Option<(String, String)> = None;

    let mut line = String::new();

    loop {
        tokio::select! {
            result = reader.read_line(&mut line) => {
                let bytes_read = match result {
                    Ok(b) => b,
                    Err(e) => {
                        error!(peer = ?peer, error = %e, "service bus inbound read failed");
                        break;
                    },
                };
                if bytes_read == 0 {
                    debug!(peer = ?peer, "service bus inbound connection closed");
                    break;
                }

                if line.len() > max_message_chars {
                    warn!(peer = ?peer, length = line.len(), max_message_chars, "service bus inbound message exceeded maximum size");
                    let msg = ServiceBusProtocolMessage {
                        r#type: protocol_types::ERROR.to_string(),
                        error_code: Some("MESSAGE_TOO_LARGE".to_string()),
                        message: Some(format!("Message exceeds max allowed size of {} characters", max_message_chars)),
                        ..Default::default()
                    };
                    if let Ok(mut json) = serde_json::to_string(&msg) {
                        json.push('\n');
                        let _ = writer.write_all(json.as_bytes()).await;
                        let _ = writer.flush().await;
                    }
                    break;
                }

                let msg: ServiceBusProtocolMessage = match serde_json::from_str(&line) {
                    Ok(m) => m,
                    Err(_) => {
                        warn!(peer = ?peer, "service bus inbound payload is invalid JSON");
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
                        debug!(peer = ?peer, "service bus connect message received");
                        if connection_key.is_some() {
                            let _ = tx.send(ServiceBusProtocolMessage {
                                r#type: protocol_types::ERROR.to_string(),
                                error_code: Some("ALREADY_CONNECTED".to_string()),
                                message: Some("Connection is already established".to_string()),
                                ..Default::default()
                            });
                        } else if let (Some(sid), Some(iid), Some(token)) =
                            (msg.service_id.clone(), msg.instance_id.clone(), msg.token.clone())
                        {
                            // Reject attempts to impersonate the reserved basilisk identity.
                            if sid == BASILISK_SERVICE_ID || iid == BASILISK_INSTANCE_ID {
                                warn!(peer = ?peer, service_id = %sid, instance_id = %iid, "service bus reserved identity connect attempt rejected");
                                let _ = tx.send(ServiceBusProtocolMessage {
                                    r#type: protocol_types::ERROR.to_string(),
                                    error_code: Some("RESERVED_IDENTITY".to_string()),
                                    message: Some(format!(
                                        "service_id '{}' and instance_id '{}' are reserved",
                                        BASILISK_SERVICE_ID, BASILISK_INSTANCE_ID
                                    )),
                                    ..Default::default()
                                });
                            } else if !registry.validate_instance_token(&sid, &iid, &token) {
                                warn!(peer = ?peer, service_id = %sid, instance_id = %iid, "service bus connect authentication failed");
                                let _ = tx.send(ServiceBusProtocolMessage {
                                    r#type: protocol_types::ERROR.to_string(),
                                    error_code: Some("AUTH_FAILED".to_string()),
                                    message: Some("Authentication failed".to_string()),
                                    ..Default::default()
                                });
                            } else {
                                let key = format!("{}:{}", sid, iid);
                                connection_key = Some(key.clone());
                                connected_identity = Some((sid.clone(), iid.clone()));
                                info!(
                                    peer = ?peer,
                                    connection_key = %key,
                                    service_id = %sid,
                                    instance_id = %iid,
                                    connection_health_enabled,
                                    monitoring_enabled,
                                    "service bus client connected"
                                );
                                connection_manager.add_connection(
                                    key.clone(),
                                    ServiceBusConnection {
                                        service_id: sid.clone(),
                                        instance_id: iid.clone(),
                                        tx: tx.clone(),
                                        authenticated: true,
                                        subscriptions: Vec::new(),
                                    },
                                );

                                if connection_health_enabled {
                                    registry
                                        .update_instance_status(&sid, &iid, InstanceStatus::Up)
                                        .await;

                                    if monitoring_enabled {
                                        debug!(
                                            peer = ?peer,
                                            service_id = %sid,
                                            instance_id = %iid,
                                            "service bus connect accepted; monitoring is authoritative so next status transition is deferred to metrics heartbeat evaluation"
                                        );
                                    }
                                }

                                let _ = tx.send(ServiceBusProtocolMessage {
                                    r#type: protocol_types::ACK.to_string(),
                                    message: Some("Connected and authenticated to service bus".to_string()),
                                    ..Default::default()
                                });

                            }
                        } else {
                            let _ = tx.send(ServiceBusProtocolMessage {
                                r#type: protocol_types::ERROR.to_string(),
                                error_code: Some("AUTH_REQUIRED".to_string()),
                                message: Some("Connect requires serviceId, instanceId, and token".to_string()),
                                ..Default::default()
                            });
                        }
                    }
                    protocol_types::SUBSCRIBE => {
                        if let Some(key) = &connection_key {
                            if connection_manager.is_authenticated(key) {
                                if let Some(topics) = msg.topics {
                                    debug!(peer = ?peer, connection_key = %key, topics = ?topics, "service bus subscribe request accepted");
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
                        if let Some(key) = &connection_key
                            && connection_manager.is_authenticated(key)
                                && let Some(mut event) = msg.event {
                                    if event.topic.is_empty() {
                                        warn!(peer = ?peer, connection_key = %key, "service bus publish rejected: topic required");
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
                                        debug!(
                                            peer = ?peer,
                                            connection_key = %key,
                                            topic = %event.topic,
                                            event_id = %event.event_id,
                                            source_service_id = %event.service_id,
                                            source_instance_id = %event.instance_id,
                                            correlation_id = event.correlation_id,
                                            delivered_count,
                                            "service bus publish handled"
                                        );

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
                    protocol_types::FORWARD => {
                        if let Some(key) = &connection_key {
                            if connection_manager.is_authenticated(key) {
                                if let Some(request) = &msg.forward_request {
                                    debug!(
                                        peer = ?peer,
                                        connection_key = %key,
                                        target_service_id = %request.target_service_id,
                                        message_type = %request.message_type,
                                        timeout_ms = request.timeout_ms.unwrap_or(30_000).min(120_000),
                                        "service bus forward request accepted"
                                    );
                                }
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
                    debug!(peer = ?peer, message_type = %msg.r#type, "service bus outbound message flushed");
                    let mut json = match serde_json::to_string(&msg) {
                        Ok(json) => json,
                        Err(err) => {
                            error!(peer = ?peer, error = %err, "failed to serialize outbound service bus message");
                            break;
                        }
                    };
                    json.push('\n');
                    if let Err(err) = writer.write_all(json.as_bytes()).await {
                        warn!(peer = ?peer, error = %err, "service bus outbound write failed; terminating session");
                        break;
                    }
                    if let Err(err) = writer.flush().await {
                        warn!(peer = ?peer, error = %err, "service bus outbound flush failed; terminating session");
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    if let Some(key) = connection_key {
        if connection_health_enabled {
            let identity = connected_identity
                .clone()
                .or_else(|| connection_manager.get_connection_info(&key));
            if let Some((sid, iid)) = identity {
                if monitoring_enabled {
                    warn!(
                        peer = ?peer,
                        connection_key = %key,
                        service_id = %sid,
                        instance_id = %iid,
                        "service bus client disconnected; monitoring is authoritative so instance will transition down only after heartbeat timeout without metrics"
                    );
                } else {
                    info!(peer = ?peer, connection_key = %key, service_id = %sid, instance_id = %iid, "service bus client disconnected; marking instance down");
                    registry
                        .update_instance_status(&sid, &iid, InstanceStatus::Down)
                        .await;
                }
            }
        }
        connection_manager.remove_connection(&key);
    }
    debug!(peer = ?peer, "service bus client session ended");
    Ok(())
}

/// Handles a `forward` protocol request by fan-out and awaited reply routing.
async fn handle_forward_request(
    connection_manager: Arc<ConnectionManager>,
    key: &str,
    forward_request: Option<ServiceBusForwardRequest>,
) -> ServiceBusProtocolMessage {
    debug!(connection_key = %key, "service bus handling forward request");
    let forward_request = match forward_request {
        Some(req) => req,
        None => {
            return ServiceBusProtocolMessage {
                r#type: protocol_types::ERROR.to_string(),
                error_code: Some("INVALID_FORWARD_REQUEST".to_string()),
                message: Some("forwardRequest is required for forward messages".to_string()),
                ..Default::default()
            };
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
            };
        }
    };

    let request_id = format!("basilisk-{}", Uuid::now_v7());
    let reply_to_topic = format!("reply-to-{}", request_id);
    debug!(
        connection_key = %key,
        request_id = %request_id,
        target_service_id = %forward_request.target_service_id,
        message_type = %forward_request.message_type,
        "service bus forward relay started"
    );

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
        warn!(connection_key = %key, request_id = %request_id, target_service_id = %forward_request.target_service_id, "service bus forward relay failed: no target subscribers");
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

            debug!(
                connection_key = %key,
                request_id = %request_id,
                target_service_id = %forward_request.target_service_id,
                response_message_type = %event.message_type,
                response_correlation_id = event.correlation_id,
                "service bus forward relay completed"
            );
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
        Ok(None) => {
            warn!(connection_key = %key, request_id = %request_id, "service bus forward relay failed: response channel closed");
            ServiceBusProtocolMessage {
                r#type: protocol_types::ERROR.to_string(),
                error_code: Some("FORWARD_CHANNEL_CLOSED".to_string()),
                message: Some("Forward response channel closed".to_string()),
                ..Default::default()
            }
        }
        Err(_) => {
            warn!(connection_key = %key, request_id = %request_id, timeout_ms, "service bus forward relay timed out");
            ServiceBusProtocolMessage {
                r#type: protocol_types::ERROR.to_string(),
                error_code: Some("FORWARD_TIMEOUT".to_string()),
                message: Some("Timeout waiting for forward response".to_string()),
                ..Default::default()
            }
        }
    }
}
