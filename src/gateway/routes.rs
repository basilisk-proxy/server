use crate::gateway::AppState;
use crate::lua_config::RequestConnectionInfo;
use crate::models::{error_codes, ErrorDetail, ErrorResponse, RegistrationRequest};
use axum::extract::ConnectInfo;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{debug, info, warn};

const REGISTRY_VERSION_CACHE_KEY: &str = "registry:version";

/// Registers a service instance in the in-memory registry.
pub async fn register(
    State(state): State<Arc<AppState>>,
    ConnectInfo(socket): ConnectInfo<SocketAddr>,
    Json(request): Json<RegistrationRequest>,
) -> impl IntoResponse {
    let registry = &state.registry;
    debug!(
        "Registration attempt for service {} from instance {}",
        request.service_id,
        socket.ip().to_string()
    );

    let connection_info = RequestConnectionInfo::from_socket(socket);
    match state
        .lua_runtime
        .is_registration_ip_allowed(&connection_info)
    {
        Ok(false) => {
            warn!(
                service_id = %request.service_id,
                remote_ip = %socket.ip(),
                remote_port = socket.port(),
                "registration rejected by registration allowlist"
            );
            return (
                StatusCode::FORBIDDEN,
                Json(ErrorResponse {
                    error: ErrorDetail {
                        code: error_codes::REGISTRATION_IP_NOT_ALLOWED.to_string(),
                        message: "Registration source IP is not allowed".to_string(),
                    },
                }),
            )
                .into_response();
        }
        Ok(true) => {}
        Err(err) => {
            tracing::error!(
                service_id = %request.service_id,
                remote_ip = %socket.ip(),
                remote_port = socket.port(),
                error = %err,
                "registration allowlist evaluation failed"
            );
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    }

    // 1. Authenticate Registration (FR-1)
    if state.config.security.service_registration_auth == "TOKEN" && request.auth.r#type != "token"
        || request.auth.token != state.config.security.registration_token
    {
        warn!("Authentication failed for service {}", request.service_id);
        return (
            StatusCode::UNAUTHORIZED,
            Json(ErrorResponse {
                error: ErrorDetail {
                    code: error_codes::AUTH_FAILED.to_string(),
                    message: "Authentication failed for service registration".to_string(),
                },
            }),
        )
            .into_response();
    }

    let result = registry.register(request.clone()).await;

    if result.success {
        info!(
            service_id = %request.service_id,
            instance_id = ?result.instance_id,
            path_prefixes = ?request.path_prefixes,
            cache_enabled = state.config.cache.enabled,
            "registry registration succeeded"
        );
        if state.config.cache.enabled {
            if let Err(err) = state.cache.internal_incr(REGISTRY_VERSION_CACHE_KEY, 1) {
                warn!("failed to advance registry cache version after register: {err}");
            }
        }
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "message": "Service instance registered successfully",
                "serviceId": request.service_id,
                "instanceId": result.instance_id.unwrap(),
                "token": result.token.unwrap()
            })),
        )
            .into_response()
    } else {
        warn!(
            service_id = %request.service_id,
            error_code = ?result.error_code,
            error_message = ?result.error_message,
            "registry registration failed"
        );
        // ... same as before
        let status = if result.error_code.as_deref() == Some(error_codes::FINGERPRINT_INVALID) {
            StatusCode::FORBIDDEN
        } else {
            StatusCode::CONFLICT
        };

        (
            status,
            Json(ErrorResponse {
                error: ErrorDetail {
                    code: result
                        .error_code
                        .unwrap_or_else(|| "UNKNOWN_ERROR".to_string()),
                    message: result
                        .error_message
                        .unwrap_or_else(|| "Unknown error".to_string()),
                },
            }),
        )
            .into_response()
    }
}

/// Deregisters a service instance.
pub async fn deregister(
    State(state): State<Arc<AppState>>,
    Path((service_id, instance_id)): Path<(String, String)>,
) -> impl IntoResponse {
    debug!(service_id = %service_id, instance_id = %instance_id, "registry deregister request received");
    if state.registry.deregister(&service_id, &instance_id).await {
        info!(service_id = %service_id, instance_id = %instance_id, "registry deregister succeeded");
        if state.config.cache.enabled {
            if let Err(err) = state.cache.internal_incr(REGISTRY_VERSION_CACHE_KEY, 1) {
                warn!("failed to advance registry cache version after deregister: {err}");
            }
        }
        StatusCode::OK.into_response()
    } else {
        warn!(service_id = %service_id, instance_id = %instance_id, "registry deregister target not found");
        StatusCode::NOT_FOUND.into_response()
    }
}

/// Returns all currently registered services.
pub async fn get_all_services(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let services = state.registry.get_all_services();
    debug!(
        services_count = services.len(),
        "registry services snapshot requested"
    );
    Json(services)
}

/// Returns a single service definition when found.
pub async fn get_service(
    State(state): State<Arc<AppState>>,
    Path(service_id): Path<String>,
) -> impl IntoResponse {
    if let Some(service) = state.registry.get_service(&service_id) {
        debug!(service_id = %service_id, instances_count = service.instances.len(), "registry service details requested");
        Json(service).into_response()
    } else {
        warn!(service_id = %service_id, "registry service details requested for unknown service");
        StatusCode::NOT_FOUND.into_response()
    }
}

/// Returns runtime telemetry aggregated by the proxy server.
pub async fn get_runtime_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.telemetry.snapshot())
}
