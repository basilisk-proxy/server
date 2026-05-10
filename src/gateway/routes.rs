use crate::gateway::AppState;
use crate::models::{error_codes, ErrorDetail, ErrorResponse, RegistrationRequest};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use tracing::{info, warn};

/// Registers a service instance in the in-memory registry.
pub async fn register(
    State(state): State<Arc<AppState>>,
    Json(request): Json<RegistrationRequest>,
) -> impl IntoResponse {
    let registry = &state.registry;
    info!(
        "Registration attempt for service {} from instance {}",
        request.service_id, request.instance.instance_id
    );

    // 1. Authenticate Registration (FR-1)
    if state.config.security.service_registration_auth == "TOKEN" {
        if request.auth.r#type != "token"
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
    }

    let result = registry.register(request.clone()).await;

    if result.success {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "message": "Service instance registered successfully",
                "serviceId": request.service_id,
                "token": result.token.unwrap()
            })),
        )
            .into_response()
    } else {
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
    if state.registry.deregister(&service_id, &instance_id).await {
        StatusCode::OK.into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

/// Returns all currently registered services.
pub async fn get_all_services(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.registry.get_all_services())
}

/// Returns a single service definition when found.
pub async fn get_service(
    State(state): State<Arc<AppState>>,
    Path(service_id): Path<String>,
) -> impl IntoResponse {
    if let Some(service) = state.registry.get_service(&service_id) {
        Json(service).into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

/// Returns runtime telemetry aggregated by the proxy server.
pub async fn get_runtime_metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.telemetry.snapshot())
}
