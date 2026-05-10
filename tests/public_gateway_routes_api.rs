use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use basilisk::config::GatewayConfig;
use basilisk::gateway::{proxy::ProxyHandler, routes, AppState};
use basilisk::lua_config::LuaRuntime;
use basilisk::models::{AuthInfo, InstanceInfo, RegistrationRequest};
use basilisk::registry::ServiceRegistry;
use basilisk::service_bus::connection_manager::ConnectionManager;
use std::sync::Arc;

fn test_state() -> Arc<AppState> {
    Arc::new(AppState {
        config: GatewayConfig::default(),
        registry: Arc::new(ServiceRegistry::new()),
        connection_manager: Arc::new(ConnectionManager::new()),
        proxy_handler: ProxyHandler::new(),
        lua_runtime: LuaRuntime::allow_all(),
    })
}

fn registration_request(service_id: &str, instance_id: &str) -> RegistrationRequest {
    RegistrationRequest {
        service_id: service_id.to_string(),
        fingerprint: "fp-1".to_string(),
        health_check: "http://localhost:18080/health".to_string(),
        path_prefixes: vec!["/api/orders".to_string()],
        instance: InstanceInfo {
            instance_id: instance_id.to_string(),
            scheme: "http".to_string(),
            host: "localhost".to_string(),
            port: 18080,
            weight: 1,
        },
        auth: AuthInfo {
            r#type: "token".to_string(),
            token: "secret-token".to_string(),
        },
    }
}

#[tokio::test]
async fn registry_routes_register_query_and_deregister_instances() {
    let state = test_state();

    let register_resp = routes::register(
        State(Arc::clone(&state)),
        Json(registration_request("orders", "orders-1")),
    )
    .await
    .into_response();
    assert_eq!(register_resp.status(), StatusCode::OK);

    let services_resp = routes::get_all_services(State(Arc::clone(&state)))
        .await
        .into_response();
    assert_eq!(services_resp.status(), StatusCode::OK);

    let service_resp = routes::get_service(State(Arc::clone(&state)), Path("orders".to_string()))
        .await
        .into_response();
    assert_eq!(service_resp.status(), StatusCode::OK);

    let heartbeat_resp = routes::heartbeat(
        State(Arc::clone(&state)),
        Path(("orders".to_string(), "orders-1".to_string())),
    )
    .await
    .into_response();
    assert_eq!(heartbeat_resp.status(), StatusCode::OK);

    let deregister_resp = routes::deregister(
        State(Arc::clone(&state)),
        Path(("orders".to_string(), "orders-1".to_string())),
    )
    .await
    .into_response();
    assert_eq!(deregister_resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn register_route_rejects_invalid_token() {
    let state = test_state();
    let mut request = registration_request("payments", "payments-1");
    request.auth.token = "wrong-token".to_string();

    let response = routes::register(State(state), Json(request))
        .await
        .into_response();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
