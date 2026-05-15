use axum::extract::{ConnectInfo, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use basilisk::cache::GatewayCache;
use basilisk::config::GatewayConfig;
use basilisk::gateway::{proxy::ProxyHandler, routes, AppState};
use basilisk::lua_config::load_config_and_runtime;
use basilisk::lua_config::LuaRuntime;
use basilisk::models::{AuthInfo, InstanceInfo, RegistrationRequest};
use basilisk::observability::RuntimeTelemetry;
use basilisk::registry::ServiceRegistry;
use basilisk::service_bus::connection_manager::ConnectionManager;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

fn test_state() -> Arc<AppState> {
    Arc::new(AppState {
        config: GatewayConfig::default(),
        gateway_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        registry: Arc::new(ServiceRegistry::new()),
        connection_manager: Arc::new(ConnectionManager::new()),
        proxy_handler: ProxyHandler::new(),
        lua_runtime: LuaRuntime::allow_all(),
        cache: Arc::new(GatewayCache::new("memory").expect("cache init")),
        telemetry: Arc::new(RuntimeTelemetry::new()),
    })
}

fn registration_request(service_id: &str, instance_id: &str) -> RegistrationRequest {
    RegistrationRequest {
        service_id: service_id.to_string(),
        fingerprint: "fp-1".to_string(),
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

fn test_dir(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "basilisk-public-routes-{}-{}",
        name,
        Uuid::new_v4()
    ));
    fs::create_dir_all(&path).expect("failed to create temp test dir");
    path
}

#[tokio::test]
async fn registry_routes_register_query_and_deregister_instances() {
    let state = test_state();

    let socket_addr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        8080,
    );

    let register_resp = routes::register(
        State(Arc::clone(&state)),
        ConnectInfo(socket_addr),
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
    let socket_addr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        8080,
    );

    request.auth.token = "wrong-token".to_string();

    let response = routes::register(State(state), ConnectInfo(socket_addr), Json(request))
        .await
        .into_response();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn runtime_metrics_route_returns_snapshot_payload() {
    let state = test_state();
    state
        .telemetry
        .record_proxy_latency("proxy.middleware", std::time::Duration::from_millis(1));

    let response = routes::get_runtime_metrics(State(state))
        .await
        .into_response();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("failed to read metrics body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("body should be valid json");
    assert!(json.get("proxy_latency").is_some());
    assert!(json.get("service_distributions").is_some());
}

#[tokio::test]
async fn register_route_respects_registration_allowlist() {
    let dir = test_dir("registration_allowlist");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.security.registration_allowlist({\n\
         net_rules.is_ip('127.0.0.1'),\n\
         net_rules.is_from_subnet('10.20.0.0/16')\n\
         })\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());
    let (config, lua_runtime, cache) = load_config_and_runtime(
        &script.to_string_lossy(),
        Arc::clone(&registry),
        Arc::clone(&connection_manager),
    )
    .expect("failed to load lua runtime");

    let state = Arc::new(AppState {
        config,
        gateway_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        registry,
        connection_manager,
        proxy_handler: ProxyHandler::new(),
        lua_runtime,
        cache,
        telemetry: Arc::new(RuntimeTelemetry::new()),
    });

    let forbidden_response = routes::register(
        State(Arc::clone(&state)),
        ConnectInfo(SocketAddr::from(([192, 168, 1, 10], 9000))),
        Json(registration_request("inventory", "inv-1")),
    )
    .await
    .into_response();
    assert_eq!(forbidden_response.status(), StatusCode::FORBIDDEN);

    let allowed_response = routes::register(
        State(Arc::clone(&state)),
        ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9001))),
        Json(registration_request("inventory", "inv-2")),
    )
    .await
    .into_response();
    assert_eq!(allowed_response.status(), StatusCode::OK);

    let _ = fs::remove_dir_all(dir);
}
