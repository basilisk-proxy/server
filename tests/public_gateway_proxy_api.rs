use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use basilisk::config::GatewayConfig;
use basilisk::gateway::{proxy::ProxyHandler, AppState};
use basilisk::lua_config::load_config_and_runtime;
use basilisk::registry::ServiceRegistry;
use basilisk::service_bus::connection_manager::ConnectionManager;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

fn test_dir(name: &str) -> std::path::PathBuf {
    let path =
        std::env::temp_dir().join(format!("basilisk-public-proxy-{}-{}", name, Uuid::new_v4()));
    fs::create_dir_all(&path).expect("failed to create temp test dir");
    path
}

#[tokio::test]
async fn proxy_handler_applies_lua_middleware_before_route_resolution() {
    let dir = test_dir("middleware");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use('/blocked', function(req, res, next)\n\
         return res:status(418):send('teapot')\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());
    let (_cfg, lua_runtime) = load_config_and_runtime(
        &script.to_string_lossy(),
        Arc::clone(&registry),
        Arc::clone(&connection_manager),
    )
    .expect("failed to load lua runtime");

    let state = Arc::new(AppState {
        config: GatewayConfig::default(),
        registry,
        connection_manager,
        proxy_handler: ProxyHandler::new(),
        lua_runtime,
    });

    let socket_addr = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), 8080);

    let req = Request::builder()
        .uri("/blocked/test")
        .body(Body::empty())
        .expect("failed to build request");

    let resp = ProxyHandler::handle_proxy(State(state), ConnectInfo(socket_addr), req)
        .await
        .into_response();

    assert_eq!(resp.status(), StatusCode::IM_A_TEAPOT);
    let body = axum::body::to_bytes(resp.into_body(), 1024)
        .await
        .expect("failed to read response body");
    assert_eq!(body, "teapot");

    let _ = fs::remove_dir_all(dir);
}
