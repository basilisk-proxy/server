use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::{Router, routing::get};
use basilisk::config::GatewayConfig;
use basilisk::gateway::{
    AppState,
    proxy::{ProxyHandler, new_upstream_http_client},
};
use basilisk::lua_config::load_config_and_runtime;
use basilisk::observability::RuntimeTelemetry;
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

fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("failed to reserve port");
    let port = listener
        .local_addr()
        .expect("failed to read local addr")
        .port();
    drop(listener);
    port
}

#[tokio::test]
async fn proxy_handler_applies_lua_middleware_before_route_resolution() {
    let dir = test_dir("middleware");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use(path_rules.has_prefix('/blocked'), function(req, res, next)\n\
         return res:status(418):send('teapot')\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());
    let (_cfg, lua_runtime, cache) = load_config_and_runtime(
        &script.to_string_lossy(),
        Arc::clone(&registry),
        Arc::clone(&connection_manager),
    )
    .expect("failed to load lua runtime");

    let state = Arc::new(AppState {
        config: GatewayConfig::default(),
        gateway_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        registry,
        connection_manager,
        proxy_handler: ProxyHandler::new(),
        lua_runtime,
        cache,
        upstream_client: new_upstream_http_client(),
        telemetry: Arc::new(RuntimeTelemetry::new()),
    });

    let socket_addr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        8080,
    );

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

#[tokio::test]
async fn proxy_internal_cache_keys_are_prefixed_and_do_not_collide_with_user_keys() {
    let dir = test_dir("cache_prefix");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.cache.provider('memory')\n\
         basilisk.cache.key_prefix('proxy-api')\n\
         basilisk.cache.service_resolution_ttl_seconds(120)\n\
         basilisk.cache.set('route-resolution:v0', 'user-owned')\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());
    let (_cfg, lua_runtime, cache) = load_config_and_runtime(
        &script.to_string_lossy(),
        Arc::clone(&registry),
        Arc::clone(&connection_manager),
    )
    .expect("failed to load lua runtime");

    registry
        .register(basilisk::models::RegistrationRequest {
            service_id: "orders".to_string(),
            fingerprint: "fp-1".to_string(),
            path_prefixes: vec!["/api/orders".to_string()],
            instance: basilisk::models::InstanceInfo {
                instance_id: "orders-1".to_string(),
                scheme: "http".to_string(),
                host: "127.0.0.1".to_string(),
                port: 65530,
                weight: 1,
            },
            auth: basilisk::models::AuthInfo {
                r#type: "none".to_string(),
                token: "ignored".to_string(),
            },
        })
        .await;

    let mut config = GatewayConfig::default();
    config.cache.key_prefix = "proxy-api".to_string();
    config.cache.service_resolution_ttl_seconds = 120;
    config.cache.ttl_seconds = 120;

    let state = Arc::new(AppState {
        config,
        gateway_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        registry,
        connection_manager,
        proxy_handler: ProxyHandler::new(),
        lua_runtime,
        cache: Arc::clone(&cache),
        upstream_client: new_upstream_http_client(),
        telemetry: Arc::new(RuntimeTelemetry::new()),
    });

    let socket_addr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
        8080,
    );

    let req = Request::builder()
        .uri("/api/orders/items")
        .body(Body::empty())
        .expect("failed to build request");

    let _ = ProxyHandler::handle_proxy(State(state), ConnectInfo(socket_addr), req)
        .await
        .into_response();

    assert_eq!(
        cache
            .get("route-resolution:v0")
            .expect("cache read should succeed")
            .as_deref(),
        Some("user-owned")
    );

    let _ = fs::remove_dir_all(dir);
}

#[tokio::test]
async fn proxy_handler_routes_static_forward_and_reports_degraded_health() {
    let upstream = Router::new().route("/static/hello", get(|| async { "forwarded" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind upstream listener");
    let good_port = listener
        .local_addr()
        .expect("failed to read upstream local addr")
        .port();
    let bad_port = find_free_port();
    let upstream_handle = tokio::spawn(async move {
        axum::serve(listener, upstream)
            .await
            .expect("static upstream server failed")
    });

    let dir = test_dir("static_forward");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        format!(
            "basilisk.proxy.forward(path_rules.has_prefix('/static'), {{\n  {{ scheme = 'http', host = '127.0.0.1', port = {good_port} }},\n  {{ scheme = 'http', host = '127.0.0.1', port = {bad_port} }},\n}})\n"
        ),
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());
    let (_cfg, lua_runtime, cache) = load_config_and_runtime(
        &script.to_string_lossy(),
        Arc::clone(&registry),
        Arc::clone(&connection_manager),
    )
    .expect("failed to load lua runtime");

    let state = Arc::new(AppState {
        config: GatewayConfig::default(),
        gateway_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        registry,
        connection_manager,
        proxy_handler: ProxyHandler::new(),
        lua_runtime,
        cache,
        upstream_client: new_upstream_http_client(),
        telemetry: Arc::new(RuntimeTelemetry::new()),
    });

    let socket_addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let req = Request::builder()
        .uri("/static/hello")
        .body(Body::empty())
        .expect("failed to build request");

    let resp = ProxyHandler::handle_proxy(State(state.clone()), ConnectInfo(socket_addr), req)
        .await
        .into_response();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 1024)
        .await
        .expect("failed to read response body");
    assert_eq!(body, "forwarded");

    let snapshot = state.telemetry.snapshot();
    let summary = snapshot
        .static_forward_routes
        .into_iter()
        .next()
        .expect("static forward summary should exist");

    assert_eq!(summary.status, basilisk::models::InstanceStatus::Degraded);
    assert_eq!(summary.configured_upstreams, 2);
    assert_eq!(summary.reachable_upstreams, 1);
    let expected_target = format!("http://127.0.0.1:{good_port}");
    assert_eq!(
        summary.selected_target.as_deref(),
        Some(expected_target.as_str())
    );

    upstream_handle.abort();
    let _ = fs::remove_dir_all(dir);
}
