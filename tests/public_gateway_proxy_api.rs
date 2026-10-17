use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::{Router, routing::get};
use basilisk::cache::GatewayCache;
use basilisk::config::GatewayConfig;
use basilisk::gateway::{
    AppState,
    proxy::{ProxyHandler, new_upstream_http_client},
};
use basilisk::lua_config::LuaRuntime;
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
        cache: Arc::clone(&cache),
        response_cache: Arc::new(basilisk::cache::ResponseCache::new(
            Arc::clone(&cache),
            1024,
        )),
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
                signature: None,
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
        response_cache: Arc::new(basilisk::cache::ResponseCache::new(
            Arc::clone(&cache),
            1024,
        )),
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
        cache: Arc::clone(&cache),
        response_cache: Arc::new(basilisk::cache::ResponseCache::new(
            Arc::clone(&cache),
            1024,
        )),
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

fn upstream_state_app() -> Router {
    let hit_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cacheable = {
        let hit_counter = Arc::clone(&hit_counter);
        axum::routing::get(move || {
            let count = hit_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            async move {
                (
                    StatusCode::OK,
                    [("x-basilisk-cache", "enabled,300,LRU")],
                    format!("payload-{count}"),
                )
                    .into_response()
            }
        })
    };
    let uncacheable = {
        let hit_counter = Arc::clone(&hit_counter);
        axum::routing::get(move || {
            let count = hit_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            async move {
                (
                    StatusCode::OK,
                    [("x-basilisk-cache", "disabled")],
                    format!("payload-{count}"),
                )
                    .into_response()
            }
        })
    };
    Router::new()
        .route("/items", cacheable)
        .route("/uncached", uncacheable)
}

async fn call_proxy(
    state: &Arc<AppState>,
    uri: &str,
    socket_port: u16,
) -> axum::http::Response<axum::body::Body> {
    let req = Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("failed to build request");
    let socket_addr = SocketAddr::from(([127, 0, 0, 1], socket_port));
    ProxyHandler::handle_proxy(State(Arc::clone(state)), ConnectInfo(socket_addr), req)
        .await
        .into_response()
}

#[tokio::test]
async fn proxy_response_cache_honors_x_basilisk_cache_header() {
    let upstream_router = upstream_state_app();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind upstream listener");
    let upstream_port = listener
        .local_addr()
        .expect("failed to read upstream local addr")
        .port();
    let upstream_handle = tokio::spawn(async move {
        axum::serve(listener, upstream_router)
            .await
            .expect("response cache upstream failed")
    });

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());
    registry
        .register(basilisk::models::RegistrationRequest {
            service_id: "inventory".to_string(),
            fingerprint: "fp-cache".to_string(),
            path_prefixes: vec!["/cacheable".to_string()],
            instance: basilisk::models::InstanceInfo {
                instance_id: "inv-1".to_string(),
                scheme: "http".to_string(),
                host: "127.0.0.1".to_string(),
                port: upstream_port,
                weight: 1,
            },
            auth: basilisk::models::AuthInfo {
                r#type: "none".to_string(),
                token: String::new(),
                signature: None,
            },
        })
        .await;

    let cache = Arc::new(GatewayCache::new("memory").expect("cache init"));
    let state = Arc::new(AppState {
        config: GatewayConfig::default(),
        gateway_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        registry,
        connection_manager,
        proxy_handler: ProxyHandler::new(),
        lua_runtime: LuaRuntime::allow_all(),
        cache: Arc::clone(&cache),
        response_cache: Arc::new(basilisk::cache::ResponseCache::new(
            Arc::clone(&cache),
            1024,
        )),
        upstream_client: new_upstream_http_client(),
        telemetry: Arc::new(RuntimeTelemetry::new()),
    });

    let first = call_proxy(&state, "/cacheable/items", 8080).await;
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(
        first
            .headers()
            .get("x-basilisk-cache-status")
            .and_then(|v| v.to_str().ok()),
        Some("miss")
    );
    let body = axum::body::to_bytes(first.into_body(), 1024 * 1024)
        .await
        .expect("failed to read body");

    // Second identical request must be served from the response cache.
    let second = call_proxy(&state, "/cacheable/items", 8080).await;
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(
        second
            .headers()
            .get("x-basilisk-cache-status")
            .and_then(|v| v.to_str().ok()),
        Some("hit")
    );
    let second_body = axum::body::to_bytes(second.into_body(), 1024 * 1024)
        .await
        .expect("failed to read body");
    assert_eq!(body, second_body, "cached payload must match upstream body");

    // Responses that opted out with 'disabled' are never cached.
    let _ = call_proxy(&state, "/cacheable/uncached", 8080).await;
    let uncached_two = call_proxy(&state, "/cacheable/uncached", 8080).await;
    assert_eq!(
        uncached_two
            .headers()
            .get("x-basilisk-cache-status")
            .cloned(),
        None
    );

    upstream_handle.abort();
}

#[tokio::test]
async fn proxy_dynamic_resolver_failures_expose_diagnostics_in_body() {
    let dir = test_dir("dynamic_resolver_failures");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.registry.bind_path('/dyn-error', function(req, resolve)\n\
         \x20 error('resolver blew up deliberately')\n\
         end)\n\
         basilisk.registry.bind_path('/dyn-ghost', function(req, resolve)\n\
         \x20 resolve('ghost-service')\n\
         end)\n\
         basilisk.registry.bind_path('/dyn-fallthrough', function(req, resolve)\n\
         \x20 return\n\
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
        cache: Arc::clone(&cache),
        response_cache: Arc::new(basilisk::cache::ResponseCache::new(
            Arc::clone(&cache),
            1024,
        )),
        upstream_client: new_upstream_http_client(),
        telemetry: Arc::new(RuntimeTelemetry::new()),
    });

    // Resolver raising a Lua error surfaces as 500 with the error message.
    let resolver_error = call_proxy(&state, "/dyn-error/items", 8080).await;
    assert_eq!(resolver_error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = axum::body::to_bytes(resolver_error.into_body(), 1024 * 1024)
        .await
        .expect("failed to read body");
    let body_text = String::from_utf8_lossy(&body);
    assert!(
        body_text.contains("resolver blew up deliberately"),
        "error body must carry the resolver failure: {body_text}"
    );

    // Resolver naming an unregistered service yields 404 with the service name.
    let ghost = call_proxy(&state, "/dyn-ghost/items", 8080).await;
    assert_eq!(ghost.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(ghost.into_body(), 1024 * 1024)
        .await
        .expect("failed to read body");
    let body_text = String::from_utf8_lossy(&body);
    assert!(
        body_text.contains("ghost-service"),
        "error body must name the unregistered service: {body_text}"
    );

    // Fall-through without registry ownership yields 404 with the cause.
    let fallthrough = call_proxy(&state, "/dyn-fallthrough/items", 8080).await;
    assert_eq!(fallthrough.status(), StatusCode::NOT_FOUND);
    let body = axum::body::to_bytes(fallthrough.into_body(), 1024 * 1024)
        .await
        .expect("failed to read body");
    let body_text = String::from_utf8_lossy(&body);
    assert!(
        body_text.contains("route resolution failed"),
        "error body must explain the missing registry route: {body_text}"
    );

    let _ = fs::remove_dir_all(dir);
}

#[tokio::test]
async fn proxy_dynamic_resolver_routes_by_service_name_and_falls_through() {
    let upstream = Router::new().route("/hello", get(|| async { "via-dynamic" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind upstream listener");
    let upstream_port = listener
        .local_addr()
        .expect("failed to read upstream local addr")
        .port();
    let upstream_handle = tokio::spawn(async move {
        axum::serve(listener, upstream)
            .await
            .expect("dynamic upstream failed")
    });

    let dir = test_dir("dynamic_resolver");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.registry.bind_path('/dyn', function(req, resolve)\n\
         \x20 if string.find(req.path, 'legacy') then return end\n\
         \x20 if string.find(req.path, 'unknown') then return end\n\
         \x20 resolve('real-service')\n\
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

    registry
        .register(basilisk::models::RegistrationRequest {
            service_id: "real-service".to_string(),
            fingerprint: "fp-dyn".to_string(),
            path_prefixes: vec!["/unused-prefix".to_string()],
            instance: basilisk::models::InstanceInfo {
                instance_id: "real-1".to_string(),
                scheme: "http".to_string(),
                host: "127.0.0.1".to_string(),
                port: upstream_port,
                weight: 1,
            },
            auth: basilisk::models::AuthInfo {
                r#type: "none".to_string(),
                token: String::new(),
                signature: None,
            },
        })
        .await;

    // A falling registry-owned fallback service for non-resolver outcomes.
    registry
        .register(basilisk::models::RegistrationRequest {
            service_id: "fallback".to_string(),
            fingerprint: "fp-fallback".to_string(),
            path_prefixes: vec!["/dyn/legacy".to_string()],
            instance: basilisk::models::InstanceInfo {
                instance_id: "fallback-1".to_string(),
                scheme: "http".to_string(),
                host: "127.0.0.1".to_string(),
                port: upstream_port,
                weight: 1,
            },
            auth: basilisk::models::AuthInfo {
                r#type: "none".to_string(),
                token: String::new(),
                signature: None,
            },
        })
        .await;

    let state = Arc::new(AppState {
        config: GatewayConfig::default(),
        gateway_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        registry,
        connection_manager,
        proxy_handler: ProxyHandler::new(),
        lua_runtime,
        cache: Arc::clone(&cache),
        response_cache: Arc::new(basilisk::cache::ResponseCache::new(
            Arc::clone(&cache),
            1024,
        )),
        upstream_client: new_upstream_http_client(),
        telemetry: Arc::new(RuntimeTelemetry::new()),
    });

    // Resolver names the service: request is forwarded to real-service.
    let resolved = call_proxy(&state, "/dyn/hello", 8080).await;
    assert_eq!(resolved.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resolved.into_body(), 1024 * 1024)
        .await
        .expect("failed to read body");
    assert_eq!(body, "via-dynamic");

    // When the resolver declines (no resolve call), the registry fallback
    // resolution still applies for an owned prefix.
    let fallback = call_proxy(&state, "/dyn/legacy/hello", 8080).await;
    assert_eq!(fallback.status(), StatusCode::OK);
    let fallback_body = axum::body::to_bytes(fallback.into_body(), 1024 * 1024)
        .await
        .expect("failed to read body");
    assert_eq!(fallback_body, "via-dynamic");

    upstream_handle.abort();
    let _ = fs::remove_dir_all(dir);
}
