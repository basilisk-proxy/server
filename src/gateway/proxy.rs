use crate::gateway::AppState;
use crate::lua_config::AfterMiddlewareContext;
use crate::lua_config::MiddlewareResponse;
use crate::lua_config::RequestConnectionInfo;
use crate::models::{InstanceStatus, ServiceDefinition, ServiceInstance};
use axum::extract::ConnectInfo;
use axum::{
    body::Body,
    extract::{Request, State},
    http::Response,
    http::StatusCode,
    response::IntoResponse,
};
use dashmap::DashMap;
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

const REGISTRY_VERSION_CACHE_KEY: &str = "registry:version";
const ROUTE_CACHE_NAMESPACE: &str = "route-resolution";
const ROUTE_CACHE_MISS_SENTINEL: &str = "__basilisk:miss__";
const PROXY_HOP_COUNT_HEADER: &str = "x-basilisk-proxy-hop";
const MAX_PROXY_HOPS: u8 = 3;

pub struct ProxyHandler {
    counters: DashMap<String, AtomicUsize>,
}

impl Default for ProxyHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl ProxyHandler {
    /// Creates a new proxy handler instance with per-service load-balancing counters.
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }

    /// Reverse-proxy fallback handler for all non-registry HTTP routes.
    pub async fn handle_proxy(
        State(state): State<Arc<AppState>>,
        ConnectInfo(connect_info): ConnectInfo<SocketAddr>,
        req: Request,
    ) -> impl IntoResponse {
        let total_started = Instant::now();
        let path = req.uri().path().to_string();
        let method = req.method().as_str().to_string();
        let req_headers = req.headers().clone();
        let ip_address = connect_info.ip().to_canonical();
        let remote_ip = ip_address.to_string();
        let remote_port = connect_info.port();

        let middleware_started = Instant::now();
        let connection_info = RequestConnectionInfo::from_socket(connect_info);
        let middleware_result = match state.lua_runtime.run_middlewares_with_connection(
            &path,
            req.method().as_str(),
            req.headers(),
            &connection_info,
        ) {
            Ok(result) => result,
            Err(err) => {
                state
                    .telemetry
                    .record_proxy_latency("proxy.middleware", middleware_started.elapsed());
                state
                    .telemetry
                    .record_proxy_latency("proxy.total", total_started.elapsed());
                tracing::error!("Lua middleware execution failed: {}", err);
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };
        let middleware_elapsed = middleware_started.elapsed();
        state
            .telemetry
            .record_proxy_latency("proxy.middleware", middleware_elapsed);

        let (mut response, mut error_message, mut metrics) =
            if let Some(reject) = middleware_result.short_circuit_response {
                (Self::to_response(reject), None, HashMap::new())
            } else {
                Self::proxy_to_upstream(
                    Arc::clone(&state),
                    &path,
                    &remote_ip,
                    remote_port,
                    middleware_result.forward_headers,
                    req,
                )
                .await
            };

        metrics.insert(
            "proxy.middleware_ms".to_string(),
            duration_to_ms(middleware_elapsed),
        );
        metrics.insert(
            "proxy.total_ms_pre_after".to_string(),
            duration_to_ms(total_started.elapsed()),
        );
        metrics.insert(
            "proxy.status_code".to_string(),
            response.status().as_u16() as f64,
        );

        if error_message.is_none()
            && (response.status().is_client_error() || response.status().is_server_error())
        {
            error_message = Some(format!(
                "request completed with status {}",
                response.status().as_u16()
            ));
        }

        let after_context = AfterMiddlewareContext {
            response_status: response.status().as_u16(),
            error_message: error_message.clone(),
            metrics,
        };

        match state.lua_runtime.run_after_middlewares_with_connection(
            &path,
            &method,
            &req_headers,
            &connection_info,
            &after_context,
        ) {
            Ok(Some(after_response)) => response = Self::to_response(after_response),
            Ok(None) => {}
            Err(err) => tracing::error!("Lua after middleware execution failed: {}", err),
        }

        state
            .telemetry
            .record_proxy_latency("proxy.total", total_started.elapsed());
        response
    }

    fn resolve_service(
        state: &AppState,
        path: &str,
    ) -> Result<(String, ServiceDefinition), StatusCode> {
        let service_id = Self::resolve_service_id(state, path).ok_or(StatusCode::NOT_FOUND)?;

        let service = state
            .registry
            .get_service(&service_id)
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

        Ok((service_id, service))
    }

    async fn proxy_to_upstream(
        state: Arc<AppState>,
        path: &str,
        remote_ip: &str,
        remote_port: u16,
        forward_headers: HashMap<String, String>,
        req: Request,
    ) -> (Response<Body>, Option<String>, HashMap<String, f64>) {
        let mut metrics = HashMap::new();
        let resolve_started = Instant::now();
        let (service_id, service) = match Self::resolve_service(&state, path) {
            Ok(res) => res,
            Err(status) => {
                let resolve_elapsed = resolve_started.elapsed();
                state
                    .telemetry
                    .record_proxy_latency("proxy.resolve_service", resolve_elapsed);
                metrics.insert(
                    "proxy.resolve_service_ms".to_string(),
                    duration_to_ms(resolve_elapsed),
                );
                return (
                    status.into_response(),
                    Some(format!("route resolution failed with status {}", status)),
                    metrics,
                );
            }
        };
        let resolve_elapsed = resolve_started.elapsed();
        state
            .telemetry
            .record_proxy_latency("proxy.resolve_service", resolve_elapsed);
        metrics.insert(
            "proxy.resolve_service_ms".to_string(),
            duration_to_ms(resolve_elapsed),
        );

        let pick_started = Instant::now();
        let target_instance =
            match state
                .proxy_handler
                .pick_instance(&state, remote_ip, &service_id, &service)
            {
                Ok(inst) => inst,
                Err((status, msg)) => {
                    let pick_elapsed = pick_started.elapsed();
                    state
                        .telemetry
                        .record_proxy_latency("proxy.pick_healthy_instance", pick_elapsed);
                    metrics.insert(
                        "proxy.pick_healthy_instance_ms".to_string(),
                        duration_to_ms(pick_elapsed),
                    );
                    return (
                        (status, msg).into_response(),
                        Some(msg.to_string()),
                        metrics,
                    );
                }
            };
        let pick_elapsed = pick_started.elapsed();
        state
            .telemetry
            .record_proxy_latency("proxy.pick_healthy_instance", pick_elapsed);
        metrics.insert(
            "proxy.pick_healthy_instance_ms".to_string(),
            duration_to_ms(pick_elapsed),
        );

        let target_uri = Self::prepare_target_uri(&state, &service, path, target_instance);

        // Self-routing guard: reject immediately if the resolved target points back at
        // this gateway instance. This prevents tight routing loops without burning hop
        // budget on connections that will always fail with LOOP_DETECTED anyway.
        if is_self_routing(&target_uri, state.gateway_addr) {
            tracing::warn!(
                target_uri = %target_uri,
                gateway_addr = %state.gateway_addr,
                service_id = %service_id,
                "self-routing loop detected; service instance points back at the gateway"
            );
            return (
                StatusCode::LOOP_DETECTED.into_response(),
                Some(format!(
                    "self-routing loop: service '{}' resolves to the gateway's own address",
                    service_id
                )),
                metrics,
            );
        }

        let (response, error_message, send_ms, body_ms) = Self::forward_request(
            target_uri,
            req,
            remote_ip,
            remote_port,
            forward_headers,
            Arc::clone(&state.telemetry),
        )
        .await;
        metrics.insert("proxy.send_upstream_request_ms".to_string(), send_ms);
        metrics.insert("proxy.wait_upstream_response_body_ms".to_string(), body_ms);

        (response, error_message, metrics)
    }

    fn to_response(middleware_response: MiddlewareResponse) -> Response<Body> {
        let mut builder = Response::builder().status(
            StatusCode::from_u16(middleware_response.status)
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        );
        for (k, v) in middleware_response.headers {
            builder = builder.header(k, v);
        }
        builder
            .body(Body::from(middleware_response.body))
            .unwrap_or_else(|_| Response::new(Body::from("Internal Server Error")))
    }

    fn resolve_service_id(state: &AppState, path: &str) -> Option<String> {
        if !state.config.cache.enabled {
            return state.registry.resolve_service_by_path(path);
        }

        let ttl_seconds = state.config.cache.service_resolution_ttl_seconds;
        if ttl_seconds == 0 {
            return state.registry.resolve_service_by_path(path);
        }

        let route_cache_key = format!(
            "{ROUTE_CACHE_NAMESPACE}:v{}",
            state
                .cache
                .internal_get(REGISTRY_VERSION_CACHE_KEY)
                .ok()
                .flatten()
                .unwrap_or_else(|| "0".to_string())
        );

        if let Ok(Some(cached)) = state.cache.internal_hash_get(&route_cache_key, path) {
            return if cached == ROUTE_CACHE_MISS_SENTINEL {
                None
            } else {
                Some(cached)
            };
        }

        let resolved = state.registry.resolve_service_by_path(path);
        let cached_value = resolved
            .clone()
            .unwrap_or_else(|| ROUTE_CACHE_MISS_SENTINEL.to_string());

        if let Err(err) = state.cache.internal_hash_set_with_ttl(
            &route_cache_key,
            path,
            cached_value,
            ttl_seconds,
        ) {
            tracing::warn!("failed to cache route resolution for {path}: {err}");
        }

        resolved
    }

    fn pick_instance<'a>(
        &self,
        state: &AppState,
        address: &str,
        service_id: &str,
        service: &'a ServiceDefinition,
    ) -> Result<&'a ServiceInstance, (StatusCode, &'static str)> {
        let healthy_instances: Vec<_> = service
            .instances
            .values()
            .filter(|i| i.status == InstanceStatus::Up)
            .collect();

        if healthy_instances.is_empty() {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "No healthy instances available",
            ));
        }

        let strategy = &state.config.routing.default_load_balancing_strategy;
        let target_instance = match strategy.as_str() {
            "WEIGHTED_ROUND_ROBIN" => {
                self.pick_weighted_round_robin(state, service_id, &healthy_instances)
            }
            "WEIGHTED_RANDOM" => self.pick_weighted_random(&healthy_instances),
            "IP_HASH" => self.pick_ip_hash(address, &healthy_instances),
            _ => self.pick_round_robin(state, service_id, &healthy_instances),
        };
        Ok(target_instance)
    }

    fn prepare_target_uri(
        state: &AppState,
        service: &ServiceDefinition,
        path: &str,
        instance: &ServiceInstance,
    ) -> String {
        let mut final_path = path;
        if state.config.routing.strip_prefix {
            if let Some(prefix) = service.path_prefixes.iter().find(|p| path.starts_with(*p)) {
                final_path = &path[prefix.len()..];
            }
        }
        let stripped = final_path.strip_prefix('/').unwrap_or(final_path);
        format!("{}/{}", instance.to_uri(), stripped)
    }

    async fn forward_request(
        target_uri: String,
        req: Request,
        remote_ip: &str,
        remote_port: u16,
        forward_headers: HashMap<String, String>,
        telemetry: Arc<crate::observability::RuntimeTelemetry>,
    ) -> (Response<Body>, Option<String>, f64, f64) {
        let client = reqwest::Client::new();
        let (parts, body) = req.into_parts();

        let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
            Ok(b) => b,
            Err(_) => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE.into_response(),
                    Some("request payload too large".to_string()),
                    0.0,
                    0.0,
                );
            }
        };

        let mut proxy_req = client
            .request(parts.method.clone(), &target_uri)
            .body(body_bytes);

        let incoming_hop_count =
            header_to_u8(parts.headers.get(PROXY_HOP_COUNT_HEADER)).unwrap_or(0);
        if incoming_hop_count >= MAX_PROXY_HOPS {
            tracing::warn!(
                target_uri = %target_uri,
                remote_ip = %remote_ip,
                remote_port,
                incoming_hop_count,
                max_proxy_hops = MAX_PROXY_HOPS,
                "Proxy loop detected; rejecting request"
            );
            return (
                StatusCode::LOOP_DETECTED.into_response(),
                Some(format!(
                    "proxy loop detected after {} hops",
                    incoming_hop_count
                )),
                0.0,
                0.0,
            );
        }

        let forwarded_for =
            build_forwarded_for_header(parts.headers.get("x-forwarded-for"), remote_ip);
        let next_hop_count = incoming_hop_count.saturating_add(1);

        proxy_req = proxy_req
            .header("X-Forwarded-For", forwarded_for)
            .header("X-Forwarded-Port", remote_port.to_string())
            .header(PROXY_HOP_COUNT_HEADER, next_hop_count.to_string());

        for (name, value) in parts.headers.iter() {
            if name != "host"
                && !is_hop_by_hop_header(name.as_str())
                && name != "x-forwarded-for"
                && name != "x-forwarded-port"
                && name != PROXY_HOP_COUNT_HEADER
            {
                proxy_req = proxy_req.header(name, value);
            }
        }

        // Apply forward headers from middleware
        for (name, value) in forward_headers.iter() {
            proxy_req = proxy_req.header(name, value);
        }

        let send_started = Instant::now();
        match proxy_req.send().await {
            Ok(resp) => {
                let send_elapsed = send_started.elapsed();
                telemetry.record_proxy_latency("proxy.send_upstream_request", send_elapsed);
                let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
                let mut builder = axum::response::Response::builder().status(status);
                for (name, value) in resp.headers().iter() {
                    builder = builder.header(name, value);
                }
                let read_body_started = Instant::now();
                let resp_body = Body::from(resp.bytes().await.unwrap_or_default());
                let body_elapsed = read_body_started.elapsed();
                telemetry.record_proxy_latency("proxy.wait_upstream_response_body", body_elapsed);
                let err = if status.is_client_error() || status.is_server_error() {
                    Some(format!(
                        "upstream responded with status {}",
                        status.as_u16()
                    ))
                } else {
                    None
                };
                (
                    builder
                        .body(resp_body)
                        .unwrap_or_else(|_| Response::new(Body::from("Bad Gateway"))),
                    err,
                    duration_to_ms(send_elapsed),
                    duration_to_ms(body_elapsed),
                )
            }
            Err(e) => {
                let send_elapsed = send_started.elapsed();
                telemetry.record_proxy_latency("proxy.send_upstream_request", send_elapsed);
                tracing::error!("Proxy error: {}", e);
                (
                    StatusCode::BAD_GATEWAY.into_response(),
                    Some(e.to_string()),
                    duration_to_ms(send_elapsed),
                    0.0,
                )
            }
        }
    }

    fn pick_round_robin<'a>(
        &self,
        state: &AppState,
        service_id: &str,
        instances: &[&'a ServiceInstance],
    ) -> &'a ServiceInstance {
        if let Ok(counter) = state
            .cache
            .internal_incr(&format!("balancer:rr:{service_id}"), 1)
        {
            if counter > 0 {
                let index = (counter.saturating_sub(1) as usize) % instances.len();
                return instances[index];
            }
        }

        let counter = self
            .counters
            .entry(service_id.to_string())
            .or_insert_with(|| AtomicUsize::new(0));
        let index = counter.fetch_add(1, Ordering::SeqCst) % instances.len();
        instances[index]
    }

    fn pick_weighted_round_robin<'a>(
        &self,
        state: &AppState,
        service_id: &str,
        instances: &[&'a ServiceInstance],
    ) -> &'a ServiceInstance {
        let total_weight: i32 = instances.iter().map(|i| i.weight).sum();
        if total_weight <= 0 {
            return instances[0];
        }

        let cache_counter = state
            .cache
            .internal_incr(&format!("balancer:wrr:{service_id}"), 1)
            .ok();
        let current_val = if let Some(counter) = cache_counter.filter(|counter| *counter > 0) {
            counter.saturating_sub(1) as usize % total_weight as usize
        } else {
            let counter = self
                .counters
                .entry(service_id.to_string())
                .or_insert_with(|| AtomicUsize::new(0));
            counter.fetch_add(1, Ordering::SeqCst) % total_weight as usize
        };

        let mut running_weight = 0;
        for instance in instances {
            running_weight += instance.weight;
            if current_val < running_weight as usize {
                return instance;
            }
        }
        instances[0]
    }

    fn pick_weighted_random<'a>(&self, instances: &[&'a ServiceInstance]) -> &'a ServiceInstance {
        let total_weight: i32 = instances.iter().map(|i| i.weight).sum();
        if total_weight <= 0 {
            return instances[0];
        }

        let pick = (rand::random::<u32>() % total_weight as u32) as i32;
        let mut running_weight = 0;
        for instance in instances {
            running_weight += instance.weight;
            if pick < running_weight {
                return instance;
            }
        }
        instances[0]
    }

    fn pick_ip_hash<'a>(&self, ip: &str, instances: &[&'a ServiceInstance]) -> &'a ServiceInstance {
        let mut hasher = DefaultHasher::new();
        ip.hash(&mut hasher);
        let index = (hasher.finish() as usize) % instances.len();
        instances[index]
    }
}

fn duration_to_ms(duration: std::time::Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

/// Returns `true` if the resolved upstream `target_uri` points back at the gateway's
/// own listening address. Handles hostname resolution so that `localhost`, `127.0.0.1`,
/// `::1`, and `0.0.0.0` are all treated as equivalent to the loopback / any interface.
fn is_self_routing(target_uri: &str, gateway_addr: SocketAddr) -> bool {
    // Parse scheme://host[:port]/path from the target URI.
    let without_scheme = if let Some(rest) = target_uri
        .strip_prefix("http://")
        .or_else(|| target_uri.strip_prefix("https://"))
    {
        rest
    } else {
        return false;
    };

    // Extract host:port (authority portion before the first '/').
    let authority = without_scheme.split('/').next().unwrap_or(without_scheme);

    // Parse port: default 80 for http, 443 for https.
    let default_port: u16 = if target_uri.starts_with("https://") {
        443
    } else {
        80
    };

    let (target_host, target_port) = if let Some(colon) = authority.rfind(':') {
        let host = &authority[..colon];
        let port = authority[colon + 1..]
            .parse::<u16>()
            .unwrap_or(default_port);
        (host, port)
    } else {
        (authority, default_port)
    };

    // Port must match first — cheap check.
    if target_port != gateway_addr.port() {
        return false;
    }

    // Resolve target hostname to IP(s) and check if any match the gateway IP.
    // Special-case common loopback aliases to avoid DNS for the critical path.
    let gateway_ip = gateway_addr.ip().to_canonical();

    let target_ips: Vec<IpAddr> = match target_host {
        "localhost" => vec![
            IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
        ],
        _ => {
            // Use DNS resolution for other hostnames.
            match format!("{target_host}:0").to_socket_addrs() {
                Ok(addrs) => addrs.map(|a| a.ip().to_canonical()).collect(),
                Err(_) => return false,
            }
        }
    };

    let gateway_is_any = gateway_ip == IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        || gateway_ip == IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED);

    for ip in &target_ips {
        let ip = ip.to_canonical();
        if ip == gateway_ip {
            return true;
        }
        // If gateway binds to 0.0.0.0 / ::, any loopback or local address counts.
        if gateway_is_any
            && (ip.is_loopback()
                || ip == IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                || ip == IpAddr::V6(std::net::Ipv6Addr::LOCALHOST))
        {
            return true;
        }
    }

    false
}

fn is_hop_by_hop_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
    )
}

fn build_forwarded_for_header(
    existing: Option<&axum::http::HeaderValue>,
    remote_ip: &str,
) -> String {
    match existing.and_then(|v| v.to_str().ok()).map(str::trim) {
        Some(existing) if !existing.is_empty() => format!("{existing}, {remote_ip}"),
        _ => remote_ip.to_string(),
    }
}

fn header_to_u8(value: Option<&axum::http::HeaderValue>) -> Option<u8> {
    value
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.trim().parse::<u8>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GatewayConfig;
    use crate::gateway::AppState;
    use crate::lua_config::LuaRuntime;
    use crate::observability::RuntimeTelemetry;
    use crate::registry::ServiceRegistry;
    use crate::service_bus::connection_manager::ConnectionManager;
    use axum::body::Body;
    use axum::extract::State;

    use axum::http::HeaderValue;
    use axum::http::{Request, StatusCode};
    use axum::response::IntoResponse;
    use std::sync::Arc;

    fn test_state() -> Arc<AppState> {
        let registry = Arc::new(ServiceRegistry::new());
        let config = GatewayConfig::default();
        let connection_manager = Arc::new(ConnectionManager::new());
        let proxy_handler = ProxyHandler::new();
        Arc::new(AppState {
            config,
            gateway_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            registry,
            connection_manager,
            proxy_handler,
            lua_runtime: LuaRuntime::allow_all(),
            cache: Arc::new(crate::cache::GatewayCache::new("memory").expect("cache init")),
            telemetry: Arc::new(RuntimeTelemetry::new()),
        })
    }

    #[tokio::test]
    async fn test_handle_proxy_not_found() {
        let state = test_state();

        let connect_info = SocketAddr::from(([127, 0, 0, 1], 8080));

        let req = Request::builder()
            .uri("/unknown")
            .body(Body::empty())
            .unwrap();

        let response = ProxyHandler::handle_proxy(State(state), ConnectInfo(connect_info), req)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_handle_proxy_no_healthy_instances() {
        let state = test_state();

        let service_id = "test-service".to_string();
        state
            .registry
            .register(crate::models::RegistrationRequest {
                service_id: service_id.clone(),
                fingerprint: "abc".to_string(),
                path_prefixes: vec!["/api".to_string()],
                instance: crate::models::InstanceInfo {
                    instance_id: "inst-1".to_string(),
                    scheme: "http".to_string(),
                    host: "localhost".to_string(),
                    port: 19999, // distinct from the test gateway port (8080)
                    weight: 1,
                },
                auth: crate::models::AuthInfo {
                    r#type: "none".to_string(),
                    token: "token".to_string(),
                },
            })
            .await;

        state
            .registry
            .update_instance_status(&service_id, "inst-1", InstanceStatus::Down)
            .await;

        let connect_info = SocketAddr::from(([127, 0, 0, 1], 8080));

        let req = Request::builder()
            .uri("/api/test")
            .body(Body::empty())
            .unwrap();

        let response = ProxyHandler::handle_proxy(State(state), ConnectInfo(connect_info), req)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(body, "No healthy instances available");
    }

    #[test]
    fn is_self_routing_detects_localhost_on_same_port() {
        let gateway_addr = SocketAddr::from(([0, 0, 0, 0], 8084));
        assert!(is_self_routing("http://localhost:8084/foo", gateway_addr));
        assert!(is_self_routing("http://127.0.0.1:8084/foo", gateway_addr));
    }

    #[test]
    fn is_self_routing_passes_different_port() {
        let gateway_addr = SocketAddr::from(([0, 0, 0, 0], 8084));
        assert!(!is_self_routing("http://localhost:9000/foo", gateway_addr));
        assert!(!is_self_routing("http://127.0.0.1:9001/foo", gateway_addr));
    }

    #[test]
    fn is_self_routing_passes_external_host_same_port() {
        let gateway_addr = SocketAddr::from(([0, 0, 0, 0], 8084));
        // An external host on the same port should not be flagged
        // (DNS will return a non-loopback IP that won't match the loopback gateway).
        assert!(!is_self_routing("http://orders-svc:8084/foo", gateway_addr));
    }

    #[tokio::test]
    async fn proxy_handler_returns_loop_detected_when_upstream_is_self() {
        // Register a service whose instance address points back at the gateway port.
        let state = test_state(); // gateway_addr = 0.0.0.0:8080
        state
            .registry
            .register(crate::models::RegistrationRequest {
                service_id: "loopy".to_string(),
                fingerprint: "fp".to_string(),
                path_prefixes: vec!["/loop".to_string()],
                instance: crate::models::InstanceInfo {
                    instance_id: "loopy-1".to_string(),
                    scheme: "http".to_string(),
                    host: "localhost".to_string(),
                    port: 8080, // same as gateway_addr port
                    weight: 1,
                },
                auth: crate::models::AuthInfo {
                    r#type: "none".to_string(),
                    token: "tok".to_string(),
                },
            })
            .await;

        // Mark the instance Up so it would normally be eligible.
        state
            .registry
            .update_instance_status("loopy", "loopy-1", crate::models::InstanceStatus::Up)
            .await;

        let connect_info = SocketAddr::from(([127, 0, 0, 1], 54321));
        let req = Request::builder()
            .uri("/loop/test")
            .body(Body::empty())
            .unwrap();

        let response = ProxyHandler::handle_proxy(State(state), ConnectInfo(connect_info), req)
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::LOOP_DETECTED);
    }

    #[test]
    fn forwarded_for_header_appends_remote_ip_when_present() {
        let existing = HeaderValue::from_static("10.0.0.4, 10.0.0.5");
        let actual = build_forwarded_for_header(Some(&existing), "127.0.0.1");
        assert_eq!(actual, "10.0.0.4, 10.0.0.5, 127.0.0.1");
    }

    #[test]
    fn forwarded_for_header_uses_remote_ip_when_absent() {
        let actual = build_forwarded_for_header(None, "127.0.0.1");
        assert_eq!(actual, "127.0.0.1");
    }

    #[test]
    fn hop_count_parser_ignores_invalid_values() {
        let invalid = HeaderValue::from_static("not-a-number");
        assert_eq!(header_to_u8(Some(&invalid)), None);

        let valid = HeaderValue::from_static("7");
        assert_eq!(header_to_u8(Some(&valid)), Some(7));
    }
}
