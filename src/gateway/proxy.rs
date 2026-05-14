use crate::gateway::AppState;
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
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

const REGISTRY_VERSION_CACHE_KEY: &str = "registry:version";
const ROUTE_CACHE_NAMESPACE: &str = "route-resolution";
const ROUTE_CACHE_MISS_SENTINEL: &str = "__basilisk:miss__";

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
        let address = match ip_address {
            IpAddr::V4(raw) => format!("{}:{}", raw, connect_info.port()),
            IpAddr::V6(raw) => format!("[{}]:{}", raw, connect_info.port()),
        };

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
        state
            .telemetry
            .record_proxy_latency("proxy.middleware", middleware_started.elapsed());

        let (mut response, error_message) =
            if let Some(reject) = middleware_result.short_circuit_response {
                (Self::to_response(reject), None)
            } else {
                Self::proxy_to_upstream(
                    Arc::clone(&state),
                    &path,
                    &address,
                    middleware_result.forward_headers,
                    req,
                )
                .await
            };

        match state.lua_runtime.run_after_middlewares_with_connection(
            &path,
            &method,
            &req_headers,
            &connection_info,
            error_message.as_deref(),
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
        address: &str,
        forward_headers: HashMap<String, String>,
        req: Request,
    ) -> (Response<Body>, Option<String>) {
        let resolve_started = Instant::now();
        let (service_id, service) = match Self::resolve_service(&state, path) {
            Ok(res) => res,
            Err(status) => {
                state
                    .telemetry
                    .record_proxy_latency("proxy.resolve_service", resolve_started.elapsed());
                return (
                    status.into_response(),
                    Some(format!("route resolution failed with status {}", status)),
                );
            }
        };
        state
            .telemetry
            .record_proxy_latency("proxy.resolve_service", resolve_started.elapsed());

        let pick_started = Instant::now();
        let target_instance =
            match state
                .proxy_handler
                .pick_instance(&state, address, &service_id, &service)
            {
                Ok(inst) => inst,
                Err((status, msg)) => {
                    state.telemetry.record_proxy_latency(
                        "proxy.pick_healthy_instance",
                        pick_started.elapsed(),
                    );
                    return (status.into_response(), Some(msg.to_string()));
                }
            };
        state
            .telemetry
            .record_proxy_latency("proxy.pick_healthy_instance", pick_started.elapsed());

        let target_uri = Self::prepare_target_uri(&state, &service, path, target_instance);
        Self::forward_request(
            target_uri,
            req,
            address,
            forward_headers,
            Arc::clone(&state.telemetry),
        )
        .await
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
        address: &str,
        forward_headers: HashMap<String, String>,
        telemetry: Arc<crate::observability::RuntimeTelemetry>,
    ) -> (Response<Body>, Option<String>) {
        let client = reqwest::Client::new();
        let (parts, body) = req.into_parts();

        let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
            Ok(b) => b,
            Err(_) => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE.into_response(),
                    Some("request payload too large".to_string()),
                );
            }
        };

        let mut proxy_req = client
            .request(parts.method.clone(), &target_uri)
            .body(body_bytes);

        // Set the IP information of the original forwarding address
        proxy_req = proxy_req.header("X-Forwarded-For", address);

        for (name, value) in parts.headers.iter() {
            if name != "host" {
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
                telemetry
                    .record_proxy_latency("proxy.send_upstream_request", send_started.elapsed());
                let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
                let mut builder = axum::response::Response::builder().status(status);
                for (name, value) in resp.headers().iter() {
                    builder = builder.header(name, value);
                }
                let read_body_started = Instant::now();
                let resp_body = Body::from(resp.bytes().await.unwrap_or_default());
                telemetry.record_proxy_latency(
                    "proxy.wait_upstream_response_body",
                    read_body_started.elapsed(),
                );
                (
                    builder
                        .body(resp_body)
                        .unwrap_or_else(|_| Response::new(Body::from("Bad Gateway"))),
                    None,
                )
            }
            Err(e) => {
                telemetry
                    .record_proxy_latency("proxy.send_upstream_request", send_started.elapsed());
                tracing::error!("Proxy error: {}", e);
                (StatusCode::BAD_GATEWAY.into_response(), Some(e.to_string()))
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
                    port: 8080,
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
}
