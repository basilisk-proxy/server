use crate::gateway::AppState;
use crate::lua_config::AfterMiddlewareContext;
use crate::lua_config::MiddlewareResponse;
use crate::lua_config::RequestConnectionInfo;
use crate::models::{InstanceStatus, ServiceDefinition, ServiceInstance};
use axum::extract::ConnectInfo;
use axum::{
    body::Body,
    extract::{Request, State},
    http::HeaderMap,
    http::HeaderName,
    http::HeaderValue,
    http::Request as HttpRequest,
    http::Response,
    http::StatusCode,
    http::Version,
    http::header::CONNECTION,
    http::header::CONTENT_LENGTH,
    response::IntoResponse,
};
use dashmap::DashMap;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use smallvec::SmallVec;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const REGISTRY_VERSION_CACHE_KEY: &str = "registry:version";
const ROUTE_CACHE_NAMESPACE: &str = "route-resolution";
const ROUTE_CACHE_MISS_SENTINEL: &str = "__basilisk:miss__";
const PROXY_HOP_COUNT_HEADER: &str = "x-basilisk-proxy-hop";
const MAX_PROXY_HOPS: u8 = 3;
const MAX_PROXY_REQUEST_BODY_BYTES: u64 = 10 * 1024 * 1024;

pub type UpstreamHttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Body>;
type ForwardOutcome = (Response<Body>, Option<String>, f64, f64);
type ForwardError = (StatusCode, String);

struct ForwardingMetadata {
    forwarded_for: String,
    next_hop_count: u8,
}

/// Builds a shared Hyper client for upstream forwarding.
///
/// Reusing a single client enables TCP/TLS connection pooling and avoids
/// rebuilding the connection state for every proxied request.
pub fn new_upstream_http_client() -> UpstreamHttpClient {
    let https_connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();

    Client::builder(TokioExecutor::new())
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .pool_max_idle_per_host(64)
        .build(https_connector)
}

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
                (
                    Self::convert_middleware_to_response(reject),
                    None,
                    HashMap::new(),
                )
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
            Ok(Some(after_response)) => {
                response = Self::convert_middleware_to_response(after_response)
            }
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
            &state.upstream_client,
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

    fn convert_middleware_to_response(middleware_response: MiddlewareResponse) -> Response<Body> {
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
            .as_deref()
            .unwrap_or(ROUTE_CACHE_MISS_SENTINEL)
            .to_string();

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
        let healthy_instances: SmallVec<[&ServiceInstance; 8]> = service
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

        let strategy = state
            .config
            .routing
            .default_load_balancing_strategy
            .as_str();
        let target_instance = match strategy {
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
        if state.config.routing.strip_prefix
            && let Some(prefix) = service.path_prefixes.iter().find(|p| path.starts_with(*p))
        {
            final_path = &path[prefix.len()..];
        }
        let stripped = final_path.strip_prefix('/').unwrap_or(final_path);
        let base_uri = instance.to_uri();
        let mut target = String::with_capacity(base_uri.len() + 1 + stripped.len());
        target.push_str(&base_uri);
        target.push('/');
        target.push_str(stripped);
        target
    }

    async fn forward_request(
        client: &UpstreamHttpClient,
        target_uri: String,
        req: Request,
        remote_ip: &str,
        remote_port: u16,
        forward_headers: HashMap<String, String>,
        telemetry: Arc<crate::observability::RuntimeTelemetry>,
    ) -> ForwardOutcome {
        let (parts, body) = req.into_parts();

        let metadata = match Self::prepare_forwarding_metadata(
            &parts.headers,
            &target_uri,
            remote_ip,
            remote_port,
        ) {
            Ok(metadata) => metadata,
            Err(err) => return Self::forward_error(err.0, err.1),
        };

        let proxy_req = match Self::build_upstream_request(
            parts,
            body,
            &target_uri,
            remote_port,
            &metadata.forwarded_for,
            metadata.next_hop_count,
            forward_headers,
        ) {
            Ok(req) => req,
            Err(err) => return Self::forward_error(err.0, err.1),
        };

        Self::dispatch_upstream_request(client, proxy_req, telemetry).await
    }

    fn prepare_forwarding_metadata(
        headers: &HeaderMap,
        target_uri: &str,
        remote_ip: &str,
        remote_port: u16,
    ) -> Result<ForwardingMetadata, ForwardError> {
        let incoming_hop_count = header_to_u8(headers.get(PROXY_HOP_COUNT_HEADER)).unwrap_or(0);
        if incoming_hop_count >= MAX_PROXY_HOPS {
            tracing::warn!(
                target_uri = %target_uri,
                remote_ip = %remote_ip,
                remote_port,
                incoming_hop_count,
                max_proxy_hops = MAX_PROXY_HOPS,
                "Proxy loop detected; rejecting request"
            );
            return Err((
                StatusCode::LOOP_DETECTED,
                format!("proxy loop detected after {} hops", incoming_hop_count),
            ));
        }

        if let Some(content_length) = header_to_u64(headers.get(CONTENT_LENGTH))
            && content_length > MAX_PROXY_REQUEST_BODY_BYTES
        {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                "request payload too large".to_string(),
            ));
        }

        let forwarded_for = build_forwarded_for_header(headers.get("x-forwarded-for"), remote_ip);
        let next_hop_count = incoming_hop_count.saturating_add(1);
        Ok(ForwardingMetadata {
            forwarded_for,
            next_hop_count,
        })
    }

    fn build_upstream_request(
        parts: axum::http::request::Parts,
        body: Body,
        target_uri: &str,
        remote_port: u16,
        forwarded_for: &str,
        next_hop_count: u8,
        forward_headers: HashMap<String, String>,
    ) -> Result<HttpRequest<Body>, ForwardError> {
        let mut proxy_req_builder = HttpRequest::builder()
            .method(parts.method)
            .uri(target_uri)
            .version(normalize_upstream_http_version(parts.version));

        let headers = match proxy_req_builder.headers_mut() {
            Some(headers) => headers,
            None => {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    "failed to prepare upstream request headers".to_string(),
                ));
            }
        };

        Self::insert_forwarding_headers(headers, forwarded_for, remote_port, next_hop_count);
        Self::copy_request_headers(headers, &parts.headers);
        Self::apply_middleware_forward_headers(headers, forward_headers);

        proxy_req_builder.body(body).map_err(|err| {
            (
                StatusCode::BAD_GATEWAY,
                format!("failed to build upstream request: {err}"),
            )
        })
    }

    fn insert_forwarding_headers(
        headers: &mut HeaderMap,
        forwarded_for: &str,
        remote_port: u16,
        next_hop_count: u8,
    ) {
        if let Ok(value) = HeaderValue::from_str(forwarded_for) {
            headers.insert(HeaderName::from_static("x-forwarded-for"), value);
        }
        if let Ok(value) = HeaderValue::from_str(&remote_port.to_string()) {
            headers.insert(HeaderName::from_static("x-forwarded-port"), value);
        }
        if let Ok(value) = HeaderValue::from_str(&next_hop_count.to_string()) {
            headers.insert(HeaderName::from_static(PROXY_HOP_COUNT_HEADER), value);
        }
    }

    fn copy_request_headers(target_headers: &mut HeaderMap, source_headers: &HeaderMap) {
        for (name, value) in source_headers {
            if name != "host"
                && !is_hop_by_hop_header(name.as_str())
                && name != "x-forwarded-for"
                && name != "x-forwarded-port"
                && name != PROXY_HOP_COUNT_HEADER
            {
                target_headers.append(name, value.clone());
            }
        }
    }

    fn apply_middleware_forward_headers(
        headers: &mut HeaderMap,
        forward_headers: HashMap<String, String>,
    ) {
        for (name, value) in forward_headers {
            let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
                tracing::warn!(header = %name, "skipping invalid middleware forward header name");
                continue;
            };
            let Ok(header_value) = HeaderValue::from_str(&value) else {
                tracing::warn!(header = %name, "skipping invalid middleware forward header value");
                continue;
            };
            headers.insert(header_name, header_value);
        }
    }

    async fn dispatch_upstream_request(
        client: &UpstreamHttpClient,
        proxy_req: HttpRequest<Body>,
        telemetry: Arc<crate::observability::RuntimeTelemetry>,
    ) -> ForwardOutcome {
        let send_started = Instant::now();
        match client.request(proxy_req).await {
            Ok(resp) => {
                let send_elapsed = send_started.elapsed();
                telemetry.record_proxy_latency("proxy.send_upstream_request", send_elapsed);

                let (parts, body) = resp.into_parts();
                let status = StatusCode::from_u16(parts.status.as_u16()).unwrap_or(StatusCode::OK);
                let mut builder = axum::response::Response::builder().status(status);
                for (name, value) in &parts.headers {
                    if name != CONNECTION {
                        builder = builder.header(name, value);
                    }
                }

                let read_body_started = Instant::now();
                let resp_body = Body::new(body);
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
            Err(err) => {
                let send_elapsed = send_started.elapsed();
                telemetry.record_proxy_latency("proxy.send_upstream_request", send_elapsed);
                tracing::error!("Proxy error: {}", err);
                (
                    StatusCode::BAD_GATEWAY.into_response(),
                    Some(err.to_string()),
                    duration_to_ms(send_elapsed),
                    0.0,
                )
            }
        }
    }

    fn forward_error(status: StatusCode, message: String) -> ForwardOutcome {
        (status.into_response(), Some(message), 0.0, 0.0)
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
            && counter > 0
        {
            let index = (counter.saturating_sub(1) as usize) % instances.len();
            return instances[index];
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
    let Some((target_host, target_port)) = parse_target_host_and_port(target_uri) else {
        return false;
    };

    if target_port != gateway_addr.port() {
        return false;
    }

    let target_ips = match resolve_target_ips(target_host) {
        Some(ips) => ips,
        None => return false,
    };

    let gateway_ip = gateway_addr.ip().to_canonical();
    matches_gateway_address(gateway_ip, &target_ips)
}

fn parse_target_host_and_port(target_uri: &str) -> Option<(&str, u16)> {
    let (without_scheme, default_port) = parse_uri_authority_input(target_uri)?;
    let authority = without_scheme.split('/').next().unwrap_or(without_scheme);
    Some(split_authority_host_and_port(authority, default_port))
}

fn parse_uri_authority_input(target_uri: &str) -> Option<(&str, u16)> {
    if let Some(rest) = target_uri.strip_prefix("http://") {
        return Some((rest, 80));
    }
    if let Some(rest) = target_uri.strip_prefix("https://") {
        return Some((rest, 443));
    }
    None
}

fn split_authority_host_and_port(authority: &str, default_port: u16) -> (&str, u16) {
    if let Some(stripped) = authority.strip_prefix('[')
        && let Some(end_bracket) = stripped.find(']')
    {
        let host = &stripped[..end_bracket];
        let port = stripped[end_bracket + 1..]
            .strip_prefix(':')
            .and_then(|raw| raw.parse::<u16>().ok())
            .unwrap_or(default_port);
        return (host, port);
    }

    if let Some(colon) = authority.rfind(':') {
        let host = &authority[..colon];
        let port = authority[colon + 1..]
            .parse::<u16>()
            .unwrap_or(default_port);
        return (host, port);
    }

    (authority, default_port)
}

fn resolve_target_ips(target_host: &str) -> Option<Vec<IpAddr>> {
    if target_host.eq_ignore_ascii_case("localhost") {
        return Some(vec![
            IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
        ]);
    }

    format!("{target_host}:0")
        .to_socket_addrs()
        .ok()
        .map(|addrs| addrs.map(|a| a.ip().to_canonical()).collect())
}

fn matches_gateway_address(gateway_ip: IpAddr, target_ips: &[IpAddr]) -> bool {
    let gateway_is_any = gateway_ip == IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        || gateway_ip == IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED);

    for ip in target_ips {
        let ip = ip.to_canonical();
        if ip == gateway_ip {
            return true;
        }
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
    name.eq_ignore_ascii_case("connection")
        || name.eq_ignore_ascii_case("keep-alive")
        || name.eq_ignore_ascii_case("proxy-authenticate")
        || name.eq_ignore_ascii_case("proxy-authorization")
        || name.eq_ignore_ascii_case("te")
        || name.eq_ignore_ascii_case("trailer")
        || name.eq_ignore_ascii_case("transfer-encoding")
        || name.eq_ignore_ascii_case("upgrade")
        || name.eq_ignore_ascii_case("content-length")
}

fn build_forwarded_for_header(existing: Option<&HeaderValue>, remote_ip: &str) -> String {
    match existing.and_then(|v| v.to_str().ok()).map(str::trim) {
        Some(existing) if !existing.is_empty() => {
            let mut value = String::with_capacity(existing.len() + 2 + remote_ip.len());
            value.push_str(existing);
            value.push_str(", ");
            value.push_str(remote_ip);
            value
        }
        _ => remote_ip.to_string(),
    }
}

fn header_to_u8(value: Option<&HeaderValue>) -> Option<u8> {
    value
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.trim().parse::<u8>().ok())
}

fn header_to_u64(value: Option<&HeaderValue>) -> Option<u64> {
    value
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.trim().parse::<u64>().ok())
}

fn normalize_upstream_http_version(version: Version) -> Version {
    match version {
        Version::HTTP_09 | Version::HTTP_10 | Version::HTTP_11 | Version::HTTP_2 => version,
        _ => Version::HTTP_11,
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
            upstream_client: new_upstream_http_client(),
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

    #[test]
    fn prepare_target_uri_omits_port_when_instance_uses_default_port() {
        let state = test_state();
        let service = ServiceDefinition {
            service_id: "orders".to_string(),
            fingerprint: "fp".to_string(),
            path_prefixes: vec!["/api".to_string()],
            instances: HashMap::new(),
        };
        let instance = ServiceInstance {
            instance_id: "orders-1".to_string(),
            service_id: "orders".to_string(),
            token: "token".to_string(),
            scheme: "http".to_string(),
            host: "orders-svc".to_string(),
            port: 0,
            weight: 1,
            status: InstanceStatus::Up,
            active_connections: 0,
            last_heartbeat_utc: chrono::Utc::now(),
        };

        let target = ProxyHandler::prepare_target_uri(&state, &service, "/api/ping", &instance);

        assert_eq!(target, "http://orders-svc/ping");
    }w

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
            .update_instance_status("loopy", "loopy-1", InstanceStatus::Up)
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

    #[test]
    fn content_length_parser_ignores_invalid_values() {
        let invalid = HeaderValue::from_static("not-a-number");
        assert_eq!(header_to_u64(Some(&invalid)), None);

        let valid = HeaderValue::from_static("1048576");
        assert_eq!(header_to_u64(Some(&valid)), Some(1_048_576));
    }

    #[tokio::test]
    async fn forward_request_rejects_known_oversized_content_length() {
        let request = Request::builder()
            .method("POST")
            .uri("/upload")
            .header(
                CONTENT_LENGTH,
                (MAX_PROXY_REQUEST_BODY_BYTES + 1).to_string(),
            )
            .body(Body::empty())
            .expect("request build");

        let telemetry = Arc::new(RuntimeTelemetry::new());
        let client = new_upstream_http_client();
        let (response, error, send_ms, body_ms) = ProxyHandler::forward_request(
            &client,
            "http://127.0.0.1:1/upload".to_string(),
            request,
            "127.0.0.1",
            50000,
            HashMap::new(),
            telemetry,
        )
        .await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(error.as_deref(), Some("request payload too large"));
        assert_eq!(send_ms, 0.0);
        assert_eq!(body_ms, 0.0);
    }
}
