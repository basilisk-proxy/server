pub mod header_limiter;
pub mod proxy;
pub mod routes;

use crate::cache::GatewayCache;
use crate::config::GatewayConfig;
use crate::gateway::proxy::{ProxyHandler, UpstreamHttpClient};
use crate::lua_config::LuaRuntime;
use crate::observability::RuntimeTelemetry;
use crate::registry::ServiceRegistry;
use crate::service_bus::connection_manager::ConnectionManager;
use std::net::SocketAddr;
use std::sync::Arc;

/// Shared state passed to HTTP handlers and the reverse proxy fallback.
pub struct AppState {
    /// Effective runtime configuration loaded from Lua.
    pub config: GatewayConfig,
    /// The address this gateway is listening to on, used to detect self-routing loops.
    pub gateway_addr: SocketAddr,
    /// In-memory service registry used for registration and route resolution.
    pub registry: Arc<ServiceRegistry>,
    /// Service bus connection manager used by service bus APIs.
    pub connection_manager: Arc<ConnectionManager>,
    /// Reverse proxy strategy and balancing implementation.
    pub proxy_handler: ProxyHandler,
    /// Lua runtime hosting request middlewares.
    pub lua_runtime: Arc<LuaRuntime>,
    /// Shared gateway cache used by Lua primitives and proxy internals.
    pub cache: Arc<GatewayCache>,
    /// Shared upstream HTTP client used by reverse-proxy forwarding.
    pub upstream_client: UpstreamHttpClient,
    /// Aggregated runtime telemetry (latencies + monitored service distributions).
    pub telemetry: Arc<RuntimeTelemetry>,
}
