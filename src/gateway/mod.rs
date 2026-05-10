pub mod proxy;
pub mod routes;

use crate::config::GatewayConfig;
use crate::gateway::proxy::ProxyHandler;
use crate::lua_config::LuaRuntime;
use crate::registry::ServiceRegistry;
use crate::service_bus::connection_manager::ConnectionManager;
use std::sync::Arc;

/// Shared state passed to HTTP handlers and the reverse proxy fallback.
pub struct AppState {
    /// Effective runtime configuration loaded from Lua.
    pub config: GatewayConfig,
    /// In-memory service registry used for registration and route resolution.
    pub registry: Arc<ServiceRegistry>,
    /// Service bus connection manager used by service bus APIs.
    pub connection_manager: Arc<ConnectionManager>,
    /// Reverse proxy strategy and balancing implementation.
    pub proxy_handler: ProxyHandler,
    /// Lua runtime hosting request middlewares.
    pub lua_runtime: Arc<LuaRuntime>,
}
