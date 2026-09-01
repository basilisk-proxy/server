use crate::cache::GatewayCache;
use crate::config::GatewayConfig;
use crate::helper::set_headers;
use crate::models::InstanceStatus;
use crate::registry::ServiceRegistry;
use crate::service_bus::connection_manager::ConnectionManager;
use crate::service_bus::contracts::{
    BASILISK_INSTANCE_ID, BASILISK_SERVICE_ID, ServiceBusEventEnvelope, ServiceBusForwardRequest,
};
use anyhow::{Context, anyhow};
use axum::http::HeaderMap;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use mlua::{Function, Lua, MultiValue, RegistryKey, Result as LuaResult, Table, Value};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use uuid::Uuid;

/// Hosts the embedded Lua VM and registered HTTP middlewares.
pub struct LuaRuntime {
    lua: Mutex<Lua>,
    before_middlewares: Mutex<Vec<MiddlewareMount>>,
    after_middlewares: Mutex<Vec<MiddlewareMount>>,
    static_forward_mounts: Mutex<Vec<StaticForwardMount>>,
    registration_allowlist_rules: Mutex<Vec<RegistryKey>>,
    /// Registered service-bus event handlers: topic → handler key.
    event_handlers: Arc<Mutex<HashMap<String, RegistryKey>>>,
    /// Channel sender used by the background bus-dispatch task to deliver events
    /// to the synchronous Lua dispatch loop.
    event_dispatch_tx: mpsc::UnboundedSender<ServiceBusEventEnvelope>,
}

/// Internal middleware registration entry.
struct MiddlewareMount {
    matcher: MiddlewareMatcher,
    handler_key: RegistryKey,
}

struct StaticForwardMount {
    route_id: String,
    matcher: MiddlewareMatcher,
    upstreams: Vec<StaticForwardUpstream>,
}

#[derive(Clone)]
struct StaticForwardUpstream {
    scheme: String,
    host: String,
    port: u16,
}

#[derive(Debug, Clone)]
pub struct StaticForwardResolution {
    pub route_id: String,
    pub status: InstanceStatus,
    pub configured_upstreams: usize,
    pub reachable_upstreams: usize,
    pub reachable_targets: Vec<String>,
}

struct PrimitiveContext {
    config: Arc<Mutex<GatewayConfig>>,
    cache: Arc<GatewayCache>,
    registry: Arc<ServiceRegistry>,
    connection_manager: Arc<ConnectionManager>,
    before_middleware_mounts: Arc<Mutex<Vec<MiddlewareMount>>>,
    after_middleware_mounts: Arc<Mutex<Vec<MiddlewareMount>>>,
    static_forward_mounts: Arc<Mutex<Vec<StaticForwardMount>>>,
    registration_allowlist_rules: Arc<Mutex<Vec<RegistryKey>>>,
    event_handlers: Arc<Mutex<HashMap<String, RegistryKey>>>,
}

struct MiddlewareExecutionInput<'a> {
    path: &'a str,
    method: &'a str,
    headers: &'a HeaderMap,
    host: &'a str,
    connection_info: &'a RequestConnectionInfo,
    after_context: Option<&'a AfterMiddlewareContext>,
    shared_context: &'a Table,
}

enum MiddlewareMatcher {
    Any,
    Predicate(RegistryKey),
}

/// Canonical connection details captured from the accepted TCP socket.
#[derive(Clone, Debug)]
pub struct RequestConnectionInfo {
    pub remote_addr: SocketAddr,
}

impl RequestConnectionInfo {
    pub fn from_socket(remote_addr: SocketAddr) -> Self {
        Self { remote_addr }
    }

    fn remote_addr_string(&self) -> String {
        self.remote_addr.to_string()
    }

    fn remote_ip_string(&self) -> String {
        self.remote_addr.ip().to_string()
    }

    fn remote_port(&self) -> u16 {
        self.remote_addr.port()
    }
}

/// HTTP response produced by a Lua middleware that short-circuits the pipeline.
pub struct MiddlewareResponse {
    pub status: u16,
    pub body: String,
    pub headers: HashMap<String, String>,
}

/// Result of middleware execution containing optional short-circuit response and forward headers.
pub struct MiddlewareExecutionResult {
    pub short_circuit_response: Option<MiddlewareResponse>,
    pub forward_headers: HashMap<String, String>,
}

/// Context available to `use_after` middleware for inspecting proxy outcomes.
#[derive(Clone, Debug, Default)]
pub struct AfterMiddlewareContext {
    pub response_status: u16,
    pub error_message: Option<String>,
    pub metrics: HashMap<String, f64>,
}

#[derive(Default)]
struct MiddlewareExecutionState {
    next_called: bool,
    status: u16,
    headers: HashMap<String, String>,
    forward_headers: HashMap<String, String>,
    body: String,
    ended: bool,
}

/// Loads `GatewayConfig` from the Lua entry file and returns the prepared runtime.
///
/// The same Lua VM instance is reused for middleware execution after startup.
pub fn load_config_and_runtime(
    entry_file: &str,
    registry: Arc<ServiceRegistry>,
    connection_manager: Arc<ConnectionManager>,
) -> anyhow::Result<(GatewayConfig, Arc<LuaRuntime>, Arc<GatewayCache>)> {
    let root = resolve_script_root(Path::new(entry_file))?;
    let canonical_entry = canonicalize_script(entry_file, &root)?;

    let lua = Lua::new();

    // Constrain package.path so require() resolves modules only from the
    // <entry-root>/modules/ directory.  This keeps the sandbox tight while
    // giving operators a conventional place to ship reusable Lua modules
    // alongside their basilisk.lua entry file.
    //
    // Note that we maintain a base module repository separately for commonly
    // used or at least very useful Lua add-on modules to make using Basilisk
    // a more enjoyable experience.
    let modules_path = root.join("modules").join("?.lua");
    let package_table: Table = lua.globals().get("package").map_err(lua_to_anyhow)?;
    package_table
        .set("path", modules_path.to_string_lossy().as_ref())
        .map_err(lua_to_anyhow)?;

    let before_middleware_mounts = Arc::new(Mutex::new(Vec::<MiddlewareMount>::new()));
    let after_middleware_mounts = Arc::new(Mutex::new(Vec::<MiddlewareMount>::new()));
    let static_forward_mounts = Arc::new(Mutex::new(Vec::<StaticForwardMount>::new()));
    let registration_allowlist_rules = Arc::new(Mutex::new(Vec::<RegistryKey>::new()));
    let config_state = Arc::new(Mutex::new(GatewayConfig::default()));
    let cache = Arc::new(GatewayCache::new("memory")?);
    let event_handlers: Arc<Mutex<HashMap<String, RegistryKey>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let primitive_context = PrimitiveContext {
        config: Arc::clone(&config_state),
        cache: Arc::clone(&cache),
        registry: Arc::clone(&registry),
        connection_manager: Arc::clone(&connection_manager),
        before_middleware_mounts: Arc::clone(&before_middleware_mounts),
        after_middleware_mounts: Arc::clone(&after_middleware_mounts),
        static_forward_mounts: Arc::clone(&static_forward_mounts),
        registration_allowlist_rules: Arc::clone(&registration_allowlist_rules),
        event_handlers: Arc::clone(&event_handlers),
    };

    register_primitives(&lua, &primitive_context).map_err(lua_to_anyhow)?;

    let loaded_files = Arc::new(Mutex::new(HashSet::<PathBuf>::new()));
    let include_root = root.clone();
    let include_loaded_files = Arc::clone(&loaded_files);
    let include_fn = lua
        .create_function(move |lua, include_path: String| {
            let include_canonical =
                canonicalize_script(&include_path, &include_root).map_err(mlua::Error::external)?;
            execute_script_file(lua, include_canonical, Arc::clone(&include_loaded_files))
        })
        .map_err(lua_to_anyhow)?;
    lua.globals()
        .set("load_lua_file", include_fn)
        .map_err(lua_to_anyhow)?;

    execute_script_file(&lua, canonical_entry, loaded_files).map_err(lua_to_anyhow)?;

    let config = config_state
        .lock()
        .map_err(|_| anyhow!("Lua config lock poisoned"))?
        .clone();

    let internal_namespace = format!("basilisk:gateway:{}", config.cache.key_prefix);
    cache
        .set_internal_namespace(&internal_namespace)
        .with_context(|| format!("Failed to set cache namespace to '{internal_namespace}'"))?;

    let (event_dispatch_tx, event_dispatch_rx) =
        mpsc::unbounded_channel::<ServiceBusEventEnvelope>();

    let runtime = Arc::new(LuaRuntime {
        lua: Mutex::new(lua),
        before_middlewares: Mutex::new(
            before_middleware_mounts
                .lock()
                .map_err(|_| anyhow!("Lua middleware lock poisoned"))?
                .drain(..)
                .collect(),
        ),
        after_middlewares: Mutex::new(
            after_middleware_mounts
                .lock()
                .map_err(|_| anyhow!("Lua middleware lock poisoned"))?
                .drain(..)
                .collect(),
        ),
        static_forward_mounts: Mutex::new(
            static_forward_mounts
                .lock()
                .map_err(|_| anyhow!("Lua static forward lock poisoned"))?
                .drain(..)
                .collect(),
        ),
        registration_allowlist_rules: Mutex::new(
            registration_allowlist_rules
                .lock()
                .map_err(|_| anyhow!("Lua registration allowlist lock poisoned"))?
                .drain(..)
                .collect(),
        ),
        event_handlers,
        event_dispatch_tx,
    });

    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        // Spawn the bus-event dispatch loop: drains the connection_manager's internal_rx
        // (messages delivered to the reserved basilisk connection) and fans them out to
        // the Lua event-dispatch channel.
        let cm_for_dispatch = Arc::clone(&connection_manager);
        let dispatch_tx = runtime.event_dispatch_tx.clone();
        handle.spawn(async move {
            let mut rx = cm_for_dispatch.internal_rx.lock().await;
            while let Some(msg) = rx.recv().await {
                if let Some(event) = msg.event {
                    let _ = dispatch_tx.send(event);
                }
            }
        });

        // Spawn the synchronous Lua handler task: calls registered Lua callbacks for each event.
        let runtime_for_handlers = Arc::clone(&runtime);
        handle.spawn(async move {
            let mut rx = event_dispatch_rx;
            while let Some(event) = rx.recv().await {
                runtime_for_handlers.dispatch_event(event);
            }
        });
    }

    Ok((config, runtime, cache))
}

impl LuaRuntime {
    /// Creates a no-op runtime that always lets requests proceed.
    pub fn allow_all() -> Arc<Self> {
        let (event_dispatch_tx, _) = mpsc::unbounded_channel::<ServiceBusEventEnvelope>();
        Arc::new(Self {
            lua: Mutex::new(Lua::new()),
            before_middlewares: Mutex::new(Vec::new()),
            after_middlewares: Mutex::new(Vec::new()),
            static_forward_mounts: Mutex::new(Vec::new()),
            registration_allowlist_rules: Mutex::new(Vec::new()),
            event_handlers: Arc::new(Mutex::new(HashMap::new())),
            event_dispatch_tx,
        })
    }

    /// Evaluates registration source-IP allowlist rules.
    ///
    /// If no rules are configured, registration is allowed.
    /// If rules are configured, at least one rule must return `true`.
    pub fn is_registration_ip_allowed(
        &self,
        connection_info: &RequestConnectionInfo,
    ) -> anyhow::Result<bool> {
        let lua = self
            .lua
            .lock()
            .map_err(|_| anyhow!("Lua runtime lock poisoned"))?;
        let rule_keys = self
            .registration_allowlist_rules
            .lock()
            .map_err(|_| anyhow!("Lua registration allowlist lock poisoned"))?;

        if rule_keys.is_empty() {
            return Ok(true);
        }

        let headers = HeaderMap::new();
        let req = build_matcher_req_table(&lua, "", "REGISTER", &headers, "", connection_info)
            .map_err(lua_to_anyhow)?;

        for rule_key in rule_keys.iter() {
            let rule: Function = lua.registry_value(rule_key).map_err(lua_to_anyhow)?;
            if rule.call::<bool>(req.clone()).map_err(lua_to_anyhow)? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Dispatches a service-bus event to the registered Lua handler for the event's topic.
    ///
    /// Called from the background event-dispatch task. Acquires the Lua VM lock
    /// synchronously so handlers execute one at a time in arrival order.
    fn dispatch_event(&self, event: ServiceBusEventEnvelope) {
        let lua = match self.lua.lock() {
            Ok(l) => l,
            Err(_) => {
                tracing::error!("Lua runtime lock poisoned during event dispatch");
                return;
            }
        };
        let handlers = match self.event_handlers.lock() {
            Ok(h) => h,
            Err(_) => {
                tracing::error!("Lua event handlers lock poisoned");
                return;
            }
        };
        let handler_key = match handlers.get(&event.topic) {
            Some(k) => k,
            None => return,
        };
        let handler: Function = match lua.registry_value(handler_key) {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Failed to retrieve Lua event handler: {}", e);
                return;
            }
        };

        let event_table = match build_event_table(&lua, &event) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!("Failed to build Lua event table: {}", e);
                return;
            }
        };

        if let Err(e) = handler.call::<()>(event_table) {
            tracing::error!("Lua event handler error on topic '{}': {}", event.topic, e);
        }
    }

    /// Runs registered Lua middlewares for a request in registration order.
    ///
    /// Returns a `MiddlewareExecutionResult` containing an optional short-circuit response
    /// and any forward headers accumulated during middleware execution.
    /// Forward headers are propagated to the downstream service regardless of whether
    /// middleware short-circuits or continues to the proxy.
    pub fn run_middlewares(
        &self,
        path: &str,
        method: &str,
        headers: &HeaderMap,
    ) -> anyhow::Result<MiddlewareExecutionResult> {
        let fallback_info = RequestConnectionInfo::from_socket(SocketAddr::from(([0, 0, 0, 0], 0)));
        self.run_middlewares_with_connection(path, method, headers, &fallback_info)
    }

    /// Runs registered Lua middlewares for a request using real remote connection data.
    pub fn run_middlewares_with_connection(
        &self,
        path: &str,
        method: &str,
        headers: &HeaderMap,
        connection_info: &RequestConnectionInfo,
    ) -> anyhow::Result<MiddlewareExecutionResult> {
        let lua = self
            .lua
            .lock()
            .map_err(|_| anyhow!("Lua runtime lock poisoned"))?;
        let middleware_mounts = self
            .before_middlewares
            .lock()
            .map_err(|_| anyhow!("Lua middleware lock poisoned"))?;

        let mut accumulated_forward_headers = HashMap::new();
        let shared_context = lua.create_table().map_err(lua_to_anyhow)?;
        let host = extract_host(headers);

        for mount in middleware_mounts.iter() {
            if !middleware_applies(&lua, mount, path, method, headers, &host, connection_info)
                .map_err(lua_to_anyhow)?
            {
                continue;
            }

            let input = MiddlewareExecutionInput {
                path,
                method,
                headers,
                host: &host,
                connection_info,
                after_context: None,
                shared_context: &shared_context,
            };

            let (response, forward_headers) =
                execute_middleware(&lua, mount, &input).map_err(lua_to_anyhow)?;

            // Accumulate forward headers
            accumulated_forward_headers.extend(forward_headers);

            if response.is_some() {
                return Ok(MiddlewareExecutionResult {
                    short_circuit_response: response,
                    forward_headers: accumulated_forward_headers,
                });
            }
        }

        Ok(MiddlewareExecutionResult {
            short_circuit_response: None,
            forward_headers: accumulated_forward_headers,
        })
    }

    /// Runs registered post-proxy middlewares after routing/forwarding completes.
    pub fn run_after_middlewares_with_connection(
        &self,
        path: &str,
        method: &str,
        headers: &HeaderMap,
        connection_info: &RequestConnectionInfo,
        context: &AfterMiddlewareContext,
    ) -> anyhow::Result<Option<MiddlewareResponse>> {
        let lua = self
            .lua
            .lock()
            .map_err(|_| anyhow!("Lua runtime lock poisoned"))?;
        let middleware_mounts = self
            .after_middlewares
            .lock()
            .map_err(|_| anyhow!("Lua middleware lock poisoned"))?;

        let host = extract_host(headers);
        let shared_context = lua.create_table().map_err(lua_to_anyhow)?;
        let mut current_response = None;

        for mount in middleware_mounts.iter() {
            if !middleware_applies(&lua, mount, path, method, headers, &host, connection_info)
                .map_err(lua_to_anyhow)?
            {
                continue;
            }

            let input = MiddlewareExecutionInput {
                path,
                method,
                headers,
                host: &host,
                connection_info,
                after_context: Some(context),
                shared_context: &shared_context,
            };

            let (response, _) = execute_middleware(&lua, mount, &input).map_err(lua_to_anyhow)?;

            if response.is_some() {
                current_response = response;
            }
        }

        Ok(current_response)
    }

    /// Resolves a statically configured HTTP forwarding route if one matches.
    pub fn resolve_static_forward(
        &self,
        path: &str,
        method: &str,
        headers: &HeaderMap,
        connection_info: &RequestConnectionInfo,
    ) -> anyhow::Result<Option<StaticForwardResolution>> {
        let lua = self
            .lua
            .lock()
            .map_err(|_| anyhow!("Lua runtime lock poisoned"))?;
        let mounts = self
            .static_forward_mounts
            .lock()
            .map_err(|_| anyhow!("Lua static forward lock poisoned"))?;
        let host = extract_host(headers);

        for mount in mounts.iter() {
            if !matcher_applies(
                &lua,
                &mount.matcher,
                path,
                method,
                headers,
                &host,
                connection_info,
            )
            .map_err(lua_to_anyhow)?
            {
                continue;
            }

            let reachable_targets: Vec<String> = mount
                .upstreams
                .iter()
                .filter(|upstream| static_forward_upstream_reachable(upstream))
                .map(StaticForwardUpstream::to_uri)
                .collect();

            let status = if reachable_targets.is_empty() {
                InstanceStatus::Down
            } else if reachable_targets.len() < mount.upstreams.len() {
                InstanceStatus::Degraded
            } else {
                InstanceStatus::Up
            };

            return Ok(Some(StaticForwardResolution {
                route_id: mount.route_id.clone(),
                status,
                configured_upstreams: mount.upstreams.len(),
                reachable_upstreams: reachable_targets.len(),
                reachable_targets,
            }));
        }

        Ok(None)
    }
}

fn matcher_applies(
    lua: &Lua,
    matcher: &MiddlewareMatcher,
    path: &str,
    method: &str,
    headers: &HeaderMap,
    host: &str,
    connection_info: &RequestConnectionInfo,
) -> LuaResult<bool> {
    match matcher {
        MiddlewareMatcher::Any => Ok(true),
        MiddlewareMatcher::Predicate(rule_key) => {
            let rule: Function = lua.registry_value(rule_key)?;
            let req = build_matcher_req_table(lua, path, method, headers, host, connection_info)?;
            rule.call::<bool>(req)
        }
    }
}

fn middleware_applies(
    lua: &Lua,
    mount: &MiddlewareMount,
    path: &str,
    method: &str,
    headers: &HeaderMap,
    host: &str,
    connection_info: &RequestConnectionInfo,
) -> LuaResult<bool> {
    matcher_applies(
        lua,
        &mount.matcher,
        path,
        method,
        headers,
        host,
        connection_info,
    )
}

fn execute_middleware(
    lua: &Lua,
    mount: &MiddlewareMount,
    input: &MiddlewareExecutionInput<'_>,
) -> LuaResult<(Option<MiddlewareResponse>, HashMap<String, String>)> {
    let state = Arc::new(Mutex::new(MiddlewareExecutionState {
        next_called: false,
        status: 200,
        headers: HashMap::new(),
        forward_headers: HashMap::new(),
        body: String::new(),
        ended: false,
    }));
    let req = build_req_table(lua, input, Arc::clone(&state))?;
    let res = build_res_table(lua, Arc::clone(&state), input.after_context)?;

    let next_state = Arc::clone(&state);
    let next_fn = lua.create_function(move |_, ()| {
        mark_next_called(&next_state)?;
        Ok(())
    })?;

    let handler: Function = lua.registry_value(&mount.handler_key)?;
    let _: Value = handler.call((req, res, next_fn))?;

    to_middleware_response_with_forward_headers(state)
}

fn build_req_table(
    lua: &Lua,
    input: &MiddlewareExecutionInput<'_>,
    state: Arc<Mutex<MiddlewareExecutionState>>,
) -> LuaResult<Table> {
    let req = lua.create_table()?;
    req.set("path", input.path)?;
    req.set("method", input.method)?;

    let lua_headers = lua.create_table()?;
    set_headers(input.headers, &lua_headers)?;
    req.set("headers", lua_headers)?;
    req.set("host", input.host)?;
    set_remote_info(lua, &req, input.headers, input.connection_info)?;
    match input
        .after_context
        .and_then(|ctx| ctx.error_message.as_deref())
    {
        Some(err) => req.set("err", err)?,
        None => req.set("err", Value::Nil)?,
    }

    // Use the shared context dictionary (passed from middleware lifecycle)
    req.set("ctx", input.shared_context.clone())?;

    let auth_state = Arc::clone(&state);
    req.set(
        "auth",
        lua.create_function(move |_, (_self_table, payload): (Table, Table)| {
            let json_payload = lua_table_to_json(payload)?;
            let payload_string =
                serde_json::to_string(&json_payload).map_err(mlua::Error::external)?;
            let encoded = URL_SAFE_NO_PAD.encode(payload_string.as_bytes());
            with_middleware_state(&auth_state, |s| {
                s.forward_headers
                    .insert("X-Basilisk-Auth".to_string(), encoded);
            })?;
            Ok(())
        })?,
    )?;

    Ok(req)
}

fn build_matcher_req_table(
    lua: &Lua,
    path: &str,
    method: &str,
    headers: &HeaderMap,
    host: &str,
    connection_info: &RequestConnectionInfo,
) -> LuaResult<Table> {
    let req = lua.create_table()?;
    req.set("path", path)?;
    req.set("method", method)?;
    req.set("host", host)?;

    let lua_headers = lua.create_table()?;
    set_headers(headers, &lua_headers)?;
    req.set("headers", lua_headers)?;

    set_remote_info(lua, &req, headers, connection_info)?;
    Ok(req)
}

fn set_remote_info(
    lua: &Lua,
    req: &Table,
    headers: &HeaderMap,
    connection_info: &RequestConnectionInfo,
) -> LuaResult<()> {
    req.set("remote_addr", connection_info.remote_addr_string())?;
    req.set("remote_ip", connection_info.remote_ip_string())?;
    req.set("remote_port", connection_info.remote_port())?;

    let claimed_ip = header_value(headers, "x-forwarded-for")
        .and_then(parse_first_forwarded_for_ip)
        .unwrap_or_default();
    let claimed_port = header_value(headers, "x-forwarded-port")
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0);

    req.set("claimed_ip", claimed_ip.clone())?;
    req.set("claimed_port", claimed_port)?;
    req.set(
        "has_claimed_ip_mismatch",
        !claimed_ip.is_empty() && claimed_ip != connection_info.remote_ip_string(),
    )?;
    req.set(
        "has_claimed_port_mismatch",
        claimed_port != 0 && claimed_port != connection_info.remote_port(),
    )?;
    req.set("is_spoofed_source", {
        let ip_mismatch =
            !claimed_ip.is_empty() && claimed_ip != connection_info.remote_ip_string();
        let port_mismatch = claimed_port != 0 && claimed_port != connection_info.remote_port();
        ip_mismatch || port_mismatch
    })?;

    let remote = lua.create_table()?;
    remote.set("addr", connection_info.remote_addr_string())?;
    remote.set("ip", connection_info.remote_ip_string())?;
    remote.set("port", connection_info.remote_port())?;
    req.set("remote", remote)?;

    Ok(())
}

fn build_res_table(
    lua: &Lua,
    state: Arc<Mutex<MiddlewareExecutionState>>,
    after_context: Option<&AfterMiddlewareContext>,
) -> LuaResult<Table> {
    let res = lua.create_table()?;

    if let Some(context) = after_context {
        let metrics = lua.create_table()?;
        for (metric, value) in &context.metrics {
            metrics.set(metric.as_str(), *value)?;
        }
        res.set("metrics", metrics)?;
        res.set("status_code", context.response_status)?;
    } else {
        res.set("metrics", Value::Nil)?;
        res.set("status_code", Value::Nil)?;
    }

    let status_state = Arc::clone(&state);
    res.set(
        "status",
        lua.create_function(move |_, (self_table, code): (Table, u16)| {
            with_middleware_state(&status_state, |s| s.status = code)?;
            Ok(self_table)
        })?,
    )?;

    let set_state = Arc::clone(&state);
    res.set(
        "set",
        lua.create_function(
            move |_, (self_table, key, value): (Table, String, String)| {
                with_middleware_state(&set_state, |s| {
                    s.headers.insert(key, value);
                })?;
                Ok(self_table)
            },
        )?,
    )?;

    let send_state = Arc::clone(&state);
    res.set(
        "send",
        lua.create_function(move |_, (self_table, body): (Table, String)| {
            with_middleware_state(&send_state, |s| {
                s.body = body;
                s.ended = true;
            })?;
            Ok(self_table)
        })?,
    )?;

    let json_state = Arc::clone(&state);
    res.set(
        "json",
        lua.create_function(move |_, (self_table, body): (Table, String)| {
            with_middleware_state(&json_state, |s| {
                s.headers
                    .insert("content-type".to_string(), "application/json".to_string());
                s.body = body;
                s.ended = true;
            })?;
            Ok(self_table)
        })?,
    )?;

    let end_state = Arc::clone(&state);
    res.set(
        "end",
        lua.create_function(move |_, (self_table,): (Table,)| {
            with_middleware_state(&end_state, |s| s.ended = true)?;
            Ok(self_table)
        })?,
    )?;

    let forward_headers_state = Arc::clone(&state);
    res.set(
        "forward_headers",
        lua.create_function(
            move |_, (self_table, key, value): (Table, String, String)| {
                // X-Basilisk-Auth is reserved and cannot be set by middleware
                if key.to_lowercase() == "x-basilisk-auth" {
                    return Err(mlua::Error::external(
                        "Header 'X-Basilisk-Auth' is reserved and cannot be set by middleware",
                    ));
                }
                with_middleware_state(&forward_headers_state, |s| {
                    s.forward_headers.insert(key, value);
                })?;
                Ok(self_table)
            },
        )?,
    )?;

    Ok(res)
}

fn mark_next_called(state: &Arc<Mutex<MiddlewareExecutionState>>) -> LuaResult<()> {
    with_middleware_state(state, |s| s.next_called = true)
}

fn with_middleware_state<F>(state: &Arc<Mutex<MiddlewareExecutionState>>, f: F) -> LuaResult<()>
where
    F: FnOnce(&mut MiddlewareExecutionState),
{
    let mut guard = state
        .lock()
        .map_err(|_| mlua::Error::external("Lua middleware state lock poisoned"))?;
    f(&mut guard);
    Ok(())
}

fn to_middleware_response_with_forward_headers(
    state: Arc<Mutex<MiddlewareExecutionState>>,
) -> LuaResult<(Option<MiddlewareResponse>, HashMap<String, String>)> {
    let state = state
        .lock()
        .map_err(|_| mlua::Error::external("Lua middleware state lock poisoned"))?;

    let response = if state.ended {
        Some(MiddlewareResponse {
            status: state.status,
            body: state.body.clone(),
            headers: state.headers.clone(),
        })
    } else {
        None
    };

    Ok((response, state.forward_headers.clone()))
}

fn register_primitives(lua: &Lua, context: &PrimitiveContext) -> LuaResult<()> {
    let basilisk = lua.create_table()?;

    basilisk.set("server", make_server_api(lua, Arc::clone(&context.config))?)?;
    basilisk.set(
        "gateway",
        make_gateway_api(lua, Arc::clone(&context.config))?,
    )?;
    basilisk.set(
        "cache",
        make_cache_api(lua, Arc::clone(&context.config), Arc::clone(&context.cache))?,
    )?;
    basilisk.set(
        "security",
        make_security_api(
            lua,
            Arc::clone(&context.config),
            Arc::clone(&context.registration_allowlist_rules),
        )?,
    )?;
    basilisk.set(
        "observability",
        make_observability_api(lua, Arc::clone(&context.config))?,
    )?;
    basilisk.set(
        "service_bus",
        make_service_bus_api(
            lua,
            Arc::clone(&context.config),
            Arc::clone(&context.connection_manager),
            Arc::clone(&context.event_handlers),
        )?,
    )?;
    basilisk.set(
        "registry",
        make_registry_api(lua, Arc::clone(&context.registry))?,
    )?;
    basilisk.set(
        "proxy",
        make_proxy_api(
            lua,
            Arc::clone(&context.before_middleware_mounts),
            Arc::clone(&context.after_middleware_mounts),
            Arc::clone(&context.static_forward_mounts),
        )?,
    )?;

    lua.globals().set("basilisk", basilisk)?;
    lua.globals().set("path_rules", make_path_rules_api(lua)?)?;
    lua.globals().set("net_rules", make_net_rules_api(lua)?)?;
    Ok(())
}

fn make_server_api(lua: &Lua, config: Arc<Mutex<GatewayConfig>>) -> LuaResult<Table> {
    let table = lua.create_table()?;

    let cfg = Arc::clone(&config);
    table.set(
        "host",
        lua.create_function(move |_, host: String| {
            with_config_mut(&cfg, |c| c.server.host = host)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "port",
        lua.create_function(move |_, port: u16| with_config_mut(&cfg, |c| c.server.port = port))?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "tls_enabled",
        lua.create_function(move |_, enabled: bool| {
            with_config_mut(&cfg, |c| c.server.tls.enabled = enabled)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "tls_cert_file",
        lua.create_function(move |_, cert_file: String| {
            with_config_mut(&cfg, |c| c.server.tls.cert_file = cert_file)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "tls_key_file",
        lua.create_function(move |_, key_file: String| {
            with_config_mut(&cfg, |c| c.server.tls.key_file = key_file)
        })?,
    )?;

    Ok(table)
}

fn make_gateway_api(lua: &Lua, config: Arc<Mutex<GatewayConfig>>) -> LuaResult<Table> {
    let table = lua.create_table()?;

    let cfg = Arc::clone(&config);
    table.set(
        "load_balancing_strategy",
        lua.create_function(move |_, strategy: String| {
            with_config_mut(&cfg, |c| {
                c.routing.default_load_balancing_strategy = strategy
            })
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "strip_prefix",
        lua.create_function(move |_, strip_prefix: bool| {
            with_config_mut(&cfg, |c| c.routing.strip_prefix = strip_prefix)
        })?,
    )?;

    Ok(table)
}

fn make_cache_api(
    lua: &Lua,
    config: Arc<Mutex<GatewayConfig>>,
    cache: Arc<GatewayCache>,
) -> LuaResult<Table> {
    let table = lua.create_table()?;

    let cfg = Arc::clone(&config);
    table.set(
        "enabled",
        lua.create_function(move |_, enabled: bool| {
            with_config_mut(&cfg, |c| c.cache.enabled = enabled)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    let cache_handle = Arc::clone(&cache);
    table.set(
        "provider",
        lua.create_function(move |_, provider: String| {
            cache_handle
                .reconfigure_provider(&provider)
                .map_err(cache_to_lua_error)?;
            with_config_mut(&cfg, |c| c.cache.provider = provider)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "key_prefix",
        lua.create_function(move |_, key_prefix: String| {
            with_config_mut(&cfg, |c| c.cache.key_prefix = key_prefix)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "service_resolution_ttl_seconds",
        lua.create_function(move |_, ttl_seconds: u64| {
            with_config_mut(&cfg, |c| {
                c.cache.service_resolution_ttl_seconds = ttl_seconds;
                // Keep ttl_seconds aligned for compatibility with older scripts.
                c.cache.ttl_seconds = ttl_seconds;
            })
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "ttl_seconds",
        lua.create_function(move |_, ttl_seconds: u64| {
            with_config_mut(&cfg, |c| {
                c.cache.ttl_seconds = ttl_seconds;
                c.cache.service_resolution_ttl_seconds = ttl_seconds;
            })
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "strategy",
        lua.create_function(move |_, strategy: String| {
            with_config_mut(&cfg, |c| c.cache.strategy = strategy)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "get",
        lua.create_function(move |_, key: String| {
            cache_handle.get(&key).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "get_or_set",
        lua.create_function(move |_, (key, value): (String, String)| {
            cache_handle
                .get_or_set(&key, value)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set",
        lua.create_function(move |_, (key, value): (String, String)| {
            cache_handle.set(&key, value).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_with_ttl",
        lua.create_function(move |_, (key, value, ttl): (String, String, u64)| {
            cache_handle
                .set_with_ttl(&key, value, ttl)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_if_not_exists",
        lua.create_function(move |_, (key, value): (String, String)| {
            cache_handle
                .set_if_not_exists(&key, value)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "incr",
        lua.create_function(move |_, (key, delta): (String, i64)| {
            cache_handle.incr(&key, delta).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "decr",
        lua.create_function(move |_, (key, delta): (String, i64)| {
            cache_handle.decr(&key, delta).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "list_append",
        lua.create_function(move |_, (key, value): (String, String)| {
            cache_handle
                .list_append(&key, value)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "list_prepend",
        lua.create_function(move |_, (key, value): (String, String)| {
            cache_handle
                .list_prepend(&key, value)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "list_pop_left",
        lua.create_function(move |_, key: String| {
            cache_handle.list_pop_left(&key).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "list_pop_right",
        lua.create_function(move |_, key: String| {
            cache_handle
                .list_pop_right(&key)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "list_length",
        lua.create_function(move |_, key: String| {
            cache_handle.list_length(&key).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "list_index",
        lua.create_function(move |_, (key, index): (String, usize)| {
            cache_handle
                .list_index(&key, index)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "list_range",
        lua.create_function(move |lua, (key, start, end): (String, usize, usize)| {
            string_vec_to_lua_table(
                lua,
                cache_handle
                    .list_range(&key, start, end)
                    .map_err(cache_to_lua_error)?,
            )
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_add",
        lua.create_function(move |_, (key, value): (String, String)| {
            cache_handle
                .set_add(&key, value)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_remove",
        lua.create_function(move |_, (key, value): (String, String)| {
            cache_handle
                .set_remove(&key, value)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_is_member",
        lua.create_function(move |_, (key, value): (String, String)| {
            cache_handle
                .set_is_member(&key, value)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_members",
        lua.create_function(move |lua, key: String| {
            string_vec_to_lua_table(
                lua,
                cache_handle.set_members(&key).map_err(cache_to_lua_error)?,
            )
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_random_member",
        lua.create_function(move |_, key: String| {
            cache_handle
                .set_random_member(&key)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_sort",
        lua.create_function(move |lua, key: String| {
            string_vec_to_lua_table(
                lua,
                cache_handle.set_sort(&key).map_err(cache_to_lua_error)?,
            )
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_sort_with_options",
        lua.create_function(move |lua, (key, options): (String, String)| {
            string_vec_to_lua_table(
                lua,
                cache_handle
                    .set_sort_with_options(&key, options)
                    .map_err(cache_to_lua_error)?,
            )
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_sort_by_score",
        lua.create_function(
            move |lua, (key, min, max): (String, Option<f64>, Option<f64>)| {
                string_vec_to_lua_table(
                    lua,
                    cache_handle
                        .set_sort_by_score(&key, min, max)
                        .map_err(cache_to_lua_error)?,
                )
            },
        )?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_sort_by_score_with_options",
        lua.create_function(
            move |lua, (key, min, max, options): (String, Option<f64>, Option<f64>, String)| {
                string_vec_to_lua_table(
                    lua,
                    cache_handle
                        .set_sort_by_score_with_options(&key, min, max, options)
                        .map_err(cache_to_lua_error)?,
                )
            },
        )?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "set_card",
        lua.create_function(move |_, key: String| {
            cache_handle.set_card(&key).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "hash_get",
        lua.create_function(move |_, (key, field): (String, String)| {
            cache_handle
                .hash_get(&key, &field)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "hash_set",
        lua.create_function(move |_, (key, field, value): (String, String, String)| {
            cache_handle
                .hash_set(&key, &field, value)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "hash_delete",
        lua.create_function(move |_, (key, field): (String, String)| {
            cache_handle
                .hash_delete(&key, &field)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "hash_exists",
        lua.create_function(move |_, (key, field): (String, String)| {
            cache_handle
                .hash_exists(&key, &field)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "hash_fields",
        lua.create_function(move |lua, key: String| {
            string_vec_to_lua_table(
                lua,
                cache_handle.hash_fields(&key).map_err(cache_to_lua_error)?,
            )
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "hash_random_field",
        lua.create_function(move |_, key: String| {
            cache_handle
                .hash_random_field(&key)
                .map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "hash_length",
        lua.create_function(move |_, key: String| {
            cache_handle.hash_length(&key).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "keys",
        lua.create_function(move |lua, ()| {
            string_vec_to_lua_table(lua, cache_handle.keys().map_err(cache_to_lua_error)?)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "exists",
        lua.create_function(move |_, key: String| {
            cache_handle.exists(&key).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "unlink",
        lua.create_function(move |_, key: String| {
            cache_handle.unlink(&key).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "expire",
        lua.create_function(move |_, (key, ttl): (String, u64)| {
            cache_handle.expire(&key, ttl).map_err(cache_to_lua_error)
        })?,
    )?;

    let cache_handle = Arc::clone(&cache);
    table.set(
        "delete",
        lua.create_function(move |_, key: String| {
            cache_handle.delete(&key).map_err(cache_to_lua_error)
        })?,
    )?;

    Ok(table)
}

fn make_security_api(
    lua: &Lua,
    config: Arc<Mutex<GatewayConfig>>,
    registration_allowlist_rules: Arc<Mutex<Vec<RegistryKey>>>,
) -> LuaResult<Table> {
    let table = lua.create_table()?;

    let cfg = Arc::clone(&config);
    table.set(
        "service_registration_auth",
        lua.create_function(move |_, value: String| {
            with_config_mut(&cfg, |c| c.security.service_registration_auth = value)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "registration_token",
        lua.create_function(move |_, token: String| {
            with_config_mut(&cfg, |c| c.security.registration_token = token)
        })?,
    )?;

    let rules = Arc::clone(&registration_allowlist_rules);
    table.set(
        "registration_allowlist",
        lua.create_function(move |lua, value: Value| {
            let parsed = parse_rule_keys_value(lua, value, "security.registration_allowlist")?;
            let mut guard = rules
                .lock()
                .map_err(|_| mlua::Error::external("Lua registration allowlist lock poisoned"))?;
            *guard = parsed;
            Ok(())
        })?,
    )?;

    // Alias for discoverability.
    let rules = Arc::clone(&registration_allowlist_rules);
    table.set(
        "registration_whitelist",
        lua.create_function(move |lua, value: Value| {
            let parsed = parse_rule_keys_value(lua, value, "security.registration_whitelist")?;
            let mut guard = rules
                .lock()
                .map_err(|_| mlua::Error::external("Lua registration allowlist lock poisoned"))?;
            *guard = parsed;
            Ok(())
        })?,
    )?;

    Ok(table)
}

fn parse_rule_keys_value(lua: &Lua, value: Value, api_name: &str) -> LuaResult<Vec<RegistryKey>> {
    match value {
        Value::Function(rule) => Ok(vec![lua.create_registry_value(rule)?]),
        Value::Table(table) => {
            let mut keys = Vec::new();
            let mut index = 1;
            loop {
                let value: Value = table.raw_get(index)?;
                match value {
                    Value::Function(rule) => {
                        keys.push(lua.create_registry_value(rule)?);
                        index += 1;
                    }
                    Value::Nil => break,
                    _ => {
                        return Err(mlua::Error::external(format!(
                            "{api_name} expects a rule function or an array of rule functions"
                        )));
                    }
                }
            }
            if keys.is_empty() {
                return Err(mlua::Error::external(format!(
                    "{api_name} requires at least one rule function"
                )));
            }
            Ok(keys)
        }
        _ => Err(mlua::Error::external(format!(
            "{api_name} expects a rule function or an array of rule functions"
        ))),
    }
}

fn make_observability_api(lua: &Lua, config: Arc<Mutex<GatewayConfig>>) -> LuaResult<Table> {
    let table = lua.create_table()?;

    let cfg = Arc::clone(&config);
    table.set(
        "metrics_enabled",
        lua.create_function(move |_, enabled: bool| {
            with_config_mut(&cfg, |c| c.observability.metrics_enabled = enabled)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "tracing_enabled",
        lua.create_function(move |_, enabled: bool| {
            with_config_mut(&cfg, |c| c.observability.tracing_enabled = enabled)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "log_level",
        lua.create_function(move |_, level: String| {
            with_config_mut(&cfg, |c| c.observability.log_level = level)
        })?,
    )?;

    Ok(table)
}

fn make_service_bus_api(
    lua: &Lua,
    config: Arc<Mutex<GatewayConfig>>,
    connection_manager: Arc<ConnectionManager>,
    event_handlers: Arc<Mutex<HashMap<String, RegistryKey>>>,
) -> LuaResult<Table> {
    let table = lua.create_table()?;

    let cfg = Arc::clone(&config);
    table.set(
        "enabled",
        lua.create_function(move |_, enabled: bool| {
            with_config_mut(&cfg, |c| c.service_bus.enabled = enabled)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "host",
        lua.create_function(move |_, host: String| {
            with_config_mut(&cfg, |c| c.service_bus.host = host)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "port",
        lua.create_function(move |_, port: u16| {
            with_config_mut(&cfg, |c| c.service_bus.port = port)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "max_message_chars",
        lua.create_function(move |_, max_chars: usize| {
            with_config_mut(&cfg, |c| c.service_bus.max_message_chars = max_chars)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "connection_health_enabled",
        lua.create_function(move |_, enabled: bool| {
            with_config_mut(&cfg, |c| c.service_bus.connection_health_enabled = enabled)
        })?,
    )?;

    let cfg = Arc::clone(&config);
    table.set(
        "monitoring_enabled",
        lua.create_function(move |_, enabled: bool| {
            with_config_mut(&cfg, |c| c.service_bus.monitoring_enabled = enabled)
        })?,
    )?;

    // Configurable Down threshold for proxy-based health when connection_health is disabled.
    // Instance is considered Down after N consecutive unreachable proxy attempts.
    // Primary name: proxy_failure_threshold
    // Aliases provided for discoverability: connection_failure_threshold, failure_threshold,
    // down_threshold, proxy_down_threshold
    let cfg = Arc::clone(&config);
    table.set(
        "proxy_failure_threshold",
        lua.create_function(move |_, threshold: u32| {
            if threshold == 0 {
                return Err(mlua::Error::external(
                    "proxy_failure_threshold must be >= 1",
                ));
            }
            with_config_mut(&cfg, |c| c.service_bus.proxy_failure_threshold = threshold)
        })?,
    )?;
    let cfg = Arc::clone(&config);
    table.set(
        "connection_failure_threshold",
        lua.create_function(move |_, threshold: u32| {
            if threshold == 0 {
                return Err(mlua::Error::external(
                    "connection_failure_threshold must be >= 1",
                ));
            }
            with_config_mut(&cfg, |c| c.service_bus.proxy_failure_threshold = threshold)
        })?,
    )?;
    let cfg = Arc::clone(&config);
    table.set(
        "failure_threshold",
        lua.create_function(move |_, threshold: u32| {
            if threshold == 0 {
                return Err(mlua::Error::external("failure_threshold must be >= 1"));
            }
            with_config_mut(&cfg, |c| c.service_bus.proxy_failure_threshold = threshold)
        })?,
    )?;
    let cfg = Arc::clone(&config);
    table.set(
        "down_threshold",
        lua.create_function(move |_, threshold: u32| {
            if threshold == 0 {
                return Err(mlua::Error::external("down_threshold must be >= 1"));
            }
            with_config_mut(&cfg, |c| c.service_bus.proxy_failure_threshold = threshold)
        })?,
    )?;
    let cfg = Arc::clone(&config);
    table.set(
        "proxy_down_threshold",
        lua.create_function(move |_, threshold: u32| {
            if threshold == 0 {
                return Err(mlua::Error::external("proxy_down_threshold must be >= 1"));
            }
            with_config_mut(&cfg, |c| c.service_bus.proxy_failure_threshold = threshold)
        })?,
    )?;

    let cm = Arc::clone(&connection_manager);
    table.set(
        "publish",
        lua.create_function(move |_, (topic, payload_json): (String, String)| {
            let payload_value: serde_json::Value =
                serde_json::from_str(&payload_json).map_err(mlua::Error::external)?;
            let payload = payload_value
                .as_object()
                .cloned()
                .ok_or_else(|| {
                    mlua::Error::external("service_bus.publish payload must be a JSON object")
                })?
                .into_iter()
                .collect::<HashMap<String, serde_json::Value>>();

            let event = ServiceBusEventEnvelope {
                event_id: format!("basilisk-lua-{}", Uuid::now_v7()),
                emitted_at_utc: Utc::now(),
                service_id: BASILISK_SERVICE_ID.to_string(),
                instance_id: BASILISK_INSTANCE_ID.to_string(),
                topic,
                message_type: "lua_event".to_string(),
                correlation_id: cm.next_correlation_id(),
                causation_id: None,
                payload,
            };
            Ok(cm.publish(event, None))
        })?,
    )?;

    // subscribe(topic, handlerFn) — register a callback for events on the given topic.
    // The reserved basilisk connection subscribes to the topic on the bus so that
    // published events are routed to the Lua runtime.
    let cm_sub = Arc::clone(&connection_manager);
    let handlers_sub = Arc::clone(&event_handlers);
    table.set(
        "subscribe",
        lua.create_function(move |lua, (topic, handler): (String, Function)| {
            let key = lua.create_registry_value(handler)?;
            handlers_sub
                .lock()
                .map_err(|_| mlua::Error::external("Lua event handlers lock poisoned"))?
                .insert(topic.clone(), key);
            cm_sub.subscribe_basilisk(vec![topic]);
            Ok(())
        })?,
    )?;

    // unsubscribe(topic) — remove the handler and unsubscribe the basilisk connection.
    let cm_unsub = Arc::clone(&connection_manager);
    let handlers_unsub = Arc::clone(&event_handlers);
    table.set(
        "unsubscribe",
        lua.create_function(move |_, topic: String| {
            handlers_unsub
                .lock()
                .map_err(|_| mlua::Error::external("Lua event handlers lock poisoned"))?
                .remove(&topic);
            cm_unsub.unsubscribe_basilisk(vec![topic]);
            Ok(())
        })?,
    )?;

    // forward(targetServiceId, messageType, payloadJsonObject, timeoutMs) →
    //   table { message_type, payload_json } | raises error
    //
    // Sends a forward request on the bus from the reserved basilisk identity and
    // blocks (via block_in_place) until the target service replies or the timeout fires.
    let cm_fwd = Arc::clone(&connection_manager);
    table.set(
        "forward",
        lua.create_function(
            move |lua,
                  (target, message_type, payload_json, timeout_ms): (
                String,
                String,
                Option<String>,
                Option<u64>,
            )| {
                let payload: HashMap<String, serde_json::Value> = match payload_json {
                    Some(ref j) => {
                        let payload_value: serde_json::Value =
                            serde_json::from_str(j).map_err(mlua::Error::external)?;
                        payload_value
                            .as_object()
                            .cloned()
                            .ok_or_else(|| {
                                mlua::Error::external(
                                    "service_bus.forward payload must be a JSON object",
                                )
                            })?
                            .into_iter()
                            .collect()
                    }
                    None => HashMap::new(),
                };

                let req = ServiceBusForwardRequest {
                    target_service_id: target,
                    message_type,
                    payload,
                    timeout_ms,
                };

                // block_in_place lets us call async code from the synchronous Lua
                // function without leaving the async context.
                let result = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(cm_fwd.forward_from_basilisk(req))
                });

                match result {
                    Ok(resp) => {
                        let t = lua.create_table()?;
                        t.set("message_type", resp.message_type)?;
                        t.set(
                            "payload_json",
                            serde_json::to_string(&resp.payload).map_err(mlua::Error::external)?,
                        )?;
                        Ok(t)
                    }
                    Err(e) => Err(mlua::Error::external(e)),
                }
            },
        )?,
    )?;

    Ok(table)
}

fn make_registry_api(lua: &Lua, registry: Arc<ServiceRegistry>) -> LuaResult<Table> {
    let table = lua.create_table()?;

    let reg = Arc::clone(&registry);
    table.set(
        "resolve_path",
        lua.create_function(move |_, path: String| Ok(reg.resolve_service_by_path(&path)))?,
    )?;

    let reg = Arc::clone(&registry);
    table.set(
        "has_service",
        lua.create_function(move |_, service_id: String| {
            Ok(reg.get_service(&service_id).is_some())
        })?,
    )?;

    let reg = Arc::clone(&registry);
    table.set(
        "bind_path",
        lua.create_function(move |_, (path_prefix, service_id): (String, String)| {
            reg.bind_path_prefix(path_prefix, service_id);
            Ok(())
        })?,
    )?;

    Ok(table)
}

fn make_proxy_api(
    lua: &Lua,
    before_middleware_mounts: Arc<Mutex<Vec<MiddlewareMount>>>,
    after_middleware_mounts: Arc<Mutex<Vec<MiddlewareMount>>>,
    static_forward_mounts: Arc<Mutex<Vec<StaticForwardMount>>>,
) -> LuaResult<Table> {
    let table = lua.create_table()?;

    let mounts = Arc::clone(&before_middleware_mounts);
    table.set(
        "use",
        lua.create_function(move |lua, args: MultiValue| {
            let (matchers, handler) = parse_middleware_use_args(lua, args)?;
            let handler_key = lua.create_registry_value(handler)?;
            let mut guard = mounts
                .lock()
                .map_err(|_| mlua::Error::external("Lua middleware lock poisoned"))?;

            // Register middleware for each matcher variant.
            for matcher in matchers {
                guard.push(MiddlewareMount {
                    matcher,
                    handler_key: lua
                        .create_registry_value(lua.registry_value::<Function>(&handler_key)?)?,
                });
            }
            Ok(())
        })?,
    )?;

    let mounts = Arc::clone(&after_middleware_mounts);
    table.set(
        "use_after",
        lua.create_function(move |lua, args: MultiValue| {
            let (matchers, handler) = parse_middleware_use_args(lua, args)?;
            let handler_key = lua.create_registry_value(handler)?;
            let mut guard = mounts
                .lock()
                .map_err(|_| mlua::Error::external("Lua middleware lock poisoned"))?;

            // Register middleware for each matcher variant.
            for matcher in matchers {
                guard.push(MiddlewareMount {
                    matcher,
                    handler_key: lua
                        .create_registry_value(lua.registry_value::<Function>(&handler_key)?)?,
                });
            }
            Ok(())
        })?,
    )?;

    let mounts = Arc::clone(&static_forward_mounts);
    table.set(
        "forward",
        lua.create_function(move |lua, args: MultiValue| {
            let (matchers, upstreams) = parse_proxy_forward_args(lua, args)?;
            let mut guard = mounts
                .lock()
                .map_err(|_| mlua::Error::external("Lua static forward lock poisoned"))?;
            let route_id = format!("static-forward-{}", Uuid::now_v7());

            for matcher in matchers {
                guard.push(StaticForwardMount {
                    route_id: route_id.clone(),
                    matcher,
                    upstreams: upstreams.clone(),
                });
            }
            Ok(())
        })?,
    )?;

    Ok(table)
}

fn parse_middleware_use_args(
    lua: &Lua,
    args: MultiValue,
) -> LuaResult<(Vec<MiddlewareMatcher>, Function)> {
    let values: Vec<Value> = args.into_vec();
    match values.as_slice() {
        [Value::Function(handler)] => {
            // Global middleware: use(handler)
            Ok((vec![MiddlewareMatcher::Any], handler.clone()))
        }
        [Value::Function(rule), Value::Function(handler)] => {
            // Rule middleware: use(ruleFn, handler)
            let rule_key = lua.create_registry_value(rule.clone())?;
            Ok((
                vec![MiddlewareMatcher::Predicate(rule_key)],
                handler.clone(),
            ))
        }
        [Value::Table(table_val), Value::Function(handler)] => {
            // Multi-rule middleware: use({ruleFn, ...}, handler)
            let matchers = parse_matcher_array(lua, table_val)?;
            Ok((matchers, handler.clone()))
        }
        _ => Err(mlua::Error::external(
            "proxy.use expects use(handler), use(ruleFn, handler), or use({ruleFn, ...}, handler)",
        )),
    }
}

fn parse_proxy_forward_args(
    lua: &Lua,
    args: MultiValue,
) -> LuaResult<(Vec<MiddlewareMatcher>, Vec<StaticForwardUpstream>)> {
    let values: Vec<Value> = args.into_vec();
    match values.as_slice() {
        [Value::Function(rule), Value::Table(upstreams)] => {
            let rule_key = lua.create_registry_value(rule.clone())?;
            Ok((
                vec![MiddlewareMatcher::Predicate(rule_key)],
                parse_static_forward_upstreams(upstreams)?,
            ))
        }
        [Value::Table(rules), Value::Table(upstreams)] => Ok((
            parse_matcher_array(lua, rules)?,
            parse_static_forward_upstreams(upstreams)?,
        )),
        _ => Err(mlua::Error::external(
            "proxy.forward expects forward(ruleFn, upstreams) or forward({ruleFn, ...}, upstreams)",
        )),
    }
}

fn parse_static_forward_upstreams(table: &Table) -> LuaResult<Vec<StaticForwardUpstream>> {
    let mut upstreams = Vec::new();
    let mut index = 1;

    loop {
        let value: Value = table.raw_get(index)?;
        match value {
            Value::Nil => break,
            Value::String(uri) => {
                upstreams.push(parse_static_forward_upstream_uri(uri.to_str()?.as_ref())?)
            }
            Value::Table(endpoint) => {
                upstreams.push(parse_static_forward_upstream_table(&endpoint)?)
            }
            _ => {
                return Err(mlua::Error::external(
                    "proxy.forward upstreams must be strings or tables",
                ));
            }
        }
        index += 1;
    }

    if upstreams.is_empty() {
        return Err(mlua::Error::external(
            "proxy.forward requires at least one upstream",
        ));
    }

    Ok(upstreams)
}

fn parse_static_forward_upstream_table(table: &Table) -> LuaResult<StaticForwardUpstream> {
    let scheme = table
        .get::<Option<String>>("scheme")?
        .unwrap_or_else(|| "http".to_string());
    let host: String = table.get("host")?;
    let port = table.get::<Option<u16>>("port")?.unwrap_or(0);

    if host.trim().is_empty() {
        return Err(mlua::Error::external(
            "proxy.forward upstream host is required",
        ));
    }

    Ok(StaticForwardUpstream { scheme, host, port })
}

fn parse_static_forward_upstream_uri(uri: &str) -> LuaResult<StaticForwardUpstream> {
    let uri = uri.trim();
    if uri.is_empty() {
        return Err(mlua::Error::external(
            "proxy.forward upstream URI cannot be empty",
        ));
    }

    let (scheme, rest) = if let Some(rest) = uri.strip_prefix("http://") {
        ("http", rest)
    } else if let Some(rest) = uri.strip_prefix("https://") {
        ("https", rest)
    } else {
        ("http", uri)
    };

    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) =
        parse_upstream_authority(authority, if scheme == "https" { 443 } else { 80 })?;

    Ok(StaticForwardUpstream {
        scheme: scheme.to_string(),
        host,
        port,
    })
}

fn parse_upstream_authority(authority: &str, default_port: u16) -> LuaResult<(String, u16)> {
    if let Some(stripped) = authority.strip_prefix('[')
        && let Some(end_bracket) = stripped.find(']')
    {
        let host = stripped[..end_bracket].to_string();
        let port = stripped[end_bracket + 1..]
            .strip_prefix(':')
            .and_then(|raw| raw.parse::<u16>().ok())
            .unwrap_or(default_port);
        return Ok((host, port));
    }

    if let Some(colon) = authority.rfind(':') {
        let host = authority[..colon].to_string();
        let port = authority[colon + 1..]
            .parse::<u16>()
            .unwrap_or(default_port);
        return Ok((host, port));
    }

    Ok((authority.to_string(), default_port))
}

impl StaticForwardUpstream {
    fn to_uri(&self) -> String {
        let host = crate::models::format_uri_host(&self.host);
        if self.port == 0 {
            format!("{}://{}", self.scheme, host)
        } else {
            format!("{}://{}:{}", self.scheme, host, self.port)
        }
    }
}

fn static_forward_upstream_reachable(upstream: &StaticForwardUpstream) -> bool {
    let port = if upstream.port == 0 {
        default_port_for_scheme(&upstream.scheme)
    } else {
        upstream.port
    };

    let authority = if upstream.host.starts_with('[') || !upstream.host.contains(':') {
        format!("{}:{}", upstream.host, port)
    } else {
        format!("[{}]:{}", upstream.host, port)
    };

    let Some(socket_addr) = authority.to_socket_addrs().ok().and_then(|mut addrs| {
        addrs.find(|addr| matches!(addr, SocketAddr::V4(_) | SocketAddr::V6(_)))
    }) else {
        return false;
    };

    TcpStream::connect_timeout(&socket_addr, std::time::Duration::from_millis(200)).is_ok()
}

fn default_port_for_scheme(scheme: &str) -> u16 {
    if scheme.eq_ignore_ascii_case("https") {
        443
    } else {
        80
    }
}

fn parse_matcher_array(lua: &Lua, table: &Table) -> LuaResult<Vec<MiddlewareMatcher>> {
    let mut matchers = Vec::new();
    let mut index = 1;

    loop {
        let value: Value = table.raw_get(index)?;
        match value {
            Value::Function(rule) => {
                let rule_key = lua.create_registry_value(rule.clone())?;
                matchers.push(MiddlewareMatcher::Predicate(rule_key));
                index += 1;
            }
            Value::Nil => break,
            _ => {
                return Err(mlua::Error::external(
                    "matcher array must contain only rule functions",
                ));
            }
        }
    }

    if matchers.is_empty() {
        return Err(mlua::Error::external("matcher array cannot be empty"));
    }

    Ok(matchers)
}

fn make_path_rules_api(lua: &Lua) -> LuaResult<Table> {
    let table = lua.create_table()?;

    table.set("exact", create_path_rule_exact_factory(lua)?)?;
    table.set("matches", create_path_rule_matches_factory(lua)?)?;
    table.set("has_prefix", create_path_rule_has_prefix_factory(lua)?)?;
    table.set(
        "has_prefix_in",
        create_path_rule_has_prefix_in_factory(lua)?,
    )?;
    table.set("from_host", create_path_rule_from_host_factory(lua)?)?;
    table.set("is_any_of", create_path_rule_is_any_of_factory(lua)?)?;

    Ok(table)
}

fn create_path_rule_exact_factory(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(move |lua, expected: String| {
        lua.create_function(move |_, req: Table| Ok(req.get::<String>("path")? == expected))
    })
}

fn create_path_rule_matches_factory(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(move |lua, pattern: String| {
        lua.create_function(move |_, req: Table| {
            let path = req.get::<String>("path")?;
            Ok(path_matches_pattern(&path, &pattern))
        })
    })
}

fn create_path_rule_has_prefix_factory(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(move |lua, prefix: String| {
        lua.create_function(move |_, req: Table| {
            let path = req.get::<String>("path")?;
            Ok(path.starts_with(&prefix))
        })
    })
}

fn create_path_rule_has_prefix_in_factory(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(move |lua, prefixes: Table| {
        let values = parse_lua_string_array(
            &prefixes,
            "path_rules.has_prefix_in expects an array of prefixes",
            "path_rules.has_prefix_in requires at least one prefix",
        )?;

        lua.create_function(move |_, req: Table| {
            let path = req.get::<String>("path")?;
            Ok(values.iter().any(|prefix| path.starts_with(prefix)))
        })
    })
}

fn create_path_rule_from_host_factory(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(move |lua, expected_host: String| {
        let expected = normalize_host_value(&expected_host);
        lua.create_function(move |_, req: Table| {
            let host = req.get::<String>("host")?;
            Ok(normalize_host_value(&host) == expected)
        })
    })
}

fn create_path_rule_is_any_of_factory(lua: &Lua) -> LuaResult<Function> {
    lua.create_function(move |lua, rules: Table| {
        let keys = parse_lua_rule_key_array(
            lua,
            &rules,
            "path_rules.is_any_of expects an array of rule functions",
            "path_rules.is_any_of requires at least one rule function",
        )?;

        lua.create_function(move |lua, req: Table| {
            for key in &keys {
                let rule: Function = lua.registry_value(key)?;
                if rule.call::<bool>(req.clone())? {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    })
}

fn parse_lua_string_array(
    table: &Table,
    expects_error: &str,
    empty_error: &str,
) -> LuaResult<Vec<String>> {
    let mut values = Vec::new();
    let mut index = 1;

    loop {
        let value: Value = table.raw_get(index)?;
        match value {
            Value::String(value) => {
                values.push(value.to_str()?.to_string());
                index += 1;
            }
            Value::Nil => break,
            _ => return Err(mlua::Error::external(expects_error)),
        }
    }

    if values.is_empty() {
        return Err(mlua::Error::external(empty_error));
    }

    Ok(values)
}

fn parse_lua_rule_key_array(
    lua: &Lua,
    table: &Table,
    expects_error: &str,
    empty_error: &str,
) -> LuaResult<Vec<RegistryKey>> {
    let mut keys = Vec::new();
    let mut index = 1;

    loop {
        let value: Value = table.raw_get(index)?;
        match value {
            Value::Function(rule) => {
                keys.push(lua.create_registry_value(rule)?);
                index += 1;
            }
            Value::Nil => break,
            _ => return Err(mlua::Error::external(expects_error)),
        }
    }

    if keys.is_empty() {
        return Err(mlua::Error::external(empty_error));
    }

    Ok(keys)
}

fn make_net_rules_api(lua: &Lua) -> LuaResult<Table> {
    let table = lua.create_table()?;

    table.set(
        "is_ip",
        lua.create_function(move |lua, expected_ip: String| {
            let normalized = normalize_ip_string(&expected_ip);
            lua.create_function(move |_, req: Table| {
                let remote_ip = req.get::<String>("remote_ip")?;
                Ok(normalize_ip_string(&remote_ip) == normalized)
            })
        })?,
    )?;

    table.set(
        "is_ip_in",
        lua.create_function(move |lua, ips: Table| {
            let mut normalized_set = HashSet::new();
            let mut index = 1;
            loop {
                let value: Value = ips.raw_get(index)?;
                match value {
                    Value::String(ip) => {
                        normalized_set.insert(normalize_ip_string(ip.to_str()?.as_ref()));
                        index += 1;
                    }
                    Value::Nil => break,
                    _ => {
                        return Err(mlua::Error::external(
                            "net_rules.is_ip_in expects an array of IP strings",
                        ));
                    }
                }
            }
            if normalized_set.is_empty() {
                return Err(mlua::Error::external(
                    "net_rules.is_ip_in requires at least one IP",
                ));
            }
            lua.create_function(move |_, req: Table| {
                let remote_ip = req.get::<String>("remote_ip")?;
                Ok(normalized_set.contains(&normalize_ip_string(&remote_ip)))
            })
        })?,
    )?;

    table.set(
        "is_from_subnet",
        lua.create_function(move |lua, cidr: String| {
            let (network_ip, prefix_len) = parse_cidr(&cidr)?;
            lua.create_function(move |_, req: Table| {
                let remote_ip: String = req.get("remote_ip")?;
                let Ok(remote) = remote_ip.parse::<IpAddr>() else {
                    return Ok(false);
                };
                Ok(ip_in_subnet(remote, network_ip, prefix_len))
            })
        })?,
    )?;

    table.set(
        "is_from_my_subnet",
        lua.create_function(move |lua, ()| {
            lua.create_function(move |_, req: Table| {
                let remote_ip: String = req.get("remote_ip")?;
                let Ok(remote) = remote_ip.parse::<IpAddr>() else {
                    return Ok(false);
                };
                Ok(is_private_or_loopback(remote))
            })
        })?,
    )?;

    Ok(table)
}

fn extract_host(headers: &HeaderMap) -> String {
    header_value(headers, "host")
        .map(|h| normalize_host_value(&h))
        .unwrap_or_default()
}

fn header_value(headers: &HeaderMap, header_name: &str) -> Option<String> {
    headers
        .get(header_name)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim().to_string())
}

fn parse_first_forwarded_for_ip(value: String) -> Option<String> {
    value
        .split(',')
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(normalize_ip_string)
}

fn normalize_host_value(host: &str) -> String {
    host.split(':').next().unwrap_or(host).trim().to_lowercase()
}

fn normalize_ip_string(ip: &str) -> String {
    ip.trim()
        .parse::<IpAddr>()
        .map(|parsed| parsed.to_string())
        .unwrap_or_else(|_| ip.trim().to_string())
}

fn path_matches_pattern(path: &str, pattern: &str) -> bool {
    if let Some((start, end)) = pattern.split_once('*') {
        path.starts_with(start) && path.ends_with(end)
    } else {
        path.contains(pattern)
    }
}

fn parse_cidr(cidr: &str) -> LuaResult<(IpAddr, u8)> {
    let (network, prefix_len_raw) = cidr
        .split_once('/')
        .ok_or_else(|| mlua::Error::external("CIDR must look like '<ip>/<prefix>'"))?;
    let network_ip = network
        .trim()
        .parse::<IpAddr>()
        .map_err(|_| mlua::Error::external("Invalid CIDR network IP"))?;
    let prefix_len = prefix_len_raw
        .trim()
        .parse::<u8>()
        .map_err(|_| mlua::Error::external("Invalid CIDR prefix"))?;

    match network_ip {
        IpAddr::V4(_) if prefix_len <= 32 => Ok((network_ip, prefix_len)),
        IpAddr::V6(_) if prefix_len <= 128 => Ok((network_ip, prefix_len)),
        IpAddr::V4(_) => Err(mlua::Error::external("IPv4 CIDR prefix must be <= 32")),
        IpAddr::V6(_) => Err(mlua::Error::external("IPv6 CIDR prefix must be <= 128")),
    }
}

fn ip_in_subnet(ip: IpAddr, network: IpAddr, prefix_len: u8) -> bool {
    match (ip, network) {
        (IpAddr::V4(ipv4), IpAddr::V4(v4_address)) => {
            let mask = if prefix_len == 0 {
                0
            } else {
                u32::MAX << (32 - prefix_len)
            };
            (u32::from(ipv4) & mask) == (u32::from(v4_address) & mask)
        }
        (IpAddr::V6(ipv6), IpAddr::V6(v6_address)) => {
            let mask = if prefix_len == 0 {
                0
            } else {
                u128::MAX << (128 - prefix_len)
            };
            (u128::from(ipv6) & mask) == (u128::from(v6_address) & mask)
        }
        _ => false,
    }
}

fn is_private_or_loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => {
            ipv4.is_private() || ipv4.is_loopback() || ipv4.is_link_local() || ipv4.is_broadcast()
        }
        IpAddr::V6(ipv6) => {
            ipv6.is_loopback()
                || ipv6.is_unicast_link_local()
                || (ipv6.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

fn string_vec_to_lua_table(lua: &Lua, values: Vec<String>) -> LuaResult<Table> {
    let table = lua.create_table()?;
    for (index, value) in values.into_iter().enumerate() {
        table.raw_set(index + 1, value)?;
    }
    Ok(table)
}

fn lua_table_to_json(table: Table) -> LuaResult<serde_json::Value> {
    // Detect Lua array-style table with contiguous numeric keys [1...N].
    let mut array_values = Vec::new();
    let mut index = 1;
    loop {
        let value: Value = table.raw_get(index)?;
        if let Value::Nil = value {
            break;
        }
        array_values.push(lua_value_to_json(value)?);
        index += 1;
    }

    let mut is_pure_array = true;
    for pair in table.pairs::<Value, Value>() {
        let (key, _) = pair?;
        match key {
            Value::Integer(i) if i >= 1 && (i as usize) <= array_values.len() => {}
            _ => {
                is_pure_array = false;
                break;
            }
        }
    }

    if is_pure_array {
        return Ok(serde_json::Value::Array(array_values));
    }

    let mut object = serde_json::Map::new();
    for pair in table.pairs::<Value, Value>() {
        let (key, value) = pair?;
        let key_string = match key {
            Value::String(s) => s.to_str()?.to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Number(n) => n.to_string(),
            _ => {
                return Err(mlua::Error::external(
                    "req.auth payload table keys must be string or number",
                ));
            }
        };
        object.insert(key_string, lua_value_to_json(value)?);
    }
    Ok(serde_json::Value::Object(object))
}

fn lua_value_to_json(value: Value) -> LuaResult<serde_json::Value> {
    match value {
        Value::Nil => Ok(serde_json::Value::Null),
        Value::Boolean(b) => Ok(serde_json::Value::Bool(b)),
        Value::Integer(i) => Ok(serde_json::json!(i)),
        Value::Number(n) => Ok(serde_json::json!(n)),
        Value::String(s) => Ok(serde_json::Value::String(s.to_str()?.to_string())),
        Value::Table(t) => lua_table_to_json(t),
        _ => Err(mlua::Error::external(
            "req.auth payload supports only nil, boolean, number, string, and table values",
        )),
    }
}

fn build_event_table(lua: &Lua, event: &ServiceBusEventEnvelope) -> LuaResult<Table> {
    let t = lua.create_table()?;
    t.set("event_id", event.event_id.as_str())?;
    t.set("service_id", event.service_id.as_str())?;
    t.set("instance_id", event.instance_id.as_str())?;
    t.set("topic", event.topic.as_str())?;
    t.set("message_type", event.message_type.as_str())?;
    t.set("correlation_id", event.correlation_id)?;
    if let Some(ref cid) = event.causation_id {
        t.set("causation_id", cid.as_str())?;
    }
    // Serialize the payload map to a Lua table of JSON-string values so that
    // handlers can introspect payload fields without a full JSON library.
    let payload_table = lua.create_table()?;
    for (k, v) in &event.payload {
        payload_table.set(k.as_str(), v.to_string())?;
    }
    t.set("payload", payload_table)?;
    Ok(t)
}

fn with_config_mut<F>(config: &Arc<Mutex<GatewayConfig>>, f: F) -> LuaResult<()>
where
    F: FnOnce(&mut GatewayConfig),
{
    let mut guard = config
        .lock()
        .map_err(|_| mlua::Error::external("Lua config lock poisoned"))?;
    f(&mut guard);
    Ok(())
}

fn resolve_script_root(entry_path: &Path) -> anyhow::Result<PathBuf> {
    let dir = entry_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .with_context(|| {
            format!(
                "Failed to resolve Lua script root for {}",
                entry_path.display()
            )
        })?;
    Ok(dir)
}

fn canonicalize_script(path: &str, root: &Path) -> anyhow::Result<PathBuf> {
    if path.contains("://") {
        return Err(anyhow!("Only local filesystem Lua files are supported"));
    }

    let candidate = {
        let p = Path::new(path);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            root.join(p)
        }
    };

    let canonical = candidate
        .canonicalize()
        .with_context(|| format!("Failed to canonicalize Lua file: {}", candidate.display()))?;

    if !canonical.starts_with(root) {
        return Err(anyhow!(
            "Lua file {} is outside of allowed root {}",
            canonical.display(),
            root.display()
        ));
    }

    if canonical.extension().and_then(|e| e.to_str()) != Some("lua") {
        return Err(anyhow!(
            "Lua file must end in .lua: {}",
            canonical.display()
        ));
    }

    Ok(canonical)
}

fn execute_script_file(
    lua: &Lua,
    path: PathBuf,
    loaded_files: Arc<Mutex<HashSet<PathBuf>>>,
) -> LuaResult<()> {
    {
        let mut loaded = loaded_files
            .lock()
            .map_err(|_| mlua::Error::external("Lua loaded files lock poisoned"))?;
        if loaded.contains(&path) {
            return Ok(());
        }
        loaded.insert(path.clone());
    }

    let script = std::fs::read_to_string(&path).map_err(mlua::Error::external)?;
    lua.load(&script)
        .set_name(path.to_string_lossy().as_ref())
        .exec()
}

fn lua_to_anyhow(err: mlua::Error) -> anyhow::Error {
    anyhow!("Lua runtime error: {err}")
}

fn cache_to_lua_error(err: anyhow::Error) -> mlua::Error {
    mlua::Error::external(err)
}

#[cfg(test)]
mod tests {
    use super::load_config_and_runtime;
    use crate::registry::ServiceRegistry;
    use crate::service_bus::connection_manager::ConnectionManager;
    use axum::http::HeaderMap;
    use std::fs;
    use std::sync::Arc;
    use uuid::Uuid;

    fn test_dir(name: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("basilisk-lua-runtime-{}-{}", name, Uuid::new_v4()));
        fs::create_dir_all(&path).expect("failed to create test dir");
        path
    }

    #[test]
    fn lua_is_the_only_config_source() {
        let dir = test_dir("config");
        let script = dir.join("basilisk.lua");
        fs::write(
            &script,
            "basilisk.server.port(9191)\nbasilisk.gateway.strip_prefix(true)\n",
        )
        .expect("failed to write script");

        let registry = Arc::new(ServiceRegistry::new());
        let connection_manager = Arc::new(ConnectionManager::new());

        let (config, _runtime, _cache) =
            load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
                .expect("failed to load runtime");

        assert_eq!(config.server.port, 9191);
        assert!(config.routing.strip_prefix);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn lua_proxy_middleware_can_short_circuit_requests() {
        let dir = test_dir("auth");
        let script = dir.join("basilisk.lua");
        fs::write(
            &script,
            "basilisk.proxy.use(path_rules.has_prefix('/api/private'), function(req, res, next)\n  res:status(403):send('blocked')\nend)\n",
        )
        .expect("failed to write middleware script");

        let registry = Arc::new(ServiceRegistry::new());
        let connection_manager = Arc::new(ConnectionManager::new());

        let (_config, runtime, _cache) =
            load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
                .expect("failed to load runtime");

        let result = runtime
            .run_middlewares("/api/private/resource", "GET", &HeaderMap::new())
            .expect("middleware execution failed");

        let rejection = result
            .short_circuit_response
            .expect("expected short-circuit response");

        assert_eq!(rejection.status, 403);
        assert_eq!(rejection.body, "blocked");

        let _ = fs::remove_dir_all(dir);
    }
}
