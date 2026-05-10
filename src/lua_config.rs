use crate::cache::GatewayCache;
use crate::config::GatewayConfig;
use crate::registry::ServiceRegistry;
use crate::service_bus::connection_manager::ConnectionManager;
use crate::service_bus::contracts::{
    ServiceBusEventEnvelope, ServiceBusForwardRequest, BASILISK_INSTANCE_ID, BASILISK_SERVICE_ID,
};
use anyhow::{anyhow, Context};
use axum::http::HeaderMap;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::Utc;
use mlua::{Function, Lua, MultiValue, RegistryKey, Result as LuaResult, Table, Value};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use uuid::Uuid;

/// Hosts the embedded Lua VM and registered HTTP middlewares.
pub struct LuaRuntime {
    lua: Mutex<Lua>,
    middlewares: Mutex<Vec<MiddlewareMount>>,
    /// Registered service-bus event handlers: topic → handler key.
    event_handlers: Arc<Mutex<HashMap<String, RegistryKey>>>,
    /// Channel sender used by the background bus-dispatch task to deliver events
    /// to the synchronous Lua dispatch loop.
    event_dispatch_tx: mpsc::UnboundedSender<ServiceBusEventEnvelope>,
}

/// Internal middleware registration entry.
struct MiddlewareMount {
    path_prefix: Option<String>,
    handler_key: RegistryKey,
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

    let middleware_mounts = Arc::new(Mutex::new(Vec::<MiddlewareMount>::new()));
    let config_state = Arc::new(Mutex::new(GatewayConfig::default()));
    let cache = Arc::new(GatewayCache::new("memory")?);
    let event_handlers: Arc<Mutex<HashMap<String, RegistryKey>>> =
        Arc::new(Mutex::new(HashMap::new()));

    register_primitives(
        &lua,
        Arc::clone(&config_state),
        Arc::clone(&cache),
        Arc::clone(&registry),
        Arc::clone(&connection_manager),
        Arc::clone(&middleware_mounts),
        Arc::clone(&event_handlers),
    )
    .map_err(lua_to_anyhow)?;

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
        middlewares: Mutex::new(
            middleware_mounts
                .lock()
                .map_err(|_| anyhow!("Lua middleware lock poisoned"))?
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
            middlewares: Mutex::new(Vec::new()),
            event_handlers: Arc::new(Mutex::new(HashMap::new())),
            event_dispatch_tx,
        })
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
        let lua = self
            .lua
            .lock()
            .map_err(|_| anyhow!("Lua runtime lock poisoned"))?;
        let middleware_mounts = self
            .middlewares
            .lock()
            .map_err(|_| anyhow!("Lua middleware lock poisoned"))?;

        let mut accumulated_forward_headers = HashMap::new();
        let shared_context = lua.create_table().map_err(lua_to_anyhow)?;

        for mount in middleware_mounts.iter() {
            if !middleware_applies(mount, path) {
                continue;
            }

            let (response, forward_headers) =
                execute_middleware(&lua, mount, path, method, headers, &shared_context)
                    .map_err(lua_to_anyhow)?;

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
}

fn middleware_applies(mount: &MiddlewareMount, path: &str) -> bool {
    match &mount.path_prefix {
        Some(prefix) => path.starts_with(prefix),
        None => true,
    }
}

fn execute_middleware(
    lua: &Lua,
    mount: &MiddlewareMount,
    path: &str,
    method: &str,
    headers: &HeaderMap,
    shared_context: &Table,
) -> LuaResult<(Option<MiddlewareResponse>, HashMap<String, String>)> {
    let state = Arc::new(Mutex::new(MiddlewareExecutionState {
        next_called: false,
        status: 200,
        headers: HashMap::new(),
        forward_headers: HashMap::new(),
        body: String::new(),
        ended: false,
    }));
    let req = build_req_table(
        lua,
        path,
        method,
        headers,
        shared_context,
        Arc::clone(&state),
    )?;
    let res = build_res_table(lua, Arc::clone(&state))?;

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
    path: &str,
    method: &str,
    headers: &HeaderMap,
    shared_context: &Table,
    state: Arc<Mutex<MiddlewareExecutionState>>,
) -> LuaResult<Table> {
    let req = lua.create_table()?;
    req.set("path", path)?;
    req.set("method", method)?;

    let lua_headers = lua.create_table()?;
    for (key, value) in headers.iter() {
        if let Ok(value) = value.to_str() {
            lua_headers.set(key.as_str(), value)?;
        }
    }
    req.set("headers", lua_headers)?;

    // Use the shared context dictionary (passed from middleware lifecycle)
    req.set("ctx", shared_context.clone())?;

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

fn build_res_table(lua: &Lua, state: Arc<Mutex<MiddlewareExecutionState>>) -> LuaResult<Table> {
    let res = lua.create_table()?;

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

fn register_primitives(
    lua: &Lua,
    config: Arc<Mutex<GatewayConfig>>,
    cache: Arc<GatewayCache>,
    registry: Arc<ServiceRegistry>,
    connection_manager: Arc<ConnectionManager>,
    middleware_mounts: Arc<Mutex<Vec<MiddlewareMount>>>,
    event_handlers: Arc<Mutex<HashMap<String, RegistryKey>>>,
) -> LuaResult<()> {
    let basilisk = lua.create_table()?;

    basilisk.set("server", make_server_api(lua, Arc::clone(&config))?)?;
    basilisk.set("gateway", make_gateway_api(lua, Arc::clone(&config))?)?;
    basilisk.set("cache", make_cache_api(lua, Arc::clone(&config), cache)?)?;
    basilisk.set("security", make_security_api(lua, Arc::clone(&config))?)?;
    basilisk.set(
        "observability",
        make_observability_api(lua, Arc::clone(&config))?,
    )?;
    basilisk.set(
        "service_bus",
        make_service_bus_api(lua, Arc::clone(&config), connection_manager, event_handlers)?,
    )?;
    basilisk.set("registry", make_registry_api(lua, registry)?)?;
    basilisk.set("proxy", make_proxy_api(lua, middleware_mounts)?)?;

    lua.globals().set("basilisk", basilisk)?;
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

fn make_security_api(lua: &Lua, config: Arc<Mutex<GatewayConfig>>) -> LuaResult<Table> {
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

    Ok(table)
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
    middleware_mounts: Arc<Mutex<Vec<MiddlewareMount>>>,
) -> LuaResult<Table> {
    let table = lua.create_table()?;

    let mounts = Arc::clone(&middleware_mounts);
    table.set(
        "use",
        lua.create_function(move |lua, args: MultiValue| {
            let (path_prefixes, handler) = parse_middleware_use_args(args)?;
            let handler_key = lua.create_registry_value(handler)?;
            let mut guard = mounts
                .lock()
                .map_err(|_| mlua::Error::external("Lua middleware lock poisoned"))?;

            // Register middleware for each path prefix
            for path_prefix in path_prefixes {
                guard.push(MiddlewareMount {
                    path_prefix,
                    handler_key: lua
                        .create_registry_value(lua.registry_value::<Function>(&handler_key)?)?,
                });
            }
            Ok(())
        })?,
    )?;

    Ok(table)
}

fn parse_middleware_use_args(args: MultiValue) -> LuaResult<(Vec<Option<String>>, Function)> {
    let values: Vec<Value> = args.into_vec();
    match values.as_slice() {
        [Value::Function(handler)] => {
            // Global middleware: use(handler)
            Ok((vec![None], handler.clone()))
        }
        [Value::String(prefix), Value::Function(handler)] => {
            // Single path middleware: use(pathPrefix, handler)
            Ok((
                vec![Some(prefix.to_str()?.to_string())],
                handler.clone(),
            ))
        }
        [Value::Table(table_val), Value::Function(handler)] => {
            // Multi-path middleware: use({pathPrefix1, pathPrefix2, ...}, handler)
            let path_prefixes = parse_path_prefix_array(table_val)?;
            Ok((path_prefixes, handler.clone()))
        }
        _ => Err(mlua::Error::external(
            "proxy.use expects use(handler), use(pathPrefix, handler), or use({pathPrefix, ...}, handler)",
        )),
    }
}

fn parse_path_prefix_array(table: &Table) -> LuaResult<Vec<Option<String>>> {
    let mut prefixes = Vec::new();
    let mut index = 1;

    loop {
        let value: Value = table.raw_get(index)?;
        match value {
            Value::String(s) => {
                prefixes.push(Some(s.to_str()?.to_string()));
                index += 1;
            }
            Value::Nil => break,
            _ => {
                return Err(mlua::Error::external(
                    "path prefix array must contain only strings",
                ))
            }
        }
    }

    if prefixes.is_empty() {
        return Err(mlua::Error::external("path prefix array cannot be empty"));
    }

    Ok(prefixes)
}

fn string_vec_to_lua_table(lua: &Lua, values: Vec<String>) -> LuaResult<Table> {
    let table = lua.create_table()?;
    for (index, value) in values.into_iter().enumerate() {
        table.raw_set(index + 1, value)?;
    }
    Ok(table)
}

fn lua_table_to_json(table: Table) -> LuaResult<serde_json::Value> {
    // Detect Lua array-style table with contiguous numeric keys [1..N].
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
                ))
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
            "basilisk.proxy.use('/api/private', function(req, res, next)\n  res:status(403):send('blocked')\nend)\n",
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
