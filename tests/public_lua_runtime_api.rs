use axum::http::HeaderMap;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use basilisk::lua_config::{load_config_and_runtime, LuaRuntime};
use basilisk::registry::ServiceRegistry;
use basilisk::service_bus::connection_manager::{ConnectionManager, ServiceBusConnection};
use basilisk::service_bus::contracts::{
    protocol_types, ServiceBusEventEnvelope, ServiceBusProtocolMessage,
};
use chrono::Utc;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

fn test_dir(name: &str) -> std::path::PathBuf {
    let path =
        std::env::temp_dir().join(format!("basilisk-public-lua-{}-{}", name, Uuid::new_v4()));
    fs::create_dir_all(&path).expect("failed to create temp test dir");
    path
}

#[test]
fn load_config_and_runtime_exposes_lua_only_configuration() {
    let dir = test_dir("config");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.server.port(9191)\n\
         basilisk.gateway.strip_prefix(true)\n\
         basilisk.service_bus.max_message_chars(9000)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (config, _runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    assert_eq!(config.server.port, 9191);
    assert!(config.routing.strip_prefix);
    assert_eq!(config.service_bus.max_message_chars, 9000);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn middleware_pipeline_supports_next_and_short_circuit() {
    let dir = test_dir("middleware");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use(function(req, res, next)\n\
         if req.path == '/public' then\n\
           return next()\n\
         end\n\
         return next()\n\
         end)\n\
         basilisk.proxy.use('/blocked', function(req, res, next)\n\
         return res:status(403):set('x-policy', 'lua'):send('blocked')\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    let allowed = runtime
        .run_middlewares("/public", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");
    assert!(allowed.short_circuit_response.is_none());

    let denied_result = runtime
        .run_middlewares("/blocked/resource", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");
    let denied = denied_result
        .short_circuit_response
        .expect("expected middleware short-circuit response");
    assert_eq!(denied.status, 403);
    assert_eq!(denied.body, "blocked");
    assert_eq!(
        denied.headers.get("x-policy").map(String::as_str),
        Some("lua")
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn allow_all_runtime_never_short_circuits() {
    let runtime = LuaRuntime::allow_all();
    let result = runtime
        .run_middlewares("/any", "GET", &HeaderMap::new())
        .expect("allow_all should not fail");
    assert!(result.short_circuit_response.is_none());
}

#[test]
fn context_is_preserved_and_extended_across_middleware() {
    let dir = test_dir("context");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use(function(req, res, next)\n\
          req.ctx['user_id'] = 'user-123'\n\
          return next()\n\
         end)\n\
         basilisk.proxy.use(function(req, res, next)\n\
          if req.ctx['user_id'] == 'user-123' then\n\
            res:forward_headers('X-User-ID', req.ctx['user_id'])\n\
          end\n\
          return next()\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    let result = runtime
        .run_middlewares("/api/test", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");

    assert!(result.short_circuit_response.is_none());
    assert_eq!(
        result.forward_headers.get("X-User-ID").map(String::as_str),
        Some("user-123")
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn forward_headers_are_accumulated_across_middleware() {
    let dir = test_dir("forward_headers");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use(function(req, res, next)\n\
          res:forward_headers('X-Request-ID', 'req-123')\n\
          return next()\n\
         end)\n\
         basilisk.proxy.use(function(req, res, next)\n\
          res:forward_headers('X-Service-Name', 'test-service')\n\
          return next()\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    let result = runtime
        .run_middlewares("/api/test", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");

    assert!(result.short_circuit_response.is_none());
    assert_eq!(
        result
            .forward_headers
            .get("X-Request-ID")
            .map(String::as_str),
        Some("req-123")
    );
    assert_eq!(
        result
            .forward_headers
            .get("X-Service-Name")
            .map(String::as_str),
        Some("test-service")
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn reserved_header_x_basilisk_auth_cannot_be_set_by_middleware() {
    let dir = test_dir("reserved_header");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use(function(req, res, next)\n\
          res:forward_headers('X-Basilisk-Auth', 'forbidden')\n\
          return next()\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    let result = runtime.run_middlewares("/api/test", "GET", &HeaderMap::new());

    // Should be an error because X-Basilisk-Auth is reserved
    assert!(
        result.is_err(),
        "Setting X-Basilisk-Auth should cause an error"
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn req_auth_sets_reserved_forward_header_with_base64url_payload() {
    let dir = test_dir("req_auth_header");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use(function(req, res, next)\n\
          req:auth({sub='user-123', roles={'admin'}, enabled=true, level=7})\n\
          return next()\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    let result = runtime
        .run_middlewares("/api/test", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");

    let encoded = result
        .forward_headers
        .get("X-Basilisk-Auth")
        .expect("X-Basilisk-Auth should be set by req.auth");
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded.as_bytes())
        .expect("forwarded auth header should be valid Base64URL");
    let parsed: serde_json::Value =
        serde_json::from_slice(&decoded).expect("decoded auth payload should be valid JSON");
    assert_eq!(parsed["sub"], "user-123");
    assert_eq!(parsed["roles"], serde_json::json!(["admin"]));
    assert_eq!(parsed["enabled"], true);
    assert_eq!(parsed["level"], 7);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn middleware_can_be_mounted_on_multiple_routes_using_array() {
    let dir = test_dir("multi_route");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use({'/api/users', '/api/orders', '/api/products'}, function(req, res, next)\n\
          res:forward_headers('X-Checked', 'true')\n\
          return next()\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    // All three paths should have the middleware applied
    for path in &["/api/users", "/api/orders", "/api/products"] {
        let result = runtime
            .run_middlewares(path, "GET", &HeaderMap::new())
            .expect("middleware execution should succeed");
        assert!(result.short_circuit_response.is_none());
        assert_eq!(
            result.forward_headers.get("X-Checked").map(String::as_str),
            Some("true"),
            "middleware should apply to path {}",
            path
        );
    }

    // Other paths should not have the middleware applied
    let result = runtime
        .run_middlewares("/api/inventory", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");
    assert!(result.short_circuit_response.is_none());
    assert!(result.forward_headers.get("X-Checked").is_none());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn multi_path_middleware_can_short_circuit() {
    let dir = test_dir("multi_route_short_circuit");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use({'/admin', '/restricted', '/private'}, function(req, res, next)\n\
          return res:status(403):send('forbidden')\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    // All paths in the array should be blocked
    for path in &["/admin", "/restricted", "/private"] {
        let result = runtime
            .run_middlewares(path, "GET", &HeaderMap::new())
            .expect("middleware execution should succeed");
        let response = result
            .short_circuit_response
            .expect("expected short-circuit response");
        assert_eq!(response.status, 403);
        assert_eq!(response.body, "forbidden");
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn multi_path_middleware_stores_context_properly() {
    let dir = test_dir("multi_route_context");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use({'/api/a', '/api/b'}, function(req, res, next)\n\
          req.ctx['route_group'] = 'api_group'\n\
          return next()\n\
         end)\n\
         basilisk.proxy.use({'/api/a', '/api/b'}, function(req, res, next)\n\
          if req.ctx['route_group'] == 'api_group' then\n\
            res:forward_headers('X-Route-Group', req.ctx['route_group'])\n\
          end\n\
          return next()\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    for path in &["/api/a", "/api/b"] {
        let result = runtime
            .run_middlewares(path, "GET", &HeaderMap::new())
            .expect("middleware execution should succeed");
        assert!(result.short_circuit_response.is_none());
        assert_eq!(
            result
                .forward_headers
                .get("X-Route-Group")
                .map(String::as_str),
            Some("api_group"),
            "context should be preserved for path {}",
            path
        );
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn multi_path_and_single_path_middleware_can_coexist() {
    let dir = test_dir("mixed_middleware");
    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "basilisk.proxy.use({'/api/v1', '/api/v2'}, function(req, res, next)\n\
          res:forward_headers('X-Version', 'multi')\n\
          return next()\n\
         end)\n\
         basilisk.proxy.use('/admin', function(req, res, next)\n\
          res:forward_headers('X-Admin', 'true')\n\
          return next()\n\
         end)\n\
         basilisk.proxy.use(function(req, res, next)\n\
          res:forward_headers('X-Global', 'true')\n\
          return next()\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    // /api/v1 should have multi and global headers
    let result = runtime
        .run_middlewares("/api/v1", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");
    assert_eq!(
        result.forward_headers.get("X-Version").map(String::as_str),
        Some("multi")
    );
    assert_eq!(
        result.forward_headers.get("X-Global").map(String::as_str),
        Some("true")
    );
    assert!(result.forward_headers.get("X-Admin").is_none());

    // /admin should have admin and global headers
    let result = runtime
        .run_middlewares("/admin", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");
    assert_eq!(
        result.forward_headers.get("X-Admin").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        result.forward_headers.get("X-Global").map(String::as_str),
        Some("true")
    );
    assert!(result.forward_headers.get("X-Version").is_none());

    // /other should only have global header
    let result = runtime
        .run_middlewares("/other", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");
    assert_eq!(
        result.forward_headers.get("X-Global").map(String::as_str),
        Some("true")
    );
    assert!(result.forward_headers.get("X-Version").is_none());
    assert!(result.forward_headers.get("X-Admin").is_none());

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn lua_can_require_modules_from_modules_directory() {
    let dir = test_dir("require_module");
    let modules_dir = dir.join("modules");
    fs::create_dir_all(&modules_dir).expect("failed to create modules dir");

    // Write a reusable Lua module
    fs::write(
        modules_dir.join("auth.lua"),
        "local M = {}\n\
         M.token = 'module-secret'\n\
         function M.make_header(token)\n\
           return 'Bearer ' .. token\n\
         end\n\
         return M\n",
    )
    .expect("failed to write module file");

    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "local auth = require('auth')\n\
         basilisk.proxy.use('/api', function(req, res, next)\n\
           res:forward_headers('X-Auth', auth.make_header(auth.token))\n\
           return next()\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    let result = runtime
        .run_middlewares("/api/resource", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");

    assert!(result.short_circuit_response.is_none());
    assert_eq!(
        result.forward_headers.get("X-Auth").map(String::as_str),
        Some("Bearer module-secret")
    );

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn lua_cannot_require_modules_outside_modules_directory() {
    let dir = test_dir("require_outside");

    // Write a Lua file directly in the root (not in modules/)
    fs::write(
        dir.join("sneaky.lua"),
        "return { secret = 'should-not-load' }\n",
    )
    .expect("failed to write file");

    let script = dir.join("basilisk.lua");
    fs::write(&script, "local sneaky = require('sneaky')\n").expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    // require('sneaky') should fail because package.path only covers modules/
    let result = load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager);
    assert!(result.is_err(), "require outside modules/ should fail");

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn lua_modules_can_be_shared_across_middleware() {
    let dir = test_dir("shared_modules");
    let modules_dir = dir.join("modules");
    fs::create_dir_all(&modules_dir).expect("failed to create modules dir");

    fs::write(
        modules_dir.join("constants.lua"),
        "return {\n\
           api_version = 'v2',\n\
           rate_limit = '100/minute',\n\
         }\n",
    )
    .expect("failed to write constants module");

    let script = dir.join("basilisk.lua");
    fs::write(
        &script,
        "local constants = require('constants')\n\
         basilisk.proxy.use('/api', function(req, res, next)\n\
           res:forward_headers('X-API-Version', constants.api_version)\n\
           return next()\n\
         end)\n\
         basilisk.proxy.use('/api', function(req, res, next)\n\
           res:forward_headers('X-Rate-Limit', constants.rate_limit)\n\
           return next()\n\
         end)\n",
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    let result = runtime
        .run_middlewares("/api/test", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");

    assert!(result.short_circuit_response.is_none());
    assert_eq!(
        result
            .forward_headers
            .get("X-API-Version")
            .map(String::as_str),
        Some("v2")
    );
    assert_eq!(
        result
            .forward_headers
            .get("X-Rate-Limit")
            .map(String::as_str),
        Some("100/minute")
    );

    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn lua_service_bus_subscribe_receives_published_events() {
    let dir = test_dir("sub");
    let script = dir.join("basilisk.lua");

    fs::write(
        &script,
        r#"
received_topic = nil
basilisk.service_bus.subscribe("lua.ping", function(event)
  received_topic = event.topic
end)
basilisk.proxy.use("/_check_sub", function(req, res, next)
  if received_topic then
    res:forward_headers("X-Received-Topic", received_topic)
  end
  return next()
end)
"#,
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());
    let cm = Arc::clone(&connection_manager);

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    let event = ServiceBusEventEnvelope {
        event_id: "evt-lua-sub".to_string(),
        emitted_at_utc: Utc::now(),
        service_id: "test-producer".to_string(),
        instance_id: "test-inst".to_string(),
        topic: "lua.ping".to_string(),
        message_type: "test".to_string(),
        correlation_id: cm.next_correlation_id(),
        causation_id: None,
        payload: HashMap::new(),
    };
    let delivered = cm.publish(event, None);
    assert!(delivered >= 1);

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let result = runtime
        .run_middlewares("/_check_sub", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");
    assert_eq!(
        result
            .forward_headers
            .get("X-Received-Topic")
            .map(String::as_str),
        Some("lua.ping")
    );

    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn lua_service_bus_unsubscribe_stops_handler_invocations() {
    let dir = test_dir("unsub");
    let script = dir.join("basilisk.lua");

    fs::write(
        &script,
        r#"
received_after_unsub = nil
basilisk.service_bus.subscribe("lua.once", function(event)
  received_after_unsub = event.topic
end)
basilisk.service_bus.unsubscribe("lua.once")
basilisk.proxy.use("/_check_unsub", function(req, res, next)
  if received_after_unsub == nil then
    res:forward_headers("X-After-Unsub", "nil")
  else
    res:forward_headers("X-After-Unsub", received_after_unsub)
  end
  return next()
end)
"#,
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());
    let cm = Arc::clone(&connection_manager);

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    let event = ServiceBusEventEnvelope {
        event_id: "evt-lua-unsub".to_string(),
        emitted_at_utc: Utc::now(),
        service_id: "test-producer".to_string(),
        instance_id: "test-inst".to_string(),
        topic: "lua.once".to_string(),
        message_type: "test".to_string(),
        correlation_id: cm.next_correlation_id(),
        causation_id: None,
        payload: HashMap::new(),
    };
    let _ = cm.publish(event, None);

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let result = runtime
        .run_middlewares("/_check_unsub", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");
    assert_eq!(
        result
            .forward_headers
            .get("X-After-Unsub")
            .map(String::as_str),
        Some("nil")
    );

    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn lua_service_bus_forward_returns_response() {
    let dir = test_dir("fwd_success");
    let script = dir.join("basilisk.lua");

    fs::write(
        &script,
        r#"
forward_message_type = nil
forward_payload_json = nil

local ok, resp = pcall(function()
  return basilisk.service_bus.forward("orders", "order.query", '{"orderId":"42"}', 1000)
end)

if ok and resp then
  forward_message_type = resp.message_type
  forward_payload_json = resp.payload_json
end

basilisk.proxy.use("/_check_forward", function(req, res, next)
  if forward_message_type then
    res:forward_headers("X-Forward-Message-Type", forward_message_type)
  end
  if forward_payload_json then
    res:forward_headers("X-Forward-Payload", forward_payload_json)
  end
  return next()
end)
"#,
    )
    .expect("failed to write script");

    let registry = Arc::new(ServiceRegistry::new());
    let connection_manager = Arc::new(ConnectionManager::new());
    let cm = Arc::clone(&connection_manager);

    let (orders_tx, mut orders_rx) = mpsc::unbounded_channel::<ServiceBusProtocolMessage>();
    cm.add_connection(
        "orders:orders-1".to_string(),
        ServiceBusConnection {
            service_id: "orders".to_string(),
            instance_id: "orders-1".to_string(),
            tx: orders_tx,
            authenticated: true,
            subscriptions: vec!["service-orders".to_string()],
        },
    );

    let cm_responder = Arc::clone(&cm);
    let responder = tokio::spawn(async move {
        if let Some(msg) = orders_rx.recv().await {
            if msg.r#type == protocol_types::EVENT {
                if let Some(event) = msg.event {
                    if let Some(reply_to) = event.payload.get("reply_to").and_then(|v| v.as_str()) {
                        let mut payload = HashMap::new();
                        payload.insert("ok".to_string(), serde_json::json!(true));
                        payload.insert("upstream".to_string(), serde_json::json!("orders"));

                        let response_event = ServiceBusEventEnvelope {
                            event_id: "evt-forward-response".to_string(),
                            emitted_at_utc: Utc::now(),
                            service_id: "orders".to_string(),
                            instance_id: "orders-1".to_string(),
                            topic: reply_to.to_string(),
                            message_type: "order.reply".to_string(),
                            correlation_id: event.correlation_id,
                            causation_id: Some(event.event_id),
                            payload,
                        };

                        cm_responder.publish(response_event, None);
                    }
                }
            }
        }
    });

    let (_config, runtime) =
        load_config_and_runtime(&script.to_string_lossy(), registry, connection_manager)
            .expect("failed to load runtime");

    responder.await.expect("responder task failed");

    let result = runtime
        .run_middlewares("/_check_forward", "GET", &HeaderMap::new())
        .expect("middleware execution should succeed");
    assert_eq!(
        result
            .forward_headers
            .get("X-Forward-Message-Type")
            .map(String::as_str),
        Some("order.reply")
    );
    let payload_json = result
        .forward_headers
        .get("X-Forward-Payload")
        .expect("forward payload header should be present");
    let parsed: serde_json::Value =
        serde_json::from_str(payload_json).expect("forward payload should be valid JSON");
    assert_eq!(parsed["ok"], true);
    assert_eq!(parsed["upstream"], "orders");

    let _ = fs::remove_dir_all(dir);
}
