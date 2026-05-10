# Basilisk

Basilisk is a programmable reverse proxy with three built-in control planes:

- HTTP service registry (`/registry/*`)
- TCP service bus (publish/subscribe and forward requests)
- Lua runtime for configuration and request middleware

The runtime is configured from a single Lua entry file passed on process startup.

## Table of Contents

- [1. Architecture](#1-architecture)
- [2. Repository Structure](#2-repository-structure)
- [3. Runtime Model](#3-runtime-model)
- [4. Getting Started](#4-getting-started)
- [5. Configuration (`basilisk.lua`)](#5-configuration-basilisklua)
- [6. Lua Middleware API (Express-style)](#6-lua-middleware-api-express-style)
- [7. HTTP Registry API](#7-http-registry-api)
- [8. Service Bus Protocol](#8-service-bus-protocol)
- [9. Request Flow](#9-request-flow)
- [10. Common Use Cases](#10-common-use-cases)
- [11. Testing](#11-testing)
- [12. Operational Notes](#12-operational-notes)
- [13. Troubleshooting](#13-troubleshooting)

## 1. Architecture

Basilisk runs two server surfaces in one process:

1. **HTTP gateway** (`axum`):
   - Registry endpoints under `/registry/*`
   - Reverse-proxy fallback for all non-registry paths
2. **TCP service bus**:
   - Line-delimited JSON protocol
   - Client connect/auth/subscribe/publish/forward operations

Both surfaces share an in-memory `ServiceRegistry` and `ConnectionManager`.

## 2. Repository Structure

Core modules:

- `src/main.rs`: process bootstrap, CLI argument handling, HTTP route wiring, task spawning
- `src/config.rs`: runtime config model (`GatewayConfig`) and defaults
- `src/lua_config.rs`: Lua VM bootstrap, primitive registration, middleware execution
- `src/gateway/`:
  - `mod.rs`: `AppState`
  - `routes.rs`: HTTP registry handlers
  - `proxy.rs`: reverse proxy handler and load-balancing
- `src/registry/`:
  - `mod.rs`: service registry storage and APIs
  - `maintenance.rs`: health checking and stale cleanup loop
- `src/service_bus/`:
  - `contracts.rs`: protocol message types
  - `connection_manager.rs`: in-memory connection/subscription manager
  - `server.rs`: TCP protocol server
- `tests/`: top-level integration tests for public APIs

## 3. Runtime Model

### Startup contract

Basilisk expects exactly one CLI argument: the Lua entry file path.

```bash
cargo run -- basilisk.lua
```

If missing or extra arguments are provided, the startup fails with usage help.

### Single config source of truth

- The Lua entry file is the canonical configuration source.
- Additional files can be imported with `load_lua_file("relative/path.lua")`.
- Imports are constrained to local `.lua` files under the entry file directory root.
- URL-like paths (`://`) are rejected.
- Lua modules can be placed in a `modules/` directory next to the entry file and loaded with `require`.

### Module system

Basilisk sets Lua's `package.path` to `<entry-root>/modules/?.lua` at startup. Files in that directory can be loaded with standard `require`:

```
basilisk.lua
modules/
  auth.lua
  constants.lua
```

```lua
-- basilisk.lua
local auth = require("auth")
local constants = require("constants")
```

`require` is intentionally scoped to the `modules/` folder only. Files in the entry root itself are not reachable via `require`; use `load_lua_file` for those.

## 4. Getting Started

```bash
cp basilisk.example.lua basilisk.lua
cargo run -- basilisk.lua
```

For development validation:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test -- --nocapture
```

## 5. Configuration (`basilisk.lua`)

All configuration is performed through `basilisk.*` primitives.

### 5.1 Minimal config

```lua
basilisk.server.host("0.0.0.0")
basilisk.server.port(8080)

basilisk.gateway.load_balancing_strategy("ROUND_ROBIN")
basilisk.gateway.strip_prefix(false)

basilisk.security.service_registration_auth("TOKEN")
basilisk.security.registration_token("secret-token")

basilisk.service_bus.enabled(true)
basilisk.service_bus.host("0.0.0.0")
basilisk.service_bus.port(5090)
```

### 5.2 Primitive reference

`basilisk.server`

- `host(string)`
- `port(number)`
- `tls_enabled(boolean)`
- `tls_cert_file(string)`
- `tls_key_file(string)`

`basilisk.gateway`

- `load_balancing_strategy(string)`
  - Supported values in proxy selection: `ROUND_ROBIN`, `WEIGHTED_ROUND_ROBIN`, `WEIGHTED_RANDOM`, `IP_HASH`
- `strip_prefix(boolean)`

`basilisk.security`

- `service_registration_auth(string)`
- `registration_token(string)`

`basilisk.observability`

- `metrics_enabled(boolean)`
- `tracing_enabled(boolean)`
- `log_level(string)`

`basilisk.registry`

- `bind_path(pathPrefix, serviceId)`
- `resolve_path(path)` -> `serviceId | nil`
- `has_service(serviceId)` -> `boolean`

`basilisk.service_bus`

- `enabled(boolean)`
- `host(string)`
- `port(number)`
- `max_message_chars(number)`
- `publish(topic, payloadJsonObjectString)`
- `subscribe(topic, handlerFn)` - subscribes Lua runtime to a topic; handler receives `event`
- `unsubscribe(topic)` - removes Lua handler and topic subscription for the runtime
- `forward(targetServiceId, path, method, headersJsonOrNil, bodyOrNil, timeoutMsOrNil)` -> `{ status, body, headers }`

### 5.3 Lua service bus callbacks and forwarding

The Lua runtime runs on a reserved bus identity (`service_id = "basilisk"`, `instance_id = "lua-runtime"`) that is pre-authenticated at startup.

```lua
basilisk.service_bus.subscribe("billing.events", function(event)
  -- event.topic, event.message_type, event.payload are available
end)

local response = basilisk.service_bus.forward(
  "orders",                 -- target service
  "/v1/orders/42",          -- path
  "GET",                    -- method
  '{"x-request-id":"abc"}',-- optional JSON headers
  nil,                       -- optional body
  30000                      -- optional timeout in ms
)

print(response.status)
```

`basilisk.proxy`

- `use(handlerFn)` - global middleware
- `use(pathPrefix, handlerFn)` - middleware on single path prefix
- `use({pathPrefix1, pathPrefix2, ...}, handlerFn)` - middleware on multiple path prefixes

## 6. Lua Middleware API (Express-style)

Middlewares run before proxy routing and can either pass control or short-circuit.

### 6.1 Handler signature

```lua
function (req, res, next)
    -- middleware logic here
end
```

`req` fields:

- `req.path`
- `req.method`
- `req.headers` (header map, lowercase lookup is recommended)
- `req.ctx` (context dictionary for storing arbitrary values shared across middleware)

`res` methods:

- `res:status(code)`
- `res:set(name, value)` - sets response headers for middleware termination
- `res:send(body)`
- `res:json(jsonString)`
- `res:end()`
- `res:forward_headers(name, value)` - adds headers to forward to the downstream service

`next()` continues execution to the next middleware (or proxy flow if a chain ends).

### 6.2 Request context dictionary

The `req.ctx` object allows you to store arbitrary key-value pairs that persist across middleware executions. This is useful for propagating authentication credentials or other request context:

```lua
basilisk.proxy.use(function(req, res, next)
  -- Store authentication context for downstream use
  req.ctx["user_id"] = "user123"
  req.ctx["permissions"] = "read,write"
  return next()
end)

basilisk.proxy.use("/api/protected", function(req, res, next)
  -- Access context stored by previous middleware
  local user_id = req.ctx["user_id"]
  if not user_id then
    return res:status(401):send("unauthorized")
  end
  
  -- Forward auth data to downstream service
  local auth_data = string.format('{"user_id":"%s"}', user_id)
  local encoded = require("base64").encode(auth_data)
  res:forward_headers("X-Auth-Payload", encoded)
  return next()
end)
```

### 6.3 Forward headers

Forward headers are propagated to the downstream service regardless of whether middleware short-circuits or continues. This is useful for adding authentication tokens, request IDs, or other metadata to proxied requests:

```lua
basilisk.proxy.use("/api/", function(req, res, next)
  -- Add tracing header for downstream service
  res:forward_headers("X-Request-ID", "req-" .. os.time())
  return next()
end)
```

**Reserved headers**: The header `X-Basilisk-Auth` is reserved and cannot be set by middleware. Attempting to set it will result in an error.

### 6.4 Route groups with array syntax

Multiple path prefixes can be combined into single middleware using Lua array syntax. This is useful for applying the same middleware logic to several related routes:

```lua
basilisk.proxy.use({"/api/users", "/api/orders", "/api/products"}, function(req, res, next)
  -- This middleware applies to all three paths
  res:forward_headers("X-API-Version", "v2")
  return next()
end)
```

Array syntax works seamlessly with context and forward headers:

```lua
basilisk.proxy.use({"/admin", "/restricted"}, function(req, res, next)
  local auth = req.headers["authorization"]
  if not auth then
    return res:status(403):send("forbidden")
  end
  
  -- Store auth info in context
  req.ctx.authenticated = true
  return next()
end)

basilisk.proxy.use({"/admin", "/restricted"}, function(req, res, next)
  if req.ctx.authenticated then
    res:forward_headers("X-Authenticated", "true")
  end
  return next()
end)
```

### 6.5 Example: endpoint auth termination at proxy edge

```lua
basilisk.proxy.use("/api/private", function(req, res, next)
  local auth = req.headers["authorization"]
  if auth == "Bearer internal-token" then
    return next()
  end

  return res:status(401)
    :set("www-authenticate", "Bearer")
    :send("unauthorized")
end)
```

### 6.6 Example: global middleware

```lua
basilisk.proxy.use(function(req, res, next)
  if req.method == "OPTIONS" then
    return res:status(204):send();
  end
  return next()
end)
```

## 7. HTTP Registry API

Base URL: `http://<host>:<port>`.

### 7.1 Register instance

`POST /registry/register`

Example request:

```json
{
  "serviceId": "orders",
  "fingerprint": "orders-v1",
  "healthCheck": "/health",
  "pathPrefixes": ["/api/orders"],
  "instance": {
    "instanceId": "orders-1",
    "scheme": "http",
    "host": "127.0.0.1",
    "port": 7001,
    "weight": 1
  },
  "auth": {
    "type": "token",
    "token": "secret-token"
  }
}
```

Notes:

- If `service_registration_auth == "TOKEN"`, `auth.type` must be `token` and token must match `registration_token`.
- Path prefix ownership is exclusive across services.

### 7.2 Heartbeat

`POST /registry/heartbeat/{service_id}/{instance_id}`

### 7.3 Deregister

`DELETE /registry/services/{service_id}/instances/{instance_id}`

### 7.4 Query

- `GET /registry/services`
- `GET /registry/services/{service_id}`

## 8. Service Bus Protocol

Transport: TCP, newline-delimited JSON messages.

### 8.1 Supported message `type` values

- `connect`
- `authenticate`
- `subscribe`
- `publish`
- `forward`
- `event` (server outbound)
- `ack` (server outbound)
- `error` (server outbound)
- `forward_response` (server outbound)

### 8.2 Connect/auth flow

```json
{ "type": "connect", "serviceId": "orders", "instanceId": "orders-1" }
```

```json
{ "type": "authenticate", "token": "<registration-token-issued-by-registry>" }
```

`serviceId = "basilisk"` and `instanceId = "lua-runtime"` are reserved by the runtime and cannot be used by external clients. A connect attempt using either value is rejected with `errorCode = "RESERVED_IDENTITY"`.

### 8.3 Subscribe/publish

```json
{ "type": "subscribe", "topics": ["service-orders", "billing-events"] }
```

```json
{
  "type": "publish",
  "event": {
    "eventId": "",
    "emittedAtUtc": "1970-01-01T00:00:00Z",
    "serviceId": "",
    "instanceId": "",
    "topic": "billing-events",
    "messageType": "invoice_created",
    "correlationId": 0,
    "causationId": null,
    "payload": { "invoiceId": "inv-1" }
  }
}
```

Server normalizes server-controlled event fields (`eventId`, `emittedAtUtc`, `serviceId`, `instanceId`, `correlationId`).

### 8.4 Forward request/response

Forward request:

```json
{
  "type": "forward",
  "forwardRequest": {
    "targetServiceId": "orders",
    "path": "/v1/orders/42",
    "method": "GET",
    "headers": { "x-request-id": "abc123" },
    "body": null,
    "timeoutMs": 30000
  }
}
```

Forward response:

```json
{
  "type": "forward_response",
  "forwardResponse": {
    "status": 200,
    "headers": { "content-type": "application/json" },
    "body": "{\"ok\":true}"
  },
  "message": "Forward request completed"
}
```

## 9. Request Flow

### 9.1 HTTP reverse proxy flow

1. Incoming request hits HTTP gateway.
2. Registry endpoints are matched first.
3. Non-registry routes enter `ProxyHandler` fallback.
4. Lua middleware chain executes in registration order.
5. If middleware ends response, request is short-circuited.
6. Otherwise, service is resolved by longest matching path prefix.
7. Healthy instance selected by configured load-balancing strategy.
8. Request is forwarded to selected instance.

### 9.2 Registry maintenance flow

Background task:

- periodically runs active health checks
- marks unhealthy instances as `Down`
- removes stale down instances beyond timeout

## 10. Common Use Cases

### 10.1 API gateway with dynamic policy

- Register backend services via `/registry/register`.
- Enforce auth, method restrictions, and custom deny/allow logic in Lua middlewares.
- Route traffic by prefix ownership.

### 10.2 Lightweight service-to-service event bus
- Connect/authenticate service instances.
- Subscribe to topic namespaces.
- Publish typed events with correlation IDs.

### 10.3 Bus-driven forwarding between services
- Use `type = "forward"` messages to request remote handling through the bus.
- Receive structured `forward_response` payloads.

## 11. Testing
Top-level integration tests are in `tests/`:

- `tests/public_registry_api.rs`
- `tests/public_lua_runtime_api.rs`
- `tests/public_gateway_routes_api.rs`
- `tests/public_gateway_proxy_api.rs`
- `tests/public_service_bus_api.rs`

Run all tests:

```bash
cargo test -- --nocapture
```

## 12. Operational Notes
- Registry and service buses are in-memory; state is not persisted across restarts.
- Lua middleware runs in-process; panics or heavy logic can impact request latency.
- `service_bus.max_message_chars` limits inbound line length per client message.
- TLS config fields exist in runtime config; bind/termination behavior depends on the current HTTP serving setup.

## 13. Troubleshooting

### Startup fails with a usage message

Ensure exactly one argument is provided:

```bash
cargo run -- basilisk.lua
```

### Lua include rejected

Check that:
- file ends with `.lua`
- this file is a local filesystem path (not URL)
- the file stays under the entry file root directory

### `require` fails at startup

`require` is scoped to `<entry-root>/modules/`. Check that:

- the module file is inside the `modules/` directory next to `basilisk.lua`
- the file is named `<module>.lua` (e.g. `require("auth")` → `modules/auth.lua`)
- you are **not** trying to `require` a file from the entry root itself — use `load_lua_file` for that

### Proxy returns `404`
- Verify service is registered and owns a matching path prefix.
- Verify the route prefix and request path alignment.

### Proxy returns `503`
- Verify at least one instance is `Up` and passing health checks.
