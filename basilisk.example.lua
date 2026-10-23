-- Core cache settings
basilisk.cache.provider("memory") -- Options: "memory", "redis", "memcached"
basilisk.cache.ttl_seconds(300) -- Default TTL for cached responses
basilisk.cache.strategy("lru") -- Eviction strategy for the cache provider
basilisk.cache.max_response_entries(1024) -- Cap for proxied responses stored in the response cache

-- Core server settings
basilisk.server.host("0.0.0.0")
basilisk.server.port(8080)
basilisk.server.tls_enabled(false)

-- Gateway / proxy settings
basilisk.gateway.load_balancing_strategy("ROUND_ROBIN")
basilisk.gateway.strip_prefix(false)

-- Security settings
-- Auth modes: "TOKEN" (shared registration token) or "KEY_PAIR" (per-service Ed25519 signatures).
basilisk.security.service_registration_auth("TOKEN")
basilisk.security.registration_token("secret-token")
-- For KEY_PAIR mode, provision each service's Ed25519 public key
-- (base64, URL-safe, without padding) instead of (or alongside) a shared token:
-- basilisk.security.service_registration_auth("KEY_PAIR")
-- basilisk.security.registration_public_key("orders-service", "<public-key-base64url>")
basilisk.security.registration_allowlist({
  net_rules.is_ip("127.0.0.1"),
  net_rules.is_from_subnet("10.20.0.0/16")
})

-- Service bus settings
basilisk.service_bus.enabled(true)
basilisk.service_bus.host("0.0.0.0")
basilisk.service_bus.port(5090)
basilisk.service_bus.max_message_chars(65536)
basilisk.service_bus.connection_health_enabled(false)
basilisk.service_bus.monitoring_enabled(true)
basilisk.service_bus.proxy_failure_threshold(7)

-- Optional: load reusable Lua modules from the modules/ directory.
-- Modules are loaded with require() and scoped to <entry-root>/modules/.
-- Example: local auth = require("auth")
--
-- To split large configs while keeping this file as the entrypoint, use:
-- load_lua_file("config/routes.lua")

-- Example: bind static route ownership in the registry
basilisk.registry.bind_path("/api/orders", "orders-service")

-- Example: bind a prefix to a dynamic resolver middleware.
-- The resolver names the *service id* per request via resolve(); routes are then
-- forwarded to a registered instance of that service. If resolve() is not called,
-- resolution falls through to the next matching mount and finally to registry
-- prefix resolution (e.g. "/api/orders" above).
basilisk.registry.bind_path("/api/dynamic", function(req, resolve)
  if string.find(req.path, "/api/dynamic/reporting", 1, true) then
    resolve("reporting-service")
    return
  end
  -- Fall through without resolving: the registry decides (404 if no match).
  return
end)


-- Example: static HTTP forwarding for a matching rule function.
-- Route health is computed from upstream reachability:
-- Up (all reachable), Degraded (partial), Down (none reachable).
basilisk.proxy.forward(path_rules.has_prefix("/external"), {
   { scheme = "http", host = "10.0.0.10", port = 8080 },
   { scheme = "http", host = "10.0.0.11", port = 8080 },
})

-- Runtime logging from Lua (emitted through the tracing subscriber under target
-- "basilisk::lua"). Rust-style placeholders are supported: `{}`, `{index}`,
-- `{name}` (resolved from a trailing table), and `{{`/`}}` escapes.
basilisk.log.info("gateway starting with cache provider {}", "memory")

-- Example: middleware storing context and forwarding auth headers
basilisk.proxy.use(function(req, _, next)
  -- Perform authentication and store in context
  local auth_token = req.headers["authorization"]
  if auth_token then
    req.ctx["auth_token"] = auth_token
    basilisk.log.debug("auth token captured for {}", req.ctx["auth_token"] ~= nil and "a request" or "no one")
  else
    basilisk.log.warn("request {} arrived without auth header", req.path)
  end
  return next()
end)

-- Example: cache-backed API key rate limiting.
-- Uses a classic fixed-window counter per API key. Per-key limits live in the
-- shared cache under `basilisk:ratelimit:limit:<api-key>`; requests fall back
-- to 30 requests/second when no override is configured.
local RATE_LIMIT_WINDOW_SECONDS = 1
local RATE_LIMIT_DEFAULT_PER_SECOND = 30
local RATE_LIMIT_LIMIT_KEY_PREFIX = "basilisk:ratelimit:limit:"
local RATE_LIMIT_COUNTER_KEY_PREFIX = "basilisk:ratelimit:counter:"

-- Seed example overrides. `set_if_not_exists` keeps operator-managed limits in
-- Redis/Memcached intact across restarts.
basilisk.cache.set_if_not_exists(RATE_LIMIT_LIMIT_KEY_PREFIX .. "orders-console", "120")
basilisk.cache.set_if_not_exists(RATE_LIMIT_LIMIT_KEY_PREFIX .. "reporting-batch", "5")

basilisk.proxy.use(path_rules.has_prefix("/api"), function(req, res, next)
  -- Headers are normalized to lowercase by Basilisk, so "X-API-Key" is read
  -- as "x-api-key".
  local api_key = req.headers["x-api-key"]
  if not api_key or api_key == "" then
    return res:status(401)
      :set("www-authenticate", "ApiKey")
      :send("missing X-API-Key header")
  end

  local limit = tonumber(basilisk.cache.get(RATE_LIMIT_LIMIT_KEY_PREFIX .. api_key))
  if not limit or limit < 1 then
    limit = RATE_LIMIT_DEFAULT_PER_SECOND
  else
    limit = math.floor(limit)
  end

  -- Bucket counters by window so each fixed window starts from zero.
  local window = math.floor(os.time() / RATE_LIMIT_WINDOW_SECONDS)
  local reset_at = (window + 1) * RATE_LIMIT_WINDOW_SECONDS
  local counter_key = RATE_LIMIT_COUNTER_KEY_PREFIX .. api_key .. ":" .. tostring(window)

  local used = basilisk.cache.incr(counter_key, 1)
  -- Keep buckets bounded. Re-applying the TTL also covers providers whose
  -- `incr` replaces the entry and drops its previous expiration.
  basilisk.cache.expire(counter_key, RATE_LIMIT_WINDOW_SECONDS * 2)

  local remaining = limit - used
  if remaining < 0 then
    remaining = 0
  end

  -- `forward_headers` exposes the quota to the downstream service for
  -- successful requests; `set` adds the headers to the 429 response.
  res:forward_headers("X-RateLimit-Limit", tostring(limit))
  res:forward_headers("X-RateLimit-Remaining", tostring(remaining))
  res:forward_headers("X-RateLimit-Reset", tostring(reset_at))

  if used > limit then
    return res:status(429)
      :set("retry-after", tostring(RATE_LIMIT_WINDOW_SECONDS))
      :set("x-ratelimit-limit", tostring(limit))
      :set("x-ratelimit-remaining", "0")
      :set("x-ratelimit-reset", tostring(reset_at))
      :send("rate limit exceeded")
  end

  req.ctx["api_key"] = api_key
  req.ctx["rate_limit"] = limit
  return next()
end)

-- Example: custom middleware mounted on selected path prefix
basilisk.proxy.use(path_rules.has_prefix("/api/private"), function(req, res, next)
  if not req.ctx["auth_token"] then
    return res:status(401)
      :set("www-authenticate", "Bearer")
      :send("unauthorized")
  end

  -- Forward authentication header to downstream service
  res:forward_headers("Authorization", req.ctx["auth_token"])
  return next()
end)

-- Example: custom error handling middleware
basilisk.proxy.use_after(path_rules.matches("*"), function(req, res, next)
  if req.err then
    return res:status(500):send("Internal Server Error")
  end
  return next()
end)
