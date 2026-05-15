-- Core cache settings
basilisk.cache.provider("memory") -- Options: "memory", "redis", "memcached"
basilisk.cache.ttl_seconds(300) -- Default TTL for cached responses
basilisk.cache.strategy("lru") -- Eviction strategy for the cache provider

-- Core server settings
basilisk.server.host("0.0.0.0")
basilisk.server.port(8080)
basilisk.server.tls_enabled(false)

-- Gateway / proxy settings
basilisk.gateway.load_balancing_strategy("ROUND_ROBIN")
basilisk.gateway.strip_prefix(false)

-- Security settings
basilisk.security.service_registration_auth("TOKEN")
basilisk.security.registration_token("secret-token")
basilisk.security.registration_allowlist({
  net_rules.is_ip("127.0.0.1"),
  net_rules.is_from_subnet("10.20.0.0/16")
})

-- Service bus settings
basilisk.service_bus.enabled(true)
basilisk.service_bus.host("0.0.0.0")
basilisk.service_bus.port(5090)
basilisk.service_bus.max_message_chars(65536)
basilisk.service_bus.connection_health_enabled(true)
basilisk.service_bus.monitoring_enabled(true)

-- Optional: load reusable Lua modules from the modules/ directory.
-- Modules are loaded with require() and scoped to <entry-root>/modules/.
-- Example: local auth = require("auth")
--
-- To split large configs while keeping this file as the entrypoint, use:
-- load_lua_file("config/routes.lua")

-- Example: bind static route ownership in the registry
basilisk.registry.bind_path("/api/orders", "orders-service")

-- Example: middleware storing context and forwarding auth headers
basilisk.proxy.use(function(req, _, next)
  -- Perform authentication and store in context
  local auth_token = req.headers["authorization"]
  if auth_token then
    req.ctx["auth_token"] = auth_token
  end
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
