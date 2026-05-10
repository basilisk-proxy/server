-- Example: Multi-path middleware using array syntax
-- This demonstrates how to mount the same middleware on multiple routes

basilisk.server.host("0.0.0.0")
basilisk.server.port(8080)
basilisk.gateway.load_balancing_strategy("ROUND_ROBIN")

-- Apply API versioning header to all v2 API endpoints
basilisk.proxy.use({"/api/v2/users", "/api/v2/orders", "/api/v2/products"}, function(req, res, next)
  res:forward_headers("X-API-Version", "v2")
  return next()
end)

-- Apply authentication to all admin routes
basilisk.proxy.use({"/admin", "/admin/settings", "/admin/reports"}, function(req, res, next)
  local auth = req.headers["authorization"]
  if not auth or auth ~= "Bearer admin-token" then
    return res:status(403):send("forbidden")
  end

  req.ctx.admin = true
  return next()
end)

-- Apply additional admin logging
basilisk.proxy.use({"/admin", "/admin/settings", "/admin/reports"}, function(req, res, next)
  if req.ctx.admin then
    res:forward_headers("X-Admin-Request", "true")
  end
  return next()
end)

-- Apply rate limiting to public endpoints
basilisk.proxy.use({"/api/public/search", "/api/public/browse"}, function(req, res, next)
  res:forward_headers("X-Rate-Limit", "100/minute")
  return next()
end)

-- Global middleware for all requests
basilisk.proxy.use(function(req, res, next)
  res:forward_headers("X-Request-ID", "req-" .. os.time())
  return next()
end)
