/// Middleware helper for managing HTTP header limits.
/// Basilisk passes through many headers when proxying, which can accumulate
/// and exceed hyper's default ~16KB header size limit (which causes HTTP 431 errors).
/// This module provides utilities and documentation for handling large headers.
/// Recommendations for handling HTTP 431 (Request Header Fields Too Large) errors:
///
/// 1. **Reduce header count or size client-side**:
///    - Remove unnecessary headers from client requests
///    - Combine repeated headers if possible
///    - Use compression for header values when applicable
///
/// 2. **Configure proxy behavior in basilisk.lua**:
///    - Use middleware to strip or limit headers before proxying
///    - Example: filter out non-essential headers like X-Original-* headers
///
///    ```lua
///    basilisk.proxy.use(function(req, res, next)
///      -- Strip large custom headers that may accumulate
///      if req.headers["x-custom-large-header"] and #req.headers["x-custom-large-header"] > 4096 then
///        res:forward_headers("x-custom-large-header", "")
///      end
///      return next()
///    end)
///    ```
///
/// 3. **Deploy behind a reverse proxy** (nginx, HAProxy):
///    - Let the front proxy handle decompression and header buffering
///    - Configure a front proxy with higher header limits
///    - Example nginx config:
///      ```text
///      large_client_header_buffers 4 32k;
///      ```
///
/// 4. **Implement header filtering middleware**:
///    - Use basilisk middleware to programmatically filter/normalize headers
///    - Aggregate repeated headers efficiently
///    - Limit or strip headers exceeding reasonable sizes per-name
///
pub mod recommendations {
    pub const DEFAULT_MAX_HEADER_SIZE_BYTES: usize = 16 * 1024; // hyper default (~16KB)
    pub const RECOMMENDED_MAX_HEADER_SIZE_BYTES: usize = 32 * 1024; // suggests deployment issue
}
