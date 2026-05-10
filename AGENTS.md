# AGENTS.md

Operational guidance for human contributors and LLM/code agents working in this repository.

## 1. Purpose

Work on Basilisk safely without breaking core contracts:

- Lua-first configuration
- Reverse proxy fallback behavior
- Service registry ownership model
- Service bus protocol compatibility

## 2. Architecture Map

- `src/main.rs`: process bootstrap and server wiring
- `src/config.rs`: runtime config data model
- `src/lua_config.rs`: Lua VM primitives and middleware runtime
- `src/gateway/routes.rs`: HTTP registry endpoints
- `src/gateway/proxy.rs`: reverse proxy routing/forwarding
- `src/registry/mod.rs`: registry storage and path ownership
- `src/registry/maintenance.rs`: health checks and stale cleanup
- `src/service_bus/contracts.rs`: wire contracts and protocol types
- `src/service_bus/connection_manager.rs`: bus connection/subscription state
- `src/service_bus/server.rs`: TCP protocol handling
- `tests/*.rs`: public API integration tests

## 3. Non-Negotiable Contracts

1. **Single startup arg**: runtime expects exactly one CLI argument (`basilisk.lua` path).
2. **Lua source of truth**: runtime config is set through Lua primitives.
3. **Lua include constraints**: only local `.lua` files under the entry root are allowed.
4. **Proxy middleware pattern**: use Express-like `req, res, next` semantics.
5. **Registry route ownership**: path prefixes are exclusive between services.
6. **Service bus framing**: newline-delimited JSON messages.

## 4. Change Strategy

When making changes:

- Prefer small, focused commits.
- Preserve backward compatibility for public API fields and protocol names.
- If compatibility must change, update tests and `README.md` in the same change.
- Refactor large functions into helpers to reduce cognitive complexity.

## 5. Testing Policy

Minimum validation for non-trivial changes:

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test -- --nocapture
```

If your change touches routing, Lua primitives, or service bus contracts, add/update integration tests in `tests/`.

## 6. Documentation Policy

Update docs when the behavior changes:

- `README.md` for architecture, configuration, APIs, run behavior
- rustdoc comments for important public types/functions
- keep examples copy-paste runnable
- for any new or changed public API, middleware contract, protocol field, or config primitive, documentation updates are REQUIRED in the same change
- preserve existing documentation style and structure (headings, tone, and example format) unless a full docs restructuring is explicitly requested

Avoid including release/change-log style narrative in `README.md`.

## 7. Common Pitfalls

- Reintroducing TOML runtime config paths
- Using legacy `:param` route syntax in Axum (must use `{param}`)
- Forgetting to enforce Lua "include root" restrictions
- Adding middleware behavior that bypasses `next()` semantics without tests
- Breaking `serde(rename = ...)` field names used by external clients

## 8. Contribution Etiquette

- Do not rewrite unrelated code.
- Do not remove tests without replacement coverage.
- Keep naming explicit and domain-specific.
- If uncertain about behavior, add tests first, then implement.
