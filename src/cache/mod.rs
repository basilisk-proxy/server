//! Cache strategy layer.
//!
//! This module exposes a provider-agnostic [`CacheProvider`] trait and a small
//! factory for selecting a concrete backend at runtime. Concrete managers are
//! intentionally kept private to the module.
//!
//! Supported providers:
//! - `memory`
//! - `redis`
//! - `memcached`
//!
//! The in-memory manager stores data in-process.
//! The Redis manager uses the `redis` crate and connects via `BASILISK_REDIS_URL`
//! (default: `redis://127.0.0.1:6379/`).
//! The Memcached manager uses the `memcache` crate and connects via
//! `BASILISK_MEMCACHED_URL` (default: `memcache://127.0.0.1:11211`).

mod base;
mod in_memory;
mod memcached;
mod redis;
mod runtime;

pub use base::CacheProvider;
pub use runtime::GatewayCache;
