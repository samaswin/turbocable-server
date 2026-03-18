//! WebSocket connection lifecycle: upgrade, per-connection handler, registry, and rate limiting.

pub mod handler;
pub mod limiter;
pub mod registry;
