//! turbocable-server — high-performance WebSocket gateway for TurboCable.
//!
//! Handles 1M+ concurrent WebSocket connections with sub-50ms fan-out latency.
//! Uses NATS JetStream for message delivery and RS256 JWT for authentication.
//!
//! This crate is usable as a library to allow external tools (e.g. Criterion benchmarks)
//! to access the connection registry directly without going through the binary entry point.


pub(crate) mod auth;
pub mod config;
pub mod connection;
pub(crate) mod errors;
pub(crate) mod metrics;
pub(crate) mod presence;
pub(crate) mod protocol;
pub(crate) mod pubsub;
pub mod server;
