//! turbocable-server — high-performance WebSocket gateway for TurboCable.
//!
//! Handles 1M+ concurrent WebSocket connections with sub-50ms fan-out latency.
//! Uses NATS JetStream for message delivery and RS256 JWT for authentication.
//!
//! This crate is usable as a library to allow external tools (e.g. Criterion benchmarks)
//! to access the connection registry directly without going through the binary entry point.

// `auth` and `protocol` are pub only when building the fuzz sub-crate so
// fuzz targets can reach the parsers directly.  All other builds keep them
// crate-private.
#[cfg(not(feature = "fuzz"))]
pub(crate) mod auth;
#[cfg(feature = "fuzz")]
pub mod auth;

pub mod config;
pub mod connection;
pub(crate) mod errors;
pub(crate) mod fanout;
pub(crate) mod metrics;
pub(crate) mod presence;

#[cfg(not(feature = "fuzz"))]
pub(crate) mod protocol;
#[cfg(feature = "fuzz")]
pub mod protocol;

pub(crate) mod pubsub;
pub mod server;
