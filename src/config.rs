//! CLI and environment-based configuration for the gateway.

use clap::Parser;

/// Server configuration parsed from CLI arguments and environment variables.
#[derive(Parser, Debug, Clone)]
#[command(name = "turbocable-server", version, about)]
pub struct Config {
    /// TCP port the WebSocket server listens on.
    #[arg(long, env = "TURBOCABLE_PORT", default_value = "9292")]
    pub port: u16,

    /// NATS server URL for JetStream pub/sub.
    #[arg(
        long,
        env = "TURBOCABLE_NATS_URL",
        default_value = "nats://localhost:4222"
    )]
    pub nats_url: String,

    /// Unique identifier for this gateway node (auto-generated if omitted).
    #[arg(long, env = "TURBOCABLE_NODE_ID", default_value_t = default_node_id())]
    pub node_id: String,

    /// Interval in seconds between WebSocket ping frames.
    #[arg(long, env = "TURBOCABLE_PING_INTERVAL", default_value = "30")]
    pub ping_interval_secs: u64,

    /// Maximum concurrent WebSocket connections allowed per IP address.
    #[arg(long, env = "TURBOCABLE_MAX_CONN_PER_IP", default_value = "10")]
    pub max_connections_per_ip: u64,

    /// Path to an RSA public key PEM file for JWT verification.
    #[arg(long, env = "TURBOCABLE_JWT_PUBLIC_KEY_PATH")]
    pub jwt_public_key_path: Option<String>,
}

/// Generates a random node ID prefixed with `node_`.
fn default_node_id() -> String {
    format!("node_{}", uuid::Uuid::new_v4().simple())
}
