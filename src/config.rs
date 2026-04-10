//! CLI and environment-based configuration for the gateway.

use clap::Parser;

/// Replay enforcement phase.
///
/// Loaded from the `REPLAY_ENFORCEMENT` environment variable (default: `compat`).
/// Advance through phases as replay-capable client coverage grows and each
/// numeric promotion gate is verified.
///
/// Fast rollback: set `REPLAY_ENFORCEMENT=compat` and restart — no code change needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum ReplayEnforcement {
    /// Accept legacy clients without a hello; emit warnings and metrics for
    /// subscribe-before-hello connections.
    #[default]
    Compat,
    /// Phase B: require `hello` before any command. Subscribes without
    /// `replay_v1` on hello are **allowed** with warning logs and a dedicated
    /// metric so you can measure remaining legacy clients before Phase C.
    SoftEnforce,
    /// Phase C: require `hello` first and reject subscribes unless the client
    /// advertised `replay_v1` on hello.
    HardEnforce,
}

impl ReplayEnforcement {
    /// `true` when the client must send `hello` before other frames are processed.
    pub fn requires_hello_first(self) -> bool {
        matches!(self, Self::SoftEnforce | Self::HardEnforce)
    }

    /// `true` when subscribe without `replay_v1` must be rejected.
    pub fn rejects_non_replay_subscribe(self) -> bool {
        matches!(self, Self::HardEnforce)
    }
}

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

    /// Per-connection outbound channel capacity in message slots (live fan-out path).
    /// Increasing this reduces drops under bursty load at the cost of more memory per connection.
    /// At 200 msg/s a value of 4096 provides ~20 s of buffer before back-pressure drops occur.
    #[arg(long, env = "TURBOCABLE_WS_CHANNEL_CAPACITY", default_value = "4096")]
    pub ws_channel_capacity: usize,

    /// Capacity for the per-connection replay outbound queue (JetStream catch-up).
    /// Isolated from the live fan-out channel so replay bursts do not exhaust the live buffer;
    /// the outbound loop always drains replay before delivering live frames for ordering.
    #[arg(
        long,
        env = "TURBOCABLE_WS_REPLAY_CHANNEL_CAPACITY",
        default_value = "4096"
    )]
    pub ws_replay_channel_capacity: usize,

    /// Path to an RSA public key PEM file for JWT verification.
    #[arg(long, env = "TURBOCABLE_JWT_PUBLIC_KEY_PATH")]
    pub jwt_public_key_path: Option<String>,

    /// Maximum number of unacknowledged messages the NATS JetStream consumer allows.
    /// Controls back-pressure: prevents a slow gateway from being overwhelmed by NATS.
    #[arg(long, env = "TURBOCABLE_MAX_ACK_PENDING", default_value = "10000")]
    pub max_ack_pending: i64,

    /// Number of replicas for the TURBOCABLE JetStream stream.
    /// Use 1 for local dev (single NATS node) and 3 for production clusters.
    #[arg(long, env = "TURBOCABLE_NATS_STREAM_REPLICAS", default_value = "1")]
    pub nats_stream_replicas: usize,

    /// Replay enforcement phase.
    ///
    /// Controls hello ordering and `replay_v1` capability:
    /// - `compat` (default): allow subscribe-before-hello with a warning metric; allow
    ///   hello without `replay_v1`.
    /// - `soft_enforce`: reject commands before hello; allow subscribe even without
    ///   `replay_v1` (warning metric for migration).
    /// - `hard_enforce`: reject commands before hello; reject subscribe if hello lacked
    ///   `replay_v1`.
    ///
    /// Fast rollback: set `REPLAY_ENFORCEMENT=compat` and restart.
    #[arg(long, env = "REPLAY_ENFORCEMENT", default_value = "compat")]
    pub replay_enforcement: ReplayEnforcement,

    /// Maximum number of concurrent per-connection replay tasks on this gateway node.
    ///
    /// Excess replay requests queue (not dropped) until a slot is free.
    /// Lower values reduce memory and scheduler pressure during reconnect storms at
    /// the cost of delaying replays for connections beyond the cap.
    #[arg(
        long,
        env = "TURBOCABLE_MAX_REPLAY_CONCURRENCY",
        default_value = "1000"
    )]
    pub max_replay_concurrency: usize,

    /// Default per-stream rate limit in messages per second (0 = disabled).
    ///
    /// When non-zero, each stream gets an independent token-bucket limiter capped
    /// at this rate.  Messages that exceed the limit are dropped before fan-out
    /// and counted in `turbocable_stream_rate_limited_total`.
    ///
    /// Combine with `stream_rate_limit_burst` to allow short bursts above the
    /// sustained rate.
    #[arg(long, env = "TURBOCABLE_STREAM_RATE_LIMIT_RPS", default_value = "0")]
    pub stream_rate_limit_rps: u64,

    /// Default per-stream burst capacity in messages (0 = 2× rate).
    ///
    /// The bucket starts full, so a fresh stream can absorb up to `burst` messages
    /// instantly before the sustained `rps` limit takes effect.
    #[arg(long, env = "TURBOCABLE_STREAM_RATE_LIMIT_BURST", default_value = "0")]
    pub stream_rate_limit_burst: u64,

    /// Per-stream rate-limit overrides, semicolon-separated `"name=rps:burst"` pairs.
    ///
    /// Example: `"alerts=100:200;high_volume=5000:10000"`
    ///
    /// A stream listed here uses its own rps/burst regardless of the defaults.
    /// Use `burst=0` in an override to apply the 2× default rule.
    #[arg(long, env = "TURBOCABLE_STREAM_RATE_OVERRIDES", default_value = "")]
    pub stream_rate_overrides: String,
}

impl Config {
    /// Parses `stream_rate_overrides` into a `HashMap<stream_name, (rps, burst)>`.
    ///
    /// Invalid or malformed entries are silently skipped.  The expected format is
    /// semicolon-separated `"name=rps:burst"` pairs; whitespace around names and
    /// numbers is trimmed.
    pub fn parse_stream_rate_overrides(&self) -> std::collections::HashMap<String, (u64, u64)> {
        if self.stream_rate_overrides.is_empty() {
            return std::collections::HashMap::new();
        }
        self.stream_rate_overrides
            .split(';')
            .filter_map(|entry| {
                let (name, rest) = entry.split_once('=')?;
                let (rps_str, burst_str) = rest.split_once(':')?;
                let rps: u64 = rps_str.trim().parse().ok()?;
                let burst: u64 = burst_str.trim().parse().ok()?;
                Some((name.trim().to_owned(), (rps, burst)))
            })
            .collect()
    }
}

/// Generates a random node ID prefixed with `node_`.
fn default_node_id() -> String {
    format!("node_{}", uuid::Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use super::ReplayEnforcement;

    #[test]
    fn requires_hello_first_only_outside_compat() {
        assert!(!ReplayEnforcement::Compat.requires_hello_first());
        assert!(ReplayEnforcement::SoftEnforce.requires_hello_first());
        assert!(ReplayEnforcement::HardEnforce.requires_hello_first());
    }

    #[test]
    fn rejects_non_replay_subscribe_only_in_hard() {
        assert!(!ReplayEnforcement::Compat.rejects_non_replay_subscribe());
        assert!(!ReplayEnforcement::SoftEnforce.rejects_non_replay_subscribe());
        assert!(ReplayEnforcement::HardEnforce.rejects_non_replay_subscribe());
    }
}
