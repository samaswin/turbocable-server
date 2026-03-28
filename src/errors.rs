//! Typed error hierarchy for the gateway.

use std::time::Duration;

/// Error specific to client reconnect replay operations.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// The client's `last_seq + 1` is older than the stream's retention window.
    /// The caller should disconnect the client with `reason: "replay_window_exceeded"`.
    #[error(
        "replay window exceeded for stream '{stream}': requested seq {requested_seq}, oldest available {oldest_available}"
    )]
    WindowExceeded {
        stream: String,
        requested_seq: u64,
        oldest_available: u64,
    },
    /// Replay hit the per-stream message cap (`MAX_REPLAY_MESSAGES` in `pubsub::nats`) for this stream.
    /// The client should full-resync or reconnect to continue catch-up.
    #[error(
        "replay truncated for stream '{stream}': delivered {delivered} messages (limit {limit})"
    )]
    Truncated {
        stream: String,
        delivered: usize,
        limit: usize,
    },
    /// A JetStream operation failed during replay.
    #[error("JetStream replay error: {0}")]
    JetStream(String),
}

/// Outcome of a successful replay run (no [`ReplayError`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayDelivery {
    /// Messages delivered to the outbound replay path.
    pub delivered: usize,
    /// `true` when the WebSocket peer closed the outbound path mid-replay.
    pub peer_gone: bool,
    /// Elapsed time from replay start to first message successfully sent to the outbound channel.
    /// `None` when no messages were delivered (e.g. stream caught up with nothing to replay).
    pub first_delivery_elapsed: Option<Duration>,
}

/// All errors that can occur during gateway operation.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    /// Authentication or authorization failure.
    #[error("auth failed: {0}")]
    Auth(String),
    /// NATS connection or protocol error.
    #[error("NATS error: {0}")]
    Nats(#[from] async_nats::Error),
    /// JetStream stream or consumer operation failure.
    #[error("JetStream error: {0}")]
    JetStream(String),
    /// Client sent an invalid or unparseable protocol frame.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// Failed to serialize an outbound message.
    #[error("serialization error: {0}")]
    Serialization(String),
}
