//! Typed error hierarchy for the gateway.

/// All errors that can occur during gateway operation.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum GatewayError {
    /// Authentication or authorization failure.
    #[error("auth failed: {0}")]
    Auth(String),
    /// NATS connection or protocol error.
    #[error("NATS error: {0}")]
    Nats(#[from] async_nats::Error),
    /// Client sent an invalid or unparseable protocol frame.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// Failed to serialize an outbound message.
    #[error("serialization error: {0}")]
    Serialization(String),
}
