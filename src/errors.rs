#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum GatewayError {
    #[error("auth failed: {0}")]
    Auth(String),
    #[error("NATS error: {0}")]
    Nats(#[from] async_nats::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}
