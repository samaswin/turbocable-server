//! NATS JetStream integration for message delivery and replay.
//!
//! This module is the core of TurboCable's broadcast pipeline:
//!
//! ```text
//! Rails publishes to NATS subject TURBOCABLE.{stream}
//!   → NatsConsumer receives the message
//!   → Strips prefix → stream name
//!   → Pre-encodes for JSON + MessagePack
//!   → registry.fanout_encoded() delivers to all subscribers
//!   → NATS message is acknowledged
//! ```
//!
//! On client reconnect, the consumer supports sequence-based replay:
//! the client sends `last_seq` and receives all missed messages
//! with the `replayed: true` flag before live delivery resumes.

pub mod nats;
