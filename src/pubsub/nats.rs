//! NATS JetStream consumer: push-style message delivery, auto-reconnect, and replay.
//!
//! ## Architecture
//!
//! The [`NatsConsumer`] maintains a durable pull consumer on the `TURBOCABLE` stream.
//! Messages arrive continuously via `consumer.messages()`, are decoded and pre-encoded
//! for both JSON and MessagePack codecs, then fanned out through the connection registry.
//!
//! The consumer names itself `gw_{node_id}` so each gateway node tracks its own
//! delivery cursor independently. If a gateway restarts, it resumes from the last
//! acknowledged position — no messages are lost.
//!
//! ## Reconnection
//!
//! An outer loop wraps the consumer: on any NATS error the consumer sleeps 2 seconds
//! and reconnects. The `async-nats` client handles TCP-level reconnection automatically;
//! the outer loop covers JetStream-level failures (stream deleted, consumer expired, etc.).
//!
//! ## Replay
//!
//! When a client reconnects and sends `{ "type": "hello", "last_seq": "8841" }`,
//! the handler calls [`NatsConsumer::replay_since`] for each stream the client
//! subscribes to. An ephemeral consumer with `DeliverPolicy::ByStartSequence`
//! fetches missed messages in order, delivered with `replayed: true`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;

use crate::connection::registry::Registry;
use crate::errors::GatewayError;
use crate::metrics::Metrics;
use crate::protocol::types::ServerMessage;

/// NATS subject prefix: all TurboCable broadcasts live under `TURBOCABLE.{stream_name}`.
const SUBJECT_PREFIX: &str = "TURBOCABLE.";

/// JetStream stream name that holds all broadcast messages.
const STREAM_NAME: &str = "TURBOCABLE";

/// Maximum number of messages replayed per stream during client reconnect.
const MAX_REPLAY_MESSAGES: usize = 10_000;

/// Timeout when waiting for replay messages from JetStream.
const REPLAY_FETCH_TIMEOUT: Duration = Duration::from_secs(3);

/// Delay before retrying after a NATS consumer error.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// A message fetched from JetStream for replay delivery.
pub struct ReplayMessage {
    /// JetStream stream sequence number.
    pub sequence: u64,
    /// Raw message payload bytes (JSON or MessagePack from the publisher).
    pub payload: Bytes,
}

/// NATS JetStream consumer that drives the broadcast fan-out pipeline.
///
/// Holds a persistent NATS client and JetStream context. The consumer loop
/// and replay operations create their own consumer subscriptions as needed.
pub struct NatsConsumer {
    client: async_nats::Client,
    jetstream: async_nats::jetstream::Context,
}

impl NatsConsumer {
    /// Connects to NATS, creates (or verifies) the `TURBOCABLE` JetStream stream,
    /// and returns a ready-to-use consumer.
    ///
    /// The stream is configured with:
    /// - File-backed storage for durability
    /// - 7-day message retention
    /// - Configurable replica count (1 for dev, 3 for production)
    pub async fn connect(nats_url: &str, num_replicas: usize) -> Result<Self, GatewayError> {
        let client = async_nats::connect(nats_url)
            .await
            .map_err(|e| GatewayError::JetStream(format!("NATS connect failed: {e}")))?;

        let jetstream = async_nats::jetstream::new(client.clone());

        // Ensure the TURBOCABLE stream exists with the desired configuration.
        // get_or_create_stream is idempotent: if the stream exists with a compatible
        // config, it returns the existing stream.
        jetstream
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: STREAM_NAME.to_string(),
                subjects: vec![format!("{SUBJECT_PREFIX}>")],
                storage: async_nats::jetstream::stream::StorageType::File,
                max_age: Duration::from_secs(7 * 24 * 60 * 60),
                num_replicas,
                ..Default::default()
            })
            .await
            .map_err(|e| GatewayError::JetStream(format!("stream create/get failed: {e}")))?;

        tracing::info!(
            stream = STREAM_NAME,
            replicas = num_replicas,
            "NATS JetStream stream ready"
        );

        Ok(Self { client, jetstream })
    }

    /// Spawns the background fan-out loop that continuously pulls messages
    /// from NATS JetStream and delivers them to WebSocket subscribers.
    ///
    /// The loop auto-reconnects on any error with a 2-second backoff.
    /// This method returns immediately; the loop runs until the process exits.
    pub fn start_fanout_loop(
        self: &Arc<Self>,
        node_id: String,
        registry: Arc<Registry>,
        max_ack_pending: i64,
        metrics: Arc<Metrics>,
    ) {
        let consumer = Arc::clone(self);
        tokio::spawn(async move {
            consumer
                .fanout_loop(&node_id, &registry, max_ack_pending, &metrics)
                .await;
        });
    }

    /// Publishes a client-originated message to NATS JetStream.
    ///
    /// Subject: `TURBOCABLE.{stream_name}` — the message enters the same stream
    /// as Rails-published broadcasts, so all subscribers receive it.
    /// Awaits the JetStream publish acknowledgment for guaranteed persistence.
    pub async fn publish(&self, stream_name: &str, payload: Bytes) -> Result<(), GatewayError> {
        let subject = format!("{SUBJECT_PREFIX}{stream_name}");
        self.jetstream
            .publish(subject, payload)
            .await
            .map_err(|e| GatewayError::JetStream(format!("publish failed: {e}")))?
            .await
            .map_err(|e| GatewayError::JetStream(format!("publish ack failed: {e}")))?;
        Ok(())
    }

    /// Fetches messages from JetStream for a specific stream starting after `last_seq`.
    ///
    /// Used during client reconnection: the client tells us the last sequence it
    /// received, and we replay everything since then for the requested stream.
    /// Returns up to [`MAX_REPLAY_MESSAGES`] messages in stream-sequence order.
    pub async fn replay_since(
        &self,
        stream_name: &str,
        last_seq: u64,
    ) -> Result<Vec<ReplayMessage>, GatewayError> {
        let subject = format!("{SUBJECT_PREFIX}{stream_name}");

        let stream = self
            .jetstream
            .get_stream(STREAM_NAME)
            .await
            .map_err(|e| GatewayError::JetStream(format!("get stream for replay: {e}")))?;

        // Ephemeral consumer: no durable_name, auto-deleted after inactive_threshold.
        // Starts from the sequence after the client's last-known position.
        let config = async_nats::jetstream::consumer::pull::Config {
            filter_subject: subject,
            deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::ByStartSequence {
                start_sequence: last_seq + 1,
            },
            ack_policy: async_nats::jetstream::consumer::AckPolicy::None,
            inactive_threshold: Duration::from_secs(30),
            ..Default::default()
        };

        let consumer = stream
            .create_consumer(config)
            .await
            .map_err(|e| GatewayError::JetStream(format!("create replay consumer: {e}")))?;

        let mut messages = Vec::new();
        let mut msg_stream = consumer
            .messages()
            .await
            .map_err(|e| GatewayError::JetStream(format!("replay messages stream: {e}")))?;

        // Pull messages with a timeout — when no more messages arrive within
        // REPLAY_FETCH_TIMEOUT, we consider replay complete.
        while messages.len() < MAX_REPLAY_MESSAGES {
            match tokio::time::timeout(REPLAY_FETCH_TIMEOUT, msg_stream.next()).await {
                Ok(Some(Ok(msg))) => {
                    let sequence = extract_stream_sequence(&msg);
                    messages.push(ReplayMessage {
                        sequence,
                        payload: msg.payload.clone(),
                    });
                }
                Ok(Some(Err(e))) => {
                    tracing::warn!(
                        stream = stream_name,
                        error = %e,
                        "error during replay fetch"
                    );
                    break;
                }
                // Stream ended or timeout — replay is complete.
                Ok(None) | Err(_) => break,
            }
        }

        tracing::info!(
            stream = stream_name,
            last_seq,
            replayed = messages.len(),
            "replay complete"
        );

        Ok(messages)
    }

    /// Flushes all pending outbound NATS data and waits for the server to acknowledge.
    ///
    /// Called during graceful shutdown to ensure all acks and publishes are delivered
    /// before the process exits. Blocks until the flush completes or an error occurs.
    pub async fn flush(&self) -> Result<(), GatewayError> {
        self.client
            .flush()
            .await
            .map_err(|e| GatewayError::JetStream(format!("flush: {e}")))?;
        Ok(())
    }

    /// Returns `true` if the underlying NATS client is still connected.
    pub fn is_connected(&self) -> bool {
        self.client.connection_state() == async_nats::connection::State::Connected
    }

    /// Outer reconnection loop: re-creates the consumer on any error.
    async fn fanout_loop(
        &self,
        node_id: &str,
        registry: &Registry,
        max_ack_pending: i64,
        metrics: &Metrics,
    ) {
        loop {
            match self
                .run_consumer(node_id, registry, max_ack_pending, metrics)
                .await
            {
                Ok(()) => {
                    tracing::warn!("NATS consumer stream ended, reconnecting");
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "NATS consumer error, reconnecting in {}s",
                        RECONNECT_DELAY.as_secs()
                    );
                }
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    /// Creates (or resumes) the durable consumer and processes messages
    /// until the stream ends or an unrecoverable error occurs.
    async fn run_consumer(
        &self,
        node_id: &str,
        registry: &Registry,
        max_ack_pending: i64,
        metrics: &Metrics,
    ) -> Result<(), GatewayError> {
        let stream = self
            .jetstream
            .get_stream(STREAM_NAME)
            .await
            .map_err(|e| GatewayError::JetStream(format!("get stream: {e}")))?;

        let consumer_name = format!("gw_{node_id}");
        let mut consumer = stream
            .get_or_create_consumer(
                &consumer_name,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(consumer_name.clone()),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    max_ack_pending,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| GatewayError::JetStream(format!("create/get consumer: {e}")))?;

        tracing::info!(
            consumer = %consumer_name,
            max_ack_pending,
            "NATS durable consumer started"
        );

        update_consumer_lag(&mut consumer, metrics).await;

        let mut messages = consumer
            .messages()
            .await
            .map_err(|e| GatewayError::JetStream(format!("messages stream: {e}")))?;

        let mut processed: u64 = 0;

        while let Some(result) = messages.next().await {
            let msg = result
                .map_err(|e| GatewayError::JetStream(format!("consumer message error: {e}")))?;

            process_nats_message(&msg, registry, metrics);

            if let Err(e) = msg.ack().await {
                tracing::warn!(error = %e, "NATS ack failed");
            }

            processed += 1;

            // Refresh consumer lag gauge every 10k messages.
            if processed % 10_000 == 0 {
                update_consumer_lag(&mut consumer, metrics).await;
            }
        }

        Ok(())
    }
}

/// Strips the `TURBOCABLE.` prefix from a NATS subject to extract the stream name
/// that maps to the registry's subscription key.
///
/// Example: `"TURBOCABLE.chat_room_42"` → `"chat_room_42"`
fn extract_stream_name(subject: &str) -> &str {
    subject.strip_prefix(SUBJECT_PREFIX).unwrap_or(subject)
}

/// Extracts the JetStream stream sequence number from a consumer message.
/// Falls back to 0 if the info cannot be parsed (should not happen in practice).
fn extract_stream_sequence(msg: &async_nats::jetstream::Message) -> u64 {
    msg.info().map(|info| info.stream_sequence).unwrap_or(0)
}

/// Decodes a NATS message, pre-encodes it for both wire formats, fans out
/// to all WebSocket subscribers, and records fan-out metrics.
fn process_nats_message(
    msg: &async_nats::jetstream::Message,
    registry: &Registry,
    metrics: &Metrics,
) {
    let subject = msg.subject.as_str();
    let stream_name = extract_stream_name(subject);
    let sequence = extract_stream_sequence(msg);

    // Parse the payload as JSON first, falling back to MessagePack, then null.
    // Rails may publish either format depending on configuration.
    let payload: serde_json::Value = serde_json::from_slice(&msg.payload)
        .or_else(|_| rmp_serde::from_slice(&msg.payload))
        .unwrap_or(serde_json::Value::Null);

    let server_msg = ServerMessage::Message {
        identifier: stream_name.to_string(),
        message: payload,
        replayed: None,
        seq: Some(sequence),
    };

    // Pre-encode once per codec — fanout_encoded routes by connection type.
    let json_bytes = serde_json::to_vec(&server_msg)
        .map(Bytes::from)
        .unwrap_or_default();
    let msgpack_bytes = rmp_serde::to_vec_named(&server_msg)
        .map(Bytes::from)
        .unwrap_or_default();

    let t0 = Instant::now();
    let result = registry.fanout_encoded(stream_name, json_bytes, msgpack_bytes);
    metrics
        .fanout_duration_secs
        .observe(t0.elapsed().as_secs_f64());

    if result.sent > 0 {
        metrics.messages_fanned_out.inc_by(result.sent as u64);
    }
    if result.dropped > 0 {
        metrics.messages_dropped.inc_by(result.dropped as u64);
        tracing::debug!(
            stream = stream_name,
            seq = sequence,
            sent = result.sent,
            dropped = result.dropped,
            "fan-out completed with slow-client drops"
        );
    }
}

/// Fetches consumer info and updates the `nats_consumer_lag` Prometheus gauge.
async fn update_consumer_lag(
    consumer: &mut async_nats::jetstream::consumer::Consumer<
        async_nats::jetstream::consumer::pull::Config,
    >,
    metrics: &Metrics,
) {
    match consumer.info().await {
        Ok(info) => {
            metrics.nats_consumer_lag.set(info.num_pending as i64);
            tracing::debug!(
                num_pending = info.num_pending,
                num_ack_pending = info.num_ack_pending,
                "nats_consumer_lag updated"
            );
        }
        Err(e) => {
            tracing::debug!(error = %e, "failed to fetch consumer info for lag check");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_stream_name_strips_prefix() {
        assert_eq!(
            extract_stream_name("TURBOCABLE.chat_room_42"),
            "chat_room_42"
        );
        assert_eq!(
            extract_stream_name("TURBOCABLE.notifications"),
            "notifications"
        );
        assert_eq!(extract_stream_name("TURBOCABLE.a.b.c"), "a.b.c");
    }

    #[test]
    fn extract_stream_name_handles_missing_prefix() {
        assert_eq!(extract_stream_name("chat_room_42"), "chat_room_42");
        assert_eq!(extract_stream_name(""), "");
    }

    #[test]
    fn build_server_message_from_json_payload() {
        let payload = br#"{"text":"hello","sender":"alice"}"#;
        let parsed: serde_json::Value = serde_json::from_slice(payload).unwrap();

        let msg = ServerMessage::Message {
            identifier: "chat_room_1".to_string(),
            message: parsed,
            replayed: None,
            seq: Some(42),
        };

        let encoded = serde_json::to_string(&msg).unwrap();
        assert!(encoded.contains("chat_room_1"));
        assert!(encoded.contains("hello"));
        assert!(encoded.contains("\"seq\":42"));
        assert!(!encoded.contains("replayed"));
    }

    #[test]
    fn build_replayed_server_message() {
        let msg = ServerMessage::Message {
            identifier: "stream_x".to_string(),
            message: serde_json::json!({"data": "replayed"}),
            replayed: Some(true),
            seq: Some(8841),
        };

        let encoded = serde_json::to_string(&msg).unwrap();
        assert!(encoded.contains("\"replayed\":true"));
        assert!(encoded.contains("\"seq\":8841"));
    }

    #[test]
    fn server_message_without_optional_fields_omits_them() {
        let msg = ServerMessage::Message {
            identifier: "stream_y".to_string(),
            message: serde_json::json!({"data": "live"}),
            replayed: None,
            seq: None,
        };

        let encoded = serde_json::to_string(&msg).unwrap();
        assert!(!encoded.contains("replayed"));
        assert!(!encoded.contains("seq"));
    }

    #[test]
    fn dual_codec_encoding_produces_valid_output() {
        let msg = ServerMessage::Message {
            identifier: "chat_1".to_string(),
            message: serde_json::json!({"text": "hello"}),
            replayed: None,
            seq: Some(100),
        };

        let json_bytes = serde_json::to_vec(&msg).unwrap();
        let msgpack_bytes = rmp_serde::to_vec_named(&msg).unwrap();

        assert!(!json_bytes.is_empty());
        assert!(!msgpack_bytes.is_empty());

        // Verify round-trip: both encodings should decode to the same message.
        let from_json: ServerMessage = serde_json::from_slice(&json_bytes).unwrap();
        let from_msgpack: ServerMessage = rmp_serde::from_slice(&msgpack_bytes).unwrap();
        assert_eq!(from_json, from_msgpack);
    }

    #[test]
    fn constants_are_consistent() {
        assert!(SUBJECT_PREFIX.starts_with(STREAM_NAME));
        assert!(SUBJECT_PREFIX.ends_with('.'));
    }
}
