//! Presence tracking via NATS KV bucket `TC_PRESENCE`.
//!
//! On subscribe: writes `{stream}.{user_id}` into the `TC_PRESENCE` bucket
//! with a 30-second TTL (enforced by the bucket's `max_age`).
//! On disconnect/unsubscribe: deletes the key (best-effort).
//! Heartbeat: a background task refreshes the key every 25 seconds so
//! long-lived connections are not incorrectly evicted.
//!
//! ## Key Layout
//! ```text
//! Bucket : TC_PRESENCE
//! Key    : {stream_name}.{user_id}
//! Value  : b"1"   (presence marker, content is not meaningful)
//! ```

use std::sync::Arc;
use std::time::Duration;

use crate::errors::GatewayError;

/// NATS KV bucket that holds all presence entries.
const PRESENCE_BUCKET: &str = "TC_PRESENCE";

/// Seconds before a presence key expires if not refreshed.
const PRESENCE_TTL_SECS: u64 = 30;

/// How often the heartbeat task re-writes a key to extend its TTL.
/// Must be strictly less than [`PRESENCE_TTL_SECS`].
const HEARTBEAT_INTERVAL_SECS: u64 = 25;

/// Manages presence state in the `TC_PRESENCE` NATS KV bucket.
///
/// Cheap to clone — all clones share the same underlying KV handle.
#[derive(Clone)]
pub struct PresenceManager {
    kv: async_nats::jetstream::kv::Store,
}

impl PresenceManager {
    /// Connects to NATS and opens (or creates) the `TC_PRESENCE` KV bucket.
    ///
    /// The bucket is created with `max_age = 30s` so entries expire
    /// automatically if the gateway crashes without deleting them.
    pub async fn connect(nats_url: &str) -> Result<Arc<Self>, GatewayError> {
        let client = async_nats::connect(nats_url).await.map_err(|e| {
            GatewayError::JetStream(format!("NATS connect for presence failed: {e}"))
        })?;

        let jetstream = async_nats::jetstream::new(client);

        // Try to get an existing bucket first; create it if it doesn't exist yet.
        let kv = match jetstream.get_key_value(PRESENCE_BUCKET).await {
            Ok(store) => store,
            Err(_) => jetstream
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket: PRESENCE_BUCKET.to_string(),
                    history: 1,
                    max_age: Duration::from_secs(PRESENCE_TTL_SECS),
                    ..Default::default()
                })
                .await
                .map_err(|e| {
                    GatewayError::JetStream(format!("failed to create TC_PRESENCE bucket: {e}"))
                })?,
        };

        tracing::info!(
            bucket = PRESENCE_BUCKET,
            ttl_secs = PRESENCE_TTL_SECS,
            "presence KV bucket ready"
        );

        Ok(Arc::new(Self { kv }))
    }

    /// Writes the presence key for `(stream, user_id)`.
    ///
    /// The key expires automatically after [`PRESENCE_TTL_SECS`] seconds unless
    /// refreshed by the heartbeat task.
    pub async fn put(&self, stream: &str, user_id: &str) {
        let key = presence_key(stream, user_id);
        if let Err(e) = self.kv.put(&key, bytes::Bytes::from_static(b"1")).await {
            tracing::warn!(key, error = %e, "presence put failed");
        }
    }

    /// Deletes the presence key for `(stream, user_id)`.
    ///
    /// Best-effort: errors are logged at debug level and not propagated,
    /// because the key expires naturally on its own within [`PRESENCE_TTL_SECS`].
    pub async fn delete(&self, stream: &str, user_id: &str) {
        let key = presence_key(stream, user_id);
        if let Err(e) = self.kv.delete(&key).await {
            tracing::debug!(key, error = %e, "presence delete failed (key will expire naturally)");
        }
    }

    /// Starts a background heartbeat that re-writes the presence key every
    /// [`HEARTBEAT_INTERVAL_SECS`] seconds to prevent premature expiry.
    ///
    /// Returns a [`HeartbeatHandle`] whose `Drop` implementation aborts the task.
    /// The caller is responsible for calling [`Self::delete`] after dropping the handle.
    pub fn start_heartbeat(self: &Arc<Self>, stream: String, user_id: String) -> HeartbeatHandle {
        let manager = Arc::clone(self);
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
            interval.tick().await; // skip first tick — key was just written by the caller
            loop {
                interval.tick().await;
                manager.put(&stream, &user_id).await;
                tracing::debug!(stream, user_id, "presence heartbeat refreshed");
            }
        });
        HeartbeatHandle { handle }
    }
}

/// Formats the NATS KV key for a presence entry.
fn presence_key(stream: &str, user_id: &str) -> String {
    format!("{stream}.{user_id}")
}

/// RAII guard for a presence heartbeat task.
///
/// Aborting the underlying `JoinHandle` on drop ensures the heartbeat stops
/// when the subscription is removed or the connection closes.
pub struct HeartbeatHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presence_key_format() {
        assert_eq!(
            presence_key("chat_room_1", "user_42"),
            "chat_room_1.user_42"
        );
        assert_eq!(
            presence_key("notifications", "alice"),
            "notifications.alice"
        );
    }

    #[test]
    fn heartbeat_interval_less_than_ttl() {
        const _: () = assert!(HEARTBEAT_INTERVAL_SECS < PRESENCE_TTL_SECS);
    }
}
