//! Lock-free connection registry for million-scale WebSocket fan-out.

use bytes::Bytes;
use dashmap::DashMap;
use smallvec::SmallVec;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

/// Per-connection metadata stored in the registry alongside the outbound sender.
pub(crate) struct ConnectionEntry {
    pub sender: mpsc::Sender<Bytes>,
    /// `true` when the connection uses the binary MessagePack codec;
    /// `false` for the default JSON (ActionCable-compatible) codec.
    pub is_binary: bool,
}

/// Result of a fan-out operation to all subscribers of a stream.
pub struct FanoutResult {
    /// Number of subscribers that accepted the message.
    pub sent: usize,
    /// Number of subscribers whose channel was full (back-pressure).
    pub dropped: usize,
}

/// Thread-safe connection registry backed by sharded DashMaps.
///
/// Designed to hold 1M+ connections with lock-free reads on the fanout hot path.
/// Uses a reverse map (`conn_streams`) so deregister can clean up all stream
/// subscriptions in O(streams_per_connection) rather than scanning every stream.
pub struct Registry {
    connections: DashMap<u64, ConnectionEntry>,
    streams: DashMap<String, SmallVec<[u64; 32]>>,
    conn_streams: DashMap<u64, SmallVec<[String; 8]>>,
    next_id: AtomicU64,
    active: AtomicU64,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    /// Creates a new registry pre-allocated for ~1.1M connections.
    pub fn new() -> Self {
        Self {
            connections: DashMap::with_capacity(1_100_000),
            streams: DashMap::new(),
            conn_streams: DashMap::with_capacity(1_100_000),
            next_id: AtomicU64::new(1),
            active: AtomicU64::new(0),
        }
    }

    /// Returns a monotonically increasing connection ID.
    pub fn allocate_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Registers a new connection with its outbound sender channel.
    ///
    /// `is_binary` indicates whether the connection uses MessagePack (`true`)
    /// or JSON (`false`) encoding, which determines which pre-encoded payload
    /// is delivered during NATS fan-out.
    pub fn register(&self, conn_id: u64, sender: mpsc::Sender<Bytes>, is_binary: bool) {
        self.connections
            .insert(conn_id, ConnectionEntry { sender, is_binary });
        self.conn_streams.insert(conn_id, SmallVec::new());
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    /// Removes a connection and cleans up all its stream subscriptions.
    pub fn deregister(&self, conn_id: u64) {
        if let Some((_, stream_list)) = self.conn_streams.remove(&conn_id) {
            for stream in stream_list {
                self.remove_subscriber_from_stream(&stream, conn_id);
            }
        }
        if self.connections.remove(&conn_id).is_some() {
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Subscribes a connection to a named stream (idempotent).
    pub fn subscribe(&self, conn_id: u64, stream: &str) {
        if let Some(mut conn_streams) = self.conn_streams.get_mut(&conn_id) {
            if conn_streams.iter().any(|s| s == stream) {
                return;
            }
            self.streams
                .entry(stream.to_owned())
                .or_default()
                .push(conn_id);
            conn_streams.push(stream.to_owned());
        }
    }

    /// Removes a connection from a stream's subscriber list.
    pub fn unsubscribe(&self, conn_id: u64, stream: &str) {
        self.remove_subscriber_from_stream(stream, conn_id);
        if let Some(mut conn_streams) = self.conn_streams.get_mut(&conn_id) {
            conn_streams.retain(|s| s != stream);
        }
    }

    /// Fan-out a single payload to all subscribers of a stream.
    ///
    /// Hot path — no heap allocations, `try_send` only.
    /// Sends the same `payload` to every subscriber regardless of codec type.
    /// Use [`fanout_encoded`](Self::fanout_encoded) when different encodings
    /// are needed for JSON vs MessagePack connections (NATS consumer path).
    #[cfg(test)]
    pub fn fanout(&self, stream: &str, payload: Bytes) -> FanoutResult {
        self.fanout_encoded(stream, payload.clone(), payload)
    }

    /// Fan-out with separate pre-encoded payloads for JSON and MessagePack connections.
    ///
    /// This is the hot path used by the NATS consumer: each message is pre-encoded
    /// once per codec, then routed to the correct connections based on their codec type.
    /// No per-connection encoding happens here — only `Bytes::clone` (one atomic increment).
    pub fn fanout_encoded(
        &self,
        stream: &str,
        json_payload: Bytes,
        binary_payload: Bytes,
    ) -> FanoutResult {
        let mut sent = 0;
        let mut dropped = 0;

        if let Some(subscribers) = self.streams.get(stream) {
            for &conn_id in subscribers.iter() {
                if let Some(entry) = self.connections.get(&conn_id) {
                    let payload = if entry.is_binary {
                        binary_payload.clone()
                    } else {
                        json_payload.clone()
                    };
                    match entry.sender.try_send(payload) {
                        Ok(()) => sent += 1,
                        Err(_) => dropped += 1,
                    }
                }
            }
        }

        FanoutResult { sent, dropped }
    }

    /// Returns the current number of active connections.
    pub fn connection_count(&self) -> u64 {
        self.active.load(Ordering::Relaxed)
    }

    fn remove_subscriber_from_stream(&self, stream: &str, conn_id: u64) {
        let should_remove = if let Some(mut subscribers) = self.streams.get_mut(stream) {
            subscribers.retain(|id| *id != conn_id);
            subscribers.is_empty()
        } else {
            false
        };

        if should_remove {
            self.streams.remove_if(stream, |_, v| v.is_empty());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_registry() -> Registry {
        Registry {
            connections: DashMap::new(),
            streams: DashMap::new(),
            conn_streams: DashMap::new(),
            next_id: AtomicU64::new(1),
            active: AtomicU64::new(0),
        }
    }

    #[tokio::test]
    async fn register_and_fanout_to_1000() {
        let reg = test_registry();
        let mut receivers = Vec::with_capacity(1000);

        for _ in 0..1000 {
            let id = reg.allocate_id();
            let (tx, rx) = mpsc::channel(16);
            reg.register(id, tx, false);
            reg.subscribe(id, "chat_room_42");
            receivers.push((id, rx));
        }

        assert_eq!(reg.connection_count(), 1000);

        let payload = Bytes::from_static(b"hello");
        let result = reg.fanout("chat_room_42", payload.clone());
        assert_eq!(result.sent, 1000);
        assert_eq!(result.dropped, 0);

        for (_, mut rx) in receivers {
            let msg = rx.try_recv().expect("should have received message");
            assert_eq!(msg, payload);
        }
    }

    #[tokio::test]
    async fn slow_client_does_not_block_fanout() {
        let reg = test_registry();
        let mut receivers = Vec::with_capacity(10);

        for _ in 0..10 {
            let id = reg.allocate_id();
            let (tx, rx) = mpsc::channel(1);
            reg.register(id, tx, false);
            reg.subscribe(id, "stream_a");
            receivers.push((id, rx));
        }

        // Fill every channel so the next fanout will find them full.
        let filler = Bytes::from_static(b"fill");
        reg.fanout("stream_a", filler);

        let result = reg.fanout("stream_a", Bytes::from_static(b"overflow"));
        assert_eq!(result.dropped, 10);
        assert_eq!(result.sent, 0);

        // Drain one receiver and retry — that client should now succeed.
        let _ = receivers[0].1.try_recv();
        let result = reg.fanout("stream_a", Bytes::from_static(b"retry"));
        assert_eq!(result.sent, 1);
        assert_eq!(result.dropped, 9);
    }

    #[tokio::test]
    async fn deregister_cleans_up_from_all_streams() {
        let reg = test_registry();
        let id = reg.allocate_id();
        let (tx, _rx) = mpsc::channel(16);
        reg.register(id, tx, false);

        reg.subscribe(id, "stream_x");
        reg.subscribe(id, "stream_y");
        reg.subscribe(id, "stream_z");

        reg.deregister(id);

        assert_eq!(reg.connection_count(), 0);
        assert!(!reg.connections.contains_key(&id));
        assert!(!reg.conn_streams.contains_key(&id));

        // All stream entries should have been removed (they had only this subscriber).
        assert!(!reg.streams.contains_key("stream_x"));
        assert!(!reg.streams.contains_key("stream_y"));
        assert!(!reg.streams.contains_key("stream_z"));

        // Fanout to those streams should be a no-op.
        let result = reg.fanout("stream_x", Bytes::from_static(b"nope"));
        assert_eq!(result.sent, 0);
        assert_eq!(result.dropped, 0);
    }

    #[tokio::test]
    async fn concurrent_subscribe_from_multiple_tasks() {
        let reg = Arc::new(test_registry());
        let mut handles = Vec::new();

        for _ in 0..100 {
            let reg = Arc::clone(&reg);
            handles.push(tokio::spawn(async move {
                let id = reg.allocate_id();
                let (tx, rx) = mpsc::channel(16);
                reg.register(id, tx, false);
                reg.subscribe(id, "shared_stream");
                (id, rx)
            }));
        }

        let results: Vec<(u64, mpsc::Receiver<Bytes>)> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(reg.connection_count(), 100);

        let result = reg.fanout("shared_stream", Bytes::from_static(b"broadcast"));
        assert_eq!(result.sent, 100);

        let ids: Vec<u64> = results.into_iter().map(|(id, _)| id).collect();

        // Concurrently deregister all connections.
        let mut handles = Vec::new();
        for id in ids {
            let reg = Arc::clone(&reg);
            handles.push(tokio::spawn(async move {
                reg.deregister(id);
            }));
        }
        futures::future::join_all(handles).await;

        assert_eq!(reg.connection_count(), 0);
        assert!(!reg.streams.contains_key("shared_stream"));
    }

    #[tokio::test]
    async fn connection_count_after_concurrent_register_deregister() {
        let reg = Arc::new(test_registry());
        let mut handles = Vec::new();

        for _ in 0..500 {
            let reg = Arc::clone(&reg);
            handles.push(tokio::spawn(async move {
                let id = reg.allocate_id();
                let (tx, _rx) = mpsc::channel(1);
                reg.register(id, tx, false);
                id
            }));
        }

        let ids: Vec<u64> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(reg.connection_count(), 500);

        // Deregister half.
        let mut handles = Vec::new();
        for &id in &ids[..250] {
            let reg = Arc::clone(&reg);
            handles.push(tokio::spawn(async move {
                reg.deregister(id);
            }));
        }
        futures::future::join_all(handles).await;

        assert_eq!(reg.connection_count(), 250);
    }

    #[tokio::test]
    async fn unsubscribe_removes_from_fanout() {
        let reg = test_registry();
        let id = reg.allocate_id();
        let (tx, mut rx) = mpsc::channel(16);
        reg.register(id, tx, false);
        reg.subscribe(id, "news");

        let result = reg.fanout("news", Bytes::from_static(b"msg1"));
        assert_eq!(result.sent, 1);
        let _ = rx.try_recv().unwrap();

        reg.unsubscribe(id, "news");

        let result = reg.fanout("news", Bytes::from_static(b"msg2"));
        assert_eq!(result.sent, 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn duplicate_subscribe_is_idempotent() {
        let reg = test_registry();
        let id = reg.allocate_id();
        let (tx, mut rx) = mpsc::channel(16);
        reg.register(id, tx, false);

        reg.subscribe(id, "dup_stream");
        reg.subscribe(id, "dup_stream");
        reg.subscribe(id, "dup_stream");

        let result = reg.fanout("dup_stream", Bytes::from_static(b"once"));
        assert_eq!(result.sent, 1);
        assert_eq!(rx.try_recv().unwrap(), Bytes::from_static(b"once"));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn deregister_is_idempotent() {
        let reg = test_registry();
        let id = reg.allocate_id();
        let (tx, _rx) = mpsc::channel(16);
        reg.register(id, tx, false);
        reg.subscribe(id, "stream_q");

        reg.deregister(id);
        assert_eq!(reg.connection_count(), 0);

        reg.deregister(id);
        assert_eq!(reg.connection_count(), 0);
    }

    #[tokio::test]
    async fn fanout_to_nonexistent_stream_is_noop() {
        let reg = test_registry();
        let result = reg.fanout("does_not_exist", Bytes::from_static(b"void"));
        assert_eq!(result.sent, 0);
        assert_eq!(result.dropped, 0);
    }

    #[tokio::test]
    async fn register_and_fanout_to_10000() {
        let reg = test_registry();
        let mut receivers = Vec::with_capacity(10_000);

        for _ in 0..10_000 {
            let id = reg.allocate_id();
            let (tx, rx) = mpsc::channel(16);
            reg.register(id, tx, false);
            reg.subscribe(id, "large_stream");
            receivers.push(rx);
        }

        assert_eq!(reg.connection_count(), 10_000);

        let payload = Bytes::from_static(b"10k-fanout");
        let result = reg.fanout("large_stream", payload.clone());
        assert_eq!(result.sent, 10_000);
        assert_eq!(result.dropped, 0);

        for mut rx in receivers {
            assert_eq!(rx.try_recv().unwrap(), payload);
        }
    }

    // --- fanout_encoded tests (dual-codec support) ---

    #[tokio::test]
    async fn fanout_encoded_routes_by_codec() {
        let reg = test_registry();
        let json_id = reg.allocate_id();
        let binary_id = reg.allocate_id();

        let (json_tx, mut json_rx) = mpsc::channel(16);
        let (bin_tx, mut bin_rx) = mpsc::channel(16);

        reg.register(json_id, json_tx, false);
        reg.register(binary_id, bin_tx, true);
        reg.subscribe(json_id, "mixed");
        reg.subscribe(binary_id, "mixed");

        let json_payload = Bytes::from_static(b"json_frame");
        let binary_payload = Bytes::from_static(b"msgpack_frame");

        let result = reg.fanout_encoded("mixed", json_payload.clone(), binary_payload.clone());
        assert_eq!(result.sent, 2);
        assert_eq!(result.dropped, 0);

        assert_eq!(json_rx.try_recv().unwrap(), json_payload);
        assert_eq!(bin_rx.try_recv().unwrap(), binary_payload);
    }

    #[tokio::test]
    async fn fanout_encoded_all_json_connections() {
        let reg = test_registry();
        let mut receivers = Vec::new();

        for _ in 0..5 {
            let id = reg.allocate_id();
            let (tx, rx) = mpsc::channel(16);
            reg.register(id, tx, false);
            reg.subscribe(id, "json_only");
            receivers.push(rx);
        }

        let json_payload = Bytes::from_static(b"json_data");
        let binary_payload = Bytes::from_static(b"binary_data");

        reg.fanout_encoded("json_only", json_payload.clone(), binary_payload);

        for mut rx in receivers {
            assert_eq!(rx.try_recv().unwrap(), json_payload);
        }
    }
}
