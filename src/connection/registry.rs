use bytes::Bytes;
use dashmap::DashMap;
use smallvec::SmallVec;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

pub struct FanoutResult {
    pub sent: usize,
    pub dropped: usize,
}

/// Thread-safe connection registry backed by sharded DashMaps.
///
/// Designed to hold 1M+ connections with lock-free reads on the fanout hot path.
/// Uses a reverse map (`conn_streams`) so deregister can clean up all stream
/// subscriptions in O(streams_per_connection) rather than scanning every stream.
pub struct Registry {
    senders: DashMap<u64, mpsc::Sender<Bytes>>,
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
    pub fn new() -> Self {
        Self {
            senders: DashMap::with_capacity(1_100_000),
            streams: DashMap::new(),
            conn_streams: DashMap::with_capacity(1_100_000),
            next_id: AtomicU64::new(1),
            active: AtomicU64::new(0),
        }
    }

    pub fn allocate_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn register(&self, conn_id: u64, sender: mpsc::Sender<Bytes>) {
        self.senders.insert(conn_id, sender);
        self.conn_streams.insert(conn_id, SmallVec::new());
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn deregister(&self, conn_id: u64) {
        if let Some((_, stream_list)) = self.conn_streams.remove(&conn_id) {
            for stream in stream_list {
                self.remove_subscriber_from_stream(&stream, conn_id);
            }
        }
        if self.senders.remove(&conn_id).is_some() {
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
    }

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

    pub fn unsubscribe(&self, conn_id: u64, stream: &str) {
        self.remove_subscriber_from_stream(stream, conn_id);
        if let Some(mut conn_streams) = self.conn_streams.get_mut(&conn_id) {
            conn_streams.retain(|s| s != stream);
        }
    }

    /// Hot path — no heap allocations, `try_send` only.
    /// Returns how many clients received vs. were dropped (channel full).
    pub fn fanout(&self, stream: &str, payload: Bytes) -> FanoutResult {
        let mut sent = 0;
        let mut dropped = 0;

        if let Some(subscribers) = self.streams.get(stream) {
            for &conn_id in subscribers.iter() {
                if let Some(sender) = self.senders.get(&conn_id) {
                    match sender.try_send(payload.clone()) {
                        Ok(()) => sent += 1,
                        Err(_) => dropped += 1,
                    }
                }
            }
        }

        FanoutResult { sent, dropped }
    }

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
            senders: DashMap::new(),
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
            reg.register(id, tx);
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
            reg.register(id, tx);
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
        reg.register(id, tx);

        reg.subscribe(id, "stream_x");
        reg.subscribe(id, "stream_y");
        reg.subscribe(id, "stream_z");

        reg.deregister(id);

        assert_eq!(reg.connection_count(), 0);
        assert!(!reg.senders.contains_key(&id));
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
                reg.register(id, tx);
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
                reg.register(id, tx);
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
        reg.register(id, tx);
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
        reg.register(id, tx);

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
        reg.register(id, tx);
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
            reg.register(id, tx);
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
}
