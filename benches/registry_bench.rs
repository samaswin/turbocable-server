//! Criterion benchmarks for the connection registry fan-out hot path.
//!
//! Run with:
//!   cargo bench --bench registry_bench
//!
//! Results are written to target/criterion/. Open target/criterion/report/index.html
//! for the HTML report.

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use turbocable_server::connection::registry::Registry;

// Pre-encoded payloads representative of real fan-out messages.
const JSON_PAYLOAD: &[u8] = b"{\"identifier\":\"{\\\"channel\\\":\\\"BenchmarkChannel\\\"}\",\
      \"message\":{\"seq\":1,\"sent_at\":1700000000000,\"data\":\"bench\"}}";
const MSGPACK_PAYLOAD: &[u8] =
    b"\x83\xa3seq\x01\xa7sent_at\xcf\x00\x00\x01\x8b\xd2\x8d\x60\x00\xa4data\xa5bench";

/// Fan-out to N all-JSON connections on a single stream.
fn bench_fanout_json(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout_json");
    group.sample_size(20);

    for conn_count in [1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(conn_count as u64));
        group.bench_with_input(
            BenchmarkId::new("connections", conn_count),
            &conn_count,
            |b, &n| {
                let registry = Arc::new(Registry::new());
                // Keep receivers alive so channels are open during the bench.
                let mut receivers = Vec::with_capacity(n);

                for _ in 0..n {
                    let (tx, rx) = tokio::sync::mpsc::channel(256);
                    let id = registry.allocate_id();
                    registry.register(id, tx, false);
                    registry.subscribe(id, "bench_stream");
                    receivers.push(rx);
                }

                let json = Bytes::from_static(JSON_PAYLOAD);
                let binary = Bytes::from_static(MSGPACK_PAYLOAD);

                b.iter(|| registry.fanout_encoded("bench_stream", json.clone(), binary.clone()));

                drop(receivers);
            },
        );
    }

    group.finish();
}

/// Fan-out to N connections with a 50/50 JSON / MessagePack split.
fn bench_fanout_mixed_codecs(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout_mixed_codecs");
    group.sample_size(20);

    for conn_count in [1_000usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(conn_count as u64));
        group.bench_with_input(
            BenchmarkId::new("connections", conn_count),
            &conn_count,
            |b, &n| {
                let registry = Arc::new(Registry::new());
                let mut receivers = Vec::with_capacity(n);

                for i in 0..n {
                    let (tx, rx) = tokio::sync::mpsc::channel(256);
                    let id = registry.allocate_id();
                    // Alternate: even = JSON, odd = MessagePack
                    registry.register(id, tx, i % 2 == 1);
                    registry.subscribe(id, "bench_stream");
                    receivers.push(rx);
                }

                let json = Bytes::from_static(JSON_PAYLOAD);
                let binary = Bytes::from_static(MSGPACK_PAYLOAD);

                b.iter(|| registry.fanout_encoded("bench_stream", json.clone(), binary.clone()));

                drop(receivers);
            },
        );
    }

    group.finish();
}

/// Back-pressure path: fan-out when all channel buffers are full (try_send drops).
fn bench_fanout_backpressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout_backpressure");
    group.sample_size(20);

    for conn_count in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(conn_count as u64));
        group.bench_with_input(
            BenchmarkId::new("connections", conn_count),
            &conn_count,
            |b, &n| {
                let registry = Arc::new(Registry::new());
                let mut receivers = Vec::with_capacity(n);

                for _ in 0..n {
                    // Capacity 1: fill it immediately, then every subsequent fanout drops.
                    let (tx, rx) = tokio::sync::mpsc::channel(1);
                    let id = registry.allocate_id();
                    registry.register(id, tx, false);
                    registry.subscribe(id, "bench_stream");
                    receivers.push(rx);
                }

                let json = Bytes::from_static(JSON_PAYLOAD);
                let binary = Bytes::from_static(MSGPACK_PAYLOAD);

                // Fill all channels so subsequent iterations exercise the drop path.
                registry.fanout_encoded("bench_stream", json.clone(), binary.clone());

                b.iter(|| registry.fanout_encoded("bench_stream", json.clone(), binary.clone()));

                drop(receivers);
            },
        );
    }

    group.finish();
}

/// Subscribe + deregister cost for connections with multiple stream subscriptions.
fn bench_subscribe_deregister(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscribe_deregister");
    group.sample_size(50);

    group.bench_function("1000_conns_3_streams", |b| {
        b.iter(|| {
            let registry = Registry::new();
            let mut ids = Vec::with_capacity(1_000);

            for _ in 0..1_000 {
                let (tx, _rx) = tokio::sync::mpsc::channel(16);
                let id = registry.allocate_id();
                registry.register(id, tx, false);
                registry.subscribe(id, "stream_a");
                registry.subscribe(id, "stream_b");
                registry.subscribe(id, "stream_c");
                ids.push(id);
            }

            for id in ids {
                registry.deregister(id);
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_fanout_json,
    bench_fanout_mixed_codecs,
    bench_fanout_backpressure,
    bench_subscribe_deregister,
);
criterion_main!(benches);
