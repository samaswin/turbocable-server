# TurboCable Load-Testing Infrastructure

This directory contains everything needed to validate Phase 10 targets:

| Target | Metric | Goal |
|--------|--------|------|
| Single node | Max connections | 333k sustained 10 min |
| 3-node cluster | Max connections | 1M sustained 10 min |
| Fan-out latency | p99 @ 1M | < 50 ms |
| Memory | Per connection | < 8 KB |
| Message loss | Sequence gaps | 0 |

---

## Directory Layout

```
bench/
├── k6/
│   └── load_1m.js          Main k6 WebSocket load test
└── scripts/
    ├── run_single_node.sh   Phase 10.1 — single-node test runner
    ├── run_cluster.sh       Phase 10.2 — cluster test runner (one per agent)
    ├── memory_profile.sh    Phase 10.3 — RSS / per-connection memory monitor
    └── tune_os.sh           One-time OS tuning (fd limits, somaxconn)

benches/
└── registry_bench.rs        Criterion micro-benchmarks (in-process fanout)

src/bin/
└── publish.rs               tc-publish — NATS message publisher for fanout testing
```

---

## Prerequisites

### 1. OS Tuning (every gateway node and k6 agent)

```bash
sudo bash bench/scripts/tune_os.sh
```

Raises file-descriptor limits to 2M and sets `net.core.somaxconn=65535`.

### 2. Build the publisher binary

```bash
cargo build --release --bin tc-publish
```

### 3. Install k6

Follow the [official k6 install guide](https://k6.io/docs/getting-started/installation/).

```bash
# Ubuntu / Debian
sudo gpg --no-default-keyring \
  --keyring /usr/share/keyrings/k6-archive-keyring.gpg \
  --keyserver hkp://keyserver.ubuntu.com:80 \
  --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update && sudo apt-get install k6
```

---

## Phase 10.1 — Single-Node Baseline

**Target:** 333k connections, p99 < 30 ms, < 8 KB/connection

```bash
# Quick smoke test (1000 connections)
TARGET=1000 bash bench/scripts/run_single_node.sh

# Full single-node baseline
TARGET=333000 GATEWAY_WSS_URL=ws://node1:9292/cable bash bench/scripts/run_single_node.sh
```

Or run k6 directly:

```bash
k6 run bench/k6/load_1m.js \
  -e TARGET=333000 \
  -e GATEWAY_WSS_URL=ws://node1:9292/cable
```

### Memory Profiling (Phase 10.3)

While the load test runs, monitor RSS on the gateway host:

```bash
bash bench/scripts/memory_profile.sh
```

Output:
```
Timestamp                 conns    rss(kB)    per(kB)
-----------------------------------------------------------
2024-01-15 12:00:00       333142   2548736      7.6
```

---

## Phase 10.2 — 3-Node Cluster

**Target:** 1M connections, p99 < 50 ms, zero message loss

Run `run_cluster.sh` **simultaneously** on 10 separate k6 agent machines, each targeting 100k connections. Designate exactly one agent as the publisher:

```bash
# Agent 1 (publisher)
IS_PUBLISHER=true \
TARGET=100000 \
GATEWAY_WSS_URL=wss://lb.example.com/cable \
NATS_URL=nats://nats1.example.com:4222 \
bash bench/scripts/run_cluster.sh

# Agents 2–10 (no publisher flag)
TARGET=100000 \
GATEWAY_WSS_URL=wss://lb.example.com/cable \
bash bench/scripts/run_cluster.sh
```

---

## tc-publish — Message Publisher

The `tc-publish` binary publishes messages with monotonic sequence numbers and
millisecond timestamps to `TURBOCABLE.<stream>`. The k6 clients use these to
measure end-to-end fan-out latency and detect any message loss.

```bash
# 10 msg/s for 10 minutes
./target/release/tc-publish --stream bench --rate 10 --duration 600

# 100 msg/s for 5 minutes
./target/release/tc-publish --stream bench --rate 100 --duration 300

# All options
./target/release/tc-publish --help
```

Message format (JSON):
```json
{"seq": 42, "sent_at": 1700000000123, "stream": "bench"}
```

---

## Criterion Micro-Benchmarks

Run the in-process registry fanout benchmarks (no NATS required):

```bash
cargo bench --bench registry_bench
```

Benchmarks cover:
- `fanout_json` — all-JSON clients at 1k / 10k / 100k connections
- `fanout_mixed_codecs` — 50 % JSON + 50 % MessagePack at 1k / 10k / 100k
- `fanout_backpressure` — drop path when all channel buffers are full
- `subscribe_deregister` — register + subscribe (3 streams) + deregister × 1000

Results are saved to `target/criterion/` with an HTML report.

---

## Performance Tuning (Phase 10.4)

If p99 targets are missed, check in this order:

```bash
# 1. Verify jemalloc is linked
nm target/release/turbocable-server | grep -i jemalloc

# 2. Check FD limit inside the running process
cat /proc/$(pgrep turbocable-server)/limits | grep "open files"

# 3. somaxconn
sysctl net.core.somaxconn

# 4. Reduce channel capacity if memory is over budget
#    Edit CHANNEL_CAPACITY in src/connection/handler.rs (currently 16)

# 5. DashMap shard count — try 128 for high contention
#    See src/connection/registry.rs Registry::new()

# 6. NATS max_ack_pending
#    --max-ack-pending flag or TURBOCABLE_MAX_ACK_PENDING env var
```

---

## Key Metrics (Prometheus)

| Metric | Description |
|--------|-------------|
| `turbocable_connections_active` | Current open WebSocket connections |
| `turbocable_connections_total` | Total connections since startup |
| `turbocable_connections_rejected_total` | Auth / rate-limit rejections |
| `turbocable_messages_fanned_out_total` | Messages delivered to clients |
| `turbocable_messages_dropped_total` | Messages dropped (slow clients) |
| `turbocable_fanout_duration_seconds` | Fan-out latency histogram |
| `turbocable_nats_consumer_lag` | NATS pending message backlog |

Scrape endpoint: `http://<gateway>:9292/metrics`
