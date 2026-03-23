# TurboCable Load-Testing Infrastructure

This directory contains everything needed to validate Phase 10 targets:

| Target | Metric | Goal |
|--------|--------|------|
| Single node | Max connections | 333k sustained 10 min |
| 3-node cluster | Max connections | 1M sustained 10 min |
| Fan-out latency | p99 @ 1M | < 50 ms |
| Memory | Per connection | < 8 KB |
| Message loss | Sequence gaps | 0 |

High-level guide (phases, compose stacks, prerequisites): [docs/load-testing-1m.md](../docs/load-testing-1m.md).

---

## Directory Layout

```
bench/
├── k6/
│   ├── load_1m.js           Main k6 WebSocket load test (Phase 10.1 / 10.2)
│   └── reconnect_test.js    Reconnect + replay validation (Phase 2)
└── scripts/
    ├── run_single_node.sh      Phase 10.1 — single-node test runner
    ├── run_cluster.sh          Phase 10.2 — cluster test runner (one per agent)
    ├── crash_recovery_test.sh  Phase 3   — SIGKILL + restart, verify zero data loss
    ├── memory_profile.sh       Phase 10.3 — RSS / per-connection memory monitor
    └── tune_os.sh              One-time OS tuning (fd limits, somaxconn)

benches/
└── registry_bench.rs        Criterion micro-benchmarks (in-process fanout)

src/bin/
└── publish.rs               tc-publish — NATS message publisher for fanout testing

infra/
├── docker-compose.nats.yml     3-node NATS JetStream cluster (replicas=3, FileStorage)
├── docker-compose.cluster.yml  Full stack: 3 gateways + nginx lb (needs nats compose)
├── nginx.conf                  Production nginx — SSL termination, least_conn WS LB
├── nginx-local.conf            Plain HTTP nginx — used by docker-compose.cluster.yml
├── nats/
│   ├── nats1.conf              NATS node n1 config
│   ├── nats2.conf              NATS node n2 config
│   └── nats3.conf              NATS node n3 config
├── alertmanager/
│   ├── turbocable.rules.yml    Prometheus alerting rules (lag, drop, latency)
│   └── alertmanager.yml        Alertmanager routing (PagerDuty + Slack template)
├── docker-compose.monitoring.yml  Prometheus + Alertmanager + Grafana stack
├── prometheus/
│   └── prometheus.yml          Scrape config for all 3 gateways + NATS
└── grafana/
    ├── turbocable_dashboard.json   Grafana dashboard (connections, latency, consumer lag)
    └── provisioning/
        ├── datasources/prometheus.yml
        └── dashboards/dashboards.yml
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

The apt keyserver method is unreliable on some systems. Use the direct binary download instead:

```bash
# Direct binary download (recommended)
curl -L https://github.com/grafana/k6/releases/download/v0.55.0/k6-v0.55.0-linux-amd64.tar.gz \
  -o /tmp/k6.tar.gz && \
tar -xzf /tmp/k6.tar.gz -C /tmp && \
sudo mv /tmp/k6-v0.55.0-linux-amd64/k6 /usr/local/bin/k6 && \
k6 version
```

Alternatively, use the apt repo (may fail if keyserver is unreachable):
```bash
curl -s https://dl.k6.io/key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/k6-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update && sudo apt-get install k6
```

### 4. Start the gateway with a high per-IP limit

The gateway defaults to 10 connections per IP. All k6 VUs on the same machine
share one IP (`127.0.0.1`), so you **must** raise this limit before any load test:

```bash
# Smoke test / single-node
RUST_LOG=info ./target/release/turbocable-server --port 9292 --max-connections-per-ip 5000

# Full 333k test (all connections from localhost)
RUST_LOG=info ./target/release/turbocable-server --port 9292 --max-connections-per-ip 400000

# Or via environment variable
TURBOCABLE_MAX_CONN_PER_IP=400000 ./target/release/turbocable-server --port 9292
```

Without this, the gateway will reject connections after the first 10 from the same IP
and the k6 `tc_connect_success` rate will drop to ~33%.

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

## Phase 2 — Reconnect + Replay Validation

**Target:** 100 VUs reconnect after a 5 s gap with zero sequence gaps.

```bash
# Terminal 1 — NATS
nats-server --jetstream

# Terminal 2 — Gateway (raise per-IP limit so all VUs can connect from localhost)
RUST_LOG=info ./target/release/turbocable-server --port 9292 --max-connections-per-ip 200

# Terminal 3 — tc-publish (must be running before k6 starts)
./target/release/tc-publish --stream bench --rate 1 --duration 120

# Terminal 4 — Reconnect test
k6 run bench/k6/reconnect_test.js
```

Or with custom parameters:

```bash
k6 run bench/k6/reconnect_test.js \
  -e TARGET=100 \
  -e GATEWAY_WSS_URL=ws://localhost:9292/cable \
  -e PHASE1_DURATION_S=30 \
  -e RECONNECT_GAP_S=5 \
  -e PHASE2_DURATION_S=60
```

**What it tests:**
- Each VU connects and subscribes, recording the last JetStream sequence seen.
- After 30 s all VUs disconnect simultaneously.
- After a 5 s offline gap, each VU reconnects and sends
  `{"type":"hello","last_seq":"N"}` followed by a re-subscribe.
- The server replays missed messages (`replayed: true`) before confirming the
  subscription; live messages then resume seamlessly.
- `tc_sequence_gaps` must remain `0` — any gap means a message was never
  delivered even after replay.

**Pass criteria:**
```
tc_sequence_gaps count     == 0    (no message loss after replay)
tc_connect_success rate    > 0.99
tc_reconnect_success rate  > 0.99
tc_connection_errors count < 10
```

**Custom metrics emitted:**

| Metric | Description |
|--------|-------------|
| `tc_fanout_latency_ms` | p50/p95/p99 latency for live (non-replayed) messages |
| `tc_messages_received` | Total messages received (phase 1 + replay + phase 2 live) |
| `tc_replayed_messages` | Messages delivered with `replayed=true` after reconnect |
| `tc_sequence_gaps` | Missing seqs not covered by replay (must be 0) |
| `tc_connect_success` | Rate of successful initial WS upgrades |
| `tc_reconnect_success` | Rate of successful reconnect WS upgrades |
| `tc_connection_errors` | Total connection errors across both phases |

---

## Phase 3 — Crash Recovery Test

**Target:** Zero data loss across a hard SIGKILL of the gateway mid-stream.

```bash
# Terminal 1 — NATS
nats-server --jetstream

# Terminal 2 — Run the crash recovery test (uses fixed node_id internally)
bash bench/scripts/crash_recovery_test.sh
```

Or with custom parameters:

```bash
N_MESSAGES=200 PUBLISH_RATE=5 bash bench/scripts/crash_recovery_test.sh
```

**What it tests:**
- Starts the gateway with a fixed `--node-id` (`crash-test-node` by default) so
  the durable consumer name is known: `gw_crash-test-node`.
- Publishes `N_MESSAGES` messages at `PUBLISH_RATE` msg/s via tc-publish.
- After `KILL_AFTER_S` seconds (≈ half the messages), sends SIGKILL to the gateway.
- Restarts the gateway with the **same** node-id so it finds the existing durable
  consumer and resumes from the last ACK position.
- Waits up to `WAIT_TIMEOUT_S` seconds for `nats consumer info` to report
  `num_pending = 0`.

**Pass criterion:**
```
num_pending == 0   after gateway restarts
```
This means all messages published during and after the crash window were eventually
delivered — the durable consumer replayed from its last ACK position.

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `GATEWAY_PORT` | `9292` | Gateway HTTP/WS port |
| `STREAM` | `bench` | JetStream stream name |
| `NODE_ID` | `crash-test-node` | Fixed gateway node-id (determines consumer name) |
| `N_MESSAGES` | `100` | Total messages to publish |
| `PUBLISH_RATE` | `2` | Messages per second |
| `KILL_AFTER_S` | `N/rate/2` | Seconds before SIGKILL |
| `WAIT_TIMEOUT_S` | `60` | Max seconds to wait for `num_pending=0` |

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
