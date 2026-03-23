# TurboCable — 1M Connections Plan
### What's Missing · How to Test · Tools · Reconnect & No Data Loss

---

## Status Summary

The `turbocable-server` Rust gateway is **mostly production-ready**. The core
engine — connection registry, NATS JetStream fan-out, durable consumers, JWT
auth, Prometheus metrics — is all implemented. All implementation items are complete. The gateway, test infrastructure, and
operational hardening (NATS cluster, load balancer, alerting, Grafana) are in
place. The next step is to run the phased load tests (Phase 1 → Phase 5) on
target hardware to validate the 1M connection target.

---

## What's Already Done ✅

| Area | What's in place |
|------|----------------|
| Connection registry | Lock-free `DashMap`, pre-allocated for 1.1M, < 8 KB per conn |
| NATS fan-out | Durable pull consumer `gw_{node_id}`, explicit ACKs, back-pressure |
| **Server-side no data loss** | File-backed JetStream stream (7-day retention), durable consumer resumes from last ACK position on any restart/crash |
| **Client reconnect (server half)** | `last_seq` tracked per connection; `replay_since()` creates ephemeral consumer and replays missed messages before subscription confirm |
| Graceful shutdown | Drains outbound channels, flushes NATS ACKs, sends WS `1001 Going Away` before exit |
| Dual codec | JSON (`actioncable-v1-json`) + MessagePack (`turbocable-v1-msgpack`) |
| JWT auth | RS256, hot-reloadable keys via NATS KV, glob-based stream auth |
| Per-IP rate limiting | DashMap-backed atomic limiter |
| Presence tracking | NATS KV with 30s TTL + 25s heartbeat |
| Prometheus metrics | Connections, fan-out latency histogram, consumer lag, auth duration |
| OS tuning script | `bench/scripts/tune_os.sh` — fd limits, somaxconn |
| k6 load script | `bench/k6/load_1m.js` — ramp + sustain, latency/sequence gap thresholds |
| tc-publish binary | `src/bin/publish.rs` — NATS message publisher for fan-out tests |
| Single-node test runner | `bench/scripts/run_single_node.sh` — Phase 10.1 (333k) |
| Cluster test runner | `bench/scripts/run_cluster.sh` — Phase 10.2 (1M, 10 k6 agents) |
| Criterion micro-benchmarks | `benches/registry_bench.rs` — up to 100k in-process fan-out |
| **Reconnect test (k6)** | `bench/k6/reconnect_test.js` — disconnect + reconnect + replay, zero gap validation |
| **Crash-recovery test** | `bench/scripts/crash_recovery_test.sh` — SIGKILL mid-stream, restart, verify num_pending=0 |
| **NATS 3-node cluster** | `infra/docker-compose.nats.yml` + `infra/nats/nats{1,2,3}.conf` — replicas=3, FileStorage |
| **nginx load balancer** | `infra/nginx.conf` (SSL/prod) + `infra/nginx-local.conf` (plain HTTP) — least_conn, WS upgrade, /health wired |
| **Full cluster compose** | `infra/docker-compose.cluster.yml` — 3 gateways + nginx lb, links to NATS compose |
| **Alertmanager rules** | `infra/alertmanager/turbocable.rules.yml` — consumer lag, connection drop, latency, drop rate |
| **Prometheus scrape config** | `infra/prometheus/prometheus.yml` — scrapes gw1/gw2/gw3 + NATS |
| **Monitoring stack** | `infra/docker-compose.monitoring.yml` — Prometheus + Alertmanager + Grafana |
| **Grafana dashboard** | `infra/grafana/turbocable_dashboard.json` + auto-provisioning configs |

---

## What's Missing ❌

### ~~1. No Reconnect / Crash-Recovery Test in k6~~ ✅ Done

`bench/k6/reconnect_test.js` — simulates client disconnect + reconnect with
`last_seq`, verifies server-side replay delivers all missed messages with zero
sequence gaps. See Phase 2 in `bench/README.md`.

`bench/scripts/crash_recovery_test.sh` — SIGKILL gateway mid-stream, restart
with same node-id, verify `num_pending = 0`. See Phase 3 in `bench/README.md`.

---

### ~~2. No NATS Cluster Setup for True No-Data-Loss~~ ✅ Done

`infra/docker-compose.nats.yml` brings up a 3-node JetStream cluster with
`FileStorage` and per-node persistent volumes. NATS server configs live in
`infra/nats/nats{1,2,3}.conf`.

Start the cluster and connect gateways with:
```bash
docker compose -f infra/docker-compose.nats.yml up -d

# On each gateway node
TURBOCABLE_NATS_URL=nats://localhost:4222,nats://localhost:4223,nats://localhost:4224 \
TURBOCABLE_NATS_STREAM_REPLICAS=3 \
./target/release/turbocable-server --port 9292
```

---

### ~~3. No Load Balancer Config for 1M Cluster Test~~ ✅ Done

`infra/nginx.conf` — production nginx config with SSL termination, `least_conn`
upstream across gw1/gw2/gw3, `/health` passthrough, `/metrics` internal-only,
and `proxy_read_timeout 3600s` for persistent WS connections.

`infra/nginx-local.conf` — plain HTTP variant used by `docker-compose.cluster.yml`
for local / staging.

`infra/docker-compose.cluster.yml` — brings up all 3 gateways + nginx lb in one
command (requires the NATS cluster from `docker-compose.nats.yml`).

**Note on sticky sessions:** Not needed — the gateway is fully stateless. Any
node can serve any client; NATS JetStream handles message delivery across nodes.

---

### 4. No Alerting / SLO Thresholds Documented

~~Prometheus metrics exist but there are no alerting rules.~~ ✅ Done

`infra/alertmanager/turbocable.rules.yml` — alerting rules for all key metrics.
`infra/prometheus/prometheus.yml` — scrape config for all 3 gateway nodes.
`infra/alertmanager/alertmanager.yml` — routing template (PagerDuty + Slack).
`infra/docker-compose.monitoring.yml` — Prometheus + Alertmanager + Grafana stack.

**SLO definition for the 1M test** (what counts as "pass"):
```
turbocable_connections_active (sum across nodes) >= 1,000,000
turbocable_fanout_duration_seconds p(99)         < 50 ms
turbocable_messages_dropped_total rate           == 0
turbocable_nats_consumer_lag                     < 10,000
tc_connect_success rate (k6)                     > 0.99
tc_sequence_gaps count (k6)                      == 0
```

---

### ~~5. Missing Crash-Recovery Test Script~~ ✅ Done

`bench/scripts/crash_recovery_test.sh` covers all five steps. See Phase 3 in
`bench/README.md` for usage and environment variable reference.

---

## Reconnect: How It Works (Server Side Is Done)

```
Client disconnects                  Server cleans up connection
       │                                      │
       │  [messages continue arriving         │
       │   in NATS JetStream — stored,        │
       │   not lost]                          │
       │                                      │
Client reconnects to /cable
       │
       ├─→ sends: {"type":"hello","last_seq":"8841"}
       │         (server records last_seq = 8841)
       │
       ├─→ sends: {"command":"subscribe","identifier":"chat_42"}
       │
       │   Server calls replay_since("chat_42", 8841)
       │   → ephemeral JetStream consumer, DeliverPolicy::ByStartSequence{8842}
       │   → fetches up to 10,000 missed messages
       │   → sends each with {"replayed":true,"seq":N}
       │
       └─→ sends: {"type":"confirm_subscription","identifier":"chat_42"}
           (now receiving live messages)
```

---

## No Data Loss: How It Works (Server Side Is Done)

```
Gateway crash scenario:
─────────────────────────────────────────────────────────
  t=0   Rails publishes msg #1000 → NATS JetStream stores it (file-backed)
  t=1   Gateway ACKs msg #999, processing #1000, then crashes (SIGKILL)
  t=2   msg #1000 is NOT acked — NATS holds it
  t=3   Gateway restarts → connects to NATS
  t=4   get_or_create_consumer("gw_{node_id}") → finds existing durable consumer
  t=5   Consumer resumes at last ACK → redelivers msg #1000
  t=6   Gateway fans out msg #1000 to all subscribers
─────────────────────────────────────────────────────────

What guarantees this:
  1. JetStream FileStorage  → messages survive NATS restart too
  2. Durable consumer name  → gw_{node_id} persists delivery cursor
  3. Explicit ACK policy    → msg only acked AFTER fan-out completes
  4. max_ack_pending=10000  → back-pressure, prevents unbounded in-flight
  5. Graceful shutdown      → SIGTERM drains, SIGKILL covered by durable consumer
```

**The only remaining gap:** NATS itself needs 3 replicas for NATS-node crash
durability. With `num_replicas=1`, a NATS crash loses unwritten data.

---

## How to Test 1M Connections

### Tools Required

| Tool | Purpose | Install |
|------|---------|---------|
| **k6** | WebSocket load generator | `apt install k6` (see bench/README.md) |
| **tc-publish** | NATS message publisher (in this repo) | `cargo build --release --bin tc-publish` |
| **NATS server** | Message bus | `apt install nats-server` or Docker |
| **Prometheus + Grafana** | Live metrics during test | Docker Compose |
| **wrk2** | HTTP baseline (optional) | `apt install wrk` |
| **nats CLI** | Inspect JetStream state | `apt install natscli` |

---

### Phase 1 — Smoke Test (Local, 1k connections)

**Goal:** Verify the stack starts and messages flow.

```bash
# Terminal 1 — NATS (or already running)
nats-server --jetstream

# Terminal 2 — Gateway (from repo root; on Windows use WSL and cd into the clone)
# NOTE: --max-connections-per-ip must be >= TARGET when all VUs run from localhost.
# Default is 10, which will reject all but the first 10 connections.
RUST_LOG=info ./target/release/turbocable-server --port 9292 --max-connections-per-ip 5000

# Terminal 3 — Check health
curl http://localhost:9292/health

# Terminal 4 — Run smoke test (1k connections)
TARGET=1000 bash bench/scripts/run_single_node.sh
```

**Pass criteria:**
- `tc_sequence_gaps count == 0`
- `tc_connect_success rate > 0.99`
- `tc_fanout_latency_ms p(99) < 50`

---

### Phase 2 — Reconnect Test ✅

**Goal:** Verify client reconnect + replay with zero message loss.

Script: `bench/k6/reconnect_test.js`

```
Scenario:
  1. 100 VUs connect and subscribe to "bench"
  2. tc-publish sends 1 msg/s (monotonic seq)
  3. At t=30s: all VUs disconnect simultaneously
  4. At t=35s: all VUs reconnect with last_seq they recorded
  5. Server replays missed messages (seq gaps filled by replay)
  6. Verify: total messages received = total sent, zero gaps after accounting for replayed=true
```

**Run:**

```bash
# Start tc-publish first
./target/release/tc-publish --stream bench --rate 1 --duration 120

# Run the reconnect test
k6 run bench/k6/reconnect_test.js
```

See `bench/README.md` — Phase 2 section for full instructions.

---

### Phase 3 — Crash Recovery Test

**Goal:** Verify zero data loss across a hard gateway crash.

New script `bench/scripts/crash_recovery_test.sh`:

```bash
# Step 1: Start gateway, send 100 messages
# Step 2: SIGKILL the gateway after 50 messages
# Step 3: Restart the gateway
# Step 4: Verify all 100 messages are eventually delivered
# Step 5: Check NATS consumer lag drops to 0

# Key validation:
nats consumer info TURBOCABLE gw_<node_id>
# → num_pending should reach 0 after restart
```

Script: `bench/scripts/crash_recovery_test.sh`

```bash
# Default run (100 messages, SIGKILL at 50)
bash bench/scripts/crash_recovery_test.sh

# Custom parameters
N_MESSAGES=200 PUBLISH_RATE=5 bash bench/scripts/crash_recovery_test.sh
```

See `bench/README.md` — Phase 3 section for full instructions and environment variables.

---

### Phase 4 — Single-Node Baseline (333k connections)

**Goal:** Validate 333k connections, < 30ms p99, < 8 KB/connection.

**Machine requirements (per node):**
- 16 GB RAM minimum (333k × 8 KB = ~2.6 GB for connections alone)
- 8+ CPU cores
- Linux (not WSL — WSL has fd limit constraints)
- OS tuning applied: `sudo bash bench/scripts/tune_os.sh`

```bash
# On the gateway machine
sudo bash bench/scripts/tune_os.sh
ulimit -n 1000000

# Build release binary
cargo build --release

# Start NATS
nats-server --jetstream &

# Start gateway
RUST_LOG=info ./target/release/turbocable-server --port 9292

# On k6 machine (can be same machine)
TARGET=333000 \
GATEWAY_WSS_URL=ws://<gateway-ip>:9292/cable \
bash bench/scripts/run_single_node.sh
```

**Monitor during test:**
```bash
# Connection count (live)
watch -n1 'curl -s http://localhost:9292/metrics | grep turbocable_connections_active'

# Memory per connection
bash bench/scripts/memory_profile.sh

# NATS consumer lag
nats consumer info TURBOCABLE gw_<node_id>
```

**Pass criteria:**
```
turbocable_connections_active         >= 333,000
tc_fanout_latency_ms p(99)            < 30 ms
tc_sequence_gaps count                == 0
memory per connection                 < 8 KB
tc_connect_success rate               > 0.99
```

---

### Phase 5 — 3-Node Cluster (1M connections)

**Goal:** 1M connections across 3 gateway nodes, < 50ms p99, zero message loss.

**Infrastructure:**

```
                    ┌─────────────────┐
  1M clients ──────▶│  Load Balancer  │ (nginx / HAProxy)
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
         ┌────────┐     ┌────────┐     ┌────────┐
         │ GW-1   │     │ GW-2   │     │ GW-3   │
         │ 333k   │     │ 333k   │     │ 333k   │
         └────┬───┘     └────┬───┘     └────┬───┘
              └──────────────┼──────────────┘
                             │
                    ┌────────▼────────┐
                    │ NATS Cluster    │
                    │ 3 nodes         │
                    │ FileStorage     │
                    │ replicas=3      │
                    └─────────────────┘
```

**NATS cluster setup** (`infra/docker-compose.nats.yml` + `infra/nats/nats{1,2,3}.conf`):
```bash
docker compose -f infra/docker-compose.nats.yml up -d
# Verify: nats server list --server nats://localhost:4222
```

**Load balancer** (`infra/nginx.conf` — production SSL, `infra/nginx-local.conf` — plain HTTP):
```bash
# Local / staging (via docker-compose.cluster.yml — includes nginx automatically)
docker compose -f infra/docker-compose.nats.yml \
               -f infra/docker-compose.cluster.yml up -d

# Production — deploy infra/nginx.conf on the LB host after filling in
# <YOUR_DOMAIN> and SSL cert paths, then:
nginx -c /path/to/infra/nginx.conf -t && nginx -c /path/to/infra/nginx.conf
```

**Start gateways with cluster config:**
```bash
# On each gateway node
TURBOCABLE_NATS_URL=nats://nats1:4222,nats://nats2:4222,nats://nats3:4222 \
TURBOCABLE_NATS_STREAM_REPLICAS=3 \
./target/release/turbocable-server --port 9292
```

**Run the 1M load test (10 k6 agents in parallel):**
```bash
# Agent 1 (also runs tc-publish)
IS_PUBLISHER=true \
TARGET=100000 \
GATEWAY_WSS_URL=wss://lb.example.com/cable \
NATS_URL=nats://nats1:4222 \
bash bench/scripts/run_cluster.sh

# Agents 2-10 (same command without IS_PUBLISHER)
TARGET=100000 \
GATEWAY_WSS_URL=wss://lb.example.com/cable \
bash bench/scripts/run_cluster.sh
```

**Pass criteria for 1M:**
```
turbocable_connections_active (sum)   >= 1,000,000
tc_fanout_latency_ms p(99)            < 50 ms
tc_sequence_gaps count                == 0
memory per connection                 < 8 KB
nats_consumer_lag                     < 10,000
tc_connect_success rate               > 0.99
```

---

## Prioritized TODO List

### Must Do First

- [x] **Write `bench/k6/reconnect_test.js`** — client disconnect + reconnect +
  replay validation. Zero gaps must be verified after reconnect.

- [x] **Write `bench/scripts/crash_recovery_test.sh`** — SIGKILL gateway at
  mid-stream, restart, verify all messages delivered.

- [x] **Add NATS 3-node cluster Docker Compose** (`infra/docker-compose.nats.yml`)
  — required for true no-data-loss in production.

### Before 1M Test

- [x] **Add nginx/HAProxy config** (`infra/nginx.conf` + `infra/nginx-local.conf`)
  — required for the load balancer in the cluster test.

- [ ] **Verify OS tuning on test machines** — run `tune_os.sh` on every gateway
  and every k6 agent machine. Test will fail at 65k without it.

- [ ] **Run single-node baseline first** (Phase 4) — confirm 333k before
  attempting 1M cluster.

### Before Public Release

- [x] **Add Grafana dashboard JSON** (`infra/grafana/turbocable_dashboard.json`) — visual validation
  during 1M test.

- [x] **Add Alertmanager rules** — `nats_consumer_lag > 10000`,
  `connections_active drops > 5%/min`.

---

## Quick Reference: Key Files

| File | What it does |
|------|-------------|
| [src/pubsub/nats.rs](src/pubsub/nats.rs) | Durable consumer, fan-out loop, `replay_since()` |
| [src/connection/handler.rs](src/connection/handler.rs) | `try_parse_hello()`, `replay_for_stream()`, inbound/outbound loops |
| [src/connection/registry.rs](src/connection/registry.rs) | Lock-free DashMap registry, `fanout_encoded()` |
| [bench/k6/load_1m.js](bench/k6/load_1m.js) | k6 load test with sequence gap detection |
| [bench/scripts/run_single_node.sh](bench/scripts/run_single_node.sh) | Phase 10.1 single-node runner |
| [bench/scripts/run_cluster.sh](bench/scripts/run_cluster.sh) | Phase 10.2 cluster runner |
| [bench/scripts/tune_os.sh](bench/scripts/tune_os.sh) | fd limits + somaxconn (run as root) |
| [bench/README.md](bench/README.md) | Full load testing documentation |
