# Load testing and the 1M connection target

This guide explains how to validate **333k connections per node** and **1M
connections on a three-node cluster**, what infrastructure to use, and how to use
the scripts under `bench/`.

---

## What “1M connections” means

| Scope | Target | Typical setup |
|-------|--------|----------------|
| Single gateway | ~333k sustained WebSockets | One Linux machine, tuned OS |
| Three gateways | ~1M total | Load balancer + shared NATS JetStream cluster |

Success criteria include connection count, fan-out latency (p99), zero sequence
gaps in k6, and healthy NATS consumer lag. A roadmap-style summary lives in
[1m_connections_plan.md](1m_connections_plan.md).

---

## Release / promotion gates (numeric)

Use these thresholds to decide whether a change is safe to promote. These gates mirror `docs/reliability-replay-plan.md` so reliability work doesn’t compromise the 1M+ / sub-50ms promise.

- **Connection scale**: sustain >= 1,000,000 concurrent connections for a 30-minute steady-state window (canonical load environment).
- **Fan-out latency**: p95 <= 50ms and p99 <= 75ms during steady-state plus reconnect churn scenarios.
- **Sequence integrity**: `tc_sequence_gaps == 0` and replay ordering violations == 0 in all promotion runs.
- **Replay reliability**: replay success rate >= 99.95% and replay failure rate <= 0.05% per run.
- **Backpressure safety**: forced reconnect rate <= 1.0% of active connections per minute over any 5-minute window.
- **Recovery**: post-reconnect first-delivery p95 <= 2s and full catch-up success >= 99.9% for clients inside retention window.

Phase minimums (if rolling out enforcement):

- **Phase A (compat)**: run at >= 250k sustained; pass all gates except 1M scale; replay-capable client coverage >= 80%.
- **Phase B (soft enforce)**: run at >= 600k sustained; pass all numeric gates; measure and bound non-compliant client rejects.
- **Phase C (hard enforce)**: run at full 1M sustained; pass all numeric gates for two consecutive runs; validate rollback switch with a controlled drill.

### k6 benchmark report (deterministic pass/fail)

k6 scripts enforce the same ideas as the gates above via **thresholds** (a failed threshold fails the run with a non-zero exit code). Use this table when reading CI or `bench/results/benchmark_metrics.md` alongside raw `bench/results/artifacts/k6-*.json`.

| Script | Reliability | Latency / recovery |
|--------|-------------|-------------------|
| `load_1m.js` | `tc_sequence_gaps` **count == 0**; `tc_connect_success` **rate > 0.99**; `tc_connection_errors` **count < 100** | `tc_fanout_latency_ms`: **p(95) < LATENCY_P95_MS** (default **50**), **p(99) < LATENCY_P99_MS** (default **75**). Tighten for single-node baseline, e.g. `LATENCY_P99_MS=30`. |
| `reconnect_test.js` | Same sequence + connect/reconnect rates as documented below | Live fan-out: same **p95/p99** defaults as load. **Post-reconnect first data**: `tc_post_reconnect_first_delivery_ms` **p(95) < 2000** ms (recovery gate probe). |
| `backpressure_eviction_test.js` | Same as reconnect for gaps + connect rates; optional **`REQUIRE_BACKPRESSURE_EVICT=1`** adds **`tc_backpressure_disconnects` count >= 1** (cluster-wide counter) | Same fan-out p95/p99 defaults; same **post-reconnect first delivery** p95 < 2000 ms. |

**Backpressure safety gate (Prometheus, not k6):** `turbocable_forced_reconnect_backpressure_total` rate vs active connections — see `docs/reliability-replay-plan.md` (<= 1.0% per minute over 5 minutes). k6’s `tc_backpressure_disconnects` is a client-side witness when eviction occurs.

**Mixed workload:** `backpressure_eviction_test.js` uses **`SLOW_VUS`** (default = `TARGET`, all slow) so only VUs `1..SLOW_VUS` apply **`SLOW_SLEEP_S`**; the rest consume at full speed, stressing fan-out while a subset falls behind.

---

## Prerequisites

1. **Linux on gateway and k6 agents** — Real counts above ~65k file descriptors
   are unreliable on default OS settings and impractical in WSL for **full-scale**
   333k/1M runs. Use bare metal or VMs for those targets.
2. **OS tuning** — On every gateway and every k6 machine:

   ```bash
   sudo bash bench/scripts/tune_os.sh
   ```

   Raises file-descriptor limits to 2M and sets `net.core.somaxconn=65535`.

3. **Raise per-IP limits on the gateway** — Default `TURBOCABLE_MAX_CONN_PER_IP`
   is `10`. Load tests from one IP require a much higher limit:

   ```bash
   # Smoke / moderate load
   RUST_LOG=info ./target/release/turbocable-server --port 9292 --max-connections-per-ip 5000

   # Large single-host load (e.g. 333k from localhost)
   RUST_LOG=info ./target/release/turbocable-server --port 9292 --max-connections-per-ip 400000

   # Or via environment variable
   TURBOCABLE_MAX_CONN_PER_IP=400000 ./target/release/turbocable-server --port 9292
   ```

   Without this, the gateway rejects connections after the first 10 from the same IP
   and the k6 `tc_connect_success` rate collapses.

4. **k6** — WebSocket load generator. The apt keyserver method is unreliable on some systems; prefer the direct binary:

   ```bash
   curl -L https://github.com/grafana/k6/releases/download/v0.55.0/k6-v0.55.0-linux-amd64.tar.gz \
     -o /tmp/k6.tar.gz && \
   tar -xzf /tmp/k6.tar.gz -C /tmp && \
   sudo mv /tmp/k6-v0.55.0-linux-amd64/k6 /usr/local/bin/k6 && \
   k6 version
   ```

   Alternatively, the [k6 apt repo](https://k6.io/docs/getting-started/installation/) (may fail if the keyserver is unreachable).

5. **`tc-publish`** — NATS publisher for fan-out tests:

   ```bash
   cargo build --release --bin tc-publish
   ```

6. **NATS with JetStream** — Single node for smoke tests; **three-node cluster**
   with file storage and stream replicas for production-like 1M tests:

   ```bash
   docker compose -f infra/docker-compose.nats.yml up -d
   ```

7. **Cluster stack (optional)** — Three gateways behind nginx:

   ```bash
   docker compose -f infra/docker-compose.nats.yml \
                  -f infra/docker-compose.cluster.yml up -d
   ```

8. **Monitoring (optional)** — Prometheus, Alertmanager, Grafana:

   ```bash
   docker compose -f infra/docker-compose.monitoring.yml up -d
   ```

---

## Directory layout

```
bench/
├── k6/
│   ├── load_1m.js                    Main k6 WebSocket load test (single-node + cluster agents)
│   ├── reconnect_test.js             Reconnect + replay validation
│   └── backpressure_eviction_test.js Optional: slow drain + backpressure disconnect + replay
└── scripts/
    ├── run_full_test_plan.sh   Health → reconnect → crash recovery → sustained load (WSL-friendly defaults)
    ├── run_single_node.sh      Single-node k6 + tc-publish wrapper
    ├── run_cluster.sh          Cluster test runner (one process per k6 agent)
    ├── crash_recovery_test.sh  SIGKILL gateway + restart, verify zero data loss
    ├── memory_profile.sh       RSS / per-connection memory monitor
    └── tune_os.sh              One-time OS tuning (fd limits, somaxconn)

benches/
└── registry_bench.rs        Criterion micro-benchmarks (in-process fan-out)

src/bin/
└── publish.rs               tc-publish — NATS message publisher for fan-out testing

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
│   ├── turbocable.rules.yml    Prometheus alerting rules (lag, backpressure reconnect, latency)
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

## Recommended test order

Work through these in order; do not jump straight to 1M on untuned hardware.

| Step | Goal | Where |
|------|------|--------|
| **Smoke** | ~1k connections, paths work | Below; `run_single_node.sh` |
| **Reconnect** | Replay after disconnect, zero gaps | `bench/k6/reconnect_test.js` |
| **Backpressure** | Eviction disconnect + replay (optional; needs tuning) | `bench/k6/backpressure_eviction_test.js` |
| **Crash recovery** | SIGKILL gateway, no lost fan-out | `bench/scripts/crash_recovery_test.sh` |
| **Single node** | ~333k connections, p99 latency budget | `bench/scripts/run_single_node.sh` |
| **Cluster** | ~1M connections via LB + 3 gateways | `bench/scripts/run_cluster.sh` |

---

## Automated full plan (WSL / Linux)

From the repo root, `run_full_test_plan.sh` runs in order: health check → reconnect/replay (k6) → optional **backpressure + replay** k6 → crash recovery → sustained load (`run_single_node.sh`). Defaults are safe for **WSL** (1k connections, short ramp/sustain); use `--quick` for a faster smoke.

**Requires:** `cargo`, `k6`, `curl`, `python3`; `nats-server` (or NATS already on `4222`); **`nats` CLI** for crash recovery (or `--skip-crash`). With `--quick`, if the `nats` CLI is missing, crash recovery is skipped automatically.

Add **`--with-backpressure`** to run `bench/k6/backpressure_eviction_test.js` after the reconnect scenario (mixed fast/slow VUs; higher publish rate for that segment). To **require** at least one eviction disconnect in the run, set **`REQUIRE_BACKPRESSURE_EVICT=1`** (only after tuning gateway buffer + publish rate + `SLOW_SLEEP_S` so eviction is likely). Override segment sizes with **`BP_TARGET`**, **`BP_SLOW_VUS`**, **`BP_PHASE1`**, **`BP_GAP`**, **`BP_PHASE2`**, **`BP_SLOW_SLEEP`**, **`BP_PUBLISH_RATE`** in the environment when invoking the script (see `bench/scripts/run_full_test_plan.sh`).

**NATS / JetStream:** Before crash recovery, the script runs `nats stream ls` against `NATS_URL` (default `nats://127.0.0.1:4222`). If you see “port 4222 open” but the check still fails, NATS is often running **without** JetStream — restart with `nats-server --jetstream`. For Docker, TLS, or auth, set `NATS_URL` (for example `tls://…` or `nats://user:pass@host:4222`). To ignore this step, use `--skip-crash`.

```bash
bash bench/scripts/run_full_test_plan.sh                      # full WSL-friendly run
bash bench/scripts/run_full_test_plan.sh --quick                # shorter; skips crash if `nats` CLI missing
bash bench/scripts/run_full_test_plan.sh --with-backpressure    # adds backpressure k6 after reconnect
bash bench/scripts/run_full_test_plan.sh --skip-crash           # no `nats` CLI
bash bench/scripts/run_full_test_plan.sh --target 2000        # override load VUs
bash bench/scripts/run_full_test_plan.sh --no-build           # use existing release binaries
NO_BUILD=1 bash bench/scripts/run_full_test_plan.sh
```

See `bash bench/scripts/run_full_test_plan.sh --help` for all flags (`--with-bench` runs Criterion after load).

**Metrics log:** After successful k6 steps, results append to [`bench/results/benchmark_metrics.md`](../bench/results/benchmark_metrics.md) (UTC date, approximate **send** volume from `tc-publish` rate×duration, **receive** counts and fan-out latency from k6). Override path with `BENCH_METRICS_MD`. Raw k6 JSON is under `bench/results/artifacts/` (see `.gitignore`).

---

## Smoke test (local)

```bash
# Terminal 1 — NATS
nats-server --jetstream

# Terminal 2 — gateway (per-IP limit must be ≥ concurrent connections from your IP)
RUST_LOG=info ./target/release/turbocable-server --port 9292 --max-connections-per-ip 5000

# Terminal 3 — health
curl http://localhost:9292/health

# Terminal 4 — k6 wrapper (example: 1k connections)
TARGET=1000 bash bench/scripts/run_single_node.sh
```

---

## Single-node baseline (~333k)

**Target:** 333k connections, p99 < 30 ms, < 8 KB/connection (see [1m_connections_plan.md](1m_connections_plan.md) for SLO context).

```bash
# Quick smoke (1000 connections)
TARGET=1000 bash bench/scripts/run_single_node.sh

# Full single-node baseline
TARGET=333000 GATEWAY_WSS_URL=ws://node1:9292/cable bash bench/scripts/run_single_node.sh
```

Or run k6 directly:

```bash
k6 run bench/k6/load_1m.js \
  -e TARGET=333000 \
  -e GATEWAY_WSS_URL=ws://node1:9292/cable \
  -e LATENCY_P95_MS=50 \
  -e LATENCY_P99_MS=75
```

For a stricter single-node latency baseline (e.g. p99 < 30 ms), set `LATENCY_P99_MS=30` (and optionally tighten `LATENCY_P95_MS`).

### Memory profiling

While the load test runs, monitor RSS on the gateway host:

```bash
bash bench/scripts/memory_profile.sh
```

Example output:

```
Timestamp                 conns    rss(kB)    per(kB)
-----------------------------------------------------------
2024-01-15 12:00:00       333142   2548736      7.6
```

---

## Reconnect and replay validation

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

Custom parameters:

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
- Phase 1 sends `hello` with `replay_v1` before subscribe; after a 5 s offline gap, each VU reconnects and sends
  `hello` with `last_seq` and `replay_v1`, then re-subscribes.
- The server replays missed messages (top-level `replayed: true`) before confirming the
  subscription; live messages then resume.
- `tc_sequence_gaps` must remain `0` — any gap means a message was never
  delivered even after replay.

**Pass criteria (k6 thresholds):**

```
tc_sequence_gaps count                        == 0    (no message loss after replay)
tc_connect_success rate                       > 0.99
tc_reconnect_success rate                     > 0.99
tc_connection_errors count                    < 10
tc_fanout_latency_ms   p(95) < LATENCY_P95_MS (default 50)
tc_fanout_latency_ms   p(99) < LATENCY_P99_MS (default 75)
tc_post_reconnect_first_delivery_ms p(95)    < 2000 ms
```

**Custom metrics:**

| Metric | Description |
|--------|-------------|
| `tc_fanout_latency_ms` | p50/p95/p99 latency for live (non-replayed) messages |
| `tc_post_reconnect_first_delivery_ms` | Time from phase-2 WebSocket open to first data message (replay or live) |
| `tc_messages_received` | Total messages received (initial + replay + live) |
| `tc_replayed_messages` | Messages delivered with top-level `replayed: true` after reconnect |
| `tc_sequence_gaps` | Missing seqs not covered by replay (must be 0) |
| `tc_connect_success` | Rate of successful initial WS upgrades |
| `tc_reconnect_success` | Rate of successful reconnect WS upgrades |
| `tc_connection_errors` | Total connection errors across both segments |

### Backpressure eviction + replay (promotion scenario)

`bench/k6/backpressure_eviction_test.js` sends `hello` with `replay_v1`, subscribes, and optionally adds **`SLOW_SLEEP_S`** per data message on **slow VUs only** (`SLOW_VUS` defaults to `TARGET` so every VU is slow; set e.g. `TARGET=24` and `SLOW_VUS=8` for a mixed pool). That pattern stresses live fan-out for fast clients while slow clients risk filling the per-socket outbound channel; the server should evict with `disconnect` / `backpressure_reconnect_required` (recoverable path, not silent drop). After reconnect with `last_seq`, **`tc_sequence_gaps` must stay 0**.

k6 thresholds match the reconnect test for gaps, connect rates, live **p95/p99** fan-out (defaults 50 / 75 ms), and **post-reconnect first delivery p95 < 2 s**. Set **`REQUIRE_BACKPRESSURE_EVICT=1`** only after tuning **`TURBOCABLE_WS_CHANNEL_CAPACITY`**, publish rate, **`SLOW_SLEEP_S`**, and **`SLOW_VUS`** so at least one disconnect is observed (threshold: **`tc_backpressure_disconnects` count >= 1**).

Example (tuned gateway with a small outbound buffer):

```bash
./target/release/tc-publish --stream bench --rate 200 --duration 600
TARGET=20 SLOW_VUS=6 SLOW_SLEEP_S=0.03 k6 run bench/k6/backpressure_eviction_test.js
# Stricter: require eviction
REQUIRE_BACKPRESSURE_EVICT=1 TARGET=20 SLOW_VUS=6 SLOW_SLEEP_S=0.03 k6 run bench/k6/backpressure_eviction_test.js
```

**Burst / mixed steady + churn:** combine sustained `load_1m.js` or `run_single_node.sh` with a second publisher at a higher rate for short windows, or run `run_full_test_plan.sh --with-backpressure` so reconnect churn and backpressure scenarios run in one flow.

---

## Crash recovery test

**Target:** Zero data loss across a hard SIGKILL of the gateway mid-stream.

```bash
# Terminal 1 — NATS
nats-server --jetstream

# Terminal 2 — Run the crash recovery test (uses fixed node_id internally)
bash bench/scripts/crash_recovery_test.sh
```

Custom parameters:

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

## Cluster load (~1M)

**Target:** 1M connections, p99 < 50 ms, zero message loss.

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

Use the NATS cluster compose file, deploy three gateway instances (or
`docker-compose.cluster.yml`), point a load balancer at them (see
`infra/nginx.conf` / `infra/nginx-local.conf`).

---

## tc-publish — message publisher

The `tc-publish` binary publishes messages with monotonic sequence numbers and
millisecond timestamps to `TURBOCABLE.<stream>`. k6 clients use these to
measure end-to-end fan-out latency and detect message loss.

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

## Criterion micro-benchmarks (no NATS)

In-process registry fan-out benchmarks:

```bash
cargo bench --bench registry_bench
```

Benchmarks cover:

- `fanout_json` — all-JSON clients at 1k / 10k / 100k connections
- `fanout_mixed_codecs` — 50 % JSON + 50 % MessagePack at 1k / 10k / 100k
- `fanout_backpressure` — overflow path when all outbound buffers are full (eviction + deregister; first iteration evicts all subscribers, later iterations are mostly no-op)
- `subscribe_deregister` — register + subscribe (3 streams) + deregister × 1000

Results are saved to `target/criterion/` with an HTML report.

---

## Performance tuning under load

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

## Prometheus metrics

| Metric | Description |
|--------|-------------|
| `turbocable_connections_active` | Current open WebSocket connections |
| `turbocable_connections_total` | Total connections since startup |
| `turbocable_connections_rejected_total` | Auth / rate-limit rejections |
| `turbocable_messages_fanned_out_total` | Messages delivered to clients |
| `turbocable_fanout_duration_seconds` | Fan-out latency histogram |
| `turbocable_nats_consumer_lag` | NATS pending message backlog |
| `turbocable_forced_reconnect_backpressure_total` | Connections evicted due to full outbound channel (backpressure reconnect; not silent loss) |
| `turbocable_handshake_ok_replay_capable_total` | Clients that completed hello handshake with `replay_v1` capability |
| `turbocable_handshake_ok_legacy_total` | Clients that completed hello handshake without `replay_v1` |
| `turbocable_handshake_legacy_warn_total` | Commands received before hello in compat mode (legacy client signal) |
| `turbocable_handshake_rejected_total` | Commands rejected because client had not sent hello (enforce mode) |
| `turbocable_handshake_soft_non_replay_subscribe_allowed_total` | Subscribe allowed in soft_enforce without `replay_v1` (legacy client signal) |
| `turbocable_handshake_rejected_non_replay_capable_total` | Subscribe rejected in hard_enforce: client not `replay_v1`-capable |
| `turbocable_replay_success_total` | Replay tasks that completed without error |
| `turbocable_replay_failure_total` | Replay tasks that failed with a transient JetStream error |
| `turbocable_replay_window_exceeded_total` | Replay tasks aborted: client `last_seq` outside the retention window |
| `turbocable_replay_aborted_peer_gone_total` | Replay tasks stopped because the outbound channel closed mid-replay |
| `turbocable_replay_truncated_total` | Replay tasks that hit the per-stream message cap (client resync required) |
| `turbocable_replay_in_flight` | Active replay tasks (gauge) |
| `turbocable_replay_catch_up_duration_seconds` | Wall-clock time for a replay task from start to completion (histogram) |
| `turbocable_replay_first_delivery_seconds` | Time from replay start to first message enqueued in the outbound channel (histogram) |

Scrape endpoint: `http://<gateway>:9292/metrics`

---

## Further reading

| Document | Contents |
|----------|----------|
| [1m_connections_plan.md](1m_connections_plan.md) | Roadmap, SLO summary, reconnect/crash semantics, file index |
| [architecture.md](architecture.md) | Capacity planning and system design |
| [configuration.md](configuration.md) | Flags/env vars relevant under load |

---

## Related documentation

- [setup.md](setup.md) — building the project and local verification
- [development.md](development.md) — contributor workflow (including WSL)
