# Load testing and the 1M connection target

This guide explains how to validate **333k connections per node** and **1M
connections on a three-node cluster**, what infrastructure to use, and where
the detailed scripts and roadmap live.

---

## What “1M connections” means

| Scope | Target | Typical setup |
|-------|--------|----------------|
| Single gateway | ~333k sustained WebSockets | One Linux machine, tuned OS |
| Three gateways | ~1M total | Load balancer + shared NATS JetStream cluster |

Success criteria include connection count, fan-out latency (p99), zero sequence
gaps in k6, and healthy NATS consumer lag. Exact thresholds are summarized in
[1m_connections_plan.md](1m_connections_plan.md) and in [`bench/README.md`](../bench/README.md).

---

## Prerequisites

1. **Linux on gateway and k6 agents** — Real counts above ~65k file descriptors
   are unreliable on default OS settings and impractical in WSL for full-scale
   runs. Use bare metal or VMs for Phase 4/5.
2. **OS tuning** — On every gateway and every k6 machine:

   ```bash
   sudo bash bench/scripts/tune_os.sh
   ```

3. **Raise per-IP limits on the gateway** — Default `TURBOCABLE_MAX_CONN_PER_IP`
   is `10`. Load tests from one IP require a much higher limit (see
   [`bench/README.md`](../bench/README.md)).
4. **k6** — WebSocket load generator (install steps in `bench/README.md`).
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

## Phased test flow

Work through these in order; do not skip straight to 1M on untuned hardware.

| Phase | Goal | Where it is documented |
|-------|------|-------------------------|
| **Smoke** | ~1k connections, paths work | Below; [`bench/README.md`](../bench/README.md) |
| **Reconnect** | Replay after disconnect, zero gaps | `bench/k6/reconnect_test.js`, `bench/README.md` |
| **Crash recovery** | SIGKILL gateway, no lost fan-out | `bench/scripts/crash_recovery_test.sh` |
| **Single node** | ~333k connections, p99 latency budget | `bench/scripts/run_single_node.sh` |
| **Cluster** | ~1M connections via LB + 3 gateways | `bench/scripts/run_cluster.sh` |

### Smoke test (local)

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

### Single-node baseline (~333k)

```bash
sudo bash bench/scripts/tune_os.sh
ulimit -n 1000000
cargo build --release
# Start NATS and gateway with appropriate --max-connections-per-ip

TARGET=333000 \
GATEWAY_WSS_URL=ws://<gateway-host>:9292/cable \
bash bench/scripts/run_single_node.sh
```

### Cluster (~1M)

Use the NATS cluster compose file, deploy three gateway instances (or
`docker-compose.cluster.yml`), point a load balancer at them (see
`infra/nginx.conf` / `infra/nginx-local.conf`), then run multiple k6 agents with
`bench/scripts/run_cluster.sh` as described in [`bench/README.md`](../bench/README.md).

---

## Criterion micro-benchmarks (no NATS)

In-process registry fan-out benchmarks:

```bash
cargo bench --bench registry_bench
```

---

## Further reading

| Document | Contents |
|----------|----------|
| [`bench/README.md`](../bench/README.md) | Script reference, env vars, Phase 10.1 / 10.2 details |
| [1m_connections_plan.md](1m_connections_plan.md) | Roadmap, SLO summary, reconnect/crash semantics, file index |
| [architecture.md](architecture.md) | Capacity planning and system design |
| [configuration.md](configuration.md) | Flags/env vars relevant under load |

---

## Related documentation

- [setup.md](setup.md) — building the project and local verification
- [development.md](development.md) — contributor workflow (including WSL)
