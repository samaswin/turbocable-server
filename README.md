# turbocable-server

A high-performance, standalone WebSocket gateway written in Rust, designed to handle **1M+ concurrent connections** with sub-50ms fan-out latency. Built as the core component of the [TurboCable](https://github.com/samaswin/turbocable) ecosystem.

## Why TurboCable?

WebSocket servers hit a ceiling when connections grow. Most backend frameworks
handle WebSockets in-process — each connection consumes a thread or coroutine,
memory grows linearly, and the broadcast bus becomes the bottleneck. TurboCable
moves **all WebSocket connections out of your backend** into a dedicated Rust
gateway:

```
Traditional:  1 broadcast → backend iterates N connections → N pub/sub messages → slow
TurboCable:   1 broadcast → 1 NATS publish → Rust fans out to N connections → fast
```

Your backend does O(1) work per broadcast. The Rust gateway does O(N) fan-out using
zero-copy `Bytes` cloning and lock-free `DashMap` shards — completing a fan-out
to 333k connections in under 10ms.

## Architecture

![TurboCable Architecture](docs/turbocable_architecture.svg)

```
Backend App                  NATS JetStream                 turbocable-server
┌─────────┐   publish        ┌─────────┐   push consumer   ┌──────────────┐
│  publish("room_42", data) ─┤ TURBOCABLE│──────────────────┤  Fan-out to  │
│         │                  │  stream   │                  │  1M clients  │
└─────────┘                  └─────────┘                    └──────┬───────┘
                                                                    │
                                                            WebSocket connections
                                                            (actioncable-v1-json
                                                             or turbocable-v1-msgpack)
```

- **Your backend** publishes once to NATS and moves on — it never touches WebSocket connections.
- **NATS JetStream** persists messages and pushes them to gateway consumers.
- **turbocable-server** fans out each message to all subscribers of that stream via a lock-free DashMap registry.

See [docs/architecture.md](docs/architecture.md) for the full system design.

## Features

- **1M concurrent WebSocket connections** on a 3-node cluster (333k per node)
- **Sub-50ms p99 fan-out latency** at full scale
- **< 8 KB memory per connection**
- **JSON protocol** (`actioncable-v1-json`) and **MessagePack** (`turbocable-v1-msgpack`)
- **RS256 JWT authentication** with hot-reloadable public keys via NATS KV
- **Stream-level authorization** — glob patterns in JWT claims
- **Message replay on reconnect** using JetStream sequence IDs
- **Presence tracking** via NATS KV with TTL-based cleanup
- **Prometheus metrics**, graceful shutdown, per-IP connection limits
- **SO_REUSEPORT + TCP_NODELAY**; **jemalloc** on glibc Linux for heavy loads

## Requirements

- Rust stable (minimum version in `Cargo.toml`, currently 1.88+)
- NATS Server 2.10+ with JetStream enabled

**New here?** Follow [docs/setup.md](docs/setup.md) for install, OS tuning, verification checklist, and NATS smoke tests. On Windows, use WSL for Rust — see [AGENTS.md](AGENTS.md) and [docs/development.md](docs/development.md).

## Quick start

```bash
asdf install   # or your Rust toolchain; see docs/setup.md
nats-server --jetstream &
cargo build
RUST_LOG=info cargo run
curl http://localhost:9292/health
```

**Try fan-out:** connect with `wscat -c ws://localhost:9292/cable`, subscribe with `{"command":"subscribe","identifier":"chat_room_1"}`, then `nats pub TURBOCABLE.chat_room_1 '{"text":"hello"}'`. Details: [docs/nats-jetstream.md](docs/nats-jetstream.md).

**JWT auth:** generate a key pair and set `TURBOCABLE_JWT_PUBLIC_KEY_PATH` — [docs/jwt-authentication.md](docs/jwt-authentication.md).

## Documentation

| Document | Description |
|----------|-------------|
| [docs/setup.md](docs/setup.md) | **Project setup** — Rust, NATS, OS limits, verify install, manual NATS/WebSocket checks |
| [docs/configuration.md](docs/configuration.md) | CLI/env configuration and HTTP endpoints (`/health`, `/metrics`, `/cable`, …) |
| [docs/websocket-protocol.md](docs/websocket-protocol.md) | Subscribe, message, replay, JWT claims, close codes |
| [docs/architecture.md](docs/architecture.md) | System design, data flow, capacity planning |
| [docs/load-testing-1m.md](docs/load-testing-1m.md) | **Load testing** — 333k/1M targets, scripts, k6, `tc-publish`, tuning, Prometheus |
| [docs/1m_connections_plan.md](docs/1m_connections_plan.md) | Roadmap, SLO summary, reconnect/crash semantics, file index |
| [bench/README.md](bench/README.md) | Pointer to `docs/load-testing-1m.md` and `bench/results/` |
| [docs/development.md](docs/development.md) | Tests, Clippy, fmt, CI jobs, source tree, WSL workflow |
| [docs/jwt-authentication.md](docs/jwt-authentication.md) | JWT auth and stream authorization |
| [docs/nats-jetstream.md](docs/nats-jetstream.md) | JetStream fan-out, replay, operations |
| [docs/graceful-shutdown.md](docs/graceful-shutdown.md) | SIGTERM drain and Kubernetes notes |
| [docs/binary-distribution.md](docs/binary-distribution.md) | Docker image, GitHub Releases binaries, cross-compilation |

## Observability

Start the full monitoring stack alongside the gateway cluster:

```bash
docker compose \
  -f infra/docker-compose.nats.yml \
  -f infra/docker-compose.cluster.yml \
  -f infra/docker-compose.monitoring.yml \
  up -d
```

| UI | URL | Credentials |
|----|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin (change on first login) |
| Prometheus | http://localhost:9090 | — |
| Alertmanager | http://localhost:9093 | — |

**Dashboard** — `infra/grafana/dashboards/turbocable.json` auto-provisioned by Grafana. Panels cover active connections, connect/disconnect rate, fan-out latency heatmap (p50/p95/p99), NATS consumer lag, backpressure evictions, per-IP rejections, JWT failures, and replay outcomes.

**Alert rules** — `infra/prometheus/alerts.yml` contains five production-ready rule groups:
- `ConnectionDropSpike` — active connections drop > 5% in 1 min, sustained 5 min
- `FanoutLatencyP99High` — p99 fan-out > 50 ms, sustained 10 min
- `NatsConsumerLag` — consumer pending > 10 k messages, sustained 5 min
- `BackpressureEvictions` — any evictions sustained > 5 min
- `PerIpLimiterSaturation` — rejection rate > 5%, sustained 5 min

Validate rules locally (requires `promtool` from the [Prometheus release](https://github.com/prometheus/prometheus/releases)):

```bash
promtool check rules infra/prometheus/alerts.yml
```

**Structured logs** — every connection emits a `connection` tracing span carrying `connection_id` (UUID v4) and `conn_id` (sequential registry ID). All log events within the connection lifecycle — accept, auth, subscribe, close — are tagged with these fields for correlation across distributed traces.

## Docker and binaries

```bash
docker pull ghcr.io/samaswin/turbocable-server:latest
docker run -p 9292:9292 \
  -e TURBOCABLE_NATS_URL=nats://host.docker.internal:4222 \
  ghcr.io/samaswin/turbocable-server:latest
```

The image is published under the GitHub **repository owner** name. If you use a fork, substitute your username: `ghcr.io/<owner>/turbocable-server`.

Full table of release artifacts and `docker build` instructions: [docs/binary-distribution.md](docs/binary-distribution.md).

## Load testing (summary)

```bash
sudo bash bench/scripts/tune_os.sh
cargo build --release --bin tc-publish
TARGET=333000 GATEWAY_WSS_URL=ws://localhost:9292/cable bash bench/scripts/run_single_node.sh
cargo bench --bench registry_bench
```

Use [docs/load-testing-1m.md](docs/load-testing-1m.md) for the full guide (smoke → 333k → cluster) and script reference.

## Related packages

| Package | Description |
|---------|-------------|
| [turbocable](https://github.com/samaswin/turbocable) | Ruby gem — NATS publisher for broadcasting |
| [turbocable-rails](https://github.com/samaswin/turbocable-rails) | DSL for TurboCable broadcasts |
| [@turbocable/client](https://github.com/samaswin/turbocable-client) | JavaScript client for browser connections |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for build/test commands, the PR checklist, commit style, and how to run k6 load tests.

## Security

To report a vulnerability privately, use [GitHub Security Advisories](https://github.com/samaswin/turbocable-server/security/advisories/new). See [SECURITY.md](SECURITY.md) for the full disclosure policy and supported versions.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a full history of releases.

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
