# Changelog

All notable changes to turbocable-server are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

Work planned in Phases 2–7 of the v0.5.0 → v0.6.0 gap-fix plan:

### Added
- Integration test harness (`tests/`) with scenarios for health, handshake, fan-out, JWT auth, replay, and graceful shutdown (Phase 2)
- `connection_id` UUID propagated through tracing spans for the full connection lifecycle (Phase 3)
- Prometheus alert rules (`infra/prometheus/alerts.yml`) for drop spikes, fan-out latency, NATS consumer lag, backpressure, and per-IP saturation (Phase 3)
- Grafana dashboard JSON (`infra/grafana/dashboards/turbocable.json`) (Phase 3)
- Per-stream rate limiting (token bucket, configurable via `Config`) with `turbocable_stream_rate_limited_total` and `turbocable_stream_tokens_available` metrics (Phase 4)
- Fuzz targets for JSON/MessagePack client-frame parsing and JWT decoding (`fuzz/`) (Phase 5)
- TLS termination guide (`docs/tls-termination.md`) with nginx + Let's Encrypt configuration (Phase 6)
- Kubernetes manifests (`infra/k8s/`) — Deployment, Service, HPA, ConfigMap, and Secret template (Phase 6)
- Rolling-upgrade runbook (`docs/rolling-upgrades.md`) with reconnect/replay behavior and rollback procedure (Phase 6)

---

## [0.5.1] - 2026-04-18

### Changed
- **Docker / GHCR:** Release workflow publishes to `ghcr.io/<repository-owner>/turbocable-server` (e.g. `ghcr.io/samaswin/turbocable-server`) instead of `ghcr.io/turbocable/server`, so images appear under the GitHub account that owns the repository. Set the package to **Public** in GitHub Packages for unauthenticated `docker pull`. `workflow_dispatch` also tags `latest` so manual runs produce a pullable image.

---

## [0.5.0] - 2026-03-28

### Added
- Replay-capable handshake enforcement — server validates `hello` frame with `last_seq` before accepting subscriptions (`REPLAY_ENFORCEMENT` env var: `compat` / `soft_enforce` / `hard_enforce`)
- Recoverable backpressure flow: replaced silent slow-consumer drops with a `GoAway` close and reconnect signal, tracked by `turbocable_backpressure_evictions_total`
- `tc_sequence_gaps` Prometheus counter for detecting replay ordering violations
- Benchmark suite and regression scripts under `bench/`
- Load-test pass/fail report with deterministic thresholds in `docs/load-testing-1m.md`
- k6 helper script (`bench/scripts/run_single_node.sh`) for 333k single-node runs

### Fixed
- Fan-out routing bug: messages were delivered to wrong stream subscribers when multiple streams shared a NATS subject prefix
- Replay ordering: messages were occasionally delivered out of sequence after reconnect under high concurrency
- Test harness flakiness under NATS reconnect races

### Changed
- Observability improvements: actionable Prometheus labels added to connection, auth, and fan-out metrics
- Benchmark alignment: all numeric gates now match the SLO table in `docs/1m_connections_plan.md`

---

## [0.4.0] - 2026-02-14

### Added
- Binary distribution: cross-compiled release binaries for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, and `x86_64-apple-darwin` published to GitHub Releases
- Docker image published to `ghcr.io/turbocable/server` (multi-platform: `linux/amd64`, `linux/arm64`)
- `infra/nginx.conf` example for TLS termination and WebSocket proxy
- `infra/docker-compose.yml` for local NATS + gateway development
- Load-testing documentation (`docs/load-testing-1m.md`) with smoke, 333k, and 1M cluster guides

### Fixed
- Security: removed a path that allowed unauthenticated access to stream subscriptions when `TURBOCABLE_JWT_PUBLIC_KEY_PATH` was unset
- Docker release pipeline: multi-platform build now publishes correctly to GHCR
- Grafana provisioning in Docker Compose

### Changed
- `cargo clippy` is now enforced as `-D warnings` in CI

---

## [0.3.0] - 2026-01-20

### Added
- Prometheus metrics endpoint (`GET /metrics`) exposing connection counts, fan-out latency histograms, JWT failure counters, and NATS consumer lag
- Graceful shutdown: SIGTERM drains connections with a `GoAway` WebSocket close frame before process exit; configurable drain timeout via `TURBOCABLE_SHUTDOWN_TIMEOUT_SECS`
- Presence tracking via NATS KV with TTL-based cleanup — publishes join/leave events to a configurable presence stream

### Changed
- Tokio runtime tuned with `worker_threads = num_cpus` and `SO_REUSEPORT` listener sharing for better multi-core utilisation

---

## [0.2.0] - 2025-12-15

### Added
- RS256 JWT authentication: clients present a signed token in the `Authorization` header or `token` query parameter; verified against a configurable public key
- Stream-level authorization: glob patterns in JWT `streams` claim restrict which NATS subjects a client may subscribe to
- NATS JetStream integration: push consumer per gateway with per-subject fan-out via lock-free `DashMap` registry
- Message replay on reconnect: clients send `last_seq` in their `hello` frame; the gateway replays all messages with `seq > last_seq` from JetStream before resuming the live feed
- Hot-reloadable JWT public keys via NATS KV watcher (`auth/key_watcher.rs`)

### Changed
- Minimum supported Rust version set to 1.88

---

## [0.1.0] - 2025-11-01

### Added
- Project skeleton: Axum HTTP server, health check (`GET /health`), CLI/env configuration via `clap`
- WebSocket upgrade: negotiate sub-protocol (`actioncable-v1-json` or `turbocable-v1-msgpack`) on `GET /cable`
- JSON codec (`actioncable-v1-json`): encode/decode `ClientCommand` and `ServerMessage` frames for Rails Action Cable compatibility
- MessagePack codec (`turbocable-v1-msgpack`): binary encoding via `rmp-serde` for lower overhead
- Connection registry: `DashMap`-backed registry mapping stream → set of sender handles
- Per-IP connection limits enforced at accept time (`connection/limiter.rs`)
- jemalloc allocator enabled on glibc Linux for reduced fragmentation under heavy load
- `SO_REUSEPORT` + `TCP_NODELAY` socket options applied at bind time

[Unreleased]: https://github.com/samaswin/turbocable-server/compare/v0.5.1...HEAD
[0.5.1]: https://github.com/samaswin/turbocable-server/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/samaswin/turbocable-server/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/samaswin/turbocable-server/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/samaswin/turbocable-server/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/samaswin/turbocable-server/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/samaswin/turbocable-server/releases/tag/v0.1.0
