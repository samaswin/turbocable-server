# Configuration and HTTP API

How to configure turbocable-server (CLI flags and environment variables) and
which HTTP routes the gateway exposes.

For prerequisites and first-time run instructions, see [setup.md](setup.md).

---

## Configuration

All options can be set via CLI flags or environment variables:

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `TURBOCABLE_PORT` | `9292` | Listen port |
| `--nats-url` | `TURBOCABLE_NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `--node-id` | `TURBOCABLE_NODE_ID` | `node_<uuid>` | Unique node identifier |
| `--ping-interval-secs` | `TURBOCABLE_PING_INTERVAL` | `30` | WebSocket ping interval (seconds) |
| `--max-connections-per-ip` | `TURBOCABLE_MAX_CONN_PER_IP` | `10` | Max concurrent connections per IP |
| `--jwt-public-key-path` | `TURBOCABLE_JWT_PUBLIC_KEY_PATH` | _(none)_ | Path to RSA public key PEM for JWT auth |
| `--max-ack-pending` | `TURBOCABLE_MAX_ACK_PENDING` | `10000` | Max unacknowledged NATS JetStream messages (back-pressure) |
| `--nats-stream-replicas` | `TURBOCABLE_NATS_STREAM_REPLICAS` | `1` | JetStream stream replica count (use `3` in production) |
| `--replay-enforcement` | `REPLAY_ENFORCEMENT` | `compat` | Handshake rollout: `compat` (legacy), `soft_enforce` (hello required; subscribes without `replay_v1` allowed with warning metric), `hard_enforce` (reject non-replay subscribes). Rollback: set `compat` and restart. |
| `--ws-channel-capacity` | `TURBOCABLE_WS_CHANNEL_CAPACITY` | `4096` | Per-connection live fan-out channel depth |
| `--ws-replay-channel-capacity` | `TURBOCABLE_WS_REPLAY_CHANNEL_CAPACITY` | `4096` | Per-connection replay queue depth (drained before live) |
| `--max-replay-concurrency` | `TURBOCABLE_MAX_REPLAY_CONCURRENCY` | `1000` | Max concurrent JetStream replay tasks per node |

Logging uses the standard `RUST_LOG` environment variable (for example `RUST_LOG=info` or `debug`). There is no dedicated CLI flag for log level.

### Example: full configuration

```bash
TURBOCABLE_PORT=9292 \
TURBOCABLE_NATS_URL=nats://localhost:4222 \
TURBOCABLE_NODE_ID=gateway-01 \
TURBOCABLE_PING_INTERVAL=30 \
TURBOCABLE_MAX_CONN_PER_IP=10 \
TURBOCABLE_JWT_PUBLIC_KEY_PATH=/etc/turbocable/public_key.pem \
TURBOCABLE_MAX_ACK_PENDING=10000 \
TURBOCABLE_NATS_STREAM_REPLICAS=1 \
RUST_LOG=info \
cargo run --release
```

For load testing from localhost, raise `--max-connections-per-ip` (or `TURBOCABLE_MAX_CONN_PER_IP`) so k6 virtual users are not rejected after the default limit of 10. See [load-testing-1m.md](load-testing-1m.md).

---

## HTTP endpoints

| Path | Description |
|------|-------------|
| `GET /health` | Health check — returns JSON including `status`, `version`, and `connections` |
| `GET /metrics` | Prometheus metrics in text exposition format |
| `GET /pubkey` | Current RS256 public key PEM (for verifying JWT key distribution) |
| `GET /cable` | WebSocket upgrade endpoint (pass `?token=<JWT>` when auth is enabled) |

---

## Related documentation

- [WebSocket protocol](websocket-protocol.md) — subscribe, message, replay, JWT claims
- [JWT authentication](jwt-authentication.md) — tokens, stream globs, testing
- [NATS JetStream](nats-jetstream.md) — fan-out subjects, replay, operations
- [Binary distribution](binary-distribution.md) — Docker env vars in production
