# turbocable-server

A high-performance, standalone WebSocket gateway written in Rust, designed to handle **1M+ concurrent connections** with sub-50ms fan-out latency. Built as the core component of the [TurboCable](https://github.com/samaswin/turbocable) ecosystem.

## Architecture

```
Rails App                    NATS JetStream                 turbocable-server
┌─────────┐   publish        ┌─────────┐   push consumer   ┌──────────────┐
│  .broadcast("room_42", d) ─┤ TURBOCABLE│──────────────────┤  Fan-out to  │
│         │                  │  stream   │                  │  1M clients  │
└─────────┘                  └─────────┘                    └──────┬───────┘
                                                                    │
                                                            WebSocket connections
                                                            (actioncable-v1-json
                                                             or turbocable-v1-msgpack)
```

- **Rails** publishes once to NATS and moves on — it never touches WebSocket connections.
- **NATS JetStream** persists messages and pushes them to gateway consumers.
- **turbocable-server** fans out each message to all subscribers of that stream via a lock-free DashMap registry.

## Features

- **1M concurrent WebSocket connections** on a 3-node cluster (333k per node)
- **Sub-50ms p99 fan-out latency** at full scale
- **< 8 KB memory per connection**
- **ActionCable-compatible** JSON protocol (`actioncable-v1-json`)
- **MessagePack binary protocol** (`turbocable-v1-msgpack`) for reduced bandwidth
- **RS256 JWT authentication** with hot-reloadable public keys via NATS KV
- **Message replay on reconnect** using JetStream sequence IDs
- **Presence tracking** via NATS KV with automatic TTL-based cleanup
- **Prometheus metrics** for connections, fan-out latency, NATS consumer lag
- **Graceful shutdown** — drains connections on SIGTERM with zero message loss
- **Per-IP connection limiting** to prevent resource exhaustion
- **SO_REUSEPORT + TCP_NODELAY** for optimal kernel-level performance
- **jemalloc** allocator for predictable memory usage under high load

## Requirements

- Rust stable (1.78+)
- NATS Server with JetStream enabled

## Quick Start

```bash
# Install Rust via asdf
asdf plugin add rust
asdf install

# Start NATS with JetStream
nats-server --jetstream &

# Build and run
cargo build --release
RUST_LOG=info cargo run

# Verify
curl http://localhost:9292/health
# => {"status":"ok","version":"0.1.0"}
```

## Configuration

All options can be set via CLI flags or environment variables:

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `TURBOCABLE_PORT` | `9292` | Listen port |
| `--nats-url` | `TURBOCABLE_NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `--node-id` | `TURBOCABLE_NODE_ID` | `node_<uuid>` | Unique node identifier |
| `--ping-interval` | `TURBOCABLE_PING_INTERVAL` | `30` | WebSocket ping interval (seconds) |

## Endpoints

| Path | Description |
|------|-------------|
| `GET /health` | Health check — returns `{"status":"ok"}` |
| `GET /metrics` | Prometheus metrics |
| `GET /pubkey` | Current RS256 public key PEM |
| `GET /cable` | WebSocket upgrade endpoint |

## WebSocket Connection

```bash
wscat -c 'ws://localhost:9292/cable?token=<JWT>'
```

### Subscribe

```json
{"command":"subscribe","identifier":"{\"channel\":\"ChatChannel\",\"room_id\":1}"}
```

### Unsubscribe

```json
{"command":"unsubscribe","identifier":"{\"channel\":\"ChatChannel\",\"room_id\":1}"}
```

### Message Replay

```json
{"type":"hello","last_seq":"8841"}
```

## Docker

```bash
docker build -t turbocable-server .
docker run -p 9292:9292 -e TURBOCABLE_NATS_URL=nats://host.docker.internal:4222 turbocable-server
```

The release image is built `FROM scratch` and is approximately 12 MB.

## Development

```bash
# Run with debug logging
RUST_LOG=debug cargo run

# Run tests
cargo test

# Lint
cargo clippy -- -D warnings

# Format check
cargo fmt --check
```

## Related Packages

| Package | Description |
|---------|-------------|
| [turbocable](https://github.com/samaswin/turbocable) | Ruby gem — NATS publisher for broadcasting |
| [turbocable-rails](https://github.com/samaswin/turbocable-rails) | Rails DSL for TurboCable broadcasts |
| [@turbocable/client](https://github.com/samaswin/turbocable-client) | JavaScript client for browser connections |

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
