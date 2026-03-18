# turbocable-server

A high-performance, standalone WebSocket gateway written in Rust, designed to handle **1M+ concurrent connections** with sub-50ms fan-out latency. Built as the core component of the [TurboCable](https://github.com/samaswin/turbocable) ecosystem.

## Why TurboCable?

Rails ActionCable hits a ceiling at ~10k–50k connections per process. Every
connection lives in Ruby, memory grows fast, and Redis pub/sub becomes the
bottleneck. TurboCable solves this by moving **all WebSocket connections out of
Ruby** into a dedicated Rust gateway:

```
ActionCable:  1 broadcast → Ruby iterates N connections → N Redis messages → slow
TurboCable:   1 broadcast → 1 NATS publish → Rust fans out to N connections → fast
```

Rails does O(1) work per broadcast. The Rust gateway does O(N) fan-out using
zero-copy `Bytes` cloning and lock-free `DashMap` shards — completing a fan-out
to 333k connections in under 10ms.

## Architecture

![TurboCable Architecture](docs/turbocable_architecture.svg)

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

See [docs/architecture.md](docs/architecture.md) for the full system design.

## Features

- **1M concurrent WebSocket connections** on a 3-node cluster (333k per node)
- **Sub-50ms p99 fan-out latency** at full scale
- **< 8 KB memory per connection**
- **ActionCable-compatible** JSON protocol (`actioncable-v1-json`)
- **MessagePack binary protocol** (`turbocable-v1-msgpack`) for reduced bandwidth
- **RS256 JWT authentication** with hot-reloadable public keys via NATS KV
- **Stream-level authorization** — glob patterns (`chat_room_*`, `*`) in JWT claims
- **Message replay on reconnect** using JetStream sequence IDs
- **Presence tracking** via NATS KV with automatic TTL-based cleanup
- **Prometheus metrics** for connections, fan-out latency, NATS consumer lag
- **Graceful shutdown** — drains connections on SIGTERM with zero message loss
- **Per-IP connection limiting** to prevent resource exhaustion
- **SO_REUSEPORT + TCP_NODELAY** for optimal kernel-level performance
- **jemalloc** allocator for predictable memory usage under high load

## Requirements

- Rust stable (1.78+)
- NATS Server 2.10+ with JetStream enabled

See [docs/setup.md](docs/setup.md) for detailed installation instructions.

## Quick Start

```bash
# Install Rust via asdf
asdf plugin add rust
asdf install

# Start NATS with JetStream
nats-server --jetstream &

# Build and run (without auth — for quick testing)
cargo build
RUST_LOG=info cargo run

# Verify
curl http://localhost:9292/health
# => {"status":"ok","version":"0.1.0","connections":0}
```

### With JWT Authentication

```bash
# Generate RSA key pair
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out /tmp/tc_private.pem
openssl pkey -in /tmp/tc_private.pem -pubout -out /tmp/tc_public.pem

# Start with auth enabled
TURBOCABLE_JWT_PUBLIC_KEY_PATH=/tmp/tc_public.pem RUST_LOG=info cargo run
```

See [docs/jwt-authentication.md](docs/jwt-authentication.md) for generating test
tokens and the full testing walkthrough.

## Configuration

All options can be set via CLI flags or environment variables:

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--port` | `TURBOCABLE_PORT` | `9292` | Listen port |
| `--nats-url` | `TURBOCABLE_NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `--node-id` | `TURBOCABLE_NODE_ID` | `node_<uuid>` | Unique node identifier |
| `--ping-interval-secs` | `TURBOCABLE_PING_INTERVAL` | `30` | WebSocket ping interval (seconds) |
| `--max-connections-per-ip` | `TURBOCABLE_MAX_CONN_PER_IP` | `10` | Max concurrent connections per IP |
| `--jwt-public-key-path` | `TURBOCABLE_JWT_PUBLIC_KEY_PATH` | _(none)_ | Path to RSA public key PEM for JWT auth |

## Endpoints

| Path | Description |
|------|-------------|
| `GET /health` | Health check — returns `{"status":"ok","connections":N}` |
| `GET /cable` | WebSocket upgrade endpoint (pass `?token=<JWT>` when auth is enabled) |

## WebSocket Protocol

### Connecting

```bash
# JSON sub-protocol (default, ActionCable-compatible)
wscat -c 'ws://localhost:9292/cable?token=<JWT>'

# MessagePack binary sub-protocol
wscat -c 'ws://localhost:9292/cable?token=<JWT>' --subprotocol turbocable-v1-msgpack
```

### Subscribe

```json
{"command":"subscribe","identifier":"chat_room_42"}
```

Response (allowed):
```json
{"type":"confirm_subscription","identifier":"chat_room_42"}
```

Response (not in JWT `allowed_streams`):
```json
{"type":"reject_subscription","identifier":"chat_room_42"}
```

### Unsubscribe

```json
{"command":"unsubscribe","identifier":"chat_room_42"}
```

### JWT Claims

Tokens must be RS256-signed with these claims:

```json
{
  "sub": "user_42",
  "allowed_streams": ["chat_room_*", "notifications"],
  "exp": 1710000000,
  "iat": 1709996400
}
```

Stream authorization uses glob patterns: `"*"` matches any stream,
`"chat_room_*"` matches any stream starting with `chat_room_`.

### Close Codes

| Code | Meaning |
|------|---------|
| `3000` | Authentication failed (no token, expired, invalid signature) |
| `1008` | Per-IP connection limit exceeded |

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

## Project Structure

```
src/
├── main.rs                 # jemalloc, Tokio runtime, startup
├── config.rs               # CLI/env configuration
├── server.rs               # Axum router, SO_REUSEPORT listener
├── errors.rs               # Typed error hierarchy
├── auth/
│   ├── jwt.rs              # RS256 JWT verification, stream glob matching
│   └── key_watcher.rs      # NATS KV watcher + file fallback, hot-reload
├── connection/
│   ├── handler.rs          # WebSocket upgrade, per-connection lifecycle
│   ├── registry.rs         # DashMap registry (the core data structure)
│   └── limiter.rs          # Per-IP connection limits
├── protocol/
│   ├── types.rs            # ClientCommand / ServerMessage enums
│   ├── json.rs             # ActionCable-compatible JSON codec
│   └── msgpack.rs          # Binary codec (rmp-serde)
└── metrics/
    └── mod.rs              # Prometheus metrics (stub)
```

## Documentation

| Document | Description |
|----------|-------------|
| [docs/architecture.md](docs/architecture.md) | System design, data flow, why Rust, capacity planning |
| [docs/setup.md](docs/setup.md) | Prerequisites, installation, configuration reference |
| [docs/jwt-authentication.md](docs/jwt-authentication.md) | JWT auth, stream authorization, manual testing guide |

## Related Packages

| Package | Description |
|---------|-------------|
| [turbocable](https://github.com/samaswin/turbocable) | Ruby gem — NATS publisher for broadcasting |
| [turbocable-rails](https://github.com/samaswin/turbocable-rails) | Rails DSL for TurboCable broadcasts |
| [@turbocable/client](https://github.com/samaswin/turbocable-client) | JavaScript client for browser connections |

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
