# Development Setup

Prerequisites and setup instructions for building and running turbocable-server
locally.

---

## Table of Contents

- [System Requirements](#system-requirements)
- [Install Rust (via asdf)](#install-rust-via-asdf)
- [Install NATS Server](#install-nats-server)
- [OS Tuning](#os-tuning)
- [Clone and Build](#clone-and-build)
- [Running the Server](#running-the-server)
- [Configuration Reference](#configuration-reference)
- [Development Tools](#development-tools)
- [Verifying the Setup](#verifying-the-setup)
- [Related Documentation](#related-documentation)

---

## System Requirements

- **OS**: macOS (Apple Silicon or Intel) or Linux (x86_64 / ARM64)
- **Rust**: stable 1.88+
- **NATS Server**: 2.10+ with JetStream enabled
- **asdf**: version manager (recommended for Rust toolchain)

---

## Install Rust (via asdf)

The project pins Rust via `.tool-versions` so all developers use the same
toolchain.

```bash
# Install the asdf Rust plugin
asdf plugin add rust

# Install the version pinned in .tool-versions
asdf install

# Verify
rustc --version   # should show stable 1.88+
cargo --version
```

### Alternative: rustup

If you prefer rustup over asdf:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default stable
```

---

## Install NATS Server

NATS with JetStream is required for the key watcher (Phase 5+) and message
fan-out (Phase 6+).

### macOS

```bash
brew install nats-server
```

### Linux (x86_64)

```bash
curl -L https://github.com/nats-io/nats-server/releases/latest/download/nats-server-v2.10.x-linux-amd64.zip -o nats.zip
unzip nats.zip
sudo mv nats-server /usr/local/bin/
rm nats.zip
```

### Install NATS CLI (optional, useful for debugging)

```bash
# macOS
brew install nats-io/nats-tools/nats

# Linux
curl -L https://github.com/nats-io/natscli/releases/latest/download/nats-0.1.1-linux-amd64.zip -o natscli.zip
unzip natscli.zip
sudo mv nats /usr/local/bin/
```

### Start NATS with JetStream

```bash
nats-server --jetstream
```

Verify it's running:

```bash
nats server check   # should show OK
```

For development, you can add `-DV` for verbose debug logging:

```bash
nats-server --jetstream -DV
```

---

## OS Tuning

turbocable-server is designed to handle up to 1M concurrent connections. Even
for local development, raising the file descriptor limit avoids surprises.

### macOS / Linux (current shell session)

```bash
ulimit -n 100000
```

### macOS (persistent via launchd)

```bash
sudo launchctl limit maxfiles 200000 unlimited
```

### Linux (persistent via systemd)

Add to the systemd unit file:

```ini
[Service]
LimitNOFILE=2000000
```

Or set system-wide in `/etc/security/limits.conf`:

```
*  soft  nofile  200000
*  hard  nofile  2000000
```

And in `/etc/sysctl.conf`:

```
net.core.somaxconn=65535
```

> **Note**: turbocable-server automatically attempts to raise its own fd limit
> to 2,000,000 on startup. The OS hard limit must be at least that high for
> production deployments.

---

## Clone and Build

```bash
git clone <repo-url> turbocable-server
cd turbocable-server

# Build (debug mode — faster compile, slower runtime)
cargo build

# Build (release mode — slower compile, optimised runtime)
cargo build --release
```

The first build downloads and compiles all dependencies and may take a few minutes.
On glibc Linux, jemalloc is also compiled; on musl Linux and macOS the system allocator is used.

---

## Running the Server

### Minimal (no auth, defaults)

```bash
cargo run
```

The server starts on port 9292 with auth disabled. Useful for testing
WebSocket connectivity during early development.

### With JWT authentication

```bash
# Generate a test RSA key pair
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out /tmp/tc_private.pem
openssl pkey -in /tmp/tc_private.pem -pubout -out /tmp/tc_public.pem

# Start with auth enabled
TURBOCABLE_JWT_PUBLIC_KEY_PATH=/tmp/tc_public.pem cargo run
```

See [JWT Authentication](jwt-authentication.md) for full auth documentation.

### With debug logging

```bash
RUST_LOG=debug cargo run
```

### Custom port

```bash
TURBOCABLE_PORT=8080 cargo run
```

---

## Configuration Reference

All options can be set via environment variables or CLI arguments.

| Environment Variable | CLI Argument | Default | Description |
|---------------------|--------------|---------|-------------|
| `TURBOCABLE_PORT` | `--port` | `9292` | HTTP/WebSocket listen port |
| `TURBOCABLE_NATS_URL` | `--nats-url` | `nats://localhost:4222` | NATS server connection URL |
| `TURBOCABLE_NODE_ID` | `--node-id` | `node_<uuid>` | Unique node identifier (auto-generated if not set) |
| `TURBOCABLE_PING_INTERVAL` | `--ping-interval-secs` | `30` | WebSocket ping interval in seconds |
| `TURBOCABLE_MAX_CONN_PER_IP` | `--max-connections-per-ip` | `10` | Maximum concurrent connections per IP address |
| `TURBOCABLE_JWT_PUBLIC_KEY_PATH` | `--jwt-public-key-path` | _(none)_ | Path to RSA public key PEM file for JWT verification |
| `TURBOCABLE_MAX_ACK_PENDING` | `--max-ack-pending` | `10000` | Max unacknowledged NATS JetStream messages (back-pressure) |
| `TURBOCABLE_NATS_STREAM_REPLICAS` | `--nats-stream-replicas` | `1` | JetStream stream replica count (use 3 in production) |
| `RUST_LOG` | — | `info` | Log level filter (`error`, `warn`, `info`, `debug`, `trace`) |

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

---

## Development Tools

### Linting

```bash
cargo clippy -- -D warnings
```

### Formatting

```bash
# Check formatting
cargo fmt --check

# Apply formatting
cargo fmt
```

### Running tests

```bash
# All tests
cargo test

# With output visible
cargo test -- --nocapture

# A specific test
cargo test auth::jwt::tests::valid_token_accepted
```

### WebSocket testing

Install `wscat` for quick manual WebSocket testing:

```bash
npm install -g wscat
```

Connect to the server:

```bash
# Without auth
wscat -c ws://localhost:9292/cable

# With auth
wscat -c "ws://localhost:9292/cable?token=<JWT>"

# With msgpack sub-protocol
wscat -c ws://localhost:9292/cable --subprotocol turbocable-v1-msgpack
```

---

## Verifying the Setup

Run through this checklist after initial setup to confirm everything works.

### 1. Build with zero warnings

```bash
cargo build
cargo clippy -- -D warnings
cargo fmt --check
```

### 2. All tests pass

```bash
cargo test
```

### 3. Health check responds

```bash
# In one terminal
cargo run

# In another terminal
curl -s localhost:9292/health | jq .
```

Expected:

```json
{
  "status": "ok",
  "version": "0.4.0",
  "connections": 0
}
```

### 4. WebSocket connects

```bash
wscat -c ws://localhost:9292/cable
```

Expected: receives `{"type":"welcome"}` (when auth is disabled).

### 5. Structured JSON logs appear

```bash
RUST_LOG=info cargo run
```

Logs should be JSON-formatted on stdout:

```json
{"timestamp":"...","level":"INFO","fields":{"message":"listening on 0.0.0.0:9292 (SO_REUSEPORT enabled)"},"target":"turbocable_server::server"}
```

### 6. NATS server reachable (when running)

```bash
nats-server --jetstream &
nats server check
```

> If NATS is not running, the server still starts — auth falls back to
> file-based key or runs without auth, and pub/sub is disabled. Warnings
> are logged.

### 7. NATS JetStream fan-out works

With NATS running and the gateway started:

```bash
# Terminal A: connect a WebSocket client
wscat -c ws://localhost:9292/cable
# After receiving {"type":"welcome"}, subscribe:
# > {"command":"subscribe","identifier":"chat_room_1"}
# Should receive: {"type":"confirm_subscription","identifier":"chat_room_1"}

# Terminal B: publish a NATS message
nats pub TURBOCABLE.chat_room_1 '{"text":"hello from NATS"}'
```

Terminal A should immediately receive:

```json
{"type":"message","identifier":"chat_room_1","message":{"text":"hello from NATS"},"seq":1}
```

### 8. NATS stream was created

```bash
nats stream info TURBOCABLE
```

Should show the stream configuration (subjects, storage type, replicas) and
current message count.

### 9. Message replay works

```bash
# Disconnect wscat (Ctrl+C), then publish while disconnected:
nats pub TURBOCABLE.chat_room_1 '{"text":"missed message"}'

# Reconnect:
wscat -c ws://localhost:9292/cable
# > {"type":"hello","last_seq":"1"}
# > {"command":"subscribe","identifier":"chat_room_1"}
# Should receive the missed message with "replayed":true before confirm
```

See [NATS JetStream Integration](nats-jetstream.md) for full details.

---

## Running via Docker

Pre-built multi-platform images (linux/amd64 and linux/arm64) are published to
the GitHub Container Registry on every release:

```bash
docker pull ghcr.io/turbocable/gateway:latest
docker run -p 9292:9292 \
  -e TURBOCABLE_NATS_URL=nats://host.docker.internal:4222 \
  ghcr.io/turbocable/gateway:latest
```

Build locally from source (produces a fully static ~12 MB image):

```bash
docker build -t turbocable-server .
```

See [binary-distribution.md](binary-distribution.md) for the full release pipeline and cross-compilation details.

---

## Related Documentation

- [Architecture Overview](architecture.md) — system design, data flow, and capacity planning
- [JWT Authentication](jwt-authentication.md) — token format, key rotation, and auth testing
- [NATS JetStream Integration](nats-jetstream.md) — fan-out pipeline, replay, and NATS configuration
- [Graceful Shutdown](graceful-shutdown.md) — SIGTERM handling, drain testing, Kubernetes configuration
- [Binary Distribution](binary-distribution.md) — cross-compilation targets, Docker image, release pipeline
