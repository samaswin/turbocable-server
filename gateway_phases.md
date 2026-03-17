# turbocable-server — Implementation Phases
### Rust WebSocket Gateway · Build This First · Target: 1M Connections

---

Note: the project name is turbocable-server. use this all places

## Prerequisites Before Writing Any Code - Completed

```bash
# Install tools via asdf
asdf plugin add rust
asdf install rust stable

# Verify
rustc --version   # stable 1.78+
cargo --version

# Install NATS server (local dev)
# macOS
brew install nats-server
# Linux
curl -L https://github.com/nats-io/nats-server/releases/latest/download/nats-server-v2.10.x-linux-amd64.zip -o nats.zip
unzip nats.zip && mv nats-server /usr/local/bin/

# Verify NATS
nats-server --jetstream &
nats server check   # should show OK

# OS tuning (dev machine — at least raise fd limit)
ulimit -n 100000
```

---

## Phase 1 — Skeleton, Config, and Health Check - Completed
**Goal:** Binary starts, reads config, responds to `/health`. Nothing else.
**Duration:** 2–3 days
**Done when:** `cargo run` starts the server and `curl localhost:9292/health` returns `{"status":"ok"}`.

### 1.1 Cargo.toml — All Dependencies Upfront
Add every dependency now. Avoid mid-project Cargo.lock churn.

```toml
[package]
name    = "turbocable-server"
version = "0.1.0"
edition = "2021"

[dependencies]
jemallocator      = "0.5"
tokio             = { version = "1",   features = ["full"] }
axum              = { version = "0.7", features = ["ws"] }
async-nats        = "0.35"
rmp-serde         = "1.3"
serde             = { version = "1", features = ["derive"] }
serde_json        = "1"
jsonwebtoken      = "9"
dashmap           = "5"
bytes             = "1"
smallvec          = { version = "1", features = ["union"] }
prometheus        = { version = "0.13", features = ["process"] }
tracing           = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
clap              = { version = "4", features = ["derive", "env"] }
thiserror         = "1"
uuid              = { version = "1", features = ["v4", "fast-rng"] }
num_cpus          = "1"
socket2           = "0.5"
rlimit            = "0.10"

[dev-dependencies]
tokio-test = "0.4"

[profile.release]
opt-level     = 3
lto           = "thin"
codegen-units = 1
panic         = "abort"
strip         = "symbols"
```

`.cargo/config.toml`:
```toml
[build]
rustflags = ["-D", "warnings"]
```

### 1.2 main.rs — Allocator + Runtime
```rust
// src/main.rs
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod config;
mod errors;
mod metrics;
mod server;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let cfg = config::Config::parse();
    raise_fd_limit(2_000_000);

    tracing::info!(port = cfg.port, node_id = %cfg.node_id, "starting");
    server::run(cfg).await;
}

fn raise_fd_limit(target: u64) {
    if let Ok((soft, hard)) = rlimit::Resource::NOFILE.get() {
        if soft < target {
            let new = target.min(hard);
            rlimit::Resource::NOFILE.set(new, hard).ok();
            tracing::info!("fd limit raised to {new}");
        }
    }
}
```

### 1.3 Config
```rust
// src/config.rs
#[derive(clap::Parser, Debug, Clone)]
#[command(name = "turbocable-server", version, about)]
pub struct Config {
    #[arg(long, env = "TURBOCABLE_PORT", default_value = "9292")]
    pub port: u16,

    #[arg(long, env = "TURBOCABLE_NATS_URL", default_value = "nats://localhost:4222")]
    pub nats_url: String,

    #[arg(long, env = "TURBOCABLE_NODE_ID", default_value_t = default_node_id())]
    pub node_id: String,

    #[arg(long, env = "TURBOCABLE_PING_INTERVAL", default_value = "30")]
    pub ping_interval_secs: u64,
}

fn default_node_id() -> String {
    format!("node_{}", uuid::Uuid::new_v4().simple())
}
```

### 1.4 Errors
```rust
// src/errors.rs
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("auth failed: {0}")]
    Auth(String),
    #[error("NATS error: {0}")]
    Nats(#[from] async_nats::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}
```

### 1.5 Server with /health Only
```rust
// src/server.rs
use axum::{routing::get, Json, Router};
use std::sync::Arc;

pub async fn run(cfg: config::Config) {
    let app = Router::new()
        .route("/health", get(health));

    let addr = format!("0.0.0.0:{}", cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    tracing::info!("listening on {addr}");
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}
```

### 1.6 Phase 1 Checklist
- [ ] `cargo build` with zero warnings
- [ ] `cargo clippy -- -D warnings` passes
- [ ] `cargo fmt --check` passes
- [ ] `curl localhost:9292/health` returns 200
- [ ] `TURBOCABLE_PORT=9393 cargo run` — respects env var
- [ ] Structured JSON logs appear on stdout

---

## Phase 2 — Connection Registry - Completed
**Goal:** The DashMap-based registry that will hold 1M connections.
**Duration:** 2–3 days
**Done when:** Unit tests pass for register, fanout, subscribe, unsubscribe with 10k entries.

### 2.1 Registry
```
src/connection/
├── mod.rs
└── registry.rs
```

Implement `Registry` with:
- `DashMap<u64, Sender<Bytes>>` for senders (64 shards, pre-allocated capacity 1.1M)
- `DashMap<String, SmallVec<[u64; 32]>>` for stream → conn_id lists
- `AtomicU64` for conn_id generation and active count
- `allocate_id()`, `register()`, `deregister()`, `subscribe()`, `unsubscribe()`
- `fanout()` — the hot path, no allocations, `try_send` only

### 2.2 Unit Tests for Registry
```rust
#[tokio::test]
async fn register_and_fanout_to_1000() { ... }

#[tokio::test]
async fn slow_client_does_not_block_fanout() { ... }

#[tokio::test]
async fn deregister_cleans_up_from_all_streams() { ... }

#[tokio::test]
async fn concurrent_subscribe_from_multiple_tasks() { ... }
```

### 2.3 Phase 2 Checklist
- [ ] All registry unit tests pass
- [ ] `cargo test --release` also passes
- [ ] No `unsafe` blocks
- [ ] Clippy clean
- [ ] `connection_count()` returns correct value after concurrent register/deregister

---

## Phase 3 — Protocol Codecs - Completed
**Goal:** Encode and decode both JSON (ActionCable-compat) and MessagePack frames.
**Duration:** 2 days
**Done when:** Round-trip encode/decode tests pass for all command types.

### 3.1 Types
```
src/protocol/
├── mod.rs
├── types.rs      ← Command enum, ClientMessage struct, ServerMessage struct
├── json.rs       ← ActionCable-compatible JSON codec
└── msgpack.rs    ← Binary codec via rmp-serde
```

### 3.2 Command Types
```rust
// src/protocol/types.rs
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ClientCommand {
    Subscribe   { identifier: String },
    Unsubscribe { identifier: String },
    Message     { identifier: String, data: String },
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    ConfirmSubscription { identifier: String },
    RejectSubscription  { identifier: String },
    Ping                { message: u64 },
    Welcome,
    Disconnect          { reason: String },
    Message             { identifier: String, message: serde_json::Value },
}
```

### 3.3 Unit Tests for Codecs
- JSON decode of all ActionCable command shapes
- MessagePack round-trip for each command
- Malformed input returns `Err` not panic
- Unknown command type is handled gracefully

### 3.4 Phase 3 Checklist
- [ ] All codec unit tests pass
- [ ] Fuzz test with random bytes does not panic (`cargo fuzz` optional but recommended)
- [ ] Both codecs handle missing fields gracefully
- [ ] Clippy clean

---

## Phase 4 — WebSocket Accept Loop - Completed
**Goal:** Accept WebSocket connections, parse sub-protocol, route to correct codec.
**No auth yet. No NATS yet.** Just accept → parse one command → log it.
**Duration:** 2–3 days
**Done when:** `wscat -c ws://localhost:9292/cable` connects and you see commands logged.

### 4.1 WS Handler (No Auth)
```
src/connection/
├── mod.rs
├── registry.rs
└── handler.rs    ← new
```

The handler:
1. Extracts `?token=` query param (store it, don't verify yet)
2. Negotiates sub-protocol (`actioncable-v1-json` or `turbocable-v1-msgpack`)
3. Registers connection in registry with a bounded channel (capacity 16)
4. Spawns outbound task (drains mpsc → sends to WS)
5. Inbound loop: receives frames, dispatches to protocol handler
6. On close/error: unsubscribes all streams, deregisters

### 4.2 SO_REUSEPORT Listener
Replace the basic `TcpListener::bind` from Phase 1 with the socket2-based
`SO_REUSEPORT` + `TCP_NODELAY` version. Do this now — retrofitting later is painful.

### 4.3 Connection Limiter
```rust
// src/connection/limiter.rs
// Per-IP connection limit (configurable, default 10 per IP)
// Prevent one client from exhausting all capacity
pub struct ConnectionLimiter {
    counts: DashMap<IpAddr, AtomicU64>,
    max_per_ip: u64,
}
```

### 4.4 Manual Tests
```bash
# Connect 10 clients
for i in {1..10}; do
  wscat -c ws://localhost:9292/cable &
done

# Check health shows 10 active
curl localhost:9292/health | jq .connections
```

### 4.5 Phase 4 Checklist
- [ ] WS upgrade works with both JSON and msgpack sub-protocols
- [ ] Connection count increments/decrements correctly
- [ ] Disconnecting client cleans up from registry
- [ ] Per-IP limiter rejects over-limit connections with close(1008)
- [ ] SO_REUSEPORT confirmed active in `ss -tlnp`
- [ ] `TCP_NODELAY` set on all accepted sockets

---

## Phase 5 — JWT Authentication
**Goal:** Verify RS256 JWT on every connection. Reject invalid tokens. Cache public key.
**Duration:** 2 days
**Done when:** Connection with valid JWT succeeds, expired/invalid token is rejected with close(3000).

### 5.1 JWT Verifier
```
src/auth/
├── mod.rs
├── jwt.rs          ← RS256 verify, stream glob matching
└── key_watcher.rs  ← Watch NATS KV TC_PUBKEYS, hot-reload on key change
```

JWT Claims:
```rust
#[derive(Debug, serde::Deserialize)]
pub struct Claims {
    pub sub:             String,        // user_id as string
    pub allowed_streams: Vec<String>,   // glob patterns: ["chat_room_*"]
    pub exp:             usize,
    pub iat:             usize,
}
```

Stream authorization on subscribe:
```rust
pub fn is_allowed(allowed: &[String], stream: &str) -> bool {
    allowed.iter().any(|pat| {
        if pat == "*" { return true; }
        match pat.strip_suffix('*') {
            Some(prefix) => stream.starts_with(prefix),
            None         => pat == stream,
        }
    })
}
```

### 5.2 Key Watcher (NATS KV)
On startup: connect to NATS, fetch `TC_PUBKEYS.rails_public_key`, cache PEM.
Watch key for updates, hot-reload verifier within seconds.
Fallback: accept `TURBOCABLE_JWT_PUBLIC_KEY_PATH` env var for local dev
(no NATS required for Phase 5 testing).

### 5.3 Unit Tests for Auth
```rust
#[test] fn valid_token_accepted()
#[test] fn expired_token_rejected()
#[test] fn wrong_algorithm_rejected()   // HS256 signed token
#[test] fn tampered_signature_rejected()
#[test] fn stream_glob_chat_room_star_matches_chat_room_42()
#[test] fn stream_glob_star_matches_anything()
#[test] fn stream_glob_exact_rejects_partial()
```

### 5.4 Phase 5 Checklist
- [ ] All auth unit tests pass
- [ ] Valid JWT → connection accepted → `confirm_subscription` sent
- [ ] Expired JWT → WebSocket close(3000, "token expired")
- [ ] Invalid signature → WebSocket close(3000, "auth failed")
- [ ] Key hot-reload: change PEM in NATS KV → new connections use new key within 5s
- [ ] Old connections with still-valid JWT unaffected by key rotation

---

## Phase 6 — NATS JetStream Integration
**Goal:** Gateway consumes from NATS and fans out to subscribers.
**This is the core of the whole product.**
**Duration:** 3–4 days
**Done when:** Rail publishes to NATS → connected client receives message in under 50ms.

### 6.1 NATS Consumer
```
src/pubsub/
├── mod.rs
└── nats.rs    ← push consumer, auto-reconnect, ack after fanout
```

Stream setup on startup:
- Stream name: `TURBOCABLE`
- Subjects: `TURBOCABLE.>`
- Storage: File, 7-day retention, 3 replicas
- Consumer: durable push consumer named `gw_{node_id}`

Fan-out loop:
```
msg arrives from NATS
→ strip "TURBOCABLE." prefix → stream_name
→ registry.fanout(stream_name, Bytes::copy_from_slice(&msg.payload))
→ msg.ack()
```

Auto-reconnect outer loop: on any NATS error, sleep 2s, reconnect.

### 6.2 Message Replay on Reconnect
When client sends `{ type: "hello", last_seq: "8841" }`:
1. For each stream the client subscribes to, fetch messages with seq > 8841
2. Deliver in order with `replayed: true` flag
3. Then resume live push consumer

### 6.3 Integration Test (Manual)
```bash
# Terminal 1: NATS
nats-server --jetstream

# Terminal 2: Gateway
RUST_LOG=debug cargo run

# Terminal 3: Subscribe a client
wscat -c 'ws://localhost:9292/cable?token=<valid_jwt>'
# Send: {"command":"subscribe","identifier":"{\"channel\":\"ChatChannel\",\"room_id\":1}"}

# Terminal 4: Publish from Ruby or nats CLI
nats pub TURBOCABLE.chat_room_1 '{"message":"hello"}'
# Terminal 3 should receive the message
```

### 6.4 Phase 6 Checklist
- [ ] NATS stream created on startup if not exists
- [ ] Consumer reconnects automatically on NATS failure
- [ ] Fanout latency measured: p99 < 20ms at 1k subscribers on local machine
- [ ] Message replay delivers correct messages in order after reconnect
- [ ] `max_ack_pending` set to prevent NATS overwhelming slow gateway
- [ ] Consumer lag exposed in metrics (`nats_consumer_lag` gauge)

---

## Phase 7 — Prometheus Metrics + /metrics Endpoint
**Goal:** All key metrics instrumented. Grafana dashboard template ready.
**Duration:** 1–2 days
**Done when:** `curl localhost:9292/metrics` returns valid Prometheus text format.

### 7.1 Metrics to Implement
```rust
pub struct Metrics {
    pub connections_active:   GenericGauge<AtomicI64>,    // current open connections
    pub connections_total:    GenericCounter<AtomicU64>,  // total since startup
    pub connections_rejected: GenericCounter<AtomicU64>,  // auth failures + limit
    pub messages_fanned_out:  GenericCounter<AtomicU64>,  // frames sent to clients
    pub messages_dropped:     GenericCounter<AtomicU64>,  // slow client drops
    pub fanout_duration_ms:   Histogram,                  // p50/p95/p99 fan-out time
    pub nats_consumer_lag:    GenericGauge<AtomicI64>,    // NATS messages pending
    pub auth_duration_ms:     Histogram,                  // JWT verify time
}
```

### 7.2 /pubkey Endpoint
```
GET /pubkey  → returns current RS256 public key PEM
```
Used by Rails to verify it and the gateway are in sync during debugging.

### 7.3 Phase 7 Checklist
- [ ] All metrics increment correctly under manual testing
- [ ] `/metrics` returns valid Prometheus format (test with `promtool check metrics`)
- [ ] Grafana dashboard JSON template committed to `infra/grafana/`
- [ ] Alert rules defined: connections_active > 900k, fanout p99 > 100ms

---

## Phase 8 — Graceful Shutdown
**Goal:** SIGTERM drains connections cleanly. No lost messages during deploy.
**Duration:** 1 day
**Done when:** `kill -TERM <pid>` closes all connections with WS close(1001) within 30s.

### 8.1 Shutdown Sequence
```
1. Receive SIGTERM
2. Stop accepting new connections
3. Send WS close(1001, "server shutting down") to all connections
4. Wait up to 30s for clients to acknowledge close
5. Force-close remaining connections
6. Drain NATS connection (flush pending acks)
7. Exit 0
```

### 8.2 Phase 8 Checklist
- [ ] `kill -TERM` triggers graceful shutdown
- [ ] JS client receives close(1001) and immediately reconnects to another node
- [ ] No NATS messages unacknowledged on clean shutdown
- [ ] Kubernetes `terminationGracePeriodSeconds: 35` aligns with 30s drain

---

## Phase 9 — Presence (NATS KV)
**Goal:** Gateway reads/writes presence via NATS KV bucket `TC_PRESENCE`.
**Duration:** 1–2 days
**Done when:** Subscribe to a stream → presence entry appears in KV. Disconnect → expires in 30s.

### 9.1 Presence Module
```
src/presence/
└── mod.rs
```

On subscribe: write `TC_PRESENCE.{stream}.{user_id}` with TTL 30s.
On disconnect: delete key (best-effort).
Heartbeat: gateway refreshes TTL every 25s for connected clients.

### 9.2 Phase 9 Checklist
- [ ] Presence key written on subscribe
- [ ] Presence key deleted on unsubscribe/disconnect
- [ ] Key expires naturally (30s TTL) if gateway crashes — no stale presence
- [ ] Heartbeat prevents premature expiry for long-lived connections

---

## Phase 10 — Load Testing
**Goal:** Validate 1M connections. Measure fan-out latency. Profile memory.
**Duration:** 1 week (infrastructure setup + iteration)
**Done when:** 3-node cluster sustains 1M connections for 10 minutes with p99 < 50ms.

### 10.1 Single-Node Baseline
Target: 333k connections, p99 < 30ms, < 8 KB/connection
```bash
k6 run bench/k6/load_1m.js \
  -e TARGET=333000 \
  -e GATEWAY_WSS_URL=ws://node1:9292/cable
```

### 10.2 3-Node Cluster
Target: 1M connections, p99 < 50ms, zero message loss
```bash
# Run 10 k6 agents simultaneously, each targeting 100k connections
k6 run bench/k6/load_1m.js -e TARGET=100000 -e GATEWAY_WSS_URL=wss://lb.example.com/cable
```

### 10.3 Memory Profile
```bash
PID=$(pgrep turbocable-server)
while true; do
  RSS=$(awk '/VmRSS/{print $2}' /proc/$PID/status)
  CONNS=$(curl -s localhost:9292/metrics | grep 'connections_active ' | awk '{print $2}')
  echo "conns=$CONNS rss=${RSS}kB per=$(echo "scale=1;$RSS/$CONNS" | bc)kB"
  sleep 15
done
```

### 10.4 Performance Tuning Checklist
If you miss targets, check in this order:
- [ ] `jemallocator` is linked (verify with `nm binary | grep jemalloc`)
- [ ] `LimitNOFILE=2000000` in systemd unit
- [ ] `net.core.somaxconn=65535` applied
- [ ] Channel capacity ≤ 16 (reduce if memory over budget)
- [ ] `DashMap` shard count — try 128 if you see lock contention in flamegraphs
- [ ] NATS `max_ack_pending` set appropriately

### 10.5 Phase 10 Checklist
- [ ] 333k connections sustained (1 node, 10 min)
- [ ] 1M connections sustained (3 nodes, 10 min)
- [ ] Fan-out p99 < 50ms at 1M
- [ ] Memory < 8 KB/connection
- [ ] Zero message loss (verified with sequence IDs)
- [ ] Reconnect replay verified: client reconnects → receives all missed messages
- [ ] Graceful rolling restart: restart 1 node → clients reconnect → zero messages lost

---

## Phase 11 — Binary Distribution Prep
**Goal:** Release binary ready for all platforms. Docker image published.
**Duration:** 2 days
**Done when:** `docker pull turbocable/gateway:latest` works on Linux x86_64 and ARM64.

### 11.1 Cross-Compile Targets
```
x86_64-unknown-linux-musl    ← fully static, no glibc
aarch64-unknown-linux-musl   ← ARM64 Linux (AWS Graviton)
aarch64-apple-darwin         ← Apple Silicon
x86_64-apple-darwin          ← Intel Mac
```

### 11.2 Dockerfile (scratch image, ~12 MB)
```dockerfile
FROM rust:1.78-slim AS builder
RUN apt-get update && apt-get install -y musl-tools
RUN rustup target add x86_64-unknown-linux-musl
WORKDIR /build
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

FROM scratch
COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/turbocable-server /gateway
EXPOSE 9292
ENTRYPOINT ["/gateway"]
```

### 11.3 Phase 11 Checklist
- [ ] All 4 platform binaries build cleanly in CI
- [ ] Linux musl binaries verified static: `ldd binary` shows "not a dynamic executable"
- [ ] Docker image < 20 MB
- [ ] Docker image published to ghcr.io/turbocable/gateway
- [ ] Binaries copied into `turbocable-server` gem `binaries/` directories

---

## Overall Gateway Milestones

| Phase | Deliverable | Week |
|-------|-------------|------|
| 1 | Binary starts, /health works | 1 |
| 2 | Registry unit tested | 1 |
| 3 | Protocol codecs tested | 1–2 |
| 4 | WS accept loop, SO_REUSEPORT | 2 |
| 5 | JWT auth, key hot-reload | 2–3 |
| 6 | NATS fan-out, message replay | 3–4 |
| 7 | Prometheus metrics | 4 |
| 8 | Graceful shutdown | 4 |
| 9 | Presence (NATS KV) | 5 |
| 10 | 1M load test validated | 6–7 |
| 11 | Binary distribution | 7–8 |

**Start writing gems at the end of Phase 6** (gateway can fan-out messages).
**Do not publish gems until Phase 10 passes** (1M test validated).
