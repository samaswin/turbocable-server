# TurboCable Architecture

## Why TurboCable Exists

WebSocket servers hit a hard ceiling when connections grow. Most backend
frameworks handle WebSockets in-process — each connection consumes a thread or
coroutine, memory grows linearly, and the broadcast bus becomes the bottleneck.
Scaling to 100k+ connections means running many backend processes, all contending
on the same pub/sub channels.

TurboCable takes a different approach: **move all WebSocket connections out of
your backend entirely**. A single Rust gateway process holds hundreds of thousands
of connections. Your backend publishes one NATS message per broadcast and moves
on — it never touches WebSockets, never knows how many users are connected, and
never becomes the bottleneck.

### The core insight

```
Traditional:  1 broadcast → backend iterates N connections → N pub/sub messages → slow
TurboCable:   1 broadcast → 1 NATS publish → Rust fans out to N connections → fast
```

Your backend does O(1) work per broadcast. The Rust gateway does O(N) fan-out using
zero-copy `Bytes` cloning and lock-free `DashMap` shards — completing a fan-out
to 333k connections in under 10ms.

---

## System Diagram

![TurboCable Architecture](turbocable_architecture.svg)

---

## How It Works

### The data flow

```
1. User sends a chat message
   → Backend server saves to DB
   → Backend publishes to NATS subject TURBOCABLE.chat_room_42

2. NATS publisher (e.g. turbocable gem)
   → MessagePack-encodes the payload
   → Publishes to NATS subject TURBOCABLE.chat_room_42
   → Returns in ~5μs (one TCP write)

3. NATS JetStream
   → Persists the message (7-day retention, 3 replicas)
   → Pushes to gateway consumers

4. Rust gateway (turbocable-server)
   → Receives the NATS message
   → Strips "TURBOCABLE." prefix → "chat_room_42"
   → DashMap lookup: "chat_room_42" → [conn_1, conn_2, ... conn_N]
   → For each connection: tx.try_send(frame.clone())
     (Bytes::clone = 1 atomic refcount increment, ~1ns)
   → Acknowledges the NATS message

5. Client (@turbocable/client)
   → Receives the WebSocket frame
   → Decodes MessagePack (or JSON)
   → Calls received(data) callback
```

### What each component is responsible for

| Component | Responsibility | Does NOT do |
|-----------|---------------|-------------|
| **Backend app** | Business logic, DB writes, signs JWTs, publishes 1 NATS message per broadcast | Hold WebSocket connections, fan-out, presence |
| **NATS publisher** (e.g. turbocable gem) | Thin NATS publisher wrapper | WebSocket handling, connection management |
| **NATS JetStream** | Message persistence, delivery guarantees, replay, KV storage for presence and keys | Fan-out to clients, authentication |
| **Rust gateway** | Holds all WebSocket connections, JWT auth, fan-out, presence heartbeats, metrics | Business logic, DB access |
| **JS client** | WebSocket connection, reconnect with backoff, message replay via sequence IDs | Server-side logic |

---

## Why Not Redis?

Traditional WebSocket servers use Redis pub/sub as the broadcast bus. At scale this creates
problems:

| | Redis pub/sub | NATS JetStream |
|---|---|---|
| **Message persistence** | Fire-and-forget — if subscriber is offline, message is lost | Persisted to disk, 7-day retention |
| **Replay on reconnect** | Not possible — must re-fetch from DB | Built-in: client sends `last_seq`, gateway replays |
| **Fan-out** | Redis broadcasts to all subscribers, each backend process re-fans to its connections | NATS delivers once to gateway, Rust fans out in-process |
| **Memory** | Redis holds all pub/sub state in memory | NATS uses file-backed storage, memory only for hot data |
| **Operational cost** | Redis cluster + Sentinel for HA | NATS cluster is self-healing, no external coordination |

Removing Redis simplifies the stack to: **Backend → NATS → Rust gateway**.

---

## Why Rust for the Gateway?

The gateway is the only component that must handle 1M concurrent connections.
This is primarily a memory and concurrency problem:

| Concern | Why Rust wins |
|---------|--------------|
| **Memory per connection** | ~6–8 KB (vs ~50–100 KB in Ruby/Go) |
| **Concurrency model** | Tokio async runtime — millions of tasks on 16 cores |
| **Zero-copy fan-out** | `bytes::Bytes::clone()` = 1 atomic increment, no heap allocation |
| **Lock-free registry** | `DashMap` with 64 shards — most operations touch only 1 shard |
| **Allocator** | jemalloc on glibc Linux — 15–20% throughput improvement over system allocator under sustained load (musl/macOS use the system allocator) |
| **No GC pauses** | Deterministic memory management — no stop-the-world pauses during fan-out |

### Memory budget at 1M connections

| Allocation | Per connection | 1M total |
|------------|---------------|----------|
| TCP socket (kernel) | ~3–4 KB | ~3.5 GB |
| Tokio task | ~1–2 KB | ~1.5 GB |
| Connection struct | ~256 bytes | ~256 MB |
| mpsc channel (capacity 16) | ~512 bytes | ~512 MB |
| **Total** | **~6–8 KB** | **~8–12 GB** |

A single 32 GB node comfortably holds 333k connections, and a 3-node cluster
reaches 1M.

---

## Gateway Internals

### Source layout

```
src/
├── main.rs                 # jemalloc (glibc Linux), Tokio runtime, startup
├── lib.rs                  # crate root — re-exports public modules
├── config.rs               # clap Config, env vars
├── server.rs               # Axum router, SO_REUSEPORT listener, NATS init
├── errors.rs               # Typed error hierarchy
├── metrics.rs              # Prometheus gauges, counters, histograms, /metrics handler
├── auth/
│   ├── jwt.rs              # RS256 verification, stream glob matching
│   └── key_watcher.rs      # NATS KV watcher, file fallback, hot-reload
├── connection/
│   ├── handler.rs          # WS upgrade, per-connection lifecycle
│   ├── registry.rs         # DashMap registry — the core data structure
│   └── limiter.rs          # Per-IP connection limits
├── presence/               # NATS KV-backed presence tracking (TTL-based)
├── protocol/
│   ├── types.rs            # ClientCommand / ServerMessage enums
│   ├── json.rs             # JSON codec (actioncable-v1-json sub-protocol)
│   └── msgpack.rs          # Binary codec (rmp-serde)
└── pubsub/
    └── nats.rs             # NatsConsumer: fanout, publish, replay
```

### Connection lifecycle

```
Client connects: GET /cable?token=<JWT>
  │
  ├─ Negotiate sub-protocol (JSON or MessagePack)
  ├─ Check per-IP connection limit
  ├─ Verify RS256 JWT → extract allowed_streams
  │   ├─ Invalid/expired → close(3000, reason)
  │   └─ Valid → continue
  │
  ├─ Allocate conn_id (atomic u64)
  ├─ Create bounded mpsc channel (capacity 16)
  ├─ Register in DashMap registry
  ├─ Send {"type":"welcome"}
  │
  ├─ Spawn outbound task: mpsc → WebSocket writes
  ├─ Run inbound loop:
  │     subscribe   → check allowed_streams → replay if reconnect → confirm or reject
  │     unsubscribe → remove from registry
  │     message     → publish to NATS JetStream (TURBOCABLE.{identifier})
  │     hello       → store last_seq for replay on next subscribe
  │     ping        → periodic server-initiated pings
  │
  └─ On close/error/shutdown:
       unsubscribe all streams
       deregister from registry
       release per-IP limiter slot

       SIGTERM path:
         outbound task receives shutdown watch → drains queue → sends close(1001)
         inbound loop receives shutdown watch → breaks
         server waits up to 30s for count → 0, then flushes NATS
```

### Fan-out hot path (zero allocation)

```rust
// 1 NATS message arrives from JetStream
// → strip "TURBOCABLE." prefix → stream_name
// → pre-encode as both JSON and MessagePack (one encoding per codec)
// → registry.fanout_encoded(stream_name, json_bytes, msgpack_bytes)

fn fanout_encoded(&self, stream: &str, json: Bytes, msgpack: Bytes) {
    // DashMap shard lock held for microseconds
    let targets = self.streams.get(stream);

    for conn_id in targets {
        let payload = if conn.is_binary { msgpack.clone() } else { json.clone() };
        // Bytes::clone() = 1 atomic refcount increment (~1ns)
        // try_send = non-blocking, never allocates
        conn.sender.try_send(payload);
        // If channel full → skip (slow client protection)
    }
}
```

### Message replay on reconnect

When a client reconnects and sends `{"type":"hello","last_seq":"8841"}`,
the gateway replays missed messages for each stream the client subscribes to:

```
Client reconnects → sends hello with last_seq
  → subscribes to "chat_room_42"
  → gateway creates ephemeral JetStream consumer at seq 8842
  → delivers missed messages with replayed: true
  → sends confirm_subscription
  → live delivery resumes (via the durable consumer)
```

See [NATS JetStream Integration](nats-jetstream.md) for full details.

---

## Authentication Flow

```
Backend app                          Gateway
    │                                   │
    │  Signs JWT with RS256 private key │
    │  Claims: { sub, allowed_streams,  │
    │            exp, iat }             │
    │                                   │
    │  Publishes public key to          │
    │  NATS KV: TC_PUBKEYS             │
    │                                   │
    │                                   │── Watches TC_PUBKEYS for hot-reload
    │                                   │── Or loads from file (local dev)
    │                                   │
    │         Client                    │
    │           │                       │
    │           │ GET /cable_token      │
    │           │─────────────────>│    │
    │           │ { token, url }   │    │
    │           │<─────────────────│    │
    │           │                       │
    │           │ WS /cable?token=JWT   │
    │           │──────────────────────>│── Verify RS256 signature
    │           │                       │── Check exp claim
    │           │                       │── Extract allowed_streams
    │           │     welcome           │
    │           │<──────────────────────│
    │           │                       │
    │           │ subscribe "chat_42"   │
    │           │──────────────────────>│── is_allowed(["chat_*"], "chat_42") ✓
    │           │ confirm_subscription  │
    │           │<──────────────────────│
```

Key rotation is zero-downtime: the gateway watches the NATS KV bucket and
atomically swaps the public key. Existing connections with still-valid JWTs
are unaffected.

---

## Protocol

### Sub-protocol negotiation

The client requests a sub-protocol via the `Sec-WebSocket-Protocol` header:

| Sub-protocol | Format | Use case |
|--------------|--------|----------|
| `actioncable-v1-json` | JSON text frames | Development, debugging, broad client compatibility |
| `turbocable-v1-msgpack` | Binary MessagePack frames | Production (smaller, faster) |

If no sub-protocol is requested, JSON is used as the default.

### NATS subject mapping

```
Backend:  publish to NATS subject TURBOCABLE.chat_room_42
NATS:     TURBOCABLE.chat_room_42
Gateway:  strip prefix → "chat_room_42" → registry.fanout()
Client:   subscribe to "chat_room_42"
```

The stream name in application code maps 1:1 to the NATS subject suffix.

---

## Capacity Planning

### Target: 1,000,000 concurrent connections

```
3 gateway nodes × 333k connections each = 1M

Per gateway node:
  CPU:    16 vCPU (Tokio multi-thread runtime)
  RAM:    32 GB
  Network: 10 GbE (25 GbE recommended)
  OS:     LimitNOFILE=2000000

NATS cluster:
  3 nodes (survives 1 node failure)
  RAM:    8 GB each
  Storage: SSD, 100 GB each

Backend:
  Existing cluster — no changes needed
  1 persistent NATS connection per process
```

### Performance targets

| Metric | Target | Measured at |
|--------|--------|-------------|
| Fan-out p99 latency | < 50ms | 1M connections |
| Memory per connection | < 8 KB | 333k per node |
| Connect time p99 | < 100ms | Under load |
| Message loss | 0% | Including reconnect replay |
| Key rotation downtime | 0s | Hot-reload via NATS KV |

---

## Comparison: Traditional vs TurboCable

| | Traditional WebSocket Server | TurboCable |
|---|---|---|
| **Max connections (single process)** | ~10k–50k | ~333k |
| **Max connections (cluster)** | Depends on pub/sub bus | 1M (3 nodes) |
| **Message bus** | Redis pub/sub | NATS JetStream |
| **Message persistence** | Fire-and-forget | 7-day retention |
| **Reconnect replay** | Not supported | Built-in (sequence-based) |
| **Memory per connection** | ~50–100 KB | ~6–8 KB |
| **Fan-out model** | Backend iterates connections | Zero-copy Bytes in Rust |
| **WebSocket handling** | In backend process | Dedicated Rust binary |
