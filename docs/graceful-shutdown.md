# Graceful Shutdown

turbocable-server handles `SIGTERM` (and `Ctrl-C` in dev) by cleanly closing
all WebSocket connections before the process exits. This keeps rolling deploys
and Kubernetes pod evictions loss-free.

---

## Table of Contents

- [Shutdown Sequence](#shutdown-sequence)
- [Manual Testing](#manual-testing)
- [Kubernetes Configuration](#kubernetes-configuration)
- [Implementation Notes](#implementation-notes)

---

## Shutdown Sequence

```
1. SIGTERM (or Ctrl-C) received
   └─ Axum stops accepting new TCP connections immediately.

2. Shutdown watch fires (watch::Sender<bool> → true)
   └─ All active WebSocket connection tasks receive the signal.

3. Per-connection response (concurrent across all connections):
   ├─ Outbound task: drains the remaining outbound queue
   │                 sends WebSocket close(1001, "server shutting down")
   │                 exits
   └─ Inbound task:  breaks out of the receive loop
                     triggers deregister + metrics decrement

4. Drain wait (up to 30 seconds)
   └─ Server polls registry.connection_count() every 100ms.
      When count reaches 0, or after 30s timeout, moves on.
      Remaining connections are force-closed at timeout.

5. NATS flush
   └─ client.flush() ensures all pending acks and publishes reach the
      NATS server before the TCP connection closes.

6. Process exits cleanly (exit 0)
```

Close code **1001 Going Away** (RFC 6455 §7.4.1) signals the client that the
server is shutting down, not that anything went wrong. Clients should reconnect
immediately to another gateway node rather than treating it as an error.

---

## Manual Testing

### Prerequisites

```bash
# Install wscat for WebSocket testing
npm install -g wscat

# Start the gateway (in WSL)
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && RUST_LOG=info cargo run"
```

---

### Test 1 — Single connection receives close(1001)

**Terminal A** — connect a WebSocket client:

```bash
wscat -c ws://localhost:9292/cable
```

Wait for `{"type":"welcome"}`.

**Terminal B** — send SIGTERM:

```bash
# Find the PID
pgrep -x turbocable-server

# Send SIGTERM
kill -TERM <pid>
```

**Expected in Terminal A:**

```
< {"type":"welcome"}
Disconnected (code: 1001, reason: "server shutting down")
```

**Expected in gateway logs (Terminal with `cargo run`):**

```json
{"level":"INFO","message":"SIGTERM received"}
{"level":"INFO","message":"draining connections (up to 30s)","active":1}
{"level":"INFO","message":"NATS flushed"}
{"level":"INFO","message":"shutdown complete"}
```

---

### Test 2 — Multiple connections all receive close(1001)

**Terminal A** — open 5 connections:

```bash
for i in {1..5}; do wscat -c ws://localhost:9292/cable & done
wait
```

Verify count:

```bash
curl -s localhost:9292/health | jq .connections
# Expected: 5
```

**Terminal B** — send SIGTERM:

```bash
kill -TERM $(pgrep -x turbocable-server)
```

All 5 `wscat` sessions should disconnect with `code: 1001`. The gateway logs
should show `active: 5` in the drain message and exit cleanly.

---

### Test 3 — Connections drain before the 30-second timeout

Verify no messages are lost mid-flight:

**Terminal A** — subscribe to a stream:

```bash
wscat -c ws://localhost:9292/cable
# > {"command":"subscribe","identifier":"stress_test"}
```

**Terminal B** — with NATS running, publish messages while sending SIGTERM
simultaneously:

```bash
# Publish 10 messages
for i in {1..10}; do
  nats pub TURBOCABLE.stress_test "{\"seq\":$i}"
done

# Immediately send SIGTERM
kill -TERM $(pgrep -x turbocable-server)
```

Any messages already queued in the outbound mpsc channel are flushed to the
client before the close frame is sent. Check Terminal A — close frame should
arrive last, after all queued messages.

---

### Test 4 — Health check rejects new connections after SIGTERM

While the server is draining (there are slow clients holding connections), a
health check from a new request should fail to connect since Axum is no longer
accepting:

```bash
# Cause a slow drain: open a connection and pause it
wscat -c ws://localhost:9292/cable &

# Send SIGTERM
kill -TERM $(pgrep -x turbocable-server)

# Try to connect immediately (should be refused)
wscat -c ws://localhost:9292/cable
# Expected: Connection refused
```

---

### Test 5 — Ctrl-C behaves identically to SIGTERM (dev mode)

```bash
# Start in foreground
RUST_LOG=info cargo run

# Connect a client in another terminal
wscat -c ws://localhost:9292/cable

# Press Ctrl-C in the cargo run terminal
```

Expected: same close(1001) sequence as SIGTERM.

---

### Observing the drain countdown

Set `RUST_LOG=debug` to see per-connection shutdown events:

```bash
RUST_LOG=debug cargo run
```

When SIGTERM fires you'll see a line per connection:

```json
{"level":"DEBUG","fields":{"conn_id":1,"message":"shutdown signal received"}}
```

followed by the drain summary and exit.

---

## Kubernetes Configuration

Set `terminationGracePeriodSeconds` to at least 5 seconds more than the
gateway's 30-second drain window. This gives the gateway time to drain and
flush NATS before Kubernetes escalates to `SIGKILL`.

```yaml
# deployment.yaml
spec:
  template:
    spec:
      terminationGracePeriodSeconds: 35   # 30s drain + 5s buffer

      containers:
        - name: gateway
          # Same image as releases: ghcr.io/<your-github-owner>/turbocable-server
          image: ghcr.io/samaswin/turbocable-server:latest
          lifecycle:
            preStop:
              exec:
                # Optional: small sleep so the load balancer can deregister
                # the pod before SIGTERM arrives and new connections stop.
                command: ["/bin/sleep", "2"]
```

### Rolling deploy behaviour

```
1. New pod starts, passes readiness probe → receives traffic
2. Old pod receives SIGTERM
3. Old pod stops accepting (load balancer should already be draining it)
4. Old pod sends close(1001) to all connections
5. Clients reconnect to new pod
6. Old pod exits (within 30s)
7. Kubernetes sees clean exit, continues rolling
```

The `preStop` sleep (step 2 above) is recommended when using a load balancer
that needs a moment to drain the pod's endpoints before SIGTERM arrives.

---

## Implementation Notes

### Signal handling

On Unix, both `SIGTERM` and `SIGINT` (Ctrl-C) are handled:

```rust
// src/server.rs
async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = sigterm.recv() => { /* SIGTERM */ }
        _ = tokio::signal::ctrl_c() => { /* Ctrl-C / SIGINT */ }
    }
}
```

On Windows (non-Unix builds), only Ctrl-C is handled since there is no
`SIGTERM`.

### Shutdown watch channel

A `tokio::sync::watch::channel(false)` is created at startup. Its `Receiver`
is cloned into every connection's `AppState`. When `SIGTERM` fires and Axum
returns, the server sends `true`:

```
watch::Sender<bool>  ──true──>  watch::Receiver<bool>  (one per connection)
                                 ├─ outbound_loop: drain + send close(1001)
                                 └─ inbound_loop: break
```

`watch` is used rather than `broadcast` because:
- Late-joining receivers get the current value (`true`) immediately — useful
  if a connection is being established exactly at shutdown time.
- No per-message allocation.
- Receivers don't need to be tracked.

### Outbound drain

When the shutdown watch fires in `outbound_loop`, any messages already in
the 16-slot mpsc channel are drained synchronously via `try_recv` before the
close frame is sent. This ensures in-flight fan-out messages are not silently
dropped for connections that are mid-flight.

### NATS flush

`client.flush()` blocks until NATS has acknowledged all outbound data (publish
acks and message acks). Without this, a fast process exit could drop pending
acks, causing NATS to re-deliver those messages to the next gateway consumer
after restart — resulting in duplicate deliveries.

---

## Related Documentation

- [Architecture Overview](architecture.md) — connection lifecycle
- [NATS JetStream Integration](nats-jetstream.md) — ack semantics
- [Development Setup](setup.md) — running the server locally
