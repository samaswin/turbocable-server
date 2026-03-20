# NATS JetStream Integration (Phase 6)

This document covers the NATS JetStream integration in turbocable-server —
the core message delivery pipeline that connects backend broadcasts to
WebSocket clients.

---

## Overview

The gateway consumes messages from a NATS JetStream stream called
`TURBOCABLE` and fans them out to WebSocket subscribers. This replaces
traditional Redis pub/sub with persistent, replayable message delivery.

```
Backend app                      NATS JetStream                   Gateway
    │                                 │                              │
    │ publish(                        │                              │
    │   TURBOCABLE.chat_room_42, data)│                              │
    │──publish TURBOCABLE.chat_room_42──>│                           │
    │                                 │──push to consumer gw_{node}──>│
    │                                 │                              │──fanout to subscribers
    │                                 │                              │   (zero-copy Bytes clone)
    │                                 │<──ack──────────────────────────│
```

---

## Configuration

| Environment Variable | CLI Flag | Default | Description |
|---|---|---|---|
| `TURBOCABLE_NATS_URL` | `--nats-url` | `nats://localhost:4222` | NATS server URL |
| `TURBOCABLE_NODE_ID` | `--node-id` | Auto-generated | Unique gateway node ID |
| `TURBOCABLE_MAX_ACK_PENDING` | `--max-ack-pending` | `10000` | Max unacked messages (back-pressure) |
| `TURBOCABLE_NATS_STREAM_REPLICAS` | `--nats-stream-replicas` | `1` | Stream replicas (use 3 in production) |

---

## JetStream Stream

On startup, the gateway creates (or verifies) the `TURBOCABLE` stream:

| Property | Value |
|---|---|
| Name | `TURBOCABLE` |
| Subjects | `TURBOCABLE.>` |
| Storage | File-backed |
| Retention | 7 days |
| Replicas | Configurable (default 1 for dev, 3 for production) |

The `>` wildcard means the stream captures all subjects under the
`TURBOCABLE.` prefix, supporting any number of broadcast channels.

---

## Durable Consumer

Each gateway node creates a durable pull consumer named `gw_{node_id}`:

| Property | Value |
|---|---|
| Name | `gw_{node_id}` |
| Ack Policy | Explicit |
| Max Ack Pending | Configurable (default 10,000) |
| Deliver Policy | All (resumes from last ack on restart) |

The durable consumer ensures:
- **No message loss**: if the gateway restarts, it resumes from the last
  acknowledged position
- **Back-pressure**: `max_ack_pending` prevents NATS from overwhelming a
  slow gateway
- **Independent cursor**: each gateway node tracks its own position

---

## Fan-out Pipeline

When a NATS message arrives:

1. **Extract stream name**: Strip `TURBOCABLE.` prefix from the subject
   (e.g., `TURBOCABLE.chat_room_42` → `chat_room_42`)

2. **Parse payload**: Try JSON first, fall back to MessagePack, then null.
   This supports both the NATS CLI (`nats pub ... '{"data":"..."}') and
   publisher libraries (which may MessagePack-encode payloads)

3. **Build ServerMessage**: Wrap in `ServerMessage::Message` with the stream
   name as `identifier`, the parsed payload as `message`, and the JetStream
   sequence number as `seq`

4. **Dual-codec encoding**: Pre-encode the message as both JSON and MessagePack
   bytes — one encoding per codec, not per connection

5. **Registry fanout**: `registry.fanout_encoded()` routes the correct
   encoding to each connection based on its negotiated sub-protocol.
   `Bytes::clone()` is a single atomic refcount increment (~1ns)

6. **Acknowledge**: After successful fan-out, acknowledge the NATS message.
   The consumer cursor advances

---

## Message Replay on Reconnect

When a WebSocket client reconnects after a disconnection, it can request
replay of missed messages:

### Client sends hello

```json
{ "type": "hello", "last_seq": "8841" }
```

The `last_seq` is the JetStream stream sequence number of the last message
the client received (delivered in the `seq` field of every message frame).

### Gateway replays on subscribe

When the client subsequently subscribes to a stream, the gateway:

1. Creates an ephemeral consumer starting at `last_seq + 1`
2. Filters by the specific stream's NATS subject
3. Fetches up to 10,000 messages with a 3-second timeout
4. Delivers each message with `replayed: true` and its original `seq`
5. Sends `confirm_subscription` after replay completes

### Wire format

Live message:
```json
{
  "type": "message",
  "identifier": "chat_room_42",
  "message": {"text": "hello"},
  "seq": 8845
}
```

Replayed message:
```json
{
  "type": "message",
  "identifier": "chat_room_42",
  "message": {"text": "hello"},
  "replayed": true,
  "seq": 8842
}
```

The `seq` field is omitted from live messages when the sequence is unknown,
and the `replayed` field is omitted (not present in JSON) for non-replay
messages.

---

## Client Message Publishing

When a WebSocket client sends a `message` command:

```json
{ "command": "message", "identifier": "chat_room_42", "data": "{\"action\":\"speak\"}" }
```

The gateway publishes the `data` payload to `TURBOCABLE.chat_room_42` via
JetStream. This means client messages enter the same stream as
server-initiated broadcasts and are delivered to all subscribers (including
the sender).

---

## Auto-Reconnect

The consumer runs inside a reconnection loop:

1. The `async-nats` client handles TCP-level reconnection automatically
2. An outer loop handles JetStream-level failures (stream deleted, consumer
   expired, etc.)
3. On any error, the consumer logs the error, sleeps 2 seconds, and
   re-creates the durable consumer from its last acknowledged position

---

## Graceful Degradation

If NATS is unavailable on startup:
- The gateway logs a warning and runs without pub/sub
- WebSocket connections still work (health check, auth, ping)
- Client messages are logged but not published
- This mode is useful for local development and testing

---

## Testing

### Prerequisites

- `nats-server` with JetStream support (`brew install nats-server`)
- `nats` CLI (`brew install nats-io/nats-tools/nats`)
- `wscat` (`npm install -g wscat`)

### Test 1: Basic NATS fan-out (no auth)

Open 4 terminal windows.

**Terminal 1 — Start NATS:**

```bash
nats-server --jetstream
```

Wait for `JetStream is ready` in the output.

**Terminal 2 — Start the gateway:**

```bash
cd /path/to/turbocable-server
RUST_LOG=debug cargo run
```

Watch for these log lines:
- `"NATS JetStream stream ready"` — stream created/verified
- `"NATS durable consumer started"` — consumer is pulling
- `"listening on 0.0.0.0:9292"` — ready for connections

**Terminal 3 — Connect a WebSocket client:**

```bash
wscat -c ws://localhost:9292/cable
```

You should receive `{"type":"welcome"}`. Now subscribe:

```
{"command":"subscribe","identifier":"chat_room_1"}
```

Expected response: `{"type":"confirm_subscription","identifier":"chat_room_1"}`

**Terminal 4 — Publish a message via NATS:**

```bash
nats pub TURBOCABLE.chat_room_1 '{"text":"hello from NATS!"}'
```

**Terminal 3** should immediately show:

```json
{"type":"message","identifier":"chat_room_1","message":{"text":"hello from NATS!"},"seq":1}
```

The `seq` field is the JetStream stream sequence number.

### Test 2: Message replay on reconnect

1. Note the last `seq` you received in Terminal 3 (e.g., `1`).

2. **Disconnect** the wscat client (Ctrl+C).

3. **Publish messages while client is disconnected** (Terminal 4):

```bash
nats pub TURBOCABLE.chat_room_1 '{"text":"missed message 1"}'
nats pub TURBOCABLE.chat_room_1 '{"text":"missed message 2"}'
nats pub TURBOCABLE.chat_room_1 '{"text":"missed message 3"}'
```

4. **Reconnect** (Terminal 3):

```bash
wscat -c ws://localhost:9292/cable
```

5. **Send hello with last_seq, then subscribe:**

```
{"type":"hello","last_seq":"1"}
{"command":"subscribe","identifier":"chat_room_1"}
```

6. You should receive the 3 missed messages with `"replayed":true` before
   the subscription confirmation:

```json
{"type":"message","identifier":"chat_room_1","message":{"text":"missed message 1"},"replayed":true,"seq":2}
{"type":"message","identifier":"chat_room_1","message":{"text":"missed message 2"},"replayed":true,"seq":3}
{"type":"message","identifier":"chat_room_1","message":{"text":"missed message 3"},"replayed":true,"seq":4}
{"type":"confirm_subscription","identifier":"chat_room_1"}
```

### Test 3: Client-to-NATS publishing

With a wscat client connected and subscribed to `chat_room_1`, send a
message command:

```
{"command":"message","identifier":"chat_room_1","data":"{\"action\":\"speak\",\"text\":\"hello from client\"}"}
```

Since the message is published to `TURBOCABLE.chat_room_1` via JetStream,
you will receive it back (the sender also gets the broadcast).

### Test 4: Multiple streams

Subscribe to multiple streams and verify each receives only its own
messages:

```
{"command":"subscribe","identifier":"chat_room_1"}
{"command":"subscribe","identifier":"notifications"}
```

```bash
# Terminal 4:
nats pub TURBOCABLE.chat_room_1 '{"text":"chat message"}'
nats pub TURBOCABLE.notifications '{"text":"new notification"}'
```

Each message should arrive with the correct `identifier`.

### Test 5: Health check shows active connections

```bash
curl -s localhost:9292/health | jq .
```

Expected (with one wscat client connected):

```json
{
  "status": "ok",
  "version": "0.1.0",
  "connections": 1
}
```

### Test 6: Verify NATS stream state

```bash
nats stream info TURBOCABLE
```

Shows stream config (subjects, storage, replicas) and current message
count/bytes.

### Test 7: With JWT authentication

See [JWT Authentication — Manual Testing](jwt-authentication.md#manual-testing)
for generating test tokens. When auth is enabled, add the token to the
WebSocket URL:

```bash
wscat -c "ws://localhost:9292/cable?token=$VALID_TOKEN"
```

All fan-out and replay behavior works identically — the token only controls
which streams the client is allowed to subscribe to.

### Quick reference checklist

| # | Test | Expected |
|---|------|----------|
| 1 | NATS publish → wscat receives | Message with `seq` field |
| 2 | Disconnect → publish → reconnect with `last_seq` | Replayed messages with `"replayed":true` |
| 3 | Client sends message command | Published to NATS, echoed back |
| 4 | Multiple streams | Each receives only its own messages |
| 5 | Health endpoint | Shows correct connection count |
| 6 | `nats stream info TURBOCABLE` | Stream exists with correct config |
| 7 | Gateway starts without NATS | Warns, runs without pub/sub |
| 8 | NATS stops while gateway is running | Consumer auto-reconnects when NATS returns |

---

## Source Layout

```
src/pubsub/
├── mod.rs     # Module declaration and pipeline documentation
└── nats.rs    # NatsConsumer: connect, fanout loop, publish, replay
```

Key types:
- `NatsConsumer` — holds NATS client and JetStream context
- `ReplayMessage` — sequence + payload for replay delivery
- `process_nats_message()` — the hot-path fan-out function
- `extract_stream_name()` — strips NATS subject prefix

---

## Phase 6 Checklist

- [x] NATS stream created on startup if not exists
- [x] Consumer reconnects automatically on NATS failure
- [x] Message replay delivers correct messages in order after reconnect
- [x] `max_ack_pending` set to prevent NATS overwhelming slow gateway
- [x] Consumer lag logged periodically (Prometheus gauge in Phase 7)
- [x] Dual-codec fanout (JSON + MessagePack) without per-connection encoding
- [x] Client messages published to NATS JetStream
- [x] Graceful degradation when NATS is unavailable
- [ ] Fanout latency measured: p99 < 20ms at 1k subscribers (Phase 10 load test)
