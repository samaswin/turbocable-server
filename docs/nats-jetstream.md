# NATS JetStream Integration (Phase 6)

This document covers the NATS JetStream integration in turbocable-server —
the core message delivery pipeline that connects Rails broadcasts to
WebSocket clients.

---

## Overview

The gateway consumes messages from a NATS JetStream stream called
`TURBOCABLE` and fans them out to WebSocket subscribers. This replaces the
Redis pub/sub model used by ActionCable with persistent, replayable message
delivery.

```
Rails app                        NATS JetStream                   Gateway
    │                                 │                              │
    │ TurboCable.broadcast(           │                              │
    │   "chat_room_42", data)         │                              │
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
   This supports both the NATS CLI (`nats pub ... '{"data":"..."}') and the
   Ruby gem (which MessagePack-encodes payloads)

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
the sender, matching ActionCable behavior).

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

### Manual integration test

```bash
# Terminal 1: Start NATS with JetStream
nats-server --jetstream

# Terminal 2: Start the gateway
RUST_LOG=debug cargo run

# Terminal 3: Connect a WebSocket client
wscat -c 'ws://localhost:9292/cable?token=<valid_jwt>'
# Send: {"command":"subscribe","identifier":"chat_room_1"}

# Terminal 4: Publish a message
nats pub TURBOCABLE.chat_room_1 '{"message":"hello from NATS"}'
# Terminal 3 should receive the message
```

### Replay test

```bash
# Publish messages while the client is disconnected
nats pub TURBOCABLE.chat_room_1 '{"message":"missed 1"}'
nats pub TURBOCABLE.chat_room_1 '{"message":"missed 2"}'

# Reconnect the client with last_seq from before disconnect
# Send: {"type":"hello","last_seq":"<last_known_seq>"}
# Send: {"command":"subscribe","identifier":"chat_room_1"}
# Client should receive both missed messages with replayed: true
```

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
