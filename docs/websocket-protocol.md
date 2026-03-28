# WebSocket protocol

The gateway speaks Action Cable–style JSON over WebSocket (`actioncable-v1-json`)
and an optional MessagePack sub-protocol (`turbocable-v1-msgpack`). Connection
and stream authorization use RS256 JWTs when a public key is configured.

See [configuration.md](configuration.md) for the `/cable` endpoint and
[jwt-authentication.md](jwt-authentication.md) for token issuance and testing.

---

## Connecting

```bash
# JSON sub-protocol (default, actioncable-v1-json)
wscat -c 'ws://localhost:9292/cable?token=<JWT>'

# MessagePack binary sub-protocol
wscat -c 'ws://localhost:9292/cable?token=<JWT>' --subprotocol turbocable-v1-msgpack
```

When JWT auth is disabled, omit the `token` query parameter.

---

## Subscribe

```json
{"command":"subscribe","identifier":"chat_room_42"}
```

**Allowed:**

```json
{"type":"confirm_subscription","identifier":"chat_room_42"}
```

**Rejected** (stream not in JWT `allowed_streams`):

```json
{"type":"reject_subscription","identifier":"chat_room_42"}
```

---

## Unsubscribe

```json
{"command":"unsubscribe","identifier":"chat_room_42"}
```

---

## Send message (client → NATS → subscribers)

```json
{"command":"message","identifier":"chat_room_42","data":"{\"action\":\"speak\",\"text\":\"hello\"}"}
```

---

## Receiving messages

Live message from NATS fan-out:

```json
{"type":"message","identifier":"chat_room_42","message":{"text":"hello"},"seq":42}
```

---

## Reconnect replay

When reconnecting, send a `hello` with the last received `seq` and `"replay_v1"` capability before subscribing:

```json
{"type":"hello","last_seq":42,"capabilities":["replay_v1"]}
{"command":"subscribe","identifier":"chat_room_42"}
```

`last_seq` is a **number** (integer), not a string. `capabilities` must include `"replay_v1"` for the server to treat the client as replay-capable (required in `hard_enforce` mode).

Missed messages are replayed with `"replayed":true` before the subscription confirmation:

```json
{"type":"message","identifier":"chat_room_42","message":{"text":"missed"},"replayed":true,"seq":43}
{"type":"confirm_subscription","identifier":"chat_room_42"}
```

Fresh connections that do not need replay can omit `last_seq` but should still advertise `capabilities`:

```json
{"type":"hello","capabilities":["replay_v1"]}
```

---

## Enforcement modes (`REPLAY_ENFORCEMENT`)

The server enforces the hello-first contract in three phases controlled by the `REPLAY_ENFORCEMENT` environment variable:

| Phase | Value | Behaviour |
|-------|-------|-----------|
| A | `compat` (default) | Subscribe-before-hello allowed with a warning metric. Legacy clients work unchanged. |
| B | `soft_enforce` | Commands before `hello` are rejected with `reject_subscription`. Subscribes without `replay_v1` are **allowed** with a warning metric so you can measure remaining legacy clients. |
| C | `hard_enforce` | Commands before `hello` rejected. Subscribes from clients whose hello lacked `replay_v1` are also rejected. |

When a subscribe is rejected the server sends:

```json
{"type":"reject_subscription","identifier":"chat_room_42"}
```

Rollback: set `REPLAY_ENFORCEMENT=compat` and restart — no code change needed.

---

## JWT claims

Tokens must be RS256-signed with claims such as:

```json
{
  "sub": "user_42",
  "allowed_streams": ["chat_room_*", "notifications"],
  "exp": 1710000000,
  "iat": 1709996400
}
```

Stream authorization uses glob patterns: `"*"` matches any stream; `"chat_room_*"`
matches any stream whose name starts with `chat_room_`.

---

## WebSocket close codes

| Code | Meaning |
|------|---------|
| `3000` | Authentication failed (no token, expired, invalid signature) |
| `1008` | Per-IP connection limit exceeded |

---

## Related documentation

- [NATS JetStream](nats-jetstream.md) — how messages map to subjects and replay
- [Architecture](architecture.md) — fan-out and registry overview
