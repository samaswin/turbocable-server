# JWT Authentication

turbocable-server uses RS256 (RSA + SHA-256) JSON Web Tokens to authenticate
every WebSocket connection. Tokens are passed via the `?token=` query parameter
on the `/cable` endpoint.

---

## Table of Contents

- [Overview](#overview)
- [Configuration](#configuration)
- [JWT Claims](#jwt-claims)
- [Stream Authorization](#stream-authorization)
- [Key Hot-Reload (NATS KV)](#key-hot-reload-nats-kv)
- [WebSocket Close Codes](#websocket-close-codes)
- [Manual Testing](#manual-testing)
- [Troubleshooting](#troubleshooting)

---

## Overview

```
Client                          turbocable-server
  │                                    │
  │  GET /cable?token=<JWT>            │
  │ ──────────────────────────────────>│
  │                                    │── verify RS256 signature
  │                                    │── check exp / iat claims
  │                                    │── extract allowed_streams
  │                                    │
  │  101 Switching Protocols           │  (on success)
  │ <──────────────────────────────────│
  │  {"type":"welcome"}                │
  │ <──────────────────────────────────│
  │                                    │
  │  close(3000, "token expired")      │  (on failure)
  │ <──────────────────────────────────│
```

When auth is enabled, connections without a token or with an invalid token are
immediately closed with WebSocket close code `3000`.

If no public key is configured (neither file nor NATS KV), the server starts
in **auth-disabled mode** and accepts all connections. A warning is logged at
startup in this case.

---

## Configuration

### Option 1: File-based key (recommended for local development)

Set the path to an RSA public key PEM file:

```bash
TURBOCABLE_JWT_PUBLIC_KEY_PATH=/path/to/public_key.pem cargo run
```

Or via CLI argument:

```bash
cargo run -- --jwt-public-key-path /path/to/public_key.pem
```

### Option 2: NATS KV (recommended for production)

If `TURBOCABLE_JWT_PUBLIC_KEY_PATH` is **not** set, turbocable-server will
attempt to load the key from the NATS KV bucket `TC_PUBKEYS`, key
`rails_public_key` (the key name used by the turbocable publisher gem).

A background watcher is spawned to pick up key rotations automatically —
new connections will use the updated key within seconds.

### Priority order

| Priority | Source | When |
|----------|--------|------|
| 1 | File (`TURBOCABLE_JWT_PUBLIC_KEY_PATH`) | Always used if set. NATS watcher also starts if NATS is available. |
| 2 | NATS KV (`TC_PUBKEYS.rails_public_key`) | Used when no file path is configured. |
| 3 | Auth disabled | Neither source available — all connections accepted (warning logged). |

### All related environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TURBOCABLE_JWT_PUBLIC_KEY_PATH` | _(none)_ | Path to RSA public key PEM file |
| `TURBOCABLE_NATS_URL` | `nats://localhost:4222` | NATS server URL (used for KV key watcher) |

---

## JWT Claims

Tokens must be signed with **RS256** (RSA PKCS#1 v1.5 + SHA-256). The payload
must include:

```json
{
  "sub": "user_42",
  "allowed_streams": ["chat_room_*", "notifications"],
  "exp": 1710000000,
  "iat": 1709996400
}
```

| Claim | Type | Required | Description |
|-------|------|----------|-------------|
| `sub` | string | yes | User identifier |
| `allowed_streams` | string[] | yes | Stream name patterns the user may subscribe to |
| `exp` | integer | yes | Expiration time (Unix timestamp) |
| `iat` | integer | yes | Issued-at time (Unix timestamp) |

---

## Stream Authorization

When a client sends a `subscribe` command, the requested stream identifier is
checked against the `allowed_streams` claim from the JWT.

### Pattern syntax

| Pattern | Matches | Example |
|---------|---------|---------|
| `"*"` | Any stream name | `"*"` matches `"chat_room_42"`, `"notifications"`, etc. |
| `"prefix_*"` | Any stream starting with `prefix_` | `"chat_room_*"` matches `"chat_room_1"`, `"chat_room_99"` |
| `"exact_name"` | Only that exact stream | `"notifications"` matches only `"notifications"` |

Multiple patterns can be provided — the subscribe is allowed if **any** pattern
matches.

### Behavior on subscribe

- **Allowed** — stream is registered and `confirm_subscription` is sent:
  ```json
  {"type":"confirm_subscription","identifier":"chat_room_42"}
  ```

- **Rejected** — `reject_subscription` is sent, stream is **not** registered:
  ```json
  {"type":"reject_subscription","identifier":"admin_panel"}
  ```

---

## Key Hot-Reload (NATS KV)

turbocable-server watches the NATS KV bucket `TC_PUBKEYS` for changes to the
`rails_public_key` entry (the key name used by the turbocable publisher gem). When a new key is published:

1. The background watcher detects the change.
2. The internal `DecodingKey` is atomically swapped (behind a `RwLock`).
3. New connections immediately use the new key.
4. Existing connections with still-valid JWTs are **not** affected.

This enables zero-downtime key rotation.

### Publishing a new key

```bash
# Using the nats CLI
nats kv put TC_PUBKEYS rails_public_key "$(cat /path/to/new_public_key.pem)"
```

---

## WebSocket Close Codes

| Code | Reason | When |
|------|--------|------|
| `3000` | `auth failed` | No token provided |
| `3000` | `token expired` | JWT `exp` claim is in the past |
| `3000` | `invalid signature` | Signature does not match the public key |
| `3000` | `invalid algorithm` | Token not signed with RS256 |
| `1008` | `too many connections from this IP` | Per-IP connection limit exceeded |

---

## Manual Testing

### Prerequisites

- `openssl` (for key generation)
- `wscat` (`npm install -g wscat`)
- `python3` with PyJWT (`pip install pyjwt cryptography`)

### Step 1: Generate an RSA key pair

```bash
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out /tmp/tc_private.pem
openssl pkey -in /tmp/tc_private.pem -pubout -out /tmp/tc_public.pem
```

### Step 2: Start the server

```bash
RUST_LOG=debug \
TURBOCABLE_JWT_PUBLIC_KEY_PATH=/tmp/tc_public.pem \
cargo run
```

Verify the log shows: `JWT auth enabled (key loaded from file)`.

### Step 3: Generate test JWTs

Create `/tmp/gen_jwt.py`:

```python
import jwt, time, sys
from cryptography.hazmat.primitives.serialization import load_pem_private_key

private_key = load_pem_private_key(open("/tmp/tc_private.pem", "rb").read(), password=None)
mode = sys.argv[1] if len(sys.argv) > 1 else "valid"

claims = {
    "sub": "user_42",
    "allowed_streams": ["chat_room_*", "notifications"],
    "iat": int(time.time()),
    "exp": int(time.time()) + 3600,
}

if mode == "expired":
    claims["exp"] = int(time.time()) - 60
elif mode == "wildcard":
    claims["allowed_streams"] = ["*"]
elif mode == "restricted":
    claims["allowed_streams"] = ["admin_only"]

print(jwt.encode(claims, private_key, algorithm="RS256"))
```

Generate tokens:

```bash
VALID_TOKEN=$(python3 /tmp/gen_jwt.py valid)
EXPIRED_TOKEN=$(python3 /tmp/gen_jwt.py expired)
WILDCARD_TOKEN=$(python3 /tmp/gen_jwt.py wildcard)
RESTRICTED_TOKEN=$(python3 /tmp/gen_jwt.py restricted)
```

### Step 4: Run the test scenarios

#### Valid token — connection accepted

```bash
wscat -c "ws://localhost:9292/cable?token=$VALID_TOKEN"
```

Expected: receives `{"type":"welcome"}` and periodic pings.

#### No token — rejected

```bash
wscat -c "ws://localhost:9292/cable"
```

Expected: immediate close with code 3000.

#### Expired token — rejected

```bash
wscat -c "ws://localhost:9292/cable?token=$EXPIRED_TOKEN"
```

Expected: close with reason `token expired`.

#### Tampered token — rejected

```bash
wscat -c "ws://localhost:9292/cable?token=${VALID_TOKEN}x"
```

Expected: close with reason `invalid signature` or `auth failed`.

### Step 5: Test stream authorization

Connect with the valid token:

```bash
wscat -c "ws://localhost:9292/cable?token=$VALID_TOKEN"
```

Send subscribe commands inside the wscat session:

```
# Allowed (matches chat_room_*)
{"command":"subscribe","identifier":"chat_room_42"}
→ {"type":"confirm_subscription","identifier":"chat_room_42"}

# Allowed (exact match)
{"command":"subscribe","identifier":"notifications"}
→ {"type":"confirm_subscription","identifier":"notifications"}

# Rejected (not in allowed_streams)
{"command":"subscribe","identifier":"admin_panel"}
→ {"type":"reject_subscription","identifier":"admin_panel"}
```

### Step 5b: Test NATS fan-out with auth

With NATS running and the client subscribed to `chat_room_42`:

```bash
# In another terminal
nats pub TURBOCABLE.chat_room_42 '{"text":"hello with auth"}'
```

The wscat client should receive:

```json
{"type":"message","identifier":"chat_room_42","message":{"text":"hello with auth"},"seq":1}
```

See [NATS JetStream Integration](nats-jetstream.md) for full fan-out and
replay testing instructions.

### Step 6: Test with restricted token

```bash
wscat -c "ws://localhost:9292/cable?token=$RESTRICTED_TOKEN"
```

```
# Rejected (only admin_only is allowed)
{"command":"subscribe","identifier":"chat_room_1"}
→ {"type":"reject_subscription","identifier":"chat_room_1"}

# Allowed
{"command":"subscribe","identifier":"admin_only"}
→ {"type":"confirm_subscription","identifier":"admin_only"}
```

### Step 7: Verify health endpoint

```bash
curl -s localhost:9292/health | jq .
```

```json
{
  "status": "ok",
  "version": "0.1.0",
  "connections": 1
}
```

### Quick reference checklist

| # | Test | Expected Result |
|---|------|-----------------|
| 1 | Valid JWT | `welcome`, connection stays open |
| 2 | No token | `close(3000)` |
| 3 | Expired JWT | `close(3000, "token expired")` |
| 4 | Tampered JWT | `close(3000)` |
| 5 | Subscribe to allowed stream | `confirm_subscription` |
| 6 | Subscribe to disallowed stream | `reject_subscription` |
| 7 | Wildcard token subscribes to anything | `confirm_subscription` |
| 8 | Health endpoint with active connection | `connections: N` |
| 9 | No key configured | Auth disabled, all connections accepted |

---

## Troubleshooting

### "JWT auth DISABLED" warning on startup

No public key was found. Set `TURBOCABLE_JWT_PUBLIC_KEY_PATH` to a valid PEM
file, or ensure the NATS KV bucket `TC_PUBKEYS` contains the
`rails_public_key` entry.

### "failed to read JWT public key file"

The file at `TURBOCABLE_JWT_PUBLIC_KEY_PATH` does not exist or is not readable.
Verify the path and file permissions.

### "invalid RSA PEM"

The file exists but is not a valid RSA public key in PEM format. Ensure it
starts with `-----BEGIN PUBLIC KEY-----`. Note: the **public** key is needed,
not the private key.

### "NATS KV key watcher not available"

The NATS KV bucket `TC_PUBKEYS` does not exist yet. This is expected during
local development if the key has not been published yet. The file-based key is
used as fallback.

### Connections rejected despite valid-looking token

- Verify the token was signed with the **matching private key** for the
  configured public key.
- Check that the token uses the **RS256** algorithm (not HS256).
- Ensure `exp` is in the future.
- Enable `RUST_LOG=debug` to see the specific rejection reason in server logs.
