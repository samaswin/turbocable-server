# TurboCable — Per-Stream Rate Limiting

This document covers TurboCable's **gateway-side** per-stream rate limiter and the
complementary **NATS server-side** flow-control knobs.  Use both layers together for
the strongest defence against a noisy publisher starving other streams.

---

## Gateway-Side Rate Limiting

### How It Works

TurboCable implements a **token-bucket** limiter keyed by stream name.  Each stream
maintains an independent bucket:

- The bucket starts **full** (`burst` tokens) so a freshly seen stream can absorb a
  short spike without any drops.
- Tokens refill continuously at `rps` tokens per second.
- When a NATS message arrives, one token is consumed.  If the bucket is empty the
  message is **dropped before fan-out** — it is not forwarded to any WebSocket client.
- The NATS message is **always acknowledged**, so NATS does not re-deliver it.

### Configuration

| Environment variable | CLI flag | Default | Description |
|---|---|---|---|
| `TURBOCABLE_STREAM_RATE_LIMIT_RPS` | `--stream-rate-limit-rps` | `0` | Default rate in messages/second per stream.  `0` = disabled. |
| `TURBOCABLE_STREAM_RATE_LIMIT_BURST` | `--stream-rate-limit-burst` | `0` | Default burst capacity.  `0` = 2× RPS (minimum 1). |
| `TURBOCABLE_STREAM_RATE_OVERRIDES` | `--stream-rate-overrides` | `""` | Per-stream overrides (see below). |

#### Per-Stream Overrides

Set `TURBOCABLE_STREAM_RATE_OVERRIDES` to a **semicolon-separated** list of
`name=rps:burst` pairs:

```
TURBOCABLE_STREAM_RATE_OVERRIDES="alerts=100:200;high_volume=5000:10000"
```

- A stream listed in overrides uses its own rps/burst regardless of the defaults.
- Burst `0` in an override applies the 2× default rule for that stream.
- Streams **not** listed in overrides fall back to the default `rps`/`burst`.

#### Example: Enable Limiting for All Streams

```bash
# Allow up to 1 000 msg/s per stream; short bursts up to 2 000.
TURBOCABLE_STREAM_RATE_LIMIT_RPS=1000
TURBOCABLE_STREAM_RATE_LIMIT_BURST=2000
```

#### Example: Mixed — Tight Limit on One Stream, Generous Default

```bash
TURBOCABLE_STREAM_RATE_LIMIT_RPS=5000
TURBOCABLE_STREAM_RATE_LIMIT_BURST=10000
TURBOCABLE_STREAM_RATE_OVERRIDES="system_alerts=50:100"
```

### Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `turbocable_stream_rate_limited_total` | Counter | `stream` | Messages dropped due to rate limiting.  Use `rate(...[5m])` for drop rate. |
| `turbocable_stream_tokens_available` | Gauge | `stream` | Current token count (sampled on each message).  Near 0 = stream near limit. |

**Useful PromQL queries:**

```promql
# Streams actively being rate-limited (drops > 1/s over 5 min):
rate(turbocable_stream_rate_limited_total[5m]) > 1

# Total drop rate across all streams:
sum(rate(turbocable_stream_rate_limited_total[5m]))

# Token levels for streams near their limit:
turbocable_stream_tokens_available < 10
```

### Alerts

Two alert rules ship in `infra/prometheus/alerts.yml`:

| Alert | Threshold | Severity |
|---|---|---|
| `StreamRateLimiterTriggered` | `> 10` drops/s sustained 5 m | warning |
| `StreamRateLimiterTriggeredCritical` | `> 1 000` drops/s sustained 2 m | critical |

---

## NATS Server-Side Flow Control (Complementary Defence)

NATS provides its own rate-limiting and flow-control primitives.  These operate
**before** messages even reach the gateway consumer and are a stronger safeguard
against runaway publishers overwhelming the broker itself.

### When to Prefer NATS-Side Limits

| Scenario | Prefer |
|---|---|
| Limit total throughput _into_ the NATS stream regardless of consumer count | NATS-side |
| Limit delivery _to subscribers_ per stream on this gateway node | Gateway-side |
| Prevent broker disk/memory saturation from a fire-hose publisher | NATS-side |
| Apply different limits per gateway cluster without redeploying NATS | Gateway-side |

Use **both** for defence-in-depth: NATS limits protect the broker; gateway limits
protect downstream WebSocket subscribers from a single noisy stream.

### Per-Subject Rate Limiting (`max_msgs_per_subject`)

Cap how many messages NATS retains per subject (stream name):

```bash
# Via nats CLI
nats stream edit TURBOCABLE --max-msgs-per-subject=100000

# Via NATS config file (streams section)
max_msgs_per_subject: 100000
```

This drops the **oldest** message per subject once the cap is hit (retention =
`limits` policy).  Combine with a short `max_age` to bound storage:

```
max_age: 7d
max_msgs_per_subject: 100000
```

### Per-Account Publish Rate (`max_connections`, `max_payload`)

In `nats-server.conf`:

```hcl
accounts {
  TURBOCABLE_PUBLISHER {
    users = [{user: "publisher", password: "$2a$..."}]
    limits {
      # Maximum message payload (bytes).
      max_payload: 65536
      # Maximum concurrent connections for this account.
      max_connections: 10
    }
  }
}
```

### JetStream Consumer `max_ack_pending`

TurboCable already configures `max_ack_pending` (env `TURBOCABLE_MAX_ACK_PENDING`,
default 10 000) on its durable consumer.  This is the primary back-pressure knob:
NATS stops delivering new messages to the consumer once this many messages are
unacknowledged, preventing the gateway from being overwhelmed during lag spikes.

Reduce this value if you observe the gateway falling behind:

```bash
TURBOCABLE_MAX_ACK_PENDING=2000
```

### JetStream Stream `max_msgs` and `max_bytes`

Global stream caps prevent runaway publishers from filling disk:

```bash
nats stream edit TURBOCABLE \
  --max-msgs=50000000 \
  --max-bytes=10GB
```

### Observing NATS-Side Limits

```bash
# Check stream state (msgs retained, bytes, consumer lag)
nats stream info TURBOCABLE

# Check consumer position and ack-pending
nats consumer info TURBOCABLE gw_<node_id>

# Tail live messages on a subject (useful for debugging publish rate)
nats sub "TURBOCABLE.my_stream"
```

---

## Decision Guide

```
Publisher too fast → clients miss messages?
  ├─ Is the broker running out of disk/memory?
  │    YES → Add NATS max_msgs / max_bytes limits
  │    NO  → Continue
  │
  ├─ Is gateway NATS consumer lag growing?
  │    YES → Lower max_ack_pending OR scale gateway replicas
  │    NO  → Continue
  │
  └─ Is a single stream starving others at the fan-out layer?
       YES → Set TURBOCABLE_STREAM_RATE_LIMIT_RPS (gateway-side)
       NO  → No action needed
```
