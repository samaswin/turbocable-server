# 1M Connections Plan

This document is the roadmap-and-reference hub for TurboCable's 1M-connection
target. Detailed how-to guides live in the files listed in the index at the
bottom; this page covers the big picture.

---

## Roadmap

TurboCable ships in incremental phases. The gateway (this repo) must pass each
numeric gate before the dependent layers are promoted.

| Phase | Goal | Gate |
|-------|------|------|
| A — Compat | 250k sustained, replay-capable clients ≥ 80% | All gates except 1M scale |
| B — Soft enforce | 600k sustained, non-compliant clients measured | All numeric gates |
| C — Hard enforce | 1M sustained (two consecutive runs), rollback drill | Full SLO table below |

**Implementation order** (tracking steps from the original design):

```
STEP 1  Rust gateway                   ✓ done — turbocable-server
STEP 2  NATS JetStream protocol        ✓ done — replay, push consumer, KV auth
STEP 3  turbocable Ruby gem            gem (separate repo)
STEP 4  turbocable-rails gem           gem (separate repo)
STEP 5  @turbocable/client JS          npm (separate repo)
STEP 6  turbocable-server gem          gem packaging (separate repo)
STEP 7  1M load test validation        ← Phase C gate
STEP 8  OSS publishing                 after Phase C
STEP 9  CI/CD full matrix              ongoing
```

---

## SLO Summary

These are the numeric pass/fail thresholds used to promote a release. Tests that
miss a threshold block promotion regardless of other results.

| Metric | Target | Hard fail |
|--------|--------|-----------|
| Connections (single node) | 333k sustained | < 200k |
| Connections (3-node cluster) | 1M sustained for 30 min | < 700k |
| Fan-out latency p95 | ≤ 50ms | — |
| Fan-out latency p99 | ≤ 75ms | > 100ms |
| Connect time p99 | ≤ 100ms | > 200ms |
| Memory per connection | ≤ 8 KB | > 15 KB |
| Dropped messages | 0% | > 0.01% |
| Sequence gaps (`tc_sequence_gaps`) | 0 | any |
| Replay success rate | ≥ 99.95% | — |
| Replay ordering violations | 0 | any |
| Forced reconnect rate (per 5-min window) | ≤ 1.0% of active conns/min | — |
| Post-reconnect first-delivery p95 | ≤ 2s | — |
| Full catch-up success (within retention) | ≥ 99.9% | — |

---

## Reconnect and Crash Semantics

### Client reconnect protocol

When a client reconnects (voluntary or after a gateway crash), it sends a
`hello` frame with its last known sequence number before subscribing:

```json
{"type":"hello","last_seq":8841,"capabilities":["replay_v1"]}
```

The gateway replays every message with `seq > 8841` on each subscribed stream
before resuming the live feed. Replayed messages carry `"replayed":true`:

```json
{"type":"message","identifier":"chat_room_42","message":{...},"replayed":true,"seq":8842}
{"type":"message","identifier":"chat_room_42","message":{...},"replayed":true,"seq":8843}
... (live messages follow)
```

**Zero data loss guarantee**: NATS JetStream persists messages for 7 days.
As long as the client reconnects within the retention window it will receive
every message it missed — even if the gateway itself crashed and restarted.

### Gateway crash

1. NATS JetStream keeps the stream intact — unacknowledged messages are
   retained.
2. Clients detect the WebSocket close and reconnect with exponential backoff.
3. On reconnect the client presents its `last_seq`; the gateway replays from
   that sequence.
4. No manual intervention is required. The `tc_sequence_gaps` Prometheus counter
   must remain 0 after a crash/restart cycle.

### Graceful shutdown (SIGTERM)

The gateway sends a `GoAway` close frame to every connected client before
closing. Clients should treat `GoAway` as a signal to reconnect immediately
(rather than backing off), minimising the reconnect storm window.

See [graceful-shutdown.md](graceful-shutdown.md) for the full drain sequence
and Kubernetes `preStop` hook configuration.

### Replay enforcement modes

The server enforces the hello-first contract in three phases controlled by
`REPLAY_ENFORCEMENT` (env var). Set to `compat` to accept legacy clients
without penalty, `soft_enforce` to reject commands sent before `hello`, or
`hard_enforce` to also reject clients that omit `"replay_v1"` from their
capabilities.

---

## File Index

| Document | Description |
|----------|-------------|
| [docs/setup.md](setup.md) | Install, OS limits, NATS smoke tests |
| [docs/architecture.md](architecture.md) | System design, data flow, capacity planning |
| [docs/load-testing-1m.md](load-testing-1m.md) | Full load-test guide: smoke → 333k → 1M cluster |
| [docs/websocket-protocol.md](websocket-protocol.md) | Wire protocol, replay handshake, close codes |
| [docs/nats-jetstream.md](nats-jetstream.md) | JetStream stream design, replay, operations |
| [docs/jwt-authentication.md](jwt-authentication.md) | JWT auth and stream-level authorization |
| [docs/configuration.md](configuration.md) | All environment variables and CLI flags |
| [docs/graceful-shutdown.md](graceful-shutdown.md) | SIGTERM drain, GoAway, Kubernetes notes |
| [docs/development.md](development.md) | Tests, Clippy, fmt, CI, WSL workflow |
| [docs/binary-distribution.md](binary-distribution.md) | Docker image, GitHub Releases, cross-compilation |
| [bench/README.md](../bench/README.md) | Script reference and bench results pointer |
