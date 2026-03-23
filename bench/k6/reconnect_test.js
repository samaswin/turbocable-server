/**
 * TurboCable WebSocket Reconnect Test — k6 script
 *
 * Validates that clients reconnecting with `last_seq` receive all missed
 * messages via server-side replay with zero sequence gaps.
 *
 * Scenario (per VU):
 *   1. Connect, subscribe to the bench stream, hold for PHASE1_DURATION_S.
 *      Track the last JetStream sequence number received.
 *   2. Disconnect — messages continue arriving in NATS JetStream (stored, not lost).
 *   3. Sleep RECONNECT_GAP_S — simulates the client being offline.
 *   4. Reconnect, send `{"type":"hello","last_seq":"N"}`, then re-subscribe.
 *      Server replays missed messages (replayed=true) before confirming.
 *   5. Continue receiving live messages for PHASE2_DURATION_S.
 *   6. Validate: no sequence gaps across the combined phase-1 + replay + live stream.
 *
 * IMPORTANT: Start tc-publish before running this test so messages are flowing:
 *
 *   ./target/release/tc-publish --stream bench --rate 1 --duration 120
 *
 * Usage:
 *   k6 run bench/k6/reconnect_test.js
 *
 *   k6 run bench/k6/reconnect_test.js \
 *     -e TARGET=100 \
 *     -e GATEWAY_WSS_URL=ws://localhost:9292/cable \
 *     -e PHASE1_DURATION_S=30 \
 *     -e RECONNECT_GAP_S=5 \
 *     -e PHASE2_DURATION_S=60
 *
 * Environment variables:
 *   TARGET               Number of concurrent VUs             (default: 100)
 *   GATEWAY_WSS_URL      WebSocket endpoint                   (default: ws://localhost:9292/cable)
 *   CHANNEL              ActionCable channel name             (default: BenchmarkChannel)
 *   STREAM               Stream to subscribe to               (default: bench)
 *   PHASE1_DURATION_S    Seconds to hold the initial conn     (default: 30)
 *   RECONNECT_GAP_S      Seconds offline before reconnecting  (default: 5)
 *   PHASE2_DURATION_S    Seconds to hold the reconnected conn (default: 60)
 *   JWT_TOKEN            Bearer token for JWT-auth gateways   (default: "")
 *
 * Pass criteria (thresholds):
 *   tc_sequence_gaps count == 0     — zero unreplayed message loss
 *   tc_connect_success rate > 0.99  — initial connections succeed
 *   tc_reconnect_success rate > 0.99 — reconnections succeed
 *   tc_connection_errors count < 10
 */

import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter, Gauge, Rate, Trend } from 'k6/metrics';

// ── Configuration ─────────────────────────────────────────────────────────────

const TARGET            = parseInt(__ENV.TARGET             || '100');
const URL               = __ENV.GATEWAY_WSS_URL            || 'ws://localhost:9292/cable';
const CHANNEL           = __ENV.CHANNEL                    || 'BenchmarkChannel';
const STREAM            = __ENV.STREAM                     || 'bench';
const PHASE1_DURATION_S = parseInt(__ENV.PHASE1_DURATION_S || '30');
const RECONNECT_GAP_S   = parseInt(__ENV.RECONNECT_GAP_S   || '5');
const PHASE2_DURATION_S = parseInt(__ENV.PHASE2_DURATION_S || '60');
const JWT_TOKEN         = __ENV.JWT_TOKEN                  || '';

// ── Custom metrics ─────────────────────────────────────────────────────────────

/** End-to-end fan-out latency in ms (live messages only, not replayed). */
const tcFanoutLatencyMs   = new Trend('tc_fanout_latency_ms', true);
/** Total messages received across all phases (live + replayed). */
const tcMessagesReceived  = new Counter('tc_messages_received');
/** Replayed messages received after reconnect (replayed=true). */
const tcReplayedMessages  = new Counter('tc_replayed_messages');
/**
 * Sequence gaps not covered by replay — any value > 0 means real message loss.
 *
 * A gap is counted when the next seq seen is higher than expected after
 * accounting for both live messages (phase 1) and replayed messages (phase 2).
 */
const tcSequenceGaps      = new Counter('tc_sequence_gaps');
/** WebSocket connection errors across both phases. */
const tcConnectionErrors  = new Counter('tc_connection_errors');
/** Rate of successful initial WebSocket upgrades. */
const tcConnectSuccess    = new Rate('tc_connect_success');
/** Rate of successful reconnect WebSocket upgrades. */
const tcReconnectSuccess  = new Rate('tc_reconnect_success');
/** Current active connections (updated on open/close). */
const tcActiveConns       = new Gauge('tc_active_connections');

// ── k6 scenario options ────────────────────────────────────────────────────────

// Each VU runs exactly one iteration: phase 1 → gap sleep → phase 2.
const MAX_DURATION_S = PHASE1_DURATION_S + RECONNECT_GAP_S + PHASE2_DURATION_S + 30;

export const options = {
  scenarios: {
    reconnect_test: {
      executor:    'per-vu-iterations',
      vus:         TARGET,
      iterations:  1,
      maxDuration: `${MAX_DURATION_S}s`,
    },
  },
  thresholds: {
    tc_sequence_gaps:     ['count==0'],
    tc_connect_success:   ['rate>0.99'],
    tc_reconnect_success: ['rate>0.99'],
    tc_connection_errors: ['count<10'],
  },
};

// ── VU default function ────────────────────────────────────────────────────────

export default function () {
  const identifier = JSON.stringify({ channel: CHANNEL, stream: STREAM });
  const headers    = {};
  if (JWT_TOKEN) headers['Authorization'] = `Bearer ${JWT_TOKEN}`;

  // ── Phase 1: initial connection ──────────────────────────────────────────────
  //
  // Connect and subscribe normally (no hello / last_seq).
  // Record the last JetStream sequence number seen so we can request replay
  // after reconnecting.

  let lastSeq = -1;

  const res1 = ws.connect(URL, { headers, subprotocols: ['actioncable-v1-json'] }, (socket) => {
    tcActiveConns.add(1);

    socket.on('open', () => {
      tcConnectSuccess.add(true);
      socket.send(JSON.stringify({ command: 'subscribe', identifier }));
    });

    socket.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch (_) { return; }

      if (msg.type === 'welcome' || msg.type === 'ping' || msg.type === 'confirm_subscription') {
        return;
      }

      if (msg.identifier === identifier && msg.message && msg.message.seq !== undefined) {
        tcMessagesReceived.add(1);

        // Latency measurement (phase 1 is always live).
        if (msg.message.sent_at !== undefined) {
          const latency = Date.now() - Number(msg.message.sent_at);
          if (latency >= 0 && latency < 30_000) {
            tcFanoutLatencyMs.add(latency);
          }
        }

        // Phase 1 sequence gap detection (before any disconnect).
        const seq = Number(msg.message.seq);
        if (lastSeq >= 0 && seq !== lastSeq + 1) {
          tcSequenceGaps.add(Math.max(0, seq - lastSeq - 1));
        }
        if (seq > lastSeq) {
          lastSeq = seq;
        }
      }
    });

    socket.on('error', () => {
      tcConnectSuccess.add(false);
      tcConnectionErrors.add(1);
    });

    socket.on('close', () => {
      tcActiveConns.add(-1);
    });

    // Disconnect cleanly after the phase 1 window.
    socket.setTimeout(() => {
      socket.close(1000, 'phase 1 complete');
    }, PHASE1_DURATION_S * 1000);
  });

  check(res1, { 'phase1 ws upgrade 101': (r) => r && r.status === 101 });
  if (!res1 || res1.status !== 101) {
    tcConnectSuccess.add(false);
    tcConnectionErrors.add(1);
  }

  // Simulate the client being offline.  Messages published during this window
  // are stored in NATS JetStream and will be replayed on reconnect.
  sleep(RECONNECT_GAP_S);

  // ── Phase 2: reconnect with replay ──────────────────────────────────────────
  //
  // Send `{"type":"hello","last_seq":"N"}` before subscribing.  The server
  // calls replay_since(stream, N) which delivers missed messages tagged with
  // `replayed: true` before sending confirm_subscription.  After that,
  // live messages flow normally.
  //
  // Gap validation:
  //   - expectedSeq starts at lastSeq+1 (first seq that should have been replayed).
  //   - Every replayed and live message must arrive in order with no holes.
  //   - Any missing seq increments tcSequenceGaps.

  let expectedSeq       = lastSeq >= 0 ? lastSeq + 1 : -1;
  let subscribeConfirmed = false;

  const res2 = ws.connect(URL, { headers, subprotocols: ['actioncable-v1-json'] }, (socket) => {
    tcActiveConns.add(1);

    socket.on('open', () => {
      tcReconnectSuccess.add(true);

      // Announce last_seq so the server knows where to start replay from.
      if (lastSeq >= 0) {
        socket.send(JSON.stringify({ type: 'hello', last_seq: String(lastSeq) }));
      }

      // Re-subscribe — server will replay missed messages before confirming.
      socket.send(JSON.stringify({ command: 'subscribe', identifier }));
    });

    socket.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch (_) { return; }

      if (msg.type === 'welcome' || msg.type === 'ping') return;

      if (msg.type === 'confirm_subscription') {
        subscribeConfirmed = true;
        return;
      }

      if (msg.identifier === identifier && msg.message && msg.message.seq !== undefined) {
        tcMessagesReceived.add(1);

        const seq       = Number(msg.message.seq);
        const isReplayed = msg.message.replayed === true;

        if (isReplayed) {
          tcReplayedMessages.add(1);
        } else if (msg.message.sent_at !== undefined) {
          // Only track latency for live (non-replayed) messages.
          const latency = Date.now() - Number(msg.message.sent_at);
          if (latency >= 0 && latency < 30_000) {
            tcFanoutLatencyMs.add(latency);
          }
        }

        // Continuous sequence validation across replay + live.
        if (expectedSeq >= 0) {
          if (seq > expectedSeq) {
            // Gap: seqs [expectedSeq .. seq-1] were never delivered.
            tcSequenceGaps.add(seq - expectedSeq);
          }
          // Advance only if this seq is at or beyond expectation (skip duplicates).
          if (seq >= expectedSeq) {
            expectedSeq = seq + 1;
          }
        } else {
          // No phase 1 data — begin tracking from this message onward.
          expectedSeq = seq + 1;
        }
      }
    });

    socket.on('error', () => {
      tcReconnectSuccess.add(false);
      tcConnectionErrors.add(1);
    });

    socket.on('close', () => {
      tcActiveConns.add(-1);
    });

    // Hold for phase 2 duration then close cleanly.
    socket.setTimeout(() => {
      socket.close(1000, 'phase 2 complete');
    }, PHASE2_DURATION_S * 1000);
  });

  check(res2, {
    'phase2 ws upgrade 101':                (r) => r && r.status === 101,
    'subscription confirmed after reconnect': () => subscribeConfirmed,
  });

  if (!res2 || res2.status !== 101) {
    tcReconnectSuccess.add(false);
    tcConnectionErrors.add(1);
  }
}
