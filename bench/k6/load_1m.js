/**
 * TurboCable WebSocket Load Test — k6 script
 *
 * Establishes long-lived ActionCable WebSocket connections and measures:
 *   • Connection success rate
 *   • Fan-out latency (p50 / p95 / p99) — requires tc-publish sending messages
 *   • Sequence-number gaps (zero-message-loss validation)
 *   • Subscription confirmation rate
 *
 * Usage — single-node baseline (333k connections, p99 < 30 ms):
 *   k6 run bench/k6/load_1m.js \
 *     -e TARGET=333000 \
 *     -e GATEWAY_WSS_URL=ws://node1:9292/cable
 *
 * Usage — 3-node cluster (run 10 agents simultaneously, 100k each):
 *   k6 run bench/k6/load_1m.js \
 *     -e TARGET=100000 \
 *     -e GATEWAY_WSS_URL=wss://lb.example.com/cable
 *
 * Environment variables:
 *   TARGET            Number of concurrent WebSocket connections  (default: 1000)
 *   GATEWAY_WSS_URL   WebSocket endpoint                          (default: ws://localhost:9292/cable)
 *   CHANNEL           ActionCable channel name                    (default: BenchmarkChannel)
 *   STREAM            Stream to subscribe to                      (default: bench)
 *   RAMP_DURATION     Ramp-up time before sustained phase         (default: 2m)
 *   DURATION          Sustained hold duration                     (default: 10m)
 *   LATENCY_P99_MS    p99 threshold in ms (fail if exceeded)      (default: 50)
 *   JWT_TOKEN         Bearer token for JWT-authenticated gateways (default: "")
 *   PROTOCOL          "json" or "msgpack"                         (default: json)
 */

import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// ── Configuration ─────────────────────────────────────────────────────────────

const TARGET         = parseInt(__ENV.TARGET         || '1000');
const URL            = __ENV.GATEWAY_WSS_URL         || 'ws://localhost:9292/cable';
const CHANNEL        = __ENV.CHANNEL                 || 'BenchmarkChannel';
const STREAM         = __ENV.STREAM                  || 'bench';
const RAMP_DURATION  = __ENV.RAMP_DURATION           || '2m';
const DURATION       = __ENV.DURATION                || '10m';
const LATENCY_P99_MS = parseInt(__ENV.LATENCY_P99_MS || '50');
const JWT_TOKEN      = __ENV.JWT_TOKEN               || '';
const PROTOCOL       = __ENV.PROTOCOL                || 'json';  // "json" | "msgpack"

// ── Custom metrics ─────────────────────────────────────────────────────────────

/** End-to-end fan-out latency in ms (publisher timestamp → client receipt). */
const tcFanoutLatencyMs   = new Trend('tc_fanout_latency_ms', true);
/** Total fan-out messages received across all VUs. */
const tcMessagesReceived  = new Counter('tc_messages_received');
/** Sequence number gaps detected (> 0 means message loss). */
const tcSequenceGaps      = new Counter('tc_sequence_gaps');
/** Number of WebSocket connections that reported an error. */
const tcConnectionErrors  = new Counter('tc_connection_errors');
/** Subscriptions successfully confirmed by the server. */
const tcSubscribeOk       = new Counter('tc_subscribe_confirmed');
/** Rate of successful WebSocket upgrades. */
const tcConnectSuccess    = new Rate('tc_connect_success');
/** WebSocket connections opened (use with tc_connections_closed to derive active count). */
const tcConnectionsOpened = new Counter('tc_connections_opened');
/** WebSocket connections closed. */
const tcConnectionsClosed = new Counter('tc_connections_closed');

// ── k6 scenario options ────────────────────────────────────────────────────────

export const options = {
  scenarios: {
    ramp_up: {
      executor:          'ramping-vus',
      startVUs:          0,
      stages:            [{ duration: RAMP_DURATION, target: TARGET }],
      gracefulRampDown:  '30s',
    },
    sustained: {
      executor:    'constant-vus',
      vus:         TARGET,
      duration:    DURATION,
      startTime:   RAMP_DURATION,
      gracefulStop: '30s',
    },
  },
  thresholds: {
    tc_fanout_latency_ms:  [`p(99)<${LATENCY_P99_MS}`],
    tc_sequence_gaps:      ['count==0'],
    tc_connect_success:    ['rate>0.99'],
    tc_connection_errors:  ['count<100'],
  },
};

// ── Per-VU reconnect state ─────────────────────────────────────────────────────
// Module-level variables persist across iterations within the same VU, allowing
// the VU to send a `hello` with its last known sequence on reconnect so the
// server can replay any missed messages.

/** Last publisher sequence number received by this VU (-1 = never connected). */
let vuLastSeq = -1;

// ── Helper ─────────────────────────────────────────────────────────────────────

/**
 * Parse a k6-style duration string ("2m", "10m30s", "1h") into milliseconds.
 * Used to set a socket timeout slightly beyond the full test duration so that
 * k6 — not the server — drives the teardown.
 */
function parseDurationMs(s) {
  let ms = 0;
  const h = s.match(/(\d+)h/); if (h) ms += parseInt(h[1]) * 3600000;
  const m = s.match(/(\d+)m/); if (m) ms += parseInt(m[1]) * 60000;
  const sec = s.match(/(\d+)s/); if (sec) ms += parseInt(sec[1]) * 1000;
  return ms || 600000;
}

// Total test window: ramp + sustain + 60 s grace.
const SOCKET_TIMEOUT_MS =
  parseDurationMs(RAMP_DURATION) + parseDurationMs(DURATION) + 60_000;

// ── VU default function ────────────────────────────────────────────────────────

export default function () {
  const identifier = JSON.stringify({ channel: CHANNEL, stream: STREAM });

  const headers = {};
  if (JWT_TOKEN) headers['Authorization'] = `Bearer ${JWT_TOKEN}`;

  const subprotocol =
    PROTOCOL === 'msgpack' ? 'actioncable-v1-msgpack' : 'actioncable-v1-json';

  // Pick up where this VU left off (vuLastSeq persists across iterations).
  let lastSeq = vuLastSeq;
  const isReconnect = lastSeq >= 0;

  const res = ws.connect(URL, { headers, subprotocols: [subprotocol] }, function (socket) {
    tcConnectionsOpened.add(1);

    // ── Open ────────────────────────────────────────────────────────────────
    socket.on('open', function () {
      tcConnectSuccess.add(true);

      // On reconnect, tell the server the last sequence we received so it can
      // replay any messages we missed while disconnected.
      if (isReconnect) {
        socket.send(JSON.stringify({ type: 'hello', last_seq: String(lastSeq) }));
      }

      socket.send(JSON.stringify({ command: 'subscribe', identifier }));
    });

    // ── Message ─────────────────────────────────────────────────────────────
    socket.on('message', function (raw) {
      let msg;
      try { msg = JSON.parse(raw); } catch (_) { return; }

      switch (msg.type) {
        case 'welcome':
          // Server acknowledged the connection; subscribe command already sent.
          break;

        case 'confirm_subscription':
          tcSubscribeOk.add(1);
          break;

        case 'ping':
          // ActionCable server heartbeat — no response needed.
          break;

        default:
          // Fan-out message from tc-publish (live or replayed).
          if ((msg.identifier === identifier || msg.identifier === STREAM) && msg.message) {
            // Skip replayed messages in gap and latency accounting — they arrived
            // out of the live sequence and would inflate both metrics.
            const replayed = msg.replayed === true;

            if (!replayed) {
              tcMessagesReceived.add(1);

              // Latency: publisher embeds sent_at (Unix ms); we measure receipt time.
              if (msg.message.sent_at !== undefined) {
                const latency = Date.now() - Number(msg.message.sent_at);
                if (latency >= 0 && latency < 30_000) {
                  tcFanoutLatencyMs.add(latency);
                }
              }

              // Sequence tracking: detect any gaps (dropped messages).
              // Only check against the previous live message — gaps caused by
              // the reconnect window are already captured by this metric.
              // Server frame carries JetStream sequence as a top-level `seq` field.
              // Some publishers may also include `seq` inside the payload; prefer top-level.
              const seqRaw =
                msg.seq !== undefined ? msg.seq :
                (msg.message && msg.message.seq !== undefined ? msg.message.seq : undefined);
              if (seqRaw !== undefined) {
                const seq = Number(seqRaw);
                if (!Number.isNaN(seq)) {
                  if (lastSeq >= 0 && seq !== lastSeq + 1) {
                    tcSequenceGaps.add(Math.max(0, seq - lastSeq - 1));
                  }
                  lastSeq = seq;
                  vuLastSeq = seq;  // Persist for the next reconnect iteration.
                }
              }
            }
          }
      }
    });

    // ── Error ────────────────────────────────────────────────────────────────
    socket.on('error', function () {
      tcConnectSuccess.add(false);
      tcConnectionErrors.add(1);
    });

    // ── Close ────────────────────────────────────────────────────────────────
    socket.on('close', function () {
      tcConnectionsClosed.add(1);
    });

    // Hold the connection open for the entire test; k6 tears down VUs at scenario end.
    socket.setTimeout(function () {
      socket.close(1000, 'test complete');
    }, SOCKET_TIMEOUT_MS);
  });

  // Record whether the WebSocket upgrade itself succeeded (HTTP 101).
  const upgraded = check(res, { 'ws upgrade 101': (r) => r && r.status === 101 });
  if (!upgraded) {
    tcConnectSuccess.add(false);
    tcConnectionErrors.add(1);
  }

  // If this VU has a lastSeq (i.e. it connected before and is now reconnecting),
  // back off briefly to avoid a thundering-herd of reconnects all at once.
  if (vuLastSeq >= 0) {
    sleep(1 + Math.random() * 2);
  }
}
