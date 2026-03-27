/**
 * TurboCable — backpressure eviction + replay recovery (k6)
 *
 * Exercises the path where the gateway evicts a slow consumer: outbound channel
 * fills, `fanout_encoded` fails `try_send`, the connection receives a
 * `disconnect` with reason `backpressure_reconnect_required`, then the client
 * reconnects with `last_seq` and must see zero sequence gaps (same invariant as
 * reconnect_test.js).
 *
 * **Triggering eviction:** run the gateway with a small outbound buffer and a
 * steady publisher, and slow the client with `SLOW_SLEEP_S` so it falls behind.
 * Example:
 *
 *   TURBOCABLE_WS_CHANNEL_CAPACITY=8 ./target/release/turbocable-server ...
 *   ./target/release/tc-publish --stream bench --rate 200 --duration 600
 *   SLOW_SLEEP_S=0.05 REQUIRE_BACKPRESSURE_EVICT=1 k6 run bench/k6/backpressure_eviction_test.js
 *
 * If eviction does not occur (buffer too large or publish rate too low), leave
 * `REQUIRE_BACKPRESSURE_EVICT` unset — the test still validates replay after a
 * normal phase-1 close.
 *
 * Usage:
 *   k6 run bench/k6/backpressure_eviction_test.js
 *
 * Environment variables:
 *   TARGET, GATEWAY_WSS_URL, CHANNEL, STREAM, JWT_TOKEN — same as reconnect_test.js
 *   PHASE1_DURATION_S, RECONNECT_GAP_S, PHASE2_DURATION_S
 *   SLOW_SLEEP_S           Extra delay per data message in phase 1 (seconds; default 0)
 *   REQUIRE_BACKPRESSURE_EVICT  If "1", fail unless at least one disconnect with
 *                          backpressure reason is observed in phase 1
 */

import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter, Gauge, Rate, Trend } from 'k6/metrics';

const TARGET = parseInt(__ENV.TARGET || '1');
const URL = __ENV.GATEWAY_WSS_URL || 'ws://localhost:9292/cable';
const CHANNEL = __ENV.CHANNEL || 'BenchmarkChannel';
const STREAM = __ENV.STREAM || 'bench';
const PHASE1_DURATION_S = parseInt(__ENV.PHASE1_DURATION_S || '45');
const RECONNECT_GAP_S = parseInt(__ENV.RECONNECT_GAP_S || '3');
const PHASE2_DURATION_S = parseInt(__ENV.PHASE2_DURATION_S || '60');
const JWT_TOKEN = __ENV.JWT_TOKEN || '';
const SLOW_SLEEP_S = parseFloat(__ENV.SLOW_SLEEP_S || '0');
const REQUIRE_BP = __ENV.REQUIRE_BACKPRESSURE_EVICT === '1';

const tcFanoutLatencyMs = new Trend('tc_fanout_latency_ms', true);
const tcMessagesReceived = new Counter('tc_messages_received');
const tcReplayedMessages = new Counter('tc_replayed_messages');
const tcSequenceGaps = new Counter('tc_sequence_gaps');
const tcConnectionErrors = new Counter('tc_connection_errors');
const tcConnectSuccess = new Rate('tc_connect_success');
const tcReconnectSuccess = new Rate('tc_reconnect_success');
const tcActiveConns = new Gauge('tc_active_connections');
/** Server disconnect frames with backpressure reason (recoverable eviction). */
const tcBackpressureDisconnects = new Counter('tc_backpressure_disconnects');

const MAX_DURATION_S = PHASE1_DURATION_S + RECONNECT_GAP_S + PHASE2_DURATION_S + 30;

const thresholds = {
  tc_sequence_gaps: ['count==0'],
  tc_connect_success: ['rate>0.99'],
  tc_reconnect_success: ['rate>0.99'],
  tc_connection_errors: ['count<10'],
};
if (REQUIRE_BP) {
  thresholds.tc_backpressure_disconnects = ['count>=1'];
}

export const options = {
  scenarios: {
    backpressure_eviction: {
      executor: 'per-vu-iterations',
      vus: TARGET,
      iterations: 1,
      maxDuration: `${MAX_DURATION_S}s`,
    },
  },
  thresholds,
};

const identifier = JSON.stringify({ channel: CHANNEL, stream: STREAM });

export default function () {
  const headers = {};
  if (JWT_TOKEN) headers['Authorization'] = `Bearer ${JWT_TOKEN}`;

  let lastSeq = -1;
  let sawBackpressureDisconnect = false;

  const res1 = ws.connect(URL, { headers, subprotocols: ['actioncable-v1-json'] }, (socket) => {
    tcActiveConns.add(1);

    socket.on('open', () => {
      tcConnectSuccess.add(true);
      socket.send(
        JSON.stringify({
          type: 'hello',
          capabilities: ['replay_v1'],
        }),
      );
      socket.send(JSON.stringify({ command: 'subscribe', identifier }));
    });

    socket.on('message', (raw) => {
      if (SLOW_SLEEP_S > 0) {
        sleep(SLOW_SLEEP_S);
      }
      let msg;
      try {
        msg = JSON.parse(raw);
      } catch (_) {
        return;
      }

      if (msg.type === 'disconnect') {
        const r = msg.reason || '';
        if (r.includes('backpressure_reconnect_required')) {
          sawBackpressureDisconnect = true;
          tcBackpressureDisconnects.add(1);
        }
        return;
      }

      if (msg.type === 'welcome' || msg.type === 'ping' || msg.type === 'confirm_subscription') {
        return;
      }

      if (msg.identifier === identifier && msg.message && msg.message.seq !== undefined) {
        tcMessagesReceived.add(1);
        if (msg.message.sent_at !== undefined) {
          const latency = Date.now() - Number(msg.message.sent_at);
          if (latency >= 0 && latency < 30_000) {
            tcFanoutLatencyMs.add(latency);
          }
        }
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

    socket.setTimeout(() => {
      socket.close(1000, 'phase 1 complete');
    }, PHASE1_DURATION_S * 1000);
  });

  check(res1, { 'phase1 ws upgrade 101': (r) => r && r.status === 101 });
  if (!res1 || res1.status !== 101) {
    tcConnectSuccess.add(false);
    tcConnectionErrors.add(1);
  }

  sleep(RECONNECT_GAP_S);

  let expectedSeq = lastSeq >= 0 ? lastSeq + 1 : -1;
  let subscribeConfirmed = false;

  const res2 = ws.connect(URL, { headers, subprotocols: ['actioncable-v1-json'] }, (socket) => {
    tcActiveConns.add(1);

    socket.on('open', () => {
      tcReconnectSuccess.add(true);
      if (lastSeq >= 0) {
        socket.send(
          JSON.stringify({
            type: 'hello',
            last_seq: String(lastSeq),
            capabilities: ['replay_v1'],
          }),
        );
      } else {
        socket.send(JSON.stringify({ type: 'hello', capabilities: ['replay_v1'] }));
      }
      socket.send(JSON.stringify({ command: 'subscribe', identifier }));
    });

    socket.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw);
      } catch (_) {
        return;
      }
      if (msg.type === 'welcome' || msg.type === 'ping') return;
      if (msg.type === 'confirm_subscription') {
        subscribeConfirmed = true;
        return;
      }
      if (msg.identifier === identifier && msg.message && msg.message.seq !== undefined) {
        tcMessagesReceived.add(1);
        const seq = Number(msg.message.seq);
        const isReplayed = msg.message.replayed === true;
        if (isReplayed) {
          tcReplayedMessages.add(1);
        } else if (msg.message.sent_at !== undefined) {
          const latency = Date.now() - Number(msg.message.sent_at);
          if (latency >= 0 && latency < 30_000) {
            tcFanoutLatencyMs.add(latency);
          }
        }
        if (expectedSeq >= 0) {
          if (seq > expectedSeq) {
            tcSequenceGaps.add(seq - expectedSeq);
          }
          if (seq >= expectedSeq) {
            expectedSeq = seq + 1;
          }
        } else {
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

    socket.setTimeout(() => {
      socket.close(1000, 'phase 2 complete');
    }, PHASE2_DURATION_S * 1000);
  });

  check(res2, {
    'phase2 ws upgrade 101': (r) => r && r.status === 101,
    'subscription confirmed after reconnect': () => subscribeConfirmed,
  });
  if (!res2 || res2.status !== 101) {
    tcReconnectSuccess.add(false);
    tcConnectionErrors.add(1);
  }

  if (REQUIRE_BP && !sawBackpressureDisconnect) {
    check(null, {
      'expected backpressure disconnect in phase 1': () => false,
    });
  }
}
