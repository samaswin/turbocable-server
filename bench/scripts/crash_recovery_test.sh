#!/usr/bin/env bash
# crash_recovery_test.sh — crash-recovery test (SIGKILL gateway, verify delivery).
#
# Validates zero data loss across a hard gateway crash (SIGKILL).
#
# Steps:
#   1. Start turbocable-server with a fixed --node-id
#   2. Start tc-publish: send N_MESSAGES at PUBLISH_RATE msg/s
#   3. After KILL_AFTER_S seconds (≈ half the messages), SIGKILL the gateway
#   4. Restart the gateway with the same --node-id
#   5. Wait for NATS durable consumer num_pending to reach 0
#   6. Report PASS or FAIL
#
# Prerequisites:
#   1. nats-server running with JetStream:  nats-server --jetstream
#   2. tc-publish binary compiled:  cargo build --release --bin tc-publish
#   3. turbocable-server binary compiled:  cargo build --release
#   4. nats CLI installed:  https://github.com/nats-io/natscli/releases
#
# Usage (from project root):
#   bash bench/scripts/crash_recovery_test.sh
#
#   N_MESSAGES=200 PUBLISH_RATE=5 bash bench/scripts/crash_recovery_test.sh
#
# Environment variables:
#   NATS_URL          NATS server URL              (default: nats://localhost:4222)
#   GATEWAY_PORT      Gateway HTTP/WS port         (default: 9292)
#   STREAM            JetStream stream name        (default: bench)
#   NODE_ID           Fixed gateway node-id        (default: crash-test-node)
#   N_MESSAGES        Total messages to publish    (default: 100)
#   PUBLISH_RATE      Messages per second          (default: 2)
#   KILL_AFTER_S      Seconds before SIGKILL       (default: N_MESSAGES/PUBLISH_RATE/2)
#   WAIT_TIMEOUT_S    Max seconds to wait for 0 pending after restart (default: 60)

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────

NATS_URL="${NATS_URL:-nats://localhost:4222}"
GATEWAY_PORT="${GATEWAY_PORT:-9292}"
STREAM="${STREAM:-bench}"
NODE_ID="${NODE_ID:-crash-test-node}"
N_MESSAGES="${N_MESSAGES:-100}"
PUBLISH_RATE="${PUBLISH_RATE:-2}"
# Kill the gateway after this many seconds (≈ half the messages published).
# Default: floor(N / rate / 2), minimum 5s.
_HALF=$(( N_MESSAGES / PUBLISH_RATE / 2 ))
KILL_AFTER_S="${KILL_AFTER_S:-$(( _HALF > 5 ? _HALF : 5 ))}"
WAIT_TIMEOUT_S="${WAIT_TIMEOUT_S:-60}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GATEWAY_BIN="$PROJECT_ROOT/target/release/turbocable-server"
PUBLISH_BIN="$PROJECT_ROOT/target/release/tc-publish"
CONSUMER_NAME="gw_${NODE_ID}"
GATEWAY_LOG="/tmp/tc-crash-recovery-gateway.log"

# ── Helpers ────────────────────────────────────────────────────────────────────

log()  { printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }
fail() { printf '[FAIL] %s\n' "$*" >&2; exit 1; }

check_prereqs() {
    if [[ ! -f "$GATEWAY_BIN" ]]; then
        fail "$GATEWAY_BIN not found. Run: cargo build --release"
    fi
    if [[ ! -f "$PUBLISH_BIN" ]]; then
        fail "$PUBLISH_BIN not found. Run: cargo build --release --bin tc-publish"
    fi
    if ! command -v nats &>/dev/null; then
        fail "nats CLI not found. Install from https://github.com/nats-io/natscli/releases"
    fi
    if ! nats server ping --server "$NATS_URL" &>/dev/null; then
        fail "NATS not reachable at $NATS_URL. Start it with: nats-server --jetstream"
    fi
    if ! command -v python3 &>/dev/null; then
        fail "python3 not found — required to parse nats consumer JSON output"
    fi
}

# Start the gateway binary in the background and wait until /health responds.
start_gateway() {
    log "Starting gateway (node_id=$NODE_ID, port=$GATEWAY_PORT)..."
    > "$GATEWAY_LOG"
    RUST_LOG=warn \
    TURBOCABLE_NODE_ID="$NODE_ID" \
    "$GATEWAY_BIN" \
        --port       "$GATEWAY_PORT" \
        --nats-url   "$NATS_URL" \
        --max-connections-per-ip 1000 \
        >> "$GATEWAY_LOG" 2>&1 &
    GATEWAY_PID=$!

    local attempts=0
    while ! curl -sf "http://localhost:${GATEWAY_PORT}/health" >/dev/null 2>&1; do
        attempts=$(( attempts + 1 ))
        if (( attempts > 30 )); then
            fail "Gateway did not become healthy within 15s. Check $GATEWAY_LOG"
        fi
        sleep 0.5
    done
    log "Gateway ready (PID $GATEWAY_PID)"
}

# Query the NATS consumer and return num_pending (integer), or -1 on error.
get_num_pending() {
    nats consumer info TURBOCABLE "$CONSUMER_NAME" \
        --server "$NATS_URL" --json 2>/dev/null \
    | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    # field name differs slightly across nats CLI versions
    val = d.get('num_pending', d.get('NumPending', None))
    print(int(val) if val is not None else -1)
except Exception:
    print(-1)
" 2>/dev/null \
    || echo "-1"
}

# ── Cleanup ────────────────────────────────────────────────────────────────────

GATEWAY_PID=""
PUBLISH_PID=""
cleanup() {
    [[ -n "$GATEWAY_PID"  ]] && kill "$GATEWAY_PID"  2>/dev/null || true
    [[ -n "$PUBLISH_PID"  ]] && kill "$PUBLISH_PID"  2>/dev/null || true
}
trap cleanup EXIT

# ── Banner ─────────────────────────────────────────────────────────────────────

_APPROX_AT=$(( KILL_AFTER_S * PUBLISH_RATE ))
echo "================================================================"
echo "  TurboCable — crash recovery test"
echo "  Messages:   $N_MESSAGES at $PUBLISH_RATE msg/s"
echo "  Kill after: ${KILL_AFTER_S}s  (≈ ${_APPROX_AT} messages published)"
echo "  Consumer:   $CONSUMER_NAME  on stream TURBOCABLE"
echo "  NATS:       $NATS_URL"
echo "================================================================"

check_prereqs

# ── Step 1: Start the gateway ─────────────────────────────────────────────────

start_gateway

# Brief pause so the durable consumer is created before publishing begins.
sleep 1

# ── Step 2: Start tc-publish (full N_MESSAGES burst) ──────────────────────────

# Add 10 s headroom so tc-publish exits cleanly after all messages are sent.
PUBLISH_DURATION=$(( N_MESSAGES / PUBLISH_RATE + 10 ))
log "Starting tc-publish: $N_MESSAGES messages (~${PUBLISH_DURATION}s)"
"$PUBLISH_BIN" \
    --nats-url "$NATS_URL" \
    --stream   "$STREAM" \
    --rate     "$PUBLISH_RATE" \
    --duration "$PUBLISH_DURATION" \
    --quiet &
PUBLISH_PID=$!

# ── Step 3: SIGKILL the gateway mid-stream ────────────────────────────────────

log "Waiting ${KILL_AFTER_S}s before SIGKILL..."
sleep "$KILL_AFTER_S"

log "Sending SIGKILL to gateway (PID $GATEWAY_PID)..."
kill -9 "$GATEWAY_PID" 2>/dev/null || true
GATEWAY_PID=""
# Give NATS a moment to register the client disconnect.
sleep 1

PENDING_BEFORE="$(get_num_pending)"
log "num_pending immediately after kill: $PENDING_BEFORE"

# ── Step 4: Restart the gateway with the same node_id ────────────────────────

log "Restarting gateway (same node_id=$NODE_ID)..."
start_gateway

# ── Wait for tc-publish to finish ─────────────────────────────────────────────

log "Waiting for tc-publish to finish publishing all $N_MESSAGES messages..."
wait "$PUBLISH_PID" || true
PUBLISH_PID=""
log "tc-publish done."

# ── Step 5: Poll until num_pending == 0 ───────────────────────────────────────

log "Waiting up to ${WAIT_TIMEOUT_S}s for consumer $CONSUMER_NAME to drain..."
ELAPSED=0
PENDING="-1"
while (( ELAPSED < WAIT_TIMEOUT_S )); do
    PENDING="$(get_num_pending)"
    if [[ "$PENDING" == "0" ]]; then
        break
    fi
    log "  num_pending=$PENDING  (${ELAPSED}s elapsed)"
    sleep 2
    ELAPSED=$(( ELAPSED + 2 ))
done

# ── Step 6: Report ─────────────────────────────────────────────────────────────

echo ""
echo "================================================================"
if [[ "$PENDING" == "0" ]]; then
    echo "  [PASS] All messages delivered after restart. num_pending = 0"
    echo "         Durable consumer 'gw_${NODE_ID}' resumed from last ACK"
    echo "         and redelivered all unacknowledged messages on restart."
else
    echo "  [FAIL] num_pending = $PENDING after ${WAIT_TIMEOUT_S}s"
    echo "         Some messages were not redelivered after the gateway restart."
    echo ""
    echo "  Diagnostics:"
    echo "    nats consumer info TURBOCABLE $CONSUMER_NAME --server $NATS_URL"
    echo "    cat $GATEWAY_LOG"
    exit 1
fi
echo "================================================================"

echo ""
log "Final consumer state:"
nats consumer info TURBOCABLE "$CONSUMER_NAME" --server "$NATS_URL" 2>/dev/null || true
