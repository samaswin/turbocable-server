#!/usr/bin/env bash
# run_single_node.sh — Phase 10.1 single-node baseline test.
#
# Target: 333k connections, p99 < 30 ms fan-out, < 8 KB/connection.
#
# Prerequisites:
#   1. turbocable-server running on NODE1 (default: localhost:9292)
#   2. k6 installed: https://k6.io/docs/getting-started/installation/
#   3. tc-publish binary compiled: cargo build --release --bin tc-publish
#   4. OS tuning applied: sudo bash bench/scripts/tune_os.sh
#
# Usage:
#   # Run from project root
#   bash bench/scripts/run_single_node.sh
#
#   # Override target and URL
#   TARGET=10000 GATEWAY_WSS_URL=ws://myserver:9292/cable bash bench/scripts/run_single_node.sh

set -euo pipefail

TARGET="${TARGET:-333000}"
GATEWAY_WSS_URL="${GATEWAY_WSS_URL:-ws://localhost:9292/cable}"
GATEWAY_HTTP_URL="${GATEWAY_HTTP_URL:-http://localhost:9292}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
STREAM="${STREAM:-bench}"
RAMP_DURATION="${RAMP_DURATION:-2m}"
DURATION="${DURATION:-10m}"
PUBLISH_RATE="${PUBLISH_RATE:-10}"   # msg/s during sustained phase

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
K6_SCRIPT="$PROJECT_ROOT/bench/k6/load_1m.js"
PUBLISH_BIN="$PROJECT_ROOT/target/release/tc-publish"

echo "================================================================"
echo "  TurboCable Phase 10.1 — Single-Node Baseline"
echo "  Target:   $TARGET connections"
echo "  Gateway:  $GATEWAY_WSS_URL"
echo "  Ramp:     $RAMP_DURATION  |  Sustain: $DURATION"
echo "================================================================"

# Verify prerequisites
if ! command -v k6 &>/dev/null; then
    echo "ERROR: k6 not found. Install from https://k6.io/docs/getting-started/installation/" >&2
    exit 1
fi

if [[ ! -f "$PUBLISH_BIN" ]]; then
    echo "ERROR: $PUBLISH_BIN not found. Run: cargo build --release --bin tc-publish" >&2
    exit 1
fi

# Start the message publisher in the background
PUBLISH_DURATION=$(( $(echo "$RAMP_DURATION" | grep -oP '\d+(?=m)' || echo 0) * 60 \
                   + $(echo "$DURATION"       | grep -oP '\d+(?=m)' || echo 0) * 60 \
                   + 120 ))  # add 2 min buffer

echo "--> Starting tc-publish: $PUBLISH_RATE msg/s for ${PUBLISH_DURATION}s"
"$PUBLISH_BIN" \
    --nats-url "$NATS_URL" \
    --stream   "$STREAM" \
    --rate     "$PUBLISH_RATE" \
    --duration "$PUBLISH_DURATION" \
    --quiet &
PUBLISH_PID=$!
trap 'kill $PUBLISH_PID 2>/dev/null || true' EXIT

# Start memory profiler in the background (if the server is local)
if [[ "${MEMORY_PROFILE:-true}" == "true" ]]; then
    bash "$SCRIPT_DIR/memory_profile.sh" \
        2>/dev/null | tee /tmp/tc-memory-profile.log &
    MEM_PID=$!
    trap 'kill $PUBLISH_PID $MEM_PID 2>/dev/null || true' EXIT
fi

echo "--> Running k6 load test"
k6 run "$K6_SCRIPT" \
    -e TARGET="$TARGET" \
    -e GATEWAY_WSS_URL="$GATEWAY_WSS_URL" \
    -e STREAM="$STREAM" \
    -e RAMP_DURATION="$RAMP_DURATION" \
    -e DURATION="$DURATION" \
    -e LATENCY_P99_MS=30

echo "================================================================"
echo "  Done. Check /tmp/tc-memory-profile.log for RSS data."
echo "  Prometheus metrics: $GATEWAY_HTTP_URL/metrics"
echo "================================================================"
