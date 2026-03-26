#!/usr/bin/env bash
# run_cluster.sh — multi-agent cluster load test (k6 + optional tc-publish).
#
# Target: 1M connections total, p99 < 50 ms, zero message loss.
# Run this script simultaneously on 10 separate k6 agent machines,
# each directing 100k connections at the load balancer.
#
# Prerequisites:
#   1. 3-node turbocable-server cluster behind a load balancer
#   2. k6 installed on each agent machine
#   3. tc-publish running on any one machine (it publishes to NATS once; NATS replicates)
#   4. OS tuning on all agent machines: sudo bash bench/scripts/tune_os.sh
#
# Usage (run on each of 10 agent machines simultaneously):
#   TARGET=100000 \
#   GATEWAY_WSS_URL=wss://lb.example.com/cable \
#   NATS_URL=nats://nats1.example.com:4222 \
#   bash bench/scripts/run_cluster.sh

set -euo pipefail

TARGET="${TARGET:-100000}"
GATEWAY_WSS_URL="${GATEWAY_WSS_URL:-wss://lb.example.com/cable}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
STREAM="${STREAM:-bench}"
RAMP_DURATION="${RAMP_DURATION:-2m}"
DURATION="${DURATION:-10m}"
PUBLISH_RATE="${PUBLISH_RATE:-10}"   # msg/s — only needed on the designated publisher agent

AGENT_ID="${AGENT_ID:-$(hostname)}"
IS_PUBLISHER="${IS_PUBLISHER:-false}"  # set to "true" on exactly one agent

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
K6_SCRIPT="$PROJECT_ROOT/bench/k6/load_1m.js"
PUBLISH_BIN="$PROJECT_ROOT/target/release/tc-publish"

echo "================================================================"
echo "  TurboCable — cluster load test"
echo "  Agent:    $AGENT_ID   (publisher: $IS_PUBLISHER)"
echo "  Target:   $TARGET connections (total across all agents: 10×$TARGET)"
echo "  Gateway:  $GATEWAY_WSS_URL"
echo "  Ramp:     $RAMP_DURATION  |  Sustain: $DURATION"
echo "================================================================"

if ! command -v k6 &>/dev/null; then
    echo "ERROR: k6 not found. Install from https://k6.io/docs/getting-started/installation/" >&2
    exit 1
fi

# Only one agent publishes messages to avoid duplicating fan-out load.
if [[ "$IS_PUBLISHER" == "true" ]]; then
    if [[ ! -f "$PUBLISH_BIN" ]]; then
        echo "ERROR: $PUBLISH_BIN not found. Run: cargo build --release --bin tc-publish" >&2
        exit 1
    fi

    PUBLISH_DURATION=$(( $(echo "$RAMP_DURATION" | grep -oP '\d+(?=m)' || echo 0) * 60 \
                       + $(echo "$DURATION"       | grep -oP '\d+(?=m)' || echo 0) * 60 \
                       + 120 ))

    echo "--> [publisher] Starting tc-publish: $PUBLISH_RATE msg/s for ${PUBLISH_DURATION}s"
    "$PUBLISH_BIN" \
        --nats-url "$NATS_URL" \
        --stream   "$STREAM" \
        --rate     "$PUBLISH_RATE" \
        --duration "$PUBLISH_DURATION" \
        --quiet &
    PUBLISH_PID=$!
    trap 'kill $PUBLISH_PID 2>/dev/null || true' EXIT
fi

echo "--> Running k6 (agent: $AGENT_ID)"
k6 run "$K6_SCRIPT" \
    -e TARGET="$TARGET" \
    -e GATEWAY_WSS_URL="$GATEWAY_WSS_URL" \
    -e STREAM="$STREAM" \
    -e RAMP_DURATION="$RAMP_DURATION" \
    -e DURATION="$DURATION" \
    -e LATENCY_P99_MS=50

echo "Agent $AGENT_ID complete."
