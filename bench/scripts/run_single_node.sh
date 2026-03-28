#!/usr/bin/env bash
# run_single_node.sh — single-node k6 + tc-publish load test.
#
# Target: 333k connections, p99 < 30 ms fan-out, < 8 KB/connection.
#
# Prerequisites:
#   1. turbocable-server running on NODE1 (default: 127.0.0.1:9292 — good for Ubuntu/WSL)
#   2. k6 installed: https://k6.io/docs/getting-started/installation/
#   3. tc-publish binary compiled: cargo build --release --bin tc-publish
#   4. python3 on PATH (parses RAMP_DURATION/DURATION unless PUBLISH_DURATION_SECS is set)
#   5. OS tuning applied: sudo bash bench/scripts/tune_os.sh
#
# Usage:
#   # Run from project root
#   bash bench/scripts/run_single_node.sh
#
#   # Override target and URL
#   TARGET=10000 GATEWAY_WSS_URL=ws://myserver:9292/cable bash bench/scripts/run_single_node.sh

set -euo pipefail

TARGET="${TARGET:-333000}"
# Default to 127.0.0.1 so Ubuntu/WSL resolves the same as run_full_test_plan (avoids ::1 / localhost quirks).
GATEWAY_WSS_URL="${GATEWAY_WSS_URL:-ws://127.0.0.1:9292/cable}"
GATEWAY_HTTP_URL="${GATEWAY_HTTP_URL:-http://127.0.0.1:9292}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
STREAM="${STREAM:-bench}"
RAMP_DURATION="${RAMP_DURATION:-2m}"
DURATION="${DURATION:-10m}"
PUBLISH_RATE="${PUBLISH_RATE:-500}"  # msg/s during sustained phase
PROTOCOL="${PROTOCOL:-json}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# shellcheck source=bench_metrics_lib.sh
source "$SCRIPT_DIR/bench_metrics_lib.sh"

K6_SCRIPT="$PROJECT_ROOT/bench/k6/load_1m.js"
PUBLISH_BIN="$PROJECT_ROOT/target/release/tc-publish"
K6_LOAD_SUMMARY="$PROJECT_ROOT/bench/results/artifacts/k6-load-$(date -u '+%Y%m%d-%H%M%S').json"

echo "================================================================"
echo "  TurboCable — single-node load test"
echo "  Target:   $TARGET connections"
echo "  Gateway:  $GATEWAY_WSS_URL"
echo "  Ramp:     $RAMP_DURATION  |  Sustain: $DURATION"
echo "================================================================"

# --- OS tuning ---
if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    echo "--> Applying OS tuning (tune_os.sh)..."
    bash "$SCRIPT_DIR/tune_os.sh"
else
    echo "NOTE: Run 'sudo bash bench/scripts/tune_os.sh' before load testing for best results (skipping — not root)."
fi

# Verify prerequisites
if ! command -v k6 &>/dev/null; then
    echo "ERROR: k6 not found. Install from https://k6.io/docs/getting-started/installation/" >&2
    exit 1
fi

if [[ ! -f "$PUBLISH_BIN" ]]; then
    echo "ERROR: $PUBLISH_BIN not found. Run: cargo build --release --bin tc-publish" >&2
    exit 1
fi

# Seconds tc-publish must run (ramp + sustain + buffer). Requires python3 so
# RAMP_DURATION / DURATION can use k6 forms like 30s, 2m, 1m30s (reliable on Ubuntu/WSL).
if [[ -n "${PUBLISH_DURATION_SECS:-}" ]]; then
    PUBLISH_DURATION="$PUBLISH_DURATION_SECS"
else
    command -v python3 &>/dev/null || {
        echo "ERROR: python3 required to parse RAMP_DURATION/DURATION, or set PUBLISH_DURATION_SECS" >&2
        exit 1
    }
    PUBLISH_DURATION="$(python3 -c "
import re, sys
def secs(s):
    t = 0
    for n, u in re.findall(r'(\d+)([hms])', s):
        n = int(n)
        t += {'h': 3600, 'm': 60, 's': 1}[u] * n
    return t
r, d = sys.argv[1], sys.argv[2]
print(secs(r) + secs(d) + 120)
" "$RAMP_DURATION" "$DURATION")"
fi
LATENCY_P95_MS="${LATENCY_P95_MS:-50}"
LATENCY_P99_MS="${LATENCY_P99_MS:-75}"

# --- Purge NATS stream ---
# A fresh consumer uses DeliverPolicy::All (needed for reconnect replay), so if
# the stream has messages from prior runs, the server will replay them all as a
# burst before processing live messages — corrupting latency and sequence metrics.
# Always purge before benchmarking so every run starts from a clean state.
if command -v nats &>/dev/null; then
    echo "--> Purging NATS stream: TURBOCABLE"
    nats stream purge TURBOCABLE --force 2>/dev/null \
        || echo "WARNING: stream purge failed (stream may not exist yet — first run is OK)"
else
    echo "WARNING: 'nats' CLI not found — skipping stream purge." >&2
    echo "         Old messages in the TURBOCABLE stream will cause inflated latency and false sequence gaps." >&2
    echo "         Install: https://github.com/nats-io/natscli/releases" >&2
fi

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
    if command -v bc &>/dev/null && command -v curl &>/dev/null; then
        export GATEWAY_URL="$GATEWAY_HTTP_URL"
        bash "$SCRIPT_DIR/memory_profile.sh" \
            2>/dev/null | tee /tmp/tc-memory-profile.log &
        MEM_PID=$!
        trap 'kill $PUBLISH_PID $MEM_PID 2>/dev/null || true' EXIT
    else
        echo "WARNING: bc or curl missing; skipping memory_profile.sh (install: sudo apt install bc curl)" >&2
    fi
fi

mkdir -p "$PROJECT_ROOT/bench/results/artifacts"
load_start="$(date +%s)"
echo "--> Running k6 load test"
set +e
k6 run "$K6_SCRIPT" \
    -e TARGET="$TARGET" \
    -e GATEWAY_WSS_URL="$GATEWAY_WSS_URL" \
    -e STREAM="$STREAM" \
    -e RAMP_DURATION="$RAMP_DURATION" \
    -e DURATION="$DURATION" \
    -e PROTOCOL="$PROTOCOL" \
    -e "LATENCY_P95_MS=$LATENCY_P95_MS" \
    -e "LATENCY_P99_MS=$LATENCY_P99_MS" \
    --summary-export="$K6_LOAD_SUMMARY"
k6_rc=$?
set -e
load_end="$(date +%s)"
load_wall=$((load_end - load_start))
[[ "$k6_rc" -eq 0 ]] || {
    echo "ERROR: k6 exited with code $k6_rc (no metrics row appended)" >&2
    exit "$k6_rc"
}

bench_metrics_append "$PROJECT_ROOT" "Single-node load (load_1m.js)" "$load_wall" "$K6_LOAD_SUMMARY" "$PUBLISH_RATE" "$PUBLISH_DURATION"

echo "================================================================"
echo "  Done. Metrics appended to: bench/results/benchmark_metrics.md"
echo "  k6 summary JSON: $K6_LOAD_SUMMARY"
echo "  Check /tmp/tc-memory-profile.log for RSS data (if memory profiler ran)."
echo "  Prometheus metrics: $GATEWAY_HTTP_URL/metrics"
echo "================================================================"
