#!/usr/bin/env bash
# run_full_test_plan.sh — Automated end-to-end validation (see docs/load-testing-1m.md).
#
# Runs (in order): health check → reconnect/replay (k6) → crash recovery → load (k6 + tc-publish).
# Designed for Linux and WSL2 with bash; uses conservative defaults so a full run finishes
# without exhausting file descriptors on a laptop.
#
# Prerequisites:
#   - Rust / cargo (WSL with asdf is fine)
#   - k6, curl, python3 on PATH
#   - nats-server on PATH OR NATS already listening on 4222
#   - nats CLI for crash recovery (install from https://github.com/nats-io/natscli/releases)
#     If missing, use --skip-crash
#
# Usage (from repository root):
#   bash bench/scripts/run_full_test_plan.sh
#   bash bench/scripts/run_full_test_plan.sh --quick   # skips crash if nats CLI absent
#   bash bench/scripts/run_full_test_plan.sh --target 5000 --skip-crash
#
# Environment overrides (optional):
#   GATEWAY_PORT, NATS_URL, MAX_CONN_PER_IP, TARGET_LOAD, RAMP_DURATION, DURATION,
#   LATENCY_P99_MS, PUBLISH_RATE, NO_BUILD=1, BENCH_METRICS_MD (override metrics log path)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# shellcheck source=bench_metrics_lib.sh
source "$SCRIPT_DIR/bench_metrics_lib.sh"

GATEWAY_PORT="${GATEWAY_PORT:-9292}"
GATEWAY_HTTP="http://127.0.0.1:${GATEWAY_PORT}"
GATEWAY_WS="ws://127.0.0.1:${GATEWAY_PORT}/cable"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
MAX_CONN_PER_IP="${MAX_CONN_PER_IP:-}"          # computed after --target is parsed
TARGET_LOAD="${TARGET_LOAD:-1000}"
RAMP_DURATION="${RAMP_DURATION:-1m}"
DURATION="${DURATION:-5m}"
LATENCY_P99_MS="${LATENCY_P99_MS:-50}"
PUBLISH_RATE="${PUBLISH_RATE:-500}"

QUICK=0
SKIP_CRASH=0
SKIP_RECONNECT=0
SKIP_LOAD=0
WITH_BENCH=0
NO_BUILD="${NO_BUILD:-0}"
RP_PID=""
TARGET_LOAD_CLI=0

usage() {
    cat <<EOF
run_full_test_plan.sh — automated validation on WSL/Linux (see docs/load-testing-1m.md).

Order: health → reconnect/replay (k6) → crash recovery → sustained load (k6 + tc-publish).

Prerequisites: cargo, k6, curl, python3; nats-server or NATS on 4222; nats CLI for crash test (or --skip-crash).

Environment: GATEWAY_PORT, NATS_URL, MAX_CONN_PER_IP, TARGET_LOAD, RAMP_DURATION, DURATION,
LATENCY_P99_MS, PUBLISH_RATE, NO_BUILD=1, BENCH_METRICS_MD

Options:
  --quick              Shorter reconnect + smaller load (faster on WSL); skips crash recovery if nats CLI missing
  --skip-crash         Skip crash recovery
  --skip-reconnect     Skip reconnect / replay (k6)
  --skip-load          Skip sustained load (k6 + tc-publish)
  --with-bench         After load: cargo bench --bench registry_bench
  --no-build           Do not run cargo build (binaries must exist)
  --target N           Concurrent connections for sustained load (default: ${TARGET_LOAD})
  -h, --help           Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick) QUICK=1; shift ;;
        --skip-crash) SKIP_CRASH=1; shift ;;
        --skip-reconnect) SKIP_RECONNECT=1; shift ;;
        --skip-load) SKIP_LOAD=1; shift ;;
        --with-bench) WITH_BENCH=1; shift ;;
        --no-build) NO_BUILD=1; shift ;;
        --target)
            TARGET_LOAD="$2"
            TARGET_LOAD_CLI=1
            shift 2
            ;;
        -h|--help) usage; exit 0 ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

# Default per-IP limit to TARGET + 10% + 100 so all k6 VUs (same IP) are never rejected.
MAX_CONN_PER_IP="${MAX_CONN_PER_IP:-$(( TARGET_LOAD + TARGET_LOAD / 10 + 100 ))}"

# k6 summary JSON (reconnect test); timestamp expanded once per run
K6_RECONNECT_SUMMARY="$PROJECT_ROOT/bench/results/artifacts/k6-reconnect-$(date -u '+%Y%m%d-%H%M%S').json"

if [[ "$QUICK" -eq 1 ]]; then
    [[ "$TARGET_LOAD_CLI" -eq 1 ]] || TARGET_LOAD=200
    RAMP_DURATION=30s
    DURATION=1m
    RECONNECT_VUS=50
    RECONNECT_PHASE1=10
    RECONNECT_GAP=3
    RECONNECT_PHASE2=25
    PUBLISH_RATE=5
else
    RECONNECT_VUS=100
    RECONNECT_PHASE1=30
    RECONNECT_GAP=5
    RECONNECT_PHASE2=60
fi

GATEWAY_BIN="$PROJECT_ROOT/target/release/turbocable-server"
PUBLISH_BIN="$PROJECT_ROOT/target/release/tc-publish"
K6_LOAD="$PROJECT_ROOT/bench/k6/load_1m.js"
K6_RECONNECT="$PROJECT_ROOT/bench/k6/reconnect_test.js"
GW_LOG=/tmp/tc-plan-gateway.log
NATS_LOG=/tmp/tc-plan-nats.log

GW_PID=""
NATS_PID=""

log() { printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*"; }
die() { printf '[%s] ERROR: %s\n' "$(date '+%H:%M:%S')" "$*" >&2; exit 1; }

tcp_open() {
    local host=$1 port=$2
    if command -v nc &>/dev/null; then
        nc -z "$host" "$port" 2>/dev/null
    else
        bash -c "echo >/dev/tcp/${host}/${port}" &>/dev/null
    fi
}

wait_tcp() {
    local host=$1 port=$2 msg=$3
    local i=0
    while (( i < 100 )); do
        tcp_open "$host" "$port" && return 0
        sleep 0.2
        ((i++)) || true
    done
    die "$msg"
}

cleanup() {
    if [[ -n "${RP_PID:-}" ]]; then
        kill "$RP_PID" 2>/dev/null || true
        wait "$RP_PID" 2>/dev/null || true
        RP_PID=""
    fi
    if [[ -n "${GW_PID:-}" ]]; then
        log "Stopping gateway (PID $GW_PID)..."
        kill "$GW_PID" 2>/dev/null || true
        wait "$GW_PID" 2>/dev/null || true
        GW_PID=""
    fi
    if [[ -n "${NATS_PID:-}" ]]; then
        log "Stopping NATS we started (PID $NATS_PID)..."
        kill "$NATS_PID" 2>/dev/null || true
        wait "$NATS_PID" 2>/dev/null || true
        NATS_PID=""
    fi
}

trap cleanup EXIT

# --- WSL hint (informational) ---
if [[ -f /proc/version ]] && grep -qi microsoft /proc/version; then
    log "Detected WSL — using WSL-friendly targets (see docs/load-testing-1m.md for bare-metal 333k/1M)."
fi

# --- OS tuning ---
if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    log "Applying OS tuning (tune_os.sh)..."
    bash "$SCRIPT_DIR/tune_os.sh"
else
    log "NOTE: Run 'sudo bash bench/scripts/tune_os.sh' before load testing for best results (skipping — not root)."
fi

# --- Preflight ---
for cmd in cargo k6 curl python3; do
    command -v "$cmd" &>/dev/null || die "Missing required command: $cmd"
done

if [[ "$SKIP_CRASH" -eq 0 ]] && ! command -v nats &>/dev/null; then
    if [[ "$QUICK" -eq 1 ]]; then
        log "nats CLI not found — skipping crash recovery for this --quick run. Install: https://github.com/nats-io/natscli/releases — or run without --quick and use --skip-crash if you omit the CLI."
        SKIP_CRASH=1
    else
        die "nats CLI not found (needed for crash test). Install nats CLI or pass --skip-crash"
    fi
fi

if [[ "$NO_BUILD" -eq 0 ]]; then
    log "Building release binaries..."
    cargo build --release --bin tc-publish
    cargo build --release
else
    [[ -f "$GATEWAY_BIN" ]] || die "Missing $GATEWAY_BIN (run without --no-build)"
    [[ -f "$PUBLISH_BIN" ]] || die "Missing $PUBLISH_BIN (run without --no-build)"
fi

# --- NATS ---
if tcp_open 127.0.0.1 4222; then
    log "NATS already accepting connections on 4222"
else
    command -v nats-server &>/dev/null || die "NATS not running and nats-server not in PATH. Start NATS or: sudo apt install nats-server (or run nats-server --jetstream)"
    log "Starting nats-server --jetstream (log: $NATS_LOG)"
    : >"$NATS_LOG"
    nats-server --jetstream >>"$NATS_LOG" 2>&1 &
    NATS_PID=$!
    wait_tcp 127.0.0.1 4222 "NATS did not open port 4222 (see $NATS_LOG)"
    log "NATS ready (PID $NATS_PID)"
fi

if [[ "$SKIP_CRASH" -eq 0 ]]; then
    # Crash recovery needs JetStream + nats CLI; `server ping` often fails on otherwise-working servers.
    if ! js_check_out=$(nats stream ls --server "$NATS_URL" 2>&1); then
        printf '[%s] ERROR: NATS JetStream not usable at %s (required for crash recovery).\n' "$(date '+%H:%M:%S')" "$NATS_URL" >&2
        printf '%s\n' "$js_check_out" >&2
        printf '%s\n' '' "Typical fixes: nats-server --jetstream; set NATS_URL for TLS (tls://...) or auth (nats://user:pass@...); or --skip-crash." >&2
        exit 1
    fi
    log "NATS JetStream reachable at $NATS_URL"
fi

start_gateway() {
    : >"$GW_LOG"
    log "Starting turbocable-server on port $GATEWAY_PORT (max-connections-per-ip=$MAX_CONN_PER_IP)..."
    RUST_LOG=warn \
        "$GATEWAY_BIN" \
        --port "$GATEWAY_PORT" \
        --nats-url "$NATS_URL" \
        --max-connections-per-ip "$MAX_CONN_PER_IP" \
        >>"$GW_LOG" 2>&1 &
    GW_PID=$!
    local i=0
    while (( i < 60 )); do
        if curl -sf "$GATEWAY_HTTP/health" &>/dev/null; then
            log "Gateway healthy (PID $GW_PID)"
            return 0
        fi
        sleep 0.5
        ((i++)) || true
    done
    die "Gateway did not become healthy (see $GW_LOG)"
}

stop_gateway() {
    if [[ -z "${GW_PID:-}" ]]; then
        return 0
    fi
    log "Stopping gateway PID $GW_PID..."
    kill "$GW_PID" 2>/dev/null || true
    wait "$GW_PID" 2>/dev/null || true
    GW_PID=""
    # Ensure port is free for the next step
    sleep 1
}

# =============================================================================
log "======== Health check ========="
start_gateway
curl -sf "$GATEWAY_HTTP/health" | python3 -m json.tool &>/dev/null || curl -sf "$GATEWAY_HTTP/health"
echo ""

# =============================================================================
if [[ "$SKIP_RECONNECT" -eq 0 ]]; then
    log "======== Reconnect + replay (k6) ========="
    RECONNECT_PUBLISH_SECS=$(( RECONNECT_PHASE1 + RECONNECT_GAP + RECONNECT_PHASE2 + 60 ))
    log "Starting tc-publish (${PUBLISH_RATE} msg/s, ${RECONNECT_PUBLISH_SECS}s) for reconnect test..."
    "$PUBLISH_BIN" \
        --nats-url "$NATS_URL" \
        --stream bench \
        --rate "$PUBLISH_RATE" \
        --duration "$RECONNECT_PUBLISH_SECS" \
        --quiet &
    RP_PID=$!
    sleep 2
    mkdir -p "$PROJECT_ROOT/bench/results/artifacts"
    phase1_start="$(date +%s)"
    set +e
    k6 run "$K6_RECONNECT" \
        -e "TARGET=$RECONNECT_VUS" \
        -e "GATEWAY_WSS_URL=$GATEWAY_WS" \
        -e "PHASE1_DURATION_S=$RECONNECT_PHASE1" \
        -e "RECONNECT_GAP_S=$RECONNECT_GAP" \
        -e "PHASE2_DURATION_S=$RECONNECT_PHASE2" \
        --summary-export="$K6_RECONNECT_SUMMARY"
    k6_rc=$?
    set -e
    phase1_end="$(date +%s)"
    phase1_wall=$((phase1_end - phase1_start))
    kill "$RP_PID" 2>/dev/null || true
    wait "$RP_PID" 2>/dev/null || true
    RP_PID=""
    [[ "$k6_rc" -eq 0 ]] || die "Reconnect k6 run failed (exit $k6_rc)"
    bench_metrics_append "$PROJECT_ROOT" "Reconnect + replay (k6)" "$phase1_wall" "$K6_RECONNECT_SUMMARY" "$PUBLISH_RATE" "$RECONNECT_PUBLISH_SECS"
    log "Reconnect + replay finished."
else
    log "Skipping reconnect / replay (--skip-reconnect)"
fi

# =============================================================================
stop_gateway

if [[ "$SKIP_CRASH" -eq 0 ]]; then
    log "======== Crash recovery ========="
    NATS_URL="$NATS_URL" GATEWAY_PORT="$GATEWAY_PORT" bash "$SCRIPT_DIR/crash_recovery_test.sh"
    log "Crash recovery finished."
else
    log "Skipping crash recovery (--skip-crash)"
fi

# =============================================================================
if [[ "$SKIP_LOAD" -eq 0 ]]; then
    log "======== Sustained load (k6 + tc-publish) ========="
    start_gateway
    export TARGET="$TARGET_LOAD"
    export GATEWAY_WSS_URL="$GATEWAY_WS"
    export GATEWAY_HTTP_URL="$GATEWAY_HTTP"
    export NATS_URL
    export RAMP_DURATION
    export DURATION
    export LATENCY_P99_MS
    export PUBLISH_RATE
    export MEMORY_PROFILE=false
    bash "$SCRIPT_DIR/run_single_node.sh"
    log "Sustained load finished."
    stop_gateway
else
    log "Skipping sustained load (--skip-load)"
    # If we skipped load but didn't run crash, leave gateway down; if skipped reconnect only, gateway already stopped
    [[ -n "${GW_PID:-}" ]] && stop_gateway
fi

# =============================================================================
if [[ "$WITH_BENCH" -eq 1 ]]; then
    log "======== Criterion registry bench ========="
    cargo bench --bench registry_bench
    log "Criterion bench finished."
fi

trap - EXIT
cleanup

log "================================================================"
log "All requested steps completed successfully."
log "Logs: $GW_LOG (last gateway run), $NATS_LOG (if we started NATS)"
log "Metrics: bench/results/benchmark_metrics.md (k6 runs; override with BENCH_METRICS_MD)"
log "================================================================"
