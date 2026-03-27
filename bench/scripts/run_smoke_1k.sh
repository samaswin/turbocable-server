#!/usr/bin/env bash
set -euo pipefail

# Smoke load test runner:
# - starts nats-server --jetstream
# - starts turbocable-server (release binary)
# - runs bench/scripts/run_single_node.sh
# - prints a small metrics snapshot loop
# - cleans up on exit

TARGET="${TARGET:-1000}"
PUBLISH_RATE="${PUBLISH_RATE:-50}"
RAMP_DURATION="${RAMP_DURATION:-1m}"
DURATION="${DURATION:-3m}"
LATENCY_P99_MS="${LATENCY_P99_MS:-50}"
PROTOCOL="${PROTOCOL:-json}"
ADAPTIVE="${ADAPTIVE:-false}"              # true/1/yes enables stepped publish-rate run
RATE_STEPS="${RATE_STEPS:-25,50,75,100}"  # comma-separated rates for adaptive mode

PORT="${PORT:-9292}"
NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
GATEWAY_WSS_URL="${GATEWAY_WSS_URL:-ws://127.0.0.1:${PORT}/cable}"
METRICS_URL="${METRICS_URL:-http://127.0.0.1:${PORT}/metrics}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:${PORT}/health}"

MAX_CONN_PER_IP="${TURBOCABLE_MAX_CONN_PER_IP:-5000}"
WS_CHANNEL_CAPACITY="${TURBOCABLE_WS_CHANNEL_CAPACITY:-4096}"
MAX_ACK_PENDING="${TURBOCABLE_MAX_ACK_PENDING:-10000}"

# WSL/Ubuntu typically won't apply limits.conf reliably; set per-shell.
ULIMIT_NOFILE="${ULIMIT_NOFILE:-1048576}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN_GATEWAY="${ROOT_DIR}/target/release/turbocable-server"

nats_pid=""
gw_pid=""
metrics_pid=""
run_ts="$(date -u +%Y%m%d-%H%M%S)"
LOG_DIR="${ROOT_DIR}/bench/results/artifacts"
NATS_LOG="${LOG_DIR}/nats-${run_ts}.log"
GATEWAY_LOG="${LOG_DIR}/gateway-${run_ts}.log"
NATS_PIDFILE="${LOG_DIR}/nats.pid"
GATEWAY_PIDFILE="${LOG_DIR}/gateway.pid"

cleanup() {
  set +e
  [[ -n "${metrics_pid}" ]] && kill "${metrics_pid}" 2>/dev/null || true
  [[ -n "${gw_pid}" ]] && kill "${gw_pid}" 2>/dev/null || true
  [[ -n "${nats_pid}" ]] && kill "${nats_pid}" 2>/dev/null || true
  [[ -f "${GATEWAY_PIDFILE}" ]] && rm -f "${GATEWAY_PIDFILE}" 2>/dev/null || true
  [[ -f "${NATS_PIDFILE}" ]] && rm -f "${NATS_PIDFILE}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "==> Repo: ${ROOT_DIR}"
cd "${ROOT_DIR}"
mkdir -p "${LOG_DIR}"

echo "==> Stopping any previous run (best-effort)"
stop_pidfile() {
  local pidfile="$1"
  local label="$2"
  if [[ -f "$pidfile" ]]; then
    local pid
    pid="$(cat "$pidfile" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "==> Killing previous ${label} pid=${pid}"
      kill "$pid" 2>/dev/null || true
      sleep 0.2
      kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pidfile" 2>/dev/null || true
  fi
}
stop_port() {
  local port="$1"
  local label="$2"
  if command -v fuser >/dev/null 2>&1; then
    # Kill anything bound to the port (TCP) so the script can start fresh.
    if fuser "${port}/tcp" >/dev/null 2>&1; then
      echo "==> Killing existing ${label} on port ${port}"
      fuser -k "${port}/tcp" >/dev/null 2>&1 || true
    fi
  fi
}

stop_pidfile "${GATEWAY_PIDFILE}" "gateway"
stop_pidfile "${NATS_PIDFILE}" "NATS"
stop_port "${PORT}" "gateway"
stop_port "4222" "NATS"

echo "==> ulimit -n (before): $(ulimit -n)"
ulimit -n "${ULIMIT_NOFILE}" 2>/dev/null || true
echo "==> ulimit -n (after):  $(ulimit -n)"

if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
  echo "==> Applying OS tuning"
  bash bench/scripts/tune_os.sh
else
  echo "==> NOTE: not root; skipping auto tune_os.sh"
  echo "    (recommended once: sudo bash bench/scripts/tune_os.sh)"
fi

echo "==> Starting NATS (JetStream): nats-server --jetstream"
command -v nats-server >/dev/null 2>&1 || {
  echo "ERROR: nats-server not found on PATH. Install NATS, then retry." >&2
  exit 1
}

echo "==> Starting NATS detached (nohup + setsid)"
(
  ulimit -n "${ULIMIT_NOFILE}" 2>/dev/null || true
  nohup setsid nats-server --jetstream >"${NATS_LOG}" 2>&1 < /dev/null &
  echo $! >"${NATS_PIDFILE}"
) >/dev/null 2>&1
nats_pid="$(cat "${NATS_PIDFILE}" 2>/dev/null || true)"
echo "${nats_pid}" >"${NATS_PIDFILE}"
echo "==> NATS pid=${nats_pid} (logs: ${NATS_LOG})"

echo "==> Waiting for NATS port 4222"
for _ in {1..50}; do
  if (echo >/dev/tcp/127.0.0.1/4222) >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
(echo >/dev/tcp/127.0.0.1/4222) >/dev/null 2>&1 || {
  echo "ERROR: NATS did not start (check ${NATS_LOG})" >&2
  exit 1
}

echo "==> Purging TURBOCABLE stream before starting gateway (best-effort)"
if command -v nats >/dev/null 2>&1; then
  nats stream purge TURBOCABLE --force >/dev/null 2>&1 || true
else
  echo "==> NOTE: nats CLI not found; skipping pre-purge (run_single_node.sh will purge if present)"
fi

echo "==> Building release binaries (if needed)"
if [[ ! -x "${BIN_GATEWAY}" ]]; then
  cargo build --release --bin turbocable-server --bin tc-publish
fi

echo "==> Starting turbocable-server"
export RUST_LOG="${RUST_LOG:-info}"
export TURBOCABLE_PORT="${PORT}"
export TURBOCABLE_NATS_URL="${NATS_URL}"
export TURBOCABLE_MAX_CONN_PER_IP="${MAX_CONN_PER_IP}"
export TURBOCABLE_WS_CHANNEL_CAPACITY="${WS_CHANNEL_CAPACITY}"
export TURBOCABLE_MAX_ACK_PENDING="${MAX_ACK_PENDING}"

echo "==> Starting gateway detached (nohup + setsid)"
(
  ulimit -n "${ULIMIT_NOFILE}" 2>/dev/null || true
  nohup setsid "${BIN_GATEWAY}" >"${GATEWAY_LOG}" 2>&1 < /dev/null &
  echo $! >"${GATEWAY_PIDFILE}"
) >/dev/null 2>&1
gw_pid="$(cat "${GATEWAY_PIDFILE}" 2>/dev/null || true)"
echo "${gw_pid}" >"${GATEWAY_PIDFILE}"
echo "==> gateway pid=${gw_pid} (logs: ${GATEWAY_LOG})"

echo "==> Waiting for /health"
for _ in {1..50}; do
  if curl -fsS "${HEALTH_URL}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
curl -fsS "${HEALTH_URL}" || { echo "ERROR: gateway not healthy (check ${GATEWAY_LOG})"; exit 1; }

echo "==> Starting metrics watcher (backpressure reconnects/lag/active) every 2s"
(
  while true; do
    ts="$(date -u +%H:%M:%S)"
    m="$(curl -fsS "${METRICS_URL}" || true)"
    active="$(awk '/^turbocable_connections_active /{print $2}' <<<"$m")"
    backpressure="$(awk '/^turbocable_forced_reconnect_backpressure_total /{print $2}' <<<"$m")"
    lag="$(awk '/^turbocable_nats_consumer_lag /{print $2}' <<<"$m")"
    echo "${ts} active=${active:-?} backpressure_reconnects=${backpressure:-?} lag=${lag:-?}"
    sleep 2
  done
) &
metrics_pid="$!"

echo "==> Running single-node load test"
run_one() {
  local rate="$1"
  TARGET="${TARGET}" \
  PUBLISH_RATE="${rate}" \
  RAMP_DURATION="${RAMP_DURATION}" \
  DURATION="${DURATION}" \
  PROTOCOL="${PROTOCOL}" \
  LATENCY_P99_MS="${LATENCY_P99_MS}" \
  GATEWAY_WSS_URL="${GATEWAY_WSS_URL}" \
  NATS_URL="${NATS_URL}" \
  bash bench/scripts/run_single_node.sh
}

if [[ "${ADAPTIVE,,}" == "true" || "${ADAPTIVE}" == "1" || "${ADAPTIVE,,}" == "yes" ]]; then
  echo "==> Adaptive mode enabled (PROTOCOL=${PROTOCOL}, threshold p99<${LATENCY_P99_MS}ms)"
  echo "==> Rate steps: ${RATE_STEPS}"
  IFS=',' read -r -a rates <<< "${RATE_STEPS}"

  highest_pass=""
  for rate in "${rates[@]}"; do
    rate="$(echo "$rate" | tr -d '[:space:]')"
    [[ -n "$rate" ]] || continue
    echo "==> Testing publish rate=${rate} msg/s"
    set +e
    run_one "$rate"
    rc=$?
    set -e
    if [[ "$rc" -eq 0 ]]; then
      highest_pass="$rate"
      echo "==> PASS at rate=${rate}"
    else
      echo "==> FAIL at rate=${rate} (exit ${rc})"
      break
    fi
  done

  if [[ -n "${highest_pass}" ]]; then
    echo "==> Highest passing publish rate: ${highest_pass} msg/s"
  else
    echo "==> No passing rate found in RATE_STEPS=${RATE_STEPS}" >&2
    exit 1
  fi
else
  run_one "${PUBLISH_RATE}"
fi

echo "==> Done"
