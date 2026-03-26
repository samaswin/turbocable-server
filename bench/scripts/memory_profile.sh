#!/usr/bin/env bash
# memory_profile.sh — Monitor RSS and per-connection memory usage.
#
# Polls /proc/<pid>/status every 15 s and prints:
#   conns=<active>  rss=<total_kb>kB  per=<kb_per_conn>kB
#
# Usage:
#   # Profile the running turbocable-server
#   bash bench/scripts/memory_profile.sh
#
#   # Or target a specific PID
#   PID=12345 bash bench/scripts/memory_profile.sh
#
# Requires: curl, awk, bc, and a running gateway exposing /metrics on port 9292.

set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://127.0.0.1:9292}"
POLL_INTERVAL="${POLL_INTERVAL:-15}"  # seconds

PID="${PID:-$(pgrep -x turbocable-server 2>/dev/null | head -1)}"

if [[ -z "${PID:-}" ]]; then
    echo "ERROR: turbocable-server process not found. Set PID= or start the server first." >&2
    exit 1
fi

echo "Profiling PID=$PID every ${POLL_INTERVAL}s. Press Ctrl-C to stop."
echo "Timestamp                 conns    rss(kB)    per(kB)"
echo "-----------------------------------------------------------"

while true; do
    RSS=$(awk '/VmRSS/{print $2}' /proc/"$PID"/status 2>/dev/null || echo 0)

    CONNS=$(curl -sf "${GATEWAY_URL}/metrics" \
        | awk '/^turbocable_connections_active / {print $2; exit}') \
        || CONNS=0

    if [[ "$CONNS" -gt 0 ]]; then
        PER=$(echo "scale=1; $RSS / $CONNS" | bc)
    else
        PER="N/A"
    fi

    printf "%-26s  %7s  %9s  %8s\n" \
        "$(date '+%Y-%m-%d %H:%M:%S')" "$CONNS" "$RSS" "$PER"

    sleep "$POLL_INTERVAL"
done
