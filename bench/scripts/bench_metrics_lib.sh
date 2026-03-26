#!/usr/bin/env bash
# bench_metrics_lib.sh — append k6 summary + timing to bench/results/benchmark_metrics.md
#
# Source from bench scripts after SCRIPT_DIR / PROJECT_ROOT are set:
#   # shellcheck source=bench_metrics_lib.sh
#   source "$SCRIPT_DIR/bench_metrics_lib.sh"

# Sets METRICS_MD; creates parent dirs and optional artifacts dir.
bench_metrics_ensure_file() {
    local project_root="$1"
    METRICS_MD="${BENCH_METRICS_MD:-$project_root/bench/results/benchmark_metrics.md}"
    mkdir -p "$(dirname "$METRICS_MD")"
    mkdir -p "$project_root/bench/results/artifacts"
    if [[ ! -f "$METRICS_MD" ]]; then
        cat >"$METRICS_MD" <<'EOF'
# Benchmark metrics log

Append-only history for runs on **Ubuntu / WSL** (see `docs/load-testing-1m.md`). Each block includes:

- **Date** (UTC)
- **Messages received** (and replayed, if applicable) from k6 custom metrics
- **tc-publish (send)**: configured rate × duration (approximate message count)
- **k6 wall time** and **fan-out receive latency** percentiles (`tc_fanout_latency_ms`) when exported

Optional raw k6 JSON: `bench/results/artifacts/k6-*.json` (gitignored by default).

EOF
    fi
}

# Append one Markdown section.
# Args:
#   $1 project_root
#   $2 title (e.g. "Reconnect + replay (k6)")
#   $3 wall_seconds (k6 or combined phase wall clock)
#   $4 k6_summary_json path (optional; empty skips metric extraction)
#   $5 publish_rate (optional; empty skips send row)
#   $6 publish_duration_secs (optional)
bench_metrics_append() {
    local project_root="$1"
    local title="$2"
    local wall_s="$3"
    local k6_json="${4:-}"
    local pub_rate="${5:-}"
    local pub_dur="${6:-}"

    bench_metrics_ensure_file "$project_root"
    local iso
    iso="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"

    {
        echo ""
        echo "## $title — \`$iso\`"
        echo ""
        echo "| Field | Value |"
        echo "|-------|-------|"
        echo "| **k6 wall time (script)** | ${wall_s}s |"
        if [[ -n "$pub_rate" && -n "$pub_dur" ]]; then
            local est=$((pub_rate * pub_dur))
            echo "| **tc-publish (send)** | ${pub_rate} msg/s × ${pub_dur}s ≈ **${est}** messages published* |"
        fi
        if [[ -n "$k6_json" && -f "$k6_json" ]]; then
            python3 - "$k6_json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as f:
    d = json.load(f)

metrics = d.get("metrics") or {}
state = d.get("state") or {}
tr_ms = state.get("testRunDurationMs")
if tr_ms is not None:
    print(f"| **k6 test run duration** | {tr_ms / 1000:.1f}s |")


def counter(name: str):
    m = metrics.get(name) or {}
    v = m.get("values") or {}
    return v.get("count")


def trend_line(name: str) -> str:
    m = metrics.get(name) or {}
    v = m.get("values") or {}
    if not v:
        return ""
    parts = []
    for k in ("min", "avg", "med", "max", "p(90)", "p(95)", "p(99)"):
        if k not in v:
            continue
        val = v[k]
        if isinstance(val, (int, float)):
            parts.append(f"{k}={val:.3f}ms")
        else:
            parts.append(f"{k}={val}")
    return ", ".join(parts)


c = counter("tc_messages_received")
if c is not None:
    print(f"| **Messages received (k6)** | {c} |")

cr = counter("tc_replayed_messages")
if cr is not None:
    print(f"| **Replayed messages (k6)** | {cr} |")

tg = counter("tc_sequence_gaps")
if tg is not None:
    print(f"| **Sequence gaps** | {tg} |")

lat = trend_line("tc_fanout_latency_ms")
if lat:
    print(f"| **Fan-out receive latency** | {lat} |")
PY
        elif [[ -n "$k6_json" ]]; then
            echo "| **k6 summary JSON** | missing file \`$k6_json\` |"
        fi
        if [[ -n "$pub_rate" && -n "$pub_dur" ]]; then
            echo ""
            echo "\\* Publisher message count is approximate (integer rate × duration)."
        fi
        echo ""
        echo "---"
    } >>"$METRICS_MD"
}
