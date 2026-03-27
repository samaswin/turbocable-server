# Benchmark metrics log

Append-only history for runs on **Ubuntu / WSL** (see `docs/load-testing-1m.md`). Each block includes:

- **Date** (UTC)
- **Messages received** (and replayed, if applicable) from k6 custom metrics
- **tc-publish (send)**: configured rate × duration (approximate message count)
- **k6 wall time** and **fan-out receive latency** percentiles (`tc_fanout_latency_ms`) when exported

Optional raw k6 JSON: `bench/results/artifacts/k6-*.json` (gitignored by default).

## Reconnect + replay (k6) — `2026-03-26 00:36:03 UTC`

| Field | Value |
|-------|-------|
| **k6 wall time (script)** | 96s |
| **tc-publish (send)** | 10 msg/s × 155s ≈ **1550** messages published* |

\* Publisher message count is approximate (integer rate × duration).

---

## Single-node load (load_1m.js) — `2026-03-26 00:39:35 UTC`

| Field | Value |
|-------|-------|
| **k6 wall time (script)** | 211s |
| **tc-publish (send)** | 10 msg/s × 300s ≈ **3000** messages published* |

\* Publisher message count is approximate (integer rate × duration).

---

## Reconnect + replay (k6) — `2026-03-26 00:44:31 UTC`

| Field | Value |
|-------|-------|
| **k6 wall time (script)** | 96s |
| **tc-publish (send)** | 10 msg/s × 155s ≈ **1550** messages published* |

\* Publisher message count is approximate (integer rate × duration).

---

## Single-node load (load_1m.js) — `2026-03-26 00:49:04 UTC`

| Field | Value |
|-------|-------|
| **k6 wall time (script)** | 211s |
| **tc-publish (send)** | 10 msg/s × 300s ≈ **3000** messages published* |

\* Publisher message count is approximate (integer rate × duration).

---

## Reconnect + replay (k6) — `2026-03-26 02:19:42 UTC`

| Field | Value |
|-------|-------|
| **k6 wall time (script)** | 95s |
| **tc-publish (send)** | 500 msg/s × 155s ≈ **77500** messages published* |

\* Publisher message count is approximate (integer rate × duration).

---

## Single-node load (load_1m.js) — `2026-03-26 02:23:22 UTC`

| Field | Value |
|-------|-------|
| **k6 wall time (script)** | 207s |
| **tc-publish (send)** | 500 msg/s × 300s ≈ **150000** messages published* |

\* Publisher message count is approximate (integer rate × duration).

---
