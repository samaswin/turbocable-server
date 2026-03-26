# Benchmark metrics log

Append-only history for runs on **Ubuntu / WSL** (see `docs/load-testing-1m.md`). Each block includes:

- **Date** (UTC)
- **Messages received** (and replayed, if applicable) from k6 custom metrics
- **tc-publish (send)**: configured rate × duration (approximate message count)
- **k6 wall time** and **fan-out receive latency** percentiles (`tc_fanout_latency_ms`) when exported

Optional raw k6 JSON: `bench/results/artifacts/k6-*.json` (gitignored by default).
