# `bench/` — load testing assets

Scripts, k6 tests, and benchmark results for WebSocket load validation.

**Documentation:** [docs/load-testing-1m.md](../docs/load-testing-1m.md) (prerequisites, layout, every runner, `tc-publish`, tuning, Prometheus).

**Quick links:**

- Automated plan (WSL-friendly): `bash bench/scripts/run_full_test_plan.sh --help` (add `--with-backpressure` for eviction + replay k6 after reconnect)
- Single node: `bash bench/scripts/run_single_node.sh`
- k6 only: `k6 run bench/k6/reconnect_test.js`, `k6 run bench/k6/backpressure_eviction_test.js`
- Metrics log: [bench/results/benchmark_metrics.md](results/benchmark_metrics.md)
