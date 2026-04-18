# Development

Day-to-day commands for contributors, CI expectations, coding standards, and a
map of the Rust source tree.

If you are only running the server locally, start with [setup.md](setup.md).

---

## Windows hosts: run Rust through WSL

This repository is often developed on Windows with Rust installed **inside WSL**
(Ubuntu), not on the host. From Windows, invoke Cargo like this (adjust the path
to your clone):

```bash
wsl bash -ic "cd /mnt/c/Users/<you>/Git/turbocable-server && cargo build"
```

Use an interactive login shell (`bash -ic`) so tools such as **asdf** are on
`PATH`. Full agent notes live in [AGENTS.md](../AGENTS.md) at the repo root.

---

## Common commands

```bash
# Run with debug logging
RUST_LOG=debug cargo run

# Tests
cargo test

# Lint (all targets, deny warnings)
cargo clippy --all-targets --all-features -- -D warnings

# Format check
cargo fmt --all --check

# Documentation (warnings as errors)
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

---

## CI pipeline

Every push and pull request to `main` runs these checks in GitHub Actions:

| Job | What it checks |
|-----|----------------|
| **Formatting** | `cargo fmt --all --check` — style via `rustfmt.toml` |
| **Clippy** | `cargo clippy --all-targets --all-features -- -D warnings` |
| **Tests** | `cargo test --all-features` |
| **Documentation** | `cargo doc --no-deps` with `-D warnings` |
| **Security audit** | `cargo audit` |
| **MSRV** | `cargo check` at the `rust-version` declared in `Cargo.toml` |
| **Release** | Cross-compiles platform binaries, multi-platform Docker image, publish to `ghcr.io/<repository-owner>/turbocable-server` |

---

## Coding standards

- `#![warn(missing_docs)]` — public items should have doc comments
- `rustflags = ["-D", "warnings"]` in `.cargo/config.toml` — warnings are errors
- `rustfmt.toml` — line width, indentation
- `clippy.toml` — Clippy tuning (see file for MSRV notes)
- `.editorconfig` — consistent whitespace across editors

---

## Project structure

```
src/
├── main.rs                 # jemalloc (glibc Linux), Tokio runtime, startup
├── config.rs               # CLI/env configuration
├── server.rs               # Axum router, SO_REUSEPORT listener, NATS init
├── errors.rs               # Typed error hierarchy
├── auth/
│   ├── jwt.rs              # RS256 JWT verification, stream glob matching
│   └── key_watcher.rs      # NATS KV watcher + file fallback, hot-reload
├── connection/
│   ├── handler.rs          # WebSocket upgrade, per-connection lifecycle
│   ├── registry.rs         # DashMap registry (core data structure)
│   └── limiter.rs          # Per-IP connection limits
├── protocol/
│   ├── types.rs            # ClientCommand / ServerMessage enums
│   ├── json.rs             # JSON codec (actioncable-v1-json)
│   └── msgpack.rs          # Binary codec (rmp-serde)
├── pubsub/
│   └── nats.rs             # NATS JetStream consumer, fan-out, publish, replay
└── metrics.rs              # Prometheus metrics, /metrics handler
```

---

## Related documentation

- [setup.md](setup.md) — install, verify setup, NATS smoke tests
- [load-testing-1m.md](load-testing-1m.md) — benchmarks and k6
- [bench/README.md](../bench/README.md) — pointer to load-testing doc and results
