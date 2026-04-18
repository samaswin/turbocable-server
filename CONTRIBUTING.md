# Contributing to turbocable-server

Thank you for your interest in contributing. This guide covers everything you
need to build, test, and submit changes.

---

## Development environment

### Requirements

| Tool | Version | How to install |
|------|---------|----------------|
| Rust (stable) | ≥ 1.88 | [rustup](https://rustup.rs/) or asdf (see below) |
| NATS Server | ≥ 2.10 | [nats.io/download](https://nats.io/download/) |
| k6 | latest | [grafana.com/docs/k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) |

### Windows hosts — Rust through WSL

Rust is developed and tested inside **WSL (Ubuntu 24.04 LTS)**, managed by
[asdf](https://asdf-vm.com/). There is no native Windows Rust installation in
this repo.

All `cargo` commands must be run through WSL:

```bash
wsl bash -ic "cd /mnt/c/Users/<you>/Git/turbocable-server && <command>"
```

The `-i` flag sources `.bashrc` so asdf shims are on `PATH`. See
[AGENTS.md](AGENTS.md) and [docs/development.md](docs/development.md) for the
full agent/automation notes.

### First-time setup

```bash
# Inside WSL
asdf install          # installs the Rust version from .tool-versions
nats-server --jetstream &
cargo build
RUST_LOG=info cargo run
curl http://localhost:9292/health
```

See [docs/setup.md](docs/setup.md) for OS-level tuning (file descriptor limits,
TCP settings) required for high-connection-count work.

---

## Build and test commands

All commands below should be run inside WSL.

```bash
# Build (debug)
cargo build

# Build (release)
cargo build --release

# Run
RUST_LOG=info cargo run

# Unit and integration tests
cargo test

# Lint — warnings are errors
cargo clippy --all-targets --all-features -- -D warnings

# Format check
cargo fmt --all --check

# Auto-format
cargo fmt --all

# Documentation (warnings as errors)
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

# Security audit (run when Cargo.toml / Cargo.lock change)
cargo audit
```

---

## PR checklist

Before opening a pull request, verify all of the following locally:

- [ ] `cargo fmt --all --check` passes (or run `cargo fmt --all` and commit the result)
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes with no warnings
- [ ] `cargo test` passes (all unit and integration tests green)
- [ ] If public API or rustdoc changed: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` passes
- [ ] If `Cargo.toml` / `Cargo.lock` changed: `cargo audit` shows no new vulnerabilities
- [ ] `CHANGELOG.md` `[Unreleased]` section updated with a bullet for your change
- [ ] Relevant documentation in `docs/` updated if behaviour, config, or protocol changed

---

## Commit style

- Use the imperative mood: "Add X", "Fix Y", "Remove Z"
- Keep the subject line under 72 characters
- Reference issue numbers where applicable: `Fix fanout routing (#42)`
- One logical change per commit; squash fixup commits before merging

---

## Running k6 load tests

The load-test scripts live under `bench/scripts/`. You need:
- A tuned Linux host (or WSL with increased limits — see [docs/setup.md](docs/setup.md))
- NATS Server running with JetStream enabled
- A release build of the gateway and the `tc-publish` helper binary

```bash
# Tune OS limits (run once per session; requires sudo)
sudo bash bench/scripts/tune_os.sh

# Build release binaries
cargo build --release --bin turbocable-server
cargo build --release --bin tc-publish

# Smoke test (~1k connections)
TARGET=1000 GATEWAY_WSS_URL=ws://localhost:9292/cable bash bench/scripts/run_single_node.sh

# 333k single-node target
TARGET=333000 GATEWAY_WSS_URL=ws://localhost:9292/cable bash bench/scripts/run_single_node.sh

# Cargo micro-benchmarks
cargo bench --bench registry_bench
```

Full guide including three-node cluster setup and Prometheus validation:
[docs/load-testing-1m.md](docs/load-testing-1m.md).

---

## Fuzz testing

Fuzz targets live in `fuzz/` and cover the three untrusted parsers:

| Target | What it tests |
|--------|---------------|
| `json_decode` | JSON → `ClientFrame` parser |
| `msgpack_decode` | MessagePack → `ClientFrame` parser |
| `jwt_decode` | `JwtVerifier::verify` on arbitrary token strings |

### One-time setup

`cargo-fuzz` requires a nightly Rust toolchain and is **not** installed by default.
Run the following **once** inside WSL:

```bash
# Install the nightly toolchain
rustup toolchain install nightly

# Install cargo-fuzz (uses stable cargo to install, but runs under nightly)
cargo install cargo-fuzz
```

`fuzz/rust-toolchain.toml` pins the fuzz sub-crate to nightly, so `cargo fuzz`
picks up the right compiler automatically.

### Smoke run (60 s per target)

Run from the **repo root** inside WSL:

```bash
# JSON codec
cargo +nightly fuzz run json_decode -- -max_total_time=60

# MessagePack codec (uses a learned dictionary for better coverage)
cargo +nightly fuzz run msgpack_decode -- -dict=fuzz/dict/msgpack_decode.dict -max_total_time=60

# JWT parser (uses a learned dictionary for better coverage)
cargo +nightly fuzz run jwt_decode -- -dict=fuzz/dict/jwt_decode.dict -max_total_time=60
```

Seed corpora in `fuzz/corpus/<target>/` are passed automatically.
Dictionaries in `fuzz/dict/` contain tokens libFuzzer learned from the protocol
and JWT structures; passing them speeds up coverage on long runs.

### Longer / nightly runs

Omit `-max_total_time` to run indefinitely (stop with Ctrl-C):

```bash
cargo +nightly fuzz run json_decode
```

Crashes are saved to `fuzz/artifacts/<target>/` and can be reproduced with:

```bash
cargo +nightly fuzz run json_decode fuzz/artifacts/json_decode/<crash-file>
```

### Note on CI

`cargo fuzz` is nightly-only and CPU-intensive; it is **not** part of the
standard CI pipeline.  Short smoke runs (60 s) are expected to be run manually
by the committer before opening a PR that touches the protocol codecs or JWT
verifier.

---

## Prometheus alert validation

When modifying `infra/prometheus/alerts.yml`:

```bash
promtool check rules infra/prometheus/alerts.yml
```

`promtool` ships with the [Prometheus release bundle](https://github.com/prometheus/prometheus/releases).

---

## CI pipeline

Every push and pull request to `main` runs:

| Job | Command |
|-----|---------|
| Formatting | `cargo fmt --all --check` |
| Clippy | `cargo clippy --all-targets --all-features -- -D warnings` |
| Tests | `cargo test --all-features` |
| Documentation | `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` |
| Security audit | `cargo audit` |
| MSRV check | `cargo check` at `rust-version` in `Cargo.toml` |
| Release | Cross-compile + Docker multi-platform push to `ghcr.io/<repository-owner>/turbocable-server` |

---

## Branching model

- One feature branch per phase or logical unit of work, e.g. `phase-2-integration-tests`
- Open a PR against `main`; squash-merge after review
- Tag releases on `main` with `vMAJOR.MINOR.PATCH`

---

## Questions

Open an issue at <https://github.com/samaswin/turbocable-server/issues> or start
a discussion in the GitHub Discussions tab.
