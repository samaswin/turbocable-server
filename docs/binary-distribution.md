# Binary Distribution

turbocable-server ships as fully static pre-built binaries for all major
platforms and as a multi-platform Docker image. No Rust toolchain or system
dependencies are required to run the gateway in production.

---

## Table of Contents

- [Pre-built Binaries](#pre-built-binaries)
- [Docker Image](#docker-image)
- [Cross-Compilation Targets](#cross-compilation-targets)
- [Release Pipeline](#release-pipeline)
- [Building Locally](#building-locally)
- [Allocator Notes](#allocator-notes)

---

## Pre-built Binaries

Every version tag (`v*.*.*`) publishes binaries to the
[GitHub Releases](https://github.com/samaswin/turbocable-server/releases) page:

| Binary | Platform | Linking |
|--------|----------|---------|
| `turbocable-server-x86_64-linux` | Linux x86_64 | fully static (musl) |
| `turbocable-server-aarch64-linux` | Linux ARM64 / AWS Graviton | fully static (musl) |
| `turbocable-server-aarch64-macos` | macOS Apple Silicon | dynamic (system libs) |
| `turbocable-server-x86_64-macos` | macOS Intel | dynamic (system libs) |

Linux binaries have no glibc or libc dependency — they run on Alpine, Debian,
Ubuntu, Amazon Linux, and any other Linux distribution.

### Verify a Linux binary is static

```bash
file turbocable-server-x86_64-linux
# turbocable-server-x86_64-linux: ELF 64-bit LSB executable, x86-64, statically linked

ldd turbocable-server-x86_64-linux
# not a dynamic executable
```

---

## Docker Image

Multi-platform images (linux/amd64 and linux/arm64) are published to the
GitHub Container Registry on every release:

```bash
# Pull for the current host platform (this repo → ghcr.io/samaswin/turbocable-server)
docker pull ghcr.io/samaswin/turbocable-server:latest

# Pull a specific version
docker pull ghcr.io/samaswin/turbocable-server:0.5.1

# Explicitly target ARM64 (e.g. from an x86_64 host)
docker pull --platform linux/arm64 ghcr.io/samaswin/turbocable-server:latest
```

Releases push to `ghcr.io/<repository-owner>/turbocable-server` (your GitHub user or org that owns the repository). Forks publish under the fork owner’s namespace.

### Run

```bash
docker run -p 9292:9292 \
  -e TURBOCABLE_NATS_URL=nats://host.docker.internal:4222 \
  ghcr.io/samaswin/turbocable-server:latest
```

Pass any configuration via environment variables — see the
[Configuration and HTTP API](configuration.md) for all options.

### Image details

- Base: `FROM scratch` — no shell, no libc, no OS layer
- Binary: `x86_64-unknown-linux-musl` or `aarch64-unknown-linux-musl` (fully static)
- Size: approximately 12 MB (amd64), 11 MB (arm64)
- Exposed port: `9292`

---

## Cross-Compilation Targets

| Rust Target | Platform | Linker strategy |
|-------------|----------|-----------------|
| `x86_64-unknown-linux-musl` | Linux x86_64 | bundled musl-libc, `cross` |
| `aarch64-unknown-linux-musl` | Linux ARM64 | bundled musl-libc, `cross` |
| `aarch64-apple-darwin` | macOS Apple Silicon | native macOS runner |
| `x86_64-apple-darwin` | macOS Intel | native macOS runner |

Linux musl targets are cross-compiled using
[`cross`](https://github.com/cross-rs/cross), which runs the build inside an
architecture-matched Docker container with the correct musl toolchain
pre-installed.

---

## Release Pipeline

The release workflow (`.github/workflows/release.yml`) is triggered by any
`v*.*.*` tag push or a manual `workflow_dispatch`.

### Jobs

```
build-linux          build-macos
  ├── x86_64-musl      ├── x86_64-apple-darwin
  └── aarch64-musl     └── aarch64-apple-darwin
       │                     │
       ▼                     ▼
     docker              release
  (multi-platform     (GitHub Release
   ghcr.io push)       with all 4 binaries)
```

#### `build-linux`

- Runs on `ubuntu-latest`
- Uses `cross` (via `taiki-e/install-action`) for both musl targets
- Verifies each binary is statically linked using `file`
- Uploads binaries as GitHub Actions artifacts

#### `build-macos`

- Runs on `macos-latest`
- Uses a native Rust toolchain via `rustup target add`
- Builds both `aarch64-apple-darwin` and `x86_64-apple-darwin` on the same runner

#### `docker`

- Depends on `build-linux`
- Downloads the pre-built amd64 and arm64 musl binaries
- Builds a multi-platform Docker image using `docker buildx` + `Dockerfile.dist`
- Pushes to `ghcr.io/<repository-owner>/turbocable-server` with `latest`, `MAJOR.MINOR`, and full
  version tags
- Verifies the published image is under 20 MB

#### `release`

- Depends on all four platform builds
- Assembles all binaries into a single GitHub Release
- Attaches auto-generated release notes

---

## Building Locally

### x86_64 Docker image (from source)

```bash
docker build -t turbocable-server .
```

This runs the full musl build inside Docker — no local Rust installation
required. The `Dockerfile` uses a dependency-caching layer so rebuilds after
code changes are fast.

### Native binary (requires Rust 1.88+)

```bash
# Debug build (faster compile, no optimisations)
cargo build

# Release build (LTO, codegen-units=1, symbols stripped)
cargo build --release
```

### Cross-compiled musl binary (requires `cross`)

```bash
# Install cross
cargo install cross

# x86_64 Linux (static)
cross build --release --target x86_64-unknown-linux-musl

# ARM64 Linux (static)
cross build --release --target aarch64-unknown-linux-musl
```

---

## Allocator Notes

| Target | Allocator | Reason |
|--------|-----------|--------|
| `*-linux-gnu` (glibc) | jemalloc | 15–20% higher throughput under sustained load |
| `*-linux-musl` | system (musl libc) | musl's allocator is solid; avoids C cross-compilation complexity |
| `*-apple-darwin` | system (libmalloc) | macOS system allocator is well-tuned; jemalloc gains are negligible |

The `#[global_allocator]` attribute in `main.rs` is gated by
`cfg(all(target_os = "linux", not(target_env = "musl")))`, so the correct
allocator is selected at compile time for each target.
