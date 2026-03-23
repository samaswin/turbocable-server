# Builds a fully-static x86_64 Linux binary via musl and packages it into a
# minimal scratch image (~12 MB).  For multi-platform releases the CI pipeline
# uses Dockerfile.dist with pre-built cross-compiled binaries instead.

FROM rust:1.88-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends musl-tools && \
    rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /build

# ── Dependency cache layer ────────────────────────────────────────────────────
# Copy manifests only and build a dummy binary so that dependency compilation
# is cached as a separate layer.  The real source is layered on top.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin && \
    printf 'fn main(){}' > src/main.rs && \
    printf 'fn main(){}' > src/bin/publish.rs && \
    cargo build --release --target x86_64-unknown-linux-musl && \
    rm -rf src \
           target/x86_64-unknown-linux-musl/release/turbocable-server \
           target/x86_64-unknown-linux-musl/release/tc-publish \
           target/x86_64-unknown-linux-musl/release/deps/turbocable_server* \
           target/x86_64-unknown-linux-musl/release/deps/tc_publish*

# ── Real build ────────────────────────────────────────────────────────────────
COPY . .
RUN cargo build --release --target x86_64-unknown-linux-musl

# Verify static linking before packaging
RUN ldd target/x86_64-unknown-linux-musl/release/turbocable-server 2>&1 | \
    grep -q "not a dynamic executable" || \
    (echo "ERROR: binary is not statically linked" && exit 1)

# ── Runtime image ─────────────────────────────────────────────────────────────
FROM scratch

COPY --from=builder \
    /build/target/x86_64-unknown-linux-musl/release/turbocable-server \
    /gateway

EXPOSE 9292
ENTRYPOINT ["/gateway"]
