//! turbocable-server — high-performance WebSocket gateway for TurboCable.
//!
//! Entry point: raises file-descriptor limits, initialises logging, and starts
//! the Axum/NATS gateway server.

#[cfg(all(target_os = "linux", not(target_env = "musl")))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use clap::Parser;
use turbocable_server::{config::Config, server};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .json()
        .init();

    let cfg = Config::parse();
    raise_fd_limit(2_000_000);

    tracing::info!(port = cfg.port, node_id = %cfg.node_id, "starting");
    server::run(cfg).await;
}

/// Attempts to raise the file descriptor limit to `target` for high connection counts.
fn raise_fd_limit(target: u64) {
    if let Ok((soft, hard)) = rlimit::Resource::NOFILE.get() {
        if soft < target {
            let new = target.min(hard);
            rlimit::Resource::NOFILE.set(new, hard).ok();
            tracing::info!("fd limit raised to {new}");
        }
    }
}
