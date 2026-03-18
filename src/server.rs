//! Axum HTTP server setup, routing, and TCP listener configuration.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::{routing::get, Json, Router};

use crate::auth::jwt::JwtVerifier;
use crate::auth::key_watcher;
use crate::config::Config;
use crate::connection::handler::{ws_upgrade, AppState};
use crate::connection::limiter::ConnectionLimiter;
use crate::connection::registry::Registry;
use crate::pubsub::nats::NatsConsumer;

/// Starts the gateway: builds shared state, binds the listener, and serves requests.
pub async fn run(cfg: Config) {
    let registry = Arc::new(Registry::new());
    let limiter = Arc::new(ConnectionLimiter::new(cfg.max_connections_per_ip));

    let jwt_verifier = init_jwt_verifier(&cfg).await;
    let nats_consumer = init_nats_consumer(&cfg, &registry).await;

    let state = AppState {
        registry: registry.clone(),
        limiter,
        ping_interval_secs: cfg.ping_interval_secs,
        jwt_verifier,
        nats_consumer,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/cable", get(ws_upgrade))
        .with_state(state);

    let listener = create_listener(cfg.port).unwrap_or_else(|e| {
        tracing::error!("failed to create listener: {e}");
        std::process::exit(1);
    });

    let local_addr = listener
        .local_addr()
        .expect("listener must have a local address");
    tracing::info!("listening on {local_addr} (SO_REUSEPORT enabled)");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap_or_else(|e| {
        tracing::error!("server error: {e}");
        std::process::exit(1);
    });
}

/// Creates a TCP listener with `SO_REUSEPORT` and `SO_REUSEADDR` via socket2,
/// enabling multiple server processes to bind to the same port for load balancing.
fn create_listener(port: u16) -> std::io::Result<tokio::net::TcpListener> {
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;

    socket.set_reuse_address(true)?;

    #[cfg(unix)]
    socket.set_reuse_port(true)?;

    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(8192)?;

    tokio::net::TcpListener::from_std(socket.into())
}

/// Returns a JSON health-check response with the current connection count.
async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "connections": state.registry.connection_count(),
    }))
}

/// Connects to NATS, ensures the JetStream stream, and starts the background
/// fan-out consumer loop. Returns `None` if NATS is unavailable (gateway runs
/// without pub/sub — useful for local development and testing).
async fn init_nats_consumer(cfg: &Config, registry: &Arc<Registry>) -> Option<Arc<NatsConsumer>> {
    match NatsConsumer::connect(&cfg.nats_url, cfg.nats_stream_replicas).await {
        Ok(consumer) => {
            let consumer = Arc::new(consumer);
            consumer.start_fanout_loop(
                cfg.node_id.clone(),
                Arc::clone(registry),
                cfg.max_ack_pending,
            );
            tracing::info!(
                nats_url = %cfg.nats_url,
                node_id = %cfg.node_id,
                max_ack_pending = cfg.max_ack_pending,
                "NATS JetStream consumer active"
            );
            Some(consumer)
        }
        Err(e) => {
            tracing::warn!(
                nats_url = %cfg.nats_url,
                error = %e,
                "NATS JetStream unavailable — running without pub/sub"
            );
            None
        }
    }
}

/// Initialises the JWT verifier from a local PEM file or NATS KV.
///
/// Priority:
///   1. `TURBOCABLE_JWT_PUBLIC_KEY_PATH` env/arg → load from file
///   2. Otherwise → attempt NATS KV bucket `TC_PUBKEYS`
///   3. If neither is available → run without auth (warning logged)
async fn init_jwt_verifier(cfg: &Config) -> Option<Arc<JwtVerifier>> {
    if let Some(ref path) = cfg.jwt_public_key_path {
        let pem = match tokio::fs::read(path).await {
            Ok(data) => data,
            Err(e) => {
                tracing::error!(path, error = %e, "failed to read JWT public key file");
                std::process::exit(1);
            }
        };

        let verifier = match JwtVerifier::from_rsa_pem(&pem) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(path, error = %e, "invalid JWT public key");
                std::process::exit(1);
            }
        };

        let verifier = Arc::new(verifier);
        tracing::info!(path, "JWT auth enabled (key loaded from file)");

        if let Err(e) = key_watcher::start_nats_key_watcher(&cfg.nats_url, verifier.clone()).await {
            tracing::info!(
                error = %e,
                "NATS KV key watcher not available (file key will be used)"
            );
        }

        return Some(verifier);
    }

    match key_watcher::load_and_watch_from_nats(&cfg.nats_url).await {
        Ok(verifier) => {
            tracing::info!("JWT auth enabled (key loaded from NATS KV, watcher active)");
            Some(verifier)
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "JWT auth DISABLED — set TURBOCABLE_JWT_PUBLIC_KEY_PATH or configure NATS KV"
            );
            None
        }
    }
}
