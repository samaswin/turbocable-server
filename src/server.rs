//! Axum HTTP server setup, routing, and TCP listener configuration.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing::get, Json, Router};
use tokio::sync::watch;

use crate::auth::jwt::JwtVerifier;
use crate::auth::key_watcher;
use crate::config::Config;
use crate::connection::handler::{ws_upgrade, AppState};
use crate::connection::limiter::ConnectionLimiter;
use crate::connection::registry::Registry;
use crate::metrics::Metrics;
use crate::presence::PresenceManager;
use crate::pubsub::nats::NatsConsumer;

/// Maximum time in seconds to wait for WebSocket connections to drain after SIGTERM.
const DRAIN_TIMEOUT_SECS: u64 = 30;

/// How often to poll the connection count while draining.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Starts the gateway: builds shared state, binds the listener, and serves requests.
///
/// On SIGTERM (or Ctrl-C in dev), the server:
/// 1. Stops accepting new connections.
/// 2. Signals all active WebSocket connections to close with code 1001.
/// 3. Waits up to `DRAIN_TIMEOUT_SECS` for connections to close gracefully.
/// 4. Flushes any pending NATS acks.
pub async fn run(cfg: Config) {
    let metrics = Metrics::new();
    let registry = Arc::new(Registry::new());
    let limiter = Arc::new(ConnectionLimiter::new(cfg.max_connections_per_ip));

    let jwt_verifier = init_jwt_verifier(&cfg).await;
    let nats_consumer = init_nats_consumer(&cfg, &registry, Arc::clone(&metrics)).await;
    let presence = init_presence(&cfg).await;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let state = AppState {
        registry: registry.clone(),
        limiter,
        ping_interval_secs: cfg.ping_interval_secs,
        jwt_verifier,
        nats_consumer: nats_consumer.clone(),
        presence,
        metrics,
        shutdown_rx,
        ws_channel_capacity: cfg.ws_channel_capacity,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics_handler))
        .route("/pubkey", get(pubkey_handler))
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
    .with_graceful_shutdown(shutdown_signal())
    .await
    .unwrap_or_else(|e| {
        tracing::error!("server error: {e}");
        std::process::exit(1);
    });

    // Axum has stopped accepting new connections. Signal all active WebSocket
    // connections to send a close(1001) frame and drain.
    let active = registry.connection_count();
    tracing::info!(active, "draining connections (up to {DRAIN_TIMEOUT_SECS}s)");
    let _ = shutdown_tx.send(true);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(DRAIN_TIMEOUT_SECS);
    loop {
        let remaining = registry.connection_count();
        if remaining == 0 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(
                remaining,
                "drain timeout: force-closing remaining connections"
            );
            break;
        }
        tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
    }

    // Flush any pending NATS publishes and acks before exiting.
    if let Some(ref consumer) = nats_consumer {
        if let Err(e) = consumer.flush().await {
            tracing::warn!(error = %e, "NATS flush failed during shutdown");
        } else {
            tracing::info!("NATS flushed");
        }
    }

    tracing::info!("shutdown complete");
}

/// Returns a future that resolves when SIGTERM or Ctrl-C is received.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => { tracing::info!("SIGTERM received"); }
            _ = tokio::signal::ctrl_c() => { tracing::info!("Ctrl-C received"); }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Ctrl-C received");
    }
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

/// Returns a JSON health-check response with the current connection count and NATS status.
async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let nats_connected = state
        .nats_consumer
        .as_ref()
        .map(|c| c.is_connected())
        .unwrap_or(false);
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "connections": state.registry.connection_count(),
        "nats_connected": nats_connected,
    }))
}

/// Returns all gateway metrics in Prometheus text exposition format.
async fn metrics_handler() -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        Metrics::render(),
    )
}

/// Returns the current RS256 public key PEM used for JWT verification.
///
/// Used by Rails to confirm the gateway is using the expected key during debugging.
/// Returns 404 if JWT auth is disabled.
async fn pubkey_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.jwt_verifier {
        Some(ref verifier) => {
            let pem = verifier.current_pem();
            match String::from_utf8(pem) {
                Ok(pem_str) => (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/x-pem-file")],
                    pem_str,
                )
                    .into_response(),
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Connects to NATS, ensures the JetStream stream, and starts the background
/// fan-out consumer loop. Returns `None` if NATS is unavailable (gateway runs
/// without pub/sub — useful for local development and testing).
async fn init_nats_consumer(
    cfg: &Config,
    registry: &Arc<Registry>,
    metrics: Arc<Metrics>,
) -> Option<Arc<NatsConsumer>> {
    match NatsConsumer::connect(&cfg.nats_url, cfg.nats_stream_replicas).await {
        Ok(consumer) => {
            let consumer = Arc::new(consumer);
            consumer.start_fanout_loop(
                cfg.node_id.clone(),
                Arc::clone(registry),
                cfg.max_ack_pending,
                metrics,
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

/// Connects to NATS and opens the `TC_PRESENCE` KV bucket for presence tracking.
///
/// Returns `None` if NATS is unavailable — the gateway runs without presence in that case.
async fn init_presence(cfg: &Config) -> Option<Arc<PresenceManager>> {
    match PresenceManager::connect(&cfg.nats_url).await {
        Ok(presence) => {
            tracing::info!(nats_url = %cfg.nats_url, "presence KV manager active (TC_PRESENCE)");
            Some(presence)
        }
        Err(e) => {
            tracing::warn!(
                nats_url = %cfg.nats_url,
                error = %e,
                "presence KV unavailable — running without presence tracking"
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
