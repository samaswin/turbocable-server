use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::{routing::get, Json, Router};

use crate::config::Config;
use crate::connection::handler::{ws_upgrade, AppState};
use crate::connection::limiter::ConnectionLimiter;
use crate::connection::registry::Registry;

pub async fn run(cfg: Config) {
    let registry = Arc::new(Registry::new());
    let limiter = Arc::new(ConnectionLimiter::new(cfg.max_connections_per_ip));

    let state = AppState {
        registry: registry.clone(),
        limiter,
        ping_interval_secs: cfg.ping_interval_secs,
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

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "connections": state.registry.connection_count(),
    }))
}
