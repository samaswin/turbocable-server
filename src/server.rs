use axum::{routing::get, Json, Router};

use crate::config::Config;

pub async fn run(cfg: Config) {
    let app = Router::new().route("/health", get(health));

    let addr = format!("0.0.0.0:{}", cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("failed to bind to {addr}: {e}");
            std::process::exit(1);
        });
    tracing::info!("listening on {addr}");
    axum::serve(listener, app).await.unwrap_or_else(|e| {
        tracing::error!("server error: {e}");
        std::process::exit(1);
    });
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}
