use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;

use crate::auth::jwt::{self, JwtVerifier};
use crate::connection::limiter::ConnectionLimiter;
use crate::connection::registry::Registry;
use crate::protocol::types::{ClientCommand, ServerMessage};
use crate::protocol::{self, Codec, SUB_PROTOCOL_JSON, SUB_PROTOCOL_MSGPACK};

const CHANNEL_CAPACITY: usize = 16;

const WS_CLOSE_AUTH_FAILED: u16 = 3000;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub limiter: Arc<ConnectionLimiter>,
    pub ping_interval_secs: u64,
    pub jwt_verifier: Option<Arc<JwtVerifier>>,
}

#[derive(Debug, serde::Deserialize)]
pub struct WsQueryParams {
    #[serde(default)]
    pub token: Option<String>,
}

pub async fn ws_upgrade(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    Query(params): Query<WsQueryParams>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let requested = headers
        .get(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok());

    let (protocol, ws) = match requested {
        Some(val) => {
            let chosen = negotiate_protocol(val);
            (chosen, ws.protocols([chosen]))
        }
        None => (SUB_PROTOCOL_JSON, ws),
    };

    ws.on_upgrade(move |socket| handle_socket(socket, protocol, params.token, addr, state))
}

fn negotiate_protocol(header: &str) -> &'static str {
    for proto in header.split(',').map(str::trim) {
        if proto == SUB_PROTOCOL_MSGPACK {
            return SUB_PROTOCOL_MSGPACK;
        }
        if proto == SUB_PROTOCOL_JSON {
            return SUB_PROTOCOL_JSON;
        }
    }
    SUB_PROTOCOL_JSON
}

async fn handle_socket(
    mut socket: WebSocket,
    protocol: &'static str,
    token: Option<String>,
    addr: SocketAddr,
    state: AppState,
) {
    let ip = addr.ip();

    if !state.limiter.try_acquire(ip) {
        tracing::warn!(%ip, "connection rejected: per-IP limit exceeded");
        let _ = socket
            .send(Message::Close(Some(CloseFrame {
                code: 1008,
                reason: "too many connections from this IP".into(),
            })))
            .await;
        return;
    }

    let allowed_streams = if let Some(ref verifier) = state.jwt_verifier {
        let token_str = match token.as_deref() {
            Some(t) if !t.is_empty() => t,
            _ => {
                tracing::warn!(%ip, "connection rejected: no token provided");
                let _ = socket
                    .send(Message::Close(Some(CloseFrame {
                        code: WS_CLOSE_AUTH_FAILED,
                        reason: "auth failed".into(),
                    })))
                    .await;
                state.limiter.release(ip);
                return;
            }
        };

        match verifier.verify(token_str) {
            Ok(claims) => {
                tracing::debug!(%ip, sub = %claims.sub, "JWT verified");
                claims.allowed_streams
            }
            Err(e) => {
                tracing::warn!(%ip, error = %e, "connection rejected: JWT verification failed");
                let reason = e.to_string().replace("auth failed: ", "");
                let _ = socket
                    .send(Message::Close(Some(CloseFrame {
                        code: WS_CLOSE_AUTH_FAILED,
                        reason: reason.into(),
                    })))
                    .await;
                state.limiter.release(ip);
                return;
            }
        }
    } else {
        vec![String::from("*")]
    };

    let codec = protocol::codec_for_protocol(protocol).expect("negotiated protocol must be valid");

    let conn_id = state.registry.allocate_id();
    let (tx, rx) = mpsc::channel::<Bytes>(CHANNEL_CAPACITY);
    state.registry.register(conn_id, tx.clone());

    tracing::info!(conn_id, %ip, protocol, "connection accepted");

    if let Ok(welcome) = codec.encode(&ServerMessage::Welcome) {
        let _ = tx.send(welcome).await;
    }

    let is_binary = protocol == SUB_PROTOCOL_MSGPACK;
    let (ws_sender, ws_receiver) = socket.split();

    let outbound_handle = tokio::spawn(outbound_loop(rx, ws_sender, is_binary));

    inbound_loop(ws_receiver, &tx, &*codec, conn_id, &state, &allowed_streams).await;

    state.registry.deregister(conn_id);
    state.limiter.release(ip);
    drop(tx);
    let _ = outbound_handle.await;

    tracing::info!(conn_id, %ip, "connection closed");
}

async fn outbound_loop(
    mut rx: mpsc::Receiver<Bytes>,
    mut sender: SplitSink<WebSocket, Message>,
    is_binary: bool,
) {
    while let Some(payload) = rx.recv().await {
        let msg = if is_binary {
            Message::Binary(payload.to_vec())
        } else {
            match String::from_utf8(payload.to_vec()) {
                Ok(text) => Message::Text(text),
                Err(e) => {
                    tracing::warn!("non-UTF-8 payload on JSON connection: {e}");
                    continue;
                }
            }
        };

        if sender.send(msg).await.is_err() {
            break;
        }
    }

    let _ = sender.close().await;
}

async fn inbound_loop(
    mut receiver: SplitStream<WebSocket>,
    tx: &mpsc::Sender<Bytes>,
    codec: &dyn Codec,
    conn_id: u64,
    state: &AppState,
    allowed_streams: &[String],
) {
    let mut ping_interval = tokio::time::interval(Duration::from_secs(state.ping_interval_secs));
    ping_interval.tick().await;

    loop {
        tokio::select! {
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(ref text))) => {
                        handle_client_frame(text.as_bytes(), codec, conn_id, tx, state, allowed_streams).await;
                    }
                    Some(Ok(Message::Binary(ref data))) => {
                        handle_client_frame(data, codec, conn_id, tx, state, allowed_streams).await;
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        tracing::debug!(conn_id, error = %e, "ws receive error");
                        break;
                    }
                }
            }
            _ = ping_interval.tick() => {
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if let Ok(ping) = codec.encode(&ServerMessage::Ping { message: ts }) {
                    if tx.send(ping).await.is_err() {
                        break;
                    }
                }
            }
        }
    }
}

async fn handle_client_frame(
    data: &[u8],
    codec: &dyn Codec,
    conn_id: u64,
    tx: &mpsc::Sender<Bytes>,
    state: &AppState,
    allowed_streams: &[String],
) {
    match codec.decode(data) {
        Ok(cmd) => {
            tracing::debug!(conn_id, ?cmd, "received command");
            match cmd {
                ClientCommand::Subscribe { identifier } => {
                    if !jwt::is_allowed(allowed_streams, &identifier) {
                        tracing::warn!(
                            conn_id,
                            identifier,
                            "subscribe rejected: stream not allowed"
                        );
                        if let Ok(reject) =
                            codec.encode(&ServerMessage::RejectSubscription { identifier })
                        {
                            let _ = tx.send(reject).await;
                        }
                        return;
                    }

                    state.registry.subscribe(conn_id, &identifier);
                    if let Ok(confirm) =
                        codec.encode(&ServerMessage::ConfirmSubscription { identifier })
                    {
                        let _ = tx.send(confirm).await;
                    }
                }
                ClientCommand::Unsubscribe { identifier } => {
                    state.registry.unsubscribe(conn_id, &identifier);
                }
                ClientCommand::Message {
                    identifier,
                    ref data,
                } => {
                    tracing::debug!(
                        conn_id,
                        identifier,
                        data,
                        "client message (NATS not yet wired)"
                    );
                }
            }
        }
        Err(e) => {
            tracing::warn!(conn_id, error = %e, "failed to decode client frame");
        }
    }
}
