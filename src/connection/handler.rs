//! WebSocket upgrade handler and per-connection inbound/outbound loops.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};

use crate::auth::jwt::{self, JwtVerifier};
use crate::connection::limiter::ConnectionLimiter;
use crate::connection::registry::Registry;
use crate::metrics::Metrics;
use crate::presence::{HeartbeatHandle, PresenceManager};
use crate::protocol::types::{ClientCommand, ServerMessage};
use crate::protocol::{self, Codec, SUB_PROTOCOL_JSON, SUB_PROTOCOL_MSGPACK};
use crate::pubsub::nats::NatsConsumer;

/// WebSocket close code sent when JWT authentication fails.
const WS_CLOSE_AUTH_FAILED: u16 = 3000;

/// WebSocket close code sent when the server is shutting down (RFC 6455 §7.4.1).
const WS_CLOSE_GOING_AWAY: u16 = 1001;

/// Shared application state passed to every Axum handler.
#[derive(Clone)]
pub struct AppState {
    /// Global connection registry for stream subscriptions and fan-out.
    pub registry: Arc<Registry>,
    /// Per-IP connection rate limiter.
    pub limiter: Arc<ConnectionLimiter>,
    /// Seconds between WebSocket ping frames.
    pub ping_interval_secs: u64,
    /// JWT verifier (absent when auth is disabled).
    pub jwt_verifier: Option<Arc<JwtVerifier>>,
    /// NATS JetStream consumer for publishing and replay (absent when NATS is unavailable).
    pub nats_consumer: Option<Arc<NatsConsumer>>,
    /// Presence manager backed by NATS KV (absent when NATS is unavailable).
    pub presence: Option<Arc<PresenceManager>>,
    /// Prometheus metrics handle.
    pub metrics: Arc<Metrics>,
    /// Shutdown broadcast: resolves to `true` when the server is draining.
    pub shutdown_rx: watch::Receiver<bool>,
    /// Per-connection outbound mpsc channel capacity.
    pub ws_channel_capacity: usize,
}

/// Query parameters extracted from the WebSocket upgrade URL.
#[derive(Debug, serde::Deserialize)]
pub struct WsQueryParams {
    /// Optional JWT bearer token.
    #[serde(default)]
    pub token: Option<String>,
}

/// Client-sent hello message for reconnection with replay.
/// Distinct from [`ClientCommand`] — uses `"type"` tag instead of `"command"`.
#[derive(Debug, serde::Deserialize)]
struct HelloMessage {
    #[serde(rename = "type")]
    msg_type: String,
    /// Last JetStream stream sequence the client received before disconnecting.
    last_seq: Option<String>,
}

/// Per-connection context bundling the fields that are fixed for the lifetime of one connection.
struct ConnContext<'a> {
    conn_id: u64,
    codec: &'a dyn Codec,
    is_binary: bool,
    tx: &'a mpsc::Sender<Bytes>,
    allowed_streams: &'a [String],
    user_id: Option<&'a str>,
}

/// Axum handler that negotiates the WebSocket sub-protocol and upgrades the connection.
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
        state.metrics.connections_rejected.inc();
        let _ = socket
            .send(Message::Close(Some(CloseFrame {
                code: 1008,
                reason: "too many connections from this IP".into(),
            })))
            .await;
        return;
    }

    let (allowed_streams, user_id) = if let Some(ref verifier) = state.jwt_verifier {
        let token_str = match token.as_deref() {
            Some(t) if !t.is_empty() => t,
            _ => {
                tracing::warn!(%ip, "connection rejected: no token provided");
                state.metrics.connections_rejected.inc();
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

        let t0 = Instant::now();
        let result = verifier.verify(token_str);
        state
            .metrics
            .auth_duration_secs
            .observe(t0.elapsed().as_secs_f64());

        match result {
            Ok(claims) => {
                tracing::debug!(%ip, sub = %claims.sub, "JWT verified");
                let user_id = claims.sub.clone();
                (claims.allowed_streams, Some(user_id))
            }
            Err(e) => {
                tracing::warn!(%ip, error = %e, "connection rejected: JWT verification failed");
                state.metrics.connections_rejected.inc();
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
        (vec![String::from("*")], None)
    };

    let codec = protocol::codec_for_protocol(protocol).expect("negotiated protocol must be valid");

    let is_binary = protocol == SUB_PROTOCOL_MSGPACK;
    let conn_id = state.registry.allocate_id();
    let (tx, rx) = mpsc::channel::<Bytes>(state.ws_channel_capacity);
    state.registry.register(conn_id, tx.clone(), is_binary);

    state.metrics.connections_total.inc();
    state.metrics.connections_active.inc();

    tracing::info!(conn_id, %ip, protocol, "connection accepted");

    if let Ok(welcome) = codec.encode(&ServerMessage::Welcome) {
        let _ = tx.send(welcome).await;
    }

    let (ws_sender, ws_receiver) = socket.split();

    let outbound_handle = tokio::spawn(outbound_loop(
        rx,
        ws_sender,
        is_binary,
        state.shutdown_rx.clone(),
    ));

    let ctx = ConnContext {
        conn_id,
        codec: &*codec,
        is_binary,
        tx: &tx,
        allowed_streams: &allowed_streams,
        user_id: user_id.as_deref(),
    };
    inbound_loop(ws_receiver, ctx, &state, state.shutdown_rx.clone()).await;

    state.registry.deregister(conn_id);
    state.metrics.connections_active.dec();
    state.limiter.release(ip);
    drop(tx);
    let _ = outbound_handle.await;

    tracing::info!(conn_id, %ip, "connection closed");
}

async fn outbound_loop(
    mut rx: mpsc::Receiver<Bytes>,
    mut sender: SplitSink<WebSocket, Message>,
    is_binary: bool,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            payload = rx.recv() => {
                match payload {
                    Some(payload) => {
                        if let Some(msg) = bytes_to_ws_msg(payload, is_binary) {
                            if sender.send(msg).await.is_err() {
                                break;
                            }
                        }
                    }
                    None => break,
                }
            }
            _ = shutdown_rx.changed() => {
                // Drain any queued outbound messages before closing.
                while let Ok(payload) = rx.try_recv() {
                    if let Some(msg) = bytes_to_ws_msg(payload, is_binary) {
                        if sender.send(msg).await.is_err() {
                            break;
                        }
                    }
                }
                let _ = sender
                    .send(Message::Close(Some(CloseFrame {
                        code: WS_CLOSE_GOING_AWAY,
                        reason: "server shutting down".into(),
                    })))
                    .await;
                return;
            }
        }
    }

    let _ = sender.close().await;
}

/// Converts a raw [`Bytes`] payload to a WebSocket [`Message`].
///
/// Returns `None` (and logs a warning) when a non-UTF-8 payload arrives on a
/// JSON connection — the frame is silently dropped rather than closing the socket.
fn bytes_to_ws_msg(payload: Bytes, is_binary: bool) -> Option<Message> {
    if is_binary {
        Some(Message::Binary(payload.into()))
    } else {
        match std::str::from_utf8(&payload) {
            Ok(s) => Some(Message::Text(s.to_owned())),
            Err(e) => {
                tracing::warn!("non-UTF-8 payload on JSON connection: {e}");
                None
            }
        }
    }
}

async fn inbound_loop(
    mut receiver: SplitStream<WebSocket>,
    ctx: ConnContext<'_>,
    state: &AppState,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut ping_interval = tokio::time::interval(Duration::from_secs(state.ping_interval_secs));
    ping_interval.tick().await;

    // Stores the last_seq from a client "hello" message for reconnection replay.
    // Set once per connection, consumed during subsequent subscribe commands.
    let mut last_seq: Option<u64> = None;

    // Tracks active heartbeat tasks keyed by stream name.
    // Dropping a HeartbeatHandle automatically aborts the background task.
    let mut heartbeats: std::collections::HashMap<String, HeartbeatHandle> =
        std::collections::HashMap::new();

    loop {
        tokio::select! {
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(ref text))) => {
                        handle_client_frame(
                            text.as_bytes(), &ctx, state, &mut last_seq, &mut heartbeats,
                        ).await;
                    }
                    Some(Ok(Message::Binary(ref data))) => {
                        handle_client_frame(
                            data, &ctx, state, &mut last_seq, &mut heartbeats,
                        ).await;
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        tracing::debug!(ctx.conn_id, error = %e, "ws receive error");
                        break;
                    }
                }
            }
            _ = ping_interval.tick() => {
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if let Ok(ping) = ctx.codec.encode(&ServerMessage::Ping { message: ts }) {
                    if ctx.tx.send(ping).await.is_err() {
                        break;
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                tracing::debug!(ctx.conn_id, "shutdown signal received");
                break;
            }
        }
    }

    // Clean up presence for all remaining subscriptions on disconnect.
    if let (Some(uid), Some(presence)) = (ctx.user_id, state.presence.as_ref()) {
        for stream in heartbeats.keys() {
            presence.delete(stream, uid).await;
        }
    }
    // Dropping `heartbeats` aborts all heartbeat tasks.
}

async fn handle_client_frame(
    data: &[u8],
    ctx: &ConnContext<'_>,
    state: &AppState,
    last_seq: &mut Option<u64>,
    heartbeats: &mut std::collections::HashMap<String, HeartbeatHandle>,
) {
    match ctx.codec.decode(data) {
        Ok(cmd) => {
            tracing::debug!(ctx.conn_id, ?cmd, "received command");
            match cmd {
                ClientCommand::Subscribe { identifier } => {
                    if !jwt::is_allowed(ctx.allowed_streams, &identifier) {
                        tracing::warn!(
                            ctx.conn_id,
                            identifier,
                            "subscribe rejected: stream not allowed"
                        );
                        if let Ok(reject) = ctx
                            .codec
                            .encode(&ServerMessage::RejectSubscription { identifier })
                        {
                            let _ = ctx.tx.send(reject).await;
                        }
                        return;
                    }

                    let stream_key = stream_key_from_identifier(&identifier);
                    state.registry.subscribe(ctx.conn_id, stream_key);

                    // Write presence entry and start heartbeat (if presence is configured).
                    if let (Some(uid), Some(presence)) = (ctx.user_id, state.presence.as_ref()) {
                        presence.put(&identifier, uid).await;
                        let handle = presence.start_heartbeat(identifier.clone(), uid.to_string());
                        heartbeats.insert(identifier.clone(), handle);
                        tracing::debug!(
                            ctx.conn_id,
                            stream = identifier,
                            user_id = uid,
                            "presence written"
                        );
                    }

                    // Replay missed messages when client reconnected with last_seq.
                    // Happens before confirm so the client receives replayed messages first.
                    if let Some(seq) = *last_seq {
                        replay_for_stream(&identifier, seq, ctx.conn_id, ctx.codec, ctx.tx, state)
                            .await;
                    }

                    if let Ok(confirm) = ctx
                        .codec
                        .encode(&ServerMessage::ConfirmSubscription { identifier })
                    {
                        let _ = ctx.tx.send(confirm).await;
                    }
                }
                ClientCommand::Unsubscribe { identifier } => {
                    let stream_key = stream_key_from_identifier(&identifier);
                    state.registry.unsubscribe(ctx.conn_id, stream_key);

                    // Remove heartbeat (aborts task) and delete presence key.
                    if let (Some(uid), Some(presence)) = (ctx.user_id, state.presence.as_ref()) {
                        heartbeats.remove(&identifier); // Drop aborts heartbeat task.
                        presence.delete(&identifier, uid).await;
                        tracing::debug!(
                            ctx.conn_id,
                            stream = identifier,
                            user_id = uid,
                            "presence removed"
                        );
                    }
                }
                ClientCommand::Message {
                    identifier,
                    ref data,
                } => {
                    if let Some(ref nats) = state.nats_consumer {
                        let payload = Bytes::copy_from_slice(data.as_bytes());
                        if let Err(e) = nats.publish(&identifier, payload).await {
                            tracing::warn!(
                                ctx.conn_id,
                                identifier,
                                error = %e,
                                "failed to publish client message to NATS"
                            );
                        }
                    } else {
                        tracing::debug!(
                            ctx.conn_id,
                            identifier,
                            "client message dropped (NATS not available)"
                        );
                    }
                }
            }
        }
        Err(_) => {
            // Not a standard command — try parsing as a "hello" reconnection message.
            if try_parse_hello(data, ctx.is_binary, ctx.conn_id, last_seq) {
                return;
            }
            tracing::warn!(ctx.conn_id, "failed to decode client frame");
        }
    }
}

/// Extracts the stream name from an ActionCable identifier.
///
/// The identifier is a JSON-encoded string like:
///   `{"channel":"BenchmarkChannel","stream":"bench"}`
/// Returns the `"stream"` value when present, otherwise the raw identifier.
fn stream_key_from_identifier(identifier: &str) -> &str {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(identifier) {
        if let Some(s) = v.get("stream").and_then(|s| s.as_str()) {
            if let Some(start) = identifier.find(s) {
                return &identifier[start..start + s.len()];
            }
        }
    }
    identifier
}

/// Attempts to parse the frame as a `{"type":"hello","last_seq":"N"}` message.
/// Returns `true` if successfully parsed, `false` otherwise.
fn try_parse_hello(data: &[u8], is_binary: bool, conn_id: u64, last_seq: &mut Option<u64>) -> bool {
    let hello: Option<HelloMessage> = if is_binary {
        rmp_serde::from_slice(data).ok()
    } else {
        serde_json::from_slice(data).ok()
    };

    if let Some(hello) = hello {
        if hello.msg_type == "hello" {
            let seq = hello
                .last_seq
                .as_deref()
                .and_then(|s| s.parse::<u64>().ok());
            *last_seq = seq;
            tracing::info!(
                conn_id,
                last_seq = ?seq,
                "client hello received"
            );
            return true;
        }
    }
    false
}

/// Replays missed messages from JetStream for a specific stream after client reconnection.
async fn replay_for_stream(
    stream_name: &str,
    last_seq: u64,
    conn_id: u64,
    codec: &dyn Codec,
    tx: &mpsc::Sender<Bytes>,
    state: &AppState,
) {
    let nats = match state.nats_consumer {
        Some(ref n) => n,
        None => return,
    };

    match nats.replay_since(stream_name, last_seq).await {
        Ok(messages) => {
            tracing::info!(
                conn_id,
                stream = stream_name,
                count = messages.len(),
                "replaying missed messages"
            );

            for msg in messages {
                // Parse the raw NATS payload, falling back to null for unparseable data.
                let payload: serde_json::Value = serde_json::from_slice(&msg.payload)
                    .or_else(|_| rmp_serde::from_slice(&msg.payload))
                    .unwrap_or(serde_json::Value::Null);

                let server_msg = ServerMessage::Message {
                    identifier: stream_name.to_string(),
                    message: payload,
                    replayed: Some(true),
                    seq: Some(msg.sequence),
                };

                if let Ok(encoded) = codec.encode(&server_msg) {
                    if tx.send(encoded).await.is_err() {
                        break;
                    }
                }
            }
        }
        Err(e) => {
            tracing::error!(
                conn_id,
                stream = stream_name,
                error = %e,
                "message replay failed"
            );
        }
    }
}
