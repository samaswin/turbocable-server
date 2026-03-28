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
use crate::config::ReplayEnforcement;
use crate::connection::limiter::ConnectionLimiter;
use crate::connection::registry::Registry;
use crate::errors::ReplayError;
use crate::metrics::Metrics;
use crate::presence::{HeartbeatHandle, PresenceManager};
use crate::protocol::types::{ClientCommand, ClientFrame, ServerMessage};
use crate::protocol::{self, Codec, SUB_PROTOCOL_JSON, SUB_PROTOCOL_MSGPACK};
use crate::pubsub::nats::NatsConsumer;

/// WebSocket close code sent when JWT authentication fails.
const WS_CLOSE_AUTH_FAILED: u16 = 3000;

/// WebSocket close code sent when the server is shutting down (RFC 6455 §7.4.1).
const WS_CLOSE_GOING_AWAY: u16 = 1001;

/// Max replay frames per `outbound_loop` iteration before re-entering `select!` so eviction,
/// urgent close, and shutdown stay responsive under huge catch-up backlogs.
const OUTBOUND_REPLAY_BURST: usize = 512;

/// Per-connection handshake state machine.
///
/// Every connection starts in [`ConnectionState::AwaitingHello`].  The gateway
/// transitions to [`ConnectionState::Active`] on receipt of a valid hello frame.
/// In `Compat` enforcement mode a subscribe-before-hello also transitions to
/// `Active` (with a warning metric); in stricter modes the command is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    /// Waiting for the client's hello frame.  Only hello frames are fully
    /// processed in this state; other commands are handled per enforcement mode.
    AwaitingHello,
    /// Handshake complete — all frame types are processed normally.
    Active,
}

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
    /// Per-connection live fan-out mpsc channel capacity.
    pub ws_channel_capacity: usize,
    /// Per-connection replay catch-up mpsc channel capacity.
    pub ws_replay_channel_capacity: usize,
    /// Replay enforcement phase — hello ordering and `replay_v1` requirement.
    pub replay_enforcement: ReplayEnforcement,
    /// Semaphore capping concurrent background replay tasks on this node.
    /// Excess tasks queue (not dropped) until a slot is free.
    pub replay_semaphore: Arc<tokio::sync::Semaphore>,
}

/// Query parameters extracted from the WebSocket upgrade URL.
#[derive(Debug, serde::Deserialize)]
pub struct WsQueryParams {
    /// Optional JWT bearer token.
    #[serde(default)]
    pub token: Option<String>,
}

/// Per-connection context bundling the fields that are fixed for the lifetime of one connection.
struct ConnContext<'a> {
    conn_id: u64,
    /// Arc so the codec can be cheaply cloned into background replay tasks.
    codec: Arc<dyn Codec>,
    /// Live NATS fan-out and normal server frames (welcome, ping, confirms without replay).
    live_tx: &'a mpsc::Sender<Bytes>,
    /// JetStream replay catch-up; drained before live in `outbound_loop` for ordering.
    replay_tx: &'a mpsc::Sender<Bytes>,
    /// Pre-encoded disconnect + WebSocket close (retention gap, replay cap).
    urgent_close_tx: &'a mpsc::Sender<Bytes>,
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

    let codec: Arc<dyn Codec> = Arc::from(
        protocol::codec_for_protocol(protocol).expect("negotiated protocol must be valid"),
    );

    let is_binary = protocol == SUB_PROTOCOL_MSGPACK;
    let conn_id = state.registry.allocate_id();
    let (live_tx, live_rx) = mpsc::channel::<Bytes>(state.ws_channel_capacity);
    let (replay_tx, replay_rx) = mpsc::channel::<Bytes>(state.ws_replay_channel_capacity);
    let (urgent_close_tx, urgent_close_rx) = mpsc::channel::<Bytes>(1);
    let (evict_tx, evict_rx) = mpsc::channel::<()>(1);
    state
        .registry
        .register(conn_id, live_tx.clone(), is_binary, evict_tx);

    // Pre-encode the backpressure disconnect frame once so the outbound loop
    // can send it without access to the codec.
    let disconnect_frame = codec
        .encode(&ServerMessage::Disconnect {
            reason: "backpressure_reconnect_required".to_string(),
            reconnect: Some(true),
        })
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "failed to encode backpressure disconnect frame");
            Bytes::new()
        });

    state.metrics.connections_total.inc();
    state.metrics.connections_active.inc();

    tracing::info!(conn_id, %ip, protocol, "connection accepted");

    if let Ok(welcome) = codec.encode(&ServerMessage::Welcome) {
        let _ = live_tx.send(welcome).await;
    }

    let (ws_sender, ws_receiver) = socket.split();

    let outbound_handle = tokio::spawn(outbound_loop(
        live_rx,
        replay_rx,
        urgent_close_rx,
        ws_sender,
        is_binary,
        state.shutdown_rx.clone(),
        evict_rx,
        disconnect_frame,
    ));

    let ctx = ConnContext {
        conn_id,
        codec: Arc::clone(&codec),
        live_tx: &live_tx,
        replay_tx: &replay_tx,
        urgent_close_tx: &urgent_close_tx,
        allowed_streams: &allowed_streams,
        user_id: user_id.as_deref(),
    };
    inbound_loop(ws_receiver, ctx, &state, state.shutdown_rx.clone()).await;

    state.registry.deregister(conn_id);
    state.metrics.connections_active.dec();
    state.limiter.release(ip);
    drop(live_tx);
    drop(replay_tx);
    drop(urgent_close_tx);
    let _ = outbound_handle.await;

    tracing::info!(conn_id, %ip, "connection closed");
}

async fn outbound_loop(
    mut live_rx: mpsc::Receiver<Bytes>,
    mut replay_rx: mpsc::Receiver<Bytes>,
    mut urgent_close_rx: mpsc::Receiver<Bytes>,
    mut sender: SplitSink<WebSocket, Message>,
    is_binary: bool,
    mut shutdown_rx: watch::Receiver<bool>,
    mut evict_rx: mpsc::Receiver<()>,
    disconnect_frame: Bytes,
) {
    let mut urgent_open = true;

    'ws: loop {
        tokio::select! {
            biased;

            // Eviction takes highest priority: send the disconnect frame and close.
            _ = evict_rx.recv() => {
                if let Some(msg) = bytes_to_ws_msg(disconnect_frame.clone(), is_binary) {
                    let _ = sender.send(msg).await;
                }
                let _ = sender.close().await;
                return;
            }

            urgent = urgent_close_rx.recv(), if urgent_open => {
                match urgent {
                    Some(payload) => {
                        if let Some(msg) = bytes_to_ws_msg(payload, is_binary) {
                            let _ = sender.send(msg).await;
                        }
                        let _ = sender.close().await;
                        return;
                    }
                    None => urgent_open = false,
                }
            }

            _ = shutdown_rx.changed() => {
                while let Ok(payload) = replay_rx.try_recv() {
                    if let Some(msg) = bytes_to_ws_msg(payload, is_binary) {
                        if sender.send(msg).await.is_err() {
                            break;
                        }
                    }
                }
                while let Ok(payload) = live_rx.try_recv() {
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

            // Replay before live so catch-up always precedes live fan-out on this socket.
            replay = replay_rx.recv() => {
                if let Some(payload) = replay {
                    if let Some(msg) = bytes_to_ws_msg(payload, is_binary) {
                        if sender.send(msg).await.is_err() {
                            break 'ws;
                        }
                    }
                    for _ in 0..OUTBOUND_REPLAY_BURST.saturating_sub(1) {
                        let Ok(payload) = replay_rx.try_recv() else {
                            break;
                        };
                        if let Some(msg) = bytes_to_ws_msg(payload, is_binary) {
                            if sender.send(msg).await.is_err() {
                                break 'ws;
                            }
                        }
                    }
                }
            }

            payload = live_rx.recv() => {
                match payload {
                    Some(payload) => {
                        if let Some(msg) = bytes_to_ws_msg(payload, is_binary) {
                            if sender.send(msg).await.is_err() {
                                break 'ws;
                            }
                        }
                    }
                    None => break 'ws,
                }
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

    // Per-connection handshake state.  Starts in AwaitingHello; transitions to
    // Active on receipt of a valid hello frame (or immediately in Compat mode
    // when a subscribe arrives first).
    let mut conn_state = ConnectionState::AwaitingHello;

    // Set from hello `capabilities` (`replay_v1`); stays false for compat subscribe-before-hello.
    let mut replay_capable = false;

    // Stores the last_seq from the client hello for reconnection replay.
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
                            text.as_bytes(), &ctx, state,
                            &mut conn_state, &mut replay_capable, &mut last_seq, &mut heartbeats,
                        ).await;
                    }
                    Some(Ok(Message::Binary(ref data))) => {
                        handle_client_frame(
                            data, &ctx, state,
                            &mut conn_state, &mut replay_capable, &mut last_seq, &mut heartbeats,
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
                    if ctx.live_tx.send(ping).await.is_err() {
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
    conn_state: &mut ConnectionState,
    replay_capable: &mut bool,
    last_seq: &mut Option<u64>,
    heartbeats: &mut std::collections::HashMap<String, HeartbeatHandle>,
) {
    let frame = match ctx.codec.decode(data) {
        Ok(f) => f,
        Err(_) => {
            tracing::warn!(ctx.conn_id, "failed to decode client frame");
            return;
        }
    };

    match frame {
        ClientFrame::Hello(hello) => {
            *last_seq = hello.last_seq;
            *replay_capable = hello.is_replay_capable();
            *conn_state = ConnectionState::Active;

            if hello.last_seq.is_some_and(|s| s > 0) {
                state.metrics.client_reconnect_handshake_total.inc();
            }

            if hello.is_replay_capable() {
                state.metrics.handshake_ok_replay_capable_total.inc();
                tracing::info!(
                    ctx.conn_id,
                    last_seq = ?hello.last_seq,
                    "client hello: replay_v1 capable"
                );
            } else {
                state.metrics.handshake_ok_legacy_total.inc();
                tracing::info!(
                    ctx.conn_id,
                    last_seq = ?hello.last_seq,
                    "client hello: no replay capability"
                );
            }
        }

        ClientFrame::Command(cmd) => {
            // Enforce handshake state before processing commands.
            if *conn_state == ConnectionState::AwaitingHello {
                if state.replay_enforcement.is_enforcing() {
                    // Reject the command — client must send hello first.
                    state.metrics.handshake_rejected_total.inc();
                    tracing::warn!(
                        ctx.conn_id,
                        ?cmd,
                        enforcement = ?state.replay_enforcement,
                        "command rejected: hello required before subscribe"
                    );
                    if let ClientCommand::Subscribe { ref identifier } = cmd {
                        if let Ok(reject) = ctx.codec.encode(&ServerMessage::RejectSubscription {
                            identifier: identifier.clone(),
                        }) {
                            let _ = ctx.live_tx.send(reject).await;
                        }
                    }
                    return;
                }

                // Compat mode: allow with a warning metric and transition to Active.
                state.metrics.handshake_legacy_warn_total.inc();
                tracing::warn!(
                    ctx.conn_id,
                    "subscribe before hello: transitioning to Active in compat mode (legacy client)"
                );
                *conn_state = ConnectionState::Active;
            }

            tracing::debug!(ctx.conn_id, ?cmd, "received command");
            dispatch_command(cmd, ctx, state, last_seq, heartbeats, *replay_capable).await;
        }
    }
}

/// Dispatches a fully validated [`ClientCommand`] after handshake state checks pass.
async fn dispatch_command(
    cmd: ClientCommand,
    ctx: &ConnContext<'_>,
    state: &AppState,
    last_seq: &mut Option<u64>,
    heartbeats: &mut std::collections::HashMap<String, HeartbeatHandle>,
    replay_capable: bool,
) {
    match cmd {
        ClientCommand::Subscribe { identifier } => {
            if state.replay_enforcement.is_enforcing() && !replay_capable {
                state
                    .metrics
                    .handshake_rejected_non_replay_capable_total
                    .inc();
                tracing::warn!(
                    ctx.conn_id,
                    identifier,
                    enforcement = ?state.replay_enforcement,
                    "subscribe rejected: replay_v1 capability required"
                );
                if let Ok(reject) = ctx
                    .codec
                    .encode(&ServerMessage::RejectSubscription { identifier })
                {
                    let _ = ctx.live_tx.send(reject).await;
                }
                return;
            }

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
                    let _ = ctx.live_tx.send(reject).await;
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

            // When the client reconnected with a last_seq, spawn a background replay
            // task to stream missed messages before the confirm.  The background task
            // sends the confirm itself so the client receives replayed messages first.
            // The inbound loop is not blocked during replay.
            if let (Some(seq), Some(nats)) =
                (*last_seq, state.nats_consumer.as_ref().map(Arc::clone))
            {
                let semaphore = Arc::clone(&state.replay_semaphore);
                let task_replay_tx = ctx.replay_tx.clone();
                let task_urgent_tx = ctx.urgent_close_tx.clone();
                let task_codec = Arc::clone(&ctx.codec);
                let task_metrics = Arc::clone(&state.metrics);
                let task_stream = identifier.clone();
                let conn_id = ctx.conn_id;

                tokio::spawn(async move {
                    // Queue until a replay slot is free; abort cleanly on shutdown.
                    let _permit = match semaphore.acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => return, // Semaphore closed (server shutting down).
                    };

                    let replay_t0 = Instant::now();
                    task_metrics.replay_in_flight.inc();
                    let result = nats
                        .replay_since(
                            &task_stream,
                            seq,
                            task_replay_tx.clone(),
                            task_codec.clone(),
                        )
                        .await;
                    task_metrics.replay_in_flight.dec();

                    match result {
                        Ok(d) if d.peer_gone => {
                            tracing::debug!(
                                conn_id,
                                stream = %task_stream,
                                delivered = d.delivered,
                                "replay aborted: peer outbound closed"
                            );
                            task_metrics.replay_aborted_peer_gone_total.inc();
                            task_metrics
                                .replay_catch_up_duration_secs
                                .observe(replay_t0.elapsed().as_secs_f64());
                            if let Some(fd) = d.first_delivery_elapsed {
                                task_metrics
                                    .replay_first_delivery_secs
                                    .observe(fd.as_secs_f64());
                            }
                        }
                        Ok(d) => {
                            tracing::info!(
                                conn_id,
                                stream = %task_stream,
                                count = d.delivered,
                                "replay completed"
                            );
                            task_metrics.replay_success_total.inc();
                            task_metrics
                                .replay_catch_up_duration_secs
                                .observe(replay_t0.elapsed().as_secs_f64());
                            if let Some(fd) = d.first_delivery_elapsed {
                                task_metrics
                                    .replay_first_delivery_secs
                                    .observe(fd.as_secs_f64());
                            }
                            if let Ok(confirm) =
                                task_codec.encode(&ServerMessage::ConfirmSubscription {
                                    identifier: task_stream,
                                })
                            {
                                let _ = task_replay_tx.send(confirm).await;
                            }
                        }
                        Err(ReplayError::WindowExceeded {
                            ref stream,
                            requested_seq,
                            oldest_available,
                        }) => {
                            tracing::warn!(
                                conn_id,
                                stream = %stream,
                                requested_seq,
                                oldest_available,
                                "replay window exceeded: disconnecting for full resync"
                            );
                            task_metrics.replay_window_exceeded_total.inc();
                            task_metrics
                                .replay_catch_up_duration_secs
                                .observe(replay_t0.elapsed().as_secs_f64());
                            if let Ok(disconnect) = task_codec.encode(&ServerMessage::Disconnect {
                                reason: "replay_window_exceeded".to_string(),
                                reconnect: Some(true),
                            }) {
                                let _ = task_urgent_tx.send(disconnect).await;
                            }
                        }
                        Err(ReplayError::Truncated {
                            ref stream,
                            delivered,
                            limit,
                        }) => {
                            tracing::warn!(
                                conn_id,
                                stream = %stream,
                                delivered,
                                limit,
                                "replay truncated at cap: disconnecting for continued catch-up"
                            );
                            task_metrics.replay_truncated_total.inc();
                            task_metrics
                                .replay_catch_up_duration_secs
                                .observe(replay_t0.elapsed().as_secs_f64());
                            if let Ok(disconnect) = task_codec.encode(&ServerMessage::Disconnect {
                                reason: "replay_truncated".to_string(),
                                reconnect: Some(true),
                            }) {
                                let _ = task_urgent_tx.send(disconnect).await;
                            }
                        }
                        Err(ReplayError::JetStream(ref e)) => {
                            tracing::error!(
                                conn_id,
                                stream = %task_stream,
                                error = %e,
                                "replay JetStream error"
                            );
                            task_metrics.replay_failure_total.inc();
                            task_metrics
                                .replay_catch_up_duration_secs
                                .observe(replay_t0.elapsed().as_secs_f64());
                            // Still confirm so the client can continue with live messages.
                            if let Ok(confirm) =
                                task_codec.encode(&ServerMessage::ConfirmSubscription {
                                    identifier: task_stream,
                                })
                            {
                                let _ = task_replay_tx.send(confirm).await;
                            }
                        }
                    }
                });
            } else {
                // No replay needed — confirm immediately.
                if let Ok(confirm) = ctx
                    .codec
                    .encode(&ServerMessage::ConfirmSubscription { identifier })
                {
                    let _ = ctx.live_tx.send(confirm).await;
                }
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
