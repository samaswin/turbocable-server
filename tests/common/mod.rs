//! Shared helpers for integration tests.
//!
//! Each test binary includes this module with `mod common;`. Helpers here
//! spin up ephemeral NATS instances, start the server in-process, and provide
//! a thin WebSocket client for sending/receiving frames.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// RSA test key pair (2048-bit) — never use in production.
// Identical to the keys embedded in src/auth/jwt.rs tests.
// ---------------------------------------------------------------------------

pub const TEST_RSA_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDH0NRdQOkpkbDm
iTfhST6rIThVkJM1pa3cJPA+sPdRoViPLHC9o648ygEATXsTjZtefpq/niPu0f0h
tERYuNQyom+MGbYqm4qXkdGfOc7/bk5UOpftEQ50GzkrlfmfyXGreIl+bIOMFVjd
OkZujNH4Gw1TOQQ0oOavquSOwSaifYnl1Fnew+XEC0uScBzUXp/QIJdjCOCf1u5Q
T9imYPHM4ZOF8JF3MnqvESvMV6FJ+w2WfJSmseLHXjlBfu9juHFACzGxkXpFqO9f
+558p4F03YWp2sxNTbz2PoiX3CO0FWJLOYpVkLMGvz5hkeTteo1LrKe0XSxqYZfF
bt+Bv+BFAgMBAAECggEAEmbRf+sP7gOcUobVjhpUOqdbDEo9vGWPLuR5+ZQLmsls
ofbaRSSzUaba07/O81yJr/ih4L68GWzeToHO/4q6BBXAhxsBE0hyyYWk0/Cbdxud
/BTPVAZLmfa92506uXPwU3XM18c/kCGRJwKMZPb0CVDYd88a64vb4tauqNTx7WnP
YQiKY9EkJPHObT+Ud6Uebgyi6BGACyunNO2Ty0eVJzuRvv/Peae128qTsNl2Vl7m
3PpSeF6LDOj6RdXeqk6pVdFsTxBKq+yIh7i3HbqIKevVn9pfVlXVBqKQEv8HkI/v
+V35kQa/UBEnwSKm2pU3WGHlPzcGwa665teU8UVIawKBgQD9eADGPaCbpBE+i2d7
lcOi+IY4lNvwSo/l11eBdKfP2Hx44q+YL4AHU45+RA24WQbXLLoCTLjlkgq7s/LJ
6tBvMQ7RwTkBlzr25K/uqy3JN0vnlb+vxVKYNtqecVtqCemoduagIjWtiTmuaWXm
VJXSwQUdxPAOFuIJGcwGQCKUHwKBgQDJz6llw01o9R4m8O9R2qDAxkCOk/bE6dSE
EYb/FOgsrCbL2P36rmYaDRYodMrNp1+vHLQ8xQd/qOhiDeQuHhoClvA9DXBBs5ML
PWEn8SHsejktxmaEvN3BXnd1nFm4r24luNc2VjU8YlWpUloEGfC203jVaMmFtKF8
fgTLsIWfGwKBgQDLJdkJCe+ljrO7eyNva7Mm9SUuSDCWwEvgnN0nhoXREeOBR74Q
rVFhjdiQ3p5YeBIBd3mFylQOuyQbGLiomKiB1cHY35J+8eRyaQuQsGW79bPCYsUF
bZMrKBvEDXqE3HkHanShN4nqEifG3/apynViOw2MtIDp6fEz9hcNk22jZQKBgGM/
UwmOwLULRubTun5AzKnBVeJIdiVk8XR5wjAUMhI2H2ZEsrLjrabGJM2EknANDgtq
TGFObF+ly5LdTgg4GYaIgGEmCLzm+Tuf1fX0qkBH43LVjXleAJimQo1+dMlUzRCU
FJLOVqP5oDMDIu29bBodaeFaBTFSIdC9kNIzX6NdAoGBAPanzGOVw8i2ytBNKHnc
nj9LeLDSU5hTdMbkfsQcSlYp7cm6IouIekFzrfGg2+R7ePB1zTdQT86NoH7bnbPO
obpj1op1zYyZdhOUmAMyEfpcWKUknGXSTl9NJQp2Dh7kv+VfFSEoOxA4KLagTnMF
CM21qtHKzbkqhZvIvYUOhpdX
-----END PRIVATE KEY-----";

pub const TEST_RSA_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAx9DUXUDpKZGw5ok34Uk+
qyE4VZCTNaWt3CTwPrD3UaFYjyxwvaOuPMoBAE17E42bXn6av54j7tH9IbREWLjU
MqJvjBm2KpuKl5HRnznO/25OVDqX7REOdBs5K5X5n8lxq3iJfmyDjBVY3TpGbozR
+BsNUzkENKDmr6rkjsEmon2J5dRZ3sPlxAtLknAc1F6f0CCXYwjgn9buUE/YpmDx
zOGThfCRdzJ6rxErzFehSfsNlnyUprHix145QX7vY7hxQAsxsZF6RajvX/uefKeB
dN2FqdrMTU289j6Il9wjtBViSzmKVZCzBr8+YZHk7XqNS6yntF0samGXxW7fgb/g
RQIDAQAB
-----END PUBLIC KEY-----";

// ---------------------------------------------------------------------------
// Free port finder
// ---------------------------------------------------------------------------

/// Binds to port 0 to let the OS assign an ephemeral port, then returns it.
///
/// There is a tiny TOCTOU window between returning the port and the caller
/// binding to it, but it is negligible in a test environment.
pub fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind port 0");
    listener.local_addr().expect("local_addr").port()
}

// ---------------------------------------------------------------------------
// NATS server lifecycle
// ---------------------------------------------------------------------------

/// A NATS server child process bound to an ephemeral port.
///
/// Killed (SIGKILL) when dropped.
pub struct NatsServer {
    process: std::process::Child,
    pub url: String,
    pub port: u16,
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Starts an in-process-supervised `nats-server -js` on a free port.
///
/// Returns `None` if `nats-server` is not found on `PATH` — callers should
/// skip NATS-dependent tests in that case.
pub async fn start_nats() -> Option<NatsServer> {
    // Probe: does nats-server exist on PATH?
    let probe = std::process::Command::new("nats-server")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    if probe.is_err() {
        return None;
    }

    let port = find_free_port();
    let store_dir = format!("/tmp/nats_test_{port}");
    let child = std::process::Command::new("nats-server")
        .args(["-js", "-p", &port.to_string(), "-sd", &store_dir])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;

    let url = format!("nats://127.0.0.1:{port}");

    // Poll until nats-server is accepting connections (up to 5 s).
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if async_nats::connect(&url).await.is_ok() {
            return Some(NatsServer {
                process: child,
                url,
                port,
            });
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Server config helpers
// ---------------------------------------------------------------------------

/// Builds a [`turbocable_server::config::Config`] suitable for tests.
///
/// - `port = 0` → OS picks an ephemeral port (read back from [`ServerHandle::addr`]).
/// - Long ping interval (300 s) avoids spurious Ping frames during assertions.
/// - Small channel capacities to keep memory usage low.
pub fn test_config(nats_url: &str) -> turbocable_server::config::Config {
    turbocable_server::config::Config {
        port: 0,
        nats_url: nats_url.to_string(),
        node_id: "test_node".to_string(),
        ping_interval_secs: 300,
        max_connections_per_ip: 100,
        ws_channel_capacity: 64,
        ws_replay_channel_capacity: 64,
        jwt_public_key_path: None,
        max_ack_pending: 1000,
        nats_stream_replicas: 1,
        replay_enforcement: turbocable_server::config::ReplayEnforcement::Compat,
        max_replay_concurrency: 10,
        // Rate limiting disabled by default in tests.
        stream_rate_limit_rps: 0,
        stream_rate_limit_burst: 0,
        stream_rate_overrides: String::new(),
    }
}

/// URL that reliably refuses connections — used to start the server without NATS.
pub const NO_NATS_URL: &str = "nats://127.0.0.1:1";

// ---------------------------------------------------------------------------
// WebSocket client helpers
// ---------------------------------------------------------------------------

pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Opens a WebSocket connection to `/cable` with the JSON sub-protocol.
pub async fn connect_ws(addr: SocketAddr) -> WsStream {
    let url = format!("ws://{addr}/cable");
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WebSocket connect failed");
    stream
}

/// Opens a WebSocket connection with a custom `Sec-WebSocket-Protocol` header.
pub async fn connect_ws_subprotocol(addr: SocketAddr, subprotocol: &str) -> WsStream {
    let url = format!("ws://{addr}/cable");
    let mut request = url.into_client_request().expect("build request");
    request.headers_mut().insert(
        "sec-websocket-protocol",
        subprotocol.parse().expect("header value"),
    );
    let (stream, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("WebSocket connect failed");
    stream
}

/// Opens a WebSocket connection passing a JWT in the `?token=` query parameter.
pub async fn connect_ws_token(addr: SocketAddr, token: &str) -> WsStream {
    let url = format!("ws://{addr}/cable?token={token}");
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WebSocket connect failed");
    stream
}

/// Sends a text frame.
pub async fn send_text(stream: &mut WsStream, msg: &str) {
    stream
        .send(Message::Text(msg.to_owned()))
        .await
        .expect("send text");
}

/// Receives the next text frame, skipping Ping/Pong keepalives.
pub async fn recv_text(stream: &mut WsStream) -> String {
    loop {
        match stream
            .next()
            .await
            .expect("stream ended before text message")
        {
            Ok(Message::Text(s)) => return s,
            Ok(Message::Ping(_) | Message::Pong(_)) => continue,
            Ok(other) => panic!("expected text message, got: {other:?}"),
            Err(e) => panic!("WebSocket error: {e}"),
        }
    }
}

/// Receives the next binary frame, skipping Ping/Pong keepalives.
pub async fn recv_binary(stream: &mut WsStream) -> Vec<u8> {
    loop {
        match stream
            .next()
            .await
            .expect("stream ended before binary message")
        {
            Ok(Message::Binary(b)) => return b,
            Ok(Message::Ping(_) | Message::Pong(_)) => continue,
            Ok(other) => panic!("expected binary message, got: {other:?}"),
            Err(e) => panic!("WebSocket error: {e}"),
        }
    }
}

/// Waits for a WebSocket close frame and returns the numeric close code.
///
/// Skips any data frames received before the close (they may be buffered
/// disconnect / drain messages sent immediately before the close).
pub async fn recv_close_code(stream: &mut WsStream) -> u16 {
    loop {
        match stream.next().await {
            Some(Ok(Message::Close(Some(frame)))) => return u16::from(frame.code),
            Some(Ok(Message::Close(None))) => return 1000,
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            // Skip any buffered data frames before the close.
            Some(Ok(Message::Text(_) | Message::Binary(_) | Message::Frame(_))) => continue,
            Some(Err(e)) => {
                // tungstenite may surface the close as an error on some paths.
                let s = e.to_string();
                if s.contains("1001") {
                    return 1001;
                }
                if s.contains("3000") {
                    return 3000;
                }
                panic!("WebSocket error while waiting for close: {e}");
            }
            None => return 1000, // stream ended cleanly
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP helper (no reqwest dependency)
// ---------------------------------------------------------------------------

/// Issues a minimal HTTP/1.1 GET and returns `(status_code, body)`.
pub async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("TCP connect");
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("write");

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");

    let resp = String::from_utf8_lossy(&buf);
    let status_line = resp.lines().next().unwrap_or("");
    let code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    let body = resp.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (code, body)
}

// ---------------------------------------------------------------------------
// JWT helpers
// ---------------------------------------------------------------------------

/// Creates a signed RS256 JWT valid for 1 hour with the test private key.
pub fn make_jwt(sub: &str, streams: &[&str]) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize;

    let claims = serde_json::json!({
        "sub": sub,
        "allowed_streams": streams,
        "iat": now,
        "exp": now + 3600,
    });

    let key = jsonwebtoken::EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY.as_bytes())
        .expect("test private key");
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &key,
    )
    .expect("encode JWT")
}

/// Creates a signed RS256 JWT that is already expired.
pub fn make_expired_jwt(sub: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize;

    let claims = serde_json::json!({
        "sub": sub,
        "allowed_streams": ["*"],
        "iat": now - 7200,
        "exp": now - 3600,
    });

    let key = jsonwebtoken::EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY.as_bytes())
        .expect("test private key");
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &key,
    )
    .expect("encode expired JWT")
}

// ---------------------------------------------------------------------------
// Convenience: write the test public key PEM to a temp file.
// ---------------------------------------------------------------------------

static TEST_PUBKEY_FILE_ID: AtomicU64 = AtomicU64::new(0);

/// Writes the test RSA public key to a temp file and returns the path.
///
/// Uses a unique filename per call so parallel integration tests (default
/// `cargo test` harness) never read a partially written or truncated PEM from
/// a shared path.
pub async fn write_test_pubkey() -> std::path::PathBuf {
    let id = TEST_PUBKEY_FILE_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("turbocable_test_pubkey_{id}.pem"));
    tokio::fs::write(&path, TEST_RSA_PUBLIC_KEY.as_bytes())
        .await
        .expect("write test public key");
    path
}
