mod common;

use std::time::Duration;

use bytes::Bytes;
use tokio::time::timeout;

/// Verifies that the per-stream rate limiter drops messages beyond the
/// configured burst when a stream is flooded.
///
/// Setup: rps=5, burst=5.  We publish 30 messages near-instantly.
/// The bucket starts full (5 tokens), so exactly 5 should pass through;
/// the remaining 25 are dropped.  We allow a small margin (≤ 10) because
/// wall-clock refill could add a few tokens during the delivery window.
///
/// Skipped when `nats-server` is not on PATH.
#[tokio::test]
async fn rate_limiter_drops_excess_messages() {
    let Some(nats) = common::start_nats().await else {
        eprintln!("SKIP rate_limiter_drops_excess_messages: nats-server not found on PATH");
        return;
    };

    let mut cfg = common::test_config(&nats.url);
    cfg.stream_rate_limit_rps = 5;
    cfg.stream_rate_limit_burst = 5;

    let handle = turbocable_server::server::start(cfg).await;
    let mut ws = common::connect_ws(handle.addr).await;

    // Handshake.
    let _welcome = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for Welcome");
    common::send_text(&mut ws, r#"{"type":"hello","capabilities":["replay_v1"]}"#).await;
    common::send_text(
        &mut ws,
        r#"{"command":"subscribe","identifier":"rate_test"}"#,
    )
    .await;
    let _confirm = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for ConfirmSubscription");

    // Publish 30 messages near-instantly — well above the 5-message burst.
    let nats_client = async_nats::connect(&nats.url).await.expect("NATS connect");
    let jetstream = async_nats::jetstream::new(nats_client);
    for i in 0u32..30 {
        let payload = serde_json::json!({"n": i});
        jetstream
            .publish(
                "TURBOCABLE.rate_test",
                Bytes::from(serde_json::to_vec(&payload).unwrap()),
            )
            .await
            .expect("publish")
            .await
            .expect("ack");
    }

    // Drain messages for up to 2 seconds.  The publish burst is near-instant so
    // refill is negligible; we expect at most ~10 through (burst=5 + ~1s × 5rps).
    let mut received = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, common::recv_text(&mut ws)).await {
            Ok(_msg) => received += 1,
            Err(_) => break,
        }
    }

    assert!(
        received >= 1,
        "at least 1 message should pass the burst, got {received}"
    );
    assert!(
        received <= 15,
        "expected at most ~15 messages with burst=5, rps=5 over 2s; got {received}"
    );
}

/// When rate limiting is disabled (rps=0, the default), every published
/// message reaches WebSocket subscribers.
///
/// Skipped when `nats-server` is not on PATH.
#[tokio::test]
async fn rate_limiter_disabled_passes_all_messages() {
    let Some(nats) = common::start_nats().await else {
        eprintln!("SKIP rate_limiter_disabled_passes_all_messages: nats-server not found on PATH");
        return;
    };

    // Default config has rps=0 → rate limiting disabled.
    let handle = turbocable_server::server::start(common::test_config(&nats.url)).await;
    let mut ws = common::connect_ws(handle.addr).await;

    // Handshake.
    let _welcome = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for Welcome");
    common::send_text(&mut ws, r#"{"type":"hello","capabilities":["replay_v1"]}"#).await;
    common::send_text(
        &mut ws,
        r#"{"command":"subscribe","identifier":"no_limit_test"}"#,
    )
    .await;
    let _confirm = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for ConfirmSubscription");

    // Publish 10 messages.
    let nats_client = async_nats::connect(&nats.url).await.expect("NATS connect");
    let jetstream = async_nats::jetstream::new(nats_client);
    for i in 0u32..10 {
        let payload = serde_json::json!({"n": i});
        jetstream
            .publish(
                "TURBOCABLE.no_limit_test",
                Bytes::from(serde_json::to_vec(&payload).unwrap()),
            )
            .await
            .expect("publish")
            .await
            .expect("ack");
    }

    // All 10 should arrive within 5 seconds.
    let mut received = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, common::recv_text(&mut ws)).await {
            Ok(_msg) => {
                received += 1;
                if received == 10 {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    assert_eq!(
        received, 10,
        "all 10 messages should arrive with rate limiting disabled"
    );
}

/// Per-stream override: a stream with a tight override is limited while
/// another stream using the default (disabled) is unaffected.
///
/// Skipped when `nats-server` is not on PATH.
#[tokio::test]
async fn rate_limiter_per_stream_override() {
    let Some(nats) = common::start_nats().await else {
        eprintln!("SKIP rate_limiter_per_stream_override: nats-server not found on PATH");
        return;
    };

    let mut cfg = common::test_config(&nats.url);
    // Default rps=0 (no limit), but override "tight_stream" to rps=1, burst=1.
    cfg.stream_rate_overrides = "tight_stream=1:1".to_string();

    let handle = turbocable_server::server::start(cfg).await;

    let mut ws_tight = common::connect_ws(handle.addr).await;
    let mut ws_free = common::connect_ws(handle.addr).await;

    for ws in [&mut ws_tight, &mut ws_free] {
        let _welcome = timeout(Duration::from_secs(5), common::recv_text(ws))
            .await
            .expect("Welcome");
        common::send_text(ws, r#"{"type":"hello","capabilities":["replay_v1"]}"#).await;
    }

    common::send_text(
        &mut ws_tight,
        r#"{"command":"subscribe","identifier":"tight_stream"}"#,
    )
    .await;
    let _c = timeout(Duration::from_secs(5), common::recv_text(&mut ws_tight))
        .await
        .expect("ConfirmSubscription tight_stream");

    common::send_text(
        &mut ws_free,
        r#"{"command":"subscribe","identifier":"free_stream"}"#,
    )
    .await;
    let _c = timeout(Duration::from_secs(5), common::recv_text(&mut ws_free))
        .await
        .expect("ConfirmSubscription free_stream");

    let nats_client = async_nats::connect(&nats.url).await.expect("NATS connect");
    let jetstream = async_nats::jetstream::new(nats_client);

    // Publish 10 messages to each stream simultaneously.
    for i in 0u32..10 {
        for subject in ["TURBOCABLE.tight_stream", "TURBOCABLE.free_stream"] {
            let payload = serde_json::json!({"n": i});
            jetstream
                .publish(subject, Bytes::from(serde_json::to_vec(&payload).unwrap()))
                .await
                .expect("publish")
                .await
                .expect("ack");
        }
    }

    // tight_stream: burst=1 so only 1 message expected in a short window.
    let mut tight_count = 0usize;
    let tight_deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    loop {
        let remaining = tight_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, common::recv_text(&mut ws_tight)).await {
            Ok(_) => tight_count += 1,
            Err(_) => break,
        }
    }

    // free_stream: no override, default rps=0 → all 10 should arrive.
    let mut free_count = 0usize;
    let free_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = free_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, common::recv_text(&mut ws_free)).await {
            Ok(_) => {
                free_count += 1;
                if free_count == 10 {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    assert!(
        tight_count <= 3,
        "tight_stream (burst=1, rps=1) should deliver at most 3 messages in 500ms; got {tight_count}"
    );
    assert_eq!(
        free_count, 10,
        "free_stream (no limit) should deliver all 10 messages; got {free_count}"
    );
}
