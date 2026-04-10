mod common;

use std::time::Duration;

use bytes::Bytes;
use tokio::time::timeout;

/// Pre-publish messages, then reconnect with `last_seq=0` and assert that
/// replayed messages arrive (with `replayed:true`) before the live stream
/// and before ConfirmSubscription.
///
/// Skipped when `nats-server` is not on PATH.
#[tokio::test]
async fn replay_precedes_live_on_reconnect() {
    let Some(nats) = common::start_nats().await else {
        eprintln!("SKIP replay_precedes_live_on_reconnect: nats-server not found on PATH");
        return;
    };

    // --- Phase 1: seed the JetStream stream before starting the server ---
    let seed_client = async_nats::connect(&nats.url)
        .await
        .expect("NATS connect for seeding");
    let seed_js = async_nats::jetstream::new(seed_client);

    // Create the TURBOCABLE stream so it exists before the server starts.
    seed_js
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "TURBOCABLE".to_string(),
            subjects: vec!["TURBOCABLE.>".to_string()],
            storage: async_nats::jetstream::stream::StorageType::File,
            max_age: Duration::from_secs(7 * 24 * 60 * 60),
            num_replicas: 1,
            ..Default::default()
        })
        .await
        .expect("create TURBOCABLE stream");

    // Publish 3 messages before any client is connected.
    for i in 0u32..3 {
        let payload = serde_json::json!({"n": i});
        seed_js
            .publish(
                "TURBOCABLE.replay_test",
                Bytes::from(serde_json::to_vec(&payload).unwrap()),
            )
            .await
            .expect("publish")
            .await
            .expect("ack");
    }

    // --- Phase 2: start the server and connect ---
    let handle = turbocable_server::server::start(common::test_config(&nats.url)).await;

    let mut ws = common::connect_ws(handle.addr).await;

    // Read Welcome.
    let welcome = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for Welcome");
    let v: serde_json::Value = serde_json::from_str(&welcome).unwrap();
    assert_eq!(v["type"], "welcome");

    // Send Hello with last_seq=0 and replay_v1 capability — requests replay
    // starting from the first available sequence (all 3 messages).
    common::send_text(
        &mut ws,
        r#"{"type":"hello","last_seq":0,"capabilities":["replay_v1"]}"#,
    )
    .await;

    // Subscribe — triggers the background replay task.
    common::send_text(
        &mut ws,
        r#"{"command":"subscribe","identifier":"replay_test"}"#,
    )
    .await;

    // --- Phase 3: assert replayed messages arrive before ConfirmSubscription ---
    let mut replayed_count = 0usize;
    let mut confirmed = false;

    // Expect: 3 replayed messages + ConfirmSubscription, in that order.
    for _ in 0..10 {
        let frame = timeout(Duration::from_secs(10), common::recv_text(&mut ws))
            .await
            .expect("timed out waiting for replay/confirm frame");
        let v: serde_json::Value = serde_json::from_str(&frame).unwrap();

        match v["type"].as_str().unwrap_or("") {
            "message" => {
                assert!(
                    !confirmed,
                    "received message AFTER confirm — replay ordering violated: {frame}"
                );
                assert_eq!(
                    v["replayed"], true,
                    "expected replayed:true on replay message: {frame}"
                );
                assert_eq!(v["identifier"], "replay_test");
                replayed_count += 1;
            }
            "confirm_subscription" => {
                assert_eq!(
                    replayed_count, 3,
                    "expected exactly 3 replayed messages before confirm; got {replayed_count}"
                );
                confirmed = true;
                break;
            }
            other => panic!("unexpected frame type '{other}': {frame}"),
        }
    }

    assert!(confirmed, "never received ConfirmSubscription");
    assert_eq!(replayed_count, 3);
}

/// A client that reconnects with `last_seq` pointing past the stream's
/// retention window receives a disconnect with `reason: replay_window_exceeded`.
///
/// Skipped when `nats-server` is not on PATH.
#[tokio::test]
async fn replay_window_exceeded_disconnects_client() {
    let Some(nats) = common::start_nats().await else {
        eprintln!("SKIP replay_window_exceeded_disconnects_client: nats-server not found on PATH");
        return;
    };

    let seed_client = async_nats::connect(&nats.url).await.expect("NATS connect");
    let seed_js = async_nats::jetstream::new(seed_client);

    // Create a stream with a very short max_age so messages expire quickly.
    seed_js
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "TURBOCABLE".to_string(),
            subjects: vec!["TURBOCABLE.>".to_string()],
            storage: async_nats::jetstream::stream::StorageType::File,
            // 1-second retention — messages are already past the window by the
            // time the client subscribes.
            max_age: Duration::from_secs(1),
            num_replicas: 1,
            ..Default::default()
        })
        .await
        .expect("create short-retention stream");

    // Publish one message, then wait for it to expire.
    seed_js
        .publish("TURBOCABLE.window_test", Bytes::from(b"expires".as_slice()))
        .await
        .expect("publish")
        .await
        .expect("ack");

    // Wait long enough for the message to expire.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let handle = turbocable_server::server::start(common::test_config(&nats.url)).await;

    let mut ws = common::connect_ws(handle.addr).await;

    // Skip Welcome.
    let _welcome = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("Welcome");

    // Reconnect hello claiming last_seq=1 — the only message has expired.
    common::send_text(
        &mut ws,
        r#"{"type":"hello","last_seq":1,"capabilities":["replay_v1"]}"#,
    )
    .await;

    // Subscribe — triggers replay which detects the window exceeded.
    common::send_text(
        &mut ws,
        r#"{"command":"subscribe","identifier":"window_test"}"#,
    )
    .await;

    // Expect a disconnect frame (server sends Disconnect before closing).
    let frame = timeout(Duration::from_secs(10), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for disconnect frame");
    let v: serde_json::Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(v["type"], "disconnect", "got: {frame}");
    assert_eq!(v["reason"], "replay_window_exceeded", "got: {frame}");
}
