mod common;

use std::time::Duration;

use bytes::Bytes;
use tokio::time::timeout;

/// subscribe → publish via NATS → assert message delivery and ordering.
///
/// Skipped when `nats-server` is not on PATH.
#[tokio::test]
async fn fanout_delivers_nats_message() {
    let Some(nats) = common::start_nats().await else {
        eprintln!("SKIP fanout_delivers_nats_message: nats-server not found on PATH");
        return;
    };

    let handle = turbocable_server::server::start(common::test_config(&nats.url)).await;

    // Connect and perform the handshake.
    let mut ws = common::connect_ws(handle.addr).await;

    let welcome = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for Welcome");
    let v: serde_json::Value = serde_json::from_str(&welcome).unwrap();
    assert_eq!(v["type"], "welcome");

    common::send_text(&mut ws, r#"{"type":"hello","capabilities":["replay_v1"]}"#).await;

    // Subscribe to the test stream.
    common::send_text(
        &mut ws,
        r#"{"command":"subscribe","identifier":"fanout_test"}"#,
    )
    .await;

    let confirm = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for ConfirmSubscription");
    let v: serde_json::Value = serde_json::from_str(&confirm).unwrap();
    assert_eq!(v["type"], "confirm_subscription", "got: {confirm}");
    assert_eq!(v["identifier"], "fanout_test");

    // Publish a message via NATS — the server's fan-out loop will deliver it.
    let nats_client = async_nats::connect(&nats.url).await.expect("NATS connect");
    let jetstream = async_nats::jetstream::new(nats_client);
    let payload = serde_json::json!({"text": "hello from nats"});
    jetstream
        .publish(
            "TURBOCABLE.fanout_test",
            Bytes::from(serde_json::to_vec(&payload).unwrap()),
        )
        .await
        .expect("publish")
        .await
        .expect("ack");

    // Assert the fan-out message arrives on the WebSocket.
    let msg = timeout(Duration::from_secs(10), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for fan-out message");
    let v: serde_json::Value = serde_json::from_str(&msg).unwrap();

    assert_eq!(v["type"], "message", "got: {msg}");
    assert_eq!(v["identifier"], "fanout_test");
    assert_eq!(v["message"]["text"], "hello from nats");
    // replayed should be absent for live fan-out messages.
    assert!(
        v["replayed"].is_null(),
        "live message should not have replayed flag"
    );
}

/// Two subscribers on the same stream both receive published messages in order.
///
/// Skipped when `nats-server` is not on PATH.
#[tokio::test]
async fn fanout_ordering_two_subscribers() {
    let Some(nats) = common::start_nats().await else {
        eprintln!("SKIP fanout_ordering_two_subscribers: nats-server not found on PATH");
        return;
    };

    let handle = turbocable_server::server::start(common::test_config(&nats.url)).await;

    let mut ws1 = common::connect_ws(handle.addr).await;
    let mut ws2 = common::connect_ws(handle.addr).await;

    // Handshake for both connections.
    for ws in [&mut ws1, &mut ws2] {
        let _welcome = timeout(Duration::from_secs(5), common::recv_text(ws))
            .await
            .expect("Welcome");
        common::send_text(ws, r#"{"type":"hello","capabilities":["replay_v1"]}"#).await;
        common::send_text(ws, r#"{"command":"subscribe","identifier":"order_test"}"#).await;
        let _confirm = timeout(Duration::from_secs(5), common::recv_text(ws))
            .await
            .expect("ConfirmSubscription");
    }

    // Publish two messages in sequence.
    let nats_client = async_nats::connect(&nats.url).await.expect("NATS connect");
    let jetstream = async_nats::jetstream::new(nats_client);

    for i in 0u32..2 {
        let payload = serde_json::json!({"n": i});
        jetstream
            .publish(
                "TURBOCABLE.order_test",
                Bytes::from(serde_json::to_vec(&payload).unwrap()),
            )
            .await
            .expect("publish")
            .await
            .expect("ack");
    }

    // Both subscribers receive messages in order.
    for ws in [&mut ws1, &mut ws2] {
        for expected_n in 0u32..2 {
            let msg = timeout(Duration::from_secs(10), common::recv_text(ws))
                .await
                .expect("timed out waiting for ordered message");
            let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
            assert_eq!(v["type"], "message");
            assert_eq!(
                v["message"]["n"], expected_n,
                "expected n={expected_n}, got: {msg}"
            );
        }
    }
}
