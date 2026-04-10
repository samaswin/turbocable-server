mod common;

use std::time::Duration;
use tokio::time::timeout;

/// JSON codec: connecting and receiving the Welcome frame.
///
/// The server sends `{"type":"welcome"}` immediately after the WebSocket
/// upgrade — no client hello is required to receive it.
#[tokio::test]
async fn json_welcome_on_connect() {
    let handle = turbocable_server::server::start(common::test_config(common::NO_NATS_URL)).await;

    let mut ws = common::connect_ws(handle.addr).await;

    let msg = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for Welcome");

    let v: serde_json::Value = serde_json::from_str(&msg).expect("valid JSON");
    assert_eq!(v["type"], "welcome", "expected welcome, got: {msg}");
}

/// JSON codec: sending Hello transitions the server to Active state; the
/// connection remains open and further frames can be exchanged.
#[tokio::test]
async fn json_hello_accepted() {
    let handle = turbocable_server::server::start(common::test_config(common::NO_NATS_URL)).await;

    let mut ws = common::connect_ws(handle.addr).await;

    // Read the Welcome sent on connect.
    let welcome = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for Welcome");
    let v: serde_json::Value = serde_json::from_str(&welcome).expect("valid JSON");
    assert_eq!(v["type"], "welcome");

    // Send Hello — server processes it silently (no response frame for hello).
    common::send_text(&mut ws, r#"{"type":"hello","capabilities":["replay_v1"]}"#).await;

    // Subscribe — confirms the connection is in Active state and commands work.
    common::send_text(
        &mut ws,
        r#"{"command":"subscribe","identifier":"handshake_test"}"#,
    )
    .await;

    let confirm = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for ConfirmSubscription");
    let v: serde_json::Value = serde_json::from_str(&confirm).expect("valid JSON");
    assert_eq!(v["type"], "confirm_subscription");
    assert_eq!(v["identifier"], "handshake_test");
}

/// MessagePack codec: connecting with `turbocable-v1-msgpack` sub-protocol
/// and verifying the Welcome frame arrives as a binary (MessagePack) message.
#[tokio::test]
async fn msgpack_welcome_on_connect() {
    let handle = turbocable_server::server::start(common::test_config(common::NO_NATS_URL)).await;

    let mut ws = common::connect_ws_subprotocol(handle.addr, "turbocable-v1-msgpack").await;

    // Server sends Welcome as a binary MessagePack frame.
    let bytes = timeout(Duration::from_secs(5), common::recv_binary(&mut ws))
        .await
        .expect("timed out waiting for binary Welcome");

    // Decode and verify the payload.
    let v: serde_json::Value = rmp_serde::from_slice(&bytes).expect("valid MessagePack Welcome");
    assert_eq!(
        v["type"], "welcome",
        "expected welcome in msgpack frame: {v}"
    );
}

/// MessagePack codec: the full subscribe handshake (Hello → subscribe →
/// ConfirmSubscription) works over the binary sub-protocol.
#[tokio::test]
async fn msgpack_subscribe_confirm() {
    let handle = turbocable_server::server::start(common::test_config(common::NO_NATS_URL)).await;

    let mut ws = common::connect_ws_subprotocol(handle.addr, "turbocable-v1-msgpack").await;

    // Read binary Welcome.
    let _welcome = timeout(Duration::from_secs(5), common::recv_binary(&mut ws))
        .await
        .expect("timed out waiting for Welcome");

    // Send Hello as MessagePack binary.
    #[derive(serde::Serialize)]
    struct Hello<'a> {
        #[serde(rename = "type")]
        hello_type: &'a str,
        capabilities: Vec<&'a str>,
    }
    let hello = Hello {
        hello_type: "hello",
        capabilities: vec!["replay_v1"],
    };
    let hello_bytes = rmp_serde::to_vec_named(&hello).expect("encode hello");
    use futures::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    ws.send(Message::Binary(hello_bytes))
        .await
        .expect("send hello");

    // Send Subscribe as MessagePack binary.
    #[derive(serde::Serialize)]
    struct Subscribe<'a> {
        command: &'a str,
        identifier: &'a str,
    }
    let sub = Subscribe {
        command: "subscribe",
        identifier: "msgpack_test",
    };
    let sub_bytes = rmp_serde::to_vec_named(&sub).expect("encode subscribe");
    ws.send(Message::Binary(sub_bytes))
        .await
        .expect("send subscribe");

    // Expect binary ConfirmSubscription.
    let confirm_bytes = timeout(Duration::from_secs(5), common::recv_binary(&mut ws))
        .await
        .expect("timed out waiting for ConfirmSubscription");
    let v: serde_json::Value =
        rmp_serde::from_slice(&confirm_bytes).expect("valid MessagePack response");
    assert_eq!(v["type"], "confirm_subscription");
    assert_eq!(v["identifier"], "msgpack_test");
}
