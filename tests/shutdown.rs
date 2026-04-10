mod common;

use std::time::Duration;

use tokio::time::timeout;

/// Graceful shutdown: after `shutdown_tx.send(true)` every active WebSocket
/// connection receives a Close frame with code 1001 (Going Away).
#[tokio::test]
async fn shutdown_sends_close_1001() {
    let handle = turbocable_server::server::start(common::test_config(common::NO_NATS_URL)).await;

    let mut ws = common::connect_ws(handle.addr).await;

    // Wait for Welcome — confirms the connection is fully established.
    let welcome = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for Welcome");
    let v: serde_json::Value = serde_json::from_str(&welcome).unwrap();
    assert_eq!(v["type"], "welcome");

    // Trigger graceful shutdown.
    handle.shutdown_tx.send(true).expect("send shutdown signal");

    // The outbound loop sends Close(1001) before exiting.
    let code = timeout(Duration::from_secs(5), common::recv_close_code(&mut ws))
        .await
        .expect("timed out waiting for close frame after shutdown");

    assert_eq!(code, 1001, "expected GoAway (1001), got {code}");
}

/// Multiple connections all receive Close(1001) on shutdown.
#[tokio::test]
async fn shutdown_closes_all_connections() {
    const N: usize = 5;

    let handle = turbocable_server::server::start(common::test_config(common::NO_NATS_URL)).await;

    let mut connections = Vec::with_capacity(N);
    for _ in 0..N {
        let mut ws = common::connect_ws(handle.addr).await;
        // Drain Welcome so the channel is clear before shutdown.
        let _welcome = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
            .await
            .expect("Welcome");
        connections.push(ws);
    }

    // Send shutdown signal.
    handle.shutdown_tx.send(true).expect("send shutdown signal");

    // Every connection should receive Close(1001).
    for (i, ws) in connections.iter_mut().enumerate() {
        let code = timeout(Duration::from_secs(5), common::recv_close_code(ws))
            .await
            .expect("timed out waiting for close on connection {i}");
        assert_eq!(code, 1001, "connection {i}: expected 1001, got {code}");
    }
}

/// New connections are still accepted right up until the shutdown signal is
/// sent — the signal only closes already-open connections.
///
/// (This tests that `start()` does not pre-emptively block the listener.)
#[tokio::test]
async fn connections_accepted_before_shutdown() {
    let handle = turbocable_server::server::start(common::test_config(common::NO_NATS_URL)).await;

    // Open a connection and verify it works normally.
    let mut ws = common::connect_ws(handle.addr).await;
    let welcome = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("Welcome");
    let v: serde_json::Value = serde_json::from_str(&welcome).unwrap();
    assert_eq!(v["type"], "welcome");

    // Only NOW trigger shutdown.
    handle.shutdown_tx.send(true).expect("shutdown signal");

    let code = timeout(Duration::from_secs(5), common::recv_close_code(&mut ws))
        .await
        .expect("close after shutdown");
    assert_eq!(code, 1001);
}
