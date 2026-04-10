mod common;

use std::time::Duration;

use tokio::time::timeout;

/// WebSocket close code sent by the server when JWT verification fails.
const WS_CLOSE_AUTH_FAILED: u16 = 3000;

/// Connecting without a token when JWT auth is enabled → close 3000.
#[tokio::test]
async fn no_token_rejected_with_3000() {
    let key_path = common::write_test_pubkey().await;

    let mut cfg = common::test_config(common::NO_NATS_URL);
    cfg.jwt_public_key_path = Some(key_path.to_string_lossy().into_owned());

    let handle = turbocable_server::server::start(cfg).await;

    let mut ws = common::connect_ws(handle.addr).await;

    let code = timeout(Duration::from_secs(5), common::recv_close_code(&mut ws))
        .await
        .expect("timed out waiting for close frame");

    assert_eq!(
        code, WS_CLOSE_AUTH_FAILED,
        "expected 3000 for missing token, got {code}"
    );
}

/// Connecting with an expired JWT → close 3000.
#[tokio::test]
async fn expired_token_rejected_with_3000() {
    let key_path = common::write_test_pubkey().await;

    let mut cfg = common::test_config(common::NO_NATS_URL);
    cfg.jwt_public_key_path = Some(key_path.to_string_lossy().into_owned());

    let handle = turbocable_server::server::start(cfg).await;

    let expired = common::make_expired_jwt("user_42");
    let mut ws = common::connect_ws_token(handle.addr, &expired).await;

    let code = timeout(Duration::from_secs(5), common::recv_close_code(&mut ws))
        .await
        .expect("timed out waiting for close frame");

    assert_eq!(
        code, WS_CLOSE_AUTH_FAILED,
        "expected 3000 for expired token, got {code}"
    );
}

/// Connecting with a valid JWT → Welcome is delivered; connection stays open.
#[tokio::test]
async fn valid_token_accepted() {
    let key_path = common::write_test_pubkey().await;

    let mut cfg = common::test_config(common::NO_NATS_URL);
    cfg.jwt_public_key_path = Some(key_path.to_string_lossy().into_owned());

    let handle = turbocable_server::server::start(cfg).await;

    let token = common::make_jwt("user_42", &["*"]);
    let mut ws = common::connect_ws_token(handle.addr, &token).await;

    let msg = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for Welcome");
    let v: serde_json::Value = serde_json::from_str(&msg).expect("valid JSON");
    assert_eq!(v["type"], "welcome", "got: {msg}");
}

/// JWT auth disabled (no key configured) → any connection is accepted.
#[tokio::test]
async fn no_auth_config_accepts_all() {
    // No jwt_public_key_path → auth disabled.
    let handle = turbocable_server::server::start(common::test_config(common::NO_NATS_URL)).await;

    let mut ws = common::connect_ws(handle.addr).await;

    let msg = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for Welcome");
    let v: serde_json::Value = serde_json::from_str(&msg).expect("valid JSON");
    assert_eq!(v["type"], "welcome");
}

/// A valid token with restricted stream access cannot subscribe to other streams.
#[tokio::test]
async fn restricted_token_rejects_unauthorised_stream() {
    let key_path = common::write_test_pubkey().await;

    let mut cfg = common::test_config(common::NO_NATS_URL);
    cfg.jwt_public_key_path = Some(key_path.to_string_lossy().into_owned());

    let handle = turbocable_server::server::start(cfg).await;

    // Token only allows "chat_room_*".
    let token = common::make_jwt("user_1", &["chat_room_*"]);
    let mut ws = common::connect_ws_token(handle.addr, &token).await;

    // Read Welcome.
    let _welcome = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("Welcome");

    // Subscribe to an unauthorised stream.
    common::send_text(
        &mut ws,
        r#"{"command":"subscribe","identifier":"admin_panel"}"#,
    )
    .await;

    let resp = timeout(Duration::from_secs(5), common::recv_text(&mut ws))
        .await
        .expect("timed out waiting for rejection");
    let v: serde_json::Value = serde_json::from_str(&resp).expect("valid JSON");
    assert_eq!(
        v["type"], "reject_subscription",
        "expected reject_subscription, got: {resp}"
    );
    assert_eq!(v["identifier"], "admin_panel");
}
