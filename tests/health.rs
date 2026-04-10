mod common;

/// GET /health returns HTTP 200 and a JSON body containing `"status":"ok"`.
#[tokio::test]
async fn health_returns_200() {
    let handle = turbocable_server::server::start(common::test_config(common::NO_NATS_URL)).await;

    let (status, body) = common::http_get(handle.addr, "/health").await;

    assert_eq!(status, 200, "expected 200, got {status}");
    assert!(
        body.contains("\"status\":\"ok\""),
        "body does not contain status:ok — got: {body}"
    );
}
