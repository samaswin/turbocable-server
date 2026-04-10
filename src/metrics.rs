//! Prometheus metrics for connection counts, fan-out latency, and NATS consumer lag.

use std::sync::{Arc, OnceLock};

use prometheus::{
    core::{AtomicI64, AtomicU64, GenericCounter, GenericGauge},
    Histogram, HistogramOpts, TextEncoder,
};

/// Process-wide singleton so integration tests (multiple `start()` calls in one
/// process) don't collide on the global Prometheus registry.
static METRICS_INSTANCE: OnceLock<Arc<Metrics>> = OnceLock::new();

/// All Prometheus metrics exported by the gateway.
pub struct Metrics {
    /// Current number of open WebSocket connections.
    pub connections_active: GenericGauge<AtomicI64>,
    /// Total WebSocket connections accepted since startup.
    pub connections_total: GenericCounter<AtomicU64>,
    /// Client `hello` frames with `last_seq > 0` (resume after prior delivery).
    /// Use `rate(...[5m])` as a reconnect-resume signal at the application layer.
    pub client_reconnect_handshake_total: GenericCounter<AtomicU64>,
    /// Connections rejected due to auth failure or per-IP limit.
    pub connections_rejected: GenericCounter<AtomicU64>,
    /// Total WebSocket frames delivered to clients via fan-out.
    pub messages_fanned_out: GenericCounter<AtomicU64>,
    /// Fan-out latency histogram in seconds (p50/p95/p99).
    pub fanout_duration_secs: Histogram,
    /// NATS JetStream consumer pending message count.
    pub nats_consumer_lag: GenericGauge<AtomicI64>,
    /// JWT verification latency histogram in seconds.
    pub auth_duration_secs: Histogram,

    // --- Handshake / capability metrics (Phase 1) ---
    /// Clients that completed a valid hello handshake and advertised `replay_v1`.
    pub handshake_ok_replay_capable_total: GenericCounter<AtomicU64>,
    /// Clients that completed a valid hello handshake without `replay_v1` capability.
    pub handshake_ok_legacy_total: GenericCounter<AtomicU64>,
    /// Commands (subscribe, unsubscribe, message) received before hello in compat mode (warning path).
    /// A non-zero rate signals legacy clients still in the fleet.
    pub handshake_legacy_warn_total: GenericCounter<AtomicU64>,
    /// Commands rejected because the client had not yet sent a valid hello (enforce mode).
    pub handshake_rejected_total: GenericCounter<AtomicU64>,
    /// Subscribe commands rejected in `hard_enforce`: hello lacked `replay_v1`.
    pub handshake_rejected_non_replay_capable_total: GenericCounter<AtomicU64>,
    /// Subscribes allowed in `soft_enforce` without `replay_v1` (migration visibility).
    pub handshake_soft_non_replay_subscribe_allowed_total: GenericCounter<AtomicU64>,

    // --- Backpressure eviction metrics (Phase 2) ---
    /// Connections force-disconnected because their outbound channel was full.
    /// These clients receive a `disconnect` frame with `reconnect=true` and are
    /// expected to reconnect and replay from their last sequence.
    pub forced_reconnect_backpressure_total: GenericCounter<AtomicU64>,

    // --- Replay metrics (Phase 3) ---
    /// Replay tasks that completed without error.
    pub replay_success_total: GenericCounter<AtomicU64>,
    /// Replay tasks that failed with a transient JetStream error.
    pub replay_failure_total: GenericCounter<AtomicU64>,
    /// Replay tasks aborted because the client's `last_seq` is outside the retention window.
    pub replay_window_exceeded_total: GenericCounter<AtomicU64>,
    /// Replay stopped early because the peer's outbound path closed during catch-up.
    pub replay_aborted_peer_gone_total: GenericCounter<AtomicU64>,
    /// Replay hit the per-stream cap (`MAX_REPLAY_MESSAGES`); client should full-resync.
    pub replay_truncated_total: GenericCounter<AtomicU64>,
    /// Number of active replay tasks at this moment.
    pub replay_in_flight: GenericGauge<AtomicI64>,

    // --- Replay latency histograms (Phase 4) ---
    /// Total wall-clock time from replay task start to last message delivered (or error).
    /// Measures overall catch-up duration; p95/p99 must stay within the sub-50ms SLO budget.
    pub replay_catch_up_duration_secs: Histogram,
    /// Wall-clock time from replay task start to the first message successfully enqueued.
    /// Serves as the server-side proxy for post-reconnect first-delivery latency.
    pub replay_first_delivery_secs: Histogram,
}

impl Metrics {
    /// Returns the process-wide metrics handle, registering with the default
    /// Prometheus registry on the first call and returning the cached handle
    /// on subsequent calls.
    ///
    /// Using a singleton avoids double-registration panics when the gateway is
    /// started more than once in the same process (e.g. integration tests).
    pub fn new() -> Arc<Self> {
        METRICS_INSTANCE.get_or_init(Self::create).clone()
    }

    fn create() -> Arc<Self> {
        let connections_active = prometheus::register_int_gauge!(
            "turbocable_connections_active",
            "Current number of open WebSocket connections"
        )
        .expect("metric registration failed");

        let connections_total = prometheus::register_int_counter!(
            "turbocable_connections_total",
            "Total WebSocket connections accepted since startup"
        )
        .expect("metric registration failed");

        let client_reconnect_handshake_total = prometheus::register_int_counter!(
            "turbocable_client_reconnect_handshake_total",
            "Client hello frames with last_seq > 0 (resume / reconnect with delivery history)"
        )
        .expect("metric registration failed");

        let connections_rejected = prometheus::register_int_counter!(
            "turbocable_connections_rejected_total",
            "Connections rejected due to auth failure or per-IP limit"
        )
        .expect("metric registration failed");

        let messages_fanned_out = prometheus::register_int_counter!(
            "turbocable_messages_fanned_out_total",
            "Total WebSocket frames delivered to clients via fan-out"
        )
        .expect("metric registration failed");

        let fanout_duration_secs = prometheus::register_histogram!(HistogramOpts::new(
            "turbocable_fanout_duration_seconds",
            "Fan-out latency in seconds"
        )
        .buckets(vec![
            0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
        ]))
        .expect("metric registration failed");

        let nats_consumer_lag = prometheus::register_int_gauge!(
            "turbocable_nats_consumer_lag",
            "NATS JetStream consumer pending message count"
        )
        .expect("metric registration failed");

        let auth_duration_secs = prometheus::register_histogram!(HistogramOpts::new(
            "turbocable_auth_duration_seconds",
            "JWT verification latency in seconds"
        )
        .buckets(vec![
            0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
        ]))
        .expect("metric registration failed");

        let handshake_ok_replay_capable_total = prometheus::register_int_counter!(
            "turbocable_handshake_ok_replay_capable_total",
            "Clients that completed hello handshake with replay_v1 capability"
        )
        .expect("metric registration failed");

        let handshake_ok_legacy_total = prometheus::register_int_counter!(
            "turbocable_handshake_ok_legacy_total",
            "Clients that completed hello handshake without replay_v1 capability"
        )
        .expect("metric registration failed");

        let handshake_legacy_warn_total = prometheus::register_int_counter!(
            "turbocable_handshake_legacy_warn_total",
            "Commands received before hello in compat mode (legacy client signal)"
        )
        .expect("metric registration failed");

        let handshake_rejected_total = prometheus::register_int_counter!(
            "turbocable_handshake_rejected_total",
            "Commands rejected because client had not sent hello (enforce mode)"
        )
        .expect("metric registration failed");

        let handshake_rejected_non_replay_capable_total = prometheus::register_int_counter!(
            "turbocable_handshake_rejected_non_replay_capable_total",
            "Subscribe rejected in hard_enforce: client not replay_v1-capable"
        )
        .expect("metric registration failed");

        let handshake_soft_non_replay_subscribe_allowed_total = prometheus::register_int_counter!(
            "turbocable_handshake_soft_non_replay_subscribe_allowed_total",
            "Subscribe allowed in soft_enforce without replay_v1 (legacy client signal)"
        )
        .expect("metric registration failed");

        let forced_reconnect_backpressure_total = prometheus::register_int_counter!(
            "turbocable_forced_reconnect_backpressure_total",
            "Connections evicted due to full outbound channel (backpressure reconnect)"
        )
        .expect("metric registration failed");

        let replay_success_total = prometheus::register_int_counter!(
            "turbocable_replay_success_total",
            "Replay tasks that completed without error"
        )
        .expect("metric registration failed");

        let replay_failure_total = prometheus::register_int_counter!(
            "turbocable_replay_failure_total",
            "Replay tasks that failed with a transient JetStream error"
        )
        .expect("metric registration failed");

        let replay_window_exceeded_total = prometheus::register_int_counter!(
            "turbocable_replay_window_exceeded_total",
            "Replay tasks aborted due to client last_seq outside the retention window"
        )
        .expect("metric registration failed");

        let replay_aborted_peer_gone_total = prometheus::register_int_counter!(
            "turbocable_replay_aborted_peer_gone_total",
            "Replay tasks stopped because the WebSocket outbound channel closed mid-replay"
        )
        .expect("metric registration failed");

        let replay_truncated_total = prometheus::register_int_counter!(
            "turbocable_replay_truncated_total",
            "Replay tasks that hit the per-stream message cap and required client resync"
        )
        .expect("metric registration failed");

        let replay_in_flight = prometheus::register_int_gauge!(
            "turbocable_replay_in_flight",
            "Number of active replay tasks"
        )
        .expect("metric registration failed");

        let replay_catch_up_duration_secs = prometheus::register_histogram!(HistogramOpts::new(
            "turbocable_replay_catch_up_duration_seconds",
            "Total wall-clock time for a replay task from start to completion"
        )
        .buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0,]))
        .expect("metric registration failed");

        let replay_first_delivery_secs = prometheus::register_histogram!(HistogramOpts::new(
            "turbocable_replay_first_delivery_seconds",
            "Time from replay start to first message enqueued in the outbound channel"
        )
        .buckets(vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0,
        ]))
        .expect("metric registration failed");

        Arc::new(Self {
            connections_active,
            connections_total,
            client_reconnect_handshake_total,
            connections_rejected,
            messages_fanned_out,
            fanout_duration_secs,
            nats_consumer_lag,
            auth_duration_secs,
            handshake_ok_replay_capable_total,
            handshake_ok_legacy_total,
            handshake_legacy_warn_total,
            handshake_rejected_total,
            handshake_rejected_non_replay_capable_total,
            handshake_soft_non_replay_subscribe_allowed_total,
            forced_reconnect_backpressure_total,
            replay_success_total,
            replay_failure_total,
            replay_window_exceeded_total,
            replay_aborted_peer_gone_total,
            replay_truncated_total,
            replay_in_flight,
            replay_catch_up_duration_secs,
            replay_first_delivery_secs,
        })
    }

    /// Encodes the default Prometheus registry as Prometheus text format.
    pub fn render() -> String {
        let encoder = TextEncoder::new();
        let mf = prometheus::gather();
        encoder.encode_to_string(&mf).unwrap_or_default()
    }
}
