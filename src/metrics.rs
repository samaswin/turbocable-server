//! Prometheus metrics for connection counts, fan-out latency, and NATS consumer lag.

use std::sync::Arc;

use prometheus::{
    core::{AtomicI64, AtomicU64, GenericCounter, GenericGauge},
    Histogram, HistogramOpts, TextEncoder,
};

/// All Prometheus metrics exported by the gateway.
pub struct Metrics {
    /// Current number of open WebSocket connections.
    pub connections_active: GenericGauge<AtomicI64>,
    /// Total WebSocket connections accepted since startup.
    pub connections_total: GenericCounter<AtomicU64>,
    /// Connections rejected due to auth failure or per-IP limit.
    pub connections_rejected: GenericCounter<AtomicU64>,
    /// Total WebSocket frames delivered to clients via fan-out.
    pub messages_fanned_out: GenericCounter<AtomicU64>,
    /// Messages dropped due to slow client back-pressure.
    pub messages_dropped: GenericCounter<AtomicU64>,
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
    /// Clients that sent a subscribe command before hello in compat mode (warning path).
    /// A non-zero rate signals legacy clients still in the fleet.
    pub handshake_legacy_warn_total: GenericCounter<AtomicU64>,
    /// Commands rejected because the client had not yet sent a valid hello (enforce mode).
    pub handshake_rejected_total: GenericCounter<AtomicU64>,
}

impl Metrics {
    /// Registers all metrics with the default Prometheus registry and returns the handle.
    ///
    /// Panics if any metric name is already registered (should only be called once).
    pub fn new() -> Arc<Self> {
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

        let messages_dropped = prometheus::register_int_counter!(
            "turbocable_messages_dropped_total",
            "Messages dropped due to slow client back-pressure"
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
            "Subscribe-before-hello connections allowed in compat mode (legacy client signal)"
        )
        .expect("metric registration failed");

        let handshake_rejected_total = prometheus::register_int_counter!(
            "turbocable_handshake_rejected_total",
            "Commands rejected because client had not sent hello (enforce mode)"
        )
        .expect("metric registration failed");

        Arc::new(Self {
            connections_active,
            connections_total,
            connections_rejected,
            messages_fanned_out,
            messages_dropped,
            fanout_duration_secs,
            nats_consumer_lag,
            auth_duration_secs,
            handshake_ok_replay_capable_total,
            handshake_ok_legacy_total,
            handshake_legacy_warn_total,
            handshake_rejected_total,
        })
    }

    /// Encodes the default Prometheus registry as Prometheus text format.
    pub fn render() -> String {
        let encoder = TextEncoder::new();
        let mf = prometheus::gather();
        encoder.encode_to_string(&mf).unwrap_or_default()
    }
}
