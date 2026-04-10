//! Per-stream token-bucket rate limiter for the NATS fan-out pipeline.
//!
//! ## Design
//!
//! A single [`StreamRateLimiter`] holds a map of per-stream [`BucketState`]
//! entries protected by a single `Mutex`.  This is intentionally coarse:
//! the rate limiter is called once per *NATS message* (not once per *subscriber*),
//! so the lock is held for microseconds and contention is negligible even at
//! tens of thousands of messages per second.
//!
//! ## Token-bucket algorithm
//!
//! Each stream has a "leaky bucket" with capacity equal to the configured burst.
//! On every message arrival:
//! 1. Tokens are refilled proportionally to the elapsed wall-clock time since
//!    the last check (`Δt × rate_per_sec`), capped at `burst`.
//! 2. If at least one token is available, one is consumed and the message is
//!    allowed through.
//! 3. Otherwise the message is dropped and the rate-limited counter increments.
//!
//! ## Configuration
//!
//! Rates are set via [`Config`](crate::config::Config):
//! - `stream_rate_limit_rps`   — default messages/second per stream (0 = disabled).
//! - `stream_rate_limit_burst` — default burst size (0 → 2× rate).
//! - `stream_rate_overrides`   — per-stream overrides in `"name=rps:burst"` format,
//!   semicolon-separated.  Example: `"alerts=100:200;high_volume=5000:10000"`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Token bucket state
// ---------------------------------------------------------------------------

struct BucketState {
    tokens: f64,
    last_refill: Instant,
    rps: f64,
    burst: f64,
}

impl BucketState {
    fn new(rps: f64, burst: f64) -> Self {
        Self {
            tokens: burst, // start full so the first burst can pass immediately
            last_refill: Instant::now(),
            rps,
            burst,
        }
    }

    /// Refills tokens based on elapsed time then tries to consume one.
    ///
    /// Returns `true` if the message should be forwarded, `false` if it
    /// should be dropped.
    fn try_consume(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;

        // Add tokens proportional to elapsed time, cap at burst.
        self.tokens = (self.tokens + elapsed * self.rps).min(self.burst);

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Current token count rounded down (used for gauge sampling).
    fn token_count(&self) -> i64 {
        self.tokens as i64
    }
}

// ---------------------------------------------------------------------------
// Public rate limiter
// ---------------------------------------------------------------------------

/// Per-stream token-bucket rate limiter.
///
/// Thread-safe: internally guarded by a single `Mutex`.  Instantiated once
/// per gateway node and shared (via `Arc`) between the NATS consumer and any
/// future inspection endpoints.
pub struct StreamRateLimiter {
    /// All per-stream buckets, lazily created on first message.
    buckets: Mutex<HashMap<String, BucketState>>,
    /// Default rate in messages/second.
    default_rps: f64,
    /// Default burst capacity.
    default_burst: f64,
    /// Per-stream overrides: stream name → (rps, burst).
    overrides: HashMap<String, (f64, f64)>,
}

impl StreamRateLimiter {
    /// Creates a new limiter with the given defaults and optional per-stream overrides.
    ///
    /// - `default_rps = 0` → rate limiting is **globally disabled** (all streams pass through).
    /// - `default_burst = 0` → burst defaults to `2 × rps` (minimum 1).
    /// - `overrides` entries can override both rate and burst for specific streams; a
    ///   burst of 0 in an override also applies the 2× default rule.
    pub fn new(
        default_rps: u64,
        default_burst: u64,
        overrides: HashMap<String, (u64, u64)>,
    ) -> Self {
        let eff_burst = if default_burst == 0 {
            (default_rps * 2).max(1)
        } else {
            default_burst
        };

        let overrides = overrides
            .into_iter()
            .map(|(name, (rps, burst))| {
                let eff_b = if burst == 0 { (rps * 2).max(1) } else { burst };
                (name, (rps as f64, eff_b as f64))
            })
            .collect();

        Self {
            buckets: Mutex::new(HashMap::new()),
            default_rps: default_rps as f64,
            default_burst: eff_burst as f64,
            overrides,
        }
    }

    /// Returns `true` when rate limiting is fully disabled (default_rps == 0 and
    /// no per-stream overrides with non-zero rps are configured).
    ///
    /// Callers can use this as a fast-path to skip the limiter entirely.
    pub fn is_disabled(&self) -> bool {
        self.default_rps == 0.0 && !self.overrides.values().any(|&(rps, _)| rps > 0.0)
    }

    /// Checks whether a message on `stream` should be forwarded.
    ///
    /// Returns `(allow, current_tokens)`:
    /// - `allow` — `true` if the message passes, `false` if it should be dropped.
    /// - `current_tokens` — snapshot of remaining tokens *after* this call
    ///   (used for the `turbocable_stream_tokens_available` gauge).
    ///
    /// If the stream has no configured limit (`rps == 0`) the function returns
    /// `(true, i64::MAX)` without touching the bucket map.
    pub fn check(&self, stream: &str) -> (bool, i64) {
        let (rps, burst) = self
            .overrides
            .get(stream)
            .copied()
            .unwrap_or((self.default_rps, self.default_burst));

        if rps == 0.0 {
            return (true, i64::MAX);
        }

        let mut buckets = self.buckets.lock().expect("rate limiter mutex poisoned");
        let state = buckets
            .entry(stream.to_owned())
            .or_insert_with(|| BucketState::new(rps, burst));

        let allow = state.try_consume();
        let tokens = state.token_count();
        (allow, tokens)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn limiter(rps: u64, burst: u64) -> StreamRateLimiter {
        StreamRateLimiter::new(rps, burst, HashMap::new())
    }

    #[test]
    fn disabled_when_rps_zero() {
        let rl = limiter(0, 0);
        assert!(rl.is_disabled());
        let (allow, tokens) = rl.check("any_stream");
        assert!(allow);
        assert_eq!(tokens, i64::MAX);
    }

    #[test]
    fn burst_passes_then_limits() {
        // burst=5: first 5 messages pass, 6th is dropped.
        let rl = limiter(1, 5);
        for i in 0..5 {
            let (allow, _) = rl.check("s");
            assert!(allow, "message {i} should pass (burst)");
        }
        let (allow, tokens) = rl.check("s");
        assert!(!allow, "6th message should be rate-limited");
        assert_eq!(tokens, 0);
    }

    #[test]
    fn independent_buckets_per_stream() {
        let rl = limiter(1, 1);
        let (a1, _) = rl.check("stream_a");
        let (b1, _) = rl.check("stream_b");
        assert!(a1, "first msg on stream_a should pass");
        assert!(b1, "first msg on stream_b should pass (separate bucket)");
        // Both streams are now empty.
        let (a2, _) = rl.check("stream_a");
        let (b2, _) = rl.check("stream_b");
        assert!(!a2);
        assert!(!b2);
    }

    #[test]
    fn per_stream_override_takes_precedence() {
        let mut overrides = HashMap::new();
        overrides.insert("special".to_owned(), (10u64, 10u64));
        let rl = StreamRateLimiter::new(1, 1, overrides);

        // "special" should allow 10 messages (override burst=10).
        for i in 0..10 {
            let (allow, _) = rl.check("special");
            assert!(allow, "override stream message {i} should pass");
        }
        let (allow, _) = rl.check("special");
        assert!(!allow, "11th message on special should be limited");

        // Default stream: only 1 allowed.
        let (d1, _) = rl.check("default");
        assert!(d1);
        let (d2, _) = rl.check("default");
        assert!(!d2);
    }

    #[test]
    fn default_burst_is_2x_rps() {
        // rps=5, burst=0 → effective burst should be 10.
        let rl = limiter(5, 0);
        for i in 0..10 {
            let (allow, _) = rl.check("s");
            assert!(allow, "message {i} should pass (2× burst)");
        }
        let (allow, _) = rl.check("s");
        assert!(!allow, "11th should be dropped");
    }

    #[test]
    fn not_disabled_with_override() {
        let mut overrides = HashMap::new();
        overrides.insert("x".to_owned(), (10u64, 10u64));
        let rl = StreamRateLimiter::new(0, 0, overrides);
        // default_rps == 0 but override has rps > 0 → not globally disabled.
        assert!(!rl.is_disabled());
    }
}
