//! Per-IP connection rate limiting to prevent resource exhaustion.

use std::net::IpAddr;

use dashmap::DashMap;

/// Per-IP connection limiter.
///
/// Prevents a single client from exhausting gateway capacity.
/// Uses `DashMap` entry-level locking for atomic check-and-increment.
pub struct ConnectionLimiter {
    counts: DashMap<IpAddr, u64>,
    max_per_ip: u64,
}

impl ConnectionLimiter {
    /// Creates a limiter that allows up to `max_per_ip` concurrent connections per IP.
    pub fn new(max_per_ip: u64) -> Self {
        Self {
            counts: DashMap::new(),
            max_per_ip,
        }
    }

    /// Attempt to acquire a connection slot for the given IP.
    /// Returns `true` if the connection is allowed, `false` if the limit is reached.
    pub fn try_acquire(&self, ip: IpAddr) -> bool {
        let mut entry = self.counts.entry(ip).or_insert(0);
        if *entry >= self.max_per_ip {
            return false;
        }
        *entry += 1;
        true
    }

    /// Release a connection slot for the given IP.
    /// Must be called exactly once per successful `try_acquire`.
    pub fn release(&self, ip: IpAddr) {
        let should_remove = if let Some(mut entry) = self.counts.get_mut(&ip) {
            *entry = entry.saturating_sub(1);
            *entry == 0
        } else {
            false
        };

        if should_remove {
            self.counts.remove_if(&ip, |_, v| *v == 0);
        }
    }

    #[cfg(test)]
    pub fn count_for(&self, ip: IpAddr) -> u64 {
        self.counts.get(&ip).map(|v| *v).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn localhost() -> IpAddr {
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    }

    fn other_ip() -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))
    }

    #[test]
    fn allows_up_to_limit() {
        let limiter = ConnectionLimiter::new(3);
        assert!(limiter.try_acquire(localhost()));
        assert!(limiter.try_acquire(localhost()));
        assert!(limiter.try_acquire(localhost()));
        assert!(!limiter.try_acquire(localhost()));
    }

    #[test]
    fn different_ips_are_independent() {
        let limiter = ConnectionLimiter::new(2);
        assert!(limiter.try_acquire(localhost()));
        assert!(limiter.try_acquire(localhost()));
        assert!(!limiter.try_acquire(localhost()));

        assert!(limiter.try_acquire(other_ip()));
        assert!(limiter.try_acquire(other_ip()));
        assert!(!limiter.try_acquire(other_ip()));
    }

    #[test]
    fn release_frees_slot() {
        let limiter = ConnectionLimiter::new(1);
        assert!(limiter.try_acquire(localhost()));
        assert!(!limiter.try_acquire(localhost()));

        limiter.release(localhost());
        assert!(limiter.try_acquire(localhost()));
    }

    #[test]
    fn release_cleans_up_zero_entries() {
        let limiter = ConnectionLimiter::new(1);
        assert!(limiter.try_acquire(localhost()));
        assert_eq!(limiter.count_for(localhost()), 1);

        limiter.release(localhost());
        assert_eq!(limiter.count_for(localhost()), 0);
    }

    #[test]
    fn release_without_acquire_does_not_underflow() {
        let limiter = ConnectionLimiter::new(5);
        limiter.release(localhost());
        assert_eq!(limiter.count_for(localhost()), 0);
    }

    #[tokio::test]
    async fn concurrent_acquire_respects_limit() {
        use std::sync::Arc;

        let limiter = Arc::new(ConnectionLimiter::new(50));
        let mut handles = Vec::new();

        for _ in 0..100 {
            let limiter = Arc::clone(&limiter);
            handles.push(tokio::spawn(
                async move { limiter.try_acquire(localhost()) },
            ));
        }

        let results: Vec<bool> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        let accepted = results.iter().filter(|&&v| v).count();
        assert_eq!(accepted, 50);
        assert_eq!(limiter.count_for(localhost()), 50);
    }
}
