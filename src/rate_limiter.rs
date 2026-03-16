use crate::shutdown::ShutdownToken;
use log::warn;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A thread-safe token bucket rate limiter shared across stage pollers.
///
/// Each `try_acquire` consumes one token. Tokens refill at `refill_rate` per second
/// up to `capacity`. When a 429 response is received, `signal_backoff` pauses all
/// callers for the specified duration.
pub struct RateLimiter {
    inner: Mutex<TokenBucketState>,
}

struct TokenBucketState {
    capacity: f64,
    available: f64,
    refill_rate: f64, // tokens per second
    last_refill: Instant,
    backoff_until: Option<Instant>,
}

impl RateLimiter {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, TokenBucketState> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("rate limiter mutex was poisoned; recovering inner state");
                poisoned.into_inner()
            }
        }
    }

    pub fn new(requests_per_minute: u32) -> Self {
        let rate = requests_per_minute as f64 / 60.0;
        let capacity = requests_per_minute as f64;
        Self {
            inner: Mutex::new(TokenBucketState {
                capacity,
                available: capacity,
                refill_rate: rate,
                last_refill: Instant::now(),
                backoff_until: None,
            }),
        }
    }

    /// Try to acquire a single token without blocking.
    ///
    /// Returns `Ok(())` if a token was acquired.
    /// Returns `Err(duration)` with the estimated wait time if no token is available
    /// or a backoff is active.
    pub fn try_acquire(&self) -> Result<(), Duration> {
        let mut state = self.lock_state();
        let now = Instant::now();

        // Check backoff
        if let Some(until) = state.backoff_until {
            if now < until {
                return Err(until - now);
            }
            state.backoff_until = None;
        }

        // Refill tokens
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        state.available = (state.available + elapsed * state.refill_rate).min(state.capacity);
        state.last_refill = now;

        if state.available >= 1.0 {
            state.available -= 1.0;
            Ok(())
        } else {
            let deficit = 1.0 - state.available;
            let wait_secs = deficit / state.refill_rate;
            Err(Duration::from_secs_f64(wait_secs))
        }
    }

    /// Block until a token is available or shutdown is signaled.
    ///
    /// Sleeps in short intervals (up to 100ms) to stay responsive to shutdown.
    pub fn acquire_blocking(&self, shutdown: &ShutdownToken) {
        loop {
            if shutdown.is_shutdown() {
                return;
            }
            match self.try_acquire() {
                Ok(()) => return,
                Err(wait) => {
                    let sleep_time = wait.min(Duration::from_millis(100));
                    std::thread::sleep(sleep_time);
                }
            }
        }
    }

    /// Signal a provider backoff (e.g. from a 429 response).
    ///
    /// All callers will be paused until the backoff expires.
    pub fn signal_backoff(&self, duration: Duration) {
        let mut state = self.lock_state();
        let until = Instant::now() + duration;
        // Only extend, never shorten an active backoff
        match state.backoff_until {
            Some(existing) if existing > until => {}
            _ => state.backoff_until = Some(until),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_within_capacity() {
        let limiter = RateLimiter::new(60); // 1 per second, capacity 60
                                            // Should be able to acquire up to capacity without waiting
        for _ in 0..60 {
            assert!(limiter.try_acquire().is_ok());
        }
        // Next should fail
        assert!(limiter.try_acquire().is_err());
    }

    #[test]
    fn refill_after_wait() {
        let limiter = RateLimiter::new(60);
        // Drain all tokens
        for _ in 0..60 {
            let _ = limiter.try_acquire();
        }
        assert!(limiter.try_acquire().is_err());

        // Wait enough for ~1 token to refill (1 per second at rate 60/min)
        std::thread::sleep(Duration::from_millis(1100));
        assert!(limiter.try_acquire().is_ok());
    }

    #[test]
    fn backoff_pauses_acquisition() {
        let limiter = RateLimiter::new(60);
        limiter.signal_backoff(Duration::from_millis(200));

        // Should fail during backoff
        let result = limiter.try_acquire();
        assert!(result.is_err());

        // Wait for backoff to expire
        std::thread::sleep(Duration::from_millis(250));
        assert!(limiter.try_acquire().is_ok());
    }

    #[test]
    fn backoff_only_extends() {
        let limiter = RateLimiter::new(60);
        limiter.signal_backoff(Duration::from_millis(500));
        // Shorter backoff should not override
        limiter.signal_backoff(Duration::from_millis(100));

        std::thread::sleep(Duration::from_millis(150));
        // Should still be in backoff (500ms, not 100ms)
        assert!(limiter.try_acquire().is_err());
    }

    #[test]
    fn acquire_blocking_respects_shutdown() {
        let limiter = RateLimiter::new(1); // very slow refill
                                           // Drain
        let _ = limiter.try_acquire();

        let shutdown = ShutdownToken::new();
        let shutdown_clone = shutdown.clone();

        let handle = std::thread::spawn(move || {
            limiter.acquire_blocking(&shutdown_clone);
        });

        // Signal shutdown quickly
        std::thread::sleep(Duration::from_millis(50));
        shutdown.signal();
        handle.join().unwrap();
    }
}
