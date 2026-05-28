use std::{sync::Arc, time::Duration};

use tokio::sync::Mutex;

#[derive(Clone)]
pub(super) struct CoinbaseSqlRateLimiter {
    inner: Arc<Mutex<RateLimitState>>,
    minimum_interval: Duration,
}

impl CoinbaseSqlRateLimiter {
    pub(super) fn new(qps: u32) -> Self {
        let interval_millis = 1_000_u64 / u64::from(qps.max(1));
        Self {
            inner: Arc::new(Mutex::new(RateLimitState::default())),
            minimum_interval: Duration::from_millis(interval_millis.max(1)),
        }
    }

    pub(super) async fn wait(&self) {
        let mut state = self.inner.lock().await;
        let now = tokio::time::Instant::now();
        if let Some(next_allowed_at) = state.next_allowed_at
            && next_allowed_at > now
        {
            tokio::time::sleep_until(next_allowed_at).await;
        }
        state.next_allowed_at = Some(tokio::time::Instant::now() + self.minimum_interval);
    }
}

#[derive(Default)]
struct RateLimitState {
    next_allowed_at: Option<tokio::time::Instant>,
}
