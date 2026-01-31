//! This module contains utility methods and structs for handling time.
//!
//! The [SystemClock] struct is used to measure wall-clock time. We've chosen to
//! implement our own [SystemClock] so we can mock it for testing purposes. Mocks
//! are available when the `test-util` feature is enabled.
//!
//! [DefaultSystemClock] is provided as well. It implements a system clock that
//! uses Tokio's clock to measure time duration.

#![allow(clippy::disallowed_methods)]

use chrono::{DateTime, Utc};
use std::{fmt::Debug, future::Future, pin::Pin, time::Duration};

#[cfg(feature = "test-util")]
use std::sync::atomic::{AtomicI64, Ordering};

/// Defines the physical clock used to measure wall-clock time.
pub trait SystemClock: Debug + Send + Sync {
    /// Returns the current time
    fn now(&self) -> DateTime<Utc>;
    /// Advances the clock by the specified duration
    #[cfg(feature = "test-util")]
    fn advance<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    /// Sleeps for the specified duration
    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    /// Returns a ticker that emits a signal every `duration` interval
    fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a>;
}

/// A ticker that emits a signal every `duration` interval. This allows us to use our
/// clock to control ticking.
///
/// The first tick will complete immediately. Subsequent ticks will complete after the
/// specified duration has elapsed. This is to mimic Tokio's ticker behavior.
pub struct SystemClockTicker<'a> {
    clock: &'a dyn SystemClock,
    duration: Duration,
    last_tick: DateTime<Utc>,
}

impl<'a> SystemClockTicker<'a> {
    /// Creates a new ticker that emits a signal every `duration` interval.
    pub fn new(clock: &'a dyn SystemClock, duration: Duration) -> Self {
        Self {
            clock,
            duration,
            last_tick: DateTime::<Utc>::MIN_UTC,
        }
    }

    /// Returns a future that emits a signal every `duration` interval. The next tick is
    /// calculated as last_tick + duration. The first tick will complete immediately.
    /// This is to mimic Tokio's ticker behavior.
    ///
    /// If the clock advances more than the duration between `tick()` calls, the ticker
    /// will tick immediately.
    pub fn tick(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let sleep_duration = self.calc_duration();
            self.clock.sleep(sleep_duration).await;
            self.last_tick = self.clock.now();
        })
    }

    /// Calculates the duration until the next tick.
    ///
    /// The duration is calculated as `duration - (now - last_tick)`.
    fn calc_duration(&self) -> Duration {
        let zero = Duration::from_millis(0);
        let now_dt = self.clock.now();
        let elapsed = now_dt
            .signed_duration_since(self.last_tick)
            .to_std()
            .expect("elapsed time is negative");
        // If we've already passed the next tick, sleep for 0ms to tick immediately.
        self.duration.checked_sub(elapsed).unwrap_or(zero)
    }
}

/// A system clock implementation that uses tokio::time::Instant to measure time duration.
/// Utc::now() is used to track the initial timestamp (ms since Unix epoch). This DateTime
/// is used to convert the tokio::time::Instant to a DateTime when now() is called.
///
/// Note that, because we're using tokio::time::Instant, manipulating tokio's clock with
/// tokio::time::pause(), tokio::time::advance(), and so on will affect the
/// DefaultSystemClock's time as well.
#[derive(Debug)]
pub struct DefaultSystemClock {
    initial_ts: DateTime<Utc>,
    initial_instant: tokio::time::Instant,
}

impl DefaultSystemClock {
    pub fn new() -> Self {
        Self {
            initial_ts: Utc::now(),
            initial_instant: tokio::time::Instant::now(),
        }
    }
}

impl Default for DefaultSystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemClock for DefaultSystemClock {
    fn now(&self) -> DateTime<Utc> {
        let elapsed = tokio::time::Instant::now().duration_since(self.initial_instant);
        self.initial_ts + elapsed
    }

    #[cfg(feature = "test-util")]
    fn advance<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::advance(duration))
    }

    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
        SystemClockTicker::new(self, duration)
    }
}

/// A mock system clock implementation that uses an atomic i64 to track time.
/// The clock always starts at 0 (the Unix epoch). Time only advances when the
/// `advance` method is called.
#[derive(Debug)]
#[cfg(feature = "test-util")]
pub struct MockSystemClock {
    /// The current timestamp in milliseconds since the Unix epoch.
    /// Can be negative to represent a time before the epoch.
    current_ts: AtomicI64,
}

#[cfg(feature = "test-util")]
impl Default for MockSystemClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "test-util")]
impl MockSystemClock {
    pub fn new() -> Self {
        Self::with_time(0)
    }

    /// Creates a new mock system clock with the specified timestamp
    pub fn with_time(ts_millis: i64) -> Self {
        Self {
            current_ts: AtomicI64::new(ts_millis),
        }
    }

    /// Sets the current timestamp of the mock system clock
    pub fn set(&self, ts_millis: i64) {
        self.current_ts.store(ts_millis, Ordering::SeqCst);
    }
}

#[cfg(feature = "test-util")]
impl SystemClock for MockSystemClock {
    #[allow(clippy::panic)]
    fn now(&self) -> DateTime<Utc> {
        let current_ts = self.current_ts.load(Ordering::SeqCst);
        DateTime::from_timestamp_millis(current_ts)
            .unwrap_or_else(|| panic!("invalid timestamp: {}", current_ts))
    }

    fn advance<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        self.current_ts
            .fetch_add(duration.as_millis() as i64, Ordering::SeqCst);
        Box::pin(async move {
            // An empty async block always returns Poll::Ready(()) because nothing inside
            // the block can yield control to other tasks. Calling advance() in a tight loop
            // would prevent other tasks from running in this case. Yielding control to other
            // tasks explicitly so we avoid this issue.
            tokio::task::yield_now().await;
        })
    }

    /// Sleeps for the specified duration. Note that sleep() does not advance the clock.
    /// Another thread or task must call advance() to advance the clock to unblock the sleep.
    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let end_time = self.current_ts.load(Ordering::SeqCst) + duration.as_millis() as i64;
        Box::pin(async move {
            #[allow(clippy::while_immutable_condition)]
            while self.current_ts.load(Ordering::SeqCst) < end_time {
                tokio::task::yield_now().await;
            }
        })
    }

    fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a> {
        SystemClockTicker::new(self, duration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_default() {
        let clock = MockSystemClock::default();
        assert_eq!(
            clock.now().timestamp_millis(),
            0,
            "Default MockSystemClock should start at timestamp 0"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_set_now() {
        let clock = std::sync::Arc::new(MockSystemClock::new());

        // Test positive timestamp
        let positive_ts = 1625097600000i64; // 2021-07-01T00:00:00Z in milliseconds
        clock.set(positive_ts);
        assert_eq!(
            clock.now().timestamp_millis(),
            positive_ts,
            "MockSystemClock should return the timestamp set with set_now"
        );

        // Test negative timestamp (before Unix epoch)
        let negative_ts = -1625097600000; // Before Unix epoch
        clock.set(negative_ts);
        assert_eq!(
            clock.now().timestamp_millis(),
            negative_ts,
            "MockSystemClock should handle negative timestamps correctly"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_advance() {
        let clock = std::sync::Arc::new(MockSystemClock::new());
        let initial_ts = 1000;

        // Set initial time
        clock.clone().set(initial_ts);

        // Advance by 500ms
        let duration = Duration::from_millis(500);
        clock.clone().advance(duration).await;

        // Check that time advanced correctly
        assert_eq!(
            clock.now().timestamp_millis(),
            initial_ts + 500,
            "MockSystemClock should advance time by the specified duration"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_sleep() {
        let clock = std::sync::Arc::new(MockSystemClock::new());
        let initial_ts = 2000;

        // Set initial time
        clock.clone().set(initial_ts);

        // Start sleep for 1000ms
        let sleep_duration = Duration::from_millis(1000);
        let sleep_handle1 = clock.sleep(sleep_duration);
        let sleep_handle2 = clock.sleep(sleep_duration);
        let sleep_handle3 = clock.sleep(sleep_duration);

        // Verify sleep doesn't complete immediately
        assert!(
            timeout(Duration::from_millis(10), sleep_handle1)
                .await
                .is_err(),
            "Sleep should not complete until time advances"
        );

        // Advance clock by 500ms (not enough to complete sleep)
        clock.set(initial_ts + 500);
        assert!(
            timeout(Duration::from_millis(10), sleep_handle2)
                .await
                .is_err(),
            "Sleep should not complete when time has advanced by less than sleep duration"
        );

        // Advance clock by enough to complete sleep
        clock.set(initial_ts + 1000);
        assert!(
            timeout(Duration::from_millis(100), sleep_handle3)
                .await
                .is_ok(),
            "Sleep should complete when time has advanced by at least sleep duration"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_ticker() {
        let clock = std::sync::Arc::new(MockSystemClock::new());
        let tick_duration = Duration::from_millis(100);

        // Create a ticker
        let mut ticker = clock.ticker(tick_duration);

        // First tick should complete immediately
        assert!(
            timeout(Duration::from_millis(10000), ticker.tick())
                .await
                .is_ok(),
            "First tick should complete immediately"
        );

        // Next tick should not complete because time hasn't advanced
        assert!(
            timeout(Duration::from_millis(100), ticker.tick())
                .await
                .is_err(),
            "Second tick should not complete until time advances"
        );

        // The the ticker future before we advance the clock so it's end time is
        // now + 100. Then advance the clock by 100ms and verify the tick
        // completes.
        clock.clone().set(100);
        assert!(
            timeout(Duration::from_millis(10000), ticker.tick())
                .await
                .is_ok(),
            "Tick should complete when time has advanced by at least tick duration"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_default_system_clock_now() {
        let clock = std::sync::Arc::new(DefaultSystemClock::new());

        // Record initial time
        let initial_now = clock.now();

        // Sleep a bit
        let sleep_duration = Duration::from_millis(100);
        clock.clone().sleep(sleep_duration).await;

        // Check that time advances
        let new_now = clock.clone().now();
        assert_eq!(
            new_now,
            initial_now + sleep_duration,
            "DefaultSystemClock now() should advance with time"
        );
    }

    #[tokio::test(start_paused = true)]
    #[cfg(feature = "test-util")]
    async fn test_default_system_clock_advance() {
        let clock = std::sync::Arc::new(DefaultSystemClock::new());
        let start = clock.now();
        let duration = Duration::from_millis(500);
        clock.clone().advance(duration).await;

        // Check that time advanced correctly
        assert_eq!(
            start + duration,
            clock.now(),
            "DefaultSystemClock should advance time by the specified duration"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_default_system_clock_ticker() {
        let clock = std::sync::Arc::new(DefaultSystemClock::new());
        let tick_duration = Duration::from_millis(10);

        // Create a ticker
        let mut ticker = clock.ticker(tick_duration);

        // First tick should complete immediately
        assert!(
            timeout(Duration::from_millis(10000), ticker.tick())
                .await
                .is_ok(),
            "First tick should complete immediately"
        );

        // Tokio auto-advances the time on when sleep() is called and only
        // timer futures remain. Calling tick() here will bump the clock by
        // the tick duration.
        let before = clock.now();
        ticker.tick().await;
        let after = clock.now();
        assert_eq!(before + tick_duration, after);
    }
}
