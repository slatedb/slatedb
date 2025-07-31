//! This module contains utility methods and structs for handling time.
//!
//! SlateDB has two concepts of time:
//!
//! 1. The [SystemClock], which is used to measure wall-clock time for things
//!    like garbage collection schedule ticks, compaction schedule ticks, and so
//!    on.
//! 2. The [LogicalClock], which is a monotonically increasing number used to order
//!    writes in the database. This could represent a logical sequence number
//!    (LSN) from a database, a Kafka offset, a `created_at` timestamp
//!    associated with the write, and so on.
//!
//! We've chosen to implement our own [SystemClock] so we can mock it for testing
//! purposes. Mocks are available when the `test-util` feature is enabled.
//!
//! [DefaultSystemClock] and [DefaultLogicalClock] are both provided as well.
//! [DefaultSystemClock] implements a system clock that uses Tokio's clock to measure
//! time duration. [DefaultLogicalClock] implements a logical clock that wraps
//! the [DefaultSystemClock] and returns the number of milliseconds since the
//! Unix epoch.

#![allow(clippy::disallowed_methods)]

use std::{
    cmp,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use crate::error::SlateDBError;
use chrono::{DateTime, Utc};
use log::info;

/// Defines the physical clock that SlateDB will use to measure time for things
/// like garbage collection schedule ticks, compaction schedule ticks, and so on.
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

/// A future returned by SystemClockTicker::tick() that updates the ticker state
/// only when the future completes (not when it's created or cancelled).
pub struct TickFuture<'a> {
    sleep_future: Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
    last_tick_time: &'a mut Option<DateTime<Utc>>,
    next_tick_time: DateTime<Utc>,
}

impl<'a> Future for TickFuture<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.sleep_future.as_mut().poll(cx) {
            Poll::Ready(()) => {
                *self.last_tick_time = Some(self.next_tick_time);
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A ticker that emits a signal every `duration` interval. This allows us to use our
/// clock to control ticking.
///
/// The first tick will complete immediately. Subsequent ticks will complete after the
/// specified duration has elapsed. This is to mimic Tokio's ticker behavior.
pub struct SystemClockTicker<'a> {
    clock: &'a dyn SystemClock,
    duration: Duration,
    last_tick: Option<DateTime<Utc>>,
}

impl<'a> SystemClockTicker<'a> {
    fn new(clock: &'a dyn SystemClock, duration: Duration) -> Self {
        Self {
            clock,
            duration,
            last_tick: None,
        }
    }

    pub fn tick(&mut self) -> TickFuture<'_> {
        let now = self.clock.now();

        // If last_tick hasn't been set yet (first tick future hasn't completed),
        // treat this as if no time has passed and tick immediately
        let last_tick = match self.last_tick {
            Some(time) => time,
            None => {
                return TickFuture {
                    sleep_future: Box::pin(async {}),
                    last_tick_time: &mut self.last_tick,
                    next_tick_time: now,
                };
            }
        };
        // Guard against duration overflow in chrono. This really only happens in
        // tests where we set `duration` to `Duration::MAX`
        let next_tick_time = match last_tick.checked_add_signed(
            chrono::Duration::from_std(self.duration).unwrap_or(chrono::Duration::MAX),
        ) {
            Some(time) => time,
            None => {
                // If we can't add the duration (overflow), tick immediately
                return TickFuture {
                    sleep_future: Box::pin(async {}),
                    last_tick_time: &mut self.last_tick,
                    next_tick_time: now,
                };
            }
        };

        // If we're already past the next tick time, tick immediately
        if now >= next_tick_time {
            return TickFuture {
                sleep_future: Box::pin(async {}),
                last_tick_time: &mut self.last_tick,
                next_tick_time: now,
            };
        }

        // Sleep for the remaining time until next tick
        let sleep_duration = (next_tick_time - now).to_std().unwrap();

        TickFuture {
            sleep_future: self.clock.sleep(sleep_duration),
            last_tick_time: &mut self.last_tick,
            next_tick_time,
        }
    }
}

/// A system clock implementation that uses tokio::time::Instant to measure time duration.
/// Utc::now() is used to track the initial timestamp (ms since Unix epoch). This DateTime
/// is used to convert the tokio::time::Instant to a DateTime when now() is called.
///
/// Note that, becasue we're using tokio::time::Instant, manipulating tokio's clock with
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

/// Defines the logical clock that SlateDB will use to measure time for things
/// like TTL expiration.
pub trait LogicalClock: Debug + Send + Sync {
    /// Returns a timestamp (typically measured in millis since the unix epoch).
    /// Must return monotonically increasing numbers (this is enforced
    /// at runtime and will panic if the invariant is broken).
    ///
    /// Note that this clock does not need to return a number that
    /// represents a unix timestamp; the only requirement is that
    /// it represents a sequence that can attribute a logical ordering
    /// to actions on the database.
    fn now(&self) -> i64;
}

/// A logical clock implementation that wraps the [DefaultSystemClock]
/// and returns the number of milliseconds since the Unix epoch.
#[derive(Debug)]
pub struct DefaultLogicalClock {
    last_ts: AtomicI64,
    inner: Arc<dyn SystemClock>,
}

impl Default for DefaultLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultLogicalClock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DefaultSystemClock::new()),
            last_ts: AtomicI64::new(i64::MIN),
        }
    }
}

impl LogicalClock for DefaultLogicalClock {
    fn now(&self) -> i64 {
        let current_ts = self.inner.now().timestamp_millis();
        self.last_ts.fetch_max(current_ts, Ordering::SeqCst);
        self.last_ts.load(Ordering::SeqCst)
    }
}

/// A mock logical clock implementation that uses an atomic i64 to track time.
/// The clock always starts at i64::MIN and increments by 1 on each call to now().
/// It is fully deterministic.
#[cfg(feature = "test-util")]
#[derive(Debug)]
pub struct MockLogicalClock {
    current_tick: AtomicI64,
}

#[cfg(feature = "test-util")]
impl Default for MockLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "test-util")]
impl MockLogicalClock {
    pub fn new() -> Self {
        Self {
            current_tick: AtomicI64::new(i64::MIN),
        }
    }
}

#[cfg(feature = "test-util")]
impl LogicalClock for MockLogicalClock {
    fn now(&self) -> i64 {
        self.current_tick.fetch_add(1, Ordering::SeqCst)
    }
}

/// SlateDB uses MonotonicClock internally so that it can enforce that clock ticks
/// from the underlying implementation are monotonically increasing.
pub(crate) struct MonotonicClock {
    pub(crate) last_tick: AtomicI64,
    pub(crate) last_durable_tick: AtomicI64,
    delegate: Arc<dyn LogicalClock>,
}

impl MonotonicClock {
    pub(crate) fn new(delegate: Arc<dyn LogicalClock>, init_tick: i64) -> Self {
        Self {
            delegate,
            last_tick: AtomicI64::new(init_tick),
            last_durable_tick: AtomicI64::new(init_tick),
        }
    }

    pub(crate) fn set_last_tick(&self, tick: i64) -> Result<i64, SlateDBError> {
        self.enforce_monotonic(tick)
    }

    pub(crate) fn fetch_max_last_durable_tick(&self, tick: i64) -> i64 {
        self.last_durable_tick.fetch_max(tick, Ordering::SeqCst)
    }

    pub(crate) fn get_last_durable_tick(&self) -> i64 {
        self.last_durable_tick.load(Ordering::SeqCst)
    }

    pub(crate) async fn now(&self) -> Result<i64, SlateDBError> {
        let tick = self.delegate.now();
        match self.enforce_monotonic(tick) {
            Err(SlateDBError::InvalidClockTick {
                last_tick,
                next_tick: _,
            }) => {
                let sync_millis = cmp::min(10_000, 2 * (last_tick - tick).unsigned_abs());
                info!(
                    "Clock tick {} is lagging behind the last known tick {}. \
                    Sleeping {}ms to potentially resolve skew before returning InvalidClockTick.",
                    tick, last_tick, sync_millis
                );
                tokio::time::sleep(Duration::from_millis(sync_millis)).await;
                self.enforce_monotonic(self.delegate.now())
            }
            result => result,
        }
    }

    fn enforce_monotonic(&self, tick: i64) -> Result<i64, SlateDBError> {
        let updated_last_tick = self.last_tick.fetch_max(tick, Ordering::SeqCst);
        if tick < updated_last_tick {
            return Err(SlateDBError::InvalidClockTick {
                last_tick: updated_last_tick,
                next_tick: tick,
            });
        }

        Ok(tick)
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
        let clock = Arc::new(MockSystemClock::new());

        // Test positive timestamp
        let positive_ts = 1625097600000i64; // 2021-07-01T00:00:00Z in milliseconds
        clock.clone().set(positive_ts);
        assert_eq!(
            clock.now().timestamp_millis(),
            positive_ts,
            "MockSystemClock should return the timestamp set with set_now"
        );

        // Test negative timestamp (before Unix epoch)
        let negative_ts = -1625097600000; // Before Unix epoch
        clock.clone().set(negative_ts);
        assert_eq!(
            clock.now().timestamp_millis(),
            negative_ts,
            "MockSystemClock should handle negative timestamps correctly"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_advance() {
        let clock = Arc::new(MockSystemClock::new());
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
        let clock = Arc::new(MockSystemClock::new());
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
        let clock = Arc::new(MockSystemClock::new());
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
        let tick_handle = ticker.tick();
        clock.clone().set(100);
        assert!(
            timeout(Duration::from_millis(10000), tick_handle)
                .await
                .is_ok(),
            "Tick should complete when time has advanced by at least tick duration"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_default_system_clock_now() {
        let clock = Arc::new(DefaultSystemClock::new());

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
        let clock = Arc::new(DefaultSystemClock::new());
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
        let clock = Arc::new(DefaultSystemClock::new());
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

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_ticker_starvation_in_select() {
        let clock = Arc::new(MockSystemClock::new());
        let tick_duration = Duration::from_millis(50);
        let mut ticker = clock.ticker(tick_duration);

        let mut tick_count = 0;
        let mut yield_count = 0;

        for i in 0..10 {
            // Advance the mock clock to satisfy any pending sleeps
            clock.set((i + 1) * 50);

            tokio::select! {
                _ = ticker.tick() => {
                    tick_count += 1;
                }
                _ = tokio::task::yield_now() => {
                    yield_count += 1;
                }
            }
        }

        // Ticker should complete at least once
        assert!(
            tick_count > 0,
            "Ticker was completely starved. tick_count: {}, yield_count: {}",
            tick_count,
            yield_count
        );
    }
}
