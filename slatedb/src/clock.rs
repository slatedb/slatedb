#![allow(clippy::disallowed_methods)]

use std::{
    cmp,
    fmt::Debug,
    future::Future,
    ops::{Add, Sub},
    pin::Pin,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::SlateDBError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::info;

/// Defines the physical clock that SlateDB will use to measure time for things
/// like garbage collection schedule ticks, compaction schedule ticks, and so on.
pub trait SystemClock: Debug + Send + Sync {
    /// Returns the current time
    fn now(&self) -> SystemTimestamp;
    /// Advances the clock by the specified duration
    #[cfg(feature = "test-util")]
    fn advance<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    /// Sleeps for the specified duration
    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    /// Returns a ticker that emits a signal every `duration` interval
    fn ticker<'a>(&'a self, duration: Duration) -> SystemClockTicker<'a>;
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SystemTimestamp(Duration);

impl SystemTimestamp {
    pub fn as_millis(self) -> u64 {
        self.0.as_millis() as u64
    }

    pub fn as_secs(self) -> u64 {
        self.0.as_secs()
    }

    pub fn checked_add(self, duration: Duration) -> Option<SystemTimestamp> {
        self.0.checked_add(duration).map(SystemTimestamp)
    }

    pub fn checked_sub(self, duration: Duration) -> Option<SystemTimestamp> {
        self.0.checked_sub(duration).map(SystemTimestamp)
    }

    pub fn duration_since(self, other: SystemTimestamp) -> Result<Duration, SlateDBError> {
        if self < other {
            return Err(SlateDBError::InvalidClockTick {
                last_tick: other.as_millis() as i64,
                next_tick: self.as_millis() as i64,
            });
        }
        Ok(self.0 - other.0)
    }
}

impl std::fmt::Display for SystemTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.0.as_secs(), self.0.subsec_millis())
    }
}

impl Add<Duration> for SystemTimestamp {
    type Output = Self;
    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl Sub<Duration> for SystemTimestamp {
    type Output = Self;
    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl From<SystemTimestamp> for SystemTime {
    fn from(ts: SystemTimestamp) -> Self {
        UNIX_EPOCH + ts.0
    }
}

impl From<SystemTime> for SystemTimestamp {
    fn from(ts: SystemTime) -> Self {
        SystemTimestamp(ts.duration_since(UNIX_EPOCH).unwrap())
    }
}

impl From<Duration> for SystemTimestamp {
    fn from(d: Duration) -> Self {
        SystemTimestamp(d)
    }
}

impl From<SystemTimestamp> for DateTime<Utc> {
    fn from(ts: SystemTimestamp) -> Self {
        DateTime::<Utc>::from(SystemTime::from(ts))
    }
}

impl From<DateTime<Utc>> for SystemTimestamp {
    fn from(ts: DateTime<Utc>) -> Self {
        SystemTimestamp(Duration::from_millis(ts.timestamp_millis() as u64))
    }
}

pub struct SystemClockTicker<'a> {
    clock: &'a dyn SystemClock,
    duration: Duration,
    first_tick: bool,
}

impl<'a> SystemClockTicker<'a> {
    fn new(clock: &'a dyn SystemClock, duration: Duration) -> Self {
        Self {
            clock,
            duration,
            first_tick: true,
        }
    }

    pub fn tick(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        // Tokio's ticker ticks immediately when the first tick() is called.
        // Let's emulate that behavior in our ticker.
        if self.first_tick {
            self.first_tick = false;
            Box::pin(async {})
        } else {
            self.clock.sleep(self.duration)
        }
    }
}

/// A system clock implementation that uses tokio::time::Instant to measure time duration.
/// SystemTime::now() is used to track the initial timestamp (ms since Unix epoch). This
/// timestamp is used to convert the tokio::time::Instant to a SystemTime when now() is
/// called.
///
/// Note that, becasue we're using tokio::time::Instant, manipulating tokio's clock with
/// tokio::time::pause(), tokio::time::advance(), and so on will affect the
/// DefaultSystemClock's time as well.
#[derive(Debug)]
pub struct DefaultSystemClock {
    initial_ts: SystemTimestamp,
    initial_instant: Instant,
}

impl DefaultSystemClock {
    pub fn new() -> Self {
        Self {
            initial_ts: SystemTimestamp(SystemTime::now().duration_since(UNIX_EPOCH).unwrap()),
            initial_instant: Instant::now(),
        }
    }
}

impl Default for DefaultSystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemClock for DefaultSystemClock {
    fn now(&self) -> SystemTimestamp {
        let elapsed = Instant::now().duration_since(self.initial_instant);
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
    current_ts_ms: AtomicU64,
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
        Self::with_duration(Duration::from_millis(0))
    }

    /// Creates a new mock system clock with the specified duration since the Unix epoch
    pub fn with_duration(duration: Duration) -> Self {
        Self {
            current_ts_ms: AtomicU64::new(duration.as_millis() as u64),
        }
    }

    /// Sets the current timestamp of the mock system clock
    pub fn set(&self, duration: Duration) {
        self.current_ts_ms
            .store(duration.as_millis() as u64, Ordering::SeqCst);
    }
}

#[cfg(feature = "test-util")]
impl SystemClock for MockSystemClock {
    fn now(&self) -> SystemTimestamp {
        SystemTimestamp(Duration::from_millis(
            self.current_ts_ms.load(Ordering::SeqCst),
        ))
    }

    fn advance<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        self.current_ts_ms
            .fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
        Box::pin(async move {})
    }

    /// Sleeps for the specified duration. The duration argument is a duration relative to the point
    /// at which sleep() is called. It is not the absolute duration since the Unix epoch.
    fn sleep<'a>(&'a self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let end_time = self.current_ts_ms.load(Ordering::SeqCst) + duration.as_millis() as u64;
        Box::pin(async move {
            #[allow(clippy::while_immutable_condition)]
            while self.current_ts_ms.load(Ordering::SeqCst) < end_time {
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
    /// Returns a timestamp (typically measured in millis since the unix epoch),
    /// must return monotonically increasing numbers (this is enforced
    /// at runtime and will panic if the invariant is broken).
    ///
    /// Note that this clock does not need to return a number that
    /// represents the unix timestamp; the only requirement is that
    /// it represents a sequence that can attribute a logical ordering
    /// to actions on the database.
    fn now(&self) -> i64;
}

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
        let current_ts = self.inner.now().as_millis() as i64;
        self.last_ts.fetch_max(current_ts, Ordering::SeqCst);
        self.last_ts.load(Ordering::SeqCst)
    }
}

/// SlateDB uses MonotonicClock internally so that it can enforce that clock ticks
/// from the underlying implementation are monotonically increasing
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
            clock.now(),
            SystemTimestamp(Duration::from_millis(0)),
            "Default MockSystemClock should start at timestamp 0"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_set_now() {
        let clock = Arc::new(MockSystemClock::new());

        // Test positive timestamp
        let positive_ts = 1625097600000i64; // 2021-07-01T00:00:00Z in milliseconds
        clock.clone().set(Duration::from_millis(positive_ts as u64));
        assert_eq!(
            clock.now(),
            SystemTimestamp(Duration::from_millis(positive_ts as u64)),
            "MockSystemClock should return the timestamp set with set_now"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_advance() {
        let clock = Arc::new(MockSystemClock::new());
        let initial_ts = Duration::from_millis(1000);

        // Set initial time
        clock.clone().set(initial_ts);

        // Advance by 500ms
        let duration = Duration::from_millis(500);
        clock.clone().advance(duration).await;

        // Check that time advanced correctly
        assert_eq!(
            clock.now(),
            SystemTimestamp(initial_ts + duration),
            "MockSystemClock should advance time by the specified duration"
        );
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_mock_system_clock_sleep() {
        let clock = Arc::new(MockSystemClock::new());
        let initial_ts = Duration::from_millis(2000);

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
        clock.set(initial_ts + Duration::from_millis(500));
        assert!(
            timeout(Duration::from_millis(10), sleep_handle2)
                .await
                .is_err(),
            "Sleep should not complete when time has advanced by less than sleep duration"
        );

        // Advance clock by enough to complete sleep
        clock.set(initial_ts + Duration::from_millis(1000));
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
        clock.clone().set(Duration::from_millis(100));
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
}
