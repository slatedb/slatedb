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
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    utils::{system_time_from_millis, system_time_to_millis},
    SlateDBError,
};
use tracing::info;

/// Defines the physical clock that SlateDB will use to measure time for things
/// like garbage collection schedule ticks, compaction schedule ticks, and so on.
pub trait SystemClock: Debug + Send + Sync {
    fn now(&self) -> SystemTime;
    #[cfg(test)]
    fn advance(&mut self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn sleep(self: Arc<Self>, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
    fn ticker(self: Arc<Self>, duration: Duration) -> SystemClockTicker;
}

pub struct SystemClockTicker {
    clock: Arc<dyn SystemClock>,
    duration: Duration,
    first_tick: bool,
}

impl SystemClockTicker {
    fn new(clock: Arc<dyn SystemClock>, duration: Duration) -> Self {
        Self {
            clock,
            duration,
            first_tick: true,
        }
    }

    pub fn tick(&mut self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        // Emulate first tick in tokio::time::Interval::tick by skipping the sleep
        if self.first_tick {
            self.first_tick = false;
            self.clock.clone().sleep(Duration::from_millis(0))
        } else {
            self.clock.clone().sleep(self.duration)
        }
    }
}

/// A system clock implementation that uses tokio::time::Instant to measure time. During
/// normal usage, this is equivalent to SystemTime::now().
///
/// In test cases, it is possible to advance the clock manually with
/// `#[tokio::test(start_paused = true)]` and `tokio::time::sleep`.
#[derive(Debug)]
pub struct DefaultSystemClock {
    initial_ts: i64,
    initial_instant: tokio::time::Instant,
}

impl DefaultSystemClock {
    pub fn new() -> Self {
        let ts_millis = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as i64, // Time is after the epoch
            Err(e) => -(e.duration().as_millis() as i64), // Time is before the epoch, return negative
        };
        Self {
            initial_ts: ts_millis,
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
    fn now(&self) -> SystemTime {
        let elapsed = tokio::time::Instant::now().duration_since(self.initial_instant);
        system_time_from_millis(self.initial_ts + elapsed.as_millis() as i64)
    }

    #[cfg(test)]
    fn advance(&mut self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::advance(duration))
    }

    fn sleep(self: Arc<Self>, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn ticker(self: Arc<Self>, duration: Duration) -> SystemClockTicker {
        SystemClockTicker::new(self, duration)
    }
}

#[derive(Debug)]
pub struct MockSystemClock {
    current_ts: i64,
}

impl MockSystemClock {
    pub fn new() -> Self {
        Self { current_ts: 0 }
    }

    pub fn set_now(&mut self, ts_millis: i64) {
        self.current_ts = ts_millis;
    }
}

impl SystemClock for MockSystemClock {
    fn now(&self) -> SystemTime {
        if self.current_ts < 0 {
            UNIX_EPOCH - Duration::from_millis(self.current_ts.unsigned_abs())
        } else {
            UNIX_EPOCH + Duration::from_millis(self.current_ts as u64)
        }
    }

    #[cfg(test)]
    fn advance(&mut self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.current_ts += duration.as_millis() as i64;
        Box::pin(async move {})
    }

    fn sleep(self: Arc<Self>, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let end_time = self.current_ts + duration.as_millis() as i64;
        Box::pin(async move {
            while self.current_ts < end_time {
                tokio::task::yield_now().await;
            }
        })
    }

    fn ticker(self: Arc<Self>, duration: Duration) -> SystemClockTicker {
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
        let current_ts = system_time_to_millis(self.inner.now());
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
