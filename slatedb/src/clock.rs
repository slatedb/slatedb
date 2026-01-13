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

#[cfg(feature = "test-util")]
pub use slatedb_common::clock::MockSystemClock;
pub use slatedb_common::clock::{DefaultSystemClock, SystemClock, SystemClockTicker};

use crate::error::SlateDBError;
use log::info;
use std::{
    cmp,
    fmt::Debug,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

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
