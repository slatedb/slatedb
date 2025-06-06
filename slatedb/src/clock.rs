use std::{
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use crate::utils::{system_time_from_millis, system_time_to_millis};

/// Defines the physical clock that SlateDB will use to measure time for things
/// like garbage collection schedule ticks, compaction schedule ticks, and so on.
pub trait SystemClock: Send + Sync {
    fn now(&self) -> SystemTime;
}

/// A system clock implementation that uses tokio::time::Instant to measure time. During
/// normal usage, this is equivalent to SystemTime::now().
///
/// In test cases, it is possible to advance the clock manually with
/// `#[tokio::test(start_paused = true)]` and `tokio::time::sleep`.
pub(crate) struct DefaultSystemClock {
    initial_ts: u128,
    initial_instant: tokio::time::Instant,
}

impl DefaultSystemClock {
    pub(crate) fn new() -> Self {
        let ts_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

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
        system_time_from_millis((self.initial_ts + elapsed.as_millis()) as i64)
    }
}

/// Defines the logical clock that SlateDB will use to measure time for things
/// like TTL expiration.
pub trait LogicalClock: Send + Sync {
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

pub struct DefaultLogicalClock {
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
        }
    }
}

impl LogicalClock for DefaultLogicalClock {
    fn now(&self) -> i64 {
        system_time_to_millis(self.inner.now())
    }
}
