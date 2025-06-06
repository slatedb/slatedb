use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::utils::{system_time_from_millis, system_time_to_millis};

/// Defines the physical clock that SlateDB will use to measure time for things
/// like garbage collection schedule ticks, compaction schedule ticks, and so on.
pub trait SystemClock: Send + Sync {
    fn now(&self) -> SystemTime;
}

pub struct DefaultSystemClock {
    last_tick: AtomicI64,
}

impl Default for DefaultSystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultSystemClock {
    pub fn new() -> Self {
        Self {
            last_tick: AtomicI64::new(i64::MIN),
        }
    }
}

impl SystemClock for DefaultSystemClock {
    fn now(&self) -> SystemTime {
        // since SystemTime is not guaranteed to be monotonic, we enforce it here
        let tick = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as i64, // Time is after the epoch
            Err(e) => -(e.duration().as_millis() as i64), // Time is before the epoch, return negative
        };
        self.last_tick.fetch_max(tick, Ordering::SeqCst);
        system_time_from_millis(self.last_tick.load(Ordering::SeqCst))
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

pub struct DefaultLogicalClock;

impl Default for DefaultLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultLogicalClock {
    pub fn new() -> Self {
        Self
    }
}

impl LogicalClock for DefaultLogicalClock {
    fn now(&self) -> i64 {
        // TODO use DefaultSystemClock to keep it monotonic
        system_time_to_millis(SystemTime::now())
    }
}
