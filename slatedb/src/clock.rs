use std::{cmp::Ordering, sync::atomic::AtomicI64, time::{SystemTime, UNIX_EPOCH}};

use crate::utils::system_time_to_millis;

/// Defines the physical clock that SlateDB will use to measure time for things
/// like TTL expiration, garbage collection clock ticks, and so on.
pub trait SystemClock {
    fn now(&self) -> SystemTime;
}

#[derive(Default)]
pub struct DefaultSystemClock {
    last_tick: AtomicI64,
}

impl DefaultSystemClock {
    pub fn new() -> Self {
        Self {
            last_tick: AtomicI64::new(i64::MIN),
        }
    }
}

impl SystemClock for DefaultSystemClock {
    fn now(&self) -> i64 {
        // since SystemTime is not guaranteed to be monotonic, we enforce it here
        let tick = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as i64, // Time is after the epoch
            Err(e) => -(e.duration().as_millis() as i64), // Time is before the epoch, return negative
        };
        self.last_tick.fetch_max(tick, Ordering::SeqCst);
        self.last_tick.load(Ordering::SeqCst)
    }
}

/// Defines the logical clock that SlateDB will use to measure time for things
/// like 
pub trait LogicalClock {
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

impl LogicalClock for DefaultLogicalClock {
    fn now(&self) -> i64 {
        system_time_to_millis(SystemTime::now())
    }
}