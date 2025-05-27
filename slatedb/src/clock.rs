use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tracing::info;

use crate::{config::DurabilityLevel, SlateDBError};

/// defines the clock that SlateDB will use during this session
pub trait Clock: Debug + Send + Sync {
    /// Returns a timestamp (typically measured in millis since the unix epoch),
    /// must return monotonically increasing numbers (this is enforced
    /// at runtime and will panic if the invariant is broken).
    ///
    /// Note that this clock does not need to return a number that
    /// represents the unix timestamp; the only requirement is that
    /// it represents a sequence that can attribute a logical ordering
    /// to actions on the database.
    fn now(&self) -> i64;

    /// Returns the current time as a SystemTime.
    ///
    /// This function panics if the Clock time cannot be converted to a SystemTime.
    fn now_systime(&self) -> SystemTime {
        chrono::DateTime::from_timestamp_millis(self.now())
            .map(SystemTime::from)
            .expect("Failed to convert Clock time to SystemTime")
    }
}

/// contains the default implementation of the Clock, and will return the system time
#[derive(Debug)]
pub struct SystemClock {
    last_tick: AtomicI64,
}

impl SystemClock {
    pub fn new() -> Self {
        Self {
            last_tick: AtomicI64::new(i64::MIN),
        }
    }
}

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        // since SystemTime is not guaranteed to be monotonic, we enforce it here
        let tick = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as i64, // Time is after the epoch
            Err(e) => -(e.duration().as_millis() as i64), // Time is before the epoch, return negative
        };
        self.last_tick.fetch_max(tick, Ordering::SeqCst).max(tick)
    }
}

/// SlateDB uses MonotonicClock internally so that it can enforce that clock ticks
/// from the underlying implementation are monotonically increasing
pub(crate) struct MonotonicClock {
    pub(crate) last_tick: AtomicI64,
    pub(crate) last_durable_tick: AtomicI64,
    delegate: Arc<dyn Clock>,
}

impl MonotonicClock {
    pub(crate) fn new(delegate: Arc<dyn Clock>, init_tick: i64) -> Self {
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
                let sync_millis = std::cmp::min(10_000, 2 * (last_tick - tick).unsigned_abs());
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

    // The semantics of filtering expired records on read differ slightly depending on
    // the configured ReadLevel.
    //
    // For Uncommitted we can just use the actual clock's "now"
    // as this corresponds to the current time seen by uncommitted writes but is not persisted
    // and only enforces monotonicity via the local in-memory MonotonicClock. This means it's
    // possible for the mono_clock.now() to go "backwards" following a crash and recovery, which
    // could result in records that were filtered out before the crash coming back to life and being
    // returned after the crash.
    //
    // If the read level is instead set to Committed, we only use the last_tick of the monotonic
    // clock to filter out expired records, since this corresponds to the highest time of any
    // persisted batch and is thus recoverable following a crash. Since the last tick is the
    // last persisted time we are guaranteed monotonicity of the #get_last_tick function and
    // thus will not see this "time travel" phenomenon -- with Committed, once a record is
    // filtered out due to ttl expiry, it is guaranteed not to be seen again by future Committed
    // reads.
    pub(crate) async fn now_by_durability(
        &self,
        durability_level: DurabilityLevel,
    ) -> Result<i64, SlateDBError> {
        match durability_level {
            DurabilityLevel::Memory => self.now().await,
            DurabilityLevel::Remote => Ok(self.get_last_durable_tick()),
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
#[derive(Debug)]
pub(crate) struct ManualClock {
    ticker: AtomicI64,
}

#[cfg(test)]
impl ManualClock {
    pub(crate) fn new() -> ManualClock {
        ManualClock {
            ticker: AtomicI64::new(0),
        }
    }

    pub(crate) fn set(&self, tick: i64) {
        self.ticker.store(tick, Ordering::SeqCst);
    }
}

#[cfg(test)]
impl Clock for ManualClock {
    fn now(&self) -> i64 {
        self.ticker.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TokioClock {
    initial_ts: u128,
    initial_instant: tokio::time::Instant,
}

#[cfg(test)]
impl TokioClock {
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

#[cfg(test)]
impl Clock for TokioClock {
    fn now(&self) -> i64 {
        let elapsed = tokio::time::Instant::now().duration_since(self.initial_instant);
        (self.initial_ts + elapsed.as_millis()) as i64
    }
}
