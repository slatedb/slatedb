use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use slatedb_common::clock::{DefaultSystemClock, MockSystemClock, SystemClock as Clock};

use crate::error::{Error, SlateDbError};

/// The clock a `Db`, `DbReader` or `Admin` reads wall time from. Every engine
/// timer follows it: TTL expiry, checkpoint lifetimes, flush and poll ticks,
/// the object-store retry backoff and the flush timeout.
///
/// `default_clock` follows the process clock. `mock` starts at
/// `initial_ts_millis` and moves only forward, through `advance` and `set`, so
/// a test drives every timer from outside the engine; a retry backoff or flush
/// timeout on a frozen mock waits until the test moves the clock past it.
/// Engine tasks sleeping on a mock poll it by yielding, so a handle built on
/// one keeps the runtime's workers busy: a mock is for tests only. One clock
/// may be shared by several handles; a `Db` and the `DbReader` following it
/// read the same time.
#[derive(uniffi::Object)]
pub struct SystemClock {
    inner: Inner,
}

enum Inner {
    Default(Arc<DefaultSystemClock>),
    Mock(Arc<MockSystemClock>),
}

#[uniffi::export]
impl SystemClock {
    /// The process clock.
    #[uniffi::constructor]
    pub fn default_clock() -> Arc<Self> {
        Arc::new(Self {
            inner: Inner::Default(Arc::new(DefaultSystemClock::new())),
        })
    }

    /// A clock frozen at `initial_ts_millis` (milliseconds since the Unix
    /// epoch) that moves only through `advance` and `set`. Refused when
    /// `initial_ts_millis` is not a representable timestamp.
    #[uniffi::constructor]
    pub fn mock(initial_ts_millis: i64) -> Result<Arc<Self>, Error> {
        check_timestamp(initial_ts_millis)?;
        Ok(Arc::new(Self {
            inner: Inner::Mock(Arc::new(MockSystemClock::with_time(initial_ts_millis))),
        }))
    }

    /// The clock's current time in milliseconds since the Unix epoch.
    pub fn now_millis(&self) -> i64 {
        self.inner().now().timestamp_millis()
    }

    /// Moves a mock clock forward by `millis`. Engine tasks sleeping on the
    /// clock see the new time when the runtime next polls them; `advance`
    /// returns without waiting for that, so a test must wait for the effect it
    /// expects (an expired key, a completed flush) rather than assert it at
    /// once. Refused on the default clock and when the result is not a
    /// representable timestamp.
    pub async fn advance(&self, millis: u64) -> Result<(), Error> {
        let mock = self.mock_clock()?;
        let now_millis = mock.now().timestamp_millis();
        let ts_millis =
            i64::try_from(millis).map_or(i64::MAX, |millis| now_millis.saturating_add(millis));
        check_timestamp(ts_millis)?;
        mock.advance(Duration::from_millis(millis)).await;
        Ok(())
    }

    /// Sets a mock clock to `ts_millis`. The engine's tickers require a
    /// monotonic clock, so a time before the clock's current one is refused,
    /// as is one that is not a representable timestamp. Refused on the default
    /// clock.
    pub fn set(&self, ts_millis: i64) -> Result<(), Error> {
        let mock = self.mock_clock()?;
        check_timestamp(ts_millis)?;
        let now_millis = mock.now().timestamp_millis();
        if ts_millis < now_millis {
            return Err(SlateDbError::ClockMovedBackwards {
                ts_millis,
                now_millis,
            }
            .into());
        }
        mock.set(ts_millis);
        Ok(())
    }

    pub fn is_mock(&self) -> bool {
        matches!(self.inner, Inner::Mock(_))
    }
}

impl SystemClock {
    pub(crate) fn inner(&self) -> Arc<dyn Clock> {
        match &self.inner {
            Inner::Default(clock) => clock.clone(),
            Inner::Mock(clock) => clock.clone(),
        }
    }

    fn mock_clock(&self) -> Result<&Arc<MockSystemClock>, Error> {
        match &self.inner {
            Inner::Mock(clock) => Ok(clock),
            Inner::Default(_) => Err(SlateDbError::ClockNotMock.into()),
        }
    }
}

/// `MockSystemClock::now` panics on a timestamp chrono cannot represent, so
/// every way of choosing one is checked before the clock holds it.
fn check_timestamp(ts_millis: i64) -> Result<(), Error> {
    DateTime::from_timestamp_millis(ts_millis)
        .map(|_| ())
        .ok_or(SlateDbError::InvalidTimestampMillis { ts_millis }.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_moves_only_when_told() {
        let clock = SystemClock::mock(1_000).unwrap();
        assert!(clock.is_mock());
        assert_eq!(clock.now_millis(), 1_000);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(clock.now_millis(), 1_000);
        clock.advance(500).await.unwrap();
        assert_eq!(clock.now_millis(), 1_500);
        clock.set(2_000).unwrap();
        assert_eq!(clock.now_millis(), 2_000);
        clock.set(2_000).unwrap();
        assert_eq!(clock.now_millis(), 2_000);
    }

    #[tokio::test]
    async fn mock_never_moves_backwards() {
        let clock = SystemClock::mock(1_000).unwrap();
        assert!(matches!(clock.set(999), Err(Error::Invalid { .. })));
        assert_eq!(clock.now_millis(), 1_000);
    }

    #[tokio::test]
    async fn mock_stays_inside_the_representable_range() {
        assert!(matches!(
            SystemClock::mock(i64::MAX),
            Err(Error::Invalid { .. })
        ));
        let clock = SystemClock::mock(1_000).unwrap();
        assert!(matches!(clock.set(1 << 62), Err(Error::Invalid { .. })));
        assert!(matches!(
            clock.advance(1 << 62).await,
            Err(Error::Invalid { .. })
        ));
        assert!(matches!(
            clock.advance(u64::MAX).await,
            Err(Error::Invalid { .. })
        ));
        assert_eq!(clock.now_millis(), 1_000);
    }

    #[tokio::test]
    async fn default_clock_refuses_to_be_driven() {
        let clock = SystemClock::default_clock();
        assert!(!clock.is_mock());
        assert!(matches!(clock.advance(1).await, Err(Error::Invalid { .. })));
        assert!(matches!(clock.set(1), Err(Error::Invalid { .. })));
        let before = clock.now_millis();
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(clock.now_millis() >= before);
    }
}
