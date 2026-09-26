use std::sync::Arc;
use std::time::Duration;

use slatedb_common::clock::{DefaultSystemClock, MockSystemClock, SystemClock as Clock};

use crate::error::Error;

/// The clock a `Db`, `DbReader` or `Admin` reads wall time from: TTL expiry,
/// checkpoint lifetimes, poll and schedule ticks.
///
/// `default_clock` follows the process clock. `mock` starts at
/// `initial_ts_millis` and moves only through `advance` and `set`, so a test
/// drives every timer from outside the engine. One clock may be shared by
/// several handles; a `Db` and the `DbReader` following it read the same time.
#[derive(uniffi::Object)]
pub struct SystemClock {
    pub(crate) inner: Arc<dyn Clock>,
    mock: Option<Arc<MockSystemClock>>,
}

#[uniffi::export]
impl SystemClock {
    /// The process clock.
    #[uniffi::constructor(name = "default")]
    pub fn default_clock() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(DefaultSystemClock::new()),
            mock: None,
        })
    }

    /// A clock frozen at `initial_ts_millis` (milliseconds since the Unix
    /// epoch) that moves only through `advance` and `set`.
    #[uniffi::constructor]
    pub fn mock(initial_ts_millis: i64) -> Arc<Self> {
        let mock = Arc::new(MockSystemClock::with_time(initial_ts_millis));
        Arc::new(Self {
            inner: mock.clone(),
            mock: Some(mock),
        })
    }

    /// The clock's current time in milliseconds since the Unix epoch.
    pub fn now_millis(&self) -> i64 {
        self.inner.now().timestamp_millis()
    }

    /// Moves a mock clock forward by `millis` and wakes every sleeper whose
    /// deadline has passed. Refused on the default clock.
    pub async fn advance(&self, millis: u64) -> Result<(), Error> {
        let mock = self.mock_clock()?;
        mock.advance(Duration::from_millis(millis)).await;
        Ok(())
    }

    /// Sets a mock clock to `ts_millis`. Refused on the default clock.
    pub fn set(&self, ts_millis: i64) -> Result<(), Error> {
        self.mock_clock()?.set(ts_millis);
        Ok(())
    }

    pub fn is_mock(&self) -> bool {
        self.mock.is_some()
    }
}

impl SystemClock {
    fn mock_clock(&self) -> Result<&Arc<MockSystemClock>, Error> {
        self.mock.as_ref().ok_or_else(|| Error::Invalid {
            message: "only a mock clock can be advanced or set".to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_moves_only_when_told() {
        let clock = SystemClock::mock(1_000);
        assert!(clock.is_mock());
        assert_eq!(clock.now_millis(), 1_000);
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(clock.now_millis(), 1_000);
        clock.advance(500).await.unwrap();
        assert_eq!(clock.now_millis(), 1_500);
        clock.set(-7).unwrap();
        assert_eq!(clock.now_millis(), -7);
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
