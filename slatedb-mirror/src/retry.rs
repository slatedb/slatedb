//! Retries for the mirror's own remote calls.
//!
//! The mirror sits beneath SlateDB's `RetryingObjectStore`, so that wrapper
//! doesn't see the downloads and remote-scan LISTs the mirror starts itself.
//! These retries use the same backoff and the same rules.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable, Sleeper};
use log::{debug, info};
use slatedb_common::clock::SystemClock;

use crate::MirrorError;

const MIN_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
struct SystemClockSleeper {
    clock: Arc<dyn SystemClock>,
}

impl Sleeper for SystemClockSleeper {
    type Sleep = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn sleep(&self, dur: Duration) -> Self::Sleep {
        let clock = Arc::clone(&self.clock);
        Box::pin(async move { clock.sleep(dur).await })
    }
}

/// Retries transient remote errors with exponential backoff.
#[derive(Debug, Clone)]
pub(crate) struct Retry {
    clock: Arc<dyn SystemClock>,
    /// `None` retries forever.
    max_retries: Option<u32>,
}

impl Retry {
    pub(crate) fn new(clock: Arc<dyn SystemClock>, max_retries: Option<u32>) -> Self {
        Self { clock, max_retries }
    }

    /// Runs `op` until it succeeds, fails with an error that isn't worth
    /// retrying, or runs out of retries.
    pub(crate) async fn run<T, F, Fut>(&self, op: F) -> object_store::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = object_store::Result<T>>,
    {
        let builder = ExponentialBuilder::default()
            .with_min_delay(MIN_RETRY_DELAY)
            .with_max_delay(MAX_RETRY_DELAY);
        let builder = match self.max_retries {
            Some(max_retries) => builder.with_max_times(max_retries as usize),
            None => builder.without_max_times(),
        };
        op.retry(builder)
            .sleep(SystemClockSleeper {
                clock: Arc::clone(&self.clock),
            })
            .notify(|err, duration| {
                info!(
                    "retrying mirror remote operation [error={:?}, duration={:?}]",
                    err, duration
                )
            })
            .when(should_retry)
            .await
    }
}

/// Matches `RetryingObjectStore::should_retry` in `slatedb`.
pub(crate) fn should_retry(err: &object_store::Error) -> bool {
    let retry = !matches!(
        err,
        object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotModified { .. }
            | object_store::Error::NotFound { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::NotSupported { .. }
    ) && MirrorError::find(err).is_none();
    if !retry {
        debug!("not retrying mirror remote operation [error={:?}]", err);
    }
    retry
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use slatedb_common::clock::DefaultSystemClock;

    use super::*;

    fn transient() -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: Box::new(std::io::Error::other("timeout")),
        }
    }

    #[test]
    fn should_not_retry_permanent_or_mirror_errors() {
        assert!(should_retry(&transient()));
        assert!(!should_retry(&object_store::Error::NotFound {
            path: "a".to_string(),
            source: Box::new(std::io::Error::other("missing")),
        }));
        assert!(!should_retry(&object_store::Error::from(
            MirrorError::Local {
                source: std::io::Error::other("disk full"),
            }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn should_retry_transient_errors_until_success() {
        let retry = Retry::new(Arc::new(DefaultSystemClock::new()), None);
        let attempts = AtomicUsize::new(0);
        let result = retry
            .run(|| async {
                if attempts.fetch_add(1, Ordering::SeqCst) < 3 {
                    Err(transient())
                } else {
                    Ok(7)
                }
            })
            .await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn should_stop_after_max_retries() {
        let retry = Retry::new(Arc::new(DefaultSystemClock::new()), Some(2));
        let attempts = AtomicUsize::new(0);
        let result: object_store::Result<()> = retry
            .run(|| async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(transient())
            })
            .await;
        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }
}
