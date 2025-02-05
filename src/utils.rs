use crate::config::ReadLevel::{Committed, Uncommitted};
use crate::config::{Clock, ReadLevel};
use crate::error::SlateDBError;
use crate::error::SlateDBError::{BackgroundTaskPanic, BackgroundTaskShutdown};
use crate::types::{RowEntry, ValueDeletable};
use bytes::Bytes;
use std::future::Future;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};

pub(crate) struct WatchableOnceCell<T: Clone> {
    rx: tokio::sync::watch::Receiver<Option<T>>,
    tx: tokio::sync::watch::Sender<Option<T>>,
}

pub(crate) struct WatchableOnceCellReader<T: Clone> {
    rx: tokio::sync::watch::Receiver<Option<T>>,
}

impl<T: Clone> WatchableOnceCell<T> {
    pub(crate) fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(None);
        Self { rx, tx }
    }

    pub(crate) fn write(&self, val: T) {
        self.tx.send_if_modified(|v| {
            if v.is_some() {
                return false;
            }
            v.replace(val);
            true
        });
    }

    pub(crate) fn reader(&self) -> WatchableOnceCellReader<T> {
        WatchableOnceCellReader {
            rx: self.rx.clone(),
        }
    }
}

impl<T: Clone> WatchableOnceCellReader<T> {
    pub(crate) fn read(&self) -> Option<T> {
        self.rx.borrow().clone()
    }

    pub(crate) async fn await_value(&mut self) -> T {
        self.rx
            .wait_for(|v| v.is_some())
            .await
            .expect("watch channel closed")
            .clone()
            .expect("no value found")
    }
}

/// Spawn a monitored background tokio task. The task must return a Result<T, SlateDBError>.
/// The task is spawned by a monitor task. When the task exits, the monitor task
/// calls a provided cleanup fn with the returned error, or Err(BackgroundTaskShutdown)
/// if the task exited with Ok. If the spawned task panics, the cleanup fn is called with
/// Err(BackgroundTaskFailed).
pub(crate) fn spawn_bg_task<F, T, C>(
    handle: &tokio::runtime::Handle,
    cleanup_fn: C,
    future: F,
) -> tokio::task::JoinHandle<Result<T, SlateDBError>>
where
    F: Future<Output = Result<T, SlateDBError>> + Send + 'static,
    T: Send + 'static,
    C: FnOnce(&SlateDBError) + Send + 'static,
{
    let inner_handle = handle.clone();
    handle.spawn(async move {
        let jh = inner_handle.spawn(future);
        match jh.await {
            Ok(Err(err)) => {
                // task exited with an error
                cleanup_fn(&err);
                Err(err)
            }
            Ok(result) => {
                cleanup_fn(&BackgroundTaskShutdown);
                result
            }
            Err(join_err) => {
                // task panic'd or was cancelled
                let err = BackgroundTaskPanic(Arc::new(Mutex::new(
                    join_err
                        .try_into_panic()
                        .unwrap_or_else(|_| Box::new("background task was aborted")),
                )));
                cleanup_fn(&err);
                Err(err)
            }
        }
    })
}

/// Spawn a monitored background os thread. The thread must return a Result<T, SlateDBError>.
/// The thread is spawned by a monitor thread. When the thread exits, the monitor thread
/// calls a provided cleanup fn with the returned error, or Err(BackgroundTaskShutdown)
/// if the thread exited with Ok. If the spawned thread panics, the cleanup fn is called with
/// Err(BackgroundTaskFailed).
pub(crate) fn spawn_bg_thread<F, T, C>(
    name: &str,
    cleanup_fn: C,
    f: F,
) -> std::thread::JoinHandle<Result<T, SlateDBError>>
where
    F: FnOnce() -> Result<T, SlateDBError> + Send + 'static,
    T: Send + 'static,
    C: FnOnce(&SlateDBError) + Send + 'static,
{
    let monitored_name = String::from(name);
    let monitor_name = format!("{}-monitor", name);
    std::thread::Builder::new()
        .name(monitor_name)
        .spawn(move || {
            let inner = std::thread::Builder::new()
                .name(monitored_name)
                .spawn(f)
                .expect("failed to create monitored thread");
            let result = inner.join();
            match result {
                Err(err) => {
                    // the thread panic'd
                    let err = BackgroundTaskPanic(Arc::new(Mutex::new(err)));
                    cleanup_fn(&err);
                    Err(err)
                }
                Ok(Err(err)) => {
                    // thread exited with an error
                    cleanup_fn(&err);
                    Err(err)
                }
                Ok(r) => {
                    cleanup_fn(&BackgroundTaskShutdown);
                    r
                }
            }
        })
        .expect("failed to create monitor thread")
}

// Temporary functions to convert ValueDeletable to Option<Bytes> until
// we add proper support for merges.
pub(crate) fn unwrap_result(
    value: ValueDeletable,
) -> Result<Option<Bytes>, SlateDBError> {
    match value {
        ValueDeletable::Value(v) => Ok(Some(v)),
        ValueDeletable::Merge(_) => {
            Err(unimplemented!("MergeOperator is not yet fully implemented"))
        }
        ValueDeletable::Tombstone => Ok(None),
    }
}

pub(crate) fn filter_expired(
    entry: RowEntry,
    mono_clock: Arc<MonotonicClock>,
    read_level: ReadLevel
) -> Result<Option<RowEntry>, SlateDBError> {

    if let Some(expire_ts) = entry.expire_ts {
        /*
          Note: the semantics of filtering expired records on read differ slightly depending on
          the configured ReadLevel. For Uncommitted we can just use the actual clock's "now"
          as this corresponds to the current time seen by uncommitted writes but is not persisted
          and only enforces monotonicity via the local in-memory MonotonicClock. This means it's
          possible for the mono_clock.now() to go "backwards" following a crash and recovery, which
          could result in records that were filtered out before the crash coming back to life and being
          returned after the crash.
          If the read level is instead set to Committed, we only use the last_tick of the monotonic
          clock to filter out expired records, since this corresponds to the highest time of any
          persisted batch and is thus recoverable following a crash. Since the last tick is the
          last persisted time we are guaranteed monotonicity of the #get_last_tick function and
          thus will not see this "time travel" phenomenon -- with Committed, once a record is
          filtered out due to ttl expiry, it is guaranteed not to be seen again by future Committed
          reads.
         */
        let effective_now;
        match read_level {
            Committed => {
                effective_now = mono_clock.get_last_tick()
            }
            Uncommitted => {
                effective_now = mono_clock.now()?
            }
        }

        if expire_ts <= effective_now {
            Ok(None)
        } else {
            Ok(Some(entry))
        }
    } else {
        Ok(Some(entry))
    }
}

/// SlateDB uses MonotonicClock internally so that it can enforce that clock ticks
/// from the underlying implementation are monotonically increasing
pub(crate) struct MonotonicClock {
    pub(crate) last_tick: AtomicI64,
    delegate: Arc<dyn Clock + Send + Sync>,
}

impl MonotonicClock {
    pub(crate) fn new(delegate: Arc<dyn Clock + Send + Sync>, init_tick: i64) -> Self {
        Self {
            delegate,
            last_tick: AtomicI64::new(init_tick),
        }
    }

    pub(crate) fn set_last_tick(&self, tick: i64) -> Result<i64, SlateDBError> {
        self.enforce_monotonic(tick)
    }

    pub(crate) fn get_last_tick(&self) -> i64 {
        self.last_tick.load(SeqCst)
    }

    pub(crate) fn now(&self) -> Result<i64, SlateDBError> {
        let tick = self.delegate.now();
        self.enforce_monotonic(tick)
    }

    fn enforce_monotonic(&self, tick: i64) -> Result<i64, SlateDBError> {
        let updated_last_tick = self.last_tick.fetch_max(tick, SeqCst);
        if tick < updated_last_tick {
            return Err(SlateDBError::InvalidClockTick {
                last_tick: updated_last_tick,
                next_tick: tick,
            });
        }

        Ok(tick)
    }
}

/// Merge two options using the provided function.
pub(crate) fn merge_options<T>(
    current: Option<T>,
    next: Option<T>,
    f: impl Fn(T, T) -> T,
) -> Option<T> {
    match (current, next) {
        (Some(current), Some(next)) => Some(f(current, next)),
        (None, next) => next,
        (current, None) => current,
    }
}

#[cfg(test)]
mod tests {
    use crate::error::SlateDBError;
    use crate::test_utils::TestClock;
    use crate::utils::{spawn_bg_task, spawn_bg_thread, MonotonicClock, WatchableOnceCell};
    use parking_lot::Mutex;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;

    struct ErrorCaptor {
        error: Mutex<Option<SlateDBError>>,
    }

    impl ErrorCaptor {
        fn new() -> Self {
            Self {
                error: Mutex::new(None),
            }
        }

        fn capture(&self, error: &SlateDBError) {
            let mut guard = self.error.lock();
            let prev = guard.replace(error.clone());
            assert!(prev.is_none());
        }

        fn captured(&self) -> Option<SlateDBError> {
            self.error.lock().clone()
        }
    }

    #[tokio::test]
    async fn test_should_cleanup_when_task_exits_with_error() {
        let captor = Arc::new(ErrorCaptor::new());
        let handle = tokio::runtime::Handle::current();
        let captor2 = captor.clone();

        let task = spawn_bg_task(&handle, move |err| captor2.capture(err), async {
            Err(SlateDBError::Fenced)
        });

        let result: Result<(), SlateDBError> = task.await.expect("join failure");
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(captor.captured(), Some(SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_should_cleanup_when_task_panics() {
        let monitored = async {
            panic!("oops");
        };
        let captor = Arc::new(ErrorCaptor::new());
        let handle = tokio::runtime::Handle::current();
        let captor2 = captor.clone();

        let task = spawn_bg_task(&handle, move |err| captor2.capture(err), monitored);

        let result: Result<(), SlateDBError> = task.await.expect("join failure");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskPanic(_))));
        assert!(matches!(
            captor.captured(),
            Some(SlateDBError::BackgroundTaskPanic(_))
        ));
    }

    #[tokio::test]
    async fn test_should_cleanup_when_task_exits() {
        let captor = Arc::new(ErrorCaptor::new());
        let handle = tokio::runtime::Handle::current();
        let captor2 = captor.clone();

        let task = spawn_bg_task(&handle, move |err| captor2.capture(err), async { Ok(()) });

        let result: Result<(), SlateDBError> = task.await.expect("join failure");
        assert!(matches!(result, Ok(())));
        assert!(matches!(
            captor.captured(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
    }

    #[test]
    fn test_should_cleanup_when_thread_exits_with_error() {
        let captor = Arc::new(ErrorCaptor::new());
        let captor2 = captor.clone();

        let thread = spawn_bg_thread(
            "test",
            move |err| captor2.capture(err),
            || Err(SlateDBError::Fenced),
        );

        let result: Result<(), SlateDBError> = thread.join().expect("join failure");
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(captor.captured(), Some(SlateDBError::Fenced)));
    }

    #[test]
    fn test_should_cleanup_when_thread_panics() {
        let captor = Arc::new(ErrorCaptor::new());
        let captor2 = captor.clone();

        let thread = spawn_bg_thread("test", move |err| captor2.capture(err), || panic!("oops"));

        let result: Result<(), SlateDBError> = thread.join().expect("join failure");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskPanic(_))));
        assert!(matches!(
            captor.captured(),
            Some(SlateDBError::BackgroundTaskPanic(_))
        ));
    }

    #[test]
    fn test_should_cleanup_when_thread_exits() {
        let captor = Arc::new(ErrorCaptor::new());
        let captor2 = captor.clone();

        let thread = spawn_bg_thread("test", move |err| captor2.capture(err), || Ok(()));

        let result: Result<(), SlateDBError> = thread.join().expect("join failure");
        assert!(matches!(result, Ok(())));
        assert!(matches!(
            captor.captured(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
    }

    #[tokio::test]
    async fn test_should_only_write_register_once() {
        let register = WatchableOnceCell::new();
        let reader = register.reader();
        assert_eq!(reader.read(), None);
        register.write(123);
        assert_eq!(reader.read(), Some(123));
        register.write(456);
        assert_eq!(reader.read(), Some(123));
    }

    #[tokio::test]
    async fn test_should_return_on_await_written_register() {
        let register = WatchableOnceCell::new();
        let mut reader = register.reader();
        let h = tokio::spawn(async move {
            assert_eq!(reader.await_value().await, 123);
            assert_eq!(reader.await_value().await, 123);
        });
        register.write(123);
        h.await.unwrap();
    }

    #[test]
    fn test_monotonicity_enforcement_on_mono_clock() {
        // Given:
        let clock = Arc::new(TestClock::new());
        let mono_clock = MonotonicClock::new(clock.clone(), 0);

        // When:
        clock.ticker.store(10, SeqCst);
        mono_clock.now().unwrap();
        clock.ticker.store(5, SeqCst);

        // Then:
        if let Err(SlateDBError::InvalidClockTick {
            last_tick,
            next_tick,
        }) = mono_clock.now()
        {
            assert_eq!(last_tick, 10);
            assert_eq!(next_tick, 5);
        } else {
            panic!("Expected InvalidClockTick from mono_clock")
        }
    }

    #[test]
    fn test_monotonicity_enforcement_on_mono_clock_set_tick() {
        // Given:
        let clock = Arc::new(TestClock::new());
        let mono_clock = MonotonicClock::new(clock.clone(), 0);

        // When:
        clock.ticker.store(10, SeqCst);
        mono_clock.now().unwrap();

        // Then:
        if let Err(SlateDBError::InvalidClockTick {
            last_tick,
            next_tick,
        }) = mono_clock.set_last_tick(5)
        {
            assert_eq!(last_tick, 10);
            assert_eq!(next_tick, 5);
        } else {
            panic!("Expected InvalidClockTick from mono_clock")
        }
    }
}
