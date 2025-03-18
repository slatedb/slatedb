use crate::config::ReadLevel::{Committed, Uncommitted};
use crate::config::{Clock, ReadLevel};
use crate::error::SlateDBError;
use crate::error::SlateDBError::BackgroundTaskPanic;
use crate::types::RowEntry;
use bytes::{BufMut, Bytes};
use std::cmp;
use std::future::Future;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tracing::info;

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
/// calls a provided cleanup fn with a reference to the returned result. If the spawned task
/// panics, the cleanup fn is called with Err(BackgroundTaskPanic).
pub(crate) fn spawn_bg_task<F, T, C>(
    handle: &tokio::runtime::Handle,
    cleanup_fn: C,
    future: F,
) -> tokio::task::JoinHandle<Result<T, SlateDBError>>
where
    F: Future<Output = Result<T, SlateDBError>> + Send + 'static,
    T: Send + 'static,
    C: FnOnce(&Result<T, SlateDBError>) + Send + 'static,
{
    let inner_handle = handle.clone();
    handle.spawn(async move {
        let jh = inner_handle.spawn(future);
        match jh.await {
            Ok(result) => {
                cleanup_fn(&result);
                result
            }
            Err(join_err) => {
                // task panic'd or was cancelled
                let err = Err(BackgroundTaskPanic(Arc::new(Mutex::new(
                    join_err
                        .try_into_panic()
                        .unwrap_or_else(|_| Box::new("background task was aborted")),
                ))));
                cleanup_fn(&err);
                err
            }
        }
    })
}

/// Spawn a monitored background os thread. The thread must return a Result<T, SlateDBError>.
/// The thread is spawned by a monitor thread. When the thread exits, the monitor thread
/// calls a provided cleanup fn with the returned result. If the spawned thread panics, the
/// cleanup fn is called with Err(BackgroundTaskPanic).
pub(crate) fn spawn_bg_thread<F, T, C>(
    name: &str,
    cleanup_fn: C,
    f: F,
) -> std::thread::JoinHandle<Result<T, SlateDBError>>
where
    F: FnOnce() -> Result<T, SlateDBError> + Send + 'static,
    T: Send + 'static,
    C: FnOnce(&Result<T, SlateDBError>) + Send + 'static,
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
                    let err = Err(BackgroundTaskPanic(Arc::new(Mutex::new(err))));
                    cleanup_fn(&err);
                    err
                }
                Ok(result) => {
                    cleanup_fn(&result);
                    result
                }
            }
        })
        .expect("failed to create monitor thread")
}

pub(crate) async fn get_now_for_read(
    mono_clock: Arc<MonotonicClock>,
    read_level: ReadLevel,
) -> Result<i64, SlateDBError> {
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
    match read_level {
        Committed => Ok(mono_clock.get_last_durable_tick()),
        Uncommitted => mono_clock.now().await,
    }
}

pub(crate) fn is_not_expired(entry: &RowEntry, now: i64) -> bool {
    if let Some(expire_ts) = entry.expire_ts {
        expire_ts > now
    } else {
        true
    }
}

pub(crate) fn bg_task_result_into_err(result: &Result<(), SlateDBError>) -> SlateDBError {
    match result {
        Ok(_) => SlateDBError::BackgroundTaskShutdown,
        Err(err) => err.clone(),
    }
}

/// SlateDB uses MonotonicClock internally so that it can enforce that clock ticks
/// from the underlying implementation are monotonically increasing
pub(crate) struct MonotonicClock {
    pub(crate) last_tick: AtomicI64,
    pub(crate) last_durable_tick: AtomicI64,
    delegate: Arc<dyn Clock + Send + Sync>,
}

impl MonotonicClock {
    pub(crate) fn new(delegate: Arc<dyn Clock + Send + Sync>, init_tick: i64) -> Self {
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
        self.last_durable_tick.fetch_max(tick, SeqCst)
    }

    pub(crate) fn get_last_durable_tick(&self) -> i64 {
        self.last_durable_tick.load(SeqCst)
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

fn bytes_into_minimal_vec(bytes: &Bytes) -> Vec<u8> {
    let mut clamped = Vec::new();
    clamped.reserve_exact(bytes.len());
    clamped.put_slice(bytes.as_ref());
    clamped
}

pub(crate) fn clamp_allocated_size_bytes(bytes: &Bytes) -> Bytes {
    bytes_into_minimal_vec(bytes).into()
}

pub(crate) fn now_systime(clock: &dyn Clock) -> SystemTime {
    chrono::DateTime::from_timestamp_millis(clock.now())
        .map(SystemTime::from)
        .expect("Failed to convert Clock time to SystemTime")
}

/// Computes the "index key" (lowest bound) for an SST index block, ie a key that's greater
/// than all keys in the previous block and less than or equal to all keys in the new block
pub(crate) fn index_key(prev_block_last_key: Option<Bytes>, this_block_first_key: &Bytes) -> Bytes {
    if let Some(prev_key) = prev_block_last_key {
        compute_lower_bound(&prev_key, this_block_first_key)
    } else {
        Bytes::new()
    }
}

fn compute_lower_bound(prev_block_last_key: &Bytes, this_block_first_key: &Bytes) -> Bytes {
    for i in 0..prev_block_last_key.len() {
        if prev_block_last_key[i] != this_block_first_key[i] {
            return Bytes::copy_from_slice(&this_block_first_key[..i + 1]);
        }
    }

    // if we didn't find a mismatch yet then the prev block's key must be shorter,
    // so just use the common prefix plus the next byte in this block's key
    Bytes::copy_from_slice(&this_block_first_key[..prev_block_last_key.len() + 1])
}

#[cfg(test)]
mod tests {
    use crate::error::SlateDBError;
    use crate::test_utils::TestClock;
    use crate::utils::{
        bytes_into_minimal_vec, clamp_allocated_size_bytes, index_key, spawn_bg_task,
        spawn_bg_thread, MonotonicClock, WatchableOnceCell,
    };
    use bytes::{BufMut, Bytes, BytesMut};
    use parking_lot::Mutex;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::Arc;
    use std::time::Duration;

    struct ResultCaptor<T: Clone> {
        error: Mutex<Option<Result<T, SlateDBError>>>,
    }

    impl<T: Clone> ResultCaptor<T> {
        fn new() -> Self {
            Self {
                error: Mutex::new(None),
            }
        }

        fn capture(&self, result: &Result<T, SlateDBError>) {
            let mut guard = self.error.lock();
            let prev = guard.replace(result.clone());
            assert!(prev.is_none());
        }

        fn captured(&self) -> Option<Result<T, SlateDBError>> {
            self.error.lock().clone()
        }
    }

    #[test]
    fn test_should_return_empty_for_index_of_first_block() {
        let this_block_first_key = Bytes::from(vec![0x01, 0x02, 0x03]);
        let result = index_key(None, &this_block_first_key);

        assert_eq!(result, Bytes::new());
    }

    #[test]
    fn test_should_compute_index_key() {
        assert_eq!(
            index_key(
                Some(Bytes::from(String::from("aaaac"))),
                &Bytes::from(String::from("abaaa"))
            ),
            Bytes::from(String::from("ab"))
        );

        assert_eq!(
            index_key(
                Some(Bytes::from(String::from("ababc"))),
                &Bytes::from(String::from("abacd"))
            ),
            Bytes::from(String::from("abac"))
        );

        assert_eq!(
            index_key(
                Some(Bytes::from(String::from(""))),
                &Bytes::from(String::from("a"))
            ),
            Bytes::from(String::from("a"))
        );

        assert_eq!(
            index_key(
                Some(Bytes::from(String::from("cc"))),
                &Bytes::from(String::from("ccccccc"))
            ),
            Bytes::from(String::from("ccc"))
        );

        assert_eq!(
            index_key(
                Some(Bytes::from(String::from("eed"))),
                &Bytes::from(String::from("eee"))
            ),
            Bytes::from(String::from("eee"))
        );
    }

    #[tokio::test]
    async fn test_should_cleanup_when_task_exits_with_error() {
        let captor = Arc::new(ResultCaptor::new());
        let handle = tokio::runtime::Handle::current();
        let captor2 = captor.clone();

        let task = spawn_bg_task(&handle, move |err| captor2.capture(err), async {
            Err(SlateDBError::Fenced)
        });

        let result: Result<(), SlateDBError> = task.await.expect("join failure");
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(captor.captured(), Some(Err(SlateDBError::Fenced))));
    }

    #[tokio::test]
    async fn test_should_cleanup_when_task_panics() {
        let monitored = async {
            panic!("oops");
        };
        let captor = Arc::new(ResultCaptor::new());
        let handle = tokio::runtime::Handle::current();
        let captor2 = captor.clone();

        let task = spawn_bg_task(&handle, move |err| captor2.capture(err), monitored);

        let result: Result<(), SlateDBError> = task.await.expect("join failure");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskPanic(_))));
        assert!(matches!(
            captor.captured(),
            Some(Err(SlateDBError::BackgroundTaskPanic(_)))
        ));
    }

    #[tokio::test]
    async fn test_should_cleanup_when_task_exits() {
        let captor = Arc::new(ResultCaptor::new());
        let handle = tokio::runtime::Handle::current();
        let captor2 = captor.clone();

        let task = spawn_bg_task(&handle, move |err| captor2.capture(err), async { Ok(()) });

        let result: Result<(), SlateDBError> = task.await.expect("join failure");
        assert!(matches!(result, Ok(())));
        assert!(matches!(captor.captured(), Some(Ok(()))));
    }

    #[test]
    fn test_should_cleanup_when_thread_exits_with_error() {
        let captor = Arc::new(ResultCaptor::new());
        let captor2 = captor.clone();

        let thread = spawn_bg_thread(
            "test",
            move |err| captor2.capture(err),
            || Err(SlateDBError::Fenced),
        );

        let result: Result<(), SlateDBError> = thread.join().expect("join failure");
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(captor.captured(), Some(Err(SlateDBError::Fenced))));
    }

    #[test]
    fn test_should_cleanup_when_thread_panics() {
        let captor = Arc::new(ResultCaptor::new());
        let captor2 = captor.clone();

        let thread = spawn_bg_thread("test", move |err| captor2.capture(err), || panic!("oops"));

        let result: Result<(), SlateDBError> = thread.join().expect("join failure");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskPanic(_))));
        assert!(matches!(
            captor.captured(),
            Some(Err(SlateDBError::BackgroundTaskPanic(_)))
        ));
    }

    #[test]
    fn test_should_cleanup_when_thread_exits() {
        let captor = Arc::new(ResultCaptor::new());
        let captor2 = captor.clone();

        let thread = spawn_bg_thread("test", move |err| captor2.capture(err), || Ok(()));

        let result: Result<(), SlateDBError> = thread.join().expect("join failure");
        assert!(matches!(result, Ok(())));
        assert!(matches!(captor.captured(), Some(Ok(()))));
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

    #[tokio::test]
    async fn test_monotonicity_enforcement_on_mono_clock() {
        // Given:
        let clock = Arc::new(TestClock::new());
        let mono_clock = MonotonicClock::new(clock.clone(), 0);

        // When:
        clock.ticker.store(10, SeqCst);
        mono_clock.now().await.unwrap();
        clock.ticker.store(5, SeqCst);

        // Then:
        if let Err(SlateDBError::InvalidClockTick {
            last_tick,
            next_tick,
        }) = mono_clock.now().await
        {
            assert_eq!(last_tick, 10);
            assert_eq!(next_tick, 5);
        } else {
            panic!("Expected InvalidClockTick from mono_clock")
        }
    }

    #[tokio::test]
    async fn test_monotonicity_enforcement_on_mono_clock_set_tick() {
        // Given:
        let clock = Arc::new(TestClock::new());
        let mono_clock = MonotonicClock::new(clock.clone(), 0);

        // When:
        clock.ticker.store(10, SeqCst);
        mono_clock.now().await.unwrap();

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

    #[tokio::test(start_paused = true)]
    async fn test_await_valid_tick() {
        // the delegate clock is behind the mono clock by 100ms
        let delegate_clock = Arc::new(TestClock::new());
        let mono_clock = MonotonicClock::new(delegate_clock.clone(), 100);

        tokio::spawn({
            let delegate_clock = delegate_clock.clone();
            async move {
                // wait for half the time it would wait for
                tokio::time::sleep(Duration::from_millis(50)).await;
                delegate_clock.ticker.store(101, SeqCst);
            }
        });

        let tick_future = mono_clock.now();
        tokio::time::advance(Duration::from_millis(100)).await;

        let result = tick_future.await;
        assert_eq!(result.unwrap(), 101);
    }

    #[tokio::test(start_paused = true)]
    async fn test_await_valid_tick_failure() {
        // the delegate clock is behind the mono clock by 100ms
        let delegate_clock = Arc::new(TestClock::new());
        let mono_clock = MonotonicClock::new(delegate_clock.clone(), 100);

        // wait for 10ms after the maximum time it should accept to wait
        let tick_future = mono_clock.now();
        tokio::time::advance(Duration::from_millis(110)).await;

        let result = tick_future.await;
        assert!(result.is_err());
    }

    #[test]
    fn test_should_clamp_bytes_to_minimal_vec() {
        let mut bytes = BytesMut::with_capacity(2048);
        bytes.put_bytes(0u8, 2048);
        let bytes = bytes.freeze();
        let slice = bytes.slice(100..1124);

        let clamped = bytes_into_minimal_vec(&slice);

        assert_eq!(slice.as_ref(), clamped.as_slice());
        assert_eq!(clamped.capacity(), 1024);
    }

    #[test]
    fn test_should_clamp_bytes_and_preserve_data() {
        let mut bytes = BytesMut::with_capacity(2048);
        bytes.put_bytes(0u8, 2048);
        let bytes = bytes.freeze();
        let slice = bytes.slice(100..1124);

        let clamped = clamp_allocated_size_bytes(&slice);

        assert_eq!(clamped, slice);
        // It doesn't seem to be possible to assert that the clamped block's data is actually
        // a buffer of the minimal size, as Bytes doesn't expose the underlying buffer's
        // capacity. The best we can do is assert it allocated a new buffer.
        assert_ne!(clamped.as_ptr(), slice.as_ptr());
    }
}
