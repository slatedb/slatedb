use crate::clock::MonotonicClock;
use crate::config::DurabilityLevel;
use crate::config::DurabilityLevel::{Memory, Remote};
use crate::error::SlateDBError;
use crate::error::SlateDBError::BackgroundTaskPanic;
use crate::types::RowEntry;
use bytes::{BufMut, Bytes};
use rand::RngCore;
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ulid::Ulid;
use uuid::Uuid;

static EMPTY_KEY: Bytes = Bytes::new();

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

pub(crate) fn system_time_to_millis(system_time: SystemTime) -> i64 {
    system_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

pub(crate) fn system_time_from_millis(ms: i64) -> SystemTime {
    if ms >= 0 {
        // positive or zero: just add
        UNIX_EPOCH + Duration::from_millis(ms as u64)
    } else {
        // negative (including i64::MIN): convert to i128, take abs, cast back to u64
        let abs_ms = (ms as i128).unsigned_abs() as u64;
        UNIX_EPOCH - Duration::from_millis(abs_ms)
    }
}

pub(crate) async fn get_now_for_read(
    mono_clock: Arc<MonotonicClock>,
    durability_level: DurabilityLevel,
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
    match durability_level {
        Remote => Ok(mono_clock.get_last_durable_tick()),
        Memory => mono_clock.now().await,
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

/// Computes the "index key" (lowest bound) for an SST index block, ie a key that's greater
/// than all keys in the previous block and less than or equal to all keys in the new block
pub(crate) fn compute_index_key(
    prev_block_last_key: Option<Bytes>,
    this_block_first_key: &Bytes,
) -> Bytes {
    if let Some(prev_key) = prev_block_last_key {
        compute_lower_bound(&prev_key, this_block_first_key)
    } else {
        EMPTY_KEY.clone()
    }
}

fn compute_lower_bound(prev_block_last_key: &Bytes, this_block_first_key: &Bytes) -> Bytes {
    assert!(!prev_block_last_key.is_empty() && !this_block_first_key.is_empty());

    for i in 0..prev_block_last_key.len() {
        if prev_block_last_key[i] != this_block_first_key[i] {
            return this_block_first_key.slice(..i + 1);
        }
    }

    // if the keys are equal, just use the full key
    if prev_block_last_key.len() == this_block_first_key.len() {
        return this_block_first_key.clone();
    }

    // if we didn't find a mismatch yet then the prev block's key must be shorter,
    // so just use the common prefix plus the next byte in this block's key
    this_block_first_key.slice(..prev_block_last_key.len() + 1)
}

#[derive(Debug)]
pub(crate) struct MonotonicSeq {
    val: AtomicU64,
}

impl MonotonicSeq {
    pub fn new(initial_value: u64) -> Self {
        Self {
            val: AtomicU64::new(initial_value),
        }
    }

    pub fn next(&self) -> u64 {
        self.val.fetch_add(1, SeqCst) + 1
    }

    pub fn store(&self, value: u64) {
        self.val.store(value, SeqCst);
    }

    pub fn load(&self) -> u64 {
        self.val.load(SeqCst)
    }

    pub fn store_if_greater(&self, value: u64) {
        self.val.fetch_max(value, SeqCst);
    }
}

// TODO replace this with our rand module
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
pub(crate) fn uuid() -> Uuid {
    let mut random_bytes = [0; 16];
    rand::thread_rng().fill_bytes(&mut random_bytes);
    uuid::Builder::from_random_bytes(random_bytes).into_uuid()
}

// TODO replace this with our rand module
#[allow(clippy::disallowed_methods)]
pub(crate) fn ulid() -> Ulid {
    Ulid::with_source(&mut rand::thread_rng())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::clock::MonotonicClock;
    use crate::error::SlateDBError;
    use crate::test_utils::TestClock;
    use crate::utils::{
        bytes_into_minimal_vec, clamp_allocated_size_bytes, compute_index_key, spawn_bg_task,
        WatchableOnceCell,
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
        let result = compute_index_key(None, &this_block_first_key);

        assert_eq!(result, &Bytes::new());
    }

    #[rstest]
    #[case(Some("aaaac"), "abaaa", "ab")]
    #[case(Some("ababc"), "abacd", "abac")]
    #[case(Some("cc"), "ccccccc", "ccc")]
    #[case(Some("eed"), "eee", "eee")]
    #[case(Some("abcdef"), "abcdef", "abcdef")]
    fn test_should_compute_index_key(
        #[case] prev_block_last_key: Option<&'static str>,
        #[case] this_block_first_key: &'static str,
        #[case] expected_index_key: &'static str,
    ) {
        assert_eq!(
            compute_index_key(
                prev_block_last_key.map(|s| Bytes::from(s.to_string())),
                &Bytes::from(this_block_first_key.to_string())
            ),
            Bytes::from_static(expected_index_key.as_bytes())
        );
    }

    #[rstest]
    #[case(Some(""), "a")]
    #[case(Some("a"), "")]
    #[should_panic]
    fn test_should_panic_on_empty_keys(
        #[case] prev_block_last_key: Option<&'static str>,
        #[case] this_block_first_key: &'static str,
    ) {
        compute_index_key(
            prev_block_last_key.map(|s| Bytes::from(s.to_string())),
            &Bytes::from(this_block_first_key.to_string()),
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
