use crate::clock::{MonotonicClock, SystemClock};
use crate::config::DurabilityLevel;
use crate::config::DurabilityLevel::{Memory, Remote};
use crate::error::SlateDBError;
use crate::error::SlateDBError::BackgroundTaskPanic;
use crate::types::RowEntry;
use bytes::{BufMut, Bytes};
use futures::FutureExt;
use rand::RngCore;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedSender;
use ulid::Ulid;
use uuid::Uuid;

static EMPTY_KEY: Bytes = Bytes::new();

#[derive(Clone)]
pub(crate) struct WatchableOnceCell<T: Clone> {
    rx: tokio::sync::watch::Receiver<Option<T>>,
    tx: tokio::sync::watch::Sender<Option<T>>,
}

#[derive(Clone)]
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

/// Spawn a background tokio task. The task must return a Result<T, SlateDBError>.
/// When the task exits, the provided cleanup fn is called with a reference to the returned
/// result. If the task panics, the cleanup fn is called with Err(BackgroundTaskPanic).
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
    // NOTE: It is critical that the future lives as long as the cleanup_fn.
    //       Otherwise, there is a gap where everything owned by the future is dropped
    //       before the cleanup_fn runs. Since our cleanup_fn's often set error states
    //       on the db, this would result in a gap where the db is not in an error state
    //       but resources such as channels have been dropped or closed. See #623 for
    //       details.
    let wrapped = AssertUnwindSafe(future).catch_unwind().map(move |outcome| {
        let result = match outcome {
            Ok(Ok(val)) => Ok(val),
            Ok(Err(e)) => Err(e),
            Err(panic) => Err(BackgroundTaskPanic(Arc::new(Mutex::new(panic)))),
        };
        cleanup_fn(&result);
        result
    });
    handle.spawn(wrapped)
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

/// An extension trait that adds a `.send_safely(...)` method to tokio's `UnboundedSender<T>`.
pub trait SendSafely<T> {
    /// Attempts to send a message to the channel, and if the channel is closed, returns the error
    /// in `error_reader` if it is set, otherwise panics.
    ///
    /// This is useful for handling shutdown race conditions where the receiver's channel is dropped
    /// before the sender is shut down.`
    fn send_safely(
        &self,
        error_reader: WatchableOnceCellReader<SlateDBError>,
        message: T,
    ) -> Result<(), SlateDBError>;
}

#[allow(clippy::panic, clippy::disallowed_methods)]
impl<T> SendSafely<T> for UnboundedSender<T> {
    #[inline]
    fn send_safely(
        &self,
        error_reader: WatchableOnceCellReader<SlateDBError>,
        message: T,
    ) -> Result<(), SlateDBError> {
        match self.send(message) {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(err) = error_reader.read() {
                    Err(err)
                } else {
                    panic!("Failed to send message to unbounded channel: {}", e);
                }
            }
        }
    }
}

/// Trait for generating UUIDs and ULIDs from a random number generator.
pub trait IdGenerator {
    fn gen_uuid(&mut self) -> Uuid;
    fn gen_ulid(&mut self) -> Ulid;
}

impl<R: RngCore> IdGenerator for R {
    /// Generates a random UUID using the provided RNG.
    fn gen_uuid(&mut self) -> Uuid {
        let mut bytes = [0u8; 16];
        self.fill_bytes(&mut bytes);
        // set version = 4
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        // set variant = RFC4122
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Uuid::from_bytes(bytes)
    }

    /// Generates a random ULID using the provided RNG.
    fn gen_ulid(&mut self) -> Ulid {
        Ulid::with_source(self)
    }
}

/// A timeout wrapper for futures that returns a SlateDBError::Timeout if the future
/// does not complete within the specified duration.
pub async fn timeout<T>(
    clock: Arc<dyn SystemClock>,
    duration: Duration,
    future: impl Future<Output = Result<T, SlateDBError>> + Send,
) -> Result<T, SlateDBError> {
    tokio::select! {
        biased;
        res = future => res,
        _ = clock.sleep(duration) => Err(SlateDBError::Timeout {
            msg: "Timeout".to_string(),
        })
    }
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

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_timeout_completes_before_expiry() {
        use crate::{clock::MockSystemClock, utils::timeout};

        // Given: a mock clock and a future that completes quickly
        let clock = Arc::new(MockSystemClock::new());

        // When: we execute a future with a timeout
        let completed_future = async { Ok::<_, SlateDBError>(42) };
        let timeout_future = timeout(clock, Duration::from_millis(100), completed_future);

        // Then: the future should complete successfully with the expected value
        let result = timeout_future.await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_timeout_expires() {
        use std::sync::atomic::AtomicBool;

        use crate::clock::{MockSystemClock, SystemClock};
        use crate::utils::timeout;

        // Given: a mock clock and a future that will never complete
        let clock = Arc::new(MockSystemClock::new());
        let never_completes = std::future::pending::<Result<(), SlateDBError>>();

        // When: we execute the future with a timeout and advance the clock past the timeout duration
        let timeout_future = timeout(clock.clone(), Duration::from_millis(100), never_completes);
        let done = Arc::new(AtomicBool::new(false));
        let this_done = done.clone();

        tokio::spawn(async move {
            while !this_done.load(SeqCst) {
                clock.advance(Duration::from_millis(100)).await;
                // Yield or else the scheduler keeps picking this loop, which
                // the sleep task forever.
                tokio::task::yield_now().await;
            }
        });

        // Then: the future should complete with a timeout error
        let result = timeout_future.await;
        done.store(true, SeqCst);
        assert!(matches!(result, Err(SlateDBError::Timeout { .. })));
    }

    #[tokio::test]
    #[cfg(feature = "test-util")]
    async fn test_timeout_respects_biased_select() {
        use crate::{clock::MockSystemClock, utils::timeout};

        // Given: a mock clock and two futures that complete simultaneously
        let clock = Arc::new(MockSystemClock::new());
        let completes_immediately = async { Ok::<_, SlateDBError>(42) };

        // When: we execute the future with a timeout and both are ready immediately
        let timeout_future = timeout(clock, Duration::from_millis(100), completes_immediately);

        // Then: because of the 'biased' select, the future should complete with the value
        // rather than timing out, even though both are ready
        let result = timeout_future.await;
        assert_eq!(result.unwrap(), 42);
    }
}
