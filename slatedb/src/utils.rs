use crate::clock::{MonotonicClock, SystemClock};
use crate::config::DurabilityLevel;
use crate::config::DurabilityLevel::{Memory, Remote};
use crate::db_state::SortedRun;
use crate::error::SlateDBError;
use crate::error::SlateDBError::BackgroundTaskPanic;
use crate::types::RowEntry;
use bytes::{BufMut, Bytes};
use futures::FutureExt;
use rand::{Rng, RngCore};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use ulid::Ulid;
use uuid::Uuid;

use futures::StreamExt;
use std::collections::VecDeque;

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
    fn gen_ulid(&mut self, clock: &dyn SystemClock) -> Ulid;
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

    /// Generates a random ULID using the provided RNG. The clock is used to generate
    /// the timestamp component of the ULID.
    fn gen_ulid(&mut self, clock: &dyn SystemClock) -> Ulid {
        let now = u64::try_from(clock.now().timestamp_millis())
            .expect("timestamp outside u64 range in gen_ulid");
        let random_bytes = self.random::<u128>();
        Ulid::from_parts(now, random_bytes)
    }
}

/// A timeout wrapper for futures that returns a SlateDBError::Timeout if the future
/// does not complete within the specified duration.
///
/// Arguments:
/// - `clock`: The clock to use for the timeout.
/// - `duration`: The duration to wait for the future to complete.
/// - `op`: The name of the operation that will time out, for logging purposes.
/// - `future`: The future to timeout
///
/// Returns:
/// - `Ok(T)`: If the future completes within the specified duration.
/// - `Err(SlateDBError::Timeout)`: If the future does not complete within the specified duration.
pub async fn timeout<T>(
    clock: Arc<dyn SystemClock>,
    duration: Duration,
    op: &'static str,
    future: impl Future<Output = Result<T, SlateDBError>> + Send,
) -> Result<T, SlateDBError> {
    tokio::select! {
        biased;
        res = future => res,
        _ = clock.sleep(duration) => Err(SlateDBError::Timeout {
            op,
            backoff: duration,
        })
    }
}

/// A simple bit-level writer that packs bits into a `[u8]`
pub(crate) struct BitWriter {
    buf: Vec<u8>,
    cur: u8, // the currently assembling byte
    n: u8,   // the number of "filled" bytes in `cur`
}

impl BitWriter {
    pub(crate) fn new() -> Self {
        BitWriter {
            buf: Vec::new(),
            cur: 0,
            n: 0,
        }
    }

    /// Push a single bit into the buffer
    pub(crate) fn push(&mut self, bit: bool) {
        if bit {
            self.cur |= 1 << (7 - self.n);
        }
        self.n += 1;
        if self.n == 8 {
            self.flush_byte();
        }
    }

    /// Push up to a u32 into the buffer, only considering
    /// the first `bits` number of bits
    pub(crate) fn push32(&mut self, value: u32, bits: u8) {
        // writes the lowest `bits` bits from `value` into
        // the current buffer (most significant bits first)
        for i in (0..bits).rev() {
            let bit = ((value >> i) & 1) != 0;
            self.push(bit);
        }
    }

    /// Push up to a u32 into the buffer, only considering
    /// the first `bits` number of bits
    pub(crate) fn push64(&mut self, value: u64, bits: u8) {
        for i in (0..bits).rev() {
            let bit = ((value >> i) & 1) != 0;
            self.push(bit);
        }
    }

    fn flush_byte(&mut self) {
        self.buf.push(self.cur);
        self.cur = 0;
        self.n = 0;
    }

    /// Extrat the finalized buffer from the writer
    pub(crate) fn finish(mut self) -> Vec<u8> {
        if self.n > 0 {
            self.buf.push(self.cur);
        }
        self.buf
    }
}

pub(crate) struct BitReader<'a> {
    buf: &'a [u8],
    byte_pos: usize,
    bit_pos: u8, // 0..8; next bit to read is at (7 - bit_pos)
}

impl<'a> BitReader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        BitReader {
            buf,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    /// Read one bit, or None if we've exhausted the buffer.
    pub(crate) fn read_bit(&mut self) -> Option<bool> {
        if self.byte_pos >= self.buf.len() {
            return None;
        }
        let byte = self.buf[self.byte_pos];
        let bit = ((byte >> (7 - self.bit_pos)) & 1) != 0;
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.bit_pos = 0;
            self.byte_pos += 1;
        }
        Some(bit)
    }

    /// Read `bits` bits, MSB first, returning them as the low `bits` of a u32.
    pub(crate) fn read32(&mut self, bits: u8) -> Option<u32> {
        let mut val = 0u32;
        for _ in 0..bits {
            val <<= 1;
            match self.read_bit() {
                Some(true) => val |= 1,
                Some(false) => (),
                None => return None,
            }
        }
        Some(val)
    }

    /// Read `bits` bits, MSB first, returning them as the low `bits` of a u64.
    pub(crate) fn read64(&mut self, bits: u8) -> Option<u64> {
        let mut val = 0u64;
        for _ in 0..bits {
            val <<= 1;
            match self.read_bit() {
                Some(true) => val |= 1,
                Some(false) => (),
                None => return None,
            }
        }
        Some(val)
    }
}

/// Sign‐extend the low `bits` of `val` into a full i32.
pub(crate) fn sign_extend(val: u32, bits: u8) -> i32 {
    let shift = 32 - bits;
    ((val << shift) as i32) >> shift
}

/// Compute a bounded concurrency for iterator construction.
///
/// Arguments:
/// - `l0_count`: number of L0 SSTables to scan
/// - `srs`: slice of sorted runs to scan; each run’s SST count is summed
/// - `cap`: hard ceiling for concurrency (minimum effective value is 1)
///
/// Returns:
/// - The effective max parallelism.
pub(crate) fn compute_max_parallel(l0_count: usize, srs: &[SortedRun], cap: usize) -> usize {
    let total_ssts = l0_count + srs.iter().map(|sr| sr.ssts.len()).sum::<usize>();
    total_ssts.min(cap).max(1)
}

/// Concurrently build items with a bounded level of parallelism.
///
/// This function maps each input to an async task using the provided factory `f`,
/// runs up to `max_parallel` tasks at once, and collects all successful results.
/// If any task returns `Err`, the first error is returned.
///
/// Arguments:
/// - `inputs`: the items to process
/// - `max_parallel`: maximum number of in-flight tasks (values <= 0 are clamped to 1)
/// - `f`: per-item async factory that returns `Result<Option<T>, SlateDBError>`
///
/// Returns:
/// - `Ok(VecDeque<T>)` containing all successfully built items (in completion order)
/// - `Err(SlateDBError)` if any task fails (short-circuits on the first error observed)
///
/// Concurrency & ordering:
/// - Tasks are polled concurrently up to `max_parallel` using `buffer_unordered`.
/// - Completion order is not guaranteed; results are pushed as tasks finish.
#[allow(clippy::redundant_closure)]
pub(crate) async fn build_concurrent<I, T, F, Fut>(
    inputs: I,
    max_parallel: usize,
    f: F,
) -> Result<VecDeque<T>, SlateDBError>
where
    I: IntoIterator,
    I::Item: Send,
    T: Send,
    F: Fn(I::Item) -> Fut + Send,
    Fut: std::future::Future<Output = Result<Option<T>, SlateDBError>> + Send,
{
    let mut out = VecDeque::new();

    let results = futures::stream::iter(inputs.into_iter().map(move |it| f(it)))
        .buffer_unordered(max_parallel.max(1))
        .collect::<Vec<_>>()
        .await;

    for r in results {
        match r {
            Ok(Some(t)) => out.push_back(t),
            Ok(None) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::clock::MonotonicClock;
    use crate::error::SlateDBError;
    use crate::test_utils::TestClock;
    use crate::utils::{
        build_concurrent, bytes_into_minimal_vec, clamp_allocated_size_bytes, compute_index_key,
        compute_max_parallel, spawn_bg_task, BitReader, BitWriter, WatchableOnceCell,
    };
    use bytes::{BufMut, Bytes, BytesMut};
    use parking_lot::Mutex;
    use std::collections::VecDeque;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicUsize, Ordering};
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
        let timeout_future = timeout(clock, Duration::from_millis(100), "test", completed_future);

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
        let timeout_future = timeout(
            clock.clone(),
            Duration::from_millis(100),
            "test",
            never_completes,
        );
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
        let timeout_future = timeout(
            clock,
            Duration::from_millis(100),
            "test",
            completes_immediately,
        );

        // Then: because of the 'biased' select, the future should complete with the value
        // rather than timing out, even though both are ready
        let result = timeout_future.await;
        assert_eq!(result.unwrap(), 42);
    }

    #[rstest]
    #[case("alternating_bits", vec![true, false, true, false, true, false, true, false], vec![], vec![], vec![0xAA])]
    #[case("u64_value", vec![], vec![(0xAB, 8)], vec![], vec![0xAB])]
    #[case("partial_and_multiple_bytes", vec![true, false], vec![(0x3F, 6), (0xCD, 8)], vec![], vec![0xBF, 0xCD])]
    #[case("empty_writer", vec![], vec![], vec![], vec![])]
    #[case("single_bit_true", vec![true], vec![], vec![], vec![0x80])]
    #[case("single_bit_false", vec![false], vec![], vec![], vec![0x00])]
    #[case("all_zeros", vec![false, false, false, false, false, false, false, false], vec![], vec![], vec![0x00])]
    #[case("all_ones", vec![true, true, true, true, true, true, true, true], vec![], vec![], vec![0xFF])]
    #[case("partial_byte_padding", vec![true, false, true], vec![], vec![], vec![0xA0])]
    #[case("push32_single_bit", vec![], vec![(1, 1)], vec![], vec![0x80])]
    #[case("push32_zero_bits", vec![], vec![(0xFF, 0)], vec![], vec![])]
    #[case("push32_max_bits", vec![], vec![(0xDEADBEEF, 32)], vec![], vec![0xDE, 0xAD, 0xBE, 0xEF])]
    #[case("push64_operations", vec![], vec![], vec![(0x123456789ABCDEF0, 64)], vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0])]
    #[case("push64_partial", vec![], vec![], vec![(0xABCD, 12)], vec![0xBC, 0xD0])]
    #[case("mixed_operations", vec![true], vec![(0x7F, 7)], vec![], vec![0xFF])]
    #[case("boundary_crossing", vec![true, false, true, false], vec![(0xF0, 4)], vec![], vec![0xA0])]
    #[case("multiple_partial_bytes", vec![true], vec![(0x5, 3), (0x2, 2), (0x1, 2)], vec![], vec![0xD9])]
    fn test_bit_writer(
        #[case] _description: &str,
        #[case] individual_bits: Vec<bool>,
        #[case] push32_operations: Vec<(u32, u8)>,
        #[case] push64_operations: Vec<(u64, u8)>,
        #[case] expected: Vec<u8>,
    ) {
        // Given: a new BitWriter
        let mut writer = BitWriter::new();

        // When: we perform the specified operations
        // Push individual bits first
        for bit in individual_bits {
            writer.push(bit);
        }
        // Then push32 operations
        for (value, bits) in push32_operations {
            writer.push32(value, bits);
        }
        // Finally push64 operations
        for (value, bits) in push64_operations {
            writer.push64(value, bits);
        }
        let result = writer.finish();

        // Then: it should return the expected result
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case("alternating_bits", vec![0xAA], vec![true, false, true, false, true, false, true, false], vec![], vec![])]
    #[case("u64_value", vec![0xAB], vec![], vec![(0xAB, 8)], vec![])]
    #[case("partial_and_multiple_bytes", vec![0xBF, 0xCD], vec![true, false], vec![(0x3F, 6), (0xCD, 8)], vec![])]
    #[case("empty_reader", vec![], vec![], vec![], vec![])]
    #[case("single_bit_true", vec![0x80], vec![true], vec![], vec![])]
    #[case("single_bit_false", vec![0x00], vec![false], vec![], vec![])]
    #[case("all_zeros", vec![0x00], vec![false, false, false, false, false, false, false, false], vec![], vec![])]
    #[case("all_ones", vec![0xFF], vec![true, true, true, true, true, true, true, true], vec![], vec![])]
    #[case("partial_byte_padding", vec![0xA0], vec![true, false, true], vec![], vec![])]
    #[case("push32_single_bit", vec![0x80], vec![], vec![(1, 1)], vec![])]
    #[case("push32_zero_bits", vec![], vec![], vec![], vec![])]
    #[case("push32_max_bits", vec![0xDE, 0xAD, 0xBE, 0xEF], vec![], vec![(0xDEADBEEF, 32)], vec![])]
    #[case("push64_operations", vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0], vec![], vec![], vec![(0x123456789ABCDEF0, 64)])]
    #[case("push64_partial", vec![0xBC, 0xD0], vec![], vec![], vec![(0xBCD, 12)])]
    #[case("mixed_operations", vec![0xFF], vec![true], vec![(0x7F, 7)], vec![])]
    #[case("boundary_crossing", vec![0xA0], vec![true, false, true, false], vec![(0x0, 4)], vec![])]
    #[case("multiple_partial_bytes", vec![0xD9], vec![true], vec![(0x5, 3), (0x2, 2), (0x1, 2)], vec![])]
    fn test_bit_reader(
        #[case] _description: &str,
        #[case] input_bytes: Vec<u8>,
        #[case] expected_individual_bits: Vec<bool>,
        #[case] expected_read32_operations: Vec<(u32, u8)>,
        #[case] expected_read64_operations: Vec<(u64, u8)>,
    ) {
        // Given: a BitReader with the input bytes

        let mut reader = BitReader::new(&input_bytes);

        // When: we read individual bits
        for expected_bit in expected_individual_bits {
            let actual_bit = reader.read_bit();
            assert_eq!(actual_bit, Some(expected_bit));
        }

        // Then: read32 operations
        for (expected_value, bits) in expected_read32_operations {
            let actual_value = reader.read32(bits);
            assert_eq!(actual_value, Some(expected_value));
        }

        // Finally: read64 operations
        for (expected_value, bits) in expected_read64_operations {
            let actual_value = reader.read64(bits);
            assert_eq!(actual_value, Some(expected_value));
        }

        // Verify we've consumed all bits for non-empty inputs
        if !input_bytes.is_empty() {
            // For partial bytes, there might be padding bits we should be able to read as false
            let next_bit = reader.read_bit();
            if let Some(bit) = next_bit {
                // If there are remaining bits, they should be padding (false)
                assert!(!bit);
            }
        }
    }

    #[test]
    fn test_bit_reader_exhaustion() {
        // Test that BitReader properly returns None when exhausted
        let bytes = vec![0xFF]; // Single byte with all bits set
        let mut reader = BitReader::new(&bytes);

        // Read all 8 bits
        for _ in 0..8 {
            assert_eq!(reader.read_bit(), Some(true));
        }

        // Next read should return None
        assert_eq!(reader.read_bit(), None);
        assert_eq!(reader.read32(1), None);
        assert_eq!(reader.read64(1), None);
    }

    #[test]
    fn test_compute_max_parallel_min_and_cap() {
        // No SRs; total = l0_count
        assert_eq!(compute_max_parallel(5, &[], 8), 5);
        // Cap applies
        assert_eq!(compute_max_parallel(10, &[], 8), 8);
        // Clamp to at least 1 even when cap = 0
        assert_eq!(compute_max_parallel(0, &[], 0), 1);
    }

    // Filters out None; collects only Some(T)
    #[tokio::test]
    async fn test_build_iters_concurrent_option_filters_none() {
        let inputs = 0..10usize;
        let out: VecDeque<usize> = build_concurrent(inputs, 4, |x| async move {
            // Keep evens, drop odds
            if x % 2 == 0 {
                Ok(Some(x * 3))
            } else {
                Ok(None)
            }
        })
        .await
        .expect("should succeed");

        // Expect evens only (0,2,4,6,8) multiplied by 3
        let mut got: Vec<_> = out.into_iter().collect();
        got.sort_unstable();
        assert_eq!(got, vec![0, 6, 12, 18, 24]);
    }

    // Propagates first error
    #[tokio::test]
    async fn test_build_iters_concurrent_option_error() {
        let inputs = 0..10usize;

        let res: Result<VecDeque<usize>, SlateDBError> =
            build_concurrent(inputs, 3, |x| async move {
                if x == 5 {
                    Err(SlateDBError::Unsupported("boom".into()))
                } else {
                    Ok(Some(x))
                }
            })
            .await;

        assert!(res.is_err());
    }

    // Respects max_parallel bound
    #[tokio::test]
    async fn test_build_iters_concurrent_option_respects_max_parallel() {
        let inputs = 0..16usize;
        let max_parallel = 4;
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let res: Result<VecDeque<()>, SlateDBError> = build_concurrent(inputs, max_parallel, {
            let in_flight = in_flight.clone();
            let peak = peak.clone();
            move |_x| {
                let in_flight = in_flight.clone();
                let peak = peak.clone();
                async move {
                    let cur = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(cur, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(Some(()))
                }
            }
        })
        .await;

        assert!(res.is_ok());
        let observed_peak = peak.load(Ordering::SeqCst);
        assert!(
            observed_peak <= max_parallel,
            "observed peak {} exceeds max_parallel {}",
            observed_peak,
            max_parallel
        );
    }
}
