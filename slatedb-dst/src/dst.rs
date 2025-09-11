//! # Deterministic Simulation Testing (DST)
//!
//! This module contains tools for running deterministic simulations on a SlateDB
//! instance. The main purpose of this module is to provide a [Dst::run_simulation]
//! that runs a simulation for a given number of iterations.
//!
//! Each iteration, or step, in the simulation runs a single action to perform
//! on the database. The action is determined by a [DstDistribution] implementation.
//! If the action is a read (get or scan), the simulation verifies the result
//! against an in-memory [BTreeMap] copy of the database ([Dst::state]). If the action
//! is a write (put or delete), the simulation updates both the database and the
//! in-memory copy.
//!
//! [DstAction] contains the (currently) supported actions:
//!
//! - [DstAction::Write]
//! - [DstAction::Get]
//! - [DstAction::Scan]
//! - [DstAction::Flush]
//! - [DstAction::AdvanceTime]
//!
//! [Dst] expects to be run with completely deterministic components for time, random
//! numbers, and scheduling (the async runtime). SlateDB provides the following
//! components to make this possible:
//!
//! - [SystemClock]: A mock system clock is provided by
//!   [MockSystemClock](slatedb::clock::MockSystemClock).
//! - [LogicalClock](slatedb::clock::LogicalClock): A mock logical clock is provided
//!   by [MockLogicalClock](slatedb::clock::MockLogicalClock).
//! - [DbRand]: Can be made deterministic by providing a seed in [DbRand::new].
//! - [Runtime](tokio::runtime::Runtime): A single threaded Tokio runtime with a
//!   rng_seed provided. This requires `RUSTFLAGS="--cfg tokio_unstable"` and Tokio's
//!   `rt` feature enabled.
//!
//! It is somewhat cumbersome to set up these components, so helper functions are
//! provided in [utils]. The `simulation.rs` tests show examples of how to use them.
//!
//! [Dst] can be configured with [DstOptions], which determines the maximum length of
//! keys, values, and write batches, as well as the maximum size of the in-memory
//! database ([Dst::state]).
//!
//! ## Example
//!
//! ```rust
//! # // The following environment variables must be set to test this code block:
//! # // RUSTFLAGS="--cfg tokio_unstable"
//! # // RUSTDOCFLAGS="--cfg tokio_unstable"
//! # #[cfg(tokio_unstable)] {
//! # use slatedb::*;
//! # use slatedb::clock::MockSystemClock;
//! # use slatedb::clock::MockLogicalClock;
//! # use slatedb::object_store::memory::InMemory;
//! # use slatedb_dst::*;
//! # use slatedb_dst::utils::*;
//! # use std::sync::Arc;
//! # use std::rc::Rc;
//! let system_clock = Arc::new(MockSystemClock::new());
//! let logical_clock = Arc::new(MockLogicalClock::new());
//! let rand = Rc::new(DbRand::new(12345));
//! let dst_opts = DstOptions::default();
//! let runtime = build_runtime(5678);
//! runtime.block_on(async move {
//!     let db = DbBuilder::new("test_db", Arc::new(InMemory::new()))
//!         .with_seed(1234)
//!         .with_system_clock(system_clock.clone())
//!         .with_logical_clock(logical_clock.clone())
//!         .build()
//!         .await
//!         .unwrap();
//!     let distr = Box::new(DefaultDstDistribution::new(rand.clone(), dst_opts.clone()));
//!     let mut dst = Dst::new(
//!         db,
//!         system_clock,
//!         rand.clone(),
//!         distr,
//!         dst_opts,
//!     ).unwrap();
//!     dst.run_simulation(DstDuration::Iterations(10)).await.unwrap();
//! });
//! # }
//! ```

use crate::state::SQLiteState;
use crate::state::State;
use crate::utils::truncate_bytes;
use log::debug;
use log::info;
use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::seq::IteratorRandom;
use rand::Rng;
use rand::RngCore;
use slatedb::clock::SystemClock;
use slatedb::config::PutOptions;
use slatedb::config::ReadOptions;
use slatedb::config::ScanOptions;
use slatedb::config::WriteOptions;
use slatedb::Db;
use slatedb::DbRand;
use slatedb::Error;
use slatedb::WriteBatch;
use std::future::Future;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

/// Configuration options for the simulation.
#[derive(Clone)]
pub struct DstOptions {
    /// Maximum key length in bytes. Must be smaller than u16::MAX bytes.
    max_key_bytes: u16,
    /// Maximum value length in bytes.
    max_val_bytes: u32,
    /// Maximum write batch size in bytes.
    max_write_batch_bytes: u32,
    /// Optional path to store the DST state database in.
    /// If not provided, the DST state database will be stored in memory.
    /// When the DST state database is stored in memory, the simulation will fail
    /// if the machine runs out of memory.
    state_path: Option<&'static str>,
}

impl Default for DstOptions {
    fn default() -> Self {
        Self {
            max_key_bytes: u16::MAX,                 // keys are limited to 65_535 bytes
            max_val_bytes: 1024 * 1024,              // 1 MiB
            max_write_batch_bytes: 50 * 1024 * 1024, // 50 MiB
            state_path: None,
        }
    }
}

/// A write operation to be performed by the DST. [WriteOp](slatedb::batch::WriteOp)
/// is only pub(crate), so we define a new type here.
pub type DstWriteOp = (Vec<u8>, Option<Vec<u8>>, PutOptions);

/// Actions that can be performed by the DST.
pub enum DstAction {
    /// A write operation to be performed by the DST.
    ///
    /// # Arguments
    ///
    /// * `ops` - The write operations to perform.
    /// * `options` - The write options.
    Write(Vec<DstWriteOp>, WriteOptions),
    /// A get operation to be performed by the DST.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to get.
    /// * `options` - The read options.
    Get(Vec<u8>, ReadOptions),
    /// A scan operation to be performed by the DST. The start
    /// must be strictly less than the end, or SlateDB will return an
    /// error.
    ///
    /// # Arguments
    ///
    /// * `start_key` - The key to start scanning from.
    /// * `end_key` - The key to end scanning at.
    /// * `options` - The scan options.
    Scan(Vec<u8>, Vec<u8>, ScanOptions),
    /// Calls flush() on the DB.
    Flush,
    /// Advances the system clock by the given duration.
    AdvanceTime(Duration),
}

impl std::fmt::Display for DstAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DstAction::Write(write_ops, _) => {
                write!(f, "Write(ops={})", write_ops.len())
            }
            DstAction::Get(_, _) => {
                write!(f, "Get")
            }
            DstAction::Scan(_, _, _) => {
                write!(f, "Scan")
            }
            DstAction::Flush => write!(f, "Flush"),
            DstAction::AdvanceTime(duration) => {
                write!(f, "AdvanceTime({:?})", duration)
            }
        }
    }
}

/// A little helper for debugging DST actions. Truncates bytes to 8 characters
/// for readability.
impl std::fmt::Debug for DstAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DstAction::Write(write_ops, write_options) => {
                write!(f, "Write(")?;
                for (i, (key, val, options)) in write_ops.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    match val {
                        Some(val) => write!(
                            f,
                            "Put({:?}..., {:?}, {:?})",
                            truncate_bytes(key),
                            truncate_bytes(val),
                            options
                        )?,
                        None => write!(f, "Delete({:?}...)", truncate_bytes(key))?,
                    }
                }
                write!(f, ", {:?})", write_options)
            }
            DstAction::Get(key, read_options) => {
                write!(f, "Get({:?}..., {:?})", truncate_bytes(key), read_options)
            }
            DstAction::Scan(start_key, end_key, scan_options) => {
                write!(
                    f,
                    "Scan({:?}..., {:?}..., {:?})",
                    truncate_bytes(start_key),
                    truncate_bytes(end_key),
                    scan_options
                )
            }
            DstAction::Flush => write!(f, "Flush"),
            DstAction::AdvanceTime(duration) => write!(f, "AdvanceTime({:?})", duration),
        }
    }
}

/// A trait for generating DST actions with some probability. DSTs are generated
/// by sampling actions from a distribution. The distribution implementation is left
/// to the user, but SlateDB provides a default implementation in
/// [DefaultDstDistribution]
///
/// [Dst] samples actions from the distribution and performs them on the database.
pub trait DstDistribution {
    fn sample_action(&self, state: &SQLiteState) -> DstAction;
}

/// [DefaultDstDistribution] has the following characteristics:
///
/// - All [DstAction]s have an equal probability of being selected.
/// - Each [DstWriteOp] has an 80% chance of being a write, and 20% chance of being a
///   delete.
/// - Scans are always generated with a start key < end key, and the start key. Both
///   keys are sampled using [DefaultDstDistribution::gen_key].
/// - [DefaultDstDistribution::sample_log10_uniform] is used to generate keys and values,
///   and to advance time between 1ms and 10 seconds.
pub struct DefaultDstDistribution {
    options: DstOptions,
    rand: Rc<DbRand>,
}

/// Helper functions for generating DST actions.
impl DefaultDstDistribution {
    pub fn new(rand: Rc<DbRand>, options: DstOptions) -> Self {
        Self { options, rand }
    }

    /// Generate a write action with puts and deletes.
    /// The maximum number of bytes to write is configured by [DstOptions::max_write_batch_bytes].
    ///
    /// See [DefaultDstDistribution::gen_key] and [DefaultDstDistribution::gen_val]
    /// for more information.
    fn sample_write(&self, state: &SQLiteState) -> DstAction {
        let mut write_ops = Vec::new();
        let write_options = self.get_write_options();
        let mut remaining_bytes =
            self.sample_log10_uniform(1..self.options.max_write_batch_bytes) as i64;
        while remaining_bytes > 0 {
            if self.is_put_operation() {
                let key = self.gen_key(state);
                let val = self.gen_val();
                remaining_bytes -= (key.len() + val.len()) as i64;
                write_ops.push((key, Some(val), self.gen_put_options()));
            } else {
                let key = self.gen_key(state);
                remaining_bytes -= key.len() as i64;
                write_ops.push((key, None, PutOptions::default()));
            }
        }
        DstAction::Write(write_ops, write_options)
    }

    /// Generate a get action.
    ///
    /// See [DefaultDstDistribution::gen_key] for more information.
    fn sample_get(&self, state: &SQLiteState) -> DstAction {
        DstAction::Get(self.gen_key(state), self.gen_read_options())
    }

    /// Generate a scan action.
    ///
    /// The scan will always be a valid range, i.e. the start key will be less than
    /// the end key. SlateDB panics if the range is empty, so we skip scanning empty
    /// ranges.
    ///
    /// [DefaultDstDistribution::gen_key] is used to generate the start and end keys.
    /// This means scan inherits the db_hit probability from that method.
    fn sample_scan(&self, state: &SQLiteState) -> DstAction {
        let mut start_key = self.gen_key(state);
        let mut end_key = self.gen_key(state);
        if start_key > end_key {
            std::mem::swap(&mut start_key, &mut end_key);
        } else if start_key == end_key {
            end_key.push(b'\0');
        }
        DstAction::Scan(start_key.clone(), end_key.clone(), ScanOptions::default())
    }

    fn sample_flush(&self) -> DstAction {
        DstAction::Flush
    }

    /// Generates an advance time action. The duration is sampled using a log-uniform
    /// distribution. The range is hard coded as 1..10_000_000 (1ms to 10 seconds).
    fn sample_advance_time(&self) -> DstAction {
        let sleep_micros = self.sample_log10_uniform(1..10_000_000).into();
        DstAction::AdvanceTime(Duration::from_micros(sleep_micros))
    }

    /// Samples a value from a log-uniform distribution. The log is a log10 (common log).
    /// This allows us to evenly sample from each order of magnitude of the range. For example,
    /// if the range is 1..10000, samples with 1 digit, 2 digits, 3 digits, and 4 digits will be
    /// sampled with equal probability.
    ///
    /// This is useful for covering a wide range of values without favoring small or large values.
    /// A linear uniform distribution favors large values, and both geometric and log distributions
    /// favor small values, while normal distributions favor values close to the mean.
    ///
    /// For DST, evenly sampling across orders of magnitude seems to expose the most bugs.
    #[inline]
    fn sample_log10_uniform<R: RangeBounds<u32>>(&self, range: R) -> u32 {
        let min = match range.start_bound() {
            Bound::Unbounded => 1,
            Bound::Included(min) => *min,
            Bound::Excluded(min) => min + 1,
        };
        assert!(min > 0, "min must be > 0");
        let max = match range.end_bound() {
            Bound::Unbounded => u32::MAX,
            Bound::Included(max) => *max,
            Bound::Excluded(max) => max - 1,
        };
        assert!(min < max, "range must not be empty");
        // Convert to common log space (log10) so we sample evenly across orders of magnitude.
        let min_log10 = (min as f64).log10();
        let max_log10 = (max as f64).log10();
        let log10_dist = Uniform::new(min_log10, max_log10).expect("non-empty weights and all ≥ 0");
        let u = log10_dist.sample(&mut self.rand.rng());
        // Go back to original range. Clamping biases the end of the range, but is fine for DST.
        (10f64.powf(u) as u32).clamp(min, max)
    }

    /// Generates a key for actions that require a key. The key can be either a key that's
    /// currently in the DB, or a new key. New keys are filled with random bytes. The
    /// probability of sampling an existing key is determined by
    /// [DefaultDstDistribution::maybe_get_existing_key].
    ///
    /// Key sizes are sampled using [DefaultDstDistribution::sample_log10_uniform]. This
    /// ensures that we cover a wide range of keys without favoring any particular size.
    /// See [DefaultDstDistribution::sample_log10_uniform] for more details.
    #[inline]
    fn gen_key(&self, state: &SQLiteState) -> Vec<u8> {
        if let Some(existing_key) = self.maybe_get_existing_key(state) {
            return existing_key;
        }
        let key_len = self.sample_log10_uniform(1..self.options.max_key_bytes as u32) as usize;
        let mut bytes = vec![0; key_len];
        self.rand.rng().fill_bytes(&mut bytes);
        bytes
    }

    /// Returns a random existing key from the DB with probability `hit_probability`, or `None` if
    /// the DB is empty. The probability is selected randomly on each invocation. This is currently
    /// a simple way to control the probability of sampling an existing key, but it may be
    /// improved in the future.
    #[inline]
    fn maybe_get_existing_key(&self, state: &SQLiteState) -> Option<Vec<u8>> {
        let hit_probability = self.rand.rng().random_range(0.0..1.0);
        let is_db_hit = !state.is_empty() && self.rand.rng().random_bool(hit_probability);

        if is_db_hit {
            let keys = state.keys().unwrap_or_default();
            let existing_key = keys
                .into_iter()
                .choose(&mut self.rand.rng())
                .expect("can't pick a key for an empty state")
                .clone();
            Some(existing_key)
        } else {
            None
        }
    }

    /// Generates a value for actions that require a value. The value is filled with random bytes.
    ///
    /// Value sizes are sampled using [DefaultDstDistribution::sample_log10_uniform]. This
    /// ensures that we cover a wide range of values without favoring any particular size.
    /// See [DefaultDstDistribution::sample_log10_uniform] for more details.
    #[inline]
    fn gen_val(&self) -> Vec<u8> {
        let val_len = self.sample_log10_uniform(1..self.options.max_val_bytes) as usize;
        let mut bytes = vec![0; val_len];
        self.rand.rng().fill_bytes(&mut bytes);
        bytes
    }

    #[inline]
    fn gen_put_options(&self) -> PutOptions {
        PutOptions::default()
    }

    /// Generates write options for actions that require write options.
    ///
    /// Currently, we only have one option: `await_durable`. This option is set to true 50% of
    /// the time.
    #[inline]
    fn get_write_options(&self) -> WriteOptions {
        let mut rng = self.rand.rng();
        WriteOptions {
            await_durable: rng.random_bool(0.5),
        }
    }

    #[inline]
    fn gen_read_options(&self) -> ReadOptions {
        ReadOptions::default()
    }

    /// Returns true if the operation is a put, false if it is a delete.
    ///
    /// The probability of a put is randomly sampled on each invocation.
    #[inline]
    fn is_put_operation(&self) -> bool {
        let mut rng = self.rand.rng();
        let insert_probability = rng.random_range(0.0..1.0);
        rng.random_bool(insert_probability)
    }
}

/// Samples an action from the distribution. Actions are sampled with equal probability.
impl DstDistribution for DefaultDstDistribution {
    fn sample_action(&self, state: &SQLiteState) -> DstAction {
        let weights = [1; 5]; // all actions have equal probability for now
        let dist = WeightedIndex::new(weights).expect("non-empty weights and all ≥ 0");
        let action = dist.sample(&mut self.rand.rng());
        match action {
            0 => self.sample_write(state),
            1 => self.sample_get(state),
            2 => self.sample_scan(state),
            3 => self.sample_flush(),
            4 => self.sample_advance_time(),
            _ => unreachable!(),
        }
    }
}

/// The duration to run the simulation for. This can be either an iteration count or a
/// wall-clock duration. "wall-clock" means the actual time on the machine, not the
/// simulated time.
#[derive(Debug, Copy, Clone)]
pub enum DstDuration {
    /// Run for the given number of iterations.
    Iterations(u32),
    /// Run for the given wall-clock duration. "wall-clock" means the actual time on the
    /// machine, not the simulated time.
    WallClock(Duration),
}

impl DstDuration {
    /// Returns true if the duration has not been exceeded.
    pub fn should_run(&self, current_iteration: u32, start_time: std::time::Instant) -> bool {
        match self {
            DstDuration::Iterations(max_iterations) => current_iteration < *max_iterations,
            DstDuration::WallClock(max_duration) => start_time.elapsed() < *max_duration,
        }
    }
}

/// The main struct that runs the simulation.
pub struct Dst {
    /// The SlateDB instance to simulate on.
    db: Db,
    /// The system clock to use for the simulation.
    system_clock: Arc<dyn SystemClock>,
    /// The random number generator to use for the simulation.
    rand: Rc<DbRand>,
    /// The action sampler to use for the simulation.
    action_sampler: Box<dyn DstDistribution>,
    /// A representation of what _should_ be in the DB.
    state: SQLiteState,
}

impl Dst {
    pub fn new(
        db: Db,
        system_clock: Arc<dyn SystemClock>,
        rand: Rc<DbRand>,
        action_sampler: Box<dyn DstDistribution>,
        options: DstOptions,
    ) -> Result<Self, Error> {
        let state = SQLiteState::new(options.state_path)?;
        Ok(Self {
            db,
            state,
            rand,
            action_sampler,
            system_clock,
        })
    }

    /// Runs the simulation for the given duration.
    ///
    /// Each iteration is a single step in the simulation. Each step samples an action
    /// from the action sampler and runs it. Reads (get and scan) are verified against the
    /// in-memory state. Writes are run against the DB and the in-memory state.
    pub async fn run_simulation(&mut self, dst_duration: DstDuration) -> Result<(), Error> {
        let actual_start_time = std::time::Instant::now();
        let mut step_count = 0;
        while dst_duration.should_run(step_count, actual_start_time) {
            let step_action = self.action_sampler.sample_action(&self.state);
            info!(
                "run_simulation [simulated_time={}, step_count={}, step_action={}]",
                self.system_clock.now(),
                step_count,
                step_action,
            );
            match step_action {
                DstAction::Write(write_ops, write_options) => {
                    self.run_write(&write_ops, &write_options).await?
                }
                DstAction::Get(key, read_options) => self.run_get(&key, &read_options).await?,
                DstAction::Scan(start_key, end_key, scan_options) => {
                    self.run_scan(&start_key, &end_key, &scan_options).await?
                }
                DstAction::Flush => self.run_flush().await?,
                DstAction::AdvanceTime(duration) => self.advance_time(duration).await,
            }
            step_count += 1;
        }
        self.run_flush().await?;
        self.db.close().await?;
        Ok(())
    }

    async fn run_write(
        &mut self,
        write_ops: &Vec<DstWriteOp>,
        write_options: &WriteOptions,
    ) -> Result<(), Error> {
        let mut write_batch = WriteBatch::new();

        for (key, val, options) in write_ops {
            if let Some(val) = val {
                write_batch.put_with_options(key, val, options);
            } else {
                write_batch.delete(key);
            }
        }
        let future = self.db.write_with_options(write_batch, write_options);
        // If await_durable is true, we want to flush with probability 0.01
        // to unblock the write. This will happen even if the WAL is enabled,
        // which isn't strictly needed. But we don't expose `is_wal_enabled()`
        // to the public API.
        let flush_probability = if write_options.await_durable {
            0.5
        } else {
            0f64
        };
        self.poll_await(future, flush_probability).await?;
        self.state.write_batch(write_ops)?;
        Ok(())
    }

    async fn run_get(&mut self, key: &Vec<u8>, read_options: &ReadOptions) -> Result<(), Error> {
        let future = self.db.get_with_options(key, read_options);
        let result = self.poll_await(future, 0f64).await?;
        let expected_val = self.state.get(key)?;
        let actual_val = result.map(|b| b.to_vec());
        assert_eq!(expected_val, actual_val);
        Ok(())
    }

    async fn run_scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        scan_options: &ScanOptions,
    ) -> Result<(), Error> {
        if start_key == end_key {
            debug!("run_scan (start_key == end_key)");
            // Skip because SlateDB does not allow empty ranges (see #681)
            return Ok(());
        }
        let future = self.db.scan_with_options(start_key..end_key, scan_options);
        let mut actual_itr = self.poll_await(future, 0f64).await?;
        let expected_itr = self.state.scan(start_key, end_key)?;

        for (expected_key, expected_val) in expected_itr {
            let actual_key_val = actual_itr
                .next()
                .await?
                .expect("should have more items in scan iterator");
            assert_eq!(expected_key, actual_key_val.key.as_ref());
            assert_eq!(expected_val, actual_key_val.value.as_ref());
        }
        assert!(actual_itr.next().await?.is_none());
        Ok(())
    }

    async fn run_flush(&self) -> Result<(), Error> {
        debug!("run_flush");
        self.db.flush().await
    }

    async fn advance_time(&self, duration: Duration) {
        debug!("advance_time [duration={:?}]", duration);
        self.system_clock.clone().advance(duration).await;
    }

    /// Polls a future until it is ready, advancing time if it is not ready.
    ///
    /// If `flush_probability` is non-zero, a flush will be executed with the
    /// defined probability. This is to unblock `await_durable: true` writes
    /// when the WAL is disabled. In such a case, SlateDB only flushes when
    /// `l0_max_size_bytes` is exceeded, which never happens since we're single
    /// threaded and the first write waits for a flush. See #680 for more
    /// details.
    async fn poll_await<T>(
        &self,
        future: impl Future<Output = Result<T, Error>>,
        flush_probability: f64,
    ) -> Result<T, Error> {
        use futures::task::noop_waker_ref;
        use std::task::Context;
        use std::task::Poll;

        let mut fut = Box::pin(future);
        let mut cx = Context::from_waker(noop_waker_ref());

        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(res) => {
                    return res;
                }
                Poll::Pending => {
                    let sleep_ms = self.rand.rng().random_range(0..10);
                    self.advance_time(Duration::from_millis(sleep_ms)).await;
                    if self.rand.rng().random_bool(flush_probability) {
                        self.db.flush().await?;
                    }
                }
            }
        }
    }
}
