//! # Database benchmarker
//!
//! This module contains the database benchmarker, which is used to benchmark
//! SlateDB. The benchmarker is a subcommand of the `bencher` CLI tool.
//!
//! The DB benchmarker supports:
//!
//! - Configurable key/value sizes
//! - Configurable `WriteOptions`
//! - A pluggable key generator strategy (defaults to fixed keyset)
//! - Configurable `DbOptions`` (for common variables)
//! - Charts with gnuplots
//! - Mixed read/write workloads
//! - Concurrent workloads
//!
//! ## Design
//!
//! The benchmarker spins up `concurrency` tasks, each of which runs a loop.
//! The loop generates a key (and value if needed), and then either puts they
//! key/value pair or gets the key. The ratio of puts to gets is controlled by
//! the `put_percentage`.
//!
//! Every `REPORT_INTERVAL`, the task records the number of puts and gets since
//! the last report. The stats are kept in a rolling window of fixed duration
//! (`WINDOW_SIZE`).
//!
//! Meanwhile, the main thread loops, sleeping for `REPORT_INTERVAL` and
//! then checking if it's been more than `STAT_DUMP_INTERVAL` since the last
//! dump. If so, it sums all puts and gets starting from the most recently
//! completed window, looking back `STAT_DUMP_LOOKBACK`. It then prints the sum
//! to the console.
//!
//! If `STAT_DUMP_LOOKBACK` is greater than `STAT_DUMP_INTERVAL` and
//! `WINDOW_SIZE`, the result will be the sum of multiple windows, and thus
//! some smoothing will occur.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rand::{Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::Db;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::stats::{StatsRecorder, WindowStats};

/// How frequently to dump stats to the console.
const STAT_DUMP_INTERVAL: Duration = Duration::from_secs(10);

/// How far back to look when dumping stats.
const STAT_DUMP_LOOKBACK: Duration = Duration::from_secs(60);

/// How frequently to update stats between puts and gets and
/// how frequently to check if we need to dump new stats.
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

/// Maximum number of randomly generated keys to keep track of for reuse in
/// [`RandomKeyGenerator`]. Once this limit is reached, newly generated keys
/// will replace a randomly selected existing key.
const MAX_RANDOM_USED_KEYS: usize = 10_000;

/// A key generator trait that generates keys for the benchmarker.
pub trait KeyGenerator: Send {
    /// Generate and return the next key that should be used in the workload.
    /// Implementations **must** push the generated key onto an internal
    /// `used_keys` vector so that it can later be sampled by [`Self::used_key`].
    fn next_key(&mut self) -> Bytes;

    /// Return one of the previously generated keys **at random**. If no keys
    /// have been generated yet, this method will fall back to calling
    /// [`Self::next_key`] to ensure that a valid key is always returned.
    fn used_key(&mut self) -> Bytes;
}

/// A key generator that generates random keys of a fixed length.
pub struct RandomKeyGenerator {
    key_len_bytes: usize,
    rng: XorShiftRng,
    used_keys: Vec<Bytes>,
}

impl RandomKeyGenerator {
    pub fn new(key_bytes: usize) -> Self {
        Self {
            key_len_bytes: key_bytes,
            rng: rand_xorshift::XorShiftRng::from_os_rng(),
            used_keys: Vec::new(),
        }
    }
}

impl KeyGenerator for RandomKeyGenerator {
    fn next_key(&mut self) -> Bytes {
        let mut bytes = vec![0u8; self.key_len_bytes];
        self.rng.fill_bytes(bytes.as_mut_slice());
        let key = Bytes::copy_from_slice(bytes.as_slice());
        // Track the generated key so that it can be sampled later. Keep the
        // list bounded to `MAX_RANDOM_USED_KEYS` entries by randomly replacing
        // an existing key once the limit is reached.
        if self.used_keys.len() < MAX_RANDOM_USED_KEYS {
            self.used_keys.push(key.clone());
        } else {
            let idx = self.rng.random_range(0..self.used_keys.len());
            self.used_keys[idx] = key.clone();
        }
        key
    }

    fn used_key(&mut self) -> Bytes {
        if self.used_keys.is_empty() {
            return self.next_key();
        }
        let idx = self.rng.random_range(0..self.used_keys.len());
        self.used_keys[idx].clone()
    }
}

pub struct FixedSetKeyGenerator {
    keys: Vec<Bytes>,
    rng: XorShiftRng,
    used_keys: Vec<Bytes>,
}

impl FixedSetKeyGenerator {
    pub fn new(key_bytes: usize, key_count: u64) -> Self {
        let mut random_key_generator = RandomKeyGenerator::new(key_bytes);
        let mut keys = Vec::new();
        for _ in 0..key_count {
            keys.push(random_key_generator.next_key());
        }
        Self {
            keys,
            rng: rand_xorshift::XorShiftRng::from_os_rng(),
            used_keys: Vec::new(),
        }
    }
}

impl KeyGenerator for FixedSetKeyGenerator {
    fn next_key(&mut self) -> Bytes {
        let index = self.rng.random_range(0..self.keys.len());
        let key = self.keys[index].clone();
        self.used_keys.push(key.clone());
        key
    }

    fn used_key(&mut self) -> Bytes {
        if self.used_keys.is_empty() {
            return self.next_key();
        }
        let idx = self.rng.random_range(0..self.used_keys.len());
        self.used_keys[idx].clone()
    }
}

/// The database benchmarker.
pub struct DbBench {
    key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
    val_len: usize,
    write_options: WriteOptions,
    concurrency: u32,
    num_rows: Option<u64>,
    duration: Option<Duration>,
    put_percentage: u32,
    get_hit_percentage: u32,
    db: Arc<Db>,
}

impl DbBench {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
        val_len: usize,
        write_options: WriteOptions,
        concurrency: u32,
        num_rows: Option<u64>,
        duration: Option<Duration>,
        put_percentage: u32,
        get_hit_percentage: u32,
        db: Arc<Db>,
    ) -> Self {
        Self {
            key_gen_supplier,
            val_len,
            write_options,
            concurrency,
            num_rows,
            duration,
            put_percentage,
            get_hit_percentage,
            db,
        }
    }

    /// Run the benchmarker.
    ///
    /// This method spins up `concurrency` tasks, each of which runs a loop,
    /// and then waits for all the tasks to complete. It also spawns a task
    /// to dump stats to the console.
    pub async fn run(&self) {
        let stats_recorder = Arc::new(DbStatsRecorder::new());
        let mut tasks = Vec::new();
        for _ in 0..self.concurrency {
            let mut task = Task::new(
                (*self.key_gen_supplier)(),
                self.val_len,
                self.write_options.clone(),
                self.num_rows,
                self.duration,
                self.put_percentage,
                self.get_hit_percentage,
                stats_recorder.clone(),
                self.db.clone(),
            );
            tasks.push(tokio::spawn(async move { task.run().await }));
        }
        tokio::spawn(async move { dump_stats(stats_recorder).await });
        for task in tasks {
            task.await.unwrap();
        }
    }
}

struct Task {
    key_generator: Box<dyn KeyGenerator>,
    val_len: usize,
    write_options: WriteOptions,
    num_keys: Option<u64>,
    duration: Option<Duration>,
    put_percentage: u32,
    get_hit_percentage: u32,
    stats_recorder: Arc<DbStatsRecorder>,
    db: Arc<Db>,
}

impl Task {
    #[allow(clippy::too_many_arguments)]
    fn new(
        key_generator: Box<dyn KeyGenerator>,
        val_len: usize,
        write_options: WriteOptions,
        num_keys: Option<u64>,
        duration: Option<Duration>,
        put_percentage: u32,
        get_hit_percentage: u32,
        stats_recorder: Arc<DbStatsRecorder>,
        db: Arc<Db>,
    ) -> Self {
        Self {
            key_generator,
            val_len,
            write_options,
            num_keys,
            duration,
            put_percentage,
            get_hit_percentage,
            stats_recorder,
            db,
        }
    }

    /// Run the task.
    ///
    /// This method runs a loop, generating a key (and value if needed), and
    /// then either puts the key/value pair or gets the key.
    async fn run(&mut self) {
        let mut random = rand_xorshift::XorShiftRng::from_os_rng();
        let mut puts = 0u64;
        let mut puts_bytes = 0u64;
        let mut gets = 0u64;
        let mut gets_bytes = 0u64;
        let mut gets_hits = 0u64;
        let duration = self.duration.unwrap_or(Duration::MAX);
        let num_keys = self.num_keys.unwrap_or(u64::MAX);
        let start = Instant::now();
        let mut last_report = start;
        while self.stats_recorder.puts() < num_keys && start.elapsed() < duration {
            if random.random_range(0..100) < self.put_percentage {
                let key = self.key_generator.next_key();
                let mut value = vec![0; self.val_len];
                random.fill_bytes(value.as_mut_slice());
                match self
                    .db
                    .put_with_options(key, value, &PutOptions::default(), &self.write_options)
                    .await
                {
                    Ok(_) => {
                        puts += 1;
                        puts_bytes += self.val_len as u64;
                    }
                    Err(e) => warn!("put failed [error={}]", e),
                }
            } else {
                let key = if random.random_range(0..100) < self.get_hit_percentage {
                    self.key_generator.used_key()
                } else {
                    self.key_generator.next_key()
                };
                match self.db.get(&key).await {
                    Ok(val) => {
                        gets += 1;
                        gets_hits += val.is_some() as u64;
                        gets_bytes += key.len() as u64 + val.map(|v| v.len() as u64).unwrap_or(0);
                    }
                    Err(e) => warn!("get failed [error={}]", e),
                }
            }
            if last_report.elapsed() >= REPORT_INTERVAL {
                last_report = Instant::now();
                self.stats_recorder
                    .record_puts(last_report, puts, puts_bytes);
                self.stats_recorder
                    .record_gets(last_report, gets, gets_bytes, gets_hits);
                puts = 0;
                gets = 0;
                puts_bytes = 0;
                gets_bytes = 0;
                gets_hits = 0;
            }
        }
    }
}

/// Represents the number of puts and gets in a window of time.
#[derive(Debug)]
struct DbWindow {
    range: Range<Instant>,
    puts: u64,
    gets: u64,
    puts_bytes: u64,
    gets_bytes: u64,
    gets_hits: u64,
}

impl Default for DbWindow {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            range: now..now,
            puts: 0,
            gets: 0,
            puts_bytes: 0,
            gets_bytes: 0,
            gets_hits: 0,
        }
    }
}

impl WindowStats for DbWindow {
    fn range(&self) -> Range<Instant> {
        self.range.clone()
    }

    fn set_range(&mut self, range: Range<Instant>) {
        self.range = range;
    }
}

struct DbStatsRecorder {
    recorder: StatsRecorder<DbWindow>,
    // Overall totals tracked separately
    total_puts: std::sync::atomic::AtomicU64,
    total_gets: std::sync::atomic::AtomicU64,
    total_puts_bytes: std::sync::atomic::AtomicU64,
    total_gets_bytes: std::sync::atomic::AtomicU64,
    total_gets_hits: std::sync::atomic::AtomicU64,
}

impl DbStatsRecorder {
    fn new() -> Self {
        Self {
            recorder: StatsRecorder::new(),
            total_puts: std::sync::atomic::AtomicU64::new(0),
            total_gets: std::sync::atomic::AtomicU64::new(0),
            total_puts_bytes: std::sync::atomic::AtomicU64::new(0),
            total_gets_bytes: std::sync::atomic::AtomicU64::new(0),
            total_gets_hits: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn record_puts(&self, now: Instant, puts: u64, bytes: u64) {
        self.total_puts
            .fetch_add(puts, std::sync::atomic::Ordering::Relaxed);
        self.total_puts_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);

        self.recorder.record(now, |window| {
            window.puts += puts;
            window.puts_bytes += bytes;
        });
    }

    fn record_gets(&self, now: Instant, gets: u64, bytes: u64, hits: u64) {
        self.total_gets
            .fetch_add(gets, std::sync::atomic::Ordering::Relaxed);
        self.total_gets_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        self.total_gets_hits
            .fetch_add(hits, std::sync::atomic::Ordering::Relaxed);

        self.recorder.record(now, |window| {
            window.gets += gets;
            window.gets_bytes += bytes;
            window.gets_hits += hits;
        });
    }

    fn puts(&self) -> u64 {
        self.total_puts.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn gets(&self) -> u64 {
        self.total_gets.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn operations_since(
        &self,
        lookback: Duration,
    ) -> Option<(Range<Instant>, u64, u64, u64, u64, u64)> {
        self.recorder.stats_since(lookback, |range, windows| {
            let puts = windows.iter().map(|w| w.puts).sum();
            let gets = windows.iter().map(|w| w.gets).sum();
            let puts_bytes = windows.iter().map(|w| w.puts_bytes).sum();
            let gets_bytes = windows.iter().map(|w| w.gets_bytes).sum();
            let gets_hits = windows.iter().map(|w| w.gets_hits).sum();
            (range, puts, gets, puts_bytes, gets_bytes, gets_hits)
        })
    }
}

async fn dump_stats(stats: Arc<DbStatsRecorder>) {
    let mut last_stats_dump: Option<Instant> = None;
    let mut first_dump_start: Option<Instant> = None;
    loop {
        tokio::time::sleep(REPORT_INTERVAL).await;

        let operations_since = stats.operations_since(STAT_DUMP_LOOKBACK);
        if let Some((
            range,
            puts_since,
            gets_since,
            puts_bytes_since,
            gets_bytes_since,
            gets_hits_since,
        )) = operations_since
        {
            let interval = range.end - range.start;
            let puts = stats.puts();
            let gets = stats.gets();
            let should_print = match last_stats_dump {
                Some(last_stats_dump) => (range.end - last_stats_dump) >= STAT_DUMP_INTERVAL,
                None => (range.end - range.start) >= STAT_DUMP_INTERVAL,
            };
            first_dump_start = first_dump_start.or(Some(range.start));
            if should_print {
                let put_rate = puts_since as f32 / interval.as_secs() as f32;
                let put_bytes_rate = puts_bytes_since as f32 / interval.as_secs() as f32;
                let get_rate = gets_since as f32 / interval.as_secs() as f32;
                let get_bytes_rate = gets_bytes_since as f32 / interval.as_secs() as f32;
                let get_hit_pct = if gets_since > 0 {
                    gets_hits_since as f32 / gets_since as f32
                } else {
                    0.0
                };

                info!(
                    "stats dump [elapsed {:?}, put/s: {:.3} ({:.3} MiB/s), get/s: {:.3} ({:.3} MiB/s), get db hit ratio: {:.3}%, window: {:?}, total puts: {}, total gets: {}]",
                    range.end.duration_since(first_dump_start.unwrap()).as_secs_f64(),
                    put_rate,
                    put_bytes_rate / 1_048_576.0,
                    get_rate,
                    get_bytes_rate / 1_048_576.0,
                    get_hit_pct * 100f32,
                    range.end - range.start,
                    puts,
                    gets,
                );
                last_stats_dump = Some(range.end);
            }
        }
    }
}
