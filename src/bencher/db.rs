//! # Database benchmarker
//!
//! This module contains the database benchmarker, which is used to benchmark
//! SlateDB. The benchmarker is a subcommand of the `bencher` CLI tool.
//!
//! The DB benchmarker supports:
//!
//! - Configurable key/value sizes
//! - Configurable `WriteOptions`
//! - A pluggable key generator strategy (defaults to random)
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

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use rand::{Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::db::Db;
use tokio::time::Instant;
use tracing::info;

/// How frequently to dump stats to the console.
const STAT_DUMP_INTERVAL: Duration = Duration::from_secs(10);

/// How far back to look when dumping stats.
const STAT_DUMP_LOOKBACK: Duration = Duration::from_secs(60);

/// How frequently to update stats between puts and gets and
/// how frequently to check if we need to dump new stats.
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

/// The size of each window.
const WINDOW_SIZE: Duration = Duration::from_secs(10);

/// A key generator trait that generates keys for the benchmarker.
pub trait KeyGenerator: Send {
    fn next_key(&mut self) -> Bytes;
}

/// A key generator that generates random keys of a fixed length.
pub struct RandomKeyGenerator {
    key_len_bytes: usize,
    rng: XorShiftRng,
}

impl RandomKeyGenerator {
    pub fn new(key_bytes: usize) -> Self {
        Self {
            key_len_bytes: key_bytes,
            rng: rand_xorshift::XorShiftRng::from_entropy(),
        }
    }
}

impl KeyGenerator for RandomKeyGenerator {
    fn next_key(&mut self) -> Bytes {
        let mut bytes = vec![0u8; self.key_len_bytes];
        self.rng.fill_bytes(bytes.as_mut_slice());
        Bytes::copy_from_slice(bytes.as_slice())
    }
}

pub struct FixedSetKeyGenerator {
    keys: Vec<Bytes>,
    rng: XorShiftRng,
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
            rng: rand_xorshift::XorShiftRng::from_entropy(),
        }
    }
}

impl KeyGenerator for FixedSetKeyGenerator {
    fn next_key(&mut self) -> Bytes {
        let index = self.rng.gen_range(0..self.keys.len());
        self.keys[index].clone()
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
    db: Arc<Db>,
}

impl DbBench {
    // Ignore too many args
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
        val_len: usize,
        write_options: WriteOptions,
        concurrency: u32,
        num_rows: Option<u64>,
        duration: Option<Duration>,
        put_percentage: u32,
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
            db,
        }
    }

    /// Run the benchmarker.
    ///
    /// This method spins up `concurrency` tasks, each of which runs a loop,
    /// and then waits for all the tasks to complete. It also spawns a task
    /// to dump stats to the console.
    pub async fn run(&self) {
        let stats_recorder = Arc::new(StatsRecorder::new());
        let mut tasks = Vec::new();
        for _ in 0..self.concurrency {
            let mut task = Task::new(
                (*self.key_gen_supplier)(),
                self.val_len,
                self.write_options.clone(),
                self.num_rows,
                self.duration,
                self.put_percentage,
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
    stats_recorder: Arc<StatsRecorder>,
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
        stats_recorder: Arc<StatsRecorder>,
        db: Arc<Db>,
    ) -> Self {
        Self {
            key_generator,
            val_len,
            write_options,
            num_keys,
            duration,
            put_percentage,
            stats_recorder,
            db,
        }
    }

    /// Run the task.
    ///
    /// This method runs a loop, generating a key (and value if needed), and
    /// then either puts the key/value pair or gets the key.
    async fn run(&mut self) {
        let mut random = rand_xorshift::XorShiftRng::from_entropy();
        let mut puts = 0u64;
        let mut gets = 0u64;
        let duration = self.duration.unwrap_or(Duration::MAX);
        let num_keys = self.num_keys.unwrap_or(u64::MAX);
        let start = Instant::now();
        let mut last_report = start;
        while self.stats_recorder.puts() < num_keys && start.elapsed() < duration {
            let key = &self.key_generator.next_key();
            if random.gen_range(0..100) < self.put_percentage {
                let mut value = vec![0; self.val_len];
                random.fill_bytes(value.as_mut_slice());
                self.db
                    .put_with_options(key, value, &PutOptions::default(), &self.write_options)
                    .await
                    .unwrap();
                puts += 1;
            } else {
                self.db.get(key).await.unwrap();
                gets += 1;
            }
            if last_report.elapsed() >= REPORT_INTERVAL {
                last_report = Instant::now();
                self.stats_recorder.record_puts(last_report, puts);
                self.stats_recorder.record_gets(last_report, gets);
                puts = 0;
                gets = 0;
            }
        }
    }
}

/// Represents the number of puts and gets in a window of time.
#[derive(Debug)]
struct Window {
    range: Range<Instant>,
    puts: u64,
    gets: u64,
}

struct StatsRecorderInner {
    puts: u64,
    gets: u64,
    windows: VecDeque<Window>,
}

impl StatsRecorderInner {
    /// Rolls the window if necessary. Creates a new window if there are no windows.
    /// Otherwise, checks if the front window is in the past and creates a new
    /// window if so.
    fn maybe_roll_window(now: Instant, windows: &mut VecDeque<Window>) {
        let Some(mut front) = windows.front() else {
            windows.push_front(Window {
                range: now..now + WINDOW_SIZE,
                puts: 0,
                gets: 0,
            });
            return;
        };
        while now >= front.range.end {
            windows.push_front(Window {
                range: front.range.end..front.range.end + WINDOW_SIZE,
                puts: 0,
                gets: 0,
            });
            while windows.len() > 180 {
                windows.pop_back();
            }
            front = windows.front().unwrap();
        }
    }

    fn record_puts(&mut self, now: Instant, puts: u64) {
        Self::maybe_roll_window(now, &mut self.windows);
        if let Some(front) = self.windows.front_mut() {
            front.puts += puts;
        }
        self.puts += puts;
    }

    fn record_gets(&mut self, now: Instant, gets: u64) {
        Self::maybe_roll_window(now, &mut self.windows);
        if let Some(front) = self.windows.front_mut() {
            front.gets += gets;
        }
        self.gets += gets;
    }

    fn puts(&self) -> u64 {
        self.puts
    }

    fn gets(&self) -> u64 {
        self.gets
    }

    /// Sums the puts and gets in the windows that are contained in the lookback.
    /// Partially continued windows are excluded. The lookback starts from the start
    /// of the active window, so the active window is not included in the sum.
    fn sum_windows(
        windows: &VecDeque<Window>,
        lookback: Duration,
    ) -> Option<(Range<Instant>, u64, u64)> {
        let mut puts = 0;
        let mut gets = 0;
        let mut windows_iter = windows.iter();
        // Don't count the active window, but use its start point as the end of the range.
        let active_window = windows_iter.next();
        let mut range = if let Some(window) = active_window {
            (window.range.start)..window.range.start
        } else {
            return None;
        };
        for window in windows_iter.filter(|w| w.range.start >= range.end - lookback) {
            puts += window.puts;
            gets += window.gets;
            range.start = window.range.start;
        }
        Some((range, puts, gets))
    }

    fn operations_since(&self, lookback: Duration) -> Option<(Range<Instant>, u64, u64)> {
        Self::sum_windows(&self.windows, lookback).map(|r| (r.0, r.1, r.2))
    }
}

struct StatsRecorder {
    inner: Mutex<StatsRecorderInner>,
}

impl StatsRecorder {
    fn new() -> Self {
        Self {
            inner: Mutex::new(StatsRecorderInner {
                puts: 0,
                gets: 0,
                windows: VecDeque::new(),
            }),
        }
    }

    fn record_puts(&self, now: Instant, records: u64) {
        let mut guard = self.inner.lock().expect("lock failed");
        guard.record_puts(now, records);
    }

    fn record_gets(&self, now: Instant, records: u64) {
        let mut guard = self.inner.lock().expect("lock failed");
        guard.record_gets(now, records);
    }

    fn puts(&self) -> u64 {
        let guard = self.inner.lock().expect("lock failed");
        guard.puts()
    }

    fn gets(&self) -> u64 {
        let guard = self.inner.lock().expect("lock failed");
        guard.gets()
    }

    fn operations_since(&self, lookback: Duration) -> Option<(Range<Instant>, u64, u64)> {
        let guard = self.inner.lock().expect("lock failed");
        guard.operations_since(lookback)
    }
}

async fn dump_stats(stats: Arc<StatsRecorder>) {
    let mut last_stats_dump: Option<Instant> = None;
    let mut first_dump_start: Option<Instant> = None;
    loop {
        tokio::time::sleep(REPORT_INTERVAL).await;

        let operations_since = stats.operations_since(STAT_DUMP_LOOKBACK);
        if let Some((range, puts_since, gets_since)) = operations_since {
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
                let get_rate = gets_since as f32 / interval.as_secs() as f32;
                info!(
                    "stats dump [elapsed {:?}, put/s: {:.3}, get/s: {:.3}, window: {:?}, total puts: {}, total gets: {}]",
                    range.end.duration_since(first_dump_start.unwrap()).as_secs_f64(),
                    put_rate,
                    get_rate,
                    range.end - range.start,
                    puts,
                    gets,
                );
                last_stats_dump = Some(range.end);
            }
        }
    }
}
