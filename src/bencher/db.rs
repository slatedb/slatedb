use std::collections::VecDeque;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use rand::{Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use slatedb::config::WriteOptions;
use slatedb::db::Db;
use tokio::time::Instant;
use tracing::info;

/// How frequently to dump stats to the console.
const STAT_DUMP_INTERVAL: Duration = Duration::from_secs(10);

/// How frequently to update stats between puts and gets.
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

/// The granularity of the stats window.
const WINDOW_SIZE: Duration = Duration::from_secs(10);

pub trait KeyGenerator: Send {
    fn next_key(&mut self) -> Bytes;
}

// TODO: implement other distributions

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

pub struct DbBench {
    key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
    val_len: usize,
    write_options: WriteOptions,
    concurrency: u32,
    num_rows: Option<u64>,
    duration: Option<Duration>,
    put_percentage: u32,
    plot: bool,
    db: Arc<Db>,
}

impl DbBench {
    pub fn new(
        key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
        val_len: usize,
        write_options: WriteOptions,
        concurrency: u32,
        num_rows: Option<u64>,
        duration: Option<Duration>,
        put_percentage: u32,
        plot: bool,
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
            plot,
            db,
        }
    }

    pub async fn run(&self) {
        let stats_recorder = Arc::new(StatsRecorder::new());
        let mut write_tasks = Vec::new();
        for _ in 0..self.concurrency {
            let mut write_task = Task::new(
                (*self.key_gen_supplier)(),
                self.val_len,
                self.write_options.clone(),
                self.num_rows,
                self.duration,
                self.put_percentage,
                stats_recorder.clone(),
                self.db.clone(),
            );
            write_tasks.push(tokio::spawn(async move { write_task.run().await }));
        }
        tokio::spawn(async move { dump_stats(stats_recorder).await });
        for write_task in write_tasks {
            write_task.await.unwrap();
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
                    .put_with_options(key, value.as_ref(), &self.write_options)
                    .await;
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

#[derive(Debug)]
struct Window {
    range: Range<Instant>,
    puts: f32,
    gets: f32,
}

struct StatsRecorderInner {
    puts: u64,
    gets: u64,
    windows: VecDeque<Window>,
}

impl StatsRecorderInner {
    fn maybe_roll_window(now: Instant, windows: &mut VecDeque<Window>) {
        let Some(mut front) = windows.front() else {
            windows.push_front(Window {
                range: now..now + WINDOW_SIZE,
                puts: 0f32,
                gets: 0f32,
            });
            return;
        };
        while now >= front.range.end {
            windows.push_front(Window {
                range: front.range.end..front.range.end + WINDOW_SIZE,
                puts: 0f32,
                gets: 0f32,
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
            front.puts += puts as f32;
        }
        self.puts += puts;
    }

    fn record_gets(&mut self, now: Instant, gets: u64) {
        Self::maybe_roll_window(now, &mut self.windows);
        if let Some(front) = self.windows.front_mut() {
            front.gets += gets as f32;
        }
        self.gets += gets;
    }

    fn puts(&self) -> u64 {
        self.puts
    }

    fn gets(&self) -> u64 {
        self.gets
    }

    fn sum_windows(
        windows: &VecDeque<Window>,
        lookback: Duration,
    ) -> Option<(Range<Instant>, f32, f32)> {
        let mut puts = 0f32;
        let mut gets = 0f32;
        let mut windows_iter = windows.iter();
        // Don't count the active window, but use its start point as the end of the range.
        let active_window = windows_iter.next();
        let mut range = if let Some(window) = active_window {
            (window.range.start)..window.range.start
        } else {
            return None;
        };
        for window in windows_iter.filter(|w| w.range.start >= range.end - lookback)
        {
            puts += window.puts;
            gets += window.gets;
            range.start = window.range.start;
        }
        return Some((range, puts, gets));
    }

    fn operations_since(&self, lookback: Duration) -> Option<(Range<Instant>, u64, u64)> {
        Self::sum_windows(&self.windows, lookback).map(|r| (r.0, r.1 as u64, r.2 as u64))
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
    loop {
        tokio::time::sleep(REPORT_INTERVAL).await;

        let operations_since = stats.operations_since(Duration::from_secs(60));
        if let Some((range, puts_since, gets_since)) = operations_since {
            let interval = range.end - range.start;
            let puts = stats.puts();
            let gets = stats.gets();
            let should_print = match last_stats_dump {
                Some(last_stats_dump) => (range.end - last_stats_dump) >= STAT_DUMP_INTERVAL,
                None => (range.end - range.start) >= STAT_DUMP_INTERVAL,
            };
            if should_print {
                let put_rate = puts_since as f32 / interval.as_secs() as f32;
                let get_rate = gets_since as f32 / interval.as_secs() as f32;
                info!("Stats Dump:");
                info!("---------------------------------------");
                info!("puts: {}", puts);
                info!("gets: {}", gets);
                info!("put rate: {:.3}/second over {:?}", put_rate, interval);
                info!("get rate: {:.3}/second over {:?}", get_rate, interval);
                last_stats_dump = Some(range.end);
            }
        }
    }
}
