use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use slatedb::config::WriteOptions;
use slatedb::db::Db;
use tokio::time::Instant;
use tracing::info;

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
        stats_recorder: Arc<StatsRecorder>,
        db: Arc<Db>,
    ) -> Self {
        Self {
            key_generator,
            val_len,
            write_options,
            num_keys,
            duration,
            stats_recorder,
            db,
        }
    }

    async fn run(&mut self) {
        let start = std::time::Instant::now();
        let mut keys_written = 0u64;
        let write_batch = 4;
        let mut val_rng = rand_xorshift::XorShiftRng::from_entropy();
        loop {
            if start.elapsed() >= self.duration.unwrap_or(Duration::MAX) {
                break;
            }
            if keys_written >= self.num_keys.unwrap_or(u64::MAX) {
                break;
            }
            for _ in 0..write_batch {
                let key = self.key_generator.next_key();
                let mut value = vec![0; self.val_len];
                val_rng.fill_bytes(value.as_mut_slice());
                self.db
                    .put_with_options(key.as_ref(), value.as_ref(), &self.write_options)
                    .await;
            }
            self.stats_recorder
                .record_records_written(write_batch as u64);
            keys_written += write_batch as u64;
        }
    }
}

const WINDOW_SIZE: Duration = Duration::from_secs(10);

struct Window {
    start: Instant,
    last: Instant,
    value: f32,
}

struct StatsRecorderInner {
    records_written: u64,
    records_written_windows: VecDeque<Window>,
}

impl StatsRecorderInner {
    fn maybe_roll_window(now: Instant, windows: &mut VecDeque<Window>) {
        let Some(front) = windows.front() else {
            windows.push_front(Window {
                start: now,
                value: 0f32,
                last: now,
            });
            return;
        };
        let mut front_start = front.start;
        while now.duration_since(front_start) > WINDOW_SIZE {
            windows.push_front(Window {
                start: front_start + WINDOW_SIZE,
                value: 0f32,
                last: now,
            });
            front_start = windows.front().unwrap().start;
            while windows.len() > 180 {
                windows.pop_back();
            }
        }
    }

    fn record_records_written(&mut self, now: Instant, records: u64) {
        Self::maybe_roll_window(now, &mut self.records_written_windows);
        if let Some(front) = self.records_written_windows.front_mut() {
            front.value += records as f32;
            front.last = now;
        }
        self.records_written += records;
    }

    fn records_written(&self) -> u64 {
        self.records_written
    }

    fn sum_windows(windows: &VecDeque<Window>, since: Instant) -> Option<(Instant, Instant, f32)> {
        let sum: f32 = windows
            .iter()
            .filter(|w| w.start > since)
            .map(|w| w.value)
            .sum();
        let start = windows
            .iter()
            .filter(|w| w.start > since)
            .map(|w| w.start)
            .min();
        if let Some(start) = start {
            return windows.front().map(|w| (w.start, start, sum));
        }
        None
    }

    fn records_written_since(&self, since: Instant) -> Option<(Instant, Instant, u64)> {
        Self::sum_windows(&self.records_written_windows, since).map(|r| (r.0, r.1, r.2 as u64))
    }
}

struct StatsRecorder {
    inner: Mutex<StatsRecorderInner>,
}

impl StatsRecorder {
    fn new() -> Self {
        Self {
            inner: Mutex::new(StatsRecorderInner {
                records_written: 0,
                records_written_windows: VecDeque::new(),
            }),
        }
    }

    fn record_records_written(&self, records: u64) {
        let now = Instant::now();
        let mut guard = self.inner.lock().expect("lock failed");
        guard.record_records_written(now, records);
    }

    fn records_written(&self) -> u64 {
        let guard = self.inner.lock().expect("lock failed");
        guard.records_written()
    }

    fn records_written_since(&self, since: Instant) -> Option<(Instant, Instant, u64)> {
        let guard = self.inner.lock().expect("lock failed");
        guard.records_written_since(since)
    }
}

const STAT_DUMP_INTERVAL: Duration = Duration::from_secs(10);

async fn dump_stats(stats: Arc<StatsRecorder>) {
    loop {
        let records_written = stats.records_written();
        let records_written_since =
            stats.records_written_since(Instant::now() - Duration::from_secs(60));
        let (write_rate, interval) =
            if let Some((last, start, records_written)) = records_written_since {
                let interval = last - start;
                (records_written as f32 / interval.as_secs() as f32, interval)
            } else {
                (0f32, Duration::from_secs(0))
            };

        info!("Stats Dump:");
        info!("---------------------------------------");
        info!("records written: {}", records_written);
        info!("write rate: {}/second over {:?}", write_rate, interval);

        tokio::time::sleep(STAT_DUMP_INTERVAL).await;
    }
}
