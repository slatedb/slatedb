use bytes::Bytes;
use leaky_bucket::RateLimiter;
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use slatedb::config::{ReadOptions, WriteOptions};
use slatedb::db::Db;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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
    val_size: usize,
    write_options: WriteOptions,
    read_options: ReadOptions,
    write_pct: u32,
    rate: Option<u32>,
    tasks: u32,
    num_rows: Option<u64>,
    duration: Option<Duration>,
    db: Arc<Db>,
}

impl DbBench {
    #[allow(clippy::too_many_arguments)]
    pub fn read_write(
        key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
        val_size: usize,
        write_options: WriteOptions,
        read_options: ReadOptions,
        rate: Option<u32>,
        tasks: u32,
        num_rows: Option<u64>,
        duration: Option<Duration>,
        db: Arc<Db>,
    ) -> Self {
        Self {
            key_gen_supplier,
            val_size,
            write_options,
            rate,
            read_options,
            write_pct: 100,
            tasks,
            num_rows,
            duration,
            db,
        }
    }

    pub async fn run(&self) {
        let rate_limiter = self.rate.map(|r| {
            Arc::new(
                RateLimiter::builder()
                    .initial(r as usize)
                    .max(r as usize)
                    .interval(Duration::from_millis(1))
                    .refill((r / 1000) as usize)
                    .build(),
            )
        });
        let stats_recorder = Arc::new(StatsRecorder::new());
        let mut write_tasks = Vec::new();
        for _ in 0..self.tasks {
            let mut write_task = ReadWriteTask::new(
                (*self.key_gen_supplier)(),
                self.val_size,
                self.write_options.clone(),
                self.read_options.clone(),
                self.write_pct,
                self.num_rows,
                self.duration,
                rate_limiter.clone(),
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

struct ReadWriteTask {
    key_generator: Box<dyn KeyGenerator>,
    val_size: usize,
    write_options: WriteOptions,
    read_options: ReadOptions,
    write_pct: u32,
    num_keys: Option<u64>,
    duration: Option<Duration>,
    rate_limiter: Option<Arc<RateLimiter>>,
    stats_recorder: Arc<StatsRecorder>,
    db: Arc<Db>,
}

impl ReadWriteTask {
    #[allow(clippy::too_many_arguments)]
    fn new(
        key_generator: Box<dyn KeyGenerator>,
        val_size: usize,
        write_options: WriteOptions,
        read_options: ReadOptions,
        write_pct: u32,
        num_keys: Option<u64>,
        duration: Option<Duration>,
        rate_limiter: Option<Arc<RateLimiter>>,
        stats_recorder: Arc<StatsRecorder>,
        db: Arc<Db>,
    ) -> Self {
        assert!(write_pct <= 100);
        Self {
            key_generator,
            val_size,
            write_options,
            read_options,
            write_pct,
            num_keys,
            duration,
            rate_limiter,
            stats_recorder,
            db,
        }
    }

    fn write(&self, rng: &mut XorShiftRng) -> bool {
        loop {
            let rand = rng.next_u32();
            if rand < u32::MAX - u32::MAX % 100 {
                return rand % 100 < self.write_pct
            }
        }
    }

    async fn run(&mut self) {
        let start = std::time::Instant::now();
        let mut keys_done = 0u64;
        let batch = 4;
        let mut rng = rand_xorshift::XorShiftRng::from_entropy();
        loop {
            if start.elapsed() >= self.duration.unwrap_or(Duration::MAX) {
                break;
            }
            if keys_done >= self.num_keys.unwrap_or(u64::MAX) {
                break;
            }
            if let Some(rate_limiter) = &self.rate_limiter {
                rate_limiter.acquire(batch).await;
            }
            let mut writes: u64 = 0;
            let mut reads: u64 = 0;
            let mut negative_reads: u64 = 0;
            let mut total_elapsed_reads: u128 = 0;
            for _ in 0..batch {
                let key = self.key_generator.next_key();
                if self.write(&mut rng) {
                    let mut value = vec![0; self.val_size];
                    rng.fill_bytes(value.as_mut_slice());
                    self.db
                        .put_with_options(key.as_ref(), value.as_ref(), &self.write_options)
                        .await;
                    writes += 1;
                } else {
                    let start = std::time::Instant::now();
                    let result = self.db
                        .get_with_options(key.as_ref(), &self.read_options)
                        .await
                        .expect("read failed");
                    reads += 1;
                    total_elapsed_reads += start.elapsed().as_millis();
                    if result.is_none() {
                        negative_reads += 1;
                    }
                }
            }
            self.stats_recorder.record_records_written(writes);
            self.stats_recorder.record_records_read(reads, negative_reads, total_elapsed_reads);
            keys_done += batch as u64;
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
    records_read: u64,
    records_read_windows: VecDeque<Window>,
    records_read_negative_windows: VecDeque<Window>,
    records_read_elapsed_windows: VecDeque<Window>,
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

    fn record_records_read(&mut self, now: Instant, records: u64) {
        Self::maybe_roll_window(now, &mut self.records_read_windows);
        if let Some(front) = self.records_read_windows.front_mut() {
            front.value += records as f32;
            front.last = now;
        }
        self.records_read += records;
    }

    fn record_records_read_negative(&mut self, now: Instant, records: u64) {
        Self::maybe_roll_window(now, &mut self.records_read_negative_windows);
        if let Some(front) = self.records_read_negative_windows.front_mut() {
            front.value += records as f32;
            front.last = now;
        }
    }

    fn record_records_read_elapsed(&mut self, now: Instant, elapsed: u128) {
        Self::maybe_roll_window(now, &mut self.records_read_elapsed_windows);
        if let Some(front) = self.records_read_elapsed_windows.front_mut() {
            front.value += elapsed as f32;
            front.last = now;
        }
    }

    fn records_written(&self) -> u64 {
        self.records_written
    }

    fn records_read(&self) -> u64 {
        self.records_read
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

    fn records_read_since(&self, since: Instant) -> Option<(Instant, Instant, u64)> {
        Self::sum_windows(&self.records_read_windows, since).map(|r| (r.0, r.1, r.2 as u64))
    }

    fn records_read_negative_since(&self, since: Instant) -> Option<(Instant, Instant, u64)> {
        Self::sum_windows(&self.records_read_negative_windows, since).map(|r| (r.0, r.1, r.2 as u64))
    }

    fn records_read_elapsed_since(&self, since: Instant) -> Option<(Instant, Instant, u128)> {
        Self::sum_windows(&self.records_read_elapsed_windows, since).map(|r| (r.0, r.1, r.2 as u128))
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
                records_read: 0,
                records_read_windows: VecDeque::new(),
                records_read_negative_windows: VecDeque::new(),
                records_read_elapsed_windows: VecDeque::new(),
            }),
        }
    }

    fn record_records_written(&self, records: u64) {
        let now = Instant::now();
        let mut guard = self.inner.lock().expect("lock failed");
        guard.record_records_written(now, records);
    }

    fn record_records_read(&self, records: u64, neg_reads: u64, total_elapsed: u128) {
        let now = Instant::now();
        let mut guard = self.inner.lock().expect("lock failed");
        guard.record_records_read(now, records);
        guard.record_records_read_negative(now, neg_reads);
        guard.record_records_read_elapsed(now ,total_elapsed);
    }

    fn records_written(&self) -> u64 {
        let guard = self.inner.lock().expect("lock failed");
        guard.records_written()
    }

    fn records_read(&self) -> u64 {
        let guard = self.inner.lock().expect("lock failed");
        guard.records_read()
    }

    fn records_written_since(&self, since: Instant) -> Option<(Instant, Instant, u64)> {
        let guard = self.inner.lock().expect("lock failed");
        guard.records_written_since(since)
    }

    fn records_read_since(&self, since: Instant) -> Option<(Instant, Instant, u64)> {
        let guard = self.inner.lock().expect("lock failed");
        guard.records_read_since(since)
    }

    fn records_read_negative_since(&self, since: Instant) -> Option<(Instant, Instant, u64)> {
        let guard = self.inner.lock().expect("lock failed");
        guard.records_read_negative_since(since)
    }

    fn records_read_elapsed_since(&self, since: Instant) -> Option<(Instant, Instant, u128)> {
        let guard = self.inner.lock().expect("lock failed");
        guard.records_read_elapsed_since(since)
    }
}

const STAT_DUMP_INTERVAL: Duration = Duration::from_secs(10);

async fn dump_stats(stats: Arc<StatsRecorder>) {
    loop {
        let records_written = stats.records_written();
        let sample_start = Instant::now() - Duration::from_secs(60);
        let records_written_since = stats.records_written_since(sample_start);
        let (write_rate, write_interval) =
            if let Some((last, start, records_written)) = records_written_since {
                let interval = last - start;
                (records_written as f32 / interval.as_secs() as f32, interval)
            } else {
                (0f32, Duration::from_secs(0))
            };
        let records_read = stats.records_read();
        let records_read_since = stats.records_read_since(sample_start);
        let (read_rate, read_lat, read_interval) =
            if let Some((last, start, records_read)) = records_read_since {
                let (_, _, total_elapsed) = stats.records_read_elapsed_since(sample_start).unwrap();
                let interval = last - start;
                (
                    records_read as f32 / interval.as_secs() as f32,
                    total_elapsed as f32 / records_read as f32,
                    interval
                )
            } else {
                (0f32, 0f32, Duration::from_secs(0))
            };
        let records_read_negative_since = stats.records_read_negative_since(sample_start);
        let (negative_read_rate, negative_read_interval) =
            if let Some((last, start, records_read_neg)) = records_read_negative_since {
                let interval = last - start;
                (records_read_neg as f32 / interval.as_secs() as f32, interval)
            } else {
                (0f32, Duration::from_secs(0))
            };

        println!("Stats Dump:");
        println!("---------------------------------------");
        println!("records written: {}", records_written);
        println!("write rate: {}/second over {:?}", write_rate, write_interval);
        println!("records read: {}", records_read);
        println!("read rate: {}/second over {:?}", read_rate, read_interval);
        println!("avg read lat: {}", read_lat);
        println!("read neg rate: {}/second over {:?}", negative_read_rate, negative_read_interval);
        println!();
        tokio::time::sleep(STAT_DUMP_INTERVAL).await;
    }
}
