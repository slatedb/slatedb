//! # Transaction benchmarker
//!
//! This module contains the transaction benchmarker, which is used to benchmark
//! SlateDB transactions under concurrent load. The benchmarker supports:
//!
//! - Configurable transaction size (operations per transaction)
//! - Configurable abort percentage
//! - Comparison with WriteBatch
//! - Multiple isolation levels (Snapshot, SerializableSnapshot)
//! - Concurrent workloads with contention
//!
//! ## Design
//!
//! The benchmarker spins up `concurrency` tasks, each of which runs a loop.
//! Each task repeatedly:
//! 1. Begins a transaction (or creates a WriteBatch for comparison)
//! 2. Performs `transaction_size` operations (puts/deletes)
//! 3. Either commits or aborts based on `abort_percentage`
//!
//! Stats are tracked separately for commits, aborts, and conflicts, and are
//! dumped periodically to the console.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use rand::{Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use slatedb::config::WriteOptions;
use slatedb::{Db, IsolationLevel, WriteBatch};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::db::KeyGenerator;
use crate::stats::{StatsRecorder, WindowStats};

/// How frequently to dump stats to the console.
const STAT_DUMP_INTERVAL: Duration = Duration::from_secs(10);

/// How far back to look when dumping stats.
const STAT_DUMP_LOOKBACK: Duration = Duration::from_secs(60);

/// How frequently to update stats and check if we need to dump new stats.
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

/// The transaction benchmarker.
pub struct TransactionBench {
    key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
    val_len: usize,
    write_options: WriteOptions,
    concurrency: u32,
    duration: Option<Duration>,
    transaction_size: u32,
    abort_percentage: u32,
    use_write_batch: bool,
    isolation_level: IsolationLevel,
    db: Arc<Db>,
}

impl TransactionBench {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
        val_len: usize,
        write_options: WriteOptions,
        concurrency: u32,
        duration: Option<Duration>,
        transaction_size: u32,
        abort_percentage: u32,
        use_write_batch: bool,
        isolation_level: IsolationLevel,
        db: Arc<Db>,
    ) -> Self {
        Self {
            key_gen_supplier,
            val_len,
            write_options,
            concurrency,
            duration,
            transaction_size,
            abort_percentage,
            use_write_batch,
            isolation_level,
            db,
        }
    }

    /// Run the benchmarker.
    ///
    /// This method spins up `concurrency` tasks, each of which runs a loop,
    /// and then waits for all the tasks to complete. It also spawns a task
    /// to dump stats to the console.
    pub async fn run(&self) {
        let stats_recorder = Arc::new(TransactionStatsRecorder::new());
        let mut tasks = Vec::new();
        for _ in 0..self.concurrency {
            let mut task = TransactionTask::new(
                (*self.key_gen_supplier)(),
                self.val_len,
                self.write_options.clone(),
                self.duration,
                self.transaction_size,
                self.abort_percentage,
                self.use_write_batch,
                self.isolation_level,
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

struct TransactionTask {
    key_generator: Box<dyn KeyGenerator>,
    val_len: usize,
    write_options: WriteOptions,
    duration: Option<Duration>,
    transaction_size: u32,
    abort_percentage: u32,
    use_write_batch: bool,
    isolation_level: IsolationLevel,
    stats_recorder: Arc<TransactionStatsRecorder>,
    db: Arc<Db>,
}

impl TransactionTask {
    #[allow(clippy::too_many_arguments)]
    fn new(
        key_generator: Box<dyn KeyGenerator>,
        val_len: usize,
        write_options: WriteOptions,
        duration: Option<Duration>,
        transaction_size: u32,
        abort_percentage: u32,
        use_write_batch: bool,
        isolation_level: IsolationLevel,
        stats_recorder: Arc<TransactionStatsRecorder>,
        db: Arc<Db>,
    ) -> Self {
        Self {
            key_generator,
            val_len,
            write_options,
            duration,
            transaction_size,
            abort_percentage,
            use_write_batch,
            isolation_level,
            stats_recorder,
            db,
        }
    }

    /// Run the task.
    ///
    /// This method runs a loop, executing transactions with multiple operations.
    async fn run(&mut self) {
        let mut random = rand_xorshift::XorShiftRng::from_os_rng();
        let mut commits = 0u64;
        let mut aborts = 0u64;
        let mut conflicts = 0u64;
        let mut total_ops = 0u64;
        let duration = self.duration.unwrap_or(Duration::MAX);
        let start = Instant::now();
        let mut last_report = start;

        while start.elapsed() < duration {
            let should_abort = random.random_range(0..100) < self.abort_percentage;

            if self.use_write_batch {
                // Use WriteBatch for comparison
                let result = self.run_write_batch(&mut random, should_abort).await;
                match result {
                    Ok(ops) => {
                        commits += 1;
                        total_ops += ops;
                    }
                    Err(_) => {
                        conflicts += 1;
                    }
                }
            } else {
                let result = self.run_transaction(&mut random, should_abort).await;
                match result {
                    TransactionResult::Committed(ops) => {
                        commits += 1;
                        total_ops += ops;
                    }
                    TransactionResult::Aborted => {
                        aborts += 1;
                    }
                    TransactionResult::Conflict => {
                        conflicts += 1;
                    }
                }
            }

            if last_report.elapsed() >= REPORT_INTERVAL {
                last_report = Instant::now();
                self.stats_recorder
                    .record(last_report, commits, aborts, conflicts, total_ops);
                commits = 0;
                aborts = 0;
                conflicts = 0;
                total_ops = 0;
            }
        }
    }

    async fn run_write_batch(
        &mut self,
        random: &mut XorShiftRng,
        should_abort: bool,
    ) -> Result<u64, slatedb::Error> {
        if should_abort {
            return Ok(0);
        }

        let mut batch = WriteBatch::new();
        let ops = self.transaction_size;

        for _ in 0..ops {
            let key = self.key_generator.next_key();
            let mut value = vec![0; self.val_len];
            random.fill_bytes(value.as_mut_slice());
            batch.put(key, value);
        }

        match self.db.write_with_options(batch, &self.write_options).await {
            Ok(_) => Ok(ops as u64),
            Err(e) => {
                warn!("write batch failed [error={}]", e);
                Err(e)
            }
        }
    }

    async fn run_transaction(
        &mut self,
        random: &mut XorShiftRng,
        should_abort: bool,
    ) -> TransactionResult {
        let txn = match self.db.begin(self.isolation_level).await {
            Ok(t) => t,
            Err(e) => {
                warn!("begin transaction failed [error={}]", e);
                return TransactionResult::Conflict;
            }
        };

        let ops = self.transaction_size;

        for _ in 0..ops {
            let key = self.key_generator.next_key();
            let mut value = vec![0; self.val_len];
            random.fill_bytes(value.as_mut_slice());
            if let Err(e) = txn.put(key, value) {
                warn!("transaction put failed [error={}]", e);
                txn.rollback();
                return TransactionResult::Aborted;
            }
        }

        if should_abort {
            txn.rollback();
            return TransactionResult::Aborted;
        }

        match txn.commit_with_options(&self.write_options).await {
            Ok(_) => TransactionResult::Committed(ops as u64),
            Err(e) => {
                warn!("transaction commit failed (conflict) [error={}]", e);
                TransactionResult::Conflict
            }
        }
    }
}

#[derive(Debug)]
enum TransactionResult {
    Committed(u64),
    Aborted,
    Conflict,
}

/// Represents the number of commits, aborts, and conflicts in a window of time.
#[derive(Debug)]
struct TransactionWindow {
    range: Range<Instant>,
    commits: u64,
    aborts: u64,
    conflicts: u64,
    total_ops: u64,
}

impl Default for TransactionWindow {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            range: now..now,
            commits: 0,
            aborts: 0,
            conflicts: 0,
            total_ops: 0,
        }
    }
}

impl WindowStats for TransactionWindow {
    fn range(&self) -> Range<Instant> {
        self.range.clone()
    }

    fn set_range(&mut self, range: Range<Instant>) {
        self.range = range;
    }
}

struct TransactionStatsRecorder {
    recorder: StatsRecorder<TransactionWindow>,
    // Overall totals tracked separately
    total_commits: std::sync::atomic::AtomicU64,
    total_aborts: std::sync::atomic::AtomicU64,
    total_conflicts: std::sync::atomic::AtomicU64,
    total_ops: std::sync::atomic::AtomicU64,
}

impl TransactionStatsRecorder {
    fn new() -> Self {
        Self {
            recorder: StatsRecorder::new(),
            total_commits: std::sync::atomic::AtomicU64::new(0),
            total_aborts: std::sync::atomic::AtomicU64::new(0),
            total_conflicts: std::sync::atomic::AtomicU64::new(0),
            total_ops: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn record(&self, now: Instant, commits: u64, aborts: u64, conflicts: u64, ops: u64) {
        self.total_commits
            .fetch_add(commits, std::sync::atomic::Ordering::Relaxed);
        self.total_aborts
            .fetch_add(aborts, std::sync::atomic::Ordering::Relaxed);
        self.total_conflicts
            .fetch_add(conflicts, std::sync::atomic::Ordering::Relaxed);
        self.total_ops
            .fetch_add(ops, std::sync::atomic::Ordering::Relaxed);

        self.recorder.record(now, |window| {
            window.commits += commits;
            window.aborts += aborts;
            window.conflicts += conflicts;
            window.total_ops += ops;
        });
    }

    fn commits(&self) -> u64 {
        self.total_commits
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn aborts(&self) -> u64 {
        self.total_aborts.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn conflicts(&self) -> u64 {
        self.total_conflicts
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn total_ops(&self) -> u64 {
        self.total_ops.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn stats_since(&self, lookback: Duration) -> Option<(Range<Instant>, u64, u64, u64, u64)> {
        self.recorder.stats_since(lookback, |range, windows| {
            let commits = windows.iter().map(|w| w.commits).sum();
            let aborts = windows.iter().map(|w| w.aborts).sum();
            let conflicts = windows.iter().map(|w| w.conflicts).sum();
            let ops = windows.iter().map(|w| w.total_ops).sum();
            (range, commits, aborts, conflicts, ops)
        })
    }
}

async fn dump_stats(stats: Arc<TransactionStatsRecorder>) {
    let mut last_stats_dump: Option<Instant> = None;
    let mut first_dump_start: Option<Instant> = None;
    loop {
        tokio::time::sleep(REPORT_INTERVAL).await;

        let stats_since = stats.stats_since(STAT_DUMP_LOOKBACK);
        if let Some((range, commits_since, aborts_since, conflicts_since, ops_since)) = stats_since
        {
            let interval = range.end - range.start;
            let total_commits = stats.commits();
            let total_aborts = stats.aborts();
            let total_conflicts = stats.conflicts();
            let total_ops = stats.total_ops();
            let should_print = match last_stats_dump {
                Some(last_stats_dump) => (range.end - last_stats_dump) >= STAT_DUMP_INTERVAL,
                None => (range.end - range.start) >= STAT_DUMP_INTERVAL,
            };
            first_dump_start = first_dump_start.or(Some(range.start));
            if should_print {
                let commit_rate = commits_since as f32 / interval.as_secs() as f32;
                let abort_rate = aborts_since as f32 / interval.as_secs() as f32;
                let conflict_rate = conflicts_since as f32 / interval.as_secs() as f32;
                let ops_rate = ops_since as f32 / interval.as_secs() as f32;

                let total_txns = commits_since + aborts_since + conflicts_since;
                let commit_pct = if total_txns > 0 {
                    commits_since as f32 / total_txns as f32 * 100.0
                } else {
                    0.0
                };
                let abort_pct = if total_txns > 0 {
                    aborts_since as f32 / total_txns as f32 * 100.0
                } else {
                    0.0
                };
                let conflict_pct = if total_txns > 0 {
                    conflicts_since as f32 / total_txns as f32 * 100.0
                } else {
                    0.0
                };

                info!(
                    "txn stats [elapsed {:?}, commit/s: {:.3} ({:.1}%), abort/s: {:.3} ({:.1}%), conflict/s: {:.3} ({:.1}%), ops/s: {:.3}, window: {:?}, total: commits={}, aborts={}, conflicts={}, ops={}]",
                    range.end.duration_since(first_dump_start.unwrap()).as_secs_f64(),
                    commit_rate,
                    commit_pct,
                    abort_rate,
                    abort_pct,
                    conflict_rate,
                    conflict_pct,
                    ops_rate,
                    range.end - range.start,
                    total_commits,
                    total_aborts,
                    total_conflicts,
                    total_ops,
                );
                last_stats_dump = Some(range.end);
            }
        }
    }
}
