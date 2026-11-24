//! Thread-local read bookkeeping without shared writes.
//!
//! Each thread accumulates read counts and range hits locally and folds them into shared metrics
//! and [`RangeHits`] after [`RangeCacheTuning::stats_fold_events`] events or after
//! [`RangeCacheTuning::stats_fold_interval`] (checked at power-of-two event counts).
//!
//! [`RangeCacheTuning::stats_fold_events`]: super::RangeCacheTuning::stats_fold_events
//! [`RangeCacheTuning::stats_fold_interval`]: super::RangeCacheTuning::stats_fold_interval

use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use slatedb_common::clock::SystemClock;
use slatedb_common::metrics::CounterFn;

use super::stats::{ReadCount, READ_COUNTS};
use crate::db_stats::DbStats;

/// The most caches whose counts one thread keeps at a time. A thread that reads from more caches
/// folds and forgets the block it made first.
const MAX_BLOCKS: usize = 8;

/// The read count and the last access of one cached range, which folds update.
#[derive(Debug, Default)]
pub(crate) struct RangeHits {
    /// The reads that used the range, up to the last fold of each thread.
    reads: AtomicU64,
    /// The value of the cache's access clock at the last fold that counted a read of the range,
    /// or at its load.
    last_access: AtomicU64,
}

impl RangeHits {
    #[cfg(test)]
    pub(crate) fn reads(&self) -> u64 {
        self.reads.load(Ordering::Relaxed)
    }

    pub(crate) fn last_access(&self) -> u64 {
        self.last_access.load(Ordering::Relaxed)
    }

    /// Sets the last access when the range is published. No read uses a loading range, so no
    /// fold races this store.
    pub(crate) fn set_last_access(&self, clock: u64) {
        self.last_access.store(clock, Ordering::Relaxed);
    }

    fn fold(&self, reads: u64, clock: u64) {
        self.reads.fetch_add(reads, Ordering::Relaxed);
        if self.last_access.load(Ordering::Relaxed) < clock {
            self.last_access.fetch_max(clock, Ordering::Relaxed);
        }
    }
}

/// The database counters that cached reads also count.
struct DbRequests {
    gets: Arc<dyn CounterFn>,
    scans: Arc<dyn CounterFn>,
}

/// The shared side of the bookkeeping of one cache. Thread blocks keep a reference to it.
struct Shared {
    counters: [Arc<dyn CounterFn>; READ_COUNTS],
    /// Set by the first read. See [`HitStats::bind_db`].
    db: OnceLock<DbRequests>,
    /// Advances by two on each load and rebuild. A fold copies it to the ranges that it counts.
    access_clock: AtomicU64,
    fold_events: u32,
    fold_interval_ms: i64,
    /// The database clock, read at fold points only.
    clock: Arc<dyn SystemClock>,
    /// False once the cache is dropped, so threads can forget its block.
    alive: AtomicBool,
}

impl Shared {
    fn now_ms(&self) -> i64 {
        self.clock.now().timestamp_millis()
    }

    /// Adds `counts` to the metrics. This calls the metrics recorder.
    fn add_counts(&self, counts: &[u64; READ_COUNTS]) {
        for (counter, &n) in self.counters.iter().zip(counts) {
            if n > 0 {
                counter.increment(n);
            }
        }
        if let Some(db) = self.db.get() {
            let at = |count: ReadCount| counts[count.index()];
            let gets = at(ReadCount::GetHit) + at(ReadCount::GetAbsent) + at(ReadCount::GetMerge);
            if gets > 0 {
                db.gets.increment(gets);
            }
            let scans = at(ReadCount::Scan) + at(ReadCount::MixedScan);
            if scans > 0 {
                db.scans.increment(scans);
            }
        }
    }

    /// Counts one event in the shared values directly. Used when the thread's block is not
    /// available: during the thread's exit, or from inside a fold (a metrics recorder that
    /// reads the database).
    fn add_one(&self, count: ReadCount, range: Option<(u32, &Arc<RangeHits>)>) {
        let mut counts = [0; READ_COUNTS];
        counts[count.index()] = 1;
        if let Some((_, hits)) = range {
            hits.fold(1, self.access_clock.load(Ordering::Relaxed));
        }
        self.add_counts(&counts);
    }
}

/// The reads of one range that a thread counted since its last fold.
struct Tally {
    hits: Arc<RangeHits>,
    reads: u64,
}

/// One thread's counts for one cache.
struct Block {
    shared: Arc<Shared>,
    counts: [u64; READ_COUNTS],
    /// The counts of ranges by slot (see `CachedRange::slot`). A slot that a new range reuses
    /// folds the counts of its previous range first.
    ranges: Vec<Option<Tally>>,
    /// The slots of `ranges` with reads since the last fold.
    dirty: Vec<u32>,
    /// The events since the last fold.
    events: u32,
    /// The database clock at the last fold, in milliseconds.
    last_fold_ms: i64,
}

impl Block {
    fn new(shared: Arc<Shared>) -> Self {
        Self {
            counts: [0; READ_COUNTS],
            ranges: Vec::new(),
            dirty: Vec::new(),
            events: 0,
            last_fold_ms: shared.now_ms(),
            shared,
        }
    }

    /// Counts one event. Returns true if the block must fold now.
    fn add(&mut self, count: ReadCount, range: Option<(u32, &Arc<RangeHits>)>) -> bool {
        self.counts[count.index()] += 1;
        if let Some((slot, hits)) = range {
            self.add_range(slot, hits);
        }
        self.events += 1;
        self.events >= self.shared.fold_events
            || (self.events.is_power_of_two()
                && self.shared.now_ms() - self.last_fold_ms >= self.shared.fold_interval_ms)
    }

    fn add_range(&mut self, slot: u32, hits: &Arc<RangeHits>) {
        let index = slot as usize;
        if index >= self.ranges.len() {
            self.ranges.resize_with(index + 1, || None);
        }
        let entry = &mut self.ranges[index];
        if let Some(tally) = entry {
            if Arc::ptr_eq(&tally.hits, hits) {
                if tally.reads == 0 {
                    self.dirty.push(slot);
                }
                tally.reads += 1;
                return;
            }
        }
        // The first read of this range on this thread, or a range that reuses the slot of a
        // removed one. The block keeps its own reference, so the comparison above never
        // mistakes a new range for a freed one at the same address.
        let previous = entry.replace(Tally {
            hits: hits.clone(),
            reads: 1,
        });
        match previous {
            // The slot is in `dirty` already, now for the new range.
            Some(old) if old.reads > 0 => old
                .hits
                .fold(old.reads, self.shared.access_clock.load(Ordering::Relaxed)),
            _ => self.dirty.push(slot),
        }
    }

    /// Adds the counts to the shared values and clears them, and restarts the fold interval.
    /// This calls the metrics recorder.
    fn fold(&mut self) {
        self.last_fold_ms = self.shared.now_ms();
        self.flush();
    }

    /// Adds the counts to the shared values and clears them. This calls the metrics recorder,
    /// and not the clock, so a thread-local destructor can call it.
    fn flush(&mut self) {
        let clock = self.shared.access_clock.load(Ordering::Relaxed);
        for slot in self.dirty.drain(..) {
            if let Some(tally) = &mut self.ranges[slot as usize] {
                tally.hits.fold(tally.reads, clock);
                tally.reads = 0;
            }
        }
        let counts = std::mem::replace(&mut self.counts, [0; READ_COUNTS]);
        self.events = 0;
        self.shared.add_counts(&counts);
    }
}

/// The blocks of one thread, one per cache that it read from recently.
struct Blocks(Vec<Block>);

impl Blocks {
    fn block_for(&mut self, shared: &Arc<Shared>) -> &mut Block {
        let index = match self.0.iter().position(|b| Arc::ptr_eq(&b.shared, shared)) {
            Some(index) => index,
            None => {
                self.make_room();
                self.0.push(Block::new(shared.clone()));
                self.0.len() - 1
            }
        };
        &mut self.0[index]
    }

    /// Forgets the blocks of dropped caches, and the oldest block if there are still too many.
    fn make_room(&mut self) {
        self.0.retain_mut(|block| {
            let alive = block.shared.alive.load(Ordering::Relaxed);
            if !alive {
                block.flush();
            }
            alive
        });
        if self.0.len() >= MAX_BLOCKS {
            self.0.remove(0).flush();
        }
    }
}

impl Drop for Blocks {
    /// Folds the counts of an exiting thread. A panic in a metrics recorder must not unwind out
    /// of a thread-local destructor, which would abort the process.
    fn drop(&mut self) {
        for block in &mut self.0 {
            let _ = catch_unwind(AssertUnwindSafe(|| block.flush()));
        }
    }
}

thread_local! {
    static BLOCKS: RefCell<Blocks> = const { RefCell::new(Blocks(Vec::new())) };
}

/// The read bookkeeping of one cache. See the module documentation.
pub(crate) struct HitStats {
    shared: Arc<Shared>,
}

impl HitStats {
    pub(crate) fn new(
        counters: [Arc<dyn CounterFn>; READ_COUNTS],
        fold_events: u32,
        fold_interval: Duration,
        clock: Arc<dyn SystemClock>,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                counters,
                db: OnceLock::new(),
                access_clock: AtomicU64::new(0),
                fold_events: fold_events.max(1),
                fold_interval_ms: i64::try_from(fold_interval.as_millis()).unwrap_or(i64::MAX),
                clock,
                alive: AtomicBool::new(true),
            }),
        }
    }

    /// Gives the bookkeeping the database counters that cached reads also count. The read
    /// entry points call it before they count anything. After the first call it is one load of a
    /// value that nothing writes again.
    pub(crate) fn bind_db(&self, db_stats: &DbStats) {
        if self.shared.db.get().is_none() {
            let _ = self.shared.db.set(DbRequests {
                gets: db_stats.get_requests.clone(),
                scans: db_stats.scan_requests.clone(),
            });
        }
    }

    /// The cache's access clock. See [`RangeHits::last_access`].
    pub(crate) fn access_clock(&self) -> &AtomicU64 {
        &self.shared.access_clock
    }

    /// Counts a read that used the range in `slot` with `hits`.
    pub(crate) fn read(&self, slot: u32, hits: &Arc<RangeHits>, count: ReadCount) {
        self.record(count, Some((slot, hits)));
    }

    /// Counts an event of the read path that no range gets credit for.
    pub(crate) fn count(&self, count: ReadCount) {
        self.record(count, None);
    }

    fn record(&self, count: ReadCount, range: Option<(u32, &Arc<RangeHits>)>) {
        let counted = BLOCKS
            .try_with(|blocks| {
                // A metrics recorder that reads the database from inside a fold finds the block
                // borrowed. It counts directly.
                let Ok(mut blocks) = blocks.try_borrow_mut() else {
                    return false;
                };
                let block = blocks.block_for(&self.shared);
                if block.add(count, range) {
                    block.fold();
                }
                true
            })
            .unwrap_or(false);
        if !counted {
            self.shared.add_one(count, range);
        }
    }

    /// Folds this thread's counts for this cache now.
    #[cfg(test)]
    pub(crate) fn fold_current_thread(&self) {
        BLOCKS.with(|blocks| {
            let mut blocks = blocks.borrow_mut();
            if let Some(block) = blocks
                .0
                .iter_mut()
                .find(|b| Arc::ptr_eq(&b.shared, &self.shared))
            {
                block.fold();
            }
        });
    }
}

impl Drop for HitStats {
    fn drop(&mut self) {
        self.shared.alive.store(false, Ordering::Relaxed);
    }
}

/// One side of two threads that take turns in a test. A panic on one side drops its end, so the
/// other side's wait fails at once instead of blocking the test.
#[cfg(test)]
pub(crate) struct Turn {
    tx: std::sync::mpsc::Sender<()>,
    rx: std::sync::mpsc::Receiver<()>,
}

#[cfg(test)]
impl Turn {
    pub(crate) fn pair() -> (Turn, Turn) {
        let (a_tx, b_rx) = std::sync::mpsc::channel();
        let (b_tx, a_rx) = std::sync::mpsc::channel();
        (Turn { tx: a_tx, rx: a_rx }, Turn { tx: b_tx, rx: b_rx })
    }

    /// Lets the other side go on.
    pub(crate) fn go(&self) {
        let _ = self.tx.send(());
    }

    /// Waits until the other side lets this side go on.
    pub(crate) fn wait(&self) {
        self.rx.recv().expect("the other thread of the test failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb_common::clock::MockSystemClock;
    use slatedb_common::metrics::{DefaultMetricsRecorder, MetricsRecorderHelper};

    /// Bookkeeping on a mock clock that stays at 0 unless a test sets it.
    fn stats_on(
        fold_events: u32,
        fold_interval: Duration,
        clock: Arc<MockSystemClock>,
    ) -> (HitStats, Arc<DefaultMetricsRecorder>) {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(recorder.clone(), Default::default());
        let counters = super::super::stats::read_counters(&helper);
        (
            HitStats::new(counters, fold_events, fold_interval, clock),
            recorder,
        )
    }

    fn stats(fold_events: u32, fold_interval: Duration) -> (HitStats, Arc<DefaultMetricsRecorder>) {
        stats_on(fold_events, fold_interval, Arc::new(MockSystemClock::new()))
    }

    fn hits_metric(recorder: &DefaultMetricsRecorder) -> u64 {
        slatedb_common::metrics::lookup_metric_with_labels(
            recorder,
            super::super::stats::GET_COUNT,
            &[("result", "hit")],
        )
        .unwrap_or(0) as u64
    }

    /// Runs `f` on a new thread, so that its blocks fold when it exits. The explicit join waits
    /// for the thread's exit, thread-local destructors included.
    fn on_thread(f: impl FnOnce() + Send) {
        std::thread::scope(|s| s.spawn(f).join().unwrap());
    }

    #[test]
    fn should_not_write_shared_counters_between_folds() {
        let (stats, recorder) = stats(1_000_000, Duration::from_secs(3600));
        let hits = Arc::new(RangeHits::default());
        std::thread::scope(|s| {
            let (main, worker) = Turn::pair();
            s.spawn(|| {
                let worker = worker;
                for _ in 0..10_000 {
                    stats.read(0, &hits, ReadCount::GetHit);
                }
                worker.go();
                // The main thread looks at the shared values now.
                worker.wait();
                stats.fold_current_thread();
                worker.go();
            });
            main.wait();
            // 10,000 reads, fewer than the fold size and well within the fold interval: none of
            // them may have reached a shared value.
            assert_eq!(hits_metric(&recorder), 0);
            assert_eq!(hits.reads(), 0);
            assert_eq!(hits.last_access(), 0);
            stats.access_clock().store(6, Ordering::Relaxed);
            main.go();
            main.wait();
            assert_eq!(hits_metric(&recorder), 10_000);
            assert_eq!(hits.reads(), 10_000);
            assert_eq!(hits.last_access(), 6);
        });
    }

    #[test]
    fn should_fold_after_the_fold_size() {
        let (stats, recorder) = stats(100, Duration::from_secs(3600));
        let hits = Arc::new(RangeHits::default());
        on_thread(|| {
            for i in 1..=250u64 {
                stats.read(3, &hits, ReadCount::GetHit);
                // Exactly the whole folds are visible, from this thread itself.
                assert_eq!(hits.reads(), i / 100 * 100);
                assert_eq!(hits_metric(&recorder), i / 100 * 100);
            }
        });
        // The exit of the thread folds the rest.
        assert_eq!(hits.reads(), 250);
        assert_eq!(hits_metric(&recorder), 250);
    }

    #[test]
    fn should_fold_after_the_fold_interval() {
        let clock = Arc::new(MockSystemClock::new());
        let (stats, recorder) = stats_on(1_000_000, Duration::from_millis(10), clock.clone());
        let hits = Arc::new(RangeHits::default());
        let read = |n: u64| {
            for _ in 0..n {
                stats.read(0, &hits, ReadCount::GetHit);
            }
        };
        on_thread(|| {
            read(3);
            clock.set(10);
            assert_eq!(hits.reads(), 0);
            // The 4th event after the last fold checks the time, and folds.
            read(1);
            assert_eq!(hits.reads(), 4);
            assert_eq!(hits_metric(&recorder), 4);
            // Within the interval, no fold.
            clock.set(19);
            read(4);
            assert_eq!(hits.reads(), 4);
            clock.set(20);
            // The 5th to 7th events do not check the time. The 8th does.
            read(3);
            assert_eq!(hits.reads(), 4);
            read(1);
            assert_eq!(hits.reads(), 12);
        });
        assert_eq!(hits.reads(), 12);
    }

    #[test]
    fn should_converge_to_the_true_counts() {
        let (stats, recorder) = stats(4096, Duration::from_millis(10));
        let ranges: Vec<Arc<RangeHits>> = (0..5).map(|_| Arc::new(RangeHits::default())).collect();
        std::thread::scope(|s| {
            let threads: Vec<_> = (0..8u64)
                .map(|t| {
                    let (stats, ranges) = (&stats, &ranges);
                    s.spawn(move || {
                        for i in 0..20_000u64 {
                            let r = ((i + t) % 5) as usize;
                            stats.read(r as u32, &ranges[r], ReadCount::GetHit);
                            if i % 7 == 0 {
                                stats.count(ReadCount::RetryGetSealed);
                            }
                        }
                    })
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
        });
        let total: u64 = ranges.iter().map(|r| r.reads()).sum();
        assert_eq!(total, 8 * 20_000);
        for r in &ranges {
            assert_eq!(r.reads(), 8 * 4_000);
        }
        assert_eq!(hits_metric(&recorder), 8 * 20_000);
        let retries = slatedb_common::metrics::lookup_metric_with_labels(
            &recorder,
            super::super::stats::PIN_RETRY_COUNT,
            &[("op", "get"), ("reason", "sealed")],
        );
        assert_eq!(retries, Some((8 * 20_000u64.div_ceil(7)) as i64));
    }

    #[test]
    fn should_credit_a_reused_slot_to_the_right_range() {
        let (stats, _recorder) = stats(1_000_000, Duration::from_secs(3600));
        let old = Arc::new(RangeHits::default());
        let new = Arc::new(RangeHits::default());
        on_thread(|| {
            for _ in 0..5 {
                stats.read(2, &old, ReadCount::GetHit);
            }
            // A new range takes slot 2. The old range's reads fold at once.
            stats.read(2, &new, ReadCount::GetHit);
            assert_eq!(old.reads(), 5);
            stats.read(2, &new, ReadCount::GetHit);
            // A read of the old range, pinned before its removal, takes the slot back.
            stats.read(2, &old, ReadCount::GetHit);
            assert_eq!(new.reads(), 2);
        });
        assert_eq!(old.reads(), 6);
        assert_eq!(new.reads(), 2);
    }

    #[test]
    fn should_keep_caches_apart_on_one_thread() {
        let (a, recorder_a) = stats(1_000_000, Duration::from_secs(3600));
        let (b, recorder_b) = stats(1_000_000, Duration::from_secs(3600));
        let hits_a = Arc::new(RangeHits::default());
        let hits_b = Arc::new(RangeHits::default());
        on_thread(|| {
            for _ in 0..3 {
                a.read(0, &hits_a, ReadCount::GetHit);
                b.read(0, &hits_b, ReadCount::GetHit);
                b.read(0, &hits_b, ReadCount::GetHit);
            }
        });
        assert_eq!((hits_a.reads(), hits_b.reads()), (3, 6));
        assert_eq!(hits_metric(&recorder_a), 3);
        assert_eq!(hits_metric(&recorder_b), 6);
    }

    #[test]
    fn should_fold_the_oldest_block_beyond_the_limit() {
        let mut caches: Vec<_> = (0..MAX_BLOCKS + 3)
            .map(|_| stats(1_000_000, Duration::from_secs(3600)))
            .collect();
        let (first, first_recorder) = caches.remove(0);
        let (first_hits, other_hits) = (RangeHits::default(), RangeHits::default());
        let (first_hits, other_hits) = (Arc::new(first_hits), Arc::new(other_hits));
        std::thread::scope(|s| {
            let (main, worker) = Turn::pair();
            s.spawn(|| {
                let worker = worker;
                first.read(0, &first_hits, ReadCount::GetHit);
                worker.go();
                worker.wait();
                // More caches than a thread keeps: the oldest block folds and goes.
                for (stats, _) in &caches {
                    stats.read(0, &other_hits, ReadCount::GetHit);
                }
                worker.go();
            });
            main.wait();
            assert_eq!(hits_metric(&first_recorder), 0);
            assert_eq!(first_hits.reads(), 0);
            main.go();
            main.wait();
            assert_eq!(hits_metric(&first_recorder), 1);
            assert_eq!(first_hits.reads(), 1);
        });
    }

    #[test]
    fn should_fold_the_block_of_a_dropped_cache() {
        let (dropped, dropped_recorder) = stats(1_000_000, Duration::from_secs(3600));
        let (other, _other_recorder) = stats(1_000_000, Duration::from_secs(3600));
        let hits = Arc::new(RangeHits::default());
        let dropped = std::sync::Mutex::new(Some(dropped));
        std::thread::scope(|s| {
            let (main, worker) = Turn::pair();
            s.spawn(|| {
                let worker = worker;
                if let Some(stats) = dropped.lock().unwrap().as_ref() {
                    stats.read(0, &hits, ReadCount::GetHit);
                }
                worker.go();
                worker.wait();
                // A block for a new cache makes room: the dropped cache's block folds and goes.
                other.read(0, &hits, ReadCount::GetHit);
                worker.go();
            });
            main.wait();
            drop(dropped.lock().unwrap().take());
            assert_eq!(hits_metric(&dropped_recorder), 0);
            main.go();
            main.wait();
            assert_eq!(hits_metric(&dropped_recorder), 1);
        });
    }
}
