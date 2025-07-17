use crate::utils;
use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
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
use slatedb::SlateDBError;
use slatedb::WriteBatch;
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::RangeBounds;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::debug;
use tracing::info;

#[derive(Clone)]
pub struct DstOptions {
    max_key_len: usize,
    max_val_len: usize,
    max_write_batch_bytes: usize,
    max_btree_size_bytes: usize,
}

impl Default for DstOptions {
    fn default() -> Self {
        Self {
            max_key_len: u16::MAX as usize, // keys are limited to 65_535 bytes
            max_val_len: 1024 * 1024,       // 1 MiB
            max_write_batch_bytes: 50 * 1024 * 1024, // 50 MiB
            max_btree_size_bytes: 2 * 1024 * 1024 * 1024, // 2 GiB
        }
    }
}

// Because WriteOp is only pub(crate)
pub type DstWriteOp = (Vec<u8>, Option<Vec<u8>>, PutOptions);

pub enum DstAction {
    Write(Vec<DstWriteOp>, WriteOptions),
    Get(Vec<u8>, ReadOptions),
    Scan(Vec<u8>, Vec<u8>, ScanOptions),
    Flush,
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
                write!(f, "AdvanceTime({:?})", utils::pretty_duration(duration))
            }
        }
    }
}

impl std::fmt::Debug for DstAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn truncate_bytes(bytes: &[u8]) -> &[u8] {
            if bytes.len() < 8 {
                return bytes;
            }
            &bytes[..8]
        }
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

pub trait DstDistribution {
    fn sample_action(&self, state: &SizedBTreeMap<Vec<u8>, Vec<u8>>) -> DstAction;
    fn sample_write(&self, state: &SizedBTreeMap<Vec<u8>, Vec<u8>>) -> DstAction;
    fn sample_get(&self, state: &SizedBTreeMap<Vec<u8>, Vec<u8>>) -> DstAction;
    fn sample_scan(&self, state: &SizedBTreeMap<Vec<u8>, Vec<u8>>) -> DstAction;
    fn sample_flush(&self) -> DstAction;
    fn sample_advance_time(&self) -> DstAction;
}

pub struct DefaultDstDistribution {
    options: DstOptions,
    rand: Rc<DbRand>,
}

impl DefaultDstDistribution {
    pub fn new(rand: Rc<DbRand>, options: DstOptions) -> Self {
        Self { options, rand }
    }

    #[inline]
    fn gen_key(&self, state: &SizedBTreeMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
        if let Some(existing_key) = self.maybe_gen_existing_key(state) {
            return existing_key;
        }
        let key_len = self.rand.rng().random_range(1..self.options.max_key_len);
        let mut bytes = vec![0; key_len];
        self.rand.rng().fill_bytes(&mut bytes);
        bytes
    }

    #[inline]
    fn maybe_gen_existing_key(&self, state: &SizedBTreeMap<Vec<u8>, Vec<u8>>) -> Option<Vec<u8>> {
        let hit_probability = self.rand.rng().random_range(0.0..1.0);
        let is_db_hit = state.len() > 0 && self.rand.rng().random_bool(hit_probability);
        if is_db_hit {
            let existing_key = state
                .keys()
                .choose(&mut self.rand.rng())
                .expect("can't pick a key for an empty state")
                .clone();
            Some(existing_key)
        } else {
            None
        }
    }

    #[inline]
    fn gen_val(&self) -> Vec<u8> {
        let val_len = self.rand.rng().random_range(1..self.options.max_val_len);
        let mut bytes = vec![0; val_len];
        self.rand.rng().fill_bytes(&mut bytes);
        bytes
    }

    #[inline]
    fn gen_put_options(&self) -> PutOptions {
        // TODO: implement ttl support
        PutOptions::default()
    }

    #[inline]
    fn get_write_options(&self) -> WriteOptions {
        let mut rng = self.rand.rng();
        WriteOptions {
            await_durable: rng.random_bool(0.5),
        }
    }

    #[inline]
    fn gen_read_options(&self) -> ReadOptions {
        // TODO: add random read options
        ReadOptions::default()
    }
}

impl DstDistribution for DefaultDstDistribution {
    fn sample_action(&self, state: &SizedBTreeMap<Vec<u8>, Vec<u8>>) -> DstAction {
        let weights = [1; 5]; // all actions have equal probability for now
        let dist = WeightedIndex::new(&weights).expect("non-empty weights and all ≥ 0");
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

    fn sample_write(&self, state: &SizedBTreeMap<Vec<u8>, Vec<u8>>) -> DstAction {
        let mut write_ops = Vec::new();
        let write_option = self.get_write_options();
        let put_probability = 0.8;
        let mut remaining_bytes =
            self.rand
                .rng()
                .random_range(1..=self.options.max_write_batch_bytes) as i64;
        while remaining_bytes > 0 {
            let is_put = self.rand.rng().random_bool(put_probability);
            if is_put {
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
        DstAction::Write(write_ops, write_option)
    }

    fn sample_get(&self, state: &SizedBTreeMap<Vec<u8>, Vec<u8>>) -> DstAction {
        DstAction::Get(self.gen_key(state), self.gen_read_options())
    }

    // TODO: add ScanOption variation
    fn sample_scan(&self, state: &SizedBTreeMap<Vec<u8>, Vec<u8>>) -> DstAction {
        if state.is_empty() {
            return DstAction::Scan(
                self.gen_key(state),
                self.gen_key(state),
                ScanOptions::default(),
            );
        }
        // Only scan non-empty ranges since SlateDB panics otherwise (by design, see #680)
        let start_key_prefix_idx = self.rand.rng().random_range(0..state.len());
        let end_key_prefix_idx = self
            .rand
            .rng()
            .random_range(start_key_prefix_idx..state.len())
            - start_key_prefix_idx;
        let mut keys = state.keys();
        let mut start_key_prefix = keys.nth(start_key_prefix_idx).unwrap().clone();
        let mut end_key_prefix = keys
            .nth(end_key_prefix_idx)
            .unwrap_or(&start_key_prefix)
            .clone();
        // TODO: only truncate sometimes
        start_key_prefix.truncate(8);
        end_key_prefix.truncate(8);
        DstAction::Scan(
            start_key_prefix.clone(),
            end_key_prefix.clone(),
            ScanOptions::default(),
        )
    }

    fn sample_flush(&self) -> DstAction {
        DstAction::Flush
    }

    fn sample_advance_time(&self) -> DstAction {
        let sleep_micros = self.rand.rng().random_range(0..10_000_000);
        DstAction::AdvanceTime(Duration::from_micros(sleep_micros))
    }
}

pub struct Dst {
    db: Db,
    system_clock: Arc<dyn SystemClock>,
    rand: Rc<DbRand>,
    action_sampler: Box<dyn DstDistribution>,
    options: DstOptions,
    state: SizedBTreeMap<Vec<u8>, Vec<u8>>,
}

impl Dst {
    pub fn new(
        db: Db,
        system_clock: Arc<dyn SystemClock>,
        rand: Rc<DbRand>,
        action_sampler: Box<dyn DstDistribution>,
        options: DstOptions,
    ) -> Self {
        Self {
            db,
            state: SizedBTreeMap::new(),
            rand,
            action_sampler,
            options,
            system_clock,
        }
    }

    // TODO: should we be using rng_seed (tokio_unstable) for the tokio runtime?
    pub async fn run_simulation(&mut self, iterations: u32) -> Result<(), SlateDBError> {
        let start_time = Instant::now();
        let mut step_count = 0;
        for _ in 0..iterations {
            let step_action = self.action_sampler.sample_action(&self.state);
            info!(
                step_count,
                simulated_time = utils::pretty_duration(&Instant::now().duration_since(start_time)),
                btree_size = utils::pretty_bytes(self.state.size_bytes),
                btree_entries = self.state.len(),
                step_action = %step_action,
                "run_simulation"
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
                // TODO: add DbReader open, close, get, and scan
                // TODO: add DbWriter close and open
                // TODO: add seek
                // TODO: add checkpointing?
                // TODO: add fencing?
            }
            step_count += 1;
            self.maybe_shrink_db().await?;
        }
        Ok(())
    }

    async fn run_write(
        &mut self,
        write_ops: &Vec<DstWriteOp>,
        write_options: &WriteOptions,
    ) -> Result<(), SlateDBError> {
        let mut write_batch = WriteBatch::new();
        for (key, val, options) in write_ops {
            if let Some(val) = val {
                write_batch.put_with_options(key, val, options);
                self.state.insert(key.clone(), val.clone());
            } else {
                write_batch.delete(key);
                self.state.remove(key);
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
        Ok(())
    }

    async fn run_get(
        &mut self,
        key: &Vec<u8>,
        read_options: &ReadOptions,
    ) -> Result<(), SlateDBError> {
        let future = self.db.get_with_options(key, read_options);
        let result = self.poll_await(future, 0f64).await?;
        let expected_val = self.state.get(key);
        let actual_val = result.map(|b| b.to_vec());
        assert_eq!(expected_val, actual_val.as_ref());
        Ok(())
    }

    async fn run_scan(
        &self,
        start_key: &Vec<u8>,
        end_key: &Vec<u8>,
        scan_options: &ScanOptions,
    ) -> Result<(), SlateDBError> {
        if self.state.is_empty() {
            debug!("run_scan (empty)");
            let mut db_iter = self.db.scan(start_key.clone()..end_key.clone()).await?;
            assert!(db_iter.next().await?.is_none());
            return Ok(());
        }
        let future = self
            .db
            .scan_with_options(start_key.clone()..end_key.clone(), scan_options);
        let mut actual_itr = self.poll_await(future, 0f64).await?;
        let expected_itr = self.state.range(start_key..end_key);
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

    async fn run_flush(&self) -> Result<(), SlateDBError> {
        debug!("run_flush");
        self.db.flush().await
    }

    async fn advance_time(&self, duration: Duration) {
        debug!(?duration, "advance_time");
        // TODO: should use system_clock.advance();
        tokio::time::advance(duration).await;
        self.system_clock.clone().advance(duration).await;
    }

    /// Shrinks the database and in-memory state if the in-memory state exceeds
    /// `max_btree_size_bytes`.
    ///
    /// The shrink operation deletes random keys from the database and in-memory
    /// state until the in-memory state is below a threshold. The threshold is a
    /// random size between 80% and 100% of `max_btree_size_bytes`. If the in-memory
    /// state is empty or below `max_btree_size_bytes`, the shrink operation is
    /// always skipped.
    async fn maybe_shrink_db(&mut self) -> Result<(), SlateDBError> {
        let shrink_to_bytes = self.rand.rng().random_range(
            (self.options.max_btree_size_bytes as f64 * 0.8) as usize
                ..self.options.max_btree_size_bytes,
        );
        while self.state.size_bytes > shrink_to_bytes && self.state.len() > 0 {
            let key = self
                .state
                .keys()
                .choose(&mut self.rand.rng())
                .unwrap()
                // Clone so we can mutably borrow below state without
                // holding a reference to the rand.rng here.
                .clone();
            self.poll_await(self.db.delete(&key), 0.01).await?;
            self.state.remove(&key);
        }
        Ok(())
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
        future: impl Future<Output = Result<T, SlateDBError>>,
        flush_probability: f64,
    ) -> Result<T, SlateDBError> {
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
                    let action = self.action_sampler.sample_advance_time();
                    match action {
                        DstAction::AdvanceTime(duration) => self.advance_time(duration).await,
                        _ => unreachable!(),
                    }
                    if self.rand.rng().random_bool(flush_probability) {
                        self.db.flush().await?;
                    }
                }
            }
        }
    }
}

pub struct SizedBTreeMap<K, V>
where
    K: Ord + AsRef<[u8]>,
    V: Ord + AsRef<[u8]>,
{
    inner: BTreeMap<K, V>,
    pub(crate) size_bytes: usize,
}

impl<K, V> SizedBTreeMap<K, V>
where
    K: Ord + AsRef<[u8]>,
    V: Ord + AsRef<[u8]>,
{
    fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    fn insert(&mut self, key: K, val: V) {
        self.size_bytes += key.as_ref().len() + val.as_ref().len();
        if let Some(old_value) = self.inner.insert(key, val) {
            self.size_bytes -= old_value.as_ref().len();
        }
    }

    fn remove(&mut self, key: &K) {
        if let Some(val) = self.inner.remove(key) {
            self.size_bytes -= key.as_ref().len() + val.as_ref().len();
        }
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key)
    }

    fn range(&self, range: impl RangeBounds<K>) -> std::collections::btree_map::Range<'_, K, V> {
        self.inner.range(range)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn keys(&self) -> std::collections::btree_map::Keys<'_, K, V> {
        self.inner.keys()
    }
}
