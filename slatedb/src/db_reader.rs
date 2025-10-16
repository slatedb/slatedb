use crate::bytes_range::BytesRange;
use crate::clock::{
    DefaultLogicalClock, DefaultSystemClock, LogicalClock, MonotonicClock, SystemClock,
};
use crate::config::{CheckpointOptions, DbReaderOptions, ReadOptions, ScanOptions};
use crate::db_read::DbRead;
use crate::db_state::CoreDbState;
use crate::db_stats::DbStats;
use crate::dispatcher::{MessageDispatcher, MessageFactory, MessageHandler};
use crate::error::SlateDBError;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::manifest::Manifest;
use crate::mem_table::{ImmutableMemtable, KVTable};
use crate::oracle::Oracle;
use crate::rand::DbRand;
use crate::reader::{DbStateReader, Reader};
use crate::sst_iter::SstIteratorOptions;
use crate::stats::StatRegistry;
use crate::store_provider::{DefaultStoreProvider, StoreProvider};
use crate::tablestore::TableStore;
use crate::utils::{IdGenerator, MonotonicSeq, WatchableOnceCell};
use crate::wal_replay::{WalReplayIterator, WalReplayOptions};
use crate::{Checkpoint, DbIterator};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use log::info;
use object_store::path::Path;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::ops::{RangeBounds, Sub};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Read-only interface for accessing a database from either
/// the latest persistent state or from an arbitrary checkpoint.
pub struct DbReader {
    inner: Arc<DbReaderInner>,
    manifest_poller_task: Mutex<Option<JoinHandle<Result<(), SlateDBError>>>>,
    cancellation_token: CancellationToken,
}

struct DbReaderInner {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    options: DbReaderOptions,
    state: RwLock<Arc<CheckpointState>>,
    system_clock: Arc<dyn SystemClock>,
    user_checkpoint_id: Option<Uuid>,
    oracle: Arc<Oracle>,
    reader: Reader,
    error_watcher: WatchableOnceCell<SlateDBError>,
    rand: Arc<DbRand>,
}

#[derive(Debug)]
enum DbReaderMessage {
    PollManifest,
}

#[derive(Clone)]
struct CheckpointState {
    checkpoint: Checkpoint,
    manifest: Manifest,
    imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
    last_wal_id: u64,
    last_committed_seq: u64,
}

static EMPTY_TABLE: Lazy<Arc<KVTable>> = Lazy::new(|| Arc::new(KVTable::new()));

impl DbStateReader for CheckpointState {
    fn memtable(&self) -> Arc<KVTable> {
        Arc::clone(&EMPTY_TABLE)
    }

    fn imm_memtable(&self) -> &VecDeque<Arc<ImmutableMemtable>> {
        &self.imm_memtable
    }

    fn core(&self) -> &CoreDbState {
        &self.manifest.core
    }
}

impl DbReaderInner {
    async fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: DbReaderOptions,
        checkpoint_id: Option<Uuid>,
        logical_clock: Arc<dyn LogicalClock>,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
    ) -> Result<Self, SlateDBError> {
        let mut manifest = StoredManifest::load(Arc::clone(&manifest_store)).await?;
        if !manifest.db_state().initialized {
            return Err(SlateDBError::InvalidDBState);
        }

        let checkpoint =
            Self::get_or_create_checkpoint(&mut manifest, checkpoint_id, &options, rand.clone())
                .await?;

        let replay_new_wals = checkpoint_id.is_none();
        let initial_state = Arc::new(
            Self::build_initial_checkpoint_state(
                Arc::clone(&manifest_store),
                Arc::clone(&table_store),
                &options,
                checkpoint,
                replay_new_wals,
            )
            .await?,
        );

        let mono_clock = Arc::new(MonotonicClock::new(
            logical_clock.clone(),
            initial_state.core().last_l0_clock_tick,
        ));

        // initial_state contains the last_committed_seq after WAL replay. in no-wal mode, we can simply fallback
        // to last_l0_seq.
        let last_committed_seq = MonotonicSeq::new(initial_state.core().last_l0_seq);
        last_committed_seq.store_if_greater(initial_state.last_committed_seq);
        let sequence_tracker = initial_state.core().sequence_tracker.clone();
        let oracle = Arc::new(
            Oracle::new(last_committed_seq, system_clock.clone())
                .with_sequence_tracker(sequence_tracker),
        );

        let stat_registry = Arc::new(StatRegistry::new());
        let db_stats = DbStats::new(stat_registry.as_ref());

        let state = RwLock::new(initial_state);
        let reader = Reader {
            table_store: Arc::clone(&table_store),
            db_stats: db_stats.clone(),
            mono_clock: Arc::clone(&mono_clock),
            oracle: oracle.clone(),
            merge_operator: None,
        };

        Ok(Self {
            manifest_store,
            table_store,
            options,
            state,
            system_clock,
            user_checkpoint_id: checkpoint_id,
            oracle,
            reader,
            error_watcher: WatchableOnceCell::new(),
            rand,
        })
    }

    async fn get_or_create_checkpoint(
        manifest: &mut StoredManifest,
        checkpoint_id: Option<Uuid>,
        options: &DbReaderOptions,
        rand: Arc<DbRand>,
    ) -> Result<Checkpoint, SlateDBError> {
        let checkpoint = if let Some(checkpoint_id) = checkpoint_id {
            manifest
                .db_state()
                .find_checkpoint(checkpoint_id)
                .ok_or(SlateDBError::CheckpointMissing(checkpoint_id))?
                .clone()
        } else {
            let options = CheckpointOptions {
                lifetime: Some(options.checkpoint_lifetime),
                ..CheckpointOptions::default()
            };
            let checkpoint_id = rand.rng().gen_uuid();
            manifest.write_checkpoint(checkpoint_id, &options).await?
        };
        Ok(checkpoint)
    }

    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        self.check_error()?;
        let db_state = Arc::clone(&self.state.read());
        self.reader
            .get_with_options(key, options, db_state.as_ref(), None, None)
            .await
    }

    async fn scan_with_options(
        &self,
        range: BytesRange,
        options: &ScanOptions,
    ) -> Result<DbIterator, SlateDBError> {
        self.check_error()?;
        let db_state = Arc::clone(&self.state.read());
        self.reader
            .scan_with_options(range, options, db_state.as_ref(), None, None, None)
            .await
    }

    fn should_reestablish_checkpoint(&self, latest: &CoreDbState) -> bool {
        let read_guard = self.state.read();
        let current_state = read_guard.core();
        latest.replay_after_wal_id > current_state.replay_after_wal_id
            || latest.l0_last_compacted != current_state.l0_last_compacted
            || latest.compacted != current_state.compacted
    }

    async fn replace_checkpoint(
        &self,
        stored_manifest: &mut StoredManifest,
    ) -> Result<Checkpoint, SlateDBError> {
        let current_checkpoint_id = self.state.read().checkpoint.id;
        let options = CheckpointOptions {
            lifetime: Some(self.options.checkpoint_lifetime),
            ..CheckpointOptions::default()
        };
        let new_checkpoint_id = self.rand.rng().gen_uuid();
        stored_manifest
            .replace_checkpoint(current_checkpoint_id, new_checkpoint_id, &options)
            .await
    }

    async fn reestablish_checkpoint(&self, checkpoint: Checkpoint) -> Result<(), SlateDBError> {
        let new_checkpoint_state = self.rebuild_checkpoint_state(checkpoint).await?;
        self.oracle
            .last_committed_seq
            .store_if_greater(new_checkpoint_state.last_committed_seq);
        let mut write_guard = self.state.write();
        *write_guard = Arc::new(new_checkpoint_state);
        Ok(())
    }

    async fn maybe_replay_new_wals(&self) -> Result<(), SlateDBError> {
        let last_seen_wal_id = self.table_store.last_seen_wal_id().await?;
        let last_replayed_wal_id = self.state.read().last_wal_id;
        if last_seen_wal_id > last_replayed_wal_id {
            let current_checkpoint = Arc::clone(&self.state.read());
            let mut imm_memtable = current_checkpoint.imm_memtable().clone();

            let (last_wal_id, last_committed_seq) = Self::replay_wal_into(
                Arc::clone(&self.table_store),
                &self.options,
                current_checkpoint.core(),
                &mut imm_memtable,
                true,
            )
            .await?;

            self.oracle.last_committed_seq.store(last_committed_seq);
            let mut write_guard = self.state.write();
            *write_guard = Arc::new(CheckpointState {
                checkpoint: current_checkpoint.checkpoint.clone(),
                manifest: current_checkpoint.manifest.clone(),
                imm_memtable,
                last_wal_id,
                last_committed_seq,
            });
        }
        Ok(())
    }

    async fn build_initial_checkpoint_state(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: &DbReaderOptions,
        checkpoint: Checkpoint,
        replay_new_wals: bool,
    ) -> Result<CheckpointState, SlateDBError> {
        let manifest = manifest_store.read_manifest(checkpoint.manifest_id).await?;
        let imm_memtable = VecDeque::new();
        Self::build_checkpoint_state(
            checkpoint,
            manifest,
            imm_memtable,
            replay_new_wals,
            Arc::clone(&table_store),
            options,
        )
        .await
    }

    async fn rebuild_checkpoint_state(
        &self,
        new_checkpoint: Checkpoint,
    ) -> Result<CheckpointState, SlateDBError> {
        let prior = self.state.read().clone();
        let manifest = self
            .manifest_store
            .read_manifest(new_checkpoint.manifest_id)
            .await?;

        let imm_memtable = prior
            .imm_memtable
            .iter()
            .filter(|table| table.recent_flushed_wal_id() <= manifest.core.replay_after_wal_id)
            .cloned()
            .collect();

        Self::build_checkpoint_state(
            new_checkpoint,
            manifest,
            imm_memtable,
            true,
            Arc::clone(&self.table_store),
            &self.options,
        )
        .await
    }

    async fn build_checkpoint_state(
        checkpoint: Checkpoint,
        manifest: Manifest,
        mut imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
        replay_new_wals: bool,
        table_store: Arc<TableStore>,
        options: &DbReaderOptions,
    ) -> Result<CheckpointState, SlateDBError> {
        let (last_wal_id, last_committed_seq) = Self::replay_wal_into(
            Arc::clone(&table_store),
            options,
            &manifest.core,
            &mut imm_memtable,
            replay_new_wals,
        )
        .await?;

        Ok(CheckpointState {
            checkpoint,
            manifest,
            imm_memtable,
            last_wal_id,
            last_committed_seq,
        })
    }

    async fn maybe_refresh_checkpoint(
        &self,
        stored_manifest: &mut StoredManifest,
    ) -> Result<(), SlateDBError> {
        let checkpoint = self.state.read().checkpoint.clone();
        let half_lifetime = self
            .options
            .checkpoint_lifetime
            .checked_div(2)
            .expect("Failed to divide checkpoint lifetime");
        let refresh_deadline = checkpoint
            .expire_time
            .expect("Expected checkpoint expiration time to be set")
            .sub(half_lifetime);
        if self.system_clock.now() > refresh_deadline {
            let refreshed_checkpoint = stored_manifest
                .refresh_checkpoint(checkpoint.id, self.options.checkpoint_lifetime)
                .await?;
            info!(
                "refreshed checkpoint [checkpoint_id={}, expire_time={:?}]",
                checkpoint.id, refreshed_checkpoint.expire_time
            )
        }
        Ok(())
    }

    fn spawn_manifest_poller(
        self: &Arc<Self>,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<Result<(), SlateDBError>> {
        let poller = ManifestPoller {
            inner: Arc::clone(self),
        };
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut disptacher = MessageDispatcher::new(
            Box::new(poller),
            rx,
            self.system_clock.clone(),
            cancellation_token,
        );
        tokio::spawn(async move { disptacher.run().await })
    }

    async fn replay_wal_into(
        table_store: Arc<TableStore>,
        reader_options: &DbReaderOptions,
        core: &CoreDbState,
        into_tables: &mut VecDeque<Arc<ImmutableMemtable>>,
        replay_new_wals: bool,
    ) -> Result<(u64, u64), SlateDBError> {
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 1,
            blocks_to_fetch: 256,
            cache_blocks: true,
            eager_spawn: true,
        };

        let replay_options = WalReplayOptions {
            sst_batch_size: 4,
            max_memtable_bytes: reader_options.max_memtable_bytes as usize,
            min_memtable_bytes: usize::MAX,
            sst_iter_options,
        };

        let wal_id_start = if let Some(last_replayed_table) = into_tables.back() {
            last_replayed_table.recent_flushed_wal_id() + 1
        } else {
            core.replay_after_wal_id + 1
        };
        let wal_id_end = if replay_new_wals {
            table_store.last_seen_wal_id().await? + 1
        } else {
            core.next_wal_sst_id
        };

        let mut replay_iter = WalReplayIterator::range(
            wal_id_start..wal_id_end,
            core,
            replay_options,
            Arc::clone(&table_store),
        )
        .await?;

        let mut last_wal_id = 0;
        let mut last_committed_seq = 0;
        while let Some(replayed_table) = replay_iter.next().await? {
            last_wal_id = replayed_table.last_wal_id;
            last_committed_seq = replayed_table.last_seq;
            let imm_memtable =
                ImmutableMemtable::new(replayed_table.table, replayed_table.last_wal_id);
            into_tables.push_back(Arc::new(imm_memtable));
        }

        Ok((last_wal_id, last_committed_seq))
    }

    /// Return an error if the state has encountered
    /// an unrecoverable error.
    pub(crate) fn check_error(&self) -> Result<(), SlateDBError> {
        let error_reader = self.error_watcher.reader();
        if let Some(error) = error_reader.read() {
            return Err(error.clone());
        }
        Ok(())
    }
}

struct ManifestPoller {
    inner: Arc<DbReaderInner>,
}

#[async_trait]
impl MessageHandler<DbReaderMessage> for ManifestPoller {
    fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<DbReaderMessage>>)> {
        vec![(
            self.inner.options.manifest_poll_interval,
            Box::new(|| DbReaderMessage::PollManifest),
        )]
    }

    async fn handle(&mut self, message: DbReaderMessage) -> Result<(), SlateDBError> {
        assert!(matches!(message, DbReaderMessage::PollManifest));
        let mut manifest = StoredManifest::load(Arc::clone(&self.inner.manifest_store)).await?;

        let latest_manifest = manifest.manifest();
        if self
            .inner
            .should_reestablish_checkpoint(&latest_manifest.core)
        {
            let checkpoint = self.inner.replace_checkpoint(&mut manifest).await?;
            self.inner.reestablish_checkpoint(checkpoint).await?;
        } else {
            self.inner.maybe_replay_new_wals().await?;
        }

        self.inner.maybe_refresh_checkpoint(&mut manifest).await
    }

    async fn cleanup(
        &mut self,
        _messages: BoxStream<'async_trait, DbReaderMessage>,
        _result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError> {
        let mut manifest = StoredManifest::load(Arc::clone(&self.inner.manifest_store)).await?;
        let checkpoint_id = self.inner.state.read().checkpoint.id;
        if Some(checkpoint_id) != self.inner.user_checkpoint_id {
            info!(
                "deleting reader established checkpoint for shutdown [checkpoint_id={}]",
                checkpoint_id
            );
            manifest.delete_checkpoint(checkpoint_id).await?;
        }
        Ok(())
    }
}

impl DbReader {
    fn validate_options(options: &DbReaderOptions) -> Result<(), SlateDBError> {
        if options.checkpoint_lifetime.as_millis() < 1000 {
            return Err(SlateDBError::InvalidCheckpointLifetime(
                options.checkpoint_lifetime,
            ));
        }

        let double_poll_interval = options.manifest_poll_interval.checked_mul(2).ok_or(
            SlateDBError::InvalidManifestPollInterval(options.manifest_poll_interval),
        )?;
        if options.checkpoint_lifetime < double_poll_interval {
            return Err(SlateDBError::CheckpointLifetimeTooShort {
                lifetime: options.checkpoint_lifetime,
                interval: double_poll_interval,
            });
        }
        Ok(())
    }

    /// Creates a database reader that can read the contents of a database (but cannot write any
    /// data). The caller can provide an optional checkpoint. If the checkpoint is provided, the
    /// reader will read using the specified checkpoint and will not periodically refresh the
    /// checkpoint. Otherwise, the reader creates a new checkpoint pointing to the current manifest
    /// and refreshes it periodically as specified in the options. It also removes the previous
    /// checkpoint once any ongoing reads have completed.
    pub async fn open<P: Into<Path>>(
        path: P,
        object_store: Arc<dyn ObjectStore>,
        checkpoint_id: Option<Uuid>,
        options: DbReaderOptions,
    ) -> Result<Self, crate::Error> {
        let path = path.into();
        let clock = Arc::new(DefaultSystemClock::default());
        let store_provider = DefaultStoreProvider {
            path,
            object_store,
            block_cache: options.block_cache.clone(),
            system_clock: clock.clone(),
        };

        Self::open_internal(
            &store_provider,
            checkpoint_id,
            options,
            Arc::new(DefaultLogicalClock::default()),
            clock,
            Arc::new(DbRand::default()),
        )
        .await
        .map_err(Into::into)
    }

    async fn open_internal(
        store_provider: &dyn StoreProvider,
        checkpoint_id: Option<Uuid>,
        options: DbReaderOptions,
        logical_clock: Arc<dyn LogicalClock>,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
    ) -> Result<Self, SlateDBError> {
        Self::validate_options(&options)?;

        let cancellation_token = CancellationToken::new();
        let manifest_store = store_provider.manifest_store();
        let table_store = store_provider.table_store();
        let inner = Arc::new(
            DbReaderInner::new(
                manifest_store,
                table_store,
                options,
                checkpoint_id,
                logical_clock,
                system_clock,
                rand,
            )
            .await?,
        );

        // If no checkpoint was provided, then we have established a new checkpoint
        // from the latest state, and we need to refresh it according to the params
        // of `DbReaderOptions`.
        let manifest_poller_task = if checkpoint_id.is_none() {
            Some(inner.spawn_manifest_poller(cancellation_token.clone()))
        } else {
            None
        };

        Ok(Self {
            inner,
            manifest_poller_task: Mutex::new(manifest_poller_task),
            cancellation_token,
        })
    }

    /// Get a value from the database with default read options.
    ///
    /// The `Bytes` object returned contains a slice of an entire
    /// 4 KiB block. The block will be held in memory as long as the
    /// caller holds a reference to the `Bytes` object. Consider
    /// copying the data if you need to hold it for a long time.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, Error>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `Error`: if there was an error getting the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, config::DbReaderOptions, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", Arc::clone(&object_store)).await?;
    ///     db.put(b"key", b"value").await?;
    ///     db.flush().await?;
    ///
    ///     let reader = DbReader::open(
    ///       "test_db",
    ///       Arc::clone(&object_store),
    ///       None,
    ///       DbReaderOptions::default(),
    ///     ).await?;
    ///     assert_eq!(reader.get(b"key").await?, Some("value".into()));
    ///     Ok(())
    /// }
    /// ```
    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, crate::Error> {
        self.get_with_options(key, &ReadOptions::default()).await
    }

    /// Get a value from the database with custom read options.
    ///
    /// The `Bytes` object returned contains a slice of an entire
    /// 4 KiB block. The block will be held in memory as long as the
    /// caller holds a reference to the `Bytes` object. Consider
    /// copying the data if you need to hold it for a long time.
    ///
    /// ## Arguments
    /// - `key`: the key to get
    /// - `options`: the read options to use (Note that [`ReadOptions::read_level`] has no effect
    ///   for readers, which can only observe committed state).
    ///
    /// ## Returns
    /// - `Result<Option<Bytes>, Error>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `Error`: if there was an error getting the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, config::DbReaderOptions, config::ReadOptions, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", Arc::clone(&object_store)).await?;
    ///     db.put(b"key", b"value").await?;
    ///     db.flush().await?;
    ///
    ///     let reader = DbReader::open(
    ///       "test_db",
    ///       Arc::clone(&object_store),
    ///       None,
    ///       DbReaderOptions::default(),
    ///     ).await?;
    ///     assert_eq!(db.get_with_options(b"key", &ReadOptions::default()).await?, Some("value".into()));
    ///     Ok(())
    /// }
    /// ```
    pub async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, crate::Error> {
        self.inner
            .get_with_options(key, options)
            .await
            .map_err(Into::into)
    }

    /// Scan a range of keys using the default scan options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    ///
    /// ## Errors
    /// - `Error`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, config::DbReaderOptions, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", Arc::clone(&object_store)).await?;
    ///     db.put(b"a", b"a_value").await?;
    ///     db.put(b"b", b"b_value").await?;
    ///     db.flush().await?;
    ///
    ///     let reader = DbReader::open(
    ///       "test_db",
    ///       Arc::clone(&object_store),
    ///       None,
    ///       DbReaderOptions::default(),
    ///     ).await?;
    ///     let mut iter = reader.scan("a".."b").await?;
    ///     assert_eq!(Some((b"a", b"a_value").into()), iter.next().await?);
    ///     assert_eq!(None, iter.next().await?);
    ///     Ok(())
    /// }
    /// ```
    pub async fn scan<K, T>(&self, range: T) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, &ScanOptions::default()).await
    }

    /// Scan a range of keys with the provided options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    /// - `options`: the read options to use (Note that [`ReadOptions::read_level`] has no effect
    ///   for readers, which can only observe committed state).
    ///
    /// ## Errors
    /// - `Error`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, config::DbReaderOptions, config::ScanOptions, config::DurabilityLevel, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", Arc::clone(&object_store)).await?;
    ///     db.put(b"a", b"a_value").await?;
    ///     db.put(b"b", b"b_value").await?;
    ///     db.flush().await?;
    ///
    ///     let reader = DbReader::open(
    ///       "test_db",
    ///       Arc::clone(&object_store),
    ///       None,
    ///       DbReaderOptions::default(),
    ///     ).await?;
    ///     let mut iter = reader.scan_with_options("a".."b", &ScanOptions {
    ///         read_ahead_bytes: 1024 * 1024,
    ///         ..ScanOptions::default()
    ///     }).await?;
    ///     assert_eq!(Some((b"a", b"a_value").into()), iter.next().await?);
    ///     assert_eq!(None, iter.next().await?);
    ///     Ok(())
    /// }
    pub async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        let start = range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let end = range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let range = BytesRange::from((start, end));
        self.inner
            .scan_with_options(range, options)
            .await
            .map_err(Into::into)
    }

    /// Close the database reader.
    ///
    /// ## Returns
    /// - `Result<(), Error>`: if there was an error closing the reader
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, config::DbReaderOptions, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store.clone()).await?;
    ///     let options = DbReaderOptions::default();
    ///     let reader = DbReader::open("test_db", object_store.clone(), None, options).await?;
    ///     reader.close().await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    pub async fn close(&self) -> Result<(), crate::Error> {
        self.cancellation_token.cancel();

        if let Some(manifest_poller_task) = {
            let mut maybe_manifest_poller_task = self.manifest_poller_task.lock();
            maybe_manifest_poller_task.take()
        } {
            let result = manifest_poller_task
                .await
                .expect("failed to join manifest poller task");
            info!("manifest poller task exited [result={:?}]", result);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl DbRead for DbReader {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, crate::Error> {
        self.get_with_options(key, options).await
    }

    async fn scan_with_options<K, T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, crate::Error>
    where
        K: AsRef<[u8]> + Send,
        T: RangeBounds<K> + Send,
    {
        self.scan_with_options(range, options).await
    }
}

#[cfg(test)]
mod tests {
    use crate::clock::{DefaultLogicalClock, DefaultSystemClock, LogicalClock, SystemClock};
    use crate::config::{CheckpointOptions, CheckpointScope, Settings};
    use crate::db_reader::{DbReader, DbReaderOptions};
    use crate::db_state::CoreDbState;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::Manifest;
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::proptest_util::rng::new_test_rng;
    use crate::proptest_util::sample;
    use crate::rand::DbRand;
    use crate::sst::SsTableFormat;
    use crate::store_provider::StoreProvider;
    use crate::tablestore::TableStore;
    use crate::{error::SlateDBError, test_utils, Db};
    use bytes::Bytes;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::collections::BTreeMap;
    use std::ops::RangeFull;
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    #[tokio::test]
    async fn should_get_value_from_db() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let db = test_provider.new_db(Settings::default()).await.unwrap();
        let key = b"test_key";
        let value = b"test_value";

        db.put(key, value).await.unwrap();
        db.flush().await.unwrap();

        let reader = DbReader::open(
            path.clone(),
            Arc::clone(&object_store),
            None,
            DbReaderOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            reader.get(key).await.unwrap(),
            Some(Bytes::from_static(value))
        );
    }

    #[tokio::test]
    async fn should_get_latest_value_from_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let db = test_provider.new_db(Settings::default()).await.unwrap();
        let key = b"test_key";
        let value1 = b"test_value";
        let value2 = b"updated_value";

        db.put(key, value1).await.unwrap();
        db.flush().await.unwrap();
        db.put(key, value2).await.unwrap();
        let checkpoint_result = db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();

        let reader = DbReader::open_internal(
            &test_provider,
            Some(checkpoint_result.id),
            DbReaderOptions::default(),
            test_provider.logical_clock.clone(),
            test_provider.system_clock.clone(),
            test_provider.rand.clone(),
        )
        .await
        .unwrap();

        assert_eq!(
            reader.get(key).await.unwrap(),
            Some(Bytes::from_static(value2))
        );
    }

    #[tokio::test]
    async fn should_get_from_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let db = test_provider.new_db(Settings::default()).await.unwrap();
        let key = b"test_key";
        let checkpoint_value = b"test_value";
        let updated_value = b"updated_value";

        db.put(key, checkpoint_value).await.unwrap();
        let checkpoint_result = db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        db.put(key, updated_value).await.unwrap();

        let reader = DbReader::open(
            path.clone(),
            Arc::clone(&object_store),
            Some(checkpoint_result.id),
            DbReaderOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            reader.get(key).await.unwrap(),
            Some(Bytes::from_static(checkpoint_value))
        );
    }

    #[tokio::test]
    async fn should_fail_if_db_is_uninitialized() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path, Arc::clone(&object_store));
        let manifest_store = test_provider.manifest_store();

        let parent_manifest = Manifest::initial(CoreDbState::new());
        let parent_path = "/tmp/parent_store".to_string();
        let source_checkpoint_id = uuid::Uuid::new_v4();

        let _ = StoredManifest::create_uninitialized_clone(
            Arc::clone(&manifest_store),
            &parent_manifest,
            parent_path,
            source_checkpoint_id,
            Arc::new(DbRand::default()),
        )
        .await
        .unwrap();

        let err = test_provider
            .new_db_reader(DbReaderOptions::default(), None)
            .await;
        assert!(matches!(err, Err(SlateDBError::InvalidDBState)));
    }

    #[tokio::test]
    async fn should_scan_from_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let db = test_provider.new_db(Settings::default()).await.unwrap();
        let checkpoint_key = b"checkpoint_key";
        let value = b"value";

        db.put(checkpoint_key, value).await.unwrap();
        let checkpoint_result = db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();

        let post_checkpoint_key = b"post_checkpoint_key";
        db.put(post_checkpoint_key, value).await.unwrap();

        let reader = test_provider
            .new_db_reader(DbReaderOptions::default(), Some(checkpoint_result.id))
            .await
            .unwrap();

        let mut db_iter = reader.scan::<Vec<u8>, RangeFull>(..).await.unwrap();
        let mut table = BTreeMap::new();
        table.insert(
            Bytes::copy_from_slice(checkpoint_key),
            Bytes::copy_from_slice(value),
        );

        test_utils::assert_ranged_db_scan(&table, .., &mut db_iter).await;
    }

    #[tokio::test(start_paused = true)]
    async fn should_reestablish_reader_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let db_options = Settings {
            l0_sst_size_bytes: 256,
            ..Settings::default()
        };
        let db = test_provider.new_db(db_options).await.unwrap();
        let reader_options = DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(10),
            ..DbReaderOptions::default()
        };
        let reader = test_provider
            .new_db_reader(reader_options, None)
            .await
            .unwrap();
        let manifest_store = test_provider.manifest_store();
        let manifest = manifest_store.read_latest_manifest().await.unwrap().1;
        let initial_checkpoint_id = manifest.core.checkpoints.first().unwrap().id;

        let mut rng = new_test_rng(None);
        let table = sample::table(&mut rng, 256, 10);
        for (key, value) in &table {
            db.put(key, value).await.unwrap();
        }
        db.flush().await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut db_iter = reader.scan::<Vec<u8>, _>(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&table, .., &mut db_iter).await;

        let manifest = manifest_store.read_latest_manifest().await.unwrap().1;
        assert!(!manifest.core.checkpoints.is_empty());
        assert_eq!(None, manifest.core.find_checkpoint(initial_checkpoint_id));
    }

    #[tokio::test(start_paused = true)]
    async fn should_refresh_reader_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let _db = test_provider.new_db(Settings::default()).await;
        let reader_options = DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(500),
            checkpoint_lifetime: Duration::from_millis(1000),
            ..DbReaderOptions::default()
        };

        let manifest_store = test_provider.manifest_store();
        let reader = test_provider
            .new_db_reader(reader_options, None)
            .await
            .unwrap();

        let initial_manifest = manifest_store.read_latest_manifest().await.unwrap().1;
        assert_eq!(1, initial_manifest.core.checkpoints.len());
        let initial_reader_checkpoint = initial_manifest.core.checkpoints.first().unwrap().clone();

        tokio::time::sleep(Duration::from_millis(5000)).await;

        let updated_manifest = manifest_store.read_latest_manifest().await.unwrap().1;
        assert_eq!(1, updated_manifest.core.checkpoints.len());
        let updated_reader_checkpoint = updated_manifest.core.checkpoints.first().unwrap().clone();
        assert_eq!(initial_reader_checkpoint.id, updated_reader_checkpoint.id);
        assert!(
            updated_reader_checkpoint.expire_time.unwrap()
                > initial_reader_checkpoint.expire_time.unwrap()
        );

        // The checkpoint is removed on shutdown
        reader.close().await.unwrap();
        let updated_manifest = manifest_store.read_latest_manifest().await.unwrap().1;
        assert_eq!(0, updated_manifest.core.checkpoints.len());
    }

    #[tokio::test(start_paused = true)]
    async fn should_replay_new_wals() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));
        let db = test_provider.new_db(Settings::default()).await.unwrap();

        let reader_options = DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(500),
            checkpoint_lifetime: Duration::from_millis(1000),
            ..DbReaderOptions::default()
        };

        let reader = test_provider
            .new_db_reader(reader_options, None)
            .await
            .unwrap();
        let key = b"test_key";
        let value = b"test_value";
        db.put(key, value).await.unwrap();
        db.flush().await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            reader.get(key).await.unwrap(),
            Some(Bytes::from_static(value))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn should_fail_new_reads_if_manifest_poller_crashes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));
        let _db = test_provider.new_db(Settings::default()).await.unwrap();

        let reader_options = DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(500),
            ..DbReaderOptions::default()
        };
        let reader = test_provider
            .new_db_reader(reader_options, None)
            .await
            .unwrap();

        fail_parallel::cfg(
            Arc::clone(&test_provider.fp_registry),
            "list-wal-ssts",
            "return",
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let result = reader.get(b"key").await.unwrap_err();
        dbg!(&result);
        assert_eq!(result.to_string(), "Unavailable error: io error (oops)");
    }

    struct TestProvider {
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        fp_registry: Arc<FailPointRegistry>,
        logical_clock: Arc<dyn LogicalClock>,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
    }

    impl TestProvider {
        fn new(path: Path, object_store: Arc<dyn ObjectStore>) -> Self {
            let logical_clock = Arc::new(DefaultLogicalClock::new());
            let system_clock = Arc::new(DefaultSystemClock::new());
            let rand = Arc::new(DbRand::default());
            TestProvider {
                object_store,
                path,
                fp_registry: Arc::new(FailPointRegistry::new()),
                logical_clock,
                system_clock,
                rand,
            }
        }
    }

    impl TestProvider {
        async fn new_db(&self, options: Settings) -> Result<Db, crate::Error> {
            Db::builder(self.path.clone(), self.object_store.clone())
                .with_settings(options)
                .build()
                .await
        }

        async fn new_db_reader(
            &self,
            options: DbReaderOptions,
            checkpoint: Option<Uuid>,
        ) -> Result<DbReader, SlateDBError> {
            DbReader::open_internal(
                self,
                checkpoint,
                options,
                self.logical_clock.clone() as Arc<dyn LogicalClock>,
                self.system_clock.clone() as Arc<dyn SystemClock>,
                self.rand.clone(),
            )
            .await
        }
    }

    impl StoreProvider for TestProvider {
        fn table_store(&self) -> Arc<TableStore> {
            Arc::new(TableStore::new_with_fp_registry(
                ObjectStores::new(Arc::clone(&self.object_store), None),
                SsTableFormat::default(),
                PathResolver::new(self.path.clone()),
                Arc::clone(&self.fp_registry),
                None,
            ))
        }

        fn manifest_store(&self) -> Arc<ManifestStore> {
            Arc::new(ManifestStore::new(
                &self.path,
                Arc::clone(&self.object_store),
                self.system_clock.clone(),
            ))
        }
    }
}
