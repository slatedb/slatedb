use crate::bytes_range::BytesRange;
use crate::cached_object_store::CachedObjectStore;
use crate::clock::MonotonicClock;
use crate::config::{CheckpointOptions, DbReaderOptions, ReadOptions, ScanOptions};
use crate::db_read::DbRead;
use crate::db_state::ManifestCore;
use crate::db_stats::DbStats;
use crate::db_status::{ClosedResultWriter, DbStatus, DbStatusManager};
use crate::dispatcher::{MessageFactory, MessageHandler, MessageHandlerExecutor};
use crate::error::SlateDBError;
use crate::iter::IterationOrder;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::manifest::Manifest;
use crate::mem_table::{ImmutableMemtable, KVTable};
use crate::merge_operator::MergeOperatorType;
use crate::oracle::DbReaderOracle;
use crate::paths::PathResolver;
use crate::rand::DbRand;
use crate::reader::{DbStateReader, Reader};
use crate::sst_iter::SstIteratorOptions;
use crate::store_provider::StoreProvider;
use crate::tablestore::TableStore;
use crate::types::KeyValue;
use crate::utils::IdGenerator;
use crate::wal_replay::{WalReplayIterator, WalReplayOptions};
use crate::{Checkpoint, DbIterator};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use log::info;
use object_store::path::Path;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use slatedb_common::clock::SystemClock;
use std::collections::VecDeque;
use std::ops::{RangeBounds, Sub};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use uuid::Uuid;

pub(crate) const DB_READER_TASK_NAME: &str = "manifest_poller";

/// Read-only interface for accessing a database from either
/// the latest persistent state or from an arbitrary checkpoint.
pub struct DbReader {
    inner: Arc<DbReaderInner>,
    task_executor: MessageHandlerExecutor,
}

struct DbReaderInner {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    options: DbReaderOptions,
    state: RwLock<Arc<CheckpointState>>,
    system_clock: Arc<dyn SystemClock>,
    user_checkpoint_id: Option<Uuid>,
    oracle: Arc<DbReaderOracle>,
    reader: Reader,
    status_manager: DbStatusManager,
    rand: Arc<DbRand>,
    /// Kept alive so the underlying `MetricsRecorder` is not dropped while
    /// metric handles in `DbStats` (and other stats structs) are still in use.
    /// See: https://github.com/slatedb/slatedb/issues/1469
    #[allow(dead_code)]
    recorder: slatedb_common::metrics::MetricsRecorderHelper,
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
    last_remote_persisted_seq: u64,
}

static EMPTY_TABLE: Lazy<Arc<KVTable>> = Lazy::new(|| Arc::new(KVTable::new()));

impl DbStateReader for CheckpointState {
    fn memtable(&self) -> Arc<KVTable> {
        Arc::clone(&EMPTY_TABLE)
    }

    fn imm_memtable(&self) -> &VecDeque<Arc<ImmutableMemtable>> {
        &self.imm_memtable
    }

    fn core(&self) -> &ManifestCore {
        &self.manifest.core
    }
}

impl DbReaderInner {
    async fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: DbReaderOptions,
        checkpoint_id: Option<Uuid>,
        merge_operator: Option<MergeOperatorType>,
        status_manager: DbStatusManager,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
        recorder: slatedb_common::metrics::MetricsRecorderHelper,
    ) -> Result<Self, SlateDBError> {
        let mut manifest =
            StoredManifest::load(Arc::clone(&manifest_store), system_clock.clone()).await?;
        if !manifest.db_state().initialized {
            return Err(SlateDBError::InvalidDBState);
        }

        let checkpoint =
            Self::get_or_create_checkpoint(&mut manifest, checkpoint_id, &options, rand.clone())
                .await?;

        let replay_new_wals = checkpoint_id.is_none() && !options.skip_wal_replay;
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
            system_clock.clone(),
            initial_state.core().last_l0_clock_tick,
        ));

        // initial_state contains the last_committed_seq after WAL replay. in no-wal mode, we can simply fallback
        // to last_l0_seq.
        let initial_durable_seq = initial_state
            .last_remote_persisted_seq
            .max(initial_state.core().last_l0_seq);
        status_manager.report_durable_seq(initial_durable_seq);
        let oracle = Arc::new(DbReaderOracle::new(
            initial_durable_seq,
            status_manager.clone(),
        ));

        let db_stats = DbStats::new(&recorder);

        let state = RwLock::new(initial_state);
        let reader = Reader {
            table_store: Arc::clone(&table_store),
            db_stats,
            mono_clock: Arc::clone(&mono_clock),
            oracle: oracle.clone(),
            merge_operator,
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
            status_manager,
            rand,
            recorder,
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
        self.get_key_value_with_options(key, options)
            .await
            .map(|kv_opt| kv_opt.map(|kv| kv.value))
    }

    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<KeyValue>, SlateDBError> {
        self.check_closed()?;
        let db_state = Arc::clone(&self.state.read());
        self.reader
            .get_key_value_with_options(key, options, db_state.as_ref(), None, None)
            .await
    }

    async fn scan_with_options(
        &self,
        range: BytesRange,
        options: &ScanOptions,
    ) -> Result<DbIterator, SlateDBError> {
        self.check_closed()?;
        let db_state = Arc::clone(&self.state.read());
        self.reader
            .scan_with_options(range, options, db_state.as_ref(), None, None, None)
            .await
    }

    fn should_reestablish_checkpoint(&self, latest: &ManifestCore) -> bool {
        let read_guard = self.state.read();
        let current_state = read_guard.core();
        latest.last_compacted_l0_sst_view_id != current_state.last_compacted_l0_sst_view_id
            || latest.compacted != current_state.compacted
            || latest.last_l0_seq > read_guard.last_remote_persisted_seq
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
            .advance_durable_seq(new_checkpoint_state.last_remote_persisted_seq);
        let mut write_guard = self.state.write();
        *write_guard = Arc::new(new_checkpoint_state);
        Ok(())
    }

    async fn maybe_replay_new_wals(&self) -> Result<(), SlateDBError> {
        if self.options.skip_wal_replay {
            return Ok(());
        }
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

            self.oracle.advance_durable_seq(last_committed_seq);
            let mut write_guard = self.state.write();
            *write_guard = Arc::new(CheckpointState {
                checkpoint: current_checkpoint.checkpoint.clone(),
                manifest: current_checkpoint.manifest.clone(),
                imm_memtable,
                last_wal_id,
                last_remote_persisted_seq: last_committed_seq,
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
        let mut imm_memtable = VecDeque::new();

        for table in prior.imm_memtable.iter() {
            let table_meta = table.table().metadata();
            if table_meta.last_seq <= manifest.core.last_l0_seq {
                // Skip since the entire table is older than L0+.
                continue;
            } else if table_meta.first_seq > manifest.core.last_l0_seq {
                // Keep the entire table since all rows are newer than L0+.
                imm_memtable.push_back(Arc::clone(table));
            } else {
                // The table has some rows that are newer than L0+ and some that are older. This
                // happens when the table spans multiple WAL files. Some of those WAL files can
                // have sequence numbers < manifest.core.last_l0_seq, while others have sequence
                // numbers > manifest.core.last_l0_seq. Retain only those that are more recent
                // than the manifest's last L0 sequence number.
                let filtered_table = table.filter_after_seq(manifest.core.last_l0_seq);
                // Push to the back because we are iterating prior from newest to oldest, and we
                // want the imm memtables in checkpoint state to be ordered the same way.
                imm_memtable.push_back(Arc::new(filtered_table));
            }
        }

        Self::build_checkpoint_state(
            new_checkpoint,
            manifest,
            imm_memtable,
            !self.options.skip_wal_replay,
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
            last_remote_persisted_seq: last_committed_seq,
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
        task_executor: &MessageHandlerExecutor,
    ) -> Result<(), SlateDBError> {
        let poller = ManifestPoller {
            inner: Arc::clone(self),
        };
        let (_tx, rx) = async_channel::unbounded();
        let result = task_executor.add_handler(
            DB_READER_TASK_NAME.to_string(),
            Box::new(poller),
            rx,
            &Handle::current(),
        );
        task_executor.monitor_on(&Handle::current())?;
        result
    }

    async fn replay_wal_into(
        table_store: Arc<TableStore>,
        reader_options: &DbReaderOptions,
        core: &ManifestCore,
        into_tables: &mut VecDeque<Arc<ImmutableMemtable>>,
        replay_new_wals: bool,
    ) -> Result<(u64, u64), SlateDBError> {
        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 1,
            blocks_to_fetch: 256,
            cache_blocks: true,
            eager_spawn: true,
            order: IterationOrder::Ascending,
        };

        let (mut replay_after_wal_id, mut last_committed_seq) =
            if let Some(latest_replayed_table) = into_tables.front() {
                (
                    latest_replayed_table.recent_flushed_wal_id(),
                    latest_replayed_table.table().last_seq().unwrap_or(0),
                )
            } else {
                (core.replay_after_wal_id, core.last_l0_seq)
            };
        let wal_id_end = if replay_new_wals {
            table_store.last_seen_wal_id().await? + 1
        } else {
            core.next_wal_sst_id
        };

        let replay_options = WalReplayOptions {
            sst_batch_size: 4,
            max_memtable_bytes: reader_options.max_memtable_bytes as usize,
            min_memtable_bytes: usize::MAX,
            sst_iter_options,
            // Skip entries that we already have in `imm_memtable` (that might be above last_l0_seq).
            min_seq: Some(last_committed_seq),
        };

        let mut replay_iter = WalReplayIterator::range(
            (replay_after_wal_id + 1)..wal_id_end,
            core,
            replay_options,
            Arc::clone(&table_store),
        )
        .await?;

        while let Some(replayed_table) = match replay_iter.next().await {
            Ok(Some(replayed_table)) => Some(replayed_table),
            Ok(None) => None,
            Err(err) if has_not_found_object_store_error(&err) => None,
            Err(err) => return Err(err),
        } {
            assert!(replayed_table.last_wal_id > replay_after_wal_id);
            replay_after_wal_id = replayed_table.last_wal_id;
            if !replayed_table.table.is_empty() && replayed_table.last_seq > last_committed_seq {
                let first_seq = replayed_table
                    .table
                    .table()
                    .first_seq()
                    .expect("expected first_seq on non-empty table");
                // The entire table should be newer than the last committed seq, since we filtered
                // out entries <= last_committed_seq when creating the replay iterator.
                assert!(first_seq > last_committed_seq);
                last_committed_seq = replayed_table.last_seq;
                let imm_memtable =
                    ImmutableMemtable::new(replayed_table.table, replayed_table.last_wal_id);
                into_tables.push_front(Arc::new(imm_memtable));
            }
        }

        Ok((replay_after_wal_id, last_committed_seq))
    }

    /// Returns an error if the reader has been closed.
    ///
    /// ## Returns
    /// - `Ok(())` if the reader is still open.
    /// - `Err(SlateDBError::Closed)` if the reader was closed successfully
    ///   (state.result_reader() returns Ok(())).
    /// - `Err(e)` if the reader was closed with an error, where `e` is the error
    ///   (state.result_reader() returns Err(e)).
    pub(crate) fn check_closed(&self) -> Result<(), SlateDBError> {
        let closed_result_reader = self.status_manager.result_reader();
        if let Some(result) = closed_result_reader.read() {
            return match result {
                Ok(()) => Err(SlateDBError::Closed),
                Err(e) => Err(e),
            };
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
        let mut manifest = StoredManifest::load(
            Arc::clone(&self.inner.manifest_store),
            self.inner.system_clock.clone(),
        )
        .await?;

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
        let mut manifest = StoredManifest::load(
            Arc::clone(&self.inner.manifest_store),
            self.inner.system_clock.clone(),
        )
        .await?;
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

    /// Preload the disk cache from the current manifest state.
    pub(crate) async fn preload_cache(
        &self,
        cached_obj_store: &CachedObjectStore,
        path: object_store::path::Path,
    ) -> Result<(), SlateDBError> {
        let state = Arc::clone(&self.inner.state.read());
        let external_ssts = state.manifest.external_ssts();
        let path_resolver = PathResolver::new_with_external_ssts(path, external_ssts);
        let cache_opts = &self.inner.options.object_store_cache_options;
        crate::utils::preload_cache_from_manifest(
            &state.manifest.core,
            cached_obj_store,
            &path_resolver,
            cache_opts.preload_disk_cache_on_startup,
            cache_opts.max_cache_size_bytes.unwrap_or(usize::MAX),
        )
        .await
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
        // Use the builder API internally
        let mut builder = Self::builder(path, object_store).with_options(options);
        if let Some(id) = checkpoint_id {
            builder = builder.with_checkpoint_id(id);
        }
        builder.build().await
    }

    /// Creates a new builder for a database reader at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the database.
    /// * `object_store` - The object store to use.
    ///
    /// # Returns
    ///
    /// A `DbReaderBuilder` that can be used to configure and build a `DbReader`.
    ///
    /// # Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, Error};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Error> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     // First create a database
    ///     let db = Db::open("test_db", Arc::clone(&object_store)).await?;
    ///     db.close().await?;
    ///     // Then open a reader
    ///     let reader = DbReader::builder("test_db", object_store)
    ///         .build()
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn builder<P: Into<Path>>(
        path: P,
        object_store: Arc<dyn ObjectStore>,
    ) -> crate::db::builder::DbReaderBuilder<P> {
        crate::db::builder::DbReaderBuilder::new(path, object_store)
    }

    pub(crate) async fn open_internal(
        store_provider: &dyn StoreProvider,
        checkpoint_id: Option<Uuid>,
        merge_operator: Option<MergeOperatorType>,
        options: DbReaderOptions,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
        recorder: slatedb_common::metrics::MetricsRecorderHelper,
    ) -> Result<Self, SlateDBError> {
        Self::validate_options(&options)?;

        let status_manager = DbStatusManager::new(0);
        let task_executor =
            MessageHandlerExecutor::new(Arc::new(status_manager.clone()), system_clock.clone());
        let manifest_store = store_provider.manifest_store();
        let table_store = store_provider.table_store();
        let inner = Arc::new(
            DbReaderInner::new(
                manifest_store,
                table_store,
                options,
                checkpoint_id,
                merge_operator,
                status_manager,
                system_clock,
                rand,
                recorder,
            )
            .await?,
        );

        // If no checkpoint was provided, then we have established a new checkpoint
        // from the latest state, and we need to refresh it according to the params
        // of `DbReaderOptions`.
        if checkpoint_id.is_none() {
            inner.spawn_manifest_poller(&task_executor)?;
        }

        Ok(Self {
            inner,
            task_executor,
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

    /// Get a key-value pair from the reader with default read options.
    pub async fn get_key_value<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<KeyValue>, crate::Error> {
        self.get_key_value_with_options(key, &ReadOptions::default())
            .await
    }

    /// Get a key-value pair from the reader with custom read options.
    pub async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<KeyValue>, crate::Error> {
        let kv = self
            .inner
            .get_key_value_with_options(key, options)
            .await
            .map_err(crate::Error::from)?;
        Ok(kv)
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
    ///     let kv = iter.next().await?.unwrap();
    ///     assert_eq!(kv.key.as_ref(), b"a");
    ///     assert_eq!(kv.value.as_ref(), b"a_value");
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
    ///     let kv = iter.next().await?.unwrap();
    ///     assert_eq!(kv.key.as_ref(), b"a");
    ///     assert_eq!(kv.value.as_ref(), b"a_value");
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

    /// Scan all keys that share the provided prefix using the default scan options.
    ///
    /// ## Arguments
    /// - `prefix`: the key prefix to scan
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    pub async fn scan_prefix<P>(&self, prefix: P) -> Result<DbIterator, crate::Error>
    where
        P: AsRef<[u8]> + Send,
    {
        self.scan_prefix_with_options(prefix, &ScanOptions::default())
            .await
    }

    /// Scan all keys that share the provided prefix with custom options.
    ///
    /// ## Arguments
    /// - `prefix`: the key prefix to scan
    /// - `options`: the scan options to use
    ///
    /// ## Returns
    /// - `Result<DbIterator, Error>`: An iterator with the results of the scan
    pub async fn scan_prefix_with_options<P>(
        &self,
        prefix: P,
        options: &ScanOptions,
    ) -> Result<DbIterator, crate::Error>
    where
        P: AsRef<[u8]> + Send,
    {
        self.scan_with_options(BytesRange::from_prefix(prefix.as_ref()), options)
            .await
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
        self.task_executor
            .shutdown_task(DB_READER_TASK_NAME)
            .await
            .map_err(Into::into)
    }

    /// Subscribe to database status changes.
    ///
    /// See [`Db::subscribe`](crate::Db::subscribe) for full semantics and
    /// deadlock warnings. The `durable_seq` field is updated whenever the
    /// manifest poller discovers new data written by a remote writer.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<DbStatus> {
        self.inner.status_manager.subscribe()
    }

    /// Check the reader status.
    ///
    /// See [`Db::status`](crate::Db::status) for full semantics.
    pub fn status(&self) -> Result<(), crate::Error> {
        self.inner.check_closed().map_err(Into::into)
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

    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<KeyValue>, crate::Error> {
        self.get_key_value_with_options(key, options).await
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

/// Checks if the error or any of its sources is an `object_store::Error::NotFound` error.
fn has_not_found_object_store_error(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(current_err) = current {
        if current_err
            .downcast_ref::<object_store::Error>()
            .is_some_and(|err| matches!(err, object_store::Error::NotFound { .. }))
            || current_err
                .downcast_ref::<Arc<object_store::Error>>()
                .is_some_and(|err| matches!(err.as_ref(), object_store::Error::NotFound { .. }))
        {
            return true;
        }
        current = current_err.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::CheckpointState;
    use crate::clock::MonotonicClock;
    use crate::config::{
        CheckpointOptions, CheckpointScope, FlushOptions, FlushType, MergeOptions, Settings,
        WriteOptions,
    };
    use crate::db_reader::{DbReader, DbReaderInner, DbReaderOptions};
    use crate::db_state::{ManifestCore, SsTableId};
    use crate::db_stats::DbStats;
    use crate::db_status::DbStatusManager;
    use crate::format::sst::SsTableFormat;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::Manifest;
    use crate::mem_table::{ImmutableMemtable, WritableKVTable};
    use crate::merge_operator::MergeOperatorType;
    use crate::object_stores::ObjectStores;
    use crate::oracle::DbReaderOracle;
    use crate::paths::PathResolver;
    use crate::proptest_util::rng::new_test_rng;
    use crate::proptest_util::sample;
    use crate::rand::DbRand;
    use crate::reader::Reader;
    use crate::store_provider::StoreProvider;
    use crate::tablestore::TableStore;
    use crate::types::RowEntry;
    use crate::{error::SlateDBError, test_utils, Db};
    use bytes::Bytes;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use rstest::rstest;
    use slatedb_common::clock::{DefaultSystemClock, SystemClock};
    use slatedb_common::MockSystemClock;
    use std::collections::{BTreeMap, VecDeque};
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
            None,
            DbReaderOptions::default(),
            test_provider.system_clock.clone(),
            test_provider.rand.clone(),
            slatedb_common::metrics::MetricsRecorderHelper::noop(),
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

        let parent_manifest = Manifest::initial(ManifestCore::new());
        let parent_path = "/tmp/parent_store".to_string();
        let source_checkpoint_id = uuid::Uuid::new_v4();

        let _ = StoredManifest::create_uninitialized_clone(
            Arc::clone(&manifest_store),
            &parent_manifest,
            parent_path,
            source_checkpoint_id,
            Arc::new(DbRand::default()),
            Arc::new(DefaultSystemClock::new()),
        )
        .await
        .unwrap();

        let err = test_provider
            .new_db_reader(DbReaderOptions::default(), None, None)
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
            .new_db_reader(DbReaderOptions::default(), Some(checkpoint_result.id), None)
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
            .new_db_reader(reader_options, None, None)
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
            .new_db_reader(reader_options, None, None)
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
            .new_db_reader(reader_options, None, None)
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

    #[tokio::test]
    async fn replay_wal_into_should_use_latest_existing_table_and_keep_newest_first_order() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_db_reader_replay_order");
        let test_provider = TestProvider::new(path, Arc::clone(&object_store));
        let table_store = test_provider.table_store();

        write_wal_sst(
            Arc::clone(&table_store),
            3,
            vec![RowEntry::new_value(b"stale_key", b"stale_value", 3)],
        )
        .await
        .unwrap();
        write_wal_sst(
            Arc::clone(&table_store),
            4,
            vec![RowEntry::new_value(b"fresh_key", b"fresh_value", 4)],
        )
        .await
        .unwrap();

        let mut into_tables = VecDeque::new();
        into_tables.push_front(immutable_memtable(
            3,
            vec![RowEntry::new_value(b"stale_key", b"stale_value", 3)],
        ));
        into_tables.push_back(immutable_memtable(
            2,
            vec![RowEntry::new_value(b"older_key", b"older_value", 2)],
        ));

        let mut core = ManifestCore::new();
        core.next_wal_sst_id = 5;

        let (last_wal_id, last_committed_seq) = DbReaderInner::replay_wal_into(
            Arc::clone(&table_store),
            &DbReaderOptions::default(),
            &core,
            &mut into_tables,
            false,
        )
        .await
        .unwrap();

        assert_eq!(last_wal_id, 4);
        assert_eq!(last_committed_seq, 4);

        let newest_replayed = into_tables.front().unwrap();
        assert_eq!(newest_replayed.recent_flushed_wal_id(), 4);

        let newest_table = newest_replayed.table();
        let mut newest_iter = newest_table.iter();
        test_utils::assert_iterator(
            &mut newest_iter,
            vec![RowEntry::new_value(b"fresh_key", b"fresh_value", 4)],
        )
        .await;
    }

    #[test]
    fn has_not_found_object_store_error_should_walk_nested_error_sources() {
        let err = crate::Error::from(SlateDBError::from(object_store::Error::NotFound {
            path: "missing-wal".to_string(),
            source: Box::new(std::io::Error::other("missing")),
        }));

        assert!(super::has_not_found_object_store_error(&err));
    }

    #[test]
    fn has_not_found_object_store_error_should_ignore_non_not_found_errors() {
        let err = SlateDBError::from(object_store::Error::NotImplemented);

        assert!(!super::has_not_found_object_store_error(&err));
    }

    #[tokio::test]
    async fn replay_wal_into_should_treat_missing_wal_sst_as_end_of_iteration() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_db_reader_missing_wal");
        let test_provider = TestProvider::new(path, Arc::clone(&object_store));
        let table_store = test_provider.table_store();

        write_wal_sst(
            Arc::clone(&table_store),
            1,
            vec![RowEntry::new_value(b"key", b"value", 1)],
        )
        .await
        .unwrap();

        let mut into_tables = VecDeque::new();
        let mut core = ManifestCore::new();
        core.next_wal_sst_id = 3;

        let (last_wal_id, last_committed_seq) = DbReaderInner::replay_wal_into(
            Arc::clone(&table_store),
            &DbReaderOptions::default(),
            &core,
            &mut into_tables,
            false,
        )
        .await
        .unwrap();

        assert_eq!(last_wal_id, 0);
        assert_eq!(last_committed_seq, 0);
        assert!(into_tables.is_empty());
    }

    #[tokio::test]
    async fn replay_wal_into_should_keep_previously_replayed_tables_before_missing_wal_sst() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_db_reader_missing_wal_after_replay");
        let test_provider = TestProvider::new(path, Arc::clone(&object_store));
        let table_store = test_provider.table_store();

        let wal_1_row = RowEntry::new_value(b"a", &[b'a'; 8], 1);
        let wal_2_row_1 = RowEntry::new_value(b"b", &[b'b'; 40], 2);
        let wal_2_row_2 = RowEntry::new_value(b"c", &[b'c'; 40], 3);

        let max_memtable_bytes = table_store.estimate_encoded_size_compacted(
            2,
            wal_1_row.estimated_size() + wal_2_row_1.estimated_size(),
        ) as u64;

        write_wal_sst(Arc::clone(&table_store), 1, vec![wal_1_row.clone()])
            .await
            .unwrap();
        write_wal_sst(
            Arc::clone(&table_store),
            2,
            vec![wal_2_row_1.clone(), wal_2_row_2],
        )
        .await
        .unwrap();

        let mut into_tables = VecDeque::new();
        let mut core = ManifestCore::new();
        // Force the reader to attempt to read up to 4 even though 3 and 4 don't exist.
        core.next_wal_sst_id = 4;
        let reader_options = DbReaderOptions {
            max_memtable_bytes,
            ..DbReaderOptions::default()
        };

        let (last_wal_id, last_committed_seq) = DbReaderInner::replay_wal_into(
            Arc::clone(&table_store),
            &reader_options,
            &core,
            &mut into_tables,
            false,
        )
        .await
        .unwrap();

        assert_eq!(last_wal_id, 2);
        assert_eq!(last_committed_seq, 2);
        assert_eq!(into_tables.len(), 1);

        let replayed = into_tables.front().unwrap();
        assert_eq!(replayed.recent_flushed_wal_id(), 2);

        let mut replayed_iter = replayed.table().iter();
        test_utils::assert_iterator(&mut replayed_iter, vec![wal_1_row, wal_2_row_1]).await;
    }

    #[tokio::test]
    async fn replay_wal_into_should_noop_for_fresh_db_with_no_writes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_db_reader_fresh_db_no_writes");
        let test_provider = TestProvider::new(path, Arc::clone(&object_store));
        let table_store = test_provider.table_store();

        let mut into_tables = VecDeque::new();
        let core = ManifestCore::new();

        let (last_wal_id, last_committed_seq) = DbReaderInner::replay_wal_into(
            Arc::clone(&table_store),
            &DbReaderOptions::default(),
            &core,
            &mut into_tables,
            true,
        )
        .await
        .unwrap();

        assert_eq!(last_wal_id, 0);
        assert_eq!(last_committed_seq, 0);
        assert!(into_tables.is_empty());
    }

    #[tokio::test]
    async fn replay_wal_into_should_replay_single_wal_for_fresh_db() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_db_reader_fresh_db_one_wal");
        let test_provider = TestProvider::new(path, Arc::clone(&object_store));
        let table_store = test_provider.table_store();

        let wal_row = RowEntry::new_value(b"key", b"value", 1);
        write_wal_sst(Arc::clone(&table_store), 1, vec![wal_row.clone()])
            .await
            .unwrap();

        let mut into_tables = VecDeque::new();
        let core = ManifestCore::new();

        let (last_wal_id, last_committed_seq) = DbReaderInner::replay_wal_into(
            Arc::clone(&table_store),
            &DbReaderOptions::default(),
            &core,
            &mut into_tables,
            true,
        )
        .await
        .unwrap();

        assert_eq!(last_wal_id, 1);
        assert_eq!(last_committed_seq, 1);
        assert_eq!(into_tables.len(), 1);

        let replayed = into_tables.front().unwrap();
        assert_eq!(replayed.recent_flushed_wal_id(), 1);

        let mut replayed_iter = replayed.table().iter();
        test_utils::assert_iterator(&mut replayed_iter, vec![wal_row]).await;
    }

    #[tokio::test]
    async fn replay_wal_into_should_preserve_existing_last_committed_seq_for_empty_fence_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_db_reader_empty_fence_wal");
        let test_provider = TestProvider::new(path, Arc::clone(&object_store));
        let table_store = test_provider.table_store();

        write_wal_sst(Arc::clone(&table_store), 6, vec![])
            .await
            .unwrap();

        let mut into_tables = VecDeque::new();
        into_tables.push_front(immutable_memtable(
            5,
            vec![
                RowEntry::new_value(b"existing_key_1", b"existing_value_1", 9),
                RowEntry::new_value(b"existing_key_2", b"existing_value_2", 10),
            ],
        ));

        let mut core = ManifestCore::new();
        core.last_l0_seq = 8;
        core.next_wal_sst_id = 5;

        let (last_wal_id, last_committed_seq) = DbReaderInner::replay_wal_into(
            Arc::clone(&table_store),
            &DbReaderOptions::default(),
            &core,
            &mut into_tables,
            true,
        )
        .await
        .unwrap();

        assert_eq!(last_wal_id, 6);
        assert_eq!(last_committed_seq, 10);
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
            .new_db_reader(reader_options, None, None)
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

    #[tokio::test]
    async fn skip_wal_replay_should_not_see_wal_only_writes() {
        use crate::config::{FlushOptions, FlushType};

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        // Create a DB and write some data, then flush memtable to L0 SSTs
        let db = test_provider.new_db(Settings::default()).await.unwrap();
        let flushed_key = b"flushed_key";
        let flushed_value = b"flushed_value";
        db.put(flushed_key, flushed_value).await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        // Write more data that stays in WAL (not flushed to L0)
        let wal_only_key = b"wal_only_key";
        let wal_only_value = b"wal_only_value";
        db.put(wal_only_key, wal_only_value).await.unwrap();
        // Only flush to WAL, not to L0 SSTs
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        // Open a reader with skip_wal_replay=true
        let reader_options = DbReaderOptions {
            skip_wal_replay: true,
            ..DbReaderOptions::default()
        };
        let reader = test_provider
            .new_db_reader(reader_options.clone(), None, None)
            .await
            .unwrap();

        // Should see the L0 flushed data
        assert_eq!(
            reader.get(flushed_key).await.unwrap(),
            Some(Bytes::from_static(flushed_value))
        );

        // Should NOT see the WAL-only data
        assert_eq!(reader.get(wal_only_key).await.unwrap(), None);

        // After flushing memtable to L0, a NEW reader should see the data
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        // Open a new reader - it should see the newly flushed data
        let reader2 = test_provider
            .new_db_reader(reader_options, None, None)
            .await
            .unwrap();
        assert_eq!(
            reader2.get(wal_only_key).await.unwrap(),
            Some(Bytes::from_static(wal_only_value))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn skip_wal_replay_should_be_respected_during_reestablish_checkpoint() {
        use crate::config::{FlushOptions, FlushType};

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let db = test_provider.new_db(Settings::default()).await.unwrap();

        // Write initial data and flush to L0 so the reader opens with this state
        db.put(b"key1", b"value1").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        // Open reader with skip_wal_replay=true. The poller's first tick fires
        // immediately (during the next yield) and sees no manifest change.
        let reader_options = DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(100),
            skip_wal_replay: true,
            ..DbReaderOptions::default()
        };
        let reader = test_provider
            .new_db_reader(reader_options, None, None)
            .await
            .unwrap();

        // Capture checkpoint ID before the flush so the wait condition is not
        // affected by a race where the poller replaces the checkpoint during
        // the flush.
        let manifest_store = test_provider.manifest_store();
        let mut stored_manifest =
            StoredManifest::load(manifest_store, test_provider.system_clock.clone())
                .await
                .unwrap();
        let initial_checkpoint_id = stored_manifest
            .manifest()
            .core
            .checkpoints
            .first()
            .unwrap()
            .id;

        // Inject a failpoint on WAL listing before flushing so it is active
        // when the poller fires. With the buggy replay_new_wals=true,
        // reestablish_checkpoint calls last_seen_wal_id() which lists WAL SSTs
        // and hits this failpoint. With the fix (replay_new_wals=false), the
        // WAL listing is skipped entirely.
        fail_parallel::cfg(
            Arc::clone(&test_provider.fp_registry),
            "list-wal-ssts",
            "return",
        )
        .unwrap();

        // Write more data and flush to L0, changing the manifest's L0 state.
        // This makes should_reestablish_checkpoint() return true on the next poll.
        // Note: the writer uses its own TableStore (not the test_provider's),
        // so the failpoint above does not affect the writer's flush path.
        db.put(b"key2", b"value2").await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        // Wait for the manifest poller to see the changed L0 state and
        // reestablish the checkpoint. Without the fix, the poller crashes
        // on the WAL listing failpoint.
        let timeout = Duration::from_secs(5);
        let start = tokio::time::Instant::now();
        loop {
            assert!(
                start.elapsed() < timeout,
                "timed out waiting for checkpoint reestablishment"
            );
            let manifest = stored_manifest.refresh().await.unwrap();
            let current_checkpoint = manifest.core.checkpoints.first().unwrap();
            if current_checkpoint.id != initial_checkpoint_id {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // With the fix, the reader should still work (poller didn't crash).
        // Without the fix, the poller crashes and get() returns an error.
        let result = reader.get(b"key1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn skip_wal_replay_should_see_l0_data() {
        use crate::config::{FlushOptions, FlushType};

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        // Create a DB and write data, then flush memtable to L0 SSTs
        let db = test_provider.new_db(Settings::default()).await.unwrap();
        let key = b"test_key";
        let value = b"test_value";
        db.put(key, value).await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        // Open a reader with skip_wal_replay=true
        let reader_options = DbReaderOptions {
            skip_wal_replay: true,
            ..DbReaderOptions::default()
        };
        let reader = test_provider
            .new_db_reader(reader_options, None, None)
            .await
            .unwrap();

        // Should see the L0 data
        assert_eq!(
            reader.get(key).await.unwrap(),
            Some(Bytes::from_static(value))
        );
    }

    struct TestProvider {
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        fp_registry: Arc<FailPointRegistry>,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
    }

    impl TestProvider {
        fn new(path: Path, object_store: Arc<dyn ObjectStore>) -> Self {
            let system_clock = Arc::new(DefaultSystemClock::new());
            let rand = Arc::new(DbRand::default());
            TestProvider {
                object_store,
                path,
                fp_registry: Arc::new(FailPointRegistry::new()),
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
            merge_operator: Option<MergeOperatorType>,
        ) -> Result<DbReader, SlateDBError> {
            DbReader::open_internal(
                self,
                checkpoint,
                merge_operator,
                options,
                self.system_clock.clone(),
                self.rand.clone(),
                slatedb_common::metrics::MetricsRecorderHelper::noop(),
            )
            .await
        }
    }

    fn immutable_memtable(
        recent_flushed_wal_id: u64,
        entries: Vec<RowEntry>,
    ) -> Arc<ImmutableMemtable> {
        let table = WritableKVTable::new();
        for entry in entries {
            table.put(entry);
        }
        Arc::new(ImmutableMemtable::new(table, recent_flushed_wal_id))
    }

    async fn write_wal_sst(
        table_store: Arc<TableStore>,
        wal_id: u64,
        entries: Vec<RowEntry>,
    ) -> Result<(), SlateDBError> {
        let mut writer = table_store.table_writer(SsTableId::Wal(wal_id));
        for entry in entries {
            writer.add(entry).await?;
        }
        writer.close().await?;
        Ok(())
    }

    #[derive(Debug)]
    struct InputMemtable {
        recent_flushed_wal_id: u64,
        seqs: Vec<u64>,
    }

    impl InputMemtable {
        fn new(recent_flushed_wal_id: u64, seqs: Vec<u64>) -> Self {
            Self {
                recent_flushed_wal_id,
                seqs,
            }
        }

        fn build(&self) -> Arc<ImmutableMemtable> {
            immutable_memtable(
                self.recent_flushed_wal_id,
                self.seqs
                    .iter()
                    .map(|seq| {
                        let key = format!("key-{seq:020}");
                        let value = format!("value-{seq:020}");
                        RowEntry::new_value(key.as_bytes(), value.as_bytes(), *seq)
                    })
                    .collect(),
            )
        }
    }

    #[derive(Debug)]
    struct RebuildCheckpointCase {
        last_l0_seq: u64,
        tables: Vec<InputMemtable>,
        expected: Vec<InputMemtable>,
    }

    fn test_checkpoint(manifest_id: u64, clock: Arc<dyn SystemClock>) -> crate::Checkpoint {
        crate::Checkpoint {
            id: Uuid::new_v4(),
            manifest_id,
            expire_time: None,
            create_time: clock.now(),
            name: None,
        }
    }

    #[rstest]
    #[case::skips_table_when_last_seq_is_below_last_l0_seq(RebuildCheckpointCase {
        last_l0_seq: 10,
        tables: vec![InputMemtable::new(7, vec![7, 8, 9])],
        expected: vec![],
    })]
    #[case::skips_table_when_last_seq_equals_last_l0_seq(RebuildCheckpointCase {
        last_l0_seq: 10,
        tables: vec![InputMemtable::new(7, vec![10])],
        expected: vec![],
    })]
    #[case::keeps_entire_table_when_first_seq_is_just_after_last_l0_seq(RebuildCheckpointCase {
        last_l0_seq: 10,
        tables: vec![InputMemtable::new(7, vec![11, 12])],
        expected: vec![InputMemtable::new(7, vec![11, 12])],
    })]
    #[case::filters_table_when_first_seq_equals_last_l0_seq(RebuildCheckpointCase {
        last_l0_seq: 10,
        tables: vec![InputMemtable::new(7, vec![10, 11, 12])],
        expected: vec![InputMemtable::new(7, vec![11, 12])],
    })]
    #[case::filters_table_when_only_last_row_is_newer_than_last_l0_seq(RebuildCheckpointCase {
        last_l0_seq: 10,
        tables: vec![InputMemtable::new(7, vec![8, 9, 10, 11])],
        expected: vec![InputMemtable::new(7, vec![11])],
    })]
    #[case::preserves_order_across_keep_filter_and_skip_paths(RebuildCheckpointCase {
        last_l0_seq: 20,
        tables: vec![
            InputMemtable::new(9, vec![25, 26]),
            InputMemtable::new(8, vec![20, 21, 22]),
            InputMemtable::new(7, vec![18, 19, 20]),
            InputMemtable::new(6, vec![21, 23]),
        ],
        expected: vec![
            InputMemtable::new(9, vec![25, 26]),
            InputMemtable::new(8, vec![21, 22]),
            InputMemtable::new(6, vec![21, 23]),
        ],
    })]
    #[tokio::test]
    async fn rebuild_checkpoint_state_should_filter_existing_imm_memtables_by_last_l0_seq(
        #[case] case: RebuildCheckpointCase,
    ) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from(format!(
            "/tmp/test_db_reader_rebuild_checkpoint_state_{}",
            Uuid::new_v4()
        ));
        let test_provider = TestProvider::new(path, Arc::clone(&object_store));
        let manifest_store = test_provider.manifest_store();
        let table_store = test_provider.table_store();
        let mut stored_manifest = StoredManifest::create_new_db(
            Arc::clone(&manifest_store),
            ManifestCore::new(),
            test_provider.system_clock.clone(),
        )
        .await
        .unwrap();

        // Seed the prior checkpoint state with IMMs.
        let input_tables: Vec<_> = case.tables.iter().map(InputMemtable::build).collect();
        let prior_state = CheckpointState {
            checkpoint: test_checkpoint(stored_manifest.id(), test_provider.system_clock.clone()),
            manifest: stored_manifest.manifest().clone(),
            imm_memtable: input_tables.iter().cloned().collect(),
            last_wal_id: 0,
            last_remote_persisted_seq: 0,
        };
        let next_wal_sst_id = input_tables
            .iter()
            .map(|table| table.recent_flushed_wal_id())
            .max()
            .unwrap_or(0)
            + 1;

        // Advance only the manifest fields that control the rebuild filter logic.
        // We also pin replay_after_wal_id to the last known WAL so these cases
        // exercise IMM filtering without attempting any fresh WAL replay.
        let mut dirty = stored_manifest.prepare_dirty().unwrap();
        dirty.value.core.last_l0_seq = case.last_l0_seq;
        dirty.value.core.next_wal_sst_id = next_wal_sst_id;
        dirty.value.core.replay_after_wal_id = next_wal_sst_id.saturating_sub(1);
        stored_manifest.update(dirty).await.unwrap();
        let new_manifest_id = stored_manifest.id();

        // Construct just enough DbReaderInner state to call rebuild_checkpoint_state()
        // directly. skip_wal_replay keeps the test scoped to the IMM retention logic.
        let oracle = Arc::new(DbReaderOracle::new(0, DbStatusManager::new(0)));
        let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
        let reader = Reader {
            table_store: Arc::clone(&table_store),
            db_stats: DbStats::new(&recorder),
            mono_clock: Arc::new(MonotonicClock::new(
                test_provider.system_clock.clone(),
                i64::MIN,
            )),
            oracle: oracle.clone(),
            merge_operator: None,
        };
        let inner = DbReaderInner {
            manifest_store,
            table_store,
            options: DbReaderOptions {
                skip_wal_replay: true,
                ..DbReaderOptions::default()
            },
            state: parking_lot::RwLock::new(Arc::new(prior_state)),
            system_clock: test_provider.system_clock.clone(),
            user_checkpoint_id: None,
            oracle,
            reader,
            status_manager: DbStatusManager::new(0),
            rand: test_provider.rand.clone(),
            recorder,
        };

        let rebuilt_state = inner
            .rebuild_checkpoint_state(test_checkpoint(
                new_manifest_id,
                test_provider.system_clock.clone(),
            ))
            .await
            .unwrap();

        // The rebuilt checkpoint should reflect the new manifest.
        assert_eq!(rebuilt_state.manifest.core.last_l0_seq, case.last_l0_seq);
        assert_eq!(rebuilt_state.imm_memtable.len(), case.expected.len());

        for (rebuilt_table, expected_table) in
            rebuilt_state.imm_memtable.iter().zip(case.expected.iter())
        {
            let table = rebuilt_table.table();
            let mut iter = table.iter();
            let mut seqs = Vec::new();
            while let Some(entry) = iter.next_sync() {
                seqs.push(entry.seq);
            }

            // Every retained row must be strictly newer than the manifest's last L0 seq.
            assert_eq!(seqs, expected_table.seqs);
            assert!(seqs.iter().all(|seq| *seq > case.last_l0_seq));
            assert_eq!(
                rebuilt_table.recent_flushed_wal_id(),
                expected_table.recent_flushed_wal_id
            );

            // The filtered table's metadata should agree with the rows that survived.
            let metadata = rebuilt_table.table().metadata();
            assert_eq!(metadata.first_seq, *expected_table.seqs.first().unwrap());
            assert_eq!(metadata.last_seq, *expected_table.seqs.last().unwrap());
        }
    }

    #[test]
    fn should_reestablish_checkpoint_when_latest_last_l0_seq_exceeds_last_remote_persisted_seq() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from(format!(
            "/tmp/test_db_reader_should_reestablish_checkpoint_{}",
            Uuid::new_v4()
        ));
        let test_provider = TestProvider::new(path, Arc::clone(&object_store));
        let manifest_store = test_provider.manifest_store();
        let table_store = test_provider.table_store();

        let mut current_core = ManifestCore::new();
        current_core.last_l0_seq = 10;
        current_core.next_wal_sst_id = 2;

        let prior_state = CheckpointState {
            checkpoint: test_checkpoint(1, test_provider.system_clock.clone()),
            manifest: Manifest::initial(current_core.clone()),
            imm_memtable: VecDeque::from([immutable_memtable(
                1,
                vec![RowEntry::new_value(b"key", b"value", 10)],
            )]),
            last_wal_id: 1,
            last_remote_persisted_seq: 10,
        };

        let oracle = Arc::new(DbReaderOracle::new(0, DbStatusManager::new(0)));
        let recorder = slatedb_common::metrics::MetricsRecorderHelper::noop();
        let reader = Reader {
            table_store: Arc::clone(&table_store),
            db_stats: DbStats::new(&recorder),
            mono_clock: Arc::new(MonotonicClock::new(
                test_provider.system_clock.clone(),
                i64::MIN,
            )),
            oracle: oracle.clone(),
            merge_operator: None,
        };
        let inner = DbReaderInner {
            manifest_store,
            table_store,
            options: DbReaderOptions::default(),
            state: parking_lot::RwLock::new(Arc::new(prior_state)),
            system_clock: test_provider.system_clock.clone(),
            user_checkpoint_id: None,
            oracle,
            reader,
            status_manager: DbStatusManager::new(0),
            rand: test_provider.rand.clone(),
            recorder,
        };

        assert!(!inner.should_reestablish_checkpoint(&current_core));

        let mut latest = current_core.clone();
        latest.last_l0_seq = 11;

        assert!(inner.should_reestablish_checkpoint(&latest));
    }

    #[tokio::test]
    async fn should_populate_disk_cache_on_read() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_db_reader_disk_cache");

        // Write data via Db
        let db = Db::builder(path.clone(), Arc::clone(&object_store))
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();
        db.put(b"key1", b"value1").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        // Open a DbReader with disk caching enabled
        let cache_dir = tempfile::Builder::new()
            .prefix("dbreader_cache_test_")
            .tempdir()
            .unwrap();
        let cache_path = cache_dir.keep();

        let mut reader_opts = DbReaderOptions::default();
        reader_opts.object_store_cache_options.root_folder = Some(cache_path.clone());
        reader_opts.object_store_cache_options.part_size_bytes = 1024;

        let reader = DbReader::open(path.clone(), Arc::clone(&object_store), None, reader_opts)
            .await
            .unwrap();

        // Read data to populate the cache
        let val = reader.get(b"key1").await.unwrap();
        assert_eq!(val, Some(Bytes::from_static(b"value1")));

        // Verify the cache directory has been populated
        let entries: Vec<_> = std::fs::read_dir(&cache_path).unwrap().collect();
        assert!(
            !entries.is_empty(),
            "Expected disk cache directory to be populated after read"
        );
    }

    #[tokio::test]
    async fn should_record_metrics_with_recorder() {
        use slatedb_common::metrics::{lookup_metric_with_labels, DefaultMetricsRecorder};

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_db_reader_metrics");

        // Write data via Db so there's an SST to read from
        let db = Db::builder(path.clone(), Arc::clone(&object_store))
            .with_settings(Settings::default())
            .build()
            .await
            .unwrap();
        db.put(b"key1", b"value1").await.unwrap();
        db.flush().await.unwrap();
        db.close().await.unwrap();

        // Open a DbReader with a metrics recorder
        let metrics_recorder = Arc::new(DefaultMetricsRecorder::new());
        let reader = DbReader::builder(path, object_store)
            .with_metrics_recorder(metrics_recorder.clone())
            .build()
            .await
            .unwrap();

        // Verify that get_requests metric is incremented
        let val = reader.get(b"key1").await.unwrap();
        assert_eq!(val, Some(Bytes::from_static(b"value1")));
        assert_eq!(
            lookup_metric_with_labels(
                &metrics_recorder,
                crate::db_stats::REQUEST_COUNT,
                &[("op", "get")]
            ),
            Some(1)
        );
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
            ))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn db_reader_get_returns_correct_merge_result_after_reestablish_from_l0() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_db_reader_merge_reestablish_from_l0");
        let clock = Arc::new(MockSystemClock::new());

        let mut test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));
        test_provider.system_clock = clock.clone();

        let merge_operator: crate::merge_operator::MergeOperatorType =
            Arc::new(crate::test_utils::StringConcatMergeOperator);

        let db = Db::builder(path.clone(), Arc::clone(&object_store))
            .with_settings(Settings {
                flush_interval: None,
                compactor_options: None,
                garbage_collector_options: None,
                ..Settings::default()
            })
            .with_system_clock(clock.clone())
            .with_merge_operator(merge_operator.clone())
            .build()
            .await
            .unwrap();

        let key = b"k";

        // Phase A: make the reader recover merge operands from WAL into replayed IMMs.
        db.merge_with_options(
            key,
            b"a",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.merge_with_options(
            key,
            b"b",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::Wal,
        })
        .await
        .unwrap();

        let reader = test_provider
            .new_db_reader(
                DbReaderOptions {
                    manifest_poll_interval: Duration::from_millis(100),
                    checkpoint_lifetime: Duration::from_secs(30),
                    ..DbReaderOptions::default()
                },
                None,
                Some(merge_operator),
            )
            .await
            .unwrap();

        assert_eq!(
            reader.get(key).await.unwrap(),
            Some(Bytes::from_static(b"ab"))
        );
        assert!(
            !reader.inner.state.read().imm_memtable.is_empty(),
            "reader should have replayed WAL data into immutable memtables"
        );

        // Let the reader's immediate first ticker fire and settle before phase B.
        tokio::task::yield_now().await;

        // Phase B: write newer merge operands and flush the writer memtable to L0.
        db.merge_with_options(
            key,
            b"c",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.merge_with_options(
            key,
            b"d",
            &MergeOptions::default(),
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();

        let manifest_store = test_provider.manifest_store();
        let mut stored_manifest =
            StoredManifest::load(manifest_store, test_provider.system_clock.clone())
                .await
                .unwrap();

        let start = tokio::time::Instant::now();
        loop {
            let manifest = stored_manifest.refresh().await.unwrap();
            if manifest.core.l0.len() == 1 {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "timed out waiting for writer manifest to include the L0 flush"
            );
            tokio::task::yield_now().await;
        }

        let timeout = Duration::from_secs(30);
        let start = tokio::time::Instant::now();
        loop {
            if reader.inner.state.read().manifest.core.l0.len() == 1 {
                break;
            }
            // The reader poller may observe the pre-flush manifest on one tick and
            // only see the new L0 on a later poll. Keep advancing the mock clock
            // until the reader reestablishes from the updated manifest.
            clock.advance(Duration::from_millis(100)).await;
            assert!(
                start.elapsed() < timeout,
                "timed out waiting for reader to reestablish from the new manifest"
            );
            tokio::task::yield_now().await;
        }

        // Correct behavior: the reader should return the full merged value after reestablish.
        assert_eq!(
            reader.get(key).await.unwrap(),
            Some(Bytes::from_static(b"abcd"))
        );

        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn should_subscribe_to_durable_seq_updates() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let db = test_provider
            .new_db(Settings {
                l0_sst_size_bytes: 256,
                ..Settings::default()
            })
            .await
            .unwrap();

        // Write initial data and flush so reader can see it.
        db.put(b"k1", b"v1").await.unwrap();
        db.flush().await.unwrap();

        let reader_options = DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(10),
            ..DbReaderOptions::default()
        };
        let reader = test_provider
            .new_db_reader(reader_options, None, None)
            .await
            .unwrap();

        let mut rx = reader.subscribe();
        let initial_seq = rx.borrow().durable_seq;
        assert!(initial_seq > 0);

        // Write more data and flush.
        db.put(b"k2", b"v2").await.unwrap();
        db.flush().await.unwrap();

        // Wait for the reader's manifest poll to pick up the new data.
        tokio::time::sleep(Duration::from_millis(20)).await;

        rx.changed().await.unwrap();
        let updated_seq = rx.borrow().durable_seq;
        assert!(
            updated_seq > initial_seq,
            "durable_seq should advance: {} > {}",
            updated_seq,
            initial_seq
        );

        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_return_ok_status_when_open() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let db = test_provider.new_db(Settings::default()).await.unwrap();
        let reader = test_provider
            .new_db_reader(DbReaderOptions::default(), None, None)
            .await
            .unwrap();

        assert!(reader.status().is_ok());

        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_return_err_status_when_closed() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let db = test_provider.new_db(Settings::default()).await.unwrap();
        let reader = test_provider
            .new_db_reader(DbReaderOptions::default(), None, None)
            .await
            .unwrap();

        reader.close().await.unwrap();
        assert!(reader.status().is_err());

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_report_close_via_subscribe() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");
        let test_provider = TestProvider::new(path.clone(), Arc::clone(&object_store));

        let db = test_provider.new_db(Settings::default()).await.unwrap();
        let reader = test_provider
            .new_db_reader(DbReaderOptions::default(), None, None)
            .await
            .unwrap();

        let mut rx = reader.subscribe();
        assert!(rx.borrow().close_reason.is_none());

        reader.close().await.unwrap();

        // The watch channel should report the close.
        rx.changed().await.unwrap();
        assert!(rx.borrow().close_reason.is_some());

        db.close().await.unwrap();
    }
}
