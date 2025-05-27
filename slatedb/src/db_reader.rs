use crate::bytes_range::BytesRange;
use crate::config::{
    CheckpointOptions, Clock, DbReaderOptions, ReadOptions, ScanOptions, SystemClock,
};
use crate::db_reader::ManifestPollerMsg::Shutdown;
use crate::db_state::CoreDbState;
use crate::db_stats::DbStats;
use crate::error::SlateDBError;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::manifest::Manifest;
use crate::mem_table::{ImmutableMemtable, KVTable};
use crate::reader::{ReadSnapshot, Reader};
use crate::sst_iter::SstIteratorOptions;
use crate::stats::StatRegistry;
use crate::store_provider::{DefaultStoreProvider, StoreProvider};
use crate::tablestore::TableStore;
use crate::utils::{MonotonicClock, WatchableOnceCell};
use crate::wal_replay::{WalReplayIterator, WalReplayOptions};
use crate::{utils, Checkpoint, DbIterator};
use bytes::Bytes;
use log::{info, warn};
use object_store::path::Path;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::ops::{RangeBounds, Sub};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

/// Read-only interface for accessing a database from either
/// the latest persistent state or from an arbitrary checkpoint.
pub struct DbReader {
    inner: Arc<DbReaderInner>,
    manifest_poller: Option<ManifestPoller>,
}

struct DbReaderInner {
    manifest_store: Arc<ManifestStore>,
    table_store: Arc<TableStore>,
    options: DbReaderOptions,
    state: RwLock<Arc<CheckpointState>>,
    clock: Arc<dyn Clock + Sync + Send>,
    user_checkpoint_id: Option<Uuid>,
    reader: Reader,
    error_watcher: WatchableOnceCell<SlateDBError>,
}

struct ManifestPoller {
    join_handle: Mutex<Option<tokio::task::JoinHandle<Result<(), SlateDBError>>>>,
    thread_tx: UnboundedSender<ManifestPollerMsg>,
}

enum ManifestPollerMsg {
    Shutdown,
}

#[derive(Clone)]
struct CheckpointState {
    checkpoint: Checkpoint,
    manifest: Manifest,
    imm_memtable: VecDeque<Arc<ImmutableMemtable>>,
    last_wal_id: u64,
}

static EMPTY_TABLE: Lazy<Arc<KVTable>> = Lazy::new(|| Arc::new(KVTable::new()));

impl ReadSnapshot for CheckpointState {
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
        clock: Arc<dyn Clock + Send + Sync>,
    ) -> Result<Self, SlateDBError> {
        let mut manifest = StoredManifest::load(Arc::clone(&manifest_store)).await?;
        if !manifest.db_state().initialized {
            return Err(SlateDBError::InvalidDBState);
        }

        let checkpoint =
            Self::get_or_create_checkpoint(&mut manifest, checkpoint_id, &options).await?;

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
            clock.clone(),
            initial_state.core().last_l0_clock_tick,
        ));

        let stat_registry = Arc::new(StatRegistry::new());
        let db_stats = DbStats::new(stat_registry.as_ref());

        let state = RwLock::new(initial_state);
        let reader = Reader {
            table_store: Arc::clone(&table_store),
            db_stats: db_stats.clone(),
            mono_clock: Arc::clone(&mono_clock),
            wal_enabled: true,
        };

        Ok(Self {
            manifest_store,
            table_store,
            options,
            state,
            clock,
            user_checkpoint_id: checkpoint_id,
            reader,
            error_watcher: WatchableOnceCell::new(),
        })
    }

    async fn get_or_create_checkpoint(
        manifest: &mut StoredManifest,
        checkpoint_id: Option<Uuid>,
        options: &DbReaderOptions,
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
            manifest.write_checkpoint(None, &options).await?
        };
        Ok(checkpoint)
    }

    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        self.check_error()?;
        let snapshot = Arc::clone(&self.state.read());
        self.reader
            .get_with_options(key, options, snapshot.as_ref(), None)
            .await
    }

    async fn scan_with_options(
        &self,
        range: BytesRange,
        options: &ScanOptions,
    ) -> Result<DbIterator, SlateDBError> {
        self.check_error()?;
        let snapshot = Arc::clone(&self.state.read());
        self.reader
            .scan_with_options(range, options, snapshot.as_ref(), None)
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
        stored_manifest
            .replace_checkpoint(current_checkpoint_id, &options)
            .await
    }

    async fn reestablish_checkpoint(&self, checkpoint: Checkpoint) -> Result<(), SlateDBError> {
        let new_checkpoint_state = self.rebuild_checkpoint_state(checkpoint).await?;
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

            let last_wal_id = Self::replay_wal_into(
                Arc::clone(&self.table_store),
                &self.options,
                current_checkpoint.core(),
                &mut imm_memtable,
                true,
            )
            .await?;

            let mut write_guard = self.state.write();
            *write_guard = Arc::new(CheckpointState {
                checkpoint: current_checkpoint.checkpoint.clone(),
                manifest: current_checkpoint.manifest.clone(),
                imm_memtable,
                last_wal_id,
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
        let last_wal_id = Self::replay_wal_into(
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
        if utils::now_systime(self.clock.as_ref()) > refresh_deadline {
            let refreshed_checkpoint = stored_manifest
                .refresh_checkpoint(checkpoint.id, self.options.checkpoint_lifetime)
                .await?;
            info!(
                "Refreshed checkpoint {} to expire at {:?}",
                checkpoint.id, refreshed_checkpoint.expire_time
            )
        }
        Ok(())
    }

    fn spawn_manifest_poller(self: &Arc<Self>) -> Result<ManifestPoller, SlateDBError> {
        let this = Arc::clone(self);
        async fn core_poll_loop(
            this: Arc<DbReaderInner>,
            thread_rx: &mut UnboundedReceiver<ManifestPollerMsg>,
        ) -> Result<(), SlateDBError> {
            let mut ticker = tokio::time::interval(this.options.manifest_poll_interval);
            loop {
                select! {
                    _ = ticker.tick() => {
                        let mut manifest = StoredManifest::load(
                            Arc::clone(&this.manifest_store),
                        ).await?;

                        let latest_manifest = manifest.manifest();
                        if this.should_reestablish_checkpoint(&latest_manifest.core) {
                            let checkpoint = this.replace_checkpoint(&mut manifest).await?;
                            this.reestablish_checkpoint(checkpoint).await?;
                        } else  {
                            this.maybe_replay_new_wals().await?;
                        }

                        this.maybe_refresh_checkpoint(&mut manifest).await?;
                    },
                    msg = thread_rx.recv() => {
                        return match msg.expect("channel unexpectedly closed") {
                            Shutdown => {
                                let mut manifest = StoredManifest::load(
                                    Arc::clone(&this.manifest_store),
                                ).await?;
                                let checkpoint_id = this.state.read().checkpoint.id;
                                if Some(checkpoint_id) != this.user_checkpoint_id {
                                    info!("Deleting reader established checkpoint {} for shutdown", checkpoint_id);
                                    manifest.delete_checkpoint(checkpoint_id).await?;
                                }
                                Ok(())
                            },
                        }
                    }
                }
            }
        }

        let (thread_tx, mut thread_rx) = tokio::sync::mpsc::unbounded_channel();
        let fut = async move {
            let result = core_poll_loop(this, &mut thread_rx).await;
            info!("Manifest poll thread exiting with result {:?}", result);
            result
        };

        let this = Arc::clone(self);
        let join_handle = utils::spawn_bg_task(
            &Handle::current(),
            move |result| {
                warn!("manifest polling thread exited with {:?}", result);
                if let Err(err) = result {
                    this.error_watcher.write(err.clone());
                }
            },
            fut,
        );

        Ok(ManifestPoller {
            join_handle: Mutex::new(Some(join_handle)),
            thread_tx,
        })
    }

    async fn replay_wal_into(
        table_store: Arc<TableStore>,
        reader_options: &DbReaderOptions,
        core: &CoreDbState,
        into_tables: &mut VecDeque<Arc<ImmutableMemtable>>,
        replay_new_wals: bool,
    ) -> Result<u64, SlateDBError> {
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
        while let Some(replayed_table) = replay_iter.next().await? {
            last_wal_id = replayed_table.last_wal_id;
            let imm_memtable =
                ImmutableMemtable::new(replayed_table.table, replayed_table.last_wal_id);
            into_tables.push_back(Arc::new(imm_memtable));
        }

        Ok(last_wal_id)
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

impl DbReader {
    fn validate_options(options: &DbReaderOptions) -> Result<(), SlateDBError> {
        if options.checkpoint_lifetime.as_millis() < 1000 {
            return Err(SlateDBError::InvalidArgument {
                msg: "Checkpoint lifetime must be at least 1s".to_string(),
            });
        }

        let double_poll_interval =
            options
                .manifest_poll_interval
                .checked_mul(2)
                .ok_or(SlateDBError::InvalidArgument {
                    msg: "Manifest poll interval is too large".to_string(),
                })?;
        if options.checkpoint_lifetime < double_poll_interval {
            return Err(SlateDBError::InvalidArgument {
                msg: "Checkpoint lifetime must be at least double the manifest poll interval"
                    .to_string(),
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
    ) -> Result<Self, SlateDBError> {
        let path = path.into();
        let store_provider = DefaultStoreProvider {
            path,
            object_store,
            block_cache: options.block_cache.clone(),
        };

        Self::open_internal(
            &store_provider,
            checkpoint_id,
            options,
            Arc::new(SystemClock::default()),
        )
        .await
    }

    async fn open_internal(
        store_provider: &dyn StoreProvider,
        checkpoint_id: Option<Uuid>,
        options: DbReaderOptions,
        clock: Arc<dyn Clock + Send + Sync>,
    ) -> Result<Self, SlateDBError> {
        Self::validate_options(&options)?;

        let manifest_store = store_provider.manifest_store();
        let table_store = store_provider.table_store();
        let inner = Arc::new(
            DbReaderInner::new(manifest_store, table_store, options, checkpoint_id, clock).await?,
        );

        // If no checkpoint was provided, then we have established a new checkpoint
        // from the latest state, and we need to refresh it according to the params
        // of `DbReaderOptions`.
        let manifest_poller = if checkpoint_id.is_none() {
            Some(inner.spawn_manifest_poller()?)
        } else {
            None
        };

        Ok(Self {
            inner,
            manifest_poller,
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
    /// - `Result<Option<Bytes>, SlateDBError>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error getting the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, config::DbReaderOptions, SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
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
    pub async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<Bytes>, SlateDBError> {
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
    /// - `Result<Option<Bytes>, SlateDBError>`:
    ///     - `Some(Bytes)`: the value if it exists
    ///     - `None`: if the value does not exist
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error getting the value
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, config::DbReaderOptions, config::ReadOptions, SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
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
    ) -> Result<Option<Bytes>, SlateDBError> {
        self.inner.get_with_options(key, options).await
    }

    /// Scan a range of keys using the default scan options.
    ///
    /// returns a `DbIterator`
    ///
    /// ## Arguments
    /// - `range`: the range of keys to scan
    ///
    /// ## Errors
    /// - `SlateDBError`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, SlateDBError>`: An iterator with the results of the scan
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, config::DbReaderOptions, SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
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
    pub async fn scan<K, T>(&self, range: T) -> Result<DbIterator, SlateDBError>
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
    /// - `SlateDBError`: if there was an error scanning the range of keys
    ///
    /// ## Returns
    /// - `Result<DbIterator, SlateDBError>`: An iterator with the results of the scan
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, config::DbReaderOptions, config::ScanOptions, config::DurabilityLevel, SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
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
    ) -> Result<DbIterator, SlateDBError>
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
        self.inner.scan_with_options(range, options).await
    }

    /// Close the database reader.
    ///
    /// ## Returns
    /// - `Result<(), SlateDBError>`: if there was an error closing the reader
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::{Db, DbReader, config::DbReaderOptions, SlateDBError};
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), SlateDBError> {
    ///     let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///     let db = Db::open("test_db", object_store.clone()).await?;
    ///     let options = DbReaderOptions::default();
    ///     let reader = DbReader::open("test_db", object_store.clone(), None, options).await?;
    ///     reader.close().await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    pub async fn close(&self) -> Result<(), SlateDBError> {
        if let Some(poller) = &self.manifest_poller {
            poller.thread_tx.send(Shutdown).ok();
            if let Some(join_handle) = {
                let mut guard = poller.join_handle.lock();
                guard.take()
            } {
                let result = join_handle.await.expect("Failed to join manifest poller");
                info!("Manifest poller exited with {:?}", result);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{CheckpointOptions, CheckpointScope, Clock, Settings};
    use crate::db_reader::{DbReader, DbReaderOptions};
    use crate::db_state::CoreDbState;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::Manifest;
    use crate::object_stores::ObjectStores;
    use crate::paths::PathResolver;
    use crate::proptest_util::rng::new_test_rng;
    use crate::proptest_util::sample;
    use crate::sst::SsTableFormat;
    use crate::store_provider::StoreProvider;
    use crate::tablestore::TableStore;
    use crate::test_utils::TokioClock;
    use crate::{test_utils, Db, SlateDBError};
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
            .create_checkpoint(
                CheckpointScope::All { force_flush: true },
                &CheckpointOptions::default(),
            )
            .await
            .unwrap();

        let reader = DbReader::open_internal(
            &test_provider,
            Some(checkpoint_result.id),
            DbReaderOptions::default(),
            Arc::clone(&test_provider.clock),
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
            .create_checkpoint(
                CheckpointScope::All { force_flush: true },
                &CheckpointOptions::default(),
            )
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
        let source_checkpoint_id = Uuid::new_v4();

        let _ = StoredManifest::create_uninitialized_clone(
            Arc::clone(&manifest_store),
            &parent_manifest,
            parent_path,
            source_checkpoint_id,
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
            .create_checkpoint(
                CheckpointScope::All { force_flush: true },
                &CheckpointOptions::default(),
            )
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
        let result = reader.get(b"key").await;
        let Err(err) = result else {
            panic!("Expected an error");
        };
        assert!(matches!(err, SlateDBError::IoError(_)));
    }

    struct TestProvider {
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        fp_registry: Arc<FailPointRegistry>,
        clock: Arc<dyn Clock + Send + Sync>,
    }

    impl TestProvider {
        fn new(path: Path, object_store: Arc<dyn ObjectStore>) -> Self {
            let clock = Arc::new(TokioClock::new()) as Arc<dyn Clock + Send + Sync>;
            TestProvider {
                object_store,
                path,
                fp_registry: Arc::new(FailPointRegistry::new()),
                clock,
            }
        }
    }

    impl TestProvider {
        async fn new_db(&self, options: Settings) -> Result<Db, SlateDBError> {
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
            DbReader::open_internal(self, checkpoint, options, Arc::clone(&self.clock)).await
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
            Arc::new(ManifestStore::new_with_clock(
                &self.path,
                Arc::clone(&self.object_store),
                Arc::clone(&self.clock),
            ))
        }
    }
}
