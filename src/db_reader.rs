use crate::bytes_range::BytesRange;
use crate::config::{
    CheckpointOptions, Clock, DbReaderOptions, ReadOptions, ScanOptions, SystemClock,
};
use crate::db_reader::ManifestPollerMsg::Shutdown;
use crate::db_state::CoreDbState;
use crate::error::SlateDBError;
use crate::iter::KeyValueIterator;
use crate::manifest::Manifest;
use crate::manifest_store::{ManifestStore, StoredManifest};
use crate::mem_table::{ImmutableMemtable, VecDequeKeyValueIterator, WritableKVTable};
use crate::reader::Reader;
use crate::sorted_run_iterator::SortedRunIterator;
use crate::sst::SsTableFormat;
use crate::sst_iter::{SstIterator, SstIteratorOptions};
use crate::tablestore::TableStore;
use crate::{filter, utils, wal_replay, Checkpoint, DbIterator};
use bytes::Bytes;
use log::{info, warn};
use object_store::path::Path;
use object_store::ObjectStore;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::ops::{RangeBounds, Sub};
use std::sync::Arc;
use std::{cmp, mem};
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
    state: Arc<RwLock<CheckpointState>>,
    clock: Arc<dyn Clock + Sync + Send>,
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
    pub(crate) checkpoint: Checkpoint,
    pub(crate) manifest: Manifest,
    pub(crate) imm_memtables: Vec<Arc<ImmutableMemtable>>,
}

impl DbReaderInner {
    async fn new(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: DbReaderOptions,
        checkpoint: Checkpoint,
        clock: Arc<dyn Clock + Send + Sync>,
    ) -> Result<Self, SlateDBError> {
        let initial_state = Self::build_checkpoint_state(
            Arc::clone(&manifest_store),
            Arc::clone(&table_store),
            &options,
            checkpoint,
        )
        .await?;

        Ok(Self {
            manifest_store,
            table_store,
            options,
            state: Arc::new(RwLock::new(initial_state)),
            clock,
        })
    }

    fn should_reestablish_checkpoint(&self, latest: &CoreDbState) -> bool {
        let read_guard = self.state.read();
        latest.next_wal_sst_id > read_guard.manifest.core.next_wal_sst_id
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
        let checkpoint_state = Self::build_checkpoint_state(
            Arc::clone(&self.manifest_store),
            Arc::clone(&self.table_store),
            &self.options,
            checkpoint,
        )
        .await?;
        let mut write_guard = self.state.write();
        *write_guard = checkpoint_state;
        Ok(())
    }

    async fn build_checkpoint_state(
        manifest_store: Arc<ManifestStore>,
        table_store: Arc<TableStore>,
        options: &DbReaderOptions,
        checkpoint: Checkpoint,
    ) -> Result<CheckpointState, SlateDBError> {
        let manifest = manifest_store.read_manifest(checkpoint.manifest_id).await?;
        let imm_memtables = Self::replay_wal(Arc::clone(&table_store), options, &manifest).await?;

        Ok(CheckpointState {
            checkpoint,
            manifest,
            imm_memtables,
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
        if self.clock.now_systime() > refresh_deadline {
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
                        } else {
                            this.maybe_refresh_checkpoint(&mut manifest).await?;
                        }
                    },
                    msg = thread_rx.recv() => {
                        return match msg.expect("channel unexpectedly closed") {
                            Shutdown => {
                                let mut manifest = StoredManifest::load(
                                    Arc::clone(&this.manifest_store),
                                ).await?;
                                let checkpoint_id = this.state.read().checkpoint.id;
                                info!("Deleting reader checkpoint {} for shutdown", checkpoint_id);
                                manifest.delete_checkpoint(checkpoint_id).await
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

        let join_handle = utils::spawn_bg_task(
            &Handle::current(),
            move |err| {
                warn!("manifest polling thread exited with {:?}", err);
                // TODO: notify any waiters about the failure
            },
            fut,
        );

        Ok(ManifestPoller {
            join_handle: Mutex::new(Some(join_handle)),
            thread_tx,
        })
    }

    async fn replay_wal(
        table_store: Arc<TableStore>,
        reader_options: &DbReaderOptions,
        manifest: &Manifest,
    ) -> Result<Vec<Arc<ImmutableMemtable>>, SlateDBError> {
        let wal_id_range = manifest.core.list_noncompacted_wal_ids();
        let last_wal_id = wal_id_range.end - 1;
        let mut last_tick = manifest.core.last_l0_clock_tick;

        // load the last seq number from manifest, and use it as the starting seq number.
        // there might have bigger seq number in the WALs, we'd update the last seq number
        // to the max seq number while iterating over the WALs.
        let mut last_seq = manifest.core.last_l0_seq;
        let mut curr_memtable = WritableKVTable::new();
        let mut imm_memtables = Vec::new();
        for wal_id in wal_id_range {
            let mut sst_iter = wal_replay::load_wal_iter(Arc::clone(&table_store), wal_id).await?;

            while let Some(kv) = sst_iter.next_entry().await? {
                if let Some(ts) = kv.create_ts {
                    last_tick = cmp::max(last_tick, ts);
                }

                last_seq = last_seq.max(kv.seq);
                curr_memtable.put(kv.clone());

                // TODO: We are allowing the memtable to exceed the limit
                //  Maybe we can drop the last inserted key and insert
                //  it into the next table instead
                if curr_memtable.size() as u64 > reader_options.max_memtable_bytes {
                    let completed_memtable =
                        mem::replace(&mut curr_memtable, WritableKVTable::new());
                    imm_memtables.push(Arc::new(ImmutableMemtable::new(
                        completed_memtable,
                        last_wal_id,
                    )));
                }
            }
        }

        if !curr_memtable.is_empty() {
            imm_memtables.push(Arc::new(ImmutableMemtable::new(curr_memtable, last_wal_id)));
        }

        Ok(imm_memtables)
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
                msg: "Checkpoint lifetime must be at least 1s".to_string(),
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
        Self::open_with_clock(
            path,
            object_store,
            checkpoint_id,
            options,
            Arc::new(SystemClock::default()),
        )
        .await
    }

    pub async fn open_with_clock<P: Into<Path>>(
        path: P,
        object_store: Arc<dyn ObjectStore>,
        checkpoint_id: Option<Uuid>,
        options: DbReaderOptions,
        clock: Arc<dyn Clock + Send + Sync>,
    ) -> Result<Self, SlateDBError> {
        Self::validate_options(&options)?;

        let path = path.into();
        let manifest_store = Arc::new(ManifestStore::new_with_clock(
            &path,
            Arc::clone(&object_store),
            Arc::clone(&clock),
        ));

        let table_store = Arc::new(TableStore::new(
            Arc::clone(&object_store),
            SsTableFormat::default(),
            path.clone(),
            options.block_cache.clone(),
        ));

        let mut manifest = StoredManifest::load(Arc::clone(&manifest_store)).await?;
        if !manifest.db_state().initialized {
            return Err(SlateDBError::InvalidDBState);
        }

        let checkpoint = Self::get_or_create_checkpoint(
            &mut manifest,
            checkpoint_id,
            &options,
        )
        .await?;

        let inner = Arc::new(
            DbReaderInner::new(manifest_store, table_store, options, checkpoint, clock).await?,
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

    async fn get_or_create_checkpoint(
        manifest: &mut StoredManifest,
        checkpoint_id: Option<Uuid>,
        options: &DbReaderOptions,
    ) -> Result<Checkpoint, SlateDBError> {
        let checkpoint = if let Some(checkpoint_id) = checkpoint_id {
            manifest
                .db_state()
                .find_checkpoint(&checkpoint_id)
                .ok_or(SlateDBError::CheckpointMissing(checkpoint_id))?
                .clone()
        } else {
            let options = CheckpointOptions {
                lifetime: Some(options.checkpoint_lifetime),
                ..CheckpointOptions::default()
            };
            manifest
                .write_checkpoint(None, &options)
                .await?
        };
        Ok(checkpoint)
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

#[async_trait::async_trait]
impl Reader for DbReader {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        _options: &ReadOptions,
    ) -> Result<Option<Bytes>, SlateDBError> {
        let key = key.as_ref();
        let snapshot = self.inner.state.read().clone();
        let maybe_val = snapshot
            .imm_memtables
            .iter()
            .find_map(|memtable| memtable.table().get(key));
        if let Some(val) = maybe_val {
            return Ok(val.value.as_bytes());
        }

        // Since the key remains unchanged during the point query, we only need to compute
        // the hash value once and pass it to the filter to avoid unnecessary hash computation
        let key_hash = filter::filter_hash(key);

        let sst_iter_options = SstIteratorOptions {
            cache_blocks: true,
            eager_spawn: true,
            ..SstIteratorOptions::default()
        };

        for sst in &snapshot.manifest.core.l0 {
            let filter_result = self
                .inner
                .table_store
                .sst_might_include_key(sst, key, key_hash)
                .await?;
            if filter_result.might_contain_key() {
                let mut iter = SstIterator::for_key(
                    sst,
                    key,
                    Arc::clone(&self.inner.table_store),
                    sst_iter_options,
                )
                .await?;

                if let Some(entry) = iter.next_entry().await? {
                    if entry.key == key {
                        return Ok(entry.value.as_bytes());
                    }
                }
            }
        }

        for sr in &snapshot.manifest.core.compacted {
            let filter_result = self
                .inner
                .table_store
                .sr_might_include_key(sr, key, key_hash)
                .await?;
            if filter_result.might_contain_key() {
                let mut iter = SortedRunIterator::for_key(
                    sr,
                    key,
                    Arc::clone(&self.inner.table_store),
                    sst_iter_options,
                )
                .await?;
                if let Some(entry) = iter.next_entry().await? {
                    if entry.key == key {
                        return Ok(entry.value.as_bytes());
                    }
                }
            }
        }

        Ok(None)
    }

    async fn scan_with_options<K, T>(
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
        let snapshot = self.inner.state.read().clone();

        let mut memtables = VecDeque::new();
        for memtable in &snapshot.imm_memtables {
            memtables.push_back(memtable.table());
        }

        let mem_iter =
            VecDequeKeyValueIterator::materialize_range(memtables, range.clone()).await?;

        let read_ahead_blocks = self
            .inner
            .table_store
            .bytes_to_blocks(options.read_ahead_bytes);

        let sst_iter_options = SstIteratorOptions {
            max_fetch_tasks: 1,
            blocks_to_fetch: read_ahead_blocks,
            cache_blocks: options.cache_blocks,
            eager_spawn: true,
        };

        let mut l0_iters = VecDeque::new();
        for sst in snapshot.manifest.core.l0 {
            let iter = SstIterator::new_owned(
                range.clone(),
                sst,
                Arc::clone(&self.inner.table_store),
                sst_iter_options,
            )
            .await?;
            l0_iters.push_back(iter);
        }

        let mut sr_iters = VecDeque::new();
        for sr in snapshot.manifest.core.compacted {
            let iter = SortedRunIterator::new_owned(
                range.clone(),
                sr,
                Arc::clone(&self.inner.table_store),
                sst_iter_options,
            )
            .await?;
            sr_iters.push_back(iter);
        }

        DbIterator::new(range.clone(), mem_iter, l0_iters, sr_iters).await
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{CheckpointOptions, CheckpointScope, Clock, DbOptions};
    use crate::db_reader::{DbReader, DbReaderOptions};
    use crate::db_state::CoreDbState;
    use crate::manifest::Manifest;
    use crate::manifest_store::ManifestStore;
    use crate::reader::Reader;
    use crate::test_utils::TokioClock;
    use crate::{test_utils, Db, SlateDBError};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use std::collections::BTreeMap;
    use std::ops::RangeFull;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn should_get_latest_value_from_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        let db = Db::open_with_opts(
            path.clone(),
            DbOptions::default(),
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
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
            Some(Bytes::from_static(value2))
        );
    }

    #[tokio::test]
    async fn should_get_from_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        let db = Db::open_with_opts(
            path.clone(),
            DbOptions::default(),
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
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
        let manifest_store = Arc::new(ManifestStore::new(&path, Arc::clone(&object_store)));

        let mut uninitialized_manifest = Manifest::initial(CoreDbState::new());
        uninitialized_manifest.core.initialized = false;
        manifest_store
            .write_manifest(1, &uninitialized_manifest)
            .await
            .unwrap();

        let err = DbReader::open(
            path.clone(),
            Arc::clone(&object_store),
            None,
            DbReaderOptions::default(),
        )
        .await;
        assert!(matches!(err, Err(SlateDBError::InvalidDBState)));
    }

    #[tokio::test]
    async fn should_scan_from_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        let db = Db::open_with_opts(
            path.clone(),
            DbOptions::default(),
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
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

        let reader = DbReader::open(
            path.clone(),
            Arc::clone(&object_store),
            Some(checkpoint_result.id),
            DbReaderOptions::default(),
        )
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

        let db = Db::open_with_opts(
            path.clone(),
            DbOptions::default(),
            Arc::clone(&object_store),
        )
        .await
        .unwrap();

        let reader_options = DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(10),
            ..DbReaderOptions::default()
        };
        let reader = DbReader::open(
            path.clone(),
            Arc::clone(&object_store),
            None,
            reader_options,
        )
        .await
        .unwrap();

        let key = b"key";
        assert_eq!(reader.get(key).await.unwrap(), None);

        let value = b"value";
        db.put(key, value).await.unwrap();
        db.close().await.unwrap(); // Close to force manifest update

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            reader.get(key).await.unwrap(),
            Some(Bytes::from_static(value))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn should_refresh_reader_checkpoint() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_kv_store");

        let _db = Db::open_with_opts(
            path.clone(),
            DbOptions::default(),
            Arc::clone(&object_store),
        )
        .await
        .unwrap();

        let reader_options = DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(500),
            checkpoint_lifetime: Duration::from_millis(1000),
            ..DbReaderOptions::default()
        };

        let clock = Arc::new(TokioClock::new()) as Arc<dyn Clock + Send + Sync>;

        let manifest_store = Arc::new(ManifestStore::new_with_clock(
            &path,
            Arc::clone(&object_store),
            Arc::clone(&clock),
        ));

        let reader = DbReader::open_with_clock(
            path.clone(),
            Arc::clone(&object_store),
            None,
            reader_options,
            Arc::clone(&clock),
        )
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
}
