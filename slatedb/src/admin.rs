use crate::checkpoint::{Checkpoint, CheckpointCreateResult};
use crate::compactions_store::CompactionsStore;
use crate::compactor::{Compaction, CompactionSpec, Compactor, CompactorStateView};
use crate::compactor_state_protocols::CompactorStateReader;
use crate::config::{CheckpointOptions, GarbageCollectorOptions};
use crate::db::builder::GarbageCollectorBuilder;
use crate::db_state::{ManifestCore, SsTableHandle, SsTableId};
use crate::dispatcher::MessageHandlerExecutor;
use crate::error::SlateDBError;
use crate::garbage_collector::GC_TASK_NAME;
use crate::iter::KeyValueIterator;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::sst::SsTableFormat;
use crate::tablestore::TableStore;
use slatedb_common::clock::SystemClock;

use crate::clone;
use crate::object_stores::{ObjectStoreType, ObjectStores};
use crate::rand::DbRand;
use crate::seq_tracker::{FindOption, SequenceTracker, TrackedSeq};
use crate::utils::{IdGenerator, WatchableOnceCell};
use crate::wal_replay::{WalReplayIterator, WalReplayOptions};
use chrono::{DateTime, Utc};
use fail_parallel::FailPointRegistry;
use log::debug;
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::VecDeque;
use std::env;
use std::env::VarError;
use std::error::Error;
use std::ops::Range;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;
use uuid::Uuid;

pub use crate::db::builder::AdminBuilder;
use slatedb_txn_obj::TransactionalObject;

pub struct WalToL0Result {
    sst_handles: VecDeque<SsTableHandle>,
    last_tick: Option<i64>,
    last_seq: Option<u64>,
    sequence_tracker: SequenceTracker,
}

/// An Admin struct for SlateDB administration operations.
///
/// This struct provides methods for administrative functions such as
/// reading manifests, creating checkpoints, cloning databases, and
/// running garbage collection.
pub struct Admin {
    /// The path to the database.
    pub(crate) path: Path,
    /// The object stores to use for the main database and WAL.
    pub(crate) object_stores: ObjectStores,
    /// The system clock to use for operations.
    pub(crate) system_clock: Arc<dyn SystemClock>,
    /// The random number generator to use for randomness.
    pub(crate) rand: Arc<DbRand>,
}

impl Admin {
    /// Read-only access to the latest manifest file
    pub async fn read_manifest(
        &self,
        maybe_id: Option<u64>,
    ) -> Result<Option<String>, Box<dyn Error>> {
        let manifest_store = ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        );
        let id_manifest = if let Some(id) = maybe_id {
            manifest_store
                .try_read_manifest(id)
                .await?
                .map(|manifest| (id, manifest))
        } else {
            manifest_store.try_read_latest_manifest().await?
        };

        match id_manifest {
            None => Ok(None),
            Some(result) => Ok(Some(serde_json::to_string(&result)?)),
        }
    }

    /// List manifests within a range
    pub async fn list_manifests<R: RangeBounds<u64>>(
        &self,
        range: R,
    ) -> Result<String, Box<dyn Error>> {
        let manifest_store = ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        );
        let manifests = manifest_store.list_manifests(range).await?;
        Ok(serde_json::to_string(&manifests)?)
    }

    /// Read-only access to the latest compactions file
    ///
    /// ## Arguments
    /// - `maybe_id`: Optional ID of the compactions file to read. If None, reads from the latest.
    ///
    /// ## Returns
    /// - `Ok(Some(String))`: The compactions as a JSON string if found.
    /// - `Ok(None)`: If the compactions file does not exist.
    pub async fn read_compactions(
        &self,
        maybe_id: Option<u64>,
    ) -> Result<Option<String>, Box<dyn Error>> {
        let compactions_store = self.compactions_store();
        let id_compactions = if let Some(id) = maybe_id {
            compactions_store
                .try_read_compactions(id)
                .await?
                .map(|compactions| (id, compactions))
        } else {
            compactions_store.try_read_latest_compactions().await?
        };

        match id_compactions {
            None => Ok(None),
            Some(result) => Ok(Some(serde_json::to_string(&result)?)),
        }
    }

    /// Read-only access to a compaction by id from a specific or latest compactions file.
    ///
    /// ## Arguments
    /// - `compaction_id`: The ULID of the compaction to read.
    /// - `maybe_id`: Optional ID of the compactions file to read from. If None, reads from the latest.
    ///
    /// ## Returns
    /// - `Ok(Some(Compaction))`: The compaction if found.
    /// - `Ok(None)`: If the compactions file or compaction ID does not exist.
    pub async fn read_compaction(
        &self,
        compaction_id: Ulid,
        maybe_id: Option<u64>,
    ) -> Result<Option<Compaction>, Box<dyn Error>> {
        let compactions_store = self.compactions_store();
        let compactions = if let Some(compactions_id) = maybe_id {
            compactions_store
                .try_read_compactions(compactions_id)
                .await?
        } else {
            compactions_store
                .try_read_latest_compactions()
                .await?
                .map(|(_id, compactions)| compactions)
        };
        let Some(compactions) = compactions else {
            return Ok(None);
        };
        let Some(compaction) = compactions.get(&compaction_id) else {
            return Ok(None);
        };

        Ok(Some(compaction.clone()))
    }

    /// Returns a read-only view of the current compactor state.
    pub async fn read_compactor_state_view(&self) -> Result<CompactorStateView, Box<dyn Error>> {
        let manifest_store = Arc::new(ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        ));
        let compactions_store = Arc::new(self.compactions_store());
        let reader = CompactorStateReader::new(&manifest_store, &compactions_store);
        Ok(reader.read_view().await?)
    }

    /// Generate a compaction from a spec and submit it.
    ///
    /// ## Returns
    /// - `Ok(Compaction)`: The submitted compaction.
    /// - `Err`: If there was an error during submission or reading the submitted compaction.
    pub async fn submit_compaction(
        &self,
        spec: CompactionSpec,
    ) -> Result<Compaction, Box<dyn Error>> {
        let compactions_store = Arc::new(self.compactions_store());
        let compaction_id = Compactor::submit(
            spec,
            compactions_store,
            Arc::new(DbRand::new(self.rand.seed())),
            self.system_clock.clone(),
        )
        .await?;
        let Some(compaction) = self.read_compaction(compaction_id, None).await? else {
            return Err(Box::new(SlateDBError::InvalidDBState));
        };

        Ok(compaction)
    }

    /// List compactions files within a range
    pub async fn list_compactions<R: RangeBounds<u64>>(
        &self,
        range: R,
    ) -> Result<String, Box<dyn Error>> {
        let compactions_store = self.compactions_store();
        let compactions = compactions_store.list_compactions(range).await?;
        Ok(serde_json::to_string(&compactions)?)
    }

    /// List checkpoints, optionally filtering by name. When name is provided, only checkpoints
    /// with this exact name will be returned.
    ///
    /// # Arguments
    ///
    /// * `name_filter`: Name that will be used to filter checkpoints.
    pub async fn list_checkpoints(
        &self,
        name_filter: Option<&str>,
    ) -> Result<Vec<Checkpoint>, Box<dyn Error>> {
        let manifest_store = ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        );
        let (_, manifest) = manifest_store.read_latest_manifest().await?;

        let checkpoints = match name_filter {
            Some("") => manifest
                .core
                .checkpoints
                .into_iter()
                .filter(|cp| cp.name.as_deref() == Some("") || cp.name.is_none())
                .collect(),
            Some(name) => manifest
                .core
                .checkpoints
                .into_iter()
                .filter(|cp| cp.name.as_deref() == Some(name))
                .collect(),
            None => manifest.core.checkpoints,
        };

        Ok(checkpoints)
    }

    /// Run the garbage collector once in the foreground.
    ///
    /// This function runs the garbage collector letting Tokio decide when to run the task.
    ///
    /// # Arguments
    ///
    /// * `gc_opts`: The garbage collector options.
    ///
    pub async fn run_gc_once(
        &self,
        gc_opts: GarbageCollectorOptions,
    ) -> Result<(), Box<dyn Error>> {
        let gc = GarbageCollectorBuilder::new(
            self.path.clone(),
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        )
        .with_system_clock(self.system_clock.clone())
        .with_wal_object_store(self.object_stores.store_of(ObjectStoreType::Wal).clone())
        .with_options(gc_opts)
        .with_seed(self.rand.seed())
        .build();
        gc.run_gc_once().await;
        Ok(())
    }

    /// Run the garbage collector in the background.
    ///
    /// This function runs the garbage collector in a Tokio background task.
    ///
    /// # Arguments
    ///
    /// * `gc_opts`: The garbage collector options.
    ///
    pub async fn run_gc(&self, gc_opts: GarbageCollectorOptions) -> Result<(), crate::Error> {
        let gc = GarbageCollectorBuilder::new(
            self.path.clone(),
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        )
        .with_system_clock(self.system_clock.clone())
        .with_wal_object_store(self.object_stores.store_of(ObjectStoreType::Wal).clone())
        .with_options(gc_opts)
        .with_seed(self.rand.seed())
        .build();

        let (_, rx) = mpsc::unbounded_channel();
        let closed_result = WatchableOnceCell::new();
        let task_executor = MessageHandlerExecutor::new(closed_result, self.system_clock.clone());

        task_executor
            .add_handler(
                GC_TASK_NAME.to_string(),
                Box::new(gc),
                rx,
                &Handle::current(),
            )
            .map_err(Into::<crate::Error>::into)?;

        task_executor
            .join_task(GC_TASK_NAME)
            .await
            .map_err(Into::<crate::Error>::into)
    }

    /// Run the compactor in the foreground until the provided cancellation token is cancelled.
    ///
    /// This method blocks until `cancellation_token` is cancelled, at which point it requests a
    /// graceful shutdown and waits for the compactor to stop.
    pub async fn run_compactor(
        &self,
        cancellation_token: CancellationToken,
    ) -> Result<(), crate::Error> {
        let compactor = crate::CompactorBuilder::new(
            self.path.clone(),
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        )
        .with_system_clock(self.system_clock.clone())
        .with_seed(self.rand.seed())
        .build();

        let mut run_task = tokio::spawn({
            let compactor = compactor.clone();
            async move { compactor.run().await }
        });

        tokio::select! {
            result = &mut run_task => {
                return match result {
                    Ok(inner) => inner,
                    Err(join_err) => Err(crate::Error::internal("compactor task failed".to_string()).with_source(Box::new(join_err))),
                };
            }
            _ = cancellation_token.cancelled() => {
                // fall through to shutdown logic
            }
        }

        compactor.stop().await
    }

    /// Creates a checkpoint of the db stored in the object store at the specified path using the
    /// provided options. The checkpoint will reference the current active manifest of the db. This
    /// method does not flush writer memtables or WALs before creating the checkpoint. You will be
    /// responsible for refreshing checkpoints periodically.
    ///
    /// If you have a [`crate::Db`] instance open, you can use the [`crate::Db::create_checkpoint`]
    /// method instead. That method will flush the memtables and WALs before creating the checkpoint.
    ///
    /// If you're using a [`crate::DbReader`], you might wish to have the reader manage the checkpoint
    /// for you by calling [`crate::DbReader::open`] with no `checkpoint_id` set. The reader will
    /// create a checkpoint for you and periodically refresh it.
    ///
    /// # Examples
    ///
    /// ```
    /// use slatedb::admin::{Admin, AdminBuilder};
    /// use slatedb::config::CheckpointOptions;
    /// use slatedb::Db;
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::error::Error;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn Error>> {
    ///    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///    let db = Db::open("parent_path", Arc::clone(&object_store)).await?;
    ///    db.put(b"key", b"value").await?;
    ///    db.close().await?;
    ///
    ///    let admin = AdminBuilder::new("parent_path", object_store).build();
    ///    let _ = admin.create_detached_checkpoint(
    ///      &CheckpointOptions::default(),
    ///    ).await?;
    ///
    ///    Ok(())
    /// }
    /// ```
    pub async fn create_detached_checkpoint(
        &self,
        options: &CheckpointOptions,
    ) -> Result<CheckpointCreateResult, crate::Error> {
        let manifest_store = Arc::new(ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        ));
        manifest_store
            .validate_no_wal_object_store_configured()
            .await?;
        let mut stored_manifest =
            StoredManifest::load(manifest_store, self.system_clock.clone()).await?;
        let checkpoint_id = self.rand.rng().gen_uuid();
        let checkpoint = stored_manifest
            .write_checkpoint(checkpoint_id, options)
            .await?;
        Ok(CheckpointCreateResult {
            id: checkpoint.id,
            manifest_id: checkpoint.manifest_id,
        })
    }

    /// Refresh the lifetime of an existing checkpoint. Takes the id of an existing checkpoint
    /// and a lifetime, and sets the lifetime of the checkpoint to the specified lifetime. If
    /// there is no checkpoint with the specified id, then this fn fails with
    /// SlateDBError::InvalidDbState
    pub async fn refresh_checkpoint(
        &self,
        id: Uuid,
        lifetime: Option<Duration>,
    ) -> Result<(), crate::Error> {
        let manifest_store = Arc::new(ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        ));
        let mut stored_manifest =
            StoredManifest::load(manifest_store, self.system_clock.clone()).await?;
        stored_manifest
            .maybe_apply_update(|stored_manifest| {
                let mut dirty = stored_manifest.prepare_dirty()?;
                let expire_time = lifetime.map(|l| self.system_clock.now() + l);
                let Some(_) = dirty.value.core.checkpoints.iter_mut().find_map(|c| {
                    if c.id == id {
                        c.expire_time = expire_time;
                        return Some(());
                    }
                    None
                }) else {
                    return Err(SlateDBError::InvalidDBState);
                };
                Ok(Some(dirty))
            })
            .await
            .map_err(Into::into)
    }

    /// Deletes the checkpoint with the specified id.
    pub async fn delete_checkpoint(&self, id: Uuid) -> Result<(), crate::Error> {
        let manifest_store = Arc::new(ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        ));
        let mut stored_manifest =
            StoredManifest::load(manifest_store, self.system_clock.clone()).await?;
        stored_manifest
            .maybe_apply_update(|stored_manifest| {
                let mut dirty = stored_manifest.prepare_dirty()?;
                let checkpoints: Vec<Checkpoint> = dirty
                    .value
                    .core
                    .checkpoints
                    .iter()
                    .filter(|c| c.id != id)
                    .cloned()
                    .collect();
                dirty.value.core.checkpoints = checkpoints;
                Ok(Some(dirty))
            })
            .await
            .map_err(Into::into)
    }

    /// Restores the checkpoint by duplicating the checkpointed manifest as the latest and replays
    /// the necessary WALs to new l0 ssts.
    ///
    /// sst_size (bytes) defaults to 64MB if None.
    ///
    /// Prevents concurrent operations by:
    /// - Fencing writers through uploading a fencing WAL at the start of the operation
    /// - Fencing other clients through advancing the new manifest's epochs
    ///
    /// This function preserves checkpoints from the current (latest) manifest rather than
    /// restoring checkpoints from the checkpointed manifest, as those historical checkpoints
    /// may no longer be valid at the current point in time.
    pub async fn restore_checkpoint(
        &self,
        id: Uuid,
        sst_size: Option<usize>,
    ) -> Result<(), crate::Error> {
        let manifest_store = Arc::new(self.manifest_store());
        let wal_store = Arc::new(self.table_store());
        let fencing_wal = self.fence_writers(&wal_store).await?;

        let mut current_manifest =
            StoredManifest::load(manifest_store.clone(), self.system_clock.clone()).await?;
        let checkpoint = match current_manifest.db_state().find_checkpoint(id) {
            Some(found_checkpoint) => found_checkpoint.clone(),
            None => return Err(SlateDBError::CheckpointMissing(id).into()),
        };

        let manifest_to_restore = manifest_store.read_manifest(checkpoint.manifest_id).await?;

        let WalToL0Result {
            sst_handles,
            last_tick,
            last_seq,
            sequence_tracker,
        } = self
            .replay_wal_to_l0(
                manifest_to_restore.core.replay_after_wal_id + 1
                    ..manifest_to_restore.core.next_wal_sst_id,
                &manifest_to_restore.core,
                wal_store.clone(),
                sst_size,
            )
            .await?;

        current_manifest
            .maybe_apply_update(|stored_manifest| {
                let mut dirty = stored_manifest.prepare_dirty()?;
                dirty.value = manifest_to_restore.clone();

                // do not restore old checkpoints as they can be invalid at this point in time.
                // instead keep the checkpoints of the current manifest which should all still be
                // valid.
                dirty.value.core.checkpoints = stored_manifest.object().core.checkpoints.clone();

                dirty.value.core.l0.reserve(sst_handles.len());
                for sst_handle in sst_handles.iter().rev() {
                    dirty.value.core.l0.push_front(sst_handle.clone());
                }

                if let Some(last_tick) = last_tick {
                    dirty.value.core.last_l0_clock_tick = last_tick;
                }
                if let Some(last_seq) = last_seq {
                    dirty.value.core.last_l0_seq = last_seq;
                }
                dirty
                    .value
                    .core
                    .sequence_tracker
                    .extend_from(&sequence_tracker);
                dirty.value.core.replay_after_wal_id = fencing_wal;
                dirty.value.core.next_wal_sst_id = fencing_wal + 1;

                // advance epochs to fence any other clients that might still be running
                dirty.value.writer_epoch = stored_manifest.object().writer_epoch + 1;
                dirty.value.compactor_epoch = stored_manifest.object().compactor_epoch + 1;

                Ok(Some(dirty))
            })
            .await?;

        Ok(())
    }

    /// Returns the timestamp or sequence from the latest manifest's sequence tracker.
    /// When `round_up` is true, uses the next higher value; otherwise the previous one.
    pub async fn get_timestamp_for_sequence(
        &self,
        seq: u64,
        round_up: bool,
    ) -> Result<Option<DateTime<Utc>>, crate::Error> {
        let manifest_store = self.manifest_store();

        let id_manifest = manifest_store.try_read_latest_manifest().await?;
        let Some((_id, manifest)) = id_manifest else {
            return Ok(None);
        };

        let opt = if round_up {
            FindOption::RoundUp
        } else {
            FindOption::RoundDown
        };
        Ok(manifest.core.sequence_tracker.find_ts(seq, opt))
    }

    /// Returns the sequence for a given timestamp from the latest manifest's sequence tracker.
    /// When `round_up` is true, uses the next higher value; otherwise the previous one.
    pub async fn get_sequence_for_timestamp(
        &self,
        ts: DateTime<Utc>,
        round_up: bool,
    ) -> Result<Option<u64>, crate::Error> {
        let manifest_store = self.manifest_store();

        let id_manifest = manifest_store.try_read_latest_manifest().await?;
        let Some((_id, manifest)) = id_manifest else {
            return Ok(None);
        };

        let opt = if round_up {
            FindOption::RoundUp
        } else {
            FindOption::RoundDown
        };
        Ok(manifest.core.sequence_tracker.find_seq(ts, opt))
    }

    // Writes an empty WAL SST to storage to fence other writers. Returns the id of the WAL
    // uploaded.
    async fn fence_writers(&self, wal_store: &TableStore) -> Result<u64, SlateDBError> {
        let mut empty_wal_id = wal_store.last_seen_wal_id().await?;

        loop {
            let empty_sst = wal_store.table_builder().build().await?;
            match wal_store
                .write_sst(&SsTableId::Wal(empty_wal_id), empty_sst, false)
                .await
            {
                Ok(_) => return Ok(empty_wal_id),
                Err(SlateDBError::Fenced) => empty_wal_id += 1,
                Err(e) => return Err(e),
            }
        }
    }

    // Replays WALs in the id range into SSTs and flushes them to l0.
    // Returns the handles of the newly generated and flushed SSTs, a sequence_tracker tracking the
    // sequence numbers inserted, and the latest sequence number and creation timestamp
    async fn replay_wal_to_l0(
        &self,
        wal_id_range: Range<u64>,
        core: &ManifestCore,
        table_store: Arc<TableStore>,
        l0_sst_size: Option<usize>,
    ) -> Result<WalToL0Result, crate::Error> {
        let mut result = WalToL0Result {
            sst_handles: VecDeque::new(),
            last_tick: None,
            last_seq: None,
            sequence_tracker: SequenceTracker::new(),
        };

        if wal_id_range.is_empty() {
            return Ok(result);
        }

        debug!("replaying wal_id_range {:?} to l0", &wal_id_range);

        let wal_replay_options = match l0_sst_size {
            Some(min_memtable_bytes) => WalReplayOptions {
                min_memtable_bytes,
                ..WalReplayOptions::default()
            },
            None => WalReplayOptions::default(),
        };
        let mut replay_iter = WalReplayIterator::range(
            wal_id_range,
            core,
            wal_replay_options,
            Arc::clone(&table_store),
        )
        .await?;

        while let Some(replayed_table) = replay_iter.next().await? {
            if replayed_table.table.is_empty() {
                continue;
            }

            let id = SsTableId::Compacted(self.rand.rng().gen_ulid(self.system_clock.as_ref()));
            let mut sst_builder = table_store.table_builder();
            let mut iter = replayed_table.table.table().iter();
            while let Some(entry) = iter.next_entry().await? {
                sst_builder.add(entry).await?;
            }
            let encoded_sst = sst_builder.build().await?;

            // TODO: This might need a configurable l0_max_ssts check, and if check fails, running
            // a compaction job (or return error).
            let handle = table_store.write_sst(&id, encoded_sst, false).await?;
            result.sst_handles.push_front(handle);

            result.last_seq = Some(std::cmp::max(
                result.last_seq.unwrap_or(replayed_table.last_seq),
                replayed_table.last_seq,
            ));
            result.last_tick = Some(std::cmp::max(
                result.last_tick.unwrap_or(replayed_table.last_tick),
                replayed_table.last_tick,
            ));

            // Insert sequence numbers at table-level granularity instead of per-entry to reduce complexity
            // and overhead. This is acceptable given SequenceTracker's approximate nature.
            result.sequence_tracker.insert(TrackedSeq {
                seq: replayed_table.last_seq,
                ts: self.system_clock.now(),
            });
        }

        debug!("Replayed WALs to new L0 SSTs: {:?}", result.sst_handles);

        Ok(result)
    }

    fn manifest_store(&self) -> ManifestStore {
        ManifestStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        )
    }

    fn table_store(&self) -> TableStore {
        TableStore::new(
            self.object_stores.clone(),
            SsTableFormat::default(),
            self.path.clone(),
            None,
        )
    }

    fn compactions_store(&self) -> CompactionsStore {
        CompactionsStore::new(
            &self.path,
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
        )
    }

    /// Clone a database. If no db already exists at the specified path, then this will create
    /// a new db under the path that is a clone of the db at parent_path.
    ///
    /// A clone is a shallow copy of the parent database - it starts with a manifest that
    /// references the same SSTs, but doesn't actually copy those SSTs, except for the WAL.
    /// New writes will be written to the newly created db and will not be reflected in the
    /// parent database.
    ///
    /// The clone can optionally be created from an existing checkpoint. If
    /// `parent_checkpoint` is present, then the referenced manifest is used
    /// as the base for the clone db's manifest. Otherwise, this method creates a new checkpoint
    /// for the current version of the parent db.
    ///
    /// # Examples
    ///
    /// ```
    /// use slatedb::admin::{Admin, AdminBuilder};
    /// use slatedb::Db;
    /// use slatedb::object_store::{ObjectStore, memory::InMemory};
    /// use std::error::Error;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn Error>> {
    ///    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    ///    let db = Db::open("parent_path", Arc::clone(&object_store)).await?;
    ///    db.put(b"key", b"value").await?;
    ///    db.close().await?;
    ///
    ///    let admin = AdminBuilder::new("clone_path", object_store).build();
    ///    admin.create_clone(
    ///      "parent_path",
    ///      None,
    ///    ).await?;
    ///
    ///    Ok(())
    /// }
    /// ```
    pub async fn create_clone<P: Into<Path>>(
        &self,
        parent_path: P,
        parent_checkpoint: Option<Uuid>,
    ) -> Result<(), Box<dyn Error>> {
        clone::create_clone(
            self.path.clone(),
            parent_path.into(),
            self.object_stores.store_of(ObjectStoreType::Main).clone(),
            parent_checkpoint,
            Arc::new(FailPointRegistry::new()),
            self.system_clock.clone(),
            self.rand.clone(),
        )
        .await?;
        Ok(())
    }

    /// Creates a new builder for an admin client at the given path.
    ///
    /// ## Arguments
    /// - `path`: the path to the database
    /// - `object_store`: the object store to use for the database
    ///
    /// ## Returns
    /// - `AdminBuilder`: the builder to initialize the admin client
    ///
    /// ## Examples
    ///
    /// ```
    /// use slatedb::admin::Admin;
    /// use slatedb::object_store::memory::InMemory;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let object_store = Arc::new(InMemory::new());
    ///     let admin = Admin::builder("/tmp/test_db", object_store).build();
    /// }
    /// ```
    pub fn builder<P: Into<Path>>(path: P, object_store: Arc<dyn ObjectStore>) -> AdminBuilder<P> {
        AdminBuilder::new(path, object_store)
    }
}

fn get_env_variable(name: &str) -> Result<String, SlateDBError> {
    env::var(name).map_err(|e| match e {
        VarError::NotPresent => SlateDBError::UndefinedEnvironmentVariable {
            key: name.to_string(),
        },
        VarError::NotUnicode(not_unicode_value) => SlateDBError::InvalidEnvironmentVariable {
            key: name.to_string(),
            value: format!("{:?}", not_unicode_value),
        },
    })
}

/// Loads an object store from configured environment variables.
/// The provider is specified using the CLOUD_PROVIDER variable.
/// For specific provider configurations, see the corresponding
/// method documentation:
///
/// | Provider | Value | Documentation |
/// |----------|-------|---------------|
/// | Local | `local` | [load_local] |
/// | Memory | `memory` | [load_memory] |
/// | AWS | `aws` | [load_aws] |
/// | Azure | `azure` | [load_azure] |
/// | OpenDAL | `opendal` | [load_opendal] |
pub fn load_object_store_from_env(
    env_file: Option<String>,
) -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    dotenvy::from_filename(env_file.unwrap_or(String::from(".env"))).ok();
    let cloud_provider = get_env_variable("CLOUD_PROVIDER")?;
    match cloud_provider.to_lowercase().as_str() {
        "local" => load_local(),
        "memory" => load_memory(),
        #[cfg(feature = "aws")]
        "aws" => load_aws(),
        #[cfg(feature = "azure")]
        "azure" => load_azure(),
        #[cfg(feature = "opendal")]
        "opendal" => load_opendal(),
        invalid_value => Err(SlateDBError::InvalidEnvironmentVariable {
            key: "CLOUD_PROVIDER".to_string(),
            value: invalid_value.to_string(),
        }
        .into()),
    }
}

/// Loads a local object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | LOCAL_PATH | The path to the local directory where all data will be stored | Yes |
pub fn load_local() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    let local_path = get_env_variable("LOCAL_PATH")?;
    let lfs = object_store::local::LocalFileSystem::new_with_prefix(local_path)?;
    Ok(Arc::new(lfs) as Arc<dyn ObjectStore>)
}

/// Loads an in-memory object store instance.
pub fn load_memory() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    Ok(Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>)
}

/// Loads an AWS S3 Object store instance. The environment variables consumed are
/// the same as those supported by [`AmazonS3Builder::from_env`]. Refer to the
/// builder documentation for the full list and meaning of supported variables:
/// <https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html#method.with_config>
#[cfg(feature = "aws")]
pub fn load_aws() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    use object_store::aws::S3ConditionalPut;

    let builder = object_store::aws::AmazonS3Builder::from_env()
        .with_allow_http(true)
        .with_conditional_put(S3ConditionalPut::ETagMatch);

    Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
}

/// Loads an Azure Object store instance. The environment variables consumed are
/// the same as those supported by [`MicrosoftAzureBuilder::from_env`]. Refer to
/// the builder documentation for the full list and meaning of supported variables:
/// <https://docs.rs/object_store/latest/object_store/azure/struct.MicrosoftAzureBuilder.html#method.with_config>
#[cfg(feature = "azure")]
pub fn load_azure() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    let builder = object_store::azure::MicrosoftAzureBuilder::from_env();
    Ok(Arc::new(builder.build()?) as Arc<dyn ObjectStore>)
}

/// Loads an OpenDAL Object store instance.
///
/// | Env Variable | Doc | Required |
/// |--------------|-----|----------|
/// | OPENDAL_SCHEME | The OpenDAL scheme to use | Yes |
/// | OPENDAL_* | The OpenDAL configuration | Yes |
/// full list of schemes: https://docs.rs/opendal/latest/opendal/enum.Scheme.html
/// for example, to use s3-compatible storage, you can set:
/// ```bash
/// OPENDAL_SCHEME=s3
/// OPENDAL_ENDPOINT=http://localhost:9000
/// OPENDAL_ACCESS_KEY_ID=minioadmin
/// OPENDAL_SECRET_ACCESS_KEY=minioadmin
/// OPENDAL_BUCKET=test
/// OPENDAL_REGION=us-east-1
/// OPENDAL_ROOT=/tmp
/// ```
/// full list of config: https://docs.rs/opendal/latest/opendal/services/s3/config/struct.S3Config.html
/// for example, to use oss, you can set:
/// ```bash
/// OPENDAL_SCHEME=oss
/// OPENDAL_ENDPOINT=http://oss-cn-shanghai.aliyuncs.com
/// OPENDAL_ACCESS_KEY_ID=your-access-key-id
/// OPENDAL_ACCESS_KEY_SECRET=your-access-key-secret
/// OPENDAL_BUCKET=your-bucket-name
/// OPENDAL_ROOT=/your/root/path
/// ```
/// full list of config: https://docs.rs/opendal/latest/opendal/services/oss/config/struct.OssConfig.html
#[cfg(feature = "opendal")]
pub fn load_opendal() -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    use opendal::{Operator, Scheme};
    use std::collections::HashMap;
    use std::str::FromStr;

    let scheme =
        Scheme::from_str(&env::var("OPENDAL_SCHEME").expect("OPENDAL_SCHEME must be set"))?;
    let iter = env::vars()
        .filter_map(|(k, v)| k.strip_prefix("OPENDAL_").map(|k| (k.to_lowercase(), v)))
        .collect::<HashMap<String, String>>();

    let op = Operator::via_iter(scheme, iter)?;
    Ok(Arc::new(object_store_opendal::OpendalStore::new(op)) as Arc<dyn ObjectStore>)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::admin::{load_object_store_from_env, AdminBuilder};
    use crate::admin::{Admin, WalToL0Result};
    use crate::compactions_store::{CompactionsStore, StoredCompactions};
    use crate::compactor_state::{Compaction, CompactionSpec, CompactionStatus, SourceId};
    use crate::db_state::SsTableId;
    use crate::iter::KeyValueIterator;
    use crate::sst_iter::{SstIterator, SstIteratorOptions};
    use crate::types::RowEntry;
    use crate::Db;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use ulid::Ulid;

    #[test]
    fn test_load_object_store_from_env() {
        figment::Jail::expect_with(|jail| {
            // creating an object store without CLOUD_PROVIDER env variable
            let r = load_object_store_from_env(None);
            assert!(r.is_err());
            assert_eq!(
                r.unwrap_err().to_string(),
                "undefined environment variable CLOUD_PROVIDER"
            );

            jail.create_file("invalid.env", "CLOUD_PROVIDER=invalid")
                .expect("failed to create temp env file");
            let r = load_object_store_from_env(Some("invalid.env".to_string()));
            assert!(r.is_err());
            assert_eq!(
                r.unwrap_err().to_string(),
                "invalid environment variable CLOUD_PROVIDER value `invalid`"
            );
            // unset since the environment variable loaded in from invalid.env
            // takes precedence over the memory.env file.
            std::env::remove_var("CLOUD_PROVIDER");

            jail.create_file("memory.env", "CLOUD_PROVIDER=memory")
                .expect("failed to create temp env file");
            let r = load_object_store_from_env(Some("memory.env".to_string()));
            let store = r.expect("expected memory object store");
            assert_eq!(store.to_string(), "InMemory");

            Ok(())
        });
    }

    #[tokio::test]
    async fn test_admin_read_compactions() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_admin_read_compactions");
        let compactions_store = Arc::new(CompactionsStore::new(&path, object_store.clone()));
        let mut stored = StoredCompactions::create(compactions_store.clone(), 7)
            .await
            .unwrap();

        let compaction_id = Ulid::new();
        let compaction = Compaction::new(
            compaction_id,
            CompactionSpec::new(vec![SourceId::SortedRun(3)], 7),
        );
        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.insert(compaction);
        stored.update(dirty).await.unwrap();

        let admin = AdminBuilder::new(path.clone(), object_store).build();

        let latest = admin
            .read_compactions(None)
            .await
            .unwrap()
            .expect("expected compactions");
        let latest_value: serde_json::Value = serde_json::from_str(&latest).unwrap();
        let latest_pair = latest_value.as_array().expect("expected [id, compactions]");
        assert_eq!(latest_pair[0].as_u64().unwrap(), 2);

        let latest_compactions = latest_pair[1].as_object().unwrap();
        assert_eq!(
            latest_compactions
                .get("compactor_epoch")
                .and_then(|v| v.as_u64())
                .unwrap(),
            7
        );
        let recent = latest_compactions
            .get("core")
            .expect("expected core")
            .get("recent_compactions")
            .and_then(|v| v.as_object())
            .unwrap();
        assert_eq!(recent.len(), 1);
        let compaction_id_str = compaction_id.to_string();
        let stored_compaction = recent
            .get(compaction_id_str.as_str())
            .expect("expected compaction entry");
        assert_eq!(
            stored_compaction
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap(),
            compaction_id_str
        );

        let first = admin
            .read_compactions(Some(1))
            .await
            .unwrap()
            .expect("expected compactions");
        let first_value: serde_json::Value = serde_json::from_str(&first).unwrap();
        let first_pair = first_value.as_array().expect("expected [id, compactions]");
        assert_eq!(first_pair[0].as_u64().unwrap(), 1);
        let first_compactions = first_pair[1].as_object().unwrap();
        let first_recent = first_compactions
            .get("core")
            .expect("expected core")
            .get("recent_compactions")
            .and_then(|v| v.as_object())
            .unwrap();
        assert_eq!(first_recent.len(), 0);
    }

    #[tokio::test]
    async fn test_admin_list_compactions() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_admin_list_compactions");
        let compactions_store = Arc::new(CompactionsStore::new(&path, object_store.clone()));
        let mut stored = StoredCompactions::create(compactions_store.clone(), 0)
            .await
            .unwrap();
        stored
            .update(stored.prepare_dirty().unwrap())
            .await
            .unwrap();

        let admin = AdminBuilder::new(path.clone(), object_store).build();
        let listed = admin.list_compactions(..).await.unwrap();
        let listed_value: Vec<serde_json::Value> = serde_json::from_str(&listed).unwrap();
        let ids: Vec<u64> = listed_value
            .iter()
            .filter_map(|item| item.get("id").and_then(|id| id.as_u64()))
            .collect();

        assert_eq!(ids, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_admin_read_compaction() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_admin_read_compaction");
        let compactions_store = Arc::new(CompactionsStore::new(&path, object_store.clone()));
        let mut stored = StoredCompactions::create(compactions_store.clone(), 0)
            .await
            .unwrap();

        let compaction_id = Ulid::new();
        let compaction = Compaction::new(
            compaction_id,
            CompactionSpec::new(vec![SourceId::SortedRun(3)], 7),
        );
        let mut dirty = stored.prepare_dirty().unwrap();
        dirty.value.insert(compaction);
        stored.update(dirty).await.unwrap();

        let admin = AdminBuilder::new(path.clone(), object_store).build();
        let compaction = admin
            .read_compaction(compaction_id, None)
            .await
            .unwrap()
            .expect("expected compaction");
        assert_eq!(compaction.id(), compaction_id);
    }

    #[tokio::test]
    async fn test_admin_submit_compaction() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("/tmp/test_admin_submit_compaction");
        let compactions_store = Arc::new(CompactionsStore::new(&path, object_store.clone()));
        StoredCompactions::create(compactions_store.clone(), 0)
            .await
            .unwrap();

        let admin = AdminBuilder::new(path.clone(), object_store).build();
        let spec = CompactionSpec::new(vec![SourceId::SortedRun(3)], 3);
        let compaction = admin.submit_compaction(spec).await.unwrap();

        assert_eq!(compaction.spec().destination(), 3);
        assert_eq!(compaction.spec().sources(), &vec![SourceId::SortedRun(3)]);
        assert_eq!(compaction.status(), CompactionStatus::Submitted);
    }

    #[tokio::test]
    async fn test_admin_fence_writers() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";

        // open db and assert that it can write.
        let db = Db::builder(path, object_store.clone())
            .build()
            .await
            .unwrap();
        db.put(b"1", b"1").await.unwrap();

        let admin = Admin::builder(path, object_store).build();

        let table_store = admin.table_store();
        let wal_id = admin.fence_writers(&table_store).await.unwrap();

        assert_eq!(table_store.last_seen_wal_id().await.unwrap(), wal_id);

        // assert that db can no longer write.
        let err = db.put(b"1", b"1").await.unwrap_err();
        assert_eq!(err.to_string(), "Closed error: detected newer DB client");
    }

    #[tokio::test]
    async fn test_admin_replay_wal_to_l0() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = "/tmp/test_kv_store";
        let admin = Admin::builder(path, object_store.clone()).build();
        let table_store = Arc::new(admin.table_store());
        let manifest_store = admin.manifest_store();

        // initialize a db to create a manifest
        let db = Db::builder(path, object_store).build().await.unwrap();
        db.close().await.unwrap();

        let row_entries = vec![
            RowEntry::new_value(b"key1", b"value1", 1).with_create_ts(11),
            RowEntry::new_value(b"key2", b"value2", 2).with_create_ts(12), // last_seq
            RowEntry::new_value(b"key3", b"value3", 3).with_create_ts(13),
        ];

        // setup wals
        let mut writer = table_store.table_writer(SsTableId::Wal(100));
        writer.add(row_entries[0].clone()).await.unwrap();
        writer.add(row_entries[1].clone()).await.unwrap();
        writer.close().await.unwrap();
        let mut writer = table_store.table_writer(SsTableId::Wal(101));
        writer.add(row_entries[2].clone()).await.unwrap();
        writer.close().await.unwrap();

        // replay the wal entries to l0. Do not include the last WAL to test that it is left out of
        // the replay results.
        let (_, manifest) = manifest_store
            .try_read_latest_manifest()
            .await
            .unwrap()
            .unwrap();
        let WalToL0Result {
            sst_handles,
            last_seq,
            last_tick,
            sequence_tracker,
        } = admin
            .replay_wal_to_l0(100..101, &manifest.core, table_store.clone(), None)
            .await
            .unwrap();

        // check that entries in wals are added to l0
        let mut row_entry_iter = row_entries.into_iter();
        for sst_handle in sst_handles.iter() {
            let mut actual_row_entry_iter = SstIterator::new_borrowed_initialized(
                ..,
                sst_handle,
                table_store.clone(),
                SstIteratorOptions::default(),
            )
            .await
            .unwrap()
            .unwrap();

            while let Some(entry) = actual_row_entry_iter.next_entry().await.unwrap() {
                assert_eq!(row_entry_iter.next().unwrap(), entry)
            }
        }

        assert_eq!(2, last_seq.unwrap());
        assert_eq!(12, last_tick.unwrap());
        assert!(sequence_tracker
            .find_ts(1, crate::seq_tracker::FindOption::RoundUp)
            .is_some(),);
    }
}
