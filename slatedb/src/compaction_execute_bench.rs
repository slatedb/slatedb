use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use bytes::BufMut;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{error, info};
use object_store::path::Path;
use object_store::ObjectStore;
use rand::RngCore;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use ulid::Ulid;

use crate::bytes_generator::OrderedBytesGenerator;
use crate::clock::{DefaultSystemClock, SystemClock};
use crate::compactor::stats::CompactionStats;
use crate::compactor::CompactorMessage;
use crate::compactor_executor::{
    CompactionExecutor, StartCompactionJobArgs, TokioCompactionExecutor,
};
use crate::compactor_state::{Compaction, CompactionSpec, SourceId};
use crate::config::{CompactorOptions, CompressionCodec};
use crate::db_state::{SsTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::object_stores::ObjectStores;
use crate::rand::DbRand;
use crate::sst::SsTableFormat;
use crate::stats::StatRegistry;
use crate::tablestore::TableStore;
use crate::types::RowEntry;
use crate::types::ValueDeletable;
use crate::utils::IdGenerator;

pub struct CompactionExecuteBench {
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    rand: Arc<DbRand>,
    system_clock: Arc<dyn SystemClock>,
}

impl CompactionExecuteBench {
    pub fn new(path: Path, object_store: Arc<dyn ObjectStore>) -> Self {
        Self::new_with_rand(path, object_store, Arc::new(DbRand::default()))
    }

    fn new_with_rand(path: Path, object_store: Arc<dyn ObjectStore>, rand: Arc<DbRand>) -> Self {
        Self {
            path,
            object_store,
            rand,
            system_clock: Arc::new(DefaultSystemClock::new()),
        }
    }

    fn sst_id(id: u32) -> SsTableId {
        SsTableId::Compacted(Ulid::from((id as u64, id as u64)))
    }

    pub async fn run_load(
        &self,
        num_ssts: usize,
        sst_bytes: usize,
        key_bytes: usize,
        val_bytes: usize,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<(), crate::Error> {
        let sst_format = SsTableFormat {
            compression_codec,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(self.object_store.clone(), None),
            sst_format,
            self.path.clone(),
            None,
        ));
        let num_keys = sst_bytes / (val_bytes + key_bytes);
        let mut key_start = vec![0u8; key_bytes - mem::size_of::<u32>()];
        self.rand.rng().fill_bytes(key_start.as_mut_slice());
        let mut futures = FuturesUnordered::<JoinHandle<Result<(), SlateDBError>>>::new();
        for i in 0..num_ssts {
            while futures.len() >= 4 {
                futures
                    .next()
                    .await
                    .expect("expected value")
                    .expect("join failed")?;
            }
            let ts = table_store.clone();
            let key_start_copy = key_start.clone();
            let jh = tokio::spawn(CompactionExecuteBench::load_sst(
                i as u32,
                ts,
                key_start_copy,
                num_keys,
                val_bytes,
                self.rand.clone(),
                self.system_clock.clone(),
            ));
            futures.push(jh)
        }
        while !futures.is_empty() {
            futures
                .next()
                .await
                .expect("expected value")
                .expect("join failed")?;
        }
        Ok(())
    }

    async fn load_sst(
        i: u32,
        table_store: Arc<TableStore>,
        key_start: Vec<u8>,
        num_keys: usize,
        val_bytes: usize,
        rand: Arc<DbRand>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<(), SlateDBError> {
        let mut retries = 0;
        loop {
            let result = CompactionExecuteBench::do_load_sst(
                i,
                table_store.clone(),
                key_start.clone(),
                num_keys,
                val_bytes,
                system_clock.clone(),
                rand.clone(),
            )
            .await;
            match result {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if retries >= 3 {
                        return Err(err);
                    } else {
                        error!("error loading sst [retry={}]: {:?}", retries, err)
                    }
                }
            }
            retries += 1;
            system_clock
                .clone()
                .sleep(Duration::from_secs(retries + 1))
                .await;
        }
    }

    async fn do_load_sst(
        i: u32,
        table_store: Arc<TableStore>,
        key_start: Vec<u8>,
        num_keys: usize,
        val_bytes: usize,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
    ) -> Result<(), SlateDBError> {
        let start = system_clock.now();
        let mut suffix = Vec::<u8>::new();
        suffix.put_u32(i);
        let mut key_gen =
            OrderedBytesGenerator::new_with_suffix(suffix.as_ref(), key_start.as_slice());
        let mut sst_writer = table_store.table_writer(CompactionExecuteBench::sst_id(i));
        for _ in 0..num_keys {
            let mut val = vec![0u8; val_bytes];
            rand.rng().fill_bytes(val.as_mut_slice());
            let key = key_gen.next();
            let row_entry = RowEntry::new(key, ValueDeletable::Value(val.into()), 0, None, None);
            sst_writer.add(row_entry).await?;
        }
        let sst = sst_writer.close().await?;
        let elapsed_ms = system_clock
            .now()
            .signed_duration_since(start)
            .num_milliseconds();
        info!("wrote sst [id={:?}, elapsed_ms={}]", &sst.id, elapsed_ms);
        Ok(())
    }

    #[allow(clippy::panic)]
    pub async fn run_clear(&self, num_ssts: usize) -> Result<(), crate::Error> {
        let mut del_tasks = Vec::new();
        for i in 0u32..num_ssts as u32 {
            let os = self.object_store.clone();
            let path = self.path.clone();
            del_tasks.push(tokio::spawn(async move {
                let sst_id = CompactionExecuteBench::sst_id(i);
                os.delete(&CompactionExecuteBench::sst_path(&sst_id, &path))
                    .await
            }))
        }
        let results = futures::future::join_all(del_tasks).await;
        for result in results {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(SlateDBError::from(err).into()),
                Err(err) => panic!("task failed [error={:?}]", err),
            }
        }
        Ok(())
    }

    async fn load_compaction_job(
        manifest: &StoredManifest,
        num_ssts: usize,
        table_store: &Arc<TableStore>,
        is_dest_last_run: bool,
        rand: Arc<DbRand>,
        system_clock: Arc<dyn SystemClock>,
    ) -> Result<StartCompactionJobArgs, SlateDBError> {
        let sst_ids: Vec<SsTableId> = (0u32..num_ssts as u32)
            .map(CompactionExecuteBench::sst_id)
            .collect();
        let mut futures =
            FuturesUnordered::<JoinHandle<Result<(SsTableId, SsTableHandle), SlateDBError>>>::new();
        let mut ssts_by_id = HashMap::new();
        info!("load sst");
        for id in sst_ids.clone().into_iter() {
            if futures.len() > 8 {
                let (id, handle) = futures
                    .next()
                    .await
                    .expect("missing join handle")
                    .expect("join failed")?;
                ssts_by_id.insert(id, handle);
            }
            let table_store_clone = table_store.clone();
            let jh = tokio::spawn(async move {
                match table_store_clone.open_sst(&id).await {
                    Ok(h) => Ok((id, h)),
                    Err(err) => Err(err),
                }
            });
            futures.push(jh);
        }
        while let Some(jh) = futures.next().await {
            let (id, handle) = jh.expect("join failed")?;
            ssts_by_id.insert(id, handle);
        }
        info!("finished loading");
        let ssts: Vec<SsTableHandle> = sst_ids
            .into_iter()
            .map(|id| ssts_by_id.get(&id).expect("expected sst").clone())
            .collect();
        Ok(StartCompactionJobArgs {
            id: rand.rng().gen_ulid(system_clock.as_ref()),
            compaction_id: rand.rng().gen_ulid(system_clock.as_ref()),
            destination: 0,
            ssts,
            sorted_runs: vec![],
            compaction_logical_clock_tick: manifest.db_state().last_l0_clock_tick,
            retention_min_seq: Some(manifest.db_state().recent_snapshot_min_seq),
            is_dest_last_run,
        })
    }

    fn load_compaction_as_job_args(
        manifest: &StoredManifest,
        job: &Compaction,
        is_dest_last_run: bool,
        rand: Arc<DbRand>,
        system_clock: Arc<dyn SystemClock>,
    ) -> StartCompactionJobArgs {
        let state = manifest.db_state();
        let spec = job.spec();
        let srs_by_id: HashMap<_, _> = state
            .compacted
            .iter()
            .map(|sr| (sr.id, sr.clone()))
            .collect();
        let srs: Vec<_> = spec
            .sources()
            .iter()
            .map(|sr| {
                srs_by_id
                    .get(&sr.unwrap_sorted_run())
                    .expect("expected src")
                    .clone()
            })
            .collect();
        info!("loaded compaction job");
        StartCompactionJobArgs {
            id: rand.rng().gen_ulid(system_clock.as_ref()),
            compaction_id: job.id(),
            destination: 0,
            ssts: vec![],
            sorted_runs: srs,
            compaction_logical_clock_tick: state.last_l0_clock_tick,
            retention_min_seq: Some(state.recent_snapshot_min_seq),
            is_dest_last_run,
        }
    }

    pub async fn run_bench(
        &self,
        num_ssts: usize,
        source_sr_ids: Option<Vec<u32>>,
        destination_sr_id: u32,
        compression_codec: Option<CompressionCodec>,
    ) -> Result<(), crate::Error> {
        let sst_format = SsTableFormat {
            compression_codec,
            ..SsTableFormat::default()
        };
        let table_store = Arc::new(TableStore::new(
            ObjectStores::new(self.object_store.clone(), None),
            sst_format,
            self.path.clone(),
            None,
        ));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let compactor_options = CompactorOptions::default();
        let registry = Arc::new(StatRegistry::new());
        let stats = Arc::new(CompactionStats::new(registry.clone()));
        let os = self.object_store.clone();

        let manifest_store = Arc::new(ManifestStore::new(
            &self.path,
            os.clone(),
            self.system_clock.clone(),
        ));

        let executor = TokioCompactionExecutor::new(
            Handle::current(),
            Arc::new(compactor_options),
            tx,
            table_store.clone(),
            self.rand.clone(),
            stats.clone(),
            self.system_clock.clone(),
            manifest_store.clone(),
            None,
        );

        let manifest = StoredManifest::load(manifest_store).await?;

        let sources: Vec<SourceId> = source_sr_ids
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(SourceId::SortedRun)
            .collect();

        let compactor_job = source_sr_ids.map(|_source_sr_ids| {
            let id = self.rand.rng().gen_ulid(self.system_clock.as_ref());
            let spec = CompactionSpec::new(sources, destination_sr_id);
            Compaction::new(id, spec)
        });

        info!("load compaction job");
        let job = match &compactor_job {
            Some(compactor_job) => {
                info!("load job from existing compaction");
                CompactionExecuteBench::load_compaction_as_job_args(
                    &manifest,
                    compactor_job,
                    false,
                    self.rand.clone(),
                    self.system_clock.clone(),
                )
            }
            None => {
                CompactionExecuteBench::load_compaction_job(
                    &manifest,
                    num_ssts,
                    &table_store,
                    false,
                    self.rand.clone(),
                    self.system_clock.clone(),
                )
                .await?
            }
        };
        let start = self.system_clock.now();
        info!("start compaction job");
        #[allow(clippy::disallowed_methods)]
        tokio::task::spawn_blocking(move || executor.start_compaction_job(job));
        while let Some(msg) = rx.recv().await {
            if let CompactorMessage::CompactionJobFinished { id: _, result } = msg {
                match result {
                    Ok(_) => {
                        let elapsed_ms = self
                            .system_clock
                            .now()
                            .signed_duration_since(start)
                            .num_milliseconds();
                        info!("compaction finished [elapsed_ms={}]", elapsed_ms);
                    }
                    Err(err) => return Err(err.into()),
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::panic)]
    fn sst_path(id: &SsTableId, root_path: &Path) -> Path {
        match id {
            SsTableId::Compacted(ulid) => {
                Path::from(format!("{}/compacted/{}.sst", root_path, ulid.to_string()))
            }
            _ => panic!("invalid sst type"),
        }
    }
}
