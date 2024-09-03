use crate::compactor_executor::{CompactionExecutor, CompactionJob, TokioCompactionExecutor};
use crate::compactor_state::{Compaction, SourceId};
use crate::config::CompactorOptions;
use crate::db_state::{SSTableHandle, SsTableId};
use crate::error::SlateDBError;
use crate::manifest_store::{ManifestStore, StoredManifest};
use crate::sst::SsTableFormat;
use crate::tablestore::TableStore;
use crate::test_utils::OrderedBytesGenerator;
use crate::{compactor::WorkerToOrchestratorMsg, config::CompressionCodec};
use bytes::BufMut;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use rand::{RngCore, SeedableRng};
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::{error, info};
use ulid::Ulid;

#[derive(Debug)]
struct Options {
    aws_key: String,
    aws_secret: String,
    bucket: String,
    region: String,
    path: String,
    mode: String,
    sst_bytes: usize,
    num_ssts: usize,
    key_bytes: usize,
    val_bytes: usize,
    compaction: Option<Compaction>,
    compression_codec: Option<CompressionCodec>,
}

#[cfg(feature = "aws")]
fn open_s3(options: &Options) -> Result<Arc<dyn ObjectStore>, SlateDBError> {
    Ok(Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_access_key_id(options.aws_key.as_str())
            .with_secret_access_key(options.aws_secret.as_str())
            .with_bucket_name(options.bucket.as_str())
            .with_region(options.region.as_str())
            .build()?,
    ))
}

#[cfg(not(feature = "aws"))]
fn open_s3(options: &Options) -> Result<Arc<dyn ObjectStore>, SlateDBError> {
    panic!("compaction bench requires feature s3")
}

fn open_object_store(options: &Options) -> Result<Arc<dyn ObjectStore>, SlateDBError> {
    open_s3(options)
}

#[allow(clippy::panic)]
pub fn run_compaction_execute_bench() -> Result<(), SlateDBError> {
    let options = load_options();
    let s3 = open_object_store(&options)?;
    let sst_format = SsTableFormat::new(4096, 1, options.compression_codec);
    let table_store = Arc::new(TableStore::new(
        s3.clone(),
        sst_format,
        Path::from(options.path.as_str()),
        None,
    ));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    match options.mode.as_str() {
        "RUN" => run_bench(&options, runtime.handle().clone(), table_store, s3.clone()),
        "LOAD" => runtime.block_on(run_load(&options, table_store)),
        "CLEAR" => run_clear(&options, runtime.handle().clone(), s3.clone()),
        invalid => panic!("invalid mode: {}", invalid),
    }
}

fn sst_id(id: u32) -> SsTableId {
    SsTableId::Compacted(Ulid::from((id as u64, id as u64)))
}

async fn run_load(options: &Options, table_store: Arc<TableStore>) -> Result<(), SlateDBError> {
    let num_ssts = options.num_ssts as u32;
    let sst_bytes = options.sst_bytes;
    let key_bytes = options.key_bytes;
    let val_bytes = options.val_bytes;
    let num_keys = sst_bytes / (val_bytes + key_bytes);
    let mut key_start = vec![0u8; key_bytes - mem::size_of::<u32>()];
    let mut rng = rand_xorshift::XorShiftRng::from_entropy();
    rng.fill_bytes(key_start.as_mut_slice());
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
        let jh = tokio::spawn(load_sst(i, ts, key_start_copy, num_keys, val_bytes));
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
) -> Result<(), SlateDBError> {
    let mut retries = 0;
    loop {
        let result = do_load_sst(
            i,
            table_store.clone(),
            key_start.clone(),
            num_keys,
            val_bytes,
        )
        .await;
        match result {
            Ok(()) => return Ok(()),
            Err(err) => {
                if retries >= 3 {
                    return Err(err);
                } else {
                    error!("error loading sst: {:?}", err)
                }
            }
        }
        retries += 1;
        tokio::time::sleep(Duration::from_secs(retries + 1)).await;
    }
}

async fn do_load_sst(
    i: u32,
    table_store: Arc<TableStore>,
    key_start: Vec<u8>,
    num_keys: usize,
    val_bytes: usize,
) -> Result<(), SlateDBError> {
    let mut rng = rand_xorshift::XorShiftRng::from_entropy();
    let start = std::time::Instant::now();
    let mut suffix = Vec::<u8>::new();
    suffix.put_u32(i);
    let mut key_gen = OrderedBytesGenerator::new_with_suffix(suffix.as_ref(), key_start.as_slice());
    let mut sst_writer = table_store.table_writer(sst_id(i));
    for _ in 0..num_keys {
        let mut val = vec![0u8; val_bytes];
        rng.fill_bytes(val.as_mut_slice());
        let key = key_gen.next();
        sst_writer.add(key.as_ref(), Some(val.as_ref())).await?;
    }
    let encoded = sst_writer.close().await?;
    info!("wrote sst with id: {:?} {:?}", &encoded.id, start.elapsed());
    Ok(())
}

fn run_clear(
    options: &Options,
    handle: tokio::runtime::Handle,
    s3: Arc<dyn ObjectStore>,
) -> Result<(), SlateDBError> {
    let mut del_tasks = Vec::new();
    for i in 0u32..options.num_ssts as u32 {
        let s3_copy = s3.clone();
        let path = options.path.clone();
        del_tasks.push(handle.spawn(async move {
            let sst_id = sst_id(i);
            s3_copy.delete(&sst_path(&sst_id, path.as_str())).await
        }))
    }
    while let Some(del_task) = del_tasks.pop() {
        handle.block_on(del_task).expect("join failed")?;
    }
    Ok(())
}

fn load_compaction_job(
    options: &Options,
    handle: &Handle,
    table_store: &Arc<TableStore>,
) -> Result<CompactionJob, SlateDBError> {
    let sst_ids: Vec<SsTableId> = (0u32..options.num_ssts as u32).map(sst_id).collect();
    let mut futures =
        FuturesUnordered::<JoinHandle<Result<(SsTableId, SSTableHandle), SlateDBError>>>::new();
    let mut ssts_by_id = HashMap::new();
    info!("load sst");
    for id in sst_ids.clone().into_iter() {
        if futures.len() > 8 {
            let (id, handle) = handle
                .block_on(futures.next())
                .expect("expected join handle")
                .expect("join failed")?;
            ssts_by_id.insert(id, handle);
        }
        let table_store_clone = table_store.clone();
        let jh = handle.spawn(async move {
            match table_store_clone.open_sst(&id).await {
                Ok(h) => Ok((id, h)),
                Err(err) => Err(err),
            }
        });
        futures.push(jh);
    }
    while let Some(jh) = handle.block_on(futures.next()) {
        let (id, handle) = jh.expect("join failed")?;
        ssts_by_id.insert(id, handle);
    }
    info!("finished loading");
    let ssts: Vec<SSTableHandle> = sst_ids
        .into_iter()
        .map(|id| ssts_by_id.get(&id).expect("expected sst").clone())
        .collect();
    Ok(CompactionJob {
        destination: 0,
        ssts,
        sorted_runs: vec![],
    })
}

fn load_compaction_as_job(manifest: StoredManifest, compaction: &Compaction) -> CompactionJob {
    let state = manifest.db_state();
    let srs_by_id: HashMap<_, _> = state
        .compacted
        .iter()
        .map(|sr| (sr.id, sr.clone()))
        .collect();
    let srs: Vec<_> = compaction
        .sources
        .iter()
        .map(|sr| {
            srs_by_id
                .get(&sr.unwrap_sorted_run())
                .expect("expected src")
                .clone()
        })
        .collect();
    info!("loaded compaction job");
    CompactionJob {
        destination: 0,
        ssts: vec![],
        sorted_runs: srs,
    }
}

fn run_bench(
    options: &Options,
    handle: tokio::runtime::Handle,
    table_store: Arc<TableStore>,
    os: Arc<dyn ObjectStore>,
) -> Result<(), SlateDBError> {
    let (tx, rx) = crossbeam_channel::unbounded();
    let compactor_options = CompactorOptions::default();
    let executor = TokioCompactionExecutor::new(
        handle.clone(),
        Arc::new(compactor_options),
        tx,
        table_store.clone(),
    );
    info!("load compaction job");
    let job = match &options.compaction {
        Some(compaction) => {
            info!("load job from existing compaction");
            let path = Path::from(options.path.as_str());
            let manifest_store = Arc::new(ManifestStore::new(&path, os.clone()));
            let manifest = handle
                .block_on(StoredManifest::load(manifest_store))?
                .expect("expected manifest to be present");
            load_compaction_as_job(manifest, compaction)
        }
        None => load_compaction_job(options, &handle, &table_store)?,
    };
    let start = std::time::Instant::now();
    info!("start compaction job");
    executor.start_compaction(job);
    let WorkerToOrchestratorMsg::CompactionFinished(result) = rx.recv().expect("recv failed");
    match result {
        Ok(_) => {
            info!("compaction finished in {:?} millis", start.elapsed());
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

fn load_options() -> Options {
    let aws_key = std::env::var("AWS_ACCESS_KEY_ID").expect("must supply AWS access key");
    let aws_secret = std::env::var("AWS_SECRET_ACCESS_KEY").expect("must supply AWS secret");
    let bucket = std::env::var("BUCKET").expect("must supply bucket name");
    let region = std::env::var("REGION")
        .ok()
        .unwrap_or(String::from("us-west-2"));
    let path = std::env::var("SST_BASE_PATH")
        .ok()
        .unwrap_or(String::from("/compaction-execute-bench"));
    let mode = std::env::var("MODE").expect("must specify LOAD, RUN, or CLEAR for MODE");
    let sst_bytes = std::env::var("SST_BYTES")
        .ok()
        .unwrap_or(String::from("1073741824"));
    let num_ssts = std::env::var("NUM_SSTS").ok().unwrap_or(String::from("4"));
    let key_bytes = std::env::var("KEY_BYTES")
        .ok()
        .unwrap_or(String::from("32"));
    let val_bytes = std::env::var("VAL_BYTES")
        .ok()
        .unwrap_or(String::from("224"));
    let compression_codec = std::env::var("COMPRESSION_CODEC").ok().map(|codec| {
        codec
            .parse::<CompressionCodec>()
            .expect("invalid compression codec")
    });
    let compaction = std::env::var("COMPACTION").ok().map(parse_compaction);

    let options = Options {
        aws_key,
        aws_secret,
        bucket,
        region,
        path,
        mode,
        sst_bytes: sst_bytes.parse::<usize>().expect("invalid sst bytes"),
        num_ssts: num_ssts.parse::<usize>().expect("invalid num ssts"),
        key_bytes: key_bytes.parse::<usize>().expect("invalid key bytes"),
        val_bytes: val_bytes.parse::<usize>().expect("invalid val bytes"),
        compaction,
        compression_codec,
    };
    info!("Options: {:?}", options);
    options
}

fn parse_compaction(as_str: String) -> Compaction {
    let parts = as_str.split(',');
    let srs: Vec<_> = parts
        .map(|p| SourceId::SortedRun(p.parse::<u32>().expect("invalid sr id")))
        .collect();
    let dst = srs.last().expect("no srs provided").unwrap_sorted_run();
    Compaction::new(srs, dst)
}

#[allow(clippy::panic)]
fn sst_path(id: &SsTableId, root_path: &str) -> Path {
    match id {
        SsTableId::Compacted(ulid) => {
            Path::from(format!("{}/compacted/{}.sst", root_path, ulid.to_string()))
        }
        _ => panic!("invalid sst type"),
    }
}
