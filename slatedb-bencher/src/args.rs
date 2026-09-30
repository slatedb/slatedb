use std::{
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use clap::{builder::PossibleValue, Args, Parser, Subcommand, ValueEnum};
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
};
use object_store::ObjectStore;
use slatedb::{
    config::{CompressionCodec, MultiGetOptions, Settings},
    db_cache::{
        foyer::{FoyerCache, FoyerCacheOptions},
        foyer_hybrid::FoyerHybridCache,
        CachedEntry, DbCache, SplitCache,
    },
    Error, IsolationLevel,
};
use tracing::info;

use crate::bench_object_store::{BenchObjectStore, DelayProfile};
use crate::db::{FixedSetKeyGenerator, KeyGenerator, RandomKeyGenerator};

#[derive(Parser, Clone)]
#[command(name = "bencher")]
#[command(version = "0.1.0")]
#[command(about = "A benchmark tool for SlateDB.")]
pub(crate) struct BencherArgs {
    #[arg(
        short,
        long,
        help = "A .env file to use to supply environment variables. `CLOUD_PROVIDER` must be set."
    )]
    pub(crate) env_file: Option<String>,

    #[arg(
        short,
        long,
        help = "The path in the object store to the root directory, starting from within the object store bucket.",
        default_value = "/slatedb-bencher"
    )]
    pub(crate) path: String,

    #[arg(
        long,
        help = "Clean up object storage files after the benchmark run completes",
        default_value_t = false
    )]
    pub(crate) clean: bool,

    #[command(subcommand)]
    pub(crate) command: BencherCommands,
}

#[derive(Subcommand, Clone)]
pub(crate) enum BencherCommands {
    Db(BenchmarkDbArgs),
    Compaction(BenchmarkCompactionArgs),
    Transaction(BenchmarkTransactionArgs),
}

#[derive(Args, Clone)]
pub(crate) struct DbArgs {
    #[arg(
        long,
        help = "Optional path to load the configuration from. `Slatedb.toml` is used by default if this option is not present"
    )]
    db_options_path: Option<PathBuf>,

    #[arg(long, help = "The size in bytes of the block cache.")]
    pub(crate) block_cache_size: Option<u64>,

    #[arg(long, help = "The size in bytes of the meta cache.")]
    pub(crate) meta_cache_size: Option<u64>,

    #[arg(
        long,
        help = "Use unified cache, we will use `block_cache_size` as unified cache size"
    )]
    pub(crate) unified_cache: bool,

    #[arg(
        long,
        requires = "hybrid_cache_size",
        help = "Give the block cache and the meta cache a disk tier under this directory. A lookup that misses the memory tier reads the disk before the object store. The memory tiers keep the sizes of --block-cache-size and --meta-cache-size."
    )]
    pub(crate) hybrid_cache_path: Option<PathBuf>,

    #[arg(
        long,
        requires = "hybrid_cache_path",
        help = "The size in bytes of the disk tier of the block cache. Needs --hybrid-cache-path."
    )]
    pub(crate) hybrid_cache_size: Option<u64>,

    #[arg(
        long,
        requires = "hybrid_cache_path",
        help = "The size in bytes of the disk tier of the meta cache. One eighth of --hybrid-cache-size by default."
    )]
    pub(crate) hybrid_meta_cache_size: Option<u64>,

    #[arg(
        long,
        requires = "hybrid_cache_path",
        help = "The block size in bytes of the disk tier. One read of the disk tier reads one block, so a large block over small SST blocks reads more bytes than it needs. 65536 by default."
    )]
    pub(crate) hybrid_cache_block_size: Option<usize>,

    #[arg(
        long,
        help = "Keep the object store disk cache from the configuration file. By default the benchmark disables it, so reads go to the object store.",
        default_value_t = false
    )]
    pub(crate) disk_cache: bool,

    #[arg(
        long,
        requires = "disk_cache",
        help = "The size in bytes of the object store disk cache. Needs --disk-cache."
    )]
    pub(crate) disk_cache_size: Option<usize>,

    #[arg(
        long,
        requires = "disk_cache",
        help = "The size in bytes of one part file of the object store disk cache. Needs --disk-cache."
    )]
    pub(crate) disk_cache_part_size: Option<usize>,

    #[arg(
        long,
        value_enum,
        help = "Add a delay to each object store read, sampled from a measured latency profile.",
        default_value_t = DelayProfileKind::None
    )]
    pub(crate) delay_profile: DelayProfileKind,

    #[arg(
        long,
        help = "Add a fixed delay in milliseconds to each object store read. Overrides --delay-profile."
    )]
    pub(crate) delay_ms: Option<u64>,
}

/// The two caches that the table store reads: one for data blocks, and one
/// for the filters and the indexes.
struct ReadCaches {
    block: Option<Arc<dyn DbCache>>,
    meta: Option<Arc<dyn DbCache>>,
}

impl ReadCaches {
    /// Join the two caches into the one cache that the database takes.
    fn join(self) -> Arc<dyn DbCache> {
        Arc::new(
            SplitCache::new()
                .with_block_cache(self.block)
                .with_meta_cache(self.meta)
                .build(),
        )
    }
}

/// The memory tier of a hybrid cache when its size argument is absent.
const DEFAULT_HYBRID_MEMORY_BYTES: u64 = 64 * 1024 * 1024;

/// The block size of the disk tier of a hybrid cache.
const DEFAULT_HYBRID_BLOCK_SIZE: usize = 64 * 1024;

impl DbArgs {
    /// Returns a `(Settings, Option<Arc<dyn DbCache>>)` struct based on DbArgs's arguments.
    pub(crate) async fn config(&self) -> Result<(Settings, Option<Arc<dyn DbCache>>), Error> {
        let mut settings = if let Some(path) = &self.db_options_path {
            Settings::from_file(path)?
        } else {
            Settings::load()?
        };
        if self.disk_cache {
            if let Some(size) = self.disk_cache_size {
                settings.object_store_cache_options.max_cache_size_bytes = Some(size);
            }
            if let Some(size) = self.disk_cache_part_size {
                settings.object_store_cache_options.part_size_bytes = size;
            }
        } else {
            settings.object_store_cache_options.root_folder = None;
        }

        let caches = match &self.hybrid_cache_path {
            Some(root) => self.hybrid_caches(root).await,
            None => self.memory_caches(),
        };

        Ok((settings, Some(caches.join())))
    }

    /// The block cache and the meta cache in memory.
    fn memory_caches(&self) -> ReadCaches {
        let block_cache = self.block_cache_size.map(|capacity| {
            Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: capacity,
                ..Default::default()
            })) as Arc<dyn DbCache>
        });

        // If we use unified cache, we will increase `block_cache` reference count
        let meta_cache = if self.unified_cache {
            block_cache.as_ref().map(|cache| cache.clone())
        } else {
            self.meta_cache_size.map(|capacity| {
                Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                    max_capacity: capacity,
                    ..Default::default()
                })) as Arc<dyn DbCache>
            })
        };
        ReadCaches {
            block: block_cache,
            meta: meta_cache,
        }
    }

    /// The block cache and the meta cache with a memory tier and a disk tier
    /// under `root`. Each cache gets its own directory and its own device.
    async fn hybrid_caches(&self, root: &std::path::Path) -> ReadCaches {
        let memory = self.block_cache_size.unwrap_or(DEFAULT_HYBRID_MEMORY_BYTES);
        let disk = self
            .hybrid_cache_size
            .expect("--hybrid-cache-path needs --hybrid-cache-size");
        if self.unified_cache {
            let cache = self
                .hybrid_cache("bencher_unified", root.join("unified"), memory, disk)
                .await;
            return ReadCaches {
                block: Some(cache.clone()),
                meta: Some(cache),
            };
        }
        let block = self
            .hybrid_cache("bencher_block", root.join("block"), memory, disk)
            .await;
        let meta_memory = self.meta_cache_size.unwrap_or(DEFAULT_HYBRID_MEMORY_BYTES);
        let meta_disk = self.hybrid_meta_cache_size.unwrap_or(disk / 8);
        let meta = self
            .hybrid_cache("bencher_meta", root.join("meta"), meta_memory, meta_disk)
            .await;
        ReadCaches {
            block: Some(block),
            meta: Some(meta),
        }
    }

    /// One cache with a memory tier of `memory_bytes` and a disk tier of
    /// `disk_bytes` in `dir`. The disk tier keeps its data between runs, so a
    /// second run over the same directory starts warm.
    async fn hybrid_cache(
        &self,
        name: &'static str,
        dir: PathBuf,
        memory_bytes: u64,
        disk_bytes: u64,
    ) -> Arc<dyn DbCache> {
        std::fs::create_dir_all(&dir).expect("failed to create the hybrid cache directory");
        let device = FsDeviceBuilder::new(&dir)
            .with_capacity(disk_bytes as usize)
            .build()
            .expect("failed to open the hybrid cache device");
        let block_size = self
            .hybrid_cache_block_size
            .unwrap_or(DEFAULT_HYBRID_BLOCK_SIZE);
        let cache = HybridCacheBuilder::new()
            .with_name(name)
            .memory(memory_bytes as usize)
            .with_weighter(|_, value: &CachedEntry| value.size())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(BlockEngineConfig::new(device).with_block_size(block_size))
            .build()
            .await
            .expect("failed to build the hybrid cache");
        info!(
            name,
            memory_bytes,
            disk_bytes,
            block_size,
            dir = %dir.display(),
            "hybrid cache ready"
        );
        Arc::new(FoyerHybridCache::new_with_cache(cache))
    }

    /// Wraps the object store so the benchmark can count and delay reads.
    pub(crate) fn wrap_store(&self, store: Arc<dyn ObjectStore>) -> Arc<BenchObjectStore> {
        let profile = match (self.delay_ms, &self.delay_profile) {
            (Some(millis), _) => Some(DelayProfile::fixed(millis)),
            (None, DelayProfileKind::None) => None,
            (None, DelayProfileKind::S3) => Some(DelayProfile::s3()),
            (None, DelayProfileKind::S3x) => Some(DelayProfile::s3_express()),
        };
        Arc::new(BenchObjectStore::new(store, profile))
    }
}

#[derive(Clone, Debug, ValueEnum)]
pub(crate) enum DelayProfileKind {
    /// No delay.
    None,
    /// S3 in one region.
    S3,
    /// S3 Express One Zone.
    S3x,
}

impl Display for DelayProfileKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            DelayProfileKind::None => "none",
            DelayProfileKind::S3 => "s3",
            DelayProfileKind::S3x => "s3x",
        };
        write!(f, "{name}")
    }
}

#[derive(Args, Clone)]
#[command(about = "Benchmark a SlateDB database.")]
pub(crate) struct BenchmarkDbArgs {
    #[clap(flatten)]
    pub(crate) db_args: DbArgs,

    #[arg(long, help = "The duration in seconds to run the benchmark for.")]
    pub(crate) duration: Option<u32>,

    #[arg(
        long,
        help = "The key generator to use.",
        default_value_t = KeyGeneratorType::FixedSet
    )]
    pub(crate) key_generator: KeyGeneratorType,

    #[arg(
        long,
        help = "The number of keys to generate for FixedSet key generator.",
        default_value_t = 100_000
    )]
    pub(crate) key_count: u64,

    #[arg(
        long,
        help = "The length of the keys to generate.",
        default_value_t = 16
    )]
    pub(crate) key_len: usize,

    #[arg(
        long,
        help = "Whether to await durable writes.",
        default_value_t = false
    )]
    pub(crate) await_durable: bool,

    #[arg(long, help = "The number of read/write to spawn.", default_value_t = 4)]
    pub(crate) concurrency: u32,

    #[arg(long, help = "The number of rows to write.")]
    pub(crate) num_rows: Option<u64>,

    #[arg(
        long,
        help = "The length of the values to generate.",
        default_value_t = 1024
    )]
    pub(crate) val_len: usize,

    #[arg(
        long,
        help = "The percentage of writes to perform in each task.",
        default_value_t = 20
    )]
    pub(crate) put_percentage: u32,

    #[arg(
        long,
        help = "The percentage of gets that will return a value.",
        default_value_t = 95
    )]
    pub(crate) get_hit_percentage: u32,

    #[arg(
        long,
        help = "Disable the embedded compactor. Use when running a standalone coordinator and workers.",
        default_value_t = false
    )]
    pub(crate) no_compactor: bool,

    #[arg(
        long,
        help = "Seed for the key generators. Two runs with the same seed and key count use the same fixed key set."
    )]
    pub(crate) seed: Option<u64>,

    #[arg(
        long,
        help = "Wait until compaction settles before the database closes.",
        default_value_t = false
    )]
    pub(crate) wait_compaction: bool,

    #[arg(
        long,
        help = "FixedSet covers the whole set: a load writes each key one time, and a read run picks from all keys.",
        default_value_t = false
    )]
    pub(crate) all_keys: bool,

    #[arg(
        long,
        help = "With --all-keys, a read run picks keys with a Zipf distribution of this exponent instead of a uniform one."
    )]
    pub(crate) zipf: Option<f64>,

    #[command(subcommand)]
    pub(crate) mode: Option<DbMode>,
}

/// The read mode of the `db` benchmark. No mode is a loop of single `get` calls.
#[derive(Subcommand, Clone)]
pub(crate) enum DbMode {
    /// Read keys in batches with `multi_get` or with a loop of `get` calls.
    Mget(MgetArgs),
}

#[derive(Args, Clone)]
pub(crate) struct MgetArgs {
    #[arg(
        long,
        value_enum,
        help = "How a batch is read.",
        default_value_t = Reader::MultiGet
    )]
    pub(crate) reader: Reader,

    #[arg(long, help = "The number of keys in one batch.", default_value_t = 100)]
    pub(crate) batch_size: usize,

    #[arg(
        long,
        help = "Max object store requests in flight for one batch. The `concurrent` reader runs this many gets at once.",
        default_value_t = 256
    )]
    pub(crate) read_concurrency: usize,

    #[arg(
        long,
        help = "Max known-positive SSTs that a key reads in the second round and later.",
        default_value_t = 4
    )]
    pub(crate) lookahead: usize,

    #[arg(
        long,
        help = "Two blocks go into one ranged GET when the gap between them is at most this many bytes.",
        default_value_t = 64 * 1024
    )]
    pub(crate) coalesce_gap_bytes: usize,

    #[arg(
        long,
        help = "Upper size of one merged ranged GET.",
        default_value_t = 512 * 1024
    )]
    pub(crate) max_coalesced_bytes: usize,
}

impl MgetArgs {
    pub(crate) fn mget_options(&self) -> MultiGetOptions {
        MultiGetOptions::new()
            .with_max_fetch_tasks(self.read_concurrency)
            .with_lookahead(self.lookahead)
            .with_coalesce_gap_bytes(self.coalesce_gap_bytes)
            .with_max_coalesced_bytes(self.max_coalesced_bytes)
    }
}

/// How the `mget` mode reads one batch of keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Reader {
    /// One `get` after the other.
    Seq,
    /// Up to `read_concurrency` gets at once.
    Concurrent,
    /// One `multi_get` call.
    MultiGet,
}

impl Display for Reader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Reader::Seq => "seq",
            Reader::Concurrent => "concurrent",
            Reader::MultiGet => "multi-get",
        };
        write!(f, "{name}")
    }
}

/// Trait for types that can supply key generators
pub(crate) trait KeyGeneratorSupplier {
    fn key_generator(&self) -> KeyGeneratorType;
    fn key_len(&self) -> usize;
    fn key_count(&self) -> u64;
    fn seed(&self) -> Option<u64> {
        None
    }

    /// The number of tasks that share the key set. See [`Self::all_keys`].
    fn concurrency(&self) -> u32 {
        1
    }

    /// When true, the fixed set generator walks the whole set. See
    /// [`FixedSetKeyGenerator::walk`].
    fn all_keys(&self) -> bool {
        false
    }

    /// The Zipf exponent of the read picks of a walk, or `None` for uniform
    /// picks. See [`FixedSetKeyGenerator::with_zipf`].
    fn zipf(&self) -> Option<f64> {
        None
    }

    fn key_gen_supplier(&self) -> Box<dyn Fn() -> Box<dyn KeyGenerator>> {
        let key_len = self.key_len();
        let key_count = self.key_count();
        let seed = self.seed();
        let all_keys = self.all_keys();
        let tasks = self.concurrency().max(1) as usize;
        // Each call of the supplier gets the next task index. See
        // [`task_seed`] and [`FixedSetKeyGenerator::walk`] for its use.
        let next_task = AtomicU64::new(0);
        let supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>> = match self.key_generator() {
            KeyGeneratorType::Random => {
                info!(key_len, "using random key generator");
                Box::new(move || {
                    let task = next_task.fetch_add(1, Ordering::Relaxed);
                    Box::new(RandomKeyGenerator::new(key_len, task_seed(seed, task)))
                })
            }
            KeyGeneratorType::FixedSet => {
                info!(
                    key_len,
                    key_count, all_keys, "using fixed set key generator"
                );
                // One set and one CDF for all tasks.
                let keys = FixedSetKeyGenerator::key_set(key_len, key_count, seed);
                let zipf = self
                    .zipf()
                    .filter(|_| all_keys)
                    .map(|s| FixedSetKeyGenerator::zipf_cdf(key_count, s));
                Box::new(move || {
                    let task = next_task.fetch_add(1, Ordering::Relaxed);
                    let generator =
                        FixedSetKeyGenerator::from_set(keys.clone(), task_seed(seed, task));
                    if all_keys {
                        let walker = generator.walk(task as usize, tasks);
                        match &zipf {
                            Some(cdf) => Box::new(walker.with_zipf(cdf.clone())),
                            None => Box::new(walker),
                        }
                    } else {
                        Box::new(generator)
                    }
                })
            }
        };

        supplier
    }
}

/// The pick seed of one task. Each task gets its own seed, so the tasks do
/// not read the same keys in the same order.
fn task_seed(seed: Option<u64>, task: u64) -> Option<u64> {
    seed.map(|seed| seed.wrapping_add(task))
}

impl KeyGeneratorSupplier for BenchmarkDbArgs {
    fn key_generator(&self) -> KeyGeneratorType {
        self.key_generator.clone()
    }

    fn key_len(&self) -> usize {
        self.key_len
    }

    fn key_count(&self) -> u64 {
        self.key_count
    }

    fn seed(&self) -> Option<u64> {
        self.seed
    }

    fn concurrency(&self) -> u32 {
        self.concurrency
    }

    fn all_keys(&self) -> bool {
        self.all_keys
    }

    fn zipf(&self) -> Option<f64> {
        self.zipf
    }
}

#[derive(Clone)]
pub(crate) enum KeyGeneratorType {
    Random,
    FixedSet,
}

const KEY_GENERATOR_TYPE_RANDOM: &str = "Random";
const KEY_GENERATOR_TYPE_FIXEDSET: &str = "FixedSet";

impl ValueEnum for KeyGeneratorType {
    fn value_variants<'a>() -> &'a [Self] {
        &[KeyGeneratorType::Random, KeyGeneratorType::FixedSet]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            KeyGeneratorType::Random => Some(PossibleValue::new(KEY_GENERATOR_TYPE_RANDOM)),
            KeyGeneratorType::FixedSet => Some(PossibleValue::new(KEY_GENERATOR_TYPE_FIXEDSET)),
        }
    }
}

impl Display for KeyGeneratorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                KeyGeneratorType::Random => KEY_GENERATOR_TYPE_RANDOM,
                KeyGeneratorType::FixedSet => KEY_GENERATOR_TYPE_FIXEDSET,
            }
        )
    }
}

fn parse_isolation_level(s: &str) -> Result<IsolationLevel, String> {
    match s.to_lowercase().as_str() {
        "snapshot" => Ok(IsolationLevel::Snapshot),
        "serializable" => Ok(IsolationLevel::SerializableSnapshot),
        _ => Err(format!(
            "invalid isolation level: '{}'. Valid options: 'snapshot', 'serializable'",
            s
        )),
    }
}

#[derive(Args, Clone)]
pub(crate) struct BenchmarkCompactionArgs {
    #[command(subcommand)]
    pub(crate) subcommand: CompactionSubcommands,
}

#[derive(Subcommand, Clone)]
#[command(about = "Benchmark SlateDB compaction.")]
pub(crate) enum CompactionSubcommands {
    Load(CompactionLoadArgs),
    Run(CompactionRunArgs),
    Clear(CompactionClearArgs),
}

#[derive(Args, Clone)]
#[command(about = "Load test data.")]
pub(crate) struct CompactionLoadArgs {
    #[arg(
        long,
        help = "Size of each SSTable in bytes.",
        default_value_t = 1_073_741_824
    )]
    pub(crate) sst_bytes: usize,

    #[arg(
        long,
        help = "Number of SSTables to use when loading data.",
        default_value_t = 4
    )]
    pub(crate) num_ssts: usize,

    #[arg(long, help = "Size of each key.", default_value_t = 32)]
    pub(crate) key_bytes: usize,

    #[arg(long, help = "Size of each value.", default_value_t = 224)]
    pub(crate) val_bytes: usize,

    #[arg(
        long,
        help = "Compression codec to use. If set, must `snappy`, `zlib`, `lz4`, or `zstd` (with the `--features` set)."
    )]
    pub(crate) compression_codec: Option<CompressionCodec>,
}

#[derive(Args, Clone)]
#[command(about = "Run a compaction.")]
pub(crate) struct CompactionRunArgs {
    #[arg(
        long,
        help = "Number of SSTables to use when running a compaction.",
        default_value_t = 4
    )]
    pub(crate) num_ssts: usize,

    #[arg(
        long,
        help = "A comma-separated list of sorted run IDs to compact from an existing database instead of the SSTs generated by the tool.",
        value_delimiter = ',',
        num_args = 0..
    )]
    pub(crate) compaction_sources: Option<Vec<u32>>,

    #[arg(long, help = "Destination sorted run ID.", default_value_t = 0)]
    pub(crate) compaction_destination: u32,

    #[arg(long, help = "Compression codec to use.")]
    pub(crate) compression_codec: Option<CompressionCodec>,
}

#[derive(Args, Clone)]
#[command(about = "Clear test data.")]
pub(crate) struct CompactionClearArgs {
    #[arg(
        long,
        help = "Number of SSTables to use when clearing data.",
        default_value_t = 4
    )]
    pub(crate) num_ssts: usize,
}

#[derive(Args, Clone)]
#[command(about = "Benchmark SlateDB transactions.")]
pub(crate) struct BenchmarkTransactionArgs {
    #[clap(flatten)]
    pub(crate) db_args: DbArgs,

    #[arg(long, help = "The duration in seconds to run the benchmark for.")]
    pub(crate) duration: Option<u32>,

    #[arg(
        long,
        help = "The key generator to use.",
        default_value_t = KeyGeneratorType::FixedSet
    )]
    pub(crate) key_generator: KeyGeneratorType,

    #[arg(
        long,
        help = "The number of keys to generate for FixedSet key generator.",
        default_value_t = 100_000
    )]
    pub(crate) key_count: u64,

    #[arg(
        long,
        help = "The length of the keys to generate.",
        default_value_t = 16
    )]
    pub(crate) key_len: usize,

    #[arg(long, help = "The number of concurrent workers.", default_value_t = 4)]
    pub(crate) concurrency: u32,

    #[arg(
        long,
        help = "The length of the values to generate.",
        default_value_t = 1024
    )]
    pub(crate) val_len: usize,

    #[arg(
        long,
        help = "Number of operations per transaction.",
        default_value_t = 10
    )]
    pub(crate) transaction_size: u32,

    #[arg(
        long,
        help = "Percentage of transactions that should abort/rollback.",
        default_value_t = 10
    )]
    pub(crate) abort_percentage: u32,

    #[arg(
        long,
        help = "Use WriteBatch instead of Transaction for comparison.",
        default_value_t = false
    )]
    pub(crate) use_write_batch: bool,

    #[arg(
        long,
        help = "Isolation level: 'snapshot' or 'serializable'.",
        value_parser = parse_isolation_level,
        default_value = "snapshot"
    )]
    pub(crate) isolation_level: IsolationLevel,

    #[arg(
        long,
        help = "Whether to await durable writes.",
        default_value_t = false
    )]
    pub(crate) await_durable: bool,
}

impl KeyGeneratorSupplier for BenchmarkTransactionArgs {
    fn key_generator(&self) -> KeyGeneratorType {
        self.key_generator.clone()
    }

    fn key_len(&self) -> usize {
        self.key_len
    }

    fn key_count(&self) -> u64 {
        self.key_count
    }
}
