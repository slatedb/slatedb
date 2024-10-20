use std::{
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use clap::{builder::PossibleValue, Args, Parser, Subcommand, ValueEnum};
use slatedb::{
    config::{CompressionCodec, DbOptions, ObjectStoreCacheOptions},
    db_cache::{
        moka::{MokaCache, MokaCacheOptions},
        DbCache,
    },
};

use crate::db::{KeyGenerator, RandomKeyGenerator};

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

    #[command(subcommand)]
    pub(crate) command: BencherCommands,
}

#[derive(Subcommand, Clone)]
pub(crate) enum BencherCommands {
    Db(BenchmarkDbArgs),
    Compaction(BenchmarkCompactionArgs),
}

#[derive(Args, Clone)]
pub(crate) struct DbArgs {
    #[arg(
        long,
        help = "Whether to disable the write-ahead log.",
        default_value_t = false
    )]
    pub(crate) disable_wal: bool,

    #[arg(
        long,
        help = "The interval in milliseconds to flush the write-ahead log."
    )]
    pub(crate) flush_ms: Option<u32>,

    #[arg(long, help = "The size in bytes of the L0 SSTables.")]
    pub(crate) l0_sst_size_bytes: Option<usize>,

    #[arg(long, help = "The size in bytes of the block cache.")]
    pub(crate) block_cache_size: Option<usize>,

    #[arg(
        long,
        help = "The path where object store cache part files are stored."
    )]
    pub(crate) object_cache_path: Option<String>,

    #[arg(long, help = "The size in bytes of the object store cache part files.")]
    pub(crate) object_cache_part_size: Option<usize>,

    #[arg(long, help = "The size in bytes of the object store cache.")]
    pub(crate) object_cache_size_bytes: Option<usize>,

    #[arg(long, help = "The interval in seconds to scan for expired objects.")]
    pub(crate) scan_interval: Option<u32>,
}

impl DbArgs {
    /// Returns a `DbOptions` struct based on DbArgs's arguments.
    #[allow(dead_code)]
    pub(crate) fn config(&self) -> DbOptions {
        let mut db_options = DbOptions::default();
        db_options.wal_enabled = !self.disable_wal;
        db_options.flush_interval = self
            .flush_ms
            .map(|i| Duration::from_millis(i as u64))
            .unwrap_or(db_options.flush_interval);
        db_options.l0_sst_size_bytes = self
            .l0_sst_size_bytes
            .unwrap_or(db_options.l0_sst_size_bytes);
        db_options.block_cache = self.block_cache_size.map(|size| {
            Arc::new(MokaCache::new_with_opts(MokaCacheOptions {
                max_capacity: size as u64,
                ..Default::default()
            })) as Arc<dyn DbCache>
        });
        db_options.object_store_cache_options = self
            .object_cache_path
            .clone()
            .map(|path| ObjectStoreCacheOptions {
                root_folder: Some(PathBuf::from(path)),
                ..Default::default()
            })
            .unwrap_or(db_options.object_store_cache_options);
        db_options.object_store_cache_options.part_size_bytes = self
            .object_cache_part_size
            .unwrap_or(db_options.object_store_cache_options.part_size_bytes);
        db_options.object_store_cache_options.max_cache_size_bytes = self
            .object_cache_size_bytes
            .or(db_options.object_store_cache_options.max_cache_size_bytes);
        db_options.object_store_cache_options.scan_interval = self
            .scan_interval
            .map(|i| Duration::from_secs(i as u64))
            .or(db_options.object_store_cache_options.scan_interval);
        db_options
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
        default_value_t = KeyGeneratorType::Random
    )]
    pub(crate) key_generator: KeyGeneratorType,

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
}

impl BenchmarkDbArgs {
    pub(crate) fn key_gen_supplier(&self) -> Box<dyn Fn() -> Box<dyn KeyGenerator>> {
        let supplier = match self.key_generator {
            KeyGeneratorType::Random => {
                let key_len = self.key_len;
                move || Box::new(RandomKeyGenerator::new(key_len)) as Box<dyn KeyGenerator>
            }
        };
        Box::new(supplier)
    }
}

#[derive(Clone)]
pub(crate) enum KeyGeneratorType {
    Random,
}

const KEY_GENERATOR_TYPE_RANDOM: &str = "Random";

impl ValueEnum for KeyGeneratorType {
    fn value_variants<'a>() -> &'a [Self] {
        &[KeyGeneratorType::Random]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            KeyGeneratorType::Random => Some(PossibleValue::new(KEY_GENERATOR_TYPE_RANDOM)),
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
            }
        )
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
