use std::{
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::Arc,
};

use clap::{builder::PossibleValue, Args, Parser, Subcommand, ValueEnum};
use slatedb::{
    config::{CompressionCodec, Settings},
    db_cache::{
        foyer::{FoyerCache, FoyerCacheOptions},
        DbCache, SplitCache,
    },
    Error,
};
use tracing::info;

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
}

impl DbArgs {
    /// Returns a `(Settings, Option<Arc<dyn DbCache>>)` struct based on DbArgs's arguments.
    pub(crate) fn config(&self) -> Result<(Settings, Option<Arc<dyn DbCache>>), Error> {
        let settings = if let Some(path) = &self.db_options_path {
            Settings::from_file(path)?
        } else {
            Settings::load()?
        };

        let block_cache = self.block_cache_size.map(|capacity| {
            Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: capacity,
            })) as Arc<dyn DbCache>
        });
        let meta_cache = self.meta_cache_size.map(|capacity| {
            Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: capacity,
            }))
        });
        let memory_cache = Some(Arc::new(
            SplitCache::new()
                .with_block_cache(block_cache)
                .with_meta_cache(meta_cache)
                .build(),
        ) as Arc<dyn DbCache>);

        Ok((settings, memory_cache))
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
}

impl BenchmarkDbArgs {
    pub(crate) fn key_gen_supplier(&self) -> Box<dyn Fn() -> Box<dyn KeyGenerator>> {
        let key_len = self.key_len;
        let key_count = self.key_count;
        let supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>> = match self.key_generator {
            KeyGeneratorType::Random => {
                info!(key_len, "using random key generator");
                Box::new(move || Box::new(RandomKeyGenerator::new(key_len)))
            }
            KeyGeneratorType::FixedSet => {
                info!(key_len, key_count, "using fixed set key generator");
                Box::new(move || Box::new(FixedSetKeyGenerator::new(key_len, key_count)))
            }
        };

        supplier
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
