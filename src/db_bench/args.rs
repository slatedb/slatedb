use crate::db_bench::{KeyGenerator, RandomKeyGenerator};
use clap::builder::PossibleValue;
use clap::{Args, Parser, Subcommand, ValueEnum};
use slatedb::config::WriteOptions;
use std::fmt::{Display, Formatter};

#[derive(Parser, Clone)]
#[command(version, about, long_about=None)]
pub(crate) struct DbBenchArgs {
    #[arg(short, long)]
    pub(crate) bucket: Option<String>,
    #[arg(short, long)]
    pub(crate) region: Option<String>,
    #[arg(short = 'c', long)]
    pub(crate) provider: Provider,
    #[arg(long)]
    pub(crate) dynamodb_table: Option<String>,
    #[arg(short, long)]
    pub(crate) path: String,
    #[arg(long)]
    pub(crate) flush_ms: Option<u32>,
    #[arg(long)]
    pub(crate) disable_wal: Option<bool>,
    #[arg(long)]
    pub(crate) l0_sst_size_bytes: Option<usize>,
    #[command(subcommand)]
    pub(crate) command: DbBenchCommand,
}

pub(crate) fn parse_args() -> DbBenchArgs {
    DbBenchArgs::parse()
}

#[derive(Subcommand, Clone)]
pub(crate) enum DbBenchCommand {
    Write(WriteArgs),
}

#[derive(Args, Clone)]
pub(crate) struct WriteArgs {
    #[arg(long)]
    pub(crate) duration: Option<u32>,
    #[arg(
        long,
        default_value_t = KeyDistribution::Random,
    )]
    key_distribution: KeyDistribution,
    #[arg(long)]
    key_len: usize,
    #[arg(long, default_value_t = false)]
    await_flush: bool,
    #[arg(long)]
    pub(crate) write_rate: Option<u32>,
    #[arg(long, default_value_t = 4)]
    pub(crate) write_tasks: u32,
    #[arg(long)]
    pub(crate) num_rows: Option<u64>,
    #[arg(long)]
    pub(crate) val_len: usize,
}

impl WriteArgs {
    pub(crate) fn key_gen_supplier(&self) -> Box<dyn Fn() -> Box<dyn KeyGenerator>> {
        let supplier = match self.key_distribution {
            KeyDistribution::Random => {
                let key_len = self.key_len;
                move || Box::new(RandomKeyGenerator::new(key_len)) as Box<dyn KeyGenerator>
            }
        };
        Box::new(supplier)
    }

    pub(crate) fn write_options(&self) -> WriteOptions {
        WriteOptions {
            await_flush: self.await_flush,
        }
    }
}

#[derive(Clone)]
pub(crate) enum Provider {
    Aws,
    InMemory,
}

const PROVIDER_AWS: &str = "aws";
const PROVIDER_MEMORY: &str = "memory";

impl ValueEnum for Provider {
    fn value_variants<'a>() -> &'a [Self] {
        &[Provider::Aws, Provider::InMemory]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            Provider::Aws => Some(PossibleValue::new(PROVIDER_AWS)),
            Provider::InMemory => Some(PossibleValue::new(PROVIDER_MEMORY)),
        }
    }
}

impl Display for Provider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Provider::Aws => PROVIDER_AWS,
                Provider::InMemory => PROVIDER_MEMORY,
            }
        )
    }
}

#[derive(Clone)]
pub(crate) enum KeyDistribution {
    Random,
}

const KEY_DISTRIBUTION_RANDOM: &str = "Random";

impl ValueEnum for KeyDistribution {
    fn value_variants<'a>() -> &'a [Self] {
        &[KeyDistribution::Random]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        match self {
            KeyDistribution::Random => Some(PossibleValue::new(KEY_DISTRIBUTION_RANDOM)),
        }
    }
}

impl Display for KeyDistribution {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                KeyDistribution::Random => KEY_DISTRIBUTION_RANDOM,
            }
        )
    }
}
