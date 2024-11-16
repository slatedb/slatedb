use clap::{Parser, Subcommand};
use std::time::Duration;
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "slatedb")]
#[command(version = "0.1.0")]
#[command(about, long_about = None)]
pub(crate) struct CliArgs {
    #[arg(
        short,
        long,
        help = "A .env file to use to supply environment variables"
    )]
    pub(crate) env_file: Option<String>,
    #[arg(
        short,
        long,
        help = "The path in the object store to the root directory, starting from within the object store bucket (specified when configuring the object store provider)"
    )]
    pub(crate) path: String,

    #[command(subcommand)]
    pub(crate) command: CliCommands,
}

#[derive(Subcommand, Debug)]
pub(crate) enum CliCommands {
    /// Reads the latest manifest file and outputs a readable
    /// String representation
    ReadManifest {
        /// Specify a specific manifest ULID to read, if this is
        /// not specified the latest manifest will be returned
        #[arg(short, long)]
        id: Option<u64>,
    },

    /// Lists all available manifests
    ListManifests {
        /// Optionally specify a start id for the range of manifests to lookup
        #[arg(short, long)]
        start: Option<u64>,

        /// Optionally specify an end id for the range of manifests to lookup
        #[arg(short, long)]
        end: Option<u64>,
    },

    /// Create a new checkpoint pointing to the database's current state.
    CreateCheckpoint {
        /// Optionally specify a lifetime for the created checkpoint. You can specify the lifetime
        /// in a human-friendly format that uses years/days/min/s, e.g. "7days 30min 10s". The
        /// checkpoint's expiry time will be set to the current wallclock time plus the specified
        /// lifetime. If the lifetime is not specified, then the checkpoint is set with no expiry
        /// and must be explicitly removed.
        #[arg(short, long)]
        #[clap(value_parser = humantime::parse_duration)]
        lifetime: Option<Duration>,

        /// Optionally specify the id (e.g. 01740ee5-6459-44af-9a45-85deb6e468e3) of an existing
        /// checkpoint to use as the base for the newly created checkpoint. If not provided then
        /// the checkpoint will be taken against the latest manifest.
        #[arg(short, long)]
        #[clap(value_parser = uuid::Uuid::parse_str)]
        source: Option<Uuid>,
    },

    /// Refresh an existing checkpoint's expiry time. This command will look for an existing
    /// checkpoint and update its expiry time using the specified lifetime.
    RefreshCheckpoint {
        /// The id of the checkpoint (e.g. 01740ee5-6459-44af-9a45-85deb6e468e3) to refresh.
        #[arg(short, long)]
        #[clap(value_parser = uuid::Uuid::parse_str)]
        id: Uuid,

        /// Optionally specify a new lifetime for the checkpoint. You can specify the lifetime in a
        /// human-friendly format that uses years/days/min/s, e.g. "7days 30min 10s". The
        /// checkpoint's expiry time will be set to the current wallclock time plus the specified
        /// lifetime. If the lifetime is not specified, then the checkpoint is updated with no
        /// expiry and must be explicitly removed.
        #[arg(short, long)]
        #[clap(value_parser = humantime::parse_duration)]
        lifetime: Option<Duration>,
    },

    /// Delete an existing checkpoint.
    DeleteCheckpoint {
        /// The id of the checkpoint (e.g. 01740ee5-6459-44af-9a45-85deb6e468e3) to delete.
        #[arg(short, long)]
        #[clap(value_parser = uuid::Uuid::parse_str)]
        id: Uuid,
    },

    /// List the current checkpoints of the db.
    ListCheckpoints {},
}

pub(crate) fn parse_args() -> CliArgs {
    CliArgs::parse()
}
