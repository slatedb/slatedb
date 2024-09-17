use clap::{Parser, Subcommand};

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
    ReadManifest,
}

pub(crate) fn parse_args() -> CliArgs {
    CliArgs::parse()
}
