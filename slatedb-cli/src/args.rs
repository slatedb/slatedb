use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use slatedb::FindOption;
use std::collections::HashMap;
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

    /// Runs a garbage collection for a specific resource type once
    RunGarbageCollection {
        /// the type of resource to clean up (manifest, wal, compacted)
        #[arg(short, long)]
        resource: GcResource,

        /// the minimum age of the resource before considering it for GC
        #[arg(short, long)]
        #[clap(value_parser = humantime::parse_duration)]
        min_age: Duration,
    },

    /// Converts a sequence number to its corresponding timestamp using the latest manifest's sequence tracker.
    SeqToTs {
        seq: u64,
        #[arg(long, value_parser = parse_find_option, default_value = "down")]
        round: FindOption,
    },

    /// Converts a timestamp (in seconds) to its corresponding sequence number using the latest manifest's sequence tracker.
    TsToSeq {
        ts_secs: i64,
        #[arg(long, value_parser = parse_find_option, default_value = "down")]
        round: FindOption,
    },

    /// Schedules a period garbage collection job
    #[command(group(
    ArgGroup::new("gc_config")
        .args(["manifest", "wal", "compacted"])
        .multiple(true)
        .required(true)
    ))]
    ScheduleGarbageCollection {
        /// Configuration for manifest garbage collection should be set in the
        /// format min_age=<duration>,period=<duration> -- the min_age is the
        /// minimum manifest age that should be considered for collection and
        /// the period is how often to attempt a GC
        #[arg(long, value_parser = parse_gc_schedule)]
        manifest: Option<GcSchedule>,

        /// Configuration for WAL garbage collection should be set in the
        /// format min_age=<duration>,period=<duration> -- the min_age is the
        /// minimum WAL age that should be considered for collection and
        /// the period is how often to attempt a GC
        #[arg(long, value_parser = parse_gc_schedule)]
        wal: Option<GcSchedule>,

        /// Configuration for compacted SST garbage collection should be set in the
        /// format min_age=<duration>,period=<duration> -- the min_age is the
        /// minimum SST age that should be considered for collection and
        /// the period is how often to attempt a GC
        #[arg(long, value_parser = parse_gc_schedule)]
        compacted: Option<GcSchedule>,
    },
}

#[derive(Debug, Clone, ValueEnum)]
pub(crate) enum GcResource {
    Manifest,
    Wal,
    Compacted,
}

fn parse_gc_schedule(s: &str) -> Result<GcSchedule, String> {
    let parts: HashMap<String, String> = s
        .split(',')
        .filter_map(|kv| {
            let mut parts = kv.splitn(2, '=');
            match (parts.next(), parts.next()) {
                (Some(key), Some(value)) => Some((key.to_string(), value.to_string())),
                _ => None,
            }
        })
        .collect();

    let min_age = parts
        .get("min_age")
        .ok_or_else(|| "Missing or invalid 'min_age'".to_string())
        .and_then(|v| {
            humantime::parse_duration(v).map_err(|e| {
                "Could not parse min_age as duration: "
                    .to_string()
                    .to_owned()
                    + e.to_string().as_str()
            })
        })?;
    let period = parts
        .get("period")
        .ok_or_else(|| "Missing or invalid 'period'".to_string())
        .and_then(|v| humantime::parse_duration(v).map_err(|e| e.to_string()))?;

    Ok(GcSchedule { min_age, period })
}

#[derive(Debug, Clone)]
pub(crate) struct GcSchedule {
    /// Minimum age of resources to collect
    pub(crate) min_age: Duration,

    /// How often to run the garbage collection
    pub(crate) period: Duration,
}

pub(crate) fn parse_args() -> CliArgs {
    CliArgs::parse()
}

fn parse_find_option(s: &str) -> Result<FindOption, String> {
    match s.to_ascii_lowercase().as_str() {
        "up" | "roundup" => Ok(FindOption::RoundUp),
        "down" | "rounddown" => Ok(FindOption::RoundDown),
        _ => Err("Invalid find option".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use crate::args::parse_gc_schedule;
    use rstest::rstest;
    use std::time::Duration;

    #[rstest]
    #[case(
        "min_age=10m,period=1m",
        Some(Duration::from_secs(600)),
        Some(Duration::from_secs(60)),
        None
    )]
    #[case(
        "min_age=10m,period=1m,ignored=5m",
        Some(Duration::from_secs(600)),
        Some(Duration::from_secs(60)),
        None
    )]
    #[case("period=1m", None, None, Some("Missing or invalid 'min_age'"))]
    #[case("min_age=10m", None, None, Some("Missing or invalid 'period'"))]
    #[case(
        "min_age=invalid,period=1m",
        None,
        None,
        Some("Could not parse min_age as duration")
    )]
    #[case(
        "min_age=,period=1m",
        None,
        None,
        Some("Could not parse min_age as duration: value was empty")
    )]
    fn parse_gc_schedule_tests(
        #[case] input: &str,
        #[case] expected_min_age: Option<Duration>,
        #[case] expected_period: Option<Duration>,
        #[case] expected_error: Option<&str>,
    ) {
        let result = parse_gc_schedule(input);

        match (result, expected_min_age, expected_period, expected_error) {
            // Valid case: min_age and period are parsed correctly
            (Ok(schedule), Some(min_age), Some(period), None) => {
                assert_eq!(schedule.min_age, min_age);
                assert_eq!(schedule.period, period);
            }
            // Error case: check if the error message matches
            (Err(err), None, None, Some(expected_msg)) => {
                assert!(
                    err.contains(expected_msg),
                    "Expected error to contain '{}', got '{}'",
                    expected_msg,
                    err
                );
            }
            // Any unexpected combination fails the test
            result => panic!("Unexpected test case result. {:?}", result),
        }
    }
}
