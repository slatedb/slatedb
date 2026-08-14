use std::error::Error;

use clap::{Parser, ValueEnum};
use object_store::path::Path;
use slatedb::wal::{run_bench, WalBenchPhase, DEFAULT_WAL_BENCH_DATA_SIZE_BYTES};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "wal-bench")]
#[command(about = "Benchmark SlateDB native WAL writes and replay against S3.")]
struct Args {
    #[arg(long, help = "S3 bucket used for benchmark WAL files.")]
    bucket: String,

    #[arg(long, help = "AWS region containing the S3 bucket.")]
    region: String,

    #[arg(
        long,
        alias = "path",
        help = "Object path prefix dedicated to benchmark WAL files."
    )]
    prefix: String,

    #[arg(
        long,
        value_enum,
        default_value = "both",
        help = "Benchmark phase to run."
    )]
    phase: Phase,

    #[arg(
        long,
        alias = "data-size-bytes",
        default_value_t = DEFAULT_WAL_BENCH_DATA_SIZE_BYTES,
        help = "Maximum logical key/value bytes to write or replay."
    )]
    size_bytes: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Phase {
    Write,
    Read,
    Both,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();

    let args = Args::parse();
    let result = run_bench(
        &args.bucket,
        &args.region,
        Path::from(args.prefix),
        match args.phase {
            Phase::Write => WalBenchPhase::Write,
            Phase::Read => WalBenchPhase::Read,
            Phase::Both => WalBenchPhase::Both,
        },
        args.size_bytes,
    )
    .await?;

    println!("size_limit_bytes={}", result.size_limit_bytes);
    println!("first_wal_file_id={}", result.first_wal_file_id);
    println!("last_wal_file_id={}", result.last_wal_file_id);
    println!("wal_files={}", result.wal_file_count);
    if let Some(write) = result.write {
        println!("write_bytes={}", write.data_size_bytes);
        println!("write_rows={}", write.row_count);
        println!("write_elapsed_seconds={:.3}", write.elapsed.as_secs_f64());
        println!(
            "write_throughput_bytes_per_second={:.0}",
            write.throughput_bytes_per_second
        );
        println!(
            "write_throughput_mib_per_second={:.2}",
            write.throughput_bytes_per_second / (1024.0 * 1024.0)
        );
    }
    if let Some(replay) = result.replay {
        println!("replay_bytes={}", replay.data_size_bytes);
        println!("replay_rows={}", replay.row_count);
        println!("replay_elapsed_seconds={:.3}", replay.elapsed.as_secs_f64());
        println!(
            "replay_throughput_bytes_per_second={:.0}",
            replay.throughput_bytes_per_second
        );
        println!(
            "replay_throughput_mib_per_second={:.2}",
            replay.throughput_bytes_per_second / (1024.0 * 1024.0)
        );
    }
    if let Some(max_unflushed_wal_files) = result.max_unflushed_wal_files {
        println!("max_unflushed_wal_files={max_unflushed_wal_files}");
    }

    Ok(())
}
