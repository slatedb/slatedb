//! Measures forward scans from reproducible random start keys.

use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rand::{Rng, SeedableRng};
use rand_xorshift::XorShiftRng;
use slatedb::config::ScanOptions;
use slatedb::Db;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::info;

use crate::args::BenchmarkScanArgs;

#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct ScanStats {
    scans: u64,
    rows: u64,
    bytes: u64,
}

#[derive(Default)]
struct ScanTotals {
    scans: AtomicU64,
    rows: AtomicU64,
    bytes: AtomicU64,
}

impl ScanTotals {
    fn record(&self, stats: ScanStats) {
        self.rows.fetch_add(stats.rows, Ordering::Relaxed);
        self.bytes.fetch_add(stats.bytes, Ordering::Relaxed);
        self.scans.fetch_add(stats.scans, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ScanStats {
        ScanStats {
            scans: self.scans.load(Ordering::Relaxed),
            rows: self.rows.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
        }
    }
}

pub(crate) async fn run(db: Arc<Db>, args: BenchmarkScanArgs) -> Result<ScanStats, Box<dyn Error>> {
    let options = ScanOptions::new().with_read_ahead_bytes(args.read_ahead_bytes);
    let next_scan = Arc::new(AtomicU64::new(0));
    let totals = Arc::new(ScanTotals::default());
    let duration = args
        .duration
        .map(|seconds| Duration::from_secs(seconds as u64))
        .or_else(|| args.num_scans.is_none().then_some(Duration::from_secs(60)));
    let start = Instant::now();
    let mut tasks = JoinSet::new();

    info!(
        seed = args.seed,
        concurrency = args.concurrency,
        scan_length = args.scan_length,
        read_ahead_bytes = args.read_ahead_bytes,
        "starting scan benchmark"
    );
    for _ in 0..args.concurrency {
        let db = db.clone();
        let args = args.clone();
        let options = options.clone();
        let next_scan = next_scan.clone();
        let totals = totals.clone();
        tasks.spawn(async move {
            let mut key = vec![0; args.key_len as usize];
            while duration.is_none_or(|limit| start.elapsed() < limit) {
                let scan_id = next_scan.fetch_add(1, Ordering::Relaxed);
                if args.num_scans.is_some_and(|limit| scan_id >= limit) {
                    break;
                }
                // Each scan ID selects the same key, independent of worker scheduling.
                let mut rng = XorShiftRng::seed_from_u64(args.seed.wrapping_add(scan_id));
                rng.fill_bytes(&mut key);
                let stats = scan(&db, &key, args.scan_length, &options).await?;
                totals.record(stats);
            }
            Ok::<(), slatedb::Error>(())
        });
    }

    let mut interval = tokio::time::interval(Duration::from_secs(10));
    interval.tick().await;
    while !tasks.is_empty() {
        tokio::select! {
            result = tasks.join_next() => {
                result.expect("scan worker exists")??;
            }
            _ = interval.tick() => dump_stats(&totals.snapshot(), start.elapsed(), false),
        }
    }
    let stats = totals.snapshot();
    dump_stats(&stats, start.elapsed(), true);
    Ok(stats)
}

async fn scan(
    db: &Db,
    start_key: &[u8],
    scan_length: u64,
    options: &ScanOptions,
) -> Result<ScanStats, slatedb::Error> {
    let mut iter = db.scan_with_options(start_key.., options).await?;
    let mut stats = ScanStats::default();
    for _ in 0..scan_length {
        let Some(row) = iter.next().await? else {
            break;
        };
        stats.rows += 1;
        stats.bytes += (row.key.len() + row.value.len()) as u64;
    }
    stats.scans = 1;
    Ok(stats)
}

fn dump_stats(stats: &ScanStats, elapsed: Duration, complete: bool) {
    let seconds = elapsed.as_secs_f64();
    info!(
        complete,
        "scan stats [elapsed {:.3}s, scan/s: {:.3}, rows/s: {:.3}, MiB/s: {:.3}, \
         total scans: {}, total rows: {}, total bytes: {}]",
        seconds,
        stats.scans as f64 / seconds,
        stats.rows as f64 / seconds,
        stats.bytes as f64 / seconds / 1_048_576.0,
        stats.scans,
        stats.rows,
        stats.bytes,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::{BencherArgs, BencherCommands};
    use clap::Parser;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn scan_counts_rows_at_the_limit_and_at_eof() {
        let db = Db::open("scan", Arc::new(InMemory::new())).await.unwrap();
        db.put([10], [1]).await.unwrap();
        db.put([100], [2, 3]).await.unwrap();
        db.put([250], [4, 5, 6]).await.unwrap();
        for read_ahead_bytes in [1, 65536] {
            let options = ScanOptions::new().with_read_ahead_bytes(read_ahead_bytes);
            for (key, length, rows, bytes) in [
                (0, 2, 2, 5),
                (100, 1, 1, 3),
                (101, 100, 1, 4),
                (251, 100, 0, 0),
            ] {
                assert_eq!(
                    scan(&db, &[key], length, &options).await.unwrap(),
                    ScanStats {
                        scans: 1,
                        rows,
                        bytes
                    },
                );
            }
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn scan_limit_and_seed_do_not_depend_on_worker_count() {
        let db = Arc::new(Db::open("scan", Arc::new(InMemory::new())).await.unwrap());
        for key in 0..=255u8 {
            db.put([key], [key]).await.unwrap();
        }
        let BencherCommands::Scan(args) = BencherArgs::parse_from([
            "bencher",
            "scan",
            "--key-len",
            "1",
            "--num-scans",
            "17",
            "--scan-length",
            "100",
            "--concurrency",
            "1",
        ])
        .command
        else {
            panic!("expected scan arguments");
        };
        let first = run(db.clone(), args.clone()).await.unwrap();
        assert_eq!(first.scans, 17);
        assert!(first.rows > 0);
        let other = BenchmarkScanArgs {
            concurrency: 3,
            read_ahead_bytes: 65536,
            ..args
        };
        assert_eq!(first, run(db.clone(), other).await.unwrap());
        db.close().await.unwrap();
    }
}
