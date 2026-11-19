# SlateDB Benchmarking Tool

Bencher measures SlateDB database operations, range scans, compaction, and transactions.

## `db` Subcommand

The `db` subcommand is used to benchmark SlateDB. It can be used to measure the
puts and gets per-second on a SlateDB database. The subcommand takes the following
arguments:

```
Usage: bencher db [OPTIONS]

Options:
      --db-options-path <FILE_PATH>
          Options path to a file with options for DbOptions, `SlateDb.toml` is used if this flag is not set.
      --block-cache-size <BLOCK_CACHE_SIZE>
          The size in bytes of the block cache.
      --duration <DURATION>
          The duration in seconds to run the benchmark for.
      --key-generator <KEY_GENERATOR>
          The key generator to use. [default: Random] [possible values: Random, FixedSet]
      --key-len <KEY_LEN>
          The length of the keys to generate in bytes. [default: 16]
      --key-count <KEY_COUNT>
          The number of keys to use for FixedSet key generator. [default: 100_000]
      --await-durable
          Whether to await durable writes.
      --concurrency <CONCURRENCY>
          The number of read/write to spawn. [default: 4]
      --num-rows <NUM_ROWS>
          The number of rows to write.
      --val-len <VAL_LEN>
          The length of the values to generate in bytes. [default: 1024]
      --put-percentage <PUT_PERCENTAGE>
          The percentage of writes to perform in each task. [default: 20]
      -h, --help
          Print help
```

The following command runs the benchmark for 120 seconds:

```bash
cargo run -r --package slatedb-bencher -- db --duration 120
```

If you're using the AWS cloud provider (`CLOUD_PROVIDER=aws`), make sure to set up the
following environment variables before benchmarking:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_BUCKET`
- `AWS_ENDPOINT` (optional), if you are using a custom S3 endpoint.
- `AWS_ALLOW_HTTP` (optional), if your AWS_ENDPOINT uses HTTP instead of HTTPS.
- `AWS_SESSION_TOKEN` (optional), if you are using temporary credentials. 

## `benchmark-db.sh`

There is also a shell script which runs a series of benchmarks and records
the results. Think of it as a template to start with to create a set of
benchmarks suitable for your task. The script should be run from the repository
root:

```bash
./slatedb-bencher/benchmark-db.sh
```

The command above will produce results at `target/bencher/results` directory. The results include:

- `dats`: Data files for each benchmark
- `logs`: Log files for each benchmark

### Plotting results with `gnuplot`

The `.dat` files are whitespace-delimited, with columns for elapsed time,
puts per second, and gets per second. After installing `gnuplot`, you can render
a result file to a PNG with:

```bash
gnuplot <<'EOF'
set terminal pngcairo size 1280,720
set output "target/bencher/results/20_1.png"
set title "SlateDB benchmark: 20% puts, concurrency 1"
set xlabel "Elapsed time (seconds)"
set ylabel "Requests per second"
set key outside
plot "target/bencher/results/dats/20_1.dat" using 1:2 with lines title "puts/s", \
     "target/bencher/results/dats/20_1.dat" using 1:3 with lines title "gets/s"
EOF
```

Replace `20_1.dat` and the labels with the benchmark configuration you want to
plot.

The script also has a `SLATEDB_BENCH_CLEAN` environment variable which can be set to `true` to clean up the test data in object storage after each benchmark.

## `scan` Subcommand

The `scan` subcommand measures forward scans from random start keys in an existing database.
It follows the seek-and-read workload of [RocksDB's `db_bench seekrandom`][rocksdb-bench].
Each scan reads up to `--scan-length` rows, or stops at the end of the database.
`--read-ahead-bytes` sets the target bytes per SST fetch. A value of 1 fetches one block.
The row limit serves the role of RocksDB's `--seek_nexts`.
Fetch sizing follows the purpose of RocksDB's `--readahead_size`.

Use the same `--seed`, `--key-len`, and `--num-scans` to repeat the same start keys.
Start keys contain random bytes. Match `--key-len` to the keys used by the `db` subcommand.
The scan number determines each start key, so worker scheduling does not change the workload.

The following example writes SST data once, then scans the same database with two fetch sizes.
Set `CLOUD_PROVIDER` and its storage variables first. For local storage, set `CLOUD_PROVIDER=local`
and set `LOCAL_PATH` to an existing directory.

```bash
cargo build --release -p slatedb-bencher
BENCHER=target/release/bencher

"$BENCHER" --path scan-benchmark db --no-compactor \
  --put-percentage 100 --num-rows 50000 --key-count 50000 \
  --key-len 16 --val-len 1024 --concurrency 1

for bytes in 1 65536; do
  "$BENCHER" --path scan-benchmark scan --num-scans 1000 \
    --scan-length 1000 --read-ahead-bytes "$bytes" \
    --seed 42 --key-len 16 --concurrency 1
done
```

The summary reports completed scans, returned rows, and key plus value bytes.
It also reports scans per second, rows per second, and MiB per second.
Empty scans count as completed scans. Rows beyond the end of the database do not count.
Rates include iterator creation and reading, and exclude database open and close.
Progress reports show averages since the scan workload started.

Use `--duration` to stop starting scans after a given number of seconds.
If both limits are set, the first limit stops new scans. Active scans finish.
The default duration is 60 seconds when neither limit is set.
The scan workload disables the embedded compactor and uses the shared cache options.
Without cache size options, scans use no block or metadata cache. The OS can cache local file reads.

[rocksdb-bench]: https://github.com/facebook/rocksdb/blob/dd7bd17/tools/db_bench_tool.cc

## `compaction` Subcommand

The `compaction` subcommand is used to benchmark the compaction process in SlateDB.
There are three subcommands:

```
Usage: bencher compaction <COMMAND>

Commands:
  load   Load test data.
  run    Run a compaction.
  clear  Clear test data.
  help   Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

A typical flow would load test data, run the compaction, then clear the test data:

```bash
cargo run -r --package slatedb-bencher -- compaction load
cargo run -r --package slatedb-bencher -- compaction run
cargo run -r --package slatedb-bencher -- compaction clear
```

See individual subcommands for more details.

The compaction benchmarking tool can also be used to compact specific SSTables
rather than the generated test data. To do this, set the `--compaction-sources`
argument:

```bash
cargo run -r --package slatedb-bencher -- compaction run --compaction-sources="1,2"
```
