# SlateDB Benchmarking Tool

Bencher is a tool for benchmarking SlateDB. The tool currently has two
subcommands: `db` and `compaction`.

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

### Batch reads with `mget`

The `db` subcommand has an optional mode, `mget`. It reads keys in batches
instead of one key per call. A batch goes through one of three readers:

- `seq`: one `get` after the other.
- `concurrent`: up to `--read-concurrency` gets at the same time.
- `multi-get`: one `multi_get` call.

Each stats dump then has a second line with the read calls. It shows the p50
and p99 latency of a call, the SST GET requests per call, the SST bytes per
call, and the `multi_get` layer walks per call. The GET and byte counts come from a
wrapper around the object store, so they are exact.

The `db` subcommand also has flags for repeatable read runs:

- `--seed <N>`: the fixed key set and the key picks come from this seed. Two
  runs with the same seed and key count use the same keys.
- `--wait-compaction`: flush the memtable and wait until the manifest stops to
  change before the database closes. Use it in the load run.
- `--delay-profile <s3|s3x>`: add a delay to each object store read. The
  delay is sampled from the measured GET latency of S3 or of S3 Express One
  Zone. `--delay-ms <N>` adds a fixed delay instead.
- `--disk-cache`: keep the object store disk cache from `SlateDb.toml`. The
  benchmark disables it by default, so reads reach the object store.

A typical flow loads a key set, then reads it with each reader. The load run
uses no delay, because the compactor reads through the same store:

```bash
export CLOUD_PROVIDER=local LOCAL_PATH=/tmp/slatedb-bench
cargo run -r --package slatedb-bencher -- --path /mget db \
  --seed 42 --key-count 200000 --put-percentage 100 --num-rows 200000 \
  --concurrency 1 --wait-compaction

for reader in seq concurrent multi-get; do
  cargo run -r --package slatedb-bencher -- --path /mget db \
    --seed 42 --key-count 200000 --put-percentage 0 --no-compactor \
    --block-cache-size 8388608 --meta-cache-size 268435456 \
    --delay-profile s3 --concurrency 1 --duration 120 \
    mget --reader $reader --batch-size 100
done
```

The `seq` reader with a delay is slow, so its percentiles come from few calls.

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
