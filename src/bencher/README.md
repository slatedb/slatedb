# SlateDB Benchmarking Tool

Bencher is a tool for benchmarking SlateDB. The tool currently has only one
subcommand: `db`.

## `db` Subcommand

The `db` subcommand is used to benchmark SlateDB. It can be used to measure the
puts and gets per-second on a SlateDB database. The subcommand takes the following
arguments:

```
Usage: bencher db [OPTIONS]

Options:
      --disable-wal
          Whether to disable the write-ahead log.
      --flush-ms <FLUSH_MS>
          The interval in milliseconds to flush the write-ahead log.
      --l0-sst-size-bytes <L0_SST_SIZE_BYTES>
          The size in bytes of the L0 SSTables.
      --block-cache-size <BLOCK_CACHE_SIZE>
          The size in bytes of the block cache.
      --object-cache-path <OBJECT_CACHE_PATH>
          The path where object store cache part files are stored.
      --object-cache-part-size <OBJECT_CACHE_PART_SIZE>
          The size in bytes of the object store cache part files.
      --duration <DURATION>
          The duration in seconds to run the benchmark for.
      --key-distribution <KEY_DISTRIBUTION>
          The key distribution to use. [default: Random] [possible values: Random]
      --key-len <KEY_LEN>
          The length of the keys to generate. [default: 16]
      --await-durable
          Whether to await durable writes.
      --concurrency <CONCURRENCY>
          The number of read/write to spawn. [default: 4]
      --num-rows <NUM_ROWS>
          The number of rows to write.
      --val-len <VAL_LEN>
          The length of the values to generate. [default: 1024]
      --put-percentage <PUT_PERCENTAGE>
          The percentage of writes to perform in each task. [default: 20]
  -h, --help
          Print help
```

The following command runs the benchmark for 120 seconds:

```bash
cargo run -r --bin bencher --features="bencher" -- db --duration 120
```

Make sure to set up the following environment variables before benchmarking:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_S3_BUCKET`
- `AWS_DYNAMODB_TABLE`, see
  [DynamoCommit](https://docs.rs/object_store/latest/object_store/aws/struct.DynamoCommit.html)
  for more details.
- `AWS_ENDPOINT` (optional), if you are using a custom S3 endpoint.

### Plotting Results

There is also a shell script which runs a series of benchmarks and then draws
the plots using `gnuplot`. Think of it as a template to start with to create
a set of benchmarks suitable for your task. The script should be run from
the repository root:

```bash
./src/bencher/benchmark-db.sh
```

The command above will produce results at `target/bencher/results` directory. 
