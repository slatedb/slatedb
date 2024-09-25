# S3 Benchmarking Tool

The tool should be run from the repository root. The following command will run
the benchmark for 2 minutes:

```bash
cargo run -r --bin s3-bench --features="s3_bench" -- --duration 120
```

Make sure to set up the following environment variables before benchmarking:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_S3_BUCKET`
- `AWS_DYNAMODB_TABLE`, see
  [DynamoCommit](https://docs.rs/object_store/latest/object_store/aws/struct.DynamoCommit.html)
  for more details.

There is also a shell script which runs a series of benchmarks and then draws
the plots using `gnuplot`. Think of it as a template to start with to create
a set of benchmarks suitable for your task. The script should be run from
the repository root:

```bash
./src/s3-bench/run.sh
```

The command above will produce results at `target/s3-bench/results` directory. 

## Usage and Options

```
$ s3-bench --help

Runs SlateDB S3 benchmarks

The following environment variables must be configured externally:
  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY
  - AWS_REGION
  - AWS_S3_BUCKET
  - AWS_DYNAMODB_TABLE

Usage: s3-bench [OPTIONS]

Options:
      --duration <duration>
          Sets benchmarking duration in seconds [default: 60]
      --concurrency <concurrency>
          Sets the maximum number of operations performed simultaneously [default: 1]
      --key-count <key-count>
          Sets the number of keys to benchmark with [default: 100000]
      --value-size <value-size>
          Sets the value size [default: 256]
      --put-percentage <put-percentage>
          Sets the percentage of put operations [default: 20]
      --prepopulate <prepopulate>
          Prepopulates database with the given percentage of keys [default: 0]
      --plot
          Enables output format suitable for gnuplot
      --no-wal
          Disables WAL
      --block-cache [<CAPACITY> <BLOCK_SIZE>]
          Enables block cache and optionally configures its capacity and block size
      --object-cache [<PART_SIZE> <PATH>]
          Enables object cache and optionally configures its part size and location
  -h, --help
          Print help
  -V, --version
          Print version
```
