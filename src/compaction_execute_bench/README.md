
# SlateDB Compaction Benchmarking Tool

This tool benchmarks the compaction process in SlateDB by loading data into an object store (e.g., Amazon S3), running a compaction job on the data, and optionally clearing the data afterward.


## Environment Variables

Set the following environment variables to configure the benchmark:

`AWS_ACCESS_KEY_ID` : Your AWS access key.

`AWS_SECRET_ACCESS_KEY` : Your AWS secret access key.

`BUCKET` : The name of the S3 bucket to use.

`REGION` : AWS region (default: us-west-2).

`SST_BASE_PATH` : Base path in the bucket where SSTables will be stored (default: /compaction-execute-bench).

`MODE` : Operation mode (LOAD, RUN, CLEAR).

`SST_BYTES` : Size of each SSTable in bytes (default: 1073741824 or 1GiB).

`NUM_SSTS` : Number of SSTables to use (default: 4).

`KEY_BYTES` : Size of keys in bytes (default: 32).

`VAL_BYTES` : Size of values in bytes (default: 224).


## Features

- Load Data: Generate and load random key-value pairs into SSTables stored in an object store.
- Run Compaction: Execute a compaction job that merges multiple SSTables into a smaller number of SSTables.
- Clear Data: Delete SSTables from the object store after benchmarking.


## Installation

Clone the repository:

```bash
  git clone https://github.com/your-repo/slatedb.git
  cd slatedb
  cargo build --release
```
    
## Examples

1. Load Data : 
To load random data into the object store, set MODE=LOAD and run:
```rust
// Example 1: Load Data
export MODE=LOAD

// Run the command
cargo run --bin compaction-execute-bench --release
```

2. Run Compaction : 
To perform a compaction job on the loaded SSTables, set MODE=RUN and run:
```rust
// Example 2: Run Compaction
export MODE=RUN

// Run the command
cargo run --bin compaction-execute-bench --release
```

3. Clear Data : 
To delete all SSTables from the object store, set MODE=CLEAR and run:
```rust
// Example 3: Clear Data
export MODE=CLEAR

// Run the command
cargo run --bin compaction-execute-bench --release
```

