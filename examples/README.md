# SlateDB examples

This directory contains example programs demonstrating how to use [SlateDB](https://github.com/slatedb/slatedb) in various scenarios. Each example is a standalone Rust program that can be run independently. These examples cover basic database operations, checkpoint management, and integration with different object stores. The examples are built in CI to ensure that they don't fall out of sync with the main repository branch.

## Adding new examples

1. Create a new Rust file under `src`.
2. Add the binary definition to the `Cargo.toml` file, the list is ordered alphabetically by example name:
    ```toml
    [[bin]]
    name = "tracing-subscriber"
    path = "src/tracing_subscriber.rs"
    test = false
    ```
3. Add any required dependencies to the `Cargo.toml file.


## Running the examples

1. **Build and Run an example:**

   ```sh
   cargo run --bin full_example
   ```

   Or run any other example by replacing `full_example` with the desired file name (without `.rs`).

2. **S3 example Prerequisites:**
   - For `s3_compatible.rs`, you need access to an S3-compatible service. The example is configured for LocalStack by default.
   - Start LocalStack and create a bucket named `slatedb` before running the example.
