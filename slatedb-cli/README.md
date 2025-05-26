# SlateDB CLI

A command-line interface for interacting with SlateDB databases.

## Installation

### From Cargo

```bash
cargo install --locked slatedb-cli
```

### From Source

Clone the repository and build from source:

```bash
git clone https://github.com/slatedb/slatedb.git
cd slatedb
cargo build --release -p slatedb-cli
```

The binary will be available at `target/release/slatedb`.

## Usage

The SlateDB CLI provides various commands for interacting with SlateDB databases:

```bash
slatedb [OPTIONS] --path <PATH> <COMMAND>
```

### Global Options

- `-e, --env-file <ENV_FILE>`: A .env file to use to supply environment variables
- `-p, --path <PATH>`: The path in the object store to the root directory, starting from within the object store bucket (specified when configuring the object store provider)
- `-h, --help`: Print help information
- `-V, --version`: Print version information

### Commands

#### Read Manifest

Reads the latest manifest file and outputs a readable string representation:

```bash
slatedb --path <PATH> read-manifest [OPTIONS]
```

Options:
- `-i, --id <ID>`: Specify a specific manifest ID to read. If not specified, the latest manifest will be returned.

#### List Manifests

Lists all available manifests:

```bash
slatedb --path <PATH> list-manifests [OPTIONS]
```

Options:
- `-s, --start <START>`: Optionally specify a start ID for the range of manifests to lookup
- `-e, --end <END>`: Optionally specify an end ID for the range of manifests to lookup

#### Checkpoints

SlateDB supports creating, refreshing, deleting, and listing checkpoints:

##### Create Checkpoint

```bash
slatedb --path <PATH> create-checkpoint [OPTIONS]
```

Options:
- `-l, --lifetime <LIFETIME>`: Optionally specify a lifetime for the created checkpoint in a human-friendly format (e.g., "7days 30min 10s"). Without this, the checkpoint has no expiry.
- `-s, --source <SOURCE>`: Optionally specify the UUID of an existing checkpoint to use as the base for the newly created checkpoint. If not provided, the checkpoint will be taken against the latest manifest.

##### Refresh Checkpoint

```bash
slatedb --path <PATH> refresh-checkpoint --id <ID> [OPTIONS]
```

Options:
- `-i, --id <ID>`: The UUID of the checkpoint to refresh (required)
- `-l, --lifetime <LIFETIME>`: Optionally specify a new lifetime for the checkpoint. Without this, the checkpoint is updated with no expiry.

##### Delete Checkpoint

```bash
slatedb --path <PATH> delete-checkpoint --id <ID>
```

Options:
- `-i, --id <ID>`: The UUID of the checkpoint to delete (required)

##### List Checkpoints

```bash
slatedb --path <PATH> list-checkpoints
```

#### Garbage Collection

SlateDB provides garbage collection functionality for various resources:

##### Run Garbage Collection Once

```bash
slatedb --path <PATH> run-garbage-collection --resource <RESOURCE> --min-age <MIN_AGE>
```

Options:
- `-r, --resource <RESOURCE>`: The type of resource to clean up. Possible values: `manifest`, `wal`, `compacted`
- `-m, --min-age <MIN_AGE>`: The minimum age of the resource before considering it for garbage collection (e.g., "24h", "7days")

##### Schedule Garbage Collection

```bash
slatedb --path <PATH> schedule-garbage-collection [OPTIONS]
```

Options (at least one required):
- `--manifest <MANIFEST>`: Configuration for manifest garbage collection, in the format `min_age=<duration>,period=<duration>` (e.g., `min_age=7days,period=1day`)
- `--wal <WAL>`: Configuration for WAL garbage collection, in the format `min_age=<duration>,period=<duration>`
- `--compacted <COMPACTED>`: Configuration for compacted SST garbage collection, in the format `min_age=<duration>,period=<duration>`

## Examples

### Reading the Latest Manifest

```bash
slatedb --path my-database read-manifest
```

### Listing Manifests

```bash
slatedb --path my-database list-manifests
```

### Creating a Checkpoint with 7-Day Expiry

```bash
slatedb --path my-database create-checkpoint --lifetime "7days"
```

### Running Garbage Collection on WAL Files

```bash
slatedb --path my-database run-garbage-collection --resource wal --min-age "24h"
```

### Scheduling Periodic Garbage Collection

```bash
slatedb --path my-database schedule-garbage-collection --manifest "min_age=7days,period=1day" --wal "min_age=24h,period=6h"
```

## Environment Variables

SlateDB CLI uses environment variables for object store configuration. You can either set these in your environment or provide them through an .env file using the `--env-file` option.

Required environment variables will depend on your chosen object store provider. See the SlateDB documentation for more details.

## License

Apache-2.0
