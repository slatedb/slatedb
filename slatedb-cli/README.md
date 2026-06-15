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

#### Scan

Dumps the key/value pairs of the database to stdout:

```bash
slatedb --path <PATH> scan [OPTIONS]
```

The database is opened as a non-fencing reader, so `scan` is safe to point at a database that a service is actively writing. Output is one entry per line as `<key>\t<value>`.

Options:
- `--prefix <K>`: Restrict the scan to keys starting with these bytes. Conflicts with `--from`/`--to`.
- `--from <K>`: Inclusive lower bound of the scanned key range.
- `--to <K>`: Exclusive upper bound of the scanned key range.
- `--key <auto|hex|utf8>`: How keys are rendered, and how `--prefix`/`--from`/`--to` are parsed. Default `auto`.
- `--value <auto|hex|utf8|len|none>`: How values are rendered. `none` prints keys only. Default `auto`.
- `--max-keys <N>`: Stop after emitting `N` entries.
- `--count`: Print only `<N> entries, <B> bytes` instead of the entries themselves. When combined with `--max-keys`, only the entries up to the cap are counted.
- `--checkpoint <UUID>`: Scan an existing checkpoint for a point-in-time scan that needs only read access to the store. Without it, the reader writes a transient checkpoint, so it needs write access to the store.

Encoding rules:
- `--key`/`--value` control both how the bound arguments are parsed and how keys/values are rendered. In `hex`, bound arguments are hex digits with an optional `0x` prefix. In `utf8`, they are taken literally. In `auto`, a bound is hex-decoded when it starts with `0x`/`0X` and taken literally otherwise.
- In `auto`, a key is rendered as bare text only when it is clean UTF-8 (no control characters) and does not itself start with `0x`; otherwise it is rendered as `0x`-prefixed lowercase hex. For example a key `user/42` prints as `user/42`, while a key with bytes `00 42` prints as `0x0042`.
- For values in `auto`, clean text is shown as-is and anything else as `<binary N bytes>`; switch to `--value hex` to see the raw bytes.

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

### Scanning the Key/Value Pairs

```bash
slatedb --path my-database scan --prefix user --value utf8
```

Count the entries under a binary key prefix without printing them:

```bash
slatedb --path my-database scan --prefix 0x40 --count
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
