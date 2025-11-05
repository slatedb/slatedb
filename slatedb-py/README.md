<a href="https://slatedb.io">
  <img src="https://github.com/slatedb/slatedb-website/blob/main/assets/png/gh-banner.png?raw=true" alt="SlateDB" width="100%">
</a>

![PyPI](https://img.shields.io/pypi/v/slatedb?style=flat-square)
![Python Versions](https://img.shields.io/pypi/pyversions/slatedb?style=flat-square)
![GitHub License](https://img.shields.io/github/license/slatedb/slatedb?style=flat-square)
<a href="https://slatedb.io">![slatedb.io](https://img.shields.io/badge/site-slatedb.io-00A1FF?style=flat-square)</a>
<a href="https://discord.gg/mHYmGy5MgA">![Discord](https://img.shields.io/discord/1232385660460204122?style=flat-square)</a>

## WARNING

This is alpha software and is not yet ready for production use. Missing features:

- Only uses in-memory object storage
- No range query
- No checkpoints
- No builders
- ... and more

Please see SlateDB's [Python Github issues](https://github.com/slatedb/slatedb/issues?q=is%3Aissue%20state%3Aopen%20label%3Apython) to contribute.

## Introduction

[SlateDB](https://slatedb.io) is an embedded storage engine built as a [log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree). Unlike traditional LSM-tree storage engines, SlateDB writes data to object storage (S3, GCS, ABS, MinIO, Tigris, and so on). Leveraging object storage allows SlateDB to provide bottomless storage capacity, high durability, and easy replication. The trade-off is that object storage has a higher latency and higher API cost than local disk.

To mitigate high write API costs (PUTs), SlateDB batches writes. Rather than writing every `put()` call to object storage, MemTables are flushed periodically to object storage as a string-sorted table (SST). The flush interval is configurable.

`put()` returns a `Future` that resolves when the data is durably persisted. Clients that prefer lower latency at the cost of durability can instead use `put_with_options` with `await_durable` set to `false`.

To mitigate read latency and read API costs (GETs), SlateDB will use standard LSM-tree caching techniques: in-memory block caches, compression, bloom filters, and local SST disk caches.

Checkout [slatedb.io](https://slatedb.io) to learn more.


## Installation

```bash
pip install slatedb
```

## Requirements

- Python 3.10 or higher

## Usage

### Basic Operations

```python
from slatedb import SlateDB

# Create or open a database
db = SlateDB("/path/to/your/database")

# Put a key-value pair
db.put(b"hello", b"world")

# Retrieve a value
value = db.get(b"hello")  # Returns b"world"

# Delete a key-value pair
db.delete(b"hello")

# Always close when done
db.close()
```

### Connecting to an object store based on its URL

```python
from slatedb import SlateDB

# Open the database using an object store URL
db = SlateDB("/tmp/slatedb", url="s3://my-bucket/my-prefix")

# Put a key-value pair
db.put(b"hello", b"world")

# Retrieve a value
value = db.get(b"hello")  # Returns b"world"

# Always close when done
db.close()
```

### Asynchronous API

SlateDB also provides async methods for use with asyncio:

```python
import asyncio
from slatedb import SlateDB

async def main():
    db = SlateDB("/path/to/your/database")

    # Async operations
    await db.put_async(b"hello", b"async world")
    value = await db.get_async(b"hello")
    await db.delete_async(b"hello")

    # Don't forget to close
    db.close()

# Run the async example
asyncio.run(main())
```

### Merge Operator

SlateDB supports merge operations which allow partial updates using an associative operator. Configure a merge operator when opening the database, then use `merge`/`merge_async`:

```python
from slatedb import SlateDB

# Python callable merge operator: concatenate bytes
def concat(existing: bytes | None, value: bytes) -> bytes:
    return (existing or b"") + value

db = SlateDB("/path/to/your/database", merge_operator=concat)
db.merge(b"key", b"a")
db.merge(b"key", b"b")
assert db.get(b"key") == b"ab"
db.close()
```

### Snapshots

Create a consistent, read-only view of the database using `snapshot()`. Reads from the snapshot are isolated from any subsequent writes to the database.

```python
from slatedb import SlateDB

db = SlateDB("/path/to/your/database")

# Write data and take a snapshot
db.put(b"key1", b"original")
snap = db.snapshot()

# Modify the database after creating the snapshot
db.put(b"key1", b"modified")

# Snapshot sees original state; DB sees latest
assert snap.get(b"key1") == b"original"
assert db.get(b"key1") == b"modified"

# Snapshots support range scans and async reads as well
_ = list(snap.scan(b"key"))

# Release resources when done (optional; also handled by GC)
snap.close()
db.close()
```

### Transactions

SlateDB supports multi-operation, atomic transactions with two isolation levels:

- SI (Snapshot Isolation): prevents write-write conflicts.
- SSI (Serializable Snapshot Isolation): additionally detects read-write and phantom range conflicts.

```python
from slatedb import SlateDB, TransactionError

db = SlateDB("/path/to/your/database")

# Snapshot Isolation (default)
txn = db.begin()  # or db.begin("si")
txn.put(b"k1", b"v1")
assert txn.get(b"k1") == b"v1"   # read your own writes
txn.commit()

# Serializable Snapshot Isolation
txn1 = db.begin("ssi")
txn1.put(b"k2", b"v2")

txn2 = db.begin("ssi")
_ = txn2.get(b"k2")            # reads old value
txn2.put(b"k3", b"v3")

txn1.commit()                   # makes k2 visible

try:
    txn2.commit()               # conflicts with txn1's write
except TransactionError:
    pass

db.close()
```

### Range Scans and Seek

Range scans return an iterator. Materialize with `list(...)` or iterate lazily. You can optionally use `seek(key)` to move the iterator forward within the original range.

```python
from slatedb import SlateDB, InvalidError

db = SlateDB("/path/to/your/database")
db.put(b"a1", b"v1")
db.put(b"a2", b"v2")
db.put(b"a3", b"v3")

# Iterate all keys with prefix 'a' lazily
it = db.scan(b"a")
print(next(it))         # (b"a1", b"v1")

# Seek forward to the first key >= b"a3"
it.seek(b"a3")
print(next(it))         # (b"a3", b"v3")

# Seeking backwards (<= last returned) raises InvalidError
try:
    it.seek(b"a2")
except InvalidError:
    pass

db.close()
```

### Admin API

Use the Admin API to create and list checkpoints without holding a writer instance.

```python
from slatedb import SlateDB, SlateDBAdmin, SlateDBReader

path = "/tmp/slatedb"

# Prepare some data
db = SlateDB(path)
db.put(b"keyA", b"valueA")
db.close()

# Admin supports the same object store loading strategies (url or env_file)
admin = SlateDBAdmin(path, url="s3://my-bucket/my-prefix")

# Create a detached checkpoint (optionally set lifetime in ms)
cp = admin.create_checkpoint(lifetime=60_000)
print(cp["id"], cp["manifest_id"])  # checkpoint id + manifest id

# List all known checkpoints
checkpoints = admin.list_checkpoints()
print([c["id"] for c in checkpoints])

# Reads pinned to a checkpoint don't see later writes
db2 = SlateDB(path)
db2.put(b"keyB", b"valueB")
db2.close()

# Reader supports the same strategies too (url or env_file)
reader = SlateDBReader(path, url="s3://my-bucket/my-prefix", checkpoint_id=cp["id"])  # pinned to cp
assert reader.get(b"keyA") == b"valueA"
assert reader.get(b"keyB") is None
reader.close()
```

## Error Handling

SlateDB’s Python API maps errors to specific Python exceptions that mirror error kinds in the Rust core:

- `TransactionError` – transaction conflict; retry or drop the operation
- `ClosedError` – the database/reader is closed or fenced; create a new instance
- `UnavailableError` – storage/network unavailable; retry or drop the operation
- `InvalidError` – invalid arguments, configuration, or misuse of the API
- `DataError` – persisted data invalid or incompatible version
- `InternalError` – unexpected internal error; please report

Examples:

```python
from slatedb import (
    SlateDB,
    TransactionError, ClosedError, UnavailableError,
    InvalidError, DataError, InternalError,
)

db = SlateDB("/tmp/slatedb")

# Invalid arguments raise InvalidError
try:
    db.put(b"", b"value")
except InvalidError:
    pass

# Catch specific operational errors
try:
    db.close()
    db.put(b"k", b"v")
except ClosedError:
    print("database is closed")

# Or catch broadly
try:
    value = db.get(b"k")
except (TransactionError, ClosedError, UnavailableError, InvalidError, DataError, InternalError) as e:
    print(f"SlateDB error: {e}")
```

## Documentation

- [SlateDB core documentation](https://slatedb.io/docs/introduction) - Comprehensive guide to understand how SlateDB works.
- [API Reference](https://github.com/slatedb/slatedb/tree/main/slatedb-py) - Detailed Python API documentation embedded in the source code.

## Contributing

SlateDB's Python bindings use [Uv](https://docs.astral.sh/uv/) to manage the development environment. You can install Uv following the steps on its website. Once you've installed Uv, run `uv venv` to create a virtual environment.

### Installing dependencies

Run `uv pip install` to install all dependencies. If you only want test dependencies, you can run `uv pip install -e .[test]`.

### Running tests

Run `uv run pytest` to run all tests.

### Building the project

SlateDB's Python bindings use [Maturin](https://www.maturin.rs/) to link the Rust codebase with the Python codebase.

1. Install Maturin by running `uv tool install maturin`.
2. Build the project with Maturin by running `uv run maturin develop`.

## License

SlateDB is licensed under the Apache License, Version 2.0.

## Foundation

SlateDB is a member of the [Commonhaus Foundation](https://www.commonhaus.org/).

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/commonhaus/artwork/blob/main/foundation/brand/png/CF_logo_horizontal_single_reverse_200px.png?raw=true">
  <img src="https://github.com/commonhaus/artwork/blob/main/foundation/brand/png/CF_logo_horizontal_single_default_200px.png?raw=true">
</picture>
