<a href="https://slatedb.io">
  <img src="https://github.com/slatedb/slatedb-website/blob/main/assets/png/gh-banner.png?raw=true" alt="SlateDB" width="100%">
</a>

<a href="https://pypi.org/project/slatedb/">![PyPI](https://img.shields.io/pypi/v/slatedb?style=flat-square)</a>
<a href="https://slatedb.readthedocs.io">![ReadTheDocs](https://img.shields.io/readthedocs/slatedb?style=flat-square)</a>
![Python Versions](https://img.shields.io/pypi/pyversions/slatedb?style=flat-square)
![GitHub License](https://img.shields.io/github/license/slatedb/slatedb?style=flat-square)
<a href="https://slatedb.io">![slatedb.io](https://img.shields.io/badge/site-slatedb.io-00A1FF?style=flat-square)</a>
<a href="https://discord.gg/mHYmGy5MgA">![Discord](https://img.shields.io/discord/1232385660460204122?style=flat-square)</a>

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

## Features

- Support for in-memory object store and object stores (S3, GCS, ABS, MinIO, memory)
- Sync and async APIs
- Range scans with iteration and `seek`
- Snapshots for consistent, read-only views
- Transactions with snapshot isolation (SI) and serializable snapshot isolation (SSI)
- Atomic batched writes
- Merge operator (user-defined Python callable)
- Per-operation time-to-live (TTL)
- Reader for read-only access with optional checkpoint pinning
- Admin APIs for manifests, checkpoints, clones, garbage collection, and sequence↔timestamp mapping

## Usage

The example below demonstrates common features in a single script.

```python
from __future__ import annotations

from slatedb import (
    SlateDB,
    SlateDBReader,
    SlateDBAdmin,
    WriteBatch,
)


# Optional: define a merge operator. This simple example is last-write-wins.
def concat(existing: bytes | None, value: bytes) -> bytes:
    return (existing or b"") + value

# Open a database using the in-memory object store
db = SlateDB("/tmp/slatedb-demo", url="memory:///", merge_operator=concat)

# Basic CRUD
db.put(b"user:1", b"Alice")
assert db.get(b"user:1") == b"Alice"

# Per-op TTL and durability control
db.put_with_options(b"temp", b"ok", ttl=5_000, await_durable=False)

# Batched writes
wb = WriteBatch()
wb.put(b"batch:1", b"one")
wb.delete(b"temp")
db.write(wb)

# Transactions (SSI or SI). Operations buffer until commit.
txn = db.begin("ssi")
txn.put(b"user:2", b"Bob")
txn.merge(b"counter:visits", b"1")  # uses the configured merge operator
txn.commit()

# Snapshots provide consistent, read-only views
snap = db.snapshot()
assert snap.get(b"user:2") == b"Bob"

# Range scans (prefix scan when end omitted). Iterator supports seek.
it = db.scan(b"user:")
first = next(it)
it.seek(b"user:2")
rest = list(it)

# Advanced scan options (read-ahead, caching, durability filter)
_ = list(db.scan_with_options(b"user:", cache_blocks=True, read_ahead_bytes=1 << 20))

# Flush
db.flush_with_options("wal")

# Get metrics
metrics = db.metrics()

# Create a durable checkpoint and read it with a read-only reader
ckpt = db.create_checkpoint(scope="durable")
reader = SlateDBReader("/tmp/slatedb-demo", url="memory:///", checkpoint_id=ckpt["id"])
_ = list(reader.scan(b"user:"))
reader.close()

# Admin: list checkpoints, read manifests, GC, and sequence/timestamp mapping
admin = SlateDBAdmin("/tmp/slatedb-demo", url="memory///")
_ = admin.list_checkpoints()
_ = admin.read_manifest()
admin.run_gc_once(manifest_min_age=0, wal_min_age=0, compacted_min_age=0)

db.close()
```

## Contributing

SlateDB's Python bindings use [uv](https://docs.astral.sh/uv/) to manage the development environment. You can install `uv` following the steps on its website. Once you've installed `uv`, run `uv venv` to create a virtual environment.

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
