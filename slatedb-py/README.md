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

## Error Handling

Most methods raise `ValueError` for errors like empty keys or database operation failures:

```python
try:
    db.put(b"", b"This will fail")
except ValueError as e:
    print(f"Error: {e}")
```

## Documentation

There's no Python documentation for SlateDB. Checkout [slatedb.io](https://slatedb.io) for architecture and read the Python binding source code for API documentation.

## License

SlateDB is licensed under the Apache License, Version 2.0.

## Foundation

SlateDB is a member of the [Commonhaus Foundation](https://www.commonhaus.org/).

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/commonhaus/artwork/blob/main/foundation/brand/png/CF_logo_horizontal_single_reverse_200px.png?raw=true">
  <img src="https://github.com/commonhaus/artwork/blob/main/foundation/brand/png/CF_logo_horizontal_single_default_200px.png?raw=true">
</picture>