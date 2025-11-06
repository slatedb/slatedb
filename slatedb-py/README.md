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

## Usage

## Documentation

- [SlateDB core documentation](https://slatedb.io/docs/introduction) - Comprehensive guide to understand how SlateDB works.
- [API Reference](https://github.com/slatedb/slatedb/tree/main/slatedb-py) - Detailed Python API documentation embedded in the source code.

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
