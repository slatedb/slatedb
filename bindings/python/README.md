# SlateDB Python Binding

`bindings/python` contains the official UniFFI-based Python package for SlateDB.

## Requirements

- Python 3.10 or newer

## Install

```bash
pip install slatedb
```

## API Model

- `ObjectStore.resolve(...)` opens an object store from a URL such as `memory:///`
- `DbBuilder` opens a writable database and `DbReaderBuilder` opens a read-only reader
- most database operations are `async` and should be used with `asyncio`
- `WriteBatch` is mutated synchronously and then consumed by `db.write(...)`
- `db.shutdown()` and `reader.shutdown()` explicitly close native resources

## Quick Start

```python
import asyncio

from slatedb.uniffi import (
    DbBuilder,
    IsolationLevel,
    ObjectStore,
    WriteBatch,
)


async def main() -> None:
    store = ObjectStore.resolve("memory:///")
    builder = DbBuilder("demo-db", store)
    db = await builder.build()

    try:
        await db.put(b"user:1", b"Alice")
        value = await db.get(b"user:1")
        assert value == b"Alice"

        batch = WriteBatch()
        batch.put(b"user:2", b"Bob")
        batch.put(b"user:3", b"Carol")
        await db.write(batch)

        scan = await db.scan_prefix(b"user:")
        while (row := await scan.next()) is not None:
            print(row.key, row.value)

        tx = await db.begin(IsolationLevel.SERIALIZABLE_SNAPSHOT)
        await tx.put(b"user:4", b"Dora")
        await tx.commit()
    finally:
        await db.shutdown()


asyncio.run(main())
```

## Local Development

This package reuses the shared Rust UniFFI crate at `../uniffi/Cargo.toml`. The generated Python module under `slatedb/uniffi/_slatedb_uniffi/` is build output and should not be edited by hand.

### Editable Build

From the repository root:

```bash
cd bindings/python
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install maturin "uniffi-bindgen==0.29.5" ruff
maturin develop
ruff check .
python -c "import slatedb; import slatedb.uniffi; from slatedb.uniffi import *"
```

### Run Tests

From `bindings/python` after building the extension with test extras:

```bash
maturin develop --extras test
pytest
```

### Build Release Artifacts

From `bindings/python`:

```bash
maturin build
maturin sdist
```

Editable builds generate `slatedb/uniffi/_slatedb_uniffi/` in-tree. If you switch from `maturin develop` to `maturin build` in the same checkout, remove that generated directory first or run the artifact build from a fresh checkout.

## License

SlateDB is licensed under the Apache License, Version 2.0.
