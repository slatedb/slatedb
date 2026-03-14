# SlateDB Python UniFFI Bindings

This package contains the low-level Python bindings generated from `bindings/ffi`
with UniFFI.

The raw API is exposed under `slatedb.ffi`. The `slatedb` package root is
intentionally kept open for a future higher-level Python wrapper.

## Layout

- `src/slatedb/__init__.py`: reserved for future idiomatic Python APIs
- `src/slatedb/ffi/__init__.py`: stable raw binding entrypoint
- `src/slatedb/ffi/slatedb.py`: generated UniFFI Python wrapper
- `src/slatedb/ffi/libslatedb_ffi.{so,dylib,dll}`: host-built shared library copied in for local use

## Regenerate Bindings

Run from the repository root:

```bash
./scripts/generate_python_bindings.sh
```

The script builds `slatedb-ffi`, regenerates the Python wrapper using the
workspace-pinned UniFFI CLI, copies the resulting host library beside the
generated module, and then formats `src/slatedb/ffi/slatedb.py` via `uv tool
run ruff format`. This requires `uv` to be installed.

Optional environment variables:

- `CARGO_PROFILE`: `debug` or `release` (`debug` by default)
- `PYTHON_BINDINGS_OUT_DIR`: alternate output directory (`bindings/python/src/slatedb/ffi` by default)
- `CARGO_TARGET_DIR`: alternate Cargo target directory
- `FORMAT_GENERATED_PYTHON`: `1` to format after generation, `0` to skip formatting (`1` by default)
- `PYTHON_FORMATTER_BIN`: formatter package and command to invoke with `uv tool run` (`ruff` by default)

## Local Install

```bash
python3 -m pip install -e bindings/python
```

Then import the raw bindings with:

```python
import slatedb.ffi
```

## Release Packaging

This package layout is intended to remain compatible with future Maturin-based
release work, but release automation and wheel building are intentionally out of
scope for this pass.
