# SlateDB Node Binding

`bindings/node` contains the official Node.js package for SlateDB.

## Install

Package:

```text
@slatedb/uniffi
```

Requirements:

- Node.js 20 or newer

Install from npm:

```bash
npm install @slatedb/uniffi
```

## API Model

- `ObjectStore.resolve(...)` opens an object store from a URL such as `memory:///` or `file:///...`
- `DbBuilder` opens a writable database and `DbReaderBuilder` opens a read-only reader
- keys and values are binary; pass `Buffer` or `Uint8Array`
- most database operations are async and should be awaited
- builders are single-use; `Db` and `DbReader` stay open until `shutdown()` resolves
- native-backed handles also expose `dispose()` for deterministic cleanup after `shutdown()` or when abandoning a builder

## Quick Start

```js
import assert from "node:assert/strict";
import { DbBuilder, ObjectStore } from "@slatedb/uniffi";

async function main() {
  const store = ObjectStore.resolve("memory:///");
  let db;

  try {
    const builder = new DbBuilder("demo-db", store);
    try {
      db = await builder.build();
    } finally {
      builder.dispose();
    }

    const key = Buffer.from("hello");
    const value = Buffer.from("world");

    await db.put(key, value);

    const read = await db.get(key);
    assert.deepEqual(read, value);

    console.log(Buffer.from(read).toString("utf8"));
  } finally {
    if (db != null) {
      await db.shutdown();
      db.dispose();
    }
    store.dispose();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
```

Replace `memory:///` with any object store URL supported by Rust's [`object_store`](https://docs.rs/object_store/latest/object_store/fn.parse_url_opts.html) crate.

## Local Development

The package is generated from the UniFFI `slatedb-uniffi` cdylib using [`uniffi-bindgen-node-js`](https://crates.io/crates/uniffi-bindgen-node-js).

You only need these tools when regenerating bindings, running tests from this repository, or packing the npm artifact locally:

- Node.js 20 or newer
- Rust toolchain for this repository
- `uniffi-bindgen-node-js` on `PATH`

Install the generator with:

```bash
cargo install uniffi-bindgen-node-js
```

Install the package dependency used by the generated bindings with:

```bash
npm --prefix bindings/node install
```

### Regenerate Bindings

From the repository root:

```bash
npm --prefix bindings/node run build
```

This command:

1. builds the host `slatedb-uniffi` library
2. runs `uniffi-bindgen-node-js`
3. copies the generated package files into `bindings/node`
4. stages the host native library under `bindings/node/prebuilds/<target>/`

Generated API files are written into `bindings/node` and are not committed. `package.json`, `build.mjs`, and this `README.md` are maintained by hand.

### Run Tests

From the repository root:

```bash
npm --prefix bindings/node test
```

The test script rebuilds the package and then runs `node --test` inside `bindings/node`.

### Reproduce The PR CI Flow

From the repository root, this mirrors the Node validation done in `.github/workflows/pr.yaml`:

```bash
npm --prefix bindings/node ci
npm --prefix bindings/node run build
(cd bindings/node && node --test)
git diff --exit-code -- bindings/node
rm -rf /tmp/slatedb-node-pack
mkdir -p /tmp/slatedb-node-pack
(cd bindings/node && npm pack --pack-destination /tmp/slatedb-node-pack)
TARBALL="$(find /tmp/slatedb-node-pack -maxdepth 1 -name '*.tgz' | head -n 1)"
test -n "${TARBALL}"
tar -tf "${TARBALL}" | grep -Fx 'package/index.js'
tar -tf "${TARBALL}" | grep -Fx 'package/index.d.ts'
tar -tf "${TARBALL}" | grep -Fx 'package/slatedb.js'
tar -tf "${TARBALL}" | grep -Fx 'package/slatedb.d.ts'
tar -tf "${TARBALL}" | grep -Fx 'package/slatedb-ffi.js'
tar -tf "${TARBALL}" | grep -Fx 'package/slatedb-ffi.d.ts'
tar -tf "${TARBALL}" | grep -Fx 'package/runtime/ffi-types.js'
tar -tf "${TARBALL}" | grep -Fx 'package/prebuilds/linux-x64-gnu/libslatedb_uniffi.so'
```

## Packaging And Runtime Notes

The published `@slatedb/uniffi` tarball contains generated JavaScript and TypeScript bindings plus bundled native libraries. Consumers install one npm package; there is no separate Rust build step or native download in normal usage.

At runtime, the package loads the native library that matches the current host from `prebuilds/<target>/`. The published package currently includes:

- `linux-x64-gnu`
- `linux-arm64-gnu`
- `darwin-x64`
- `darwin-arm64`
- `win32-x64`
- `win32-arm64`

Linux musl targets are not packaged today. Local builds stage only the host native library; release builds stage all supported targets into the published npm package.

When assembling a release package from a prebuilt native directory, run:

```bash
npm --prefix bindings/node run build -- --prebuilt-dir <dir>
```

The generated API files stay the same; only the packaged native libraries change between local and release builds.
