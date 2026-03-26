# SlateDB Node Binding

`bindings/node` contains the official Node.js / TypeScript binding for SlateDB.

The published package is:

```text
@slatedb/uniffi
```

## Requirements

- Node.js 20 or newer

## Install

```bash
npm install @slatedb/uniffi
```

## API Model

- `ObjectStore.resolve(...)` resolves an object store from a URL such as `memory:///`
- `DbBuilder` opens a writable database and `DbReaderBuilder` opens a read-only reader
- most database operations are `async`
- byte values use `Uint8Array`; Node `Buffer` values work because `Buffer` extends `Uint8Array`
- UniFFI-backed objects should be released with `uniffiDestroy()` when you are done with them

## Quick Start

```js
import { DbBuilder, ObjectStore } from "@slatedb/uniffi";

const store = ObjectStore.resolve("memory:///");
const builder = new DbBuilder("demo-db", store);

let db;
try {
  db = await builder.build();

  await db.put(Buffer.from("hello"), Buffer.from("world"));
  const value = await db.get(Buffer.from("hello"));

  console.log(Buffer.from(value ?? []).toString("utf8"));
  await db.shutdown();
} finally {
  db?.uniffiDestroy();
  builder.uniffiDestroy();
  store.uniffiDestroy();
}
```

## Native Prebuilds

Release builds publish one npm package that bundles native libraries for:

- `darwin-x64`
- `darwin-arm64`
- `linux-x64-gnu`
- `linux-arm64-gnu`
- `linux-x64-musl`
- `linux-arm64-musl`
- `win32-x64`
- `win32-arm64`

## Local Development

Install the generator:

```bash
cargo install uniffi-bindgen-node-js --git https://github.com/criccomini/uniffi-bindgen-node-js.git --branch main --locked
```

Then generate a local package into a throwaway directory:

```bash
PACKAGE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/slatedb-node.XXXXXX")"
./scripts/generate-node-package.sh --out-dir "${PACKAGE_DIR}" --version 0.0.0-dev --stage-host-prebuild
```

To inspect or pack the generated package:

```bash
cd "${PACKAGE_DIR}"
npm install --no-package-lock
npm pack
```

The generated package is release-time output and should not be edited by hand or committed.

## License

SlateDB is licensed under the Apache License, Version 2.0.
