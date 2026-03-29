# SlateDB Node Binding

`bindings/node` contains the official Node.js package for SlateDB:

```text
@slatedb/uniffi
```

The package is generated from the UniFFI `slatedb-uniffi` cdylib using
[`uniffi-bindgen-node-js`](https://crates.io/crates/uniffi-bindgen-node-js)
and publishes one npm artifact containing generated JavaScript bindings plus
bundled native libraries.

## Status

- `package.json`, `build.mjs`, and this `README.md` are maintained by hand
- generated bindings are written into `bindings/node` and are not committed
- local builds stage one host prebuild under `prebuilds/<target>/`
- release builds stage all supported prebuilds into one npm package

## Build Prerequisites

You only need these when regenerating bindings or packing the npm artifact from
this repository:

- Node.js 20 or newer
- Rust toolchain for this repository
- `uniffi-bindgen-node-js` on `PATH`

## Regenerate

From the repository root:

```bash
npm --prefix bindings/node run build
```

This command:

1. builds the host `slatedb-uniffi` library
2. runs `uniffi-bindgen-node-js`
3. copies the generated package files into `bindings/node`
4. stages the host native library under `bindings/node/prebuilds/<target>/`

## Release Packaging

Release builds publish one `@slatedb/uniffi` package containing native
libraries for:

- `linux-x64-gnu`
- `linux-arm64-gnu`
- `darwin-x64`
- `darwin-arm64`
- `win32-x64`
- `win32-arm64`

The release workflow assembles a prebuilt directory with that layout and then
runs:

```bash
npm --prefix bindings/node run build -- --prebuilt-dir <dir>
```

The generated API files remain the same; only the packaged native libraries
change between local and release builds.
