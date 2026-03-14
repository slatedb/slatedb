# SlateDB JNA Bindings

This module contains low-level Java bindings generated from `bindings/ffi` with UniFFI and `uniffi-bindgen-java`.

The published artifact name is `slatedb-jna`. The plain `slatedb` artifact name is intentionally left open for a future Panama/FFM-based Java binding.

## Scope

This is a mechanical JNA binding over the Rust UniFFI surface. It is meant for:

- direct access to the low-level FFI API
- validation and debugging of the UniFFI contract from Java
- future higher-level Java wrappers built on top of the generated code

The generated API lives in the `io.slatedb.jna.ffi` package. The parent `io.slatedb.jna`
package is intentionally left open for a cleaner wrapper layer on top of the raw bindings.

## Prerequisites

- Rust toolchain from this repository
- Java 21+
- `uniffi-bindgen-java` `0.2.1` if you need to regenerate the bindings

## Regenerate Bindings

Run from the repository root:

```bash
./scripts/generate_java_bindings.sh
```

The script regenerates the Java sources and then formats them via `bindings/java`'s
`formatGeneratedJava` Gradle task.

Optional environment variables:

- `UNIFFI_BINDGEN_JAVA_BIN`: path to the generator binary
- `CARGO_PROFILE`: `debug` or `release` (`debug` by default)
- `JAVA_BINDINGS_OUT_DIR`: alternate output directory (`bindings/java/src/main/java` by default)

Generated files are checked in. Normal Java compilation does not require the generator unless you are regenerating the low-level bindings.

## Build

Build the JAR from the repository root with:

```bash
./bindings/java/gradlew jar
```

The JAR is written to:

```text
bindings/java/build/libs/slatedb-jna-<version>.jar
```

## Runtime Native Library

This module does not bundle native libraries in v1. Consumers must provide a built `slatedb_ffi` dynamic library separately.

You can let JNA resolve the library through its normal search path, or override it explicitly with:

```bash
-Duniffi.component.slatedb.libraryOverride=/absolute/path/to/libslatedb_ffi.dylib
```

On Linux use `libslatedb_ffi.so`, and on Windows use `slatedb_ffi.dll`.

To build the host native library locally:

```bash
cargo build -p slatedb-ffi
```
