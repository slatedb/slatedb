# SlateDB UniFFI Java Bindings

This module contains the raw Java bindings generated from `bindings/ffi` with
`uniffi-bindgen-java`. It is intentionally separate from the legacy
`slatedb-java` binding.

## Requirements

- Rust toolchain from this repository
- Java 21+
- `uniffi-bindgen-java` installed if you want to regenerate bindings

## Regenerate Bindings

From `bindings/java`:

```bash
./generate.sh
```

The script builds `slatedb-ffi`, runs `uniffi-bindgen-java` in library mode,
and copies the generated `io/slatedb/*.java` files into `src/main/java`.

## Test

From `bindings/java`:

```bash
./gradlew test
```

The Gradle test task builds `slatedb-ffi` in debug mode and points JNA at
`../../target/debug` via `jna.library.path`.

## Native Library Loading

The generated bindings load the native library named `slatedb_ffi` via JNA.

- To add a search path, set `-Djna.library.path=/path/to/dir`
- To override the library name, set
  `-Duniffi.component.slatedb.libraryOverride=custom_name`

## API Notes

- The exported API mirrors the UniFFI surface from `bindings/ffi`
- Rust async methods are exposed as `CompletableFuture`
- `Db.shutdown()` closes the database itself
- `close()` on FFI-backed objects is the `AutoCloseable` handle cleanup path
