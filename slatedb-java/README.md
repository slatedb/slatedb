# SlateDB Java (Stub)

This is a minimal Java 24 FFI-based binding for SlateDB backed by `slatedb-c`.

**Build Steps**
1. Build the native library from the repo root:
   `cargo build -p slatedb-c`
2. Locate the native library artifact:
   `macOS: target/debug/libslatedb_c.dylib`, `Linux: target/debug/libslatedb_c.so`, `Windows: target/debug/slatedb_c.dll`
3. Build the Java binding:
   `./gradlew build` (from `slatedb-java`)

**Run Notes**
- The native library name is `slatedb_c` (e.g. `libslatedb_c.dylib` on macOS).
- Set `-Djava.library.path=...` to the directory containing the native library, or call `SlateDb.loadLibrary("<absolute path>")`.
- When running code that uses FFI, you may need `--enable-native-access=ALL-UNNAMED`.
