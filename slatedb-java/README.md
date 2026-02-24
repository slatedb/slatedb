<a href="https://slatedb.io">
  <img src="https://github.com/slatedb/slatedb/blob/main/website/public/img/slatedb-gh-banner.png?raw=true" alt="SlateDB" width="100%">
</a>

# SlateDB Java

SlateDB Java is a Java 24 binding for SlateDB built on the `slatedb-c` FFI library. It uses the Java Foreign Function and Memory (FFM) API to call native SlateDB directly, so there is no JNI wrapper layer.

## Introduction

[SlateDB](https://slatedb.io) is an embedded storage engine built as a log-structured merge-tree that writes data to object storage (S3, GCS, ABS, MinIO, Tigris, and more). These Java bindings expose the core SlateDB API (get/put/delete, scans, batching, and readers) with minimal overhead.

## Requirements

- Java 24 (required for the FFM API used by these bindings)
- Rust toolchain (to build `slatedb-c`)
- A supported object store (for local development, use `memory://` or `file://` URLs)

## Build Native Libraries

`slatedb-java` embeds native `slatedb-c` binaries into the JAR and loads them from classpath resources at runtime. You do not need to set `java.library.path`.

By default, Gradle builds the host platform native library and packages it:

```bash
./gradlew jar
```

To build and package all supported OS/architecture targets:

```bash
./gradlew jar -Pslatedb.native.targets=all
```

Supported platform IDs:
- `linux-x86_64`
- `linux-aarch64`
- `macos-x86_64`
- `macos-aarch64`
- `windows-x86_64`
- `windows-aarch64`

You can also pass a custom subset:

```bash
./gradlew jar -Pslatedb.native.targets=linux-x86_64,macos-aarch64
```

The JAR is written to:

```
slatedb-java/build/libs/slatedb-<version>.jar
```

To crossbuild on mac, you must:

1. Make sure `rustup target add` is used to add all platform IDs you want to build.
2. Run the following to build Linux targets on Mac:
   ```bash
   xcode-select --install
   brew tap messense/macos-cross-toolchains
   brew install aarch64-unknown-linux-gnu
   rustup target add aarch64-unknown-linux-gnu
   ```
3. Windows targets can't easily be built on Mac. `x86_64-pc-windows-gnu` can be used as a workaround for `windows-x86_64` but requires the MinGW toolchain to be installed and in your `PATH`.

## Hello World

1. Create `HelloSlateDb.java`:

```java
import io.slatedb.SlateDbKeyValue;
import io.slatedb.SlateDbScanIterator;
import io.slatedb.SlateDb;
import io.slatedb.SlateDbWriteBatch;
import io.slatedb.SlateDbConfig;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class HelloSlateDb {
    public static void main(String[] args) throws Exception {
        // Initialize logging (native library loads from bundled resources)
        SlateDb.initLogging(SlateDbConfig.LogLevel.INFO);

        // Local database path and local object store
        Path dbPath = Files.createTempDirectory("slatedb-java-db");
        Path objectStoreRoot = Files.createTempDirectory("slatedb-java-store");
        String objectStoreUrl = "file://" + objectStoreRoot.toAbsolutePath();

        byte[] key = "hello-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "hello-value".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(dbPath.toString(), objectStoreUrl, null)) {
            // Put + Get
            db.put(key, value);
            byte[] loaded = db.get(key);
            System.out.println("loaded=" + new String(loaded, StandardCharsets.UTF_8));

            // Delete
            db.delete(key);

            // Batch write
            try (SlateDbWriteBatch batch = SlateDb.newWriteBatch()) {
                batch.put("hello-a".getBytes(StandardCharsets.UTF_8), "value-a".getBytes(StandardCharsets.UTF_8));
                batch.put("hello-b".getBytes(StandardCharsets.UTF_8), "value-b".getBytes(StandardCharsets.UTF_8));
                db.write(batch);
            }

            // Scan by prefix
            try (SlateDbScanIterator iter = db.scanPrefix("hello-".getBytes(StandardCharsets.UTF_8))) {
                SlateDbKeyValue kv;
                while ((kv = iter.next()) != null) {
                    System.out.println(
                        new String(kv.key(), StandardCharsets.UTF_8) + "=" +
                        new String(kv.value(), StandardCharsets.UTF_8)
                    );
                }
            }
        }
    }
}
```

2. Compile (from the repository root):

```bash
javac -cp slatedb-java/build/libs/slatedb-<version>.jar HelloSlateDb.java
```

3. Run:

```bash
java --enable-native-access=ALL-UNNAMED \
  -cp slatedb-java/build/libs/slatedb-<version>.jar:. \
  HelloSlateDb
```

## API Overview

Core types:
- `SlateDb`: Read/write database handle. Always close it (try-with-resources recommended).
- `SlateDbConfig`: Options and enums for reads, writes, scans, and readers.
- `SlateDbReader`: Read-only handle for snapshot-style reads.
- `SlateDbWriteBatch`: Batch of put/delete operations written atomically.
- `SlateDbScanIterator`: Iterator for range scans and prefix scans.

Key operations:
- `put`, `get`, `delete` for basic CRUD.
- `write(SlateDbWriteBatch)` for atomic batches.
- `scan(startKey, endKey)` and `scanPrefix(prefix)` for range and prefix scans.
- `flush()` to force writes to object storage.

## Settings and Configuration

You can configure SlateDB with JSON settings or a builder:

```java
String settings = SlateDb.settingsDefault();

try (SlateDb.Builder builder = SlateDb.builder(dbPath.toString(), objectStoreUrl, null)) {
    builder.withSettingsJson(settings)
           .withSstBlockSize(SlateDbConfig.SstBlockSize.KIB_4);
    try (SlateDb db = builder.build()) {
        // use db
    }
}
```

The settings helpers:
- `settingsDefault()`: Returns the default JSON settings.
- `settingsFromFile(path)`: Loads settings from JSON/TOML/YAML.
- `settingsFromEnv(prefix)`: Loads settings from environment variables.
- `settingsLoad()`: Auto-detect settings from well-known files and env.

## Testing

`./gradlew test` automatically builds and stages the host platform native library for tests.

Then run:

```bash
./gradlew test
```

## Troubleshooting

Common issues:
- `UnsatisfiedLinkError`: The JAR does not contain a native binary for your platform. Build with `-Pslatedb.native.targets=all` (or include your platform ID in a custom list).
- Native access warnings: Use `--enable-native-access=ALL-UNNAMED` when running or testing.

## License

SlateDB is licensed under the Apache License, Version 2.0.
