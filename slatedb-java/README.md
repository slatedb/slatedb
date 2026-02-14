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

## Build the Native Library

From the repository root:

```bash
cargo build -p slatedb-c
```

Native library locations:
- macOS: `target/debug/libslatedb_c.dylib`
- Linux: `target/debug/libslatedb_c.so`
- Windows: `target/debug/slatedb_c.dll`

Optional release build:

```bash
cargo build -p slatedb-c --release
```

## Build the JAR

From `slatedb-java`:

```bash
./gradlew jar
```

The JAR will be written to:

```
slatedb-java/build/libs/slatedb-<version>.jar
```

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
        // Initialize logging (native library resolves via java.library.path)
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

3. Run (set `java.library.path` to the directory containing your `slatedb_c` native library):

```bash
java --enable-native-access=ALL-UNNAMED \
  -Djava.library.path=/absolute/path/to/native/lib/dir \
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

JUnit tests require access to the native library.

Build the native library first:

```bash
cargo build -p slatedb-c
```

The Gradle test task configures `java.library.path` to include `target/release` and `target/debug`.

Then run:

```bash
./gradlew test
```

## Troubleshooting

Common issues:
- `UnsatisfiedLinkError`: The JVM cannot find `slatedb_c`. Verify `java.library.path` includes the native library directory.
- Native access warnings: Use `--enable-native-access=ALL-UNNAMED` when running or testing.

## License

SlateDB is licensed under the Apache License, Version 2.0.
