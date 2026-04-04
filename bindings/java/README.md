# SlateDB Java Binding

`bindings/java` contains the Gradle project for the UniFFI-based Java binding for SlateDB.

The published artifact is:

```text
io.slatedb:slatedb-uniffi
```

Generated Java sources live in:

```text
io.slatedb.uniffi
```

## Status

This module is generated bindings plus a small amount of packaging glue:

- UniFFI-generated Java sources are written under `bindings/java/slatedb-uniffi/build/generated/sources/uniffi/java`
- `bindings/java/slatedb-uniffi/build.gradle` and this `README.md` are maintained by hand
- the published jar bundles the `slatedb_uniffi` native library as a JNA resource

## Add Dependency

Gradle:

```gradle
dependencies {
    implementation("io.slatedb:slatedb-uniffi:<version>")
}
```

Maven:

```xml
<dependency>
  <groupId>io.slatedb</groupId>
  <artifactId>slatedb-uniffi</artifactId>
  <version>&lt;version&gt;</version>
</dependency>
```

## API Model

- `ObjectStore.resolve(...)` resolves an object store from a URL such as `memory:///` or `file:///...`
- `DbBuilder` opens a writable database and `DbReaderBuilder` opens a read-only reader
- `DbBuilder.withMetricsRecorder(...)` and `DbReaderBuilder.withMetricsRecorder(...)` install a custom metrics sink
- `DefaultMetricsRecorder` provides an in-process recorder with snapshot and lookup helpers
- most database operations return `CompletableFuture`
- native-backed handles implement `AutoCloseable` and should be closed

## Metrics

Use `DefaultMetricsRecorder` when you want to observe SlateDB metrics from Java without wiring a
custom callback implementation:

```java
import io.slatedb.uniffi.Db;
import io.slatedb.uniffi.DbBuilder;
import io.slatedb.uniffi.DefaultMetricsRecorder;
import io.slatedb.uniffi.Metric;
import io.slatedb.uniffi.ObjectStore;

try (ObjectStore store = ObjectStore.resolve("memory:///");
        DefaultMetricsRecorder recorder = new DefaultMetricsRecorder();
        DbBuilder builder = new DbBuilder("metrics-demo", store)) {
    builder.withMetricsRecorder(recorder);

    Db db = await(builder.build());
    try (db) {
        await(db.put("hello".getBytes(), "world".getBytes()));
        await(db.shutdown());
    }

    Metric writes = recorder.metricByNameAndLabels("slatedb.db.write_ops", java.util.List.of());
    System.out.println(writes.value());
}
```

For custom integrations, implement `MetricsRecorder` and return `Counter`, `Gauge`,
`UpDownCounter`, and `Histogram` handles from the registration methods.

## Basic Example

```java
import io.slatedb.uniffi.Db;
import io.slatedb.uniffi.DbBuilder;
import io.slatedb.uniffi.ObjectStore;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class Main {
    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(30, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws Exception {
        byte[] key = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] value = "world".getBytes(StandardCharsets.UTF_8);

        try (ObjectStore store = ObjectStore.resolve("memory:///");
                DbBuilder builder = new DbBuilder("demo-db", store)) {
            Db db = await(builder.build());
            try (db) {
                await(db.put(key, value));

                byte[] read = await(db.get(key));
                if (read == null || !Arrays.equals(read, value)) {
                    throw new IllegalStateException("unexpected value");
                }

                System.out.println(new String(read, StandardCharsets.UTF_8));
                await(db.shutdown());
            }
        }
    }
}
```

Replace `memory:///` with [any object store URL](https://docs.rs/object_store/latest/object_store/fn.parse_url_opts.html) supported by Rust's `object_store` crate.

## Build Prerequisites

You only need these if you are regenerating bindings or building from source in this repository:

- Java 22 or newer
- Rust toolchain for this repository
- `uniffi-bindgen-java`

Install the generator with:

```bash
cargo install uniffi-bindgen-java --git https://github.com/criccomini/uniffi-bindgen-java.git --branch fix-histo-bug
```

## Regenerate

From the repository root:

```bash
./bindings/java/gradlew -p bindings/java :slatedb-uniffi:generateJavaBindings
```

The generated Java sources are written to `bindings/java/slatedb-uniffi/build/generated/sources/uniffi/java` and are not committed to the repository.

The checked-in Gradle default version is `0.11.1-SNAPSHOT`. Release and publish flows can override it with `-Pslatedb.version=<version>`.

## Build And Test

From the repository root:

```bash
./bindings/java/gradlew -p bindings/java :slatedb-uniffi:check
```

`check` generates Java bindings on demand before compiling and testing.

Local builds stage the host `slatedb_uniffi` native library as a classpath resource. Tests also add the host native output directory to `java.library.path`, because the current generated Java loader resolves the library via `System.loadLibrary(...)`.

## Release Packaging

Release builds publish one `slatedb-uniffi` jar that contains native libraries for:

- `linux-x86-64`
- `linux-aarch64`
- `darwin-x86-64`
- `darwin-aarch64`
- `win32-x86-64`
- `win32-aarch64`

When prebuilt natives are supplied, `-Pslatedb.native.prebuiltDir` must point at a directory laid out like:

```text
<dir>/linux-x86-64/libslatedb_uniffi.so
<dir>/linux-aarch64/libslatedb_uniffi.so
<dir>/darwin-x86-64/libslatedb_uniffi.dylib
<dir>/darwin-aarch64/libslatedb_uniffi.dylib
<dir>/win32-x86-64/slatedb_uniffi.dll
<dir>/win32-aarch64/slatedb_uniffi.dll
```
