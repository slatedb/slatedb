package io.slatedb;

import io.slatedb.SlateDbConfig.*;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

/// Java bindings for SlateDB backed by the `slatedb-c` FFI library.
///
/// SlateDB is an embedded LSM-tree database that stores its data in object storage (S3, GCS,
/// Azure Blob, MinIO, and more). These bindings expose the core SlateDB API using the Java
/// Foreign Function and Memory (FFM) API. All operations are synchronous on the Java side
/// and delegate to the native SlateDB runtime.
///
/// ### Lifecycle
///
/// - Load the native library once per JVM with [#loadLibrary()] or
///   [#loadLibrary(String)].
/// - Open a database with [#open(String,String,String)] or configure a builder via
///   [#builder(String,String,String)].
/// - Always close resources ([SlateDb], [SlateDbReader], [SlateDbWriteBatch],
///   [SlateDbScanIterator]) with try-with-resources.
///
///
/// ### Threading
///
/// Instances are safe to share across threads for typical read/write operations, but the
/// underlying native runtime manages its own thread pool. Avoid using closed instances.
///
/// ### Hello World
/// ```java
/// import io.slatedb.SlateDbKeyValue;
/// import io.slatedb.SlateDbScanIterator;
/// import io.slatedb.SlateDb;
/// import io.slatedb.SlateDbWriteBatch;
///
/// import java.nio.charset.StandardCharsets;
/// import java.nio.file.Files;
/// import java.nio.file.Path;
///
/// public final class HelloSlateDb {
///     public static void main(String[] args) throws Exception {
///         if (args.length != 1) {
///             System.err.println("Usage: HelloSlateDb <absolute path to slatedb_c native library>");
///             System.exit(2);
///         }
///
///         // Load native library and init logging
///         SlateDb.loadLibrary(args[0]);
///         SlateDb.initLogging("info");
///
///         // Local database path and local object store
///         Path dbPath = Files.createTempDirectory("slatedb-java-db");
///         Path objectStoreRoot = Files.createTempDirectory("slatedb-java-store");
///         String objectStoreUrl = "file://" + objectStoreRoot.toAbsolutePath();
///
///         byte[] key = "hello-key".getBytes(StandardCharsets.UTF_8);
///         byte[] value = "hello-value".getBytes(StandardCharsets.UTF_8);
///
///         try (SlateDb db = SlateDb.open(dbPath.toString(), objectStoreUrl, null)) {
///             db.put(key, value);
///             byte[] loaded = db.get(key);
///             System.out.println(new String(loaded, StandardCharsets.UTF_8));
///
///             try (SlateDbWriteBatch batch = SlateDb.newWriteBatch()) {
///                 batch.put("hello-a".getBytes(StandardCharsets.UTF_8),
///                     "value-a".getBytes(StandardCharsets.UTF_8));
///                 batch.put("hello-b".getBytes(StandardCharsets.UTF_8),
///                     "value-b".getBytes(StandardCharsets.UTF_8));
///                 db.write(batch);
///             }
///
///             try (SlateDbScanIterator iter = db.scanPrefix("hello-".getBytes(StandardCharsets.UTF_8))) {
///                 SlateDbKeyValue kv;
///                 while ((kv = iter.next()) != null) {
///                     System.out.println(
///                         new String(kv.key(), StandardCharsets.UTF_8) + "=" +
///                         new String(kv.value(), StandardCharsets.UTF_8)
///                     );
///                 }
///             }
///         }
///     }
/// }
/// ```
public final class SlateDb implements AutoCloseable {
    private MemorySegment handle;
    private boolean closed;

    private SlateDb(MemorySegment handle) {
        this.handle = handle;
    }

    /// Loads the SlateDB native library using `java.library.path`.
    ///
    /// This is required before any FFI calls can be made. If you do not call
    /// this explicitly, the first SlateDB call will attempt to load the library
    /// automatically using `java.library.path`.
    ///
    /// @throws UnsatisfiedLinkError if the library cannot be found.
    public static void loadLibrary() {
        Native.loadLibrary();
    }

    /// Loads the SlateDB native library from an absolute path.
    ///
    /// @param absolutePath full path to the native library (for example, `/path/to/libslatedb_c.dylib`).
    /// @throws UnsatisfiedLinkError if the library cannot be loaded.
    public static void loadLibrary(String absolutePath) {
        Native.loadLibrary(absolutePath);
    }

    /// Initializes SlateDB logging using a log level (for example, `"info"` or `"debug"`).
    ///
    /// @param level the log level string understood by SlateDB.
    public static void initLogging(String level) {
        Native.initLogging(level);
    }

    /// Returns the default SlateDB settings as a JSON string.
    ///
    /// @return JSON string containing the default settings.
    public static String settingsDefault() {
        return Native.settingsDefault();
    }

    /// Loads settings from a configuration file and returns them as JSON.
    ///
    /// Supported formats are determined by file extension: `.json`, `.toml`,
    /// `.yaml`, or `.yml`.
    ///
    /// @param path path to the settings file.
    /// @return JSON string containing the loaded settings.
    /// @throws IllegalStateException if the file cannot be read or parsed.
    public static String settingsFromFile(String path) {
        return Native.settingsFromFile(path);
    }

    /// Loads settings from environment variables using the provided prefix and returns them as JSON.
    ///
    /// @param prefix environment variable prefix to search for.
    /// @return JSON string containing the loaded settings.
    public static String settingsFromEnv(String prefix) {
        return Native.settingsFromEnv(prefix);
    }

    /// Loads settings using auto-detection (well-known files and environment variables) and returns them as JSON.
    ///
    /// @return JSON string containing the loaded settings.
    public static String settingsLoad() {
        return Native.settingsLoad();
    }

    /// Opens a SlateDB handle with default settings.
    ///
    /// @param path filesystem path for the database.
    /// @param url object store URL (for local dev, `file:///...`). If `null`, the object store is resolved from environment variables.
    /// @param envFile optional env file for object store configuration. May be `null`.
    /// @return An open [SlateDb] instance. Always close it.
    /// @throws SlateDbException if the native open fails.
    /// ### Example
    /// ```java
    /// try (SlateDb db = SlateDb.open("/tmp/slatedb", "file:///tmp/slatedb-store", null)) {
    ///     db.put("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
    /// }
    /// ```
    public static SlateDb open(String path, String url, String envFile) {
        return new SlateDb(Native.open(path, url, envFile));
    }

    /// Opens a read-only SlateDB reader.
    ///
    /// @param path filesystem path for the database.
    /// @param url object store URL (for local dev, `file:///...`). If `null`, the object store is resolved from environment variables.
    /// @param envFile optional env file for object store configuration. May be `null`.
    /// @param checkpointId optional checkpoint identifier to read from. May be `null`.
    /// @param options reader options or `null` for defaults.
    /// @return An open [SlateDbReader]. Always close it.
    /// @throws SlateDbException if the native open fails.
    /// ### Example
    /// ```java
    /// SlateDbConfig.ReaderOptions options = SlateDbConfig.ReaderOptions.builder()
    ///     .manifestPollInterval(Duration.ofSeconds(1))
    ///     .checkpointLifetime(Duration.ofMinutes(5))
    ///     .build();
    ///
    /// try (SlateDbReader reader = SlateDb.openReader(
    ///     "/tmp/slatedb",
    ///     "file:///tmp/slatedb-store",
    ///     null,
    ///     null,
    ///     options
    /// )) {
    ///     byte[] value = reader.get("key".getBytes(StandardCharsets.UTF_8));
    /// }
    /// ```
    public static SlateDbReader openReader(
        String path,
        String url,
        String envFile,
        String checkpointId,
        ReaderOptions options
    ) {
        return new SlateDbReader(Native.readerOpen(path, url, envFile, checkpointId, options));
    }

    /// Creates a new [Builder] for configuring and opening a SlateDB instance.
    ///
    /// @param path filesystem path for the database.
    /// @param url object store URL (for local dev, `file:///...`). If `null`, the object store is resolved from environment variables.
    /// @param envFile optional env file for object store configuration. May be `null`.
    /// @return A builder that must be closed if not used.
    public static Builder builder(String path, String url, String envFile) {
        return new Builder(Native.newBuilder(path, url, envFile));
    }

    /// Creates a new write batch for atomic operations.
    ///
    /// A batch is consumed after a write attempt (success or failure) and cannot be reused.
    ///
    /// @return A new [SlateDbWriteBatch] instance. Always close it.
    public static SlateDbWriteBatch newWriteBatch() {
        return new SlateDbWriteBatch(Native.newWriteBatch());
    }

    /// Writes a value into the database with default options.
    ///
    /// @param key key to write (non-empty).
    /// @param value value to write.
    /// @throws SlateDbException if the write fails.
    /// ### Example
    /// ```java
    /// db.put("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
    /// ```
    public void put(byte[] key, byte[] value) {
        Native.put(handle, key, value);
    }

    /// Writes a value into the database with custom put and write options.
    ///
    /// @param key key to write (non-empty).
    /// @param value value to write.
    /// @param putOptions put options or `null` for defaults.
    /// @param writeOptions write options or `null` for defaults.
    /// @throws SlateDbException if the write fails.
    public void put(byte[] key, byte[] value, PutOptions putOptions, WriteOptions writeOptions) {
        Native.put(handle, key, value, putOptions, writeOptions);
    }

    /// Reads a value from the database using default read options.
    ///
    /// @param key key to read.
    /// @return The value for the key, or `null` if the key does not exist.
    /// @throws SlateDbException if the read fails.
    /// ### Example
    /// ```java
    /// byte[] value = db.get("key".getBytes(StandardCharsets.UTF_8));
    /// ```
    public byte[] get(byte[] key) {
        return Native.get(handle, key);
    }

    /// Reads a value from the database with custom read options.
    ///
    /// @param key key to read.
    /// @param options read options or `null` for defaults.
    /// @return The value for the key, or `null` if the key does not exist.
    /// @throws SlateDbException if the read fails.
    public byte[] get(byte[] key, ReadOptions options) {
        return Native.get(handle, key, options);
    }

    /// Deletes a key using default write options.
    ///
    /// @param key key to delete.
    /// @throws SlateDbException if the delete fails.
    public void delete(byte[] key) {
        delete(key, WriteOptions.DEFAULT);
    }

    /// Deletes a key using custom write options.
    ///
    /// @param key key to delete.
    /// @param options write options or `null` for defaults.
    /// @throws SlateDbException if the delete fails.
    public void delete(byte[] key, WriteOptions options) {
        Native.delete(handle, key, options);
    }

    /// Writes a batch atomically using default write options.
    ///
    /// The batch is consumed after the write attempt and cannot be reused.
    ///
    /// @param batch batch to write (must be open and unconsumed).
    /// @throws SlateDbException if the write fails.
    public void write(SlateDbWriteBatch batch) {
        write(batch, WriteOptions.DEFAULT);
    }

    /// Writes a batch atomically using custom write options.
    ///
    /// The batch is consumed after the write attempt (success or failure) and cannot be reused.
    ///
    /// @param batch batch to write (must be open and unconsumed).
    /// @param options write options or `null` for defaults.
    /// @throws SlateDbException if the write fails.
    /// ### Example
    /// ```java
    /// try (SlateDbWriteBatch batch = SlateDb.newWriteBatch()) {
    ///     batch.put("a".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8));
    ///     batch.put("b".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8));
    ///     db.write(batch);
    /// }
    /// ```
    public void write(SlateDbWriteBatch batch, WriteOptions options) {
        Objects.requireNonNull(batch, "batch");
        try {
            Native.writeBatchWrite(handle, batch.handle(), options == null ? WriteOptions.DEFAULT : options);
        } finally {
            batch.markConsumed();
        }
    }

    /// Flushes in-memory data to object storage.
    ///
    /// This call blocks until in-memory writes are durably persisted.
    ///
    /// @throws SlateDbException if the flush fails.
    public void flush() {
        Native.flush(handle);
    }

    /// Creates a scan iterator over the range `[startKey, endKey)` using default scan options.
    /// Use `null` for either bound to scan from the beginning or to the end of the database.
    ///
    /// @param startKey inclusive lower bound, or `null`.
    /// @param endKey exclusive upper bound, or `null`.
    /// @return A [SlateDbScanIterator] over the range. Always close it.
    /// @throws SlateDbException if the scan fails.
    /// ### Example
    /// ```java
    /// try (SlateDbScanIterator iter = db.scan(
    ///     "a".getBytes(StandardCharsets.UTF_8),
    ///     "z".getBytes(StandardCharsets.UTF_8)
    /// )) {
    ///     SlateDbKeyValue kv;
    ///     while ((kv = iter.next()) != null) {
    ///         // consume kv
    ///     }
    /// }
    /// ```
    public SlateDbScanIterator scan(byte[] startKey, byte[] endKey) {
        return scan(startKey, endKey, null);
    }

    /// Creates a scan iterator over the range `[startKey, endKey)` using custom scan options.
    /// Use `null` for either bound to scan from the beginning or to the end of the database.
    ///
    /// @param startKey inclusive lower bound, or `null`.
    /// @param endKey exclusive upper bound, or `null`.
    /// @param options scan options or `null` for defaults.
    /// @return A [SlateDbScanIterator] over the range. Always close it.
    /// @throws SlateDbException if the scan fails.
    public SlateDbScanIterator scan(byte[] startKey, byte[] endKey, ScanOptions options) {
        return new SlateDbScanIterator(Native.scan(handle, startKey, endKey, options));
    }

    /// Creates a scan iterator for the provided key prefix using default scan options.
    ///
    /// @param prefix key prefix to scan.
    /// @return A [SlateDbScanIterator] over the prefix. Always close it.
    /// @throws SlateDbException if the scan fails.
    public SlateDbScanIterator scanPrefix(byte[] prefix) {
        return scanPrefix(prefix, null);
    }

    /// Creates a scan iterator for the provided key prefix using custom scan options.
    ///
    /// @param prefix key prefix to scan.
    /// @param options scan options or `null` for defaults.
    /// @return A [SlateDbScanIterator] over the prefix. Always close it.
    /// @throws SlateDbException if the scan fails.
    public SlateDbScanIterator scanPrefix(byte[] prefix, ScanOptions options) {
        return new SlateDbScanIterator(Native.scanPrefix(handle, prefix, options));
    }

    /// Returns a JSON string containing SlateDB metrics.
    ///
    /// @return JSON string with runtime and storage metrics.
    public String metrics() {
        return Native.metrics(handle);
    }

    /// Closes the database handle.
    ///
    /// This method is idempotent.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        Native.close(handle);
        handle = MemorySegment.NULL;
        closed = true;
    }

    /// Runtime exception thrown when SlateDB returns an error code.
    public static final class SlateDbException extends RuntimeException {
        private final int errorCode;

        /// Creates a new exception with the SlateDB error code and message.
        public SlateDbException(int errorCode, String message) {
            super(message == null ? ("SlateDB error " + errorCode) : message);
            this.errorCode = errorCode;
        }

        /// Returns the native error code.
        public int getErrorCode() {
            return errorCode;
        }
    }

    /// Builder for creating a SlateDB instance with custom settings.
    ///
    /// The builder is consumed on [#build()] and must be closed if not used.
    public static final class Builder implements AutoCloseable {
        private MemorySegment builderPtr;
        private boolean closed;

        private Builder(MemorySegment builderPtr) {
            this.builderPtr = builderPtr;
        }

        /// Applies settings JSON to this builder.
        ///
        /// @param settingsJson JSON string describing SlateDB settings.
        /// @throws IllegalArgumentException if the JSON is invalid.
        public Builder withSettingsJson(String settingsJson) {
            Native.builderWithSettings(builderPtr, settingsJson);
            return this;
        }

        /// Sets the SST block size used by the database.
        ///
        /// @param blockSize block size enum value.
        /// @throws IllegalArgumentException if the block size is invalid.
        public Builder withSstBlockSize(SstBlockSize blockSize) {
            Native.builderWithSstBlockSize(builderPtr, blockSize);
            return this;
        }

        /// Builds and opens the database. The builder is consumed after this call.
        ///
        /// @return An open [SlateDb] instance. Always close it.
        /// @throws SlateDbException if the native build fails.
        /// ### Example
        /// ```java
        /// String settings = SlateDb.settingsDefault();
        /// try (SlateDb.Builder builder = SlateDb.builder("/tmp/db", "file:///tmp/store", null)) {
        ///     builder.withSettingsJson(settings)
        ///            .withSstBlockSize(SlateDbConfig.SstBlockSize.KIB_4);
        ///     try (SlateDb db = builder.build()) {
        ///         // use db
        ///     }
        /// }
        /// ```
        public SlateDb build() {
            try {
                MemorySegment handlePtr = Native.builderBuild(builderPtr);
                return new SlateDb(handlePtr);
            } finally {
                builderPtr = MemorySegment.NULL;
                closed = true;
            }
        }

        /// Releases the native builder resources.
        ///
        /// This method is idempotent.
        @Override
        public void close() {
            if (closed) {
                return;
            }
            Native.builderFree(builderPtr);
            builderPtr = MemorySegment.NULL;
            closed = true;
        }

    }


}
