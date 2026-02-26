package io.slatedb;

import io.slatedb.SlateDbConfig.*;
import org.jspecify.annotations.Nullable;

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
/// - Native libraries are loaded automatically from bundled classpath resources.
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
///
/// ```java
/// import io.slatedb.SlateDbKeyValue;
/// import io.slatedb.SlateDbScanIterator;
/// import io.slatedb.SlateDb;
/// import io.slatedb.SlateDbWriteBatch;
/// import io.slatedb.SlateDbConfig;
///
/// import java.nio.charset.StandardCharsets;
/// import java.nio.file.Files;
/// import java.nio.file.Path;
///
/// public final class HelloSlateDb {
///     public static void main(String[] args) throws Exception {
///         // Initialize logging
///         SlateDb.initLogging(SlateDbConfig.LogLevel.INFO);
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
///
/// ### URL Formats
///
/// SlateDB uses Rust's `object_store` URL format (`memory://`, `file:///...`). See
/// [ObjectStore::parse_url_opts](https://docs.rs/object_store/latest/object_store/fn.parse_url_opts.html)
/// for details.
/// ```
public final class SlateDb implements SlateDbReadable {
    private NativeInterop.DbHandle handle;
    private NativeInterop.MergeOperatorBinding mergeOperatorBinding;
    private boolean closed;

    private SlateDb(
        NativeInterop.DbHandle handle,
        NativeInterop.MergeOperatorBinding mergeOperatorBinding
    ) {
        this.handle = handle;
        this.mergeOperatorBinding = mergeOperatorBinding;
    }

    /// Initializes SlateDB logging using a log level (for example, `"info"` or `"debug"`).
    ///
    /// @param level the log level enum value understood by SlateDB.
    public static void initLogging(LogLevel level) {
        Objects.requireNonNull(level, "level");
        NativeInterop.slatedb_logging_init(level);
    }

    /// Returns the default SlateDB settings as a JSON string.
    ///
    /// @return JSON string containing the default settings.
    public static String settingsDefault() {
        try (NativeInterop.SettingsHandle settings = NativeInterop.slatedb_settings_default()) {
            return NativeInterop.slatedb_settings_to_json_string(settings);
        }
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
        try (NativeInterop.SettingsHandle settings = NativeInterop.slatedb_settings_from_file(path)) {
            return NativeInterop.slatedb_settings_to_json_string(settings);
        }
    }

    /// Loads settings from environment variables using the provided prefix and returns them as JSON.
    ///
    /// @param prefix environment variable prefix to search for.
    /// @return JSON string containing the loaded settings.
    public static String settingsFromEnv(String prefix) {
        try (NativeInterop.SettingsHandle settings = NativeInterop.slatedb_settings_from_env(prefix)) {
            return NativeInterop.slatedb_settings_to_json_string(settings);
        }
    }

    /// Loads settings using auto-detection (well-known files and environment variables) and returns them as JSON.
    ///
    /// @return JSON string containing the loaded settings.
    public static String settingsLoad() {
        try (NativeInterop.SettingsHandle settings = NativeInterop.slatedb_settings_load()) {
            return NativeInterop.slatedb_settings_to_json_string(settings);
        }
    }

    /// Opens a SlateDB handle with default settings.
    ///
    /// @param path filesystem path for the database.
    /// @param url object store URL in `object_store` URL format (`memory://`, `file:///...`). If `null`, the object store is resolved from environment variables. See [ObjectStore::parse_url_opts](https://docs.rs/object_store/latest/object_store/fn.parse_url_opts.html) for details.
    /// @param envFile optional env file for object store configuration. May be `null`.
    /// @return An open [SlateDb] instance. Always close it.
    /// @throws SlateDbException if the native open fails.
    /// ### Example
    /// ```java
    /// try (SlateDb db = SlateDb.open("/tmp/slatedb", "file:///tmp/slatedb-store", null)) {
    ///     db.put("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
    /// }
    /// ```
    public static SlateDb open(String path, @Nullable String url, @Nullable String envFile) {
        try (NativeInterop.ObjectStoreHandle objectStore = NativeInterop.resolveObjectStore(url, envFile)) {
            return new SlateDb(NativeInterop.slatedb_db_open(path, objectStore), null);
        }
    }

    /// Opens a read-only SlateDB reader.
    ///
    /// @param path filesystem path for the database.
    /// @param url object store URL in `object_store` URL format (`memory://`, `file:///...`). If `null`, the object store is resolved from environment variables. See [ObjectStore::parse_url_opts](https://docs.rs/object_store/latest/object_store/fn.parse_url_opts.html) for details.
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
        @Nullable String url,
        @Nullable String envFile,
        @Nullable String checkpointId,
        @Nullable ReaderOptions options
    ) {
        try (NativeInterop.ObjectStoreHandle objectStore = NativeInterop.resolveObjectStore(url, envFile)) {
            return new SlateDbReader(NativeInterop.slatedb_db_reader_open(path, objectStore, checkpointId, options));
        }
    }

    /// Creates a new [Builder] for configuring and opening a SlateDB instance.
    ///
    /// @param path filesystem path for the database.
    /// @param url object store URL in `object_store` URL format (`memory://`, `file:///...`). If `null`, the object store is resolved from environment variables. See [ObjectStore::parse_url_opts](https://docs.rs/object_store/latest/object_store/fn.parse_url_opts.html) for details.
    /// @param envFile optional env file for object store configuration. May be `null`.
    /// @return A builder that must be closed if not used.
    public static Builder builder(String path, @Nullable String url, @Nullable String envFile) {
        try (NativeInterop.ObjectStoreHandle objectStore = NativeInterop.resolveObjectStore(url, envFile)) {
            return new Builder(NativeInterop.slatedb_db_builder_new(path, objectStore));
        }
    }

    /// Creates a new write batch for atomic operations.
    ///
    /// A batch is consumed after a write attempt (success or failure) and cannot be reused.
    ///
    /// @return A new [SlateDbWriteBatch] instance. Always close it.
    public static SlateDbWriteBatch newWriteBatch() {
        return new SlateDbWriteBatch(NativeInterop.slatedb_write_batch_new());
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
    public SlateDbWriteHandle put(byte[] key, byte[] value) {
        return NativeInterop.slatedb_db_put(handle, key, value);
    }

    /// Writes a value into the database with custom put and write options.
    ///
    /// @param key key to write (non-empty).
    /// @param value value to write.
    /// @param putOptions put options or `null` for defaults.
    /// @param writeOptions write options or `null` for defaults.
    /// @throws SlateDbException if the write fails.
    public SlateDbWriteHandle put(byte[] key, byte[] value, @Nullable PutOptions putOptions, @Nullable WriteOptions writeOptions) {
        return NativeInterop.slatedb_db_put_with_options(handle, key, value, putOptions, writeOptions);
    }

    /// Merges a value into a key using default merge and write options.
    ///
    /// @param key key to merge into (non-empty).
    /// @param value merge operand value.
    /// @throws SlateDbException if the merge fails.
    public SlateDbWriteHandle merge(byte[] key, byte[] value) {
        return NativeInterop.slatedb_db_merge(handle, key, value);
    }

    /// Merges a value into a key with custom merge and write options.
    ///
    /// @param key key to merge into (non-empty).
    /// @param value merge operand value.
    /// @param mergeOptions merge options or `null` for defaults.
    /// @param writeOptions write options or `null` for defaults.
    /// @throws SlateDbException if the merge fails.
    public SlateDbWriteHandle merge(byte[] key, byte[] value, @Nullable MergeOptions mergeOptions, @Nullable WriteOptions writeOptions) {
        return NativeInterop.slatedb_db_merge_with_options(handle, key, value, mergeOptions, writeOptions);
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
        return NativeInterop.slatedb_db_get(handle, key);
    }

    /// Reads a value from the database with custom read options.
    ///
    /// @param key key to read.
    /// @param options read options or `null` for defaults.
    /// @return The value for the key, or `null` if the key does not exist.
    /// @throws SlateDbException if the read fails.
    public byte[] get(byte[] key, @Nullable ReadOptions options) {
        return NativeInterop.slatedb_db_get_with_options(handle, key, options);
    }

    /// Deletes a key using default write options.
    ///
    /// @param key key to delete.
    /// @throws SlateDbException if the delete fails.
    public SlateDbWriteHandle delete(byte[] key) {
        return delete(key, WriteOptions.DEFAULT);
    }

    /// Deletes a key using custom write options.
    ///
    /// @param key key to delete.
    /// @param options write options or `null` for defaults.
    /// @throws SlateDbException if the delete fails.
    public SlateDbWriteHandle delete(byte[] key, @Nullable WriteOptions options) {
        return NativeInterop.slatedb_db_delete_with_options(handle, key, options);
    }

    /// Writes a batch atomically using default write options.
    ///
    /// The batch is consumed after the write attempt and cannot be reused.
    ///
    /// @param batch batch to write (must be open and unconsumed).
    /// @throws SlateDbException if the write fails.
    public SlateDbWriteHandle write(SlateDbWriteBatch batch) {
        return write(batch, WriteOptions.DEFAULT);
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
    public SlateDbWriteHandle write(SlateDbWriteBatch batch, @Nullable WriteOptions options) {
        Objects.requireNonNull(batch, "batch");
        try {
            return NativeInterop.slatedb_db_write_with_options(handle, batch.handle(), options == null ? WriteOptions.DEFAULT : options);
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
        NativeInterop.slatedb_db_flush(handle);
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
        return scan(startKey, endKey, ScanOptions.DEFAULT);
    }

    /// Creates a scan iterator over the range `[startKey, endKey)` using custom scan options.
    /// Use `null` for either bound to scan from the beginning or to the end of the database.
    ///
    /// @param startKey inclusive lower bound, or `null`.
    /// @param endKey exclusive upper bound, or `null`.
    /// @param options scan options or `null` for defaults.
    /// @return A [SlateDbScanIterator] over the range. Always close it.
    /// @throws SlateDbException if the scan fails.
    public SlateDbScanIterator scan(byte[] startKey, byte[] endKey, @Nullable ScanOptions options) {
        return new SlateDbScanIterator(NativeInterop.slatedb_db_scan_with_options(handle, startKey, endKey, options));
    }

    /// Creates a scan iterator for the provided key prefix using default scan options.
    ///
    /// @param prefix key prefix to scan.
    /// @return A [SlateDbScanIterator] over the prefix. Always close it.
    /// @throws SlateDbException if the scan fails.
    public SlateDbScanIterator scanPrefix(byte[] prefix) {
        return scanPrefix(prefix, ScanOptions.DEFAULT);
    }

    /// Creates a scan iterator for the provided key prefix using custom scan options.
    ///
    /// @param prefix key prefix to scan.
    /// @param options scan options or `null` for defaults.
    /// @return A [SlateDbScanIterator] over the prefix. Always close it.
    /// @throws SlateDbException if the scan fails.
    public SlateDbScanIterator scanPrefix(byte[] prefix, @Nullable ScanOptions options) {
        return new SlateDbScanIterator(NativeInterop.slatedb_db_scan_prefix_with_options(handle, prefix, options));
    }

    /// Returns a JSON string containing SlateDB metrics.
    ///
    /// @return JSON string with runtime and storage metrics.
    public String metrics() {
        return NativeInterop.slatedb_db_metrics_string(handle);
    }

    /// Closes the database handle.
    ///
    /// This method is idempotent.
    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            NativeInterop.slatedb_db_close(handle);
        } finally {
            handle = null;
            if (mergeOperatorBinding != null) {
                mergeOperatorBinding.close();
                mergeOperatorBinding = null;
            }
            closed = true;
        }
    }

    /// Builder for creating a SlateDB instance with custom settings.
    ///
    /// The builder is consumed on [#build()] and must be closed if not used.
    public static final class Builder implements AutoCloseable {
        private NativeInterop.DbBuilderHandle builderPtr;
        private NativeInterop.MergeOperatorBinding mergeOperatorBinding;
        private boolean closed;

        private Builder(NativeInterop.DbBuilderHandle builderPtr) {
            this.builderPtr = builderPtr;
        }

        /// Applies settings JSON to this builder.
        ///
        /// @param settingsJson JSON string describing SlateDB settings.
        /// @throws IllegalArgumentException if the JSON is invalid.
        public Builder withSettingsJson(String settingsJson) {
            try {
                try (NativeInterop.SettingsHandle settings = NativeInterop.slatedb_settings_from_json(settingsJson)) {
                    NativeInterop.slatedb_db_builder_with_settings(builderPtr, settings);
                }
            } catch (SlateDbException error) {
                throw new IllegalArgumentException(error.getMessage(), error);
            }
            return this;
        }

        /// Sets the SST block size used by the database.
        ///
        /// @param blockSize block size enum value.
        /// @throws IllegalArgumentException if the block size is invalid.
        public Builder withSstBlockSize(SstBlockSize blockSize) {
            NativeInterop.slatedb_db_builder_with_sst_block_size(builderPtr, blockSize);
            return this;
        }

        /// Configures a merge operator callback for this builder.
        ///
        /// @param mergeOperator callback used to merge operands.
        /// @throws SlateDbException if native registration fails.
        public Builder withMergeOperator(SlateDbMergeOperator mergeOperator) {
            Objects.requireNonNull(mergeOperator, "mergeOperator");

            NativeInterop.MergeOperatorBinding newBinding = NativeInterop.createMergeOperatorBinding(mergeOperator);
            try {
                NativeInterop.slatedb_db_builder_with_merge_operator(
                    builderPtr,
                    newBinding.mergeOperatorPtr(),
                    newBinding.freeMergeResultPtr()
                );
            } catch (RuntimeException error) {
                newBinding.close();
                throw error;
            }

            NativeInterop.MergeOperatorBinding oldBinding = mergeOperatorBinding;
            mergeOperatorBinding = newBinding;
            if (oldBinding != null) {
                oldBinding.close();
            }
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
            NativeInterop.MergeOperatorBinding bindingToTransfer = mergeOperatorBinding;
            mergeOperatorBinding = null;
            try {
                return new SlateDb(NativeInterop.slatedb_db_builder_build(builderPtr), bindingToTransfer);
            } catch (RuntimeException error) {
                if (bindingToTransfer != null) {
                    bindingToTransfer.close();
                }
                throw error;
            } finally {
                builderPtr = null;
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
            try {
                NativeInterop.slatedb_db_builder_close(builderPtr);
            } finally {
                builderPtr = null;
                if (mergeOperatorBinding != null) {
                    mergeOperatorBinding.close();
                    mergeOperatorBinding = null;
                }
                closed = true;
            }
        }

    }
}
