package com.slatedb;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.GroupLayout;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

/**
 * Minimal Java bindings for SlateDB backed by the slatedb-c FFI library.
 */
public final class SlateDb implements AutoCloseable {
    private final MemorySegment handle;

    private SlateDb(MemorySegment handle) {
        this.handle = handle;
    }

    /**
     * Loads the SlateDB native library using {@code java.library.path}.
     */
    public static void loadLibrary() {
        Native.loadLibrary();
    }

    /**
     * Loads the SlateDB native library from an absolute path.
     */
    public static void loadLibrary(String absolutePath) {
        Native.loadLibrary(absolutePath);
    }

    /**
     * Initializes SlateDB logging using a log level (e.g. "info", "debug").
     */
    public static void initLogging(String level) {
        Native.initLogging(level);
    }

    /**
     * Returns the default SlateDB settings as a JSON string.
     */
    public static String settingsDefault() {
        return Native.settingsDefault();
    }

    /**
     * Loads settings from a configuration file and returns them as JSON.
     * <p>
     * Supported formats are determined by file extension: {@code .json}, {@code .toml},
     * {@code .yaml}, or {@code .yml}.
     */
    public static String settingsFromFile(String path) {
        return Native.settingsFromFile(path);
    }

    /**
     * Loads settings from environment variables using the provided prefix and returns them as JSON.
     */
    public static String settingsFromEnv(String prefix) {
        return Native.settingsFromEnv(prefix);
    }

    /**
     * Loads settings using auto-detection (well-known files and environment variables) and returns them as JSON.
     */
    public static String settingsLoad() {
        return Native.settingsLoad();
    }

    /**
     * Opens a SlateDB handle.
     */
    public static SlateDb open(String path, String url, String envFile) {
        return new SlateDb(Native.open(path, url, envFile));
    }

    /**
     * Opens a read-only SlateDB reader.
     */
    public static SlateDbReader openReader(
        String path,
        String url,
        String envFile,
        String checkpointId,
        ReaderOptions options
    ) {
        return new SlateDbReader(Native.readerOpen(path, url, envFile, checkpointId, options));
    }

    /**
     * Creates a new {@link Builder} for configuring and opening a SlateDB instance.
     */
    public static Builder builder(String path, String url, String envFile) {
        return Native.newBuilder(path, url, envFile);
    }

    /**
     * Creates a new write batch for atomic operations.
     */
    public static WriteBatch newWriteBatch() {
        return Native.newWriteBatch();
    }

    /**
     * Stores a value for the given key.
     */
    public void put(byte[] key, byte[] value) {
        Native.put(handle, key, value);
    }

    /**
     * Stores a value for the given key using custom put and write options.
     */
    public void put(byte[] key, byte[] value, PutOptions putOptions, WriteOptions writeOptions) {
        Native.put(handle, key, value, putOptions, writeOptions);
    }

    /**
     * Returns the value for the given key, or {@code null} if not found.
     */
    public byte[] get(byte[] key) {
        return Native.get(handle, key);
    }

    /**
     * Returns the value for the given key using custom read options, or {@code null} if not found.
     */
    public byte[] get(byte[] key, ReadOptions options) {
        return Native.get(handle, key, options);
    }

    /**
     * Deletes the value for the given key using default write options.
     */
    public void delete(byte[] key) {
        delete(key, WriteOptions.DEFAULT);
    }

    /**
     * Deletes the value for the given key using custom write options.
     */
    public void delete(byte[] key, WriteOptions options) {
        Native.delete(handle, key, options);
    }

    /**
     * Writes a batch atomically using default write options.
     */
    public void write(WriteBatch batch) {
        write(batch, WriteOptions.DEFAULT);
    }

    /**
     * Writes a batch atomically using custom write options.
     */
    public void write(WriteBatch batch, WriteOptions options) {
        Objects.requireNonNull(batch, "batch");
        if (batch.closed) {
            throw new IllegalStateException("WriteBatch is closed");
        }
        if (batch.consumed) {
            throw new IllegalStateException("WriteBatch already consumed");
        }
        Native.writeBatchWrite(handle, batch.batchPtr, options == null ? WriteOptions.DEFAULT : options);
        batch.consumed = true;
    }

    /**
     * Flushes in-memory data to object storage.
     */
    public void flush() {
        Native.flush(handle);
    }

    /**
     * Creates a scan iterator over the range {@code [startKey, endKey)} using default scan options.
     * Use {@code null} for either bound to scan from the beginning or to the end of the database.
     */
    public ScanIterator scan(byte[] startKey, byte[] endKey) {
        return scan(startKey, endKey, null);
    }

    /**
     * Creates a scan iterator over the range {@code [startKey, endKey)} using custom scan options.
     * Use {@code null} for either bound to scan from the beginning or to the end of the database.
     */
    public ScanIterator scan(byte[] startKey, byte[] endKey, ScanOptions options) {
        return new ScanIterator(Native.scan(handle, startKey, endKey, options));
    }

    /**
     * Creates a scan iterator for the provided key prefix using default scan options.
     */
    public ScanIterator scanPrefix(byte[] prefix) {
        return scanPrefix(prefix, null);
    }

    /**
     * Creates a scan iterator for the provided key prefix using custom scan options.
     */
    public ScanIterator scanPrefix(byte[] prefix, ScanOptions options) {
        return new ScanIterator(Native.scanPrefix(handle, prefix, options));
    }

    /**
     * Returns a JSON string containing SlateDB metrics.
     */
    public String metrics() {
        return Native.metrics(handle);
    }

    @Override
    public void close() {
        Native.close(handle);
    }

    private static final class Native {
        private static final Linker LINKER = Linker.nativeLinker();
        private static final Object INIT_LOCK = new Object();

        private static final GroupLayout HANDLE_LAYOUT =
            MemoryLayout.structLayout(ValueLayout.ADDRESS.withName("ptr"));
        private static final GroupLayout RESULT_LAYOUT = createResultLayout();
        private static final GroupLayout HANDLE_RESULT_LAYOUT =
            MemoryLayout.structLayout(HANDLE_LAYOUT.withName("handle"), RESULT_LAYOUT.withName("result"));
        private static final GroupLayout VALUE_LAYOUT =
            MemoryLayout.structLayout(ValueLayout.ADDRESS.withName("data"), ValueLayout.JAVA_LONG.withName("len"));
        private static final GroupLayout PUT_OPTIONS_LAYOUT = createPutOptionsLayout();
        private static final GroupLayout WRITE_OPTIONS_LAYOUT =
            MemoryLayout.structLayout(ValueLayout.JAVA_BOOLEAN.withName("await_durable"));
        private static final GroupLayout READ_OPTIONS_LAYOUT = createReadOptionsLayout();
        private static final GroupLayout SCAN_OPTIONS_LAYOUT = createScanOptionsLayout();
        private static final GroupLayout READER_OPTIONS_LAYOUT = createReaderOptionsLayout();
        private static final GroupLayout READER_HANDLE_LAYOUT =
            MemoryLayout.structLayout(ValueLayout.ADDRESS.withName("ptr"));
        private static final GroupLayout READER_HANDLE_RESULT_LAYOUT =
            MemoryLayout.structLayout(READER_HANDLE_LAYOUT.withName("handle"), RESULT_LAYOUT.withName("result"));
        private static final GroupLayout KEY_VALUE_LAYOUT =
            MemoryLayout.structLayout(VALUE_LAYOUT.withName("key"), VALUE_LAYOUT.withName("value"));
        private static final GroupLayout SCAN_RESULT_LAYOUT = createScanResultLayout();

        private static final VarHandle HANDLE_PTR =
            HANDLE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("ptr"));
        private static final VarHandle RESULT_ERROR =
            RESULT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("error"));
        private static final VarHandle RESULT_MESSAGE =
            RESULT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("message"));
        private static final VarHandle VALUE_DATA =
            VALUE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("data"));
        private static final VarHandle VALUE_LEN =
            VALUE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("len"));
        private static final VarHandle PUT_OPTIONS_TTL_TYPE =
            PUT_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("ttl_type"));
        private static final VarHandle PUT_OPTIONS_TTL_VALUE =
            PUT_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("ttl_value"));
        private static final VarHandle WRITE_OPTIONS_AWAIT_DURABLE =
            WRITE_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("await_durable"));
        private static final VarHandle READ_OPTIONS_DURABILITY =
            READ_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("durability_filter"));
        private static final VarHandle READ_OPTIONS_DIRTY =
            READ_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("dirty"));
        private static final VarHandle READ_OPTIONS_CACHE_BLOCKS =
            READ_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("cache_blocks"));
        private static final VarHandle SCAN_OPTIONS_DURABILITY =
            SCAN_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("durability_filter"));
        private static final VarHandle SCAN_OPTIONS_DIRTY =
            SCAN_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("dirty"));
        private static final VarHandle SCAN_OPTIONS_READ_AHEAD_BYTES =
            SCAN_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("read_ahead_bytes"));
        private static final VarHandle SCAN_OPTIONS_CACHE_BLOCKS =
            SCAN_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("cache_blocks"));
        private static final VarHandle SCAN_OPTIONS_MAX_FETCH_TASKS =
            SCAN_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("max_fetch_tasks"));
        private static final VarHandle READER_OPTIONS_MANIFEST_POLL_INTERVAL =
            READER_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("manifest_poll_interval_ms"));
        private static final VarHandle READER_OPTIONS_CHECKPOINT_LIFETIME =
            READER_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("checkpoint_lifetime_ms"));
        private static final VarHandle READER_OPTIONS_MAX_MEMTABLE_BYTES =
            READER_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("max_memtable_bytes"));
        private static final VarHandle READER_OPTIONS_SKIP_WAL_REPLAY =
            READER_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("skip_wal_replay"));
        private static final VarHandle READER_HANDLE_PTR =
            READER_HANDLE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("ptr"));
        private static final VarHandle KEY_VALUE_KEY_DATA =
            KEY_VALUE_LAYOUT.varHandle(
                MemoryLayout.PathElement.groupElement("key"),
                MemoryLayout.PathElement.groupElement("data"));
        private static final VarHandle KEY_VALUE_KEY_LEN =
            KEY_VALUE_LAYOUT.varHandle(
                MemoryLayout.PathElement.groupElement("key"),
                MemoryLayout.PathElement.groupElement("len"));
        private static final VarHandle KEY_VALUE_VALUE_DATA =
            KEY_VALUE_LAYOUT.varHandle(
                MemoryLayout.PathElement.groupElement("value"),
                MemoryLayout.PathElement.groupElement("data"));
        private static final VarHandle KEY_VALUE_VALUE_LEN =
            KEY_VALUE_LAYOUT.varHandle(
                MemoryLayout.PathElement.groupElement("value"),
                MemoryLayout.PathElement.groupElement("len"));
        private static final VarHandle HANDLE_RESULT_HANDLE_PTR =
            HANDLE_RESULT_LAYOUT.varHandle(
                MemoryLayout.PathElement.groupElement("handle"),
                MemoryLayout.PathElement.groupElement("ptr"));
        private static final VarHandle HANDLE_RESULT_ERROR =
            HANDLE_RESULT_LAYOUT.varHandle(
                MemoryLayout.PathElement.groupElement("result"),
                MemoryLayout.PathElement.groupElement("error"));
        private static final VarHandle HANDLE_RESULT_MESSAGE =
            HANDLE_RESULT_LAYOUT.varHandle(
                MemoryLayout.PathElement.groupElement("result"),
                MemoryLayout.PathElement.groupElement("message"));
        private static final VarHandle READER_HANDLE_RESULT_HANDLE_PTR =
            READER_HANDLE_RESULT_LAYOUT.varHandle(
                MemoryLayout.PathElement.groupElement("handle"),
                MemoryLayout.PathElement.groupElement("ptr"));
        private static final VarHandle READER_HANDLE_RESULT_ERROR =
            READER_HANDLE_RESULT_LAYOUT.varHandle(
                MemoryLayout.PathElement.groupElement("result"),
                MemoryLayout.PathElement.groupElement("error"));
        private static final VarHandle READER_HANDLE_RESULT_MESSAGE =
            READER_HANDLE_RESULT_LAYOUT.varHandle(
                MemoryLayout.PathElement.groupElement("result"),
                MemoryLayout.PathElement.groupElement("message"));

        private static final long HANDLE_RESULT_RESULT_OFFSET =
            HANDLE_RESULT_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("result"));
        private static final long READER_HANDLE_RESULT_OFFSET =
            READER_HANDLE_RESULT_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("result"));
        private static final long KEY_VALUE_KEY_OFFSET =
            KEY_VALUE_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("key"));
        private static final long KEY_VALUE_VALUE_OFFSET =
            KEY_VALUE_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("value"));

        private static volatile boolean initialized;
        private static MethodHandle initLoggingHandle;
        private static MethodHandle settingsDefaultHandle;
        private static MethodHandle settingsFromFileHandle;
        private static MethodHandle settingsFromEnvHandle;
        private static MethodHandle settingsLoadHandle;
        private static MethodHandle openHandle;
        private static MethodHandle deleteHandle;
        private static MethodHandle flushHandle;
        private static MethodHandle scanHandle;
        private static MethodHandle scanPrefixHandle;
        private static MethodHandle metricsHandle;
        private static MethodHandle builderNewHandle;
        private static MethodHandle builderWithSettingsHandle;
        private static MethodHandle builderWithSstBlockSizeHandle;
        private static MethodHandle builderBuildHandle;
        private static MethodHandle builderFreeHandle;
        private static MethodHandle readerOpenHandle;
        private static MethodHandle readerGetHandle;
        private static MethodHandle readerScanHandle;
        private static MethodHandle readerScanPrefixHandle;
        private static MethodHandle readerCloseHandle;
        private static MethodHandle iteratorNextHandle;
        private static MethodHandle iteratorSeekHandle;
        private static MethodHandle iteratorCloseHandle;
        private static MethodHandle putHandle;
        private static MethodHandle getHandle;
        private static MethodHandle closeHandle;
        private static MethodHandle writeBatchNewHandle;
        private static MethodHandle writeBatchPutHandle;
        private static MethodHandle writeBatchPutWithOptionsHandle;
        private static MethodHandle writeBatchDeleteHandle;
        private static MethodHandle writeBatchWriteHandle;
        private static MethodHandle writeBatchCloseHandle;
        private static MethodHandle freeResultHandle;
        private static MethodHandle freeValueHandle;
        private static MethodHandle freeScanResultHandle;
        private static MethodHandle freeCStringHandle;

        private static GroupLayout createResultLayout() {
            long padding = Math.max(0, ValueLayout.ADDRESS.byteAlignment() - ValueLayout.JAVA_INT.byteSize());
            if (padding == 0) {
                return MemoryLayout.structLayout(
                    ValueLayout.JAVA_INT.withName("error"),
                    ValueLayout.ADDRESS.withName("message")
                );
            }
            return MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("error"),
                MemoryLayout.paddingLayout(padding),
                ValueLayout.ADDRESS.withName("message")
            );
        }

        private static GroupLayout createPutOptionsLayout() {
            long offset = ValueLayout.JAVA_INT.byteSize();
            long alignment = ValueLayout.JAVA_LONG.byteAlignment();
            long padding = (alignment - (offset % alignment)) % alignment;
            if (padding == 0) {
                return MemoryLayout.structLayout(
                    ValueLayout.JAVA_INT.withName("ttl_type"),
                    ValueLayout.JAVA_LONG.withName("ttl_value")
                );
            }
            return MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("ttl_type"),
                MemoryLayout.paddingLayout(padding),
                ValueLayout.JAVA_LONG.withName("ttl_value")
            );
        }

        private static GroupLayout createReadOptionsLayout() {
            long size = ValueLayout.JAVA_INT.byteSize() + 2 * ValueLayout.JAVA_BOOLEAN.byteSize();
            long alignment = ValueLayout.JAVA_INT.byteAlignment();
            long padding = (alignment - (size % alignment)) % alignment;
            if (padding == 0) {
                return MemoryLayout.structLayout(
                    ValueLayout.JAVA_INT.withName("durability_filter"),
                    ValueLayout.JAVA_BOOLEAN.withName("dirty"),
                    ValueLayout.JAVA_BOOLEAN.withName("cache_blocks")
                );
            }
            return MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("durability_filter"),
                ValueLayout.JAVA_BOOLEAN.withName("dirty"),
                ValueLayout.JAVA_BOOLEAN.withName("cache_blocks"),
                MemoryLayout.paddingLayout(padding)
            );
        }

        private static GroupLayout createScanOptionsLayout() {
            return MemoryLayout.structLayout(
                ValueLayout.JAVA_INT.withName("durability_filter"),
                ValueLayout.JAVA_BOOLEAN.withName("dirty"),
                MemoryLayout.paddingLayout(3),
                ValueLayout.JAVA_LONG.withName("read_ahead_bytes"),
                ValueLayout.JAVA_BOOLEAN.withName("cache_blocks"),
                MemoryLayout.paddingLayout(7),
                ValueLayout.JAVA_LONG.withName("max_fetch_tasks")
            );
        }

        private static GroupLayout createReaderOptionsLayout() {
            return MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("manifest_poll_interval_ms"),
                ValueLayout.JAVA_LONG.withName("checkpoint_lifetime_ms"),
                ValueLayout.JAVA_LONG.withName("max_memtable_bytes"),
                ValueLayout.JAVA_BOOLEAN.withName("skip_wal_replay"),
                MemoryLayout.paddingLayout(7)
            );
        }

        private static GroupLayout createScanResultLayout() {
            return MemoryLayout.structLayout(
                ValueLayout.ADDRESS.withName("items"),
                ValueLayout.JAVA_LONG.withName("count"),
                ValueLayout.JAVA_BOOLEAN.withName("has_more"),
                MemoryLayout.paddingLayout(7),
                VALUE_LAYOUT.withName("next_key")
            );
        }

        static void loadLibrary() {
            System.loadLibrary("slatedb_c");
            initHandles();
        }

        static void loadLibrary(String absolutePath) {
            Objects.requireNonNull(absolutePath, "absolutePath");
            System.load(absolutePath);
            initHandles();
        }

        static void initLogging(String level) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment levelSegment = toCString(arena, level);
                MemorySegment result = (MemorySegment) initLoggingHandle.invokeExact(
                    (SegmentAllocator) arena, levelSegment);
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static String settingsDefault() {
            ensureInitialized();
            try {
                MemorySegment cstr = (MemorySegment) settingsDefaultHandle.invokeExact();
                return readCStringAndFree(cstr, "Failed to load default settings");
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static String settingsFromFile(String path) {
            Objects.requireNonNull(path, "path");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment pathSegment = toCString(arena, path);
                MemorySegment cstr = (MemorySegment) settingsFromFileHandle.invokeExact(pathSegment);
                return readCStringAndFree(cstr, "Failed to load settings from file: " + path);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static String settingsFromEnv(String prefix) {
            Objects.requireNonNull(prefix, "prefix");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment prefixSegment = toCString(arena, prefix);
                MemorySegment cstr = (MemorySegment) settingsFromEnvHandle.invokeExact(prefixSegment);
                return readCStringAndFree(cstr, "Failed to load settings from env with prefix: " + prefix);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static String settingsLoad() {
            ensureInitialized();
            try {
                MemorySegment cstr = (MemorySegment) settingsLoadHandle.invokeExact();
                return readCStringAndFree(cstr, "Failed to load settings");
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static MemorySegment open(String path, String url, String envFile) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment pathSegment = toCString(arena, path);
                MemorySegment urlSegment = toCString(arena, url);
                MemorySegment envSegment = toCString(arena, envFile);
                MemorySegment handleResult =
                    (MemorySegment) openHandle.invokeExact((SegmentAllocator) arena, pathSegment, urlSegment, envSegment);
                int error = (int) HANDLE_RESULT_ERROR.get(handleResult);
                MemorySegment messageSegment = (MemorySegment) HANDLE_RESULT_MESSAGE.get(handleResult);
                String message = readMessage(messageSegment);
                MemorySegment resultSlice = handleResult.asSlice(HANDLE_RESULT_RESULT_OFFSET, RESULT_LAYOUT.byteSize());
                RuntimeException failure = (error == 0) ? null : new SlateDbException(error, message);
                freeResult(resultSlice, failure);
                if (failure != null) {
                    throw failure;
                }
                MemorySegment handlePtr = (MemorySegment) HANDLE_RESULT_HANDLE_PTR.get(handleResult);
                if (handlePtr == null || handlePtr.equals(MemorySegment.NULL)) {
                    throw new SlateDbException(-1, "SlateDB returned a null handle");
                }
                return handlePtr;
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static Builder newBuilder(String path, String url, String envFile) {
            Objects.requireNonNull(path, "path");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment pathSegment = toCString(arena, path);
                MemorySegment urlSegment = toCString(arena, url);
                MemorySegment envSegment = toCString(arena, envFile);
                MemorySegment builderPtr =
                    (MemorySegment) builderNewHandle.invokeExact(pathSegment, urlSegment, envSegment);
                if (builderPtr == null || builderPtr.equals(MemorySegment.NULL)) {
                    throw new IllegalStateException("Failed to create SlateDB builder");
                }
                return new Builder(builderPtr);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void builderWithSettings(MemorySegment builderPtr, String settingsJson) {
            Objects.requireNonNull(settingsJson, "settingsJson");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment settingsSegment = toCString(arena, settingsJson);
                boolean ok = (boolean) builderWithSettingsHandle.invokeExact(builderPtr, settingsSegment);
                if (!ok) {
                    throw new IllegalArgumentException("Invalid settings JSON");
                }
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void builderWithSstBlockSize(MemorySegment builderPtr, SstBlockSize blockSize) {
            Objects.requireNonNull(blockSize, "blockSize");
            ensureInitialized();
            try {
                boolean ok = (boolean) builderWithSstBlockSizeHandle.invokeExact(builderPtr, blockSize.code);
                if (!ok) {
                    throw new IllegalArgumentException("Invalid SST block size: " + blockSize);
                }
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static MemorySegment builderBuild(MemorySegment builderPtr) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct =
                    (MemorySegment) builderBuildHandle.invokeExact((SegmentAllocator) arena, builderPtr);
                MemorySegment handlePtr = (MemorySegment) HANDLE_PTR.get(handleStruct);
                if (handlePtr == null || handlePtr.equals(MemorySegment.NULL)) {
                    throw new SlateDbException(-1, "SlateDB builder failed to create a database");
                }
                return handlePtr;
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void builderFree(MemorySegment builderPtr) {
            if (builderPtr == null || builderPtr.equals(MemorySegment.NULL)) {
                return;
            }
            ensureInitialized();
            try {
                builderFreeHandle.invokeExact(builderPtr);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static MemorySegment readerOpen(
            String path,
            String url,
            String envFile,
            String checkpointId,
            ReaderOptions options
        ) {
            Objects.requireNonNull(path, "path");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment pathSegment = toCString(arena, path);
                MemorySegment urlSegment = toCString(arena, url);
                MemorySegment envSegment = toCString(arena, envFile);
                MemorySegment checkpointSegment = toCString(arena, checkpointId);
                MemorySegment optionsSegment = toReaderOptions(arena, options);
                MemorySegment handleResult = (MemorySegment) readerOpenHandle.invokeExact(
                    (SegmentAllocator) arena,
                    pathSegment,
                    urlSegment,
                    envSegment,
                    checkpointSegment,
                    optionsSegment
                );
                int error = (int) READER_HANDLE_RESULT_ERROR.get(handleResult);
                MemorySegment messageSegment = (MemorySegment) READER_HANDLE_RESULT_MESSAGE.get(handleResult);
                String message = readMessage(messageSegment);
                MemorySegment resultSlice =
                    handleResult.asSlice(READER_HANDLE_RESULT_OFFSET, RESULT_LAYOUT.byteSize());
                RuntimeException failure = (error == 0) ? null : new SlateDbException(error, message);
                freeResult(resultSlice, failure);
                if (failure != null) {
                    throw failure;
                }
                MemorySegment handlePtr = (MemorySegment) READER_HANDLE_RESULT_HANDLE_PTR.get(handleResult);
                if (handlePtr == null || handlePtr.equals(MemorySegment.NULL)) {
                    throw new SlateDbException(-1, "SlateDB returned a null reader handle");
                }
                return handlePtr;
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static WriteBatch newWriteBatch() {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment batchOut = arena.allocate(ValueLayout.ADDRESS);
                MemorySegment result =
                    (MemorySegment) writeBatchNewHandle.invokeExact((SegmentAllocator) arena, batchOut);
                checkResult(result);
                MemorySegment batchPtr = batchOut.get(ValueLayout.ADDRESS, 0);
                if (batchPtr == null || batchPtr.equals(MemorySegment.NULL)) {
                    throw new SlateDbException(-1, "SlateDB returned a null WriteBatch");
                }
                return new WriteBatch(batchPtr);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void close(MemorySegment handlePtr) {
            if (handlePtr == null || handlePtr.equals(MemorySegment.NULL)) {
                return;
            }
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment result = (MemorySegment) closeHandle.invokeExact((SegmentAllocator) arena, handleStruct);
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void delete(MemorySegment handlePtr, byte[] key, WriteOptions options) {
            Objects.requireNonNull(key, "key");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment keySegment = toByteArray(arena, key);
                MemorySegment optionsSegment = toWriteOptions(arena, options);
                MemorySegment result = (MemorySegment) deleteHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    keySegment,
                    (long) key.length,
                    optionsSegment
                );
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void flush(MemorySegment handlePtr) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment result = (MemorySegment) flushHandle.invokeExact((SegmentAllocator) arena, handleStruct);
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static MemorySegment scan(
            MemorySegment handlePtr,
            byte[] startKey,
            byte[] endKey,
            ScanOptions options
        ) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment startKeySegment = startKey == null ? MemorySegment.NULL : toByteArray(arena, startKey);
                MemorySegment endKeySegment = endKey == null ? MemorySegment.NULL : toByteArray(arena, endKey);
                MemorySegment optionsSegment = toScanOptions(arena, options);
                MemorySegment iteratorOut = arena.allocate(ValueLayout.ADDRESS);
                MemorySegment result = (MemorySegment) scanHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    startKeySegment,
                    startKey == null ? 0L : (long) startKey.length,
                    endKeySegment,
                    endKey == null ? 0L : (long) endKey.length,
                    optionsSegment,
                    iteratorOut
                );
                checkResult(result);
                MemorySegment iterPtr = iteratorOut.get(ValueLayout.ADDRESS, 0);
                if (iterPtr == null || iterPtr.equals(MemorySegment.NULL)) {
                    throw new SlateDbException(-1, "SlateDB returned a null iterator");
                }
                return iterPtr;
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static MemorySegment scanPrefix(
            MemorySegment handlePtr,
            byte[] prefix,
            ScanOptions options
        ) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment prefixSegment = prefix == null ? MemorySegment.NULL : toByteArray(arena, prefix);
                MemorySegment optionsSegment = toScanOptions(arena, options);
                MemorySegment iteratorOut = arena.allocate(ValueLayout.ADDRESS);
                MemorySegment result = (MemorySegment) scanPrefixHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    prefixSegment,
                    prefix == null ? 0L : (long) prefix.length,
                    optionsSegment,
                    iteratorOut
                );
                checkResult(result);
                MemorySegment iterPtr = iteratorOut.get(ValueLayout.ADDRESS, 0);
                if (iterPtr == null || iterPtr.equals(MemorySegment.NULL)) {
                    throw new SlateDbException(-1, "SlateDB returned a null iterator");
                }
                return iterPtr;
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static String metrics(MemorySegment handlePtr) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment valueOut = arena.allocate(VALUE_LAYOUT);
                MemorySegment result = (MemorySegment) metricsHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    valueOut
                );
                int error = (int) RESULT_ERROR.get(result);
                MemorySegment messageSegment = (MemorySegment) RESULT_MESSAGE.get(result);
                String message = readMessage(messageSegment);
                RuntimeException failure = (error == 0) ? null : new SlateDbException(error, message);
                freeResult(result, failure);
                if (failure != null) {
                    throw failure;
                }
                byte[] bytes = copyValue(valueOut, null);
                return new String(bytes, StandardCharsets.UTF_8);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static byte[] get(MemorySegment handlePtr, byte[] key, ReadOptions options) {
            Objects.requireNonNull(key, "key");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment keySegment = toByteArray(arena, key);
                MemorySegment optionsSegment = toReadOptions(arena, options);
                MemorySegment valueOut = arena.allocate(VALUE_LAYOUT);
                MemorySegment result = (MemorySegment) getHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    keySegment,
                    (long) key.length,
                    optionsSegment,
                    valueOut
                );
                int error = (int) RESULT_ERROR.get(result);
                MemorySegment messageSegment = (MemorySegment) RESULT_MESSAGE.get(result);
                String message = readMessage(messageSegment);
                RuntimeException failure = (error == 0) ? null
                    : (error == 2 ? null : new SlateDbException(error, message));
                freeResult(result, failure);
                if (failure != null) {
                    throw failure;
                }
                if (error == 2) {
                    return null;
                }
                return copyValue(valueOut, null);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void put(
            MemorySegment handlePtr,
            byte[] key,
            byte[] value,
            PutOptions putOptions,
            WriteOptions writeOptions
        ) {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment keySegment = toByteArray(arena, key);
                MemorySegment valueSegment = toByteArray(arena, value);
                MemorySegment putOptionsSegment = toPutOptions(arena, putOptions);
                MemorySegment writeOptionsSegment = toWriteOptions(arena, writeOptions);
                MemorySegment result = (MemorySegment) putHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    keySegment,
                    (long) key.length,
                    valueSegment,
                    (long) value.length,
                    putOptionsSegment,
                    writeOptionsSegment
                );
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static byte[] readerGet(MemorySegment readerHandle, byte[] key, ReadOptions options) {
            Objects.requireNonNull(key, "key");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(READER_HANDLE_LAYOUT);
                READER_HANDLE_PTR.set(handleStruct, readerHandle);
                MemorySegment keySegment = toByteArray(arena, key);
                MemorySegment optionsSegment = toReadOptions(arena, options);
                MemorySegment valueOut = arena.allocate(VALUE_LAYOUT);
                MemorySegment result = (MemorySegment) readerGetHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    keySegment,
                    (long) key.length,
                    optionsSegment,
                    valueOut
                );
                int error = (int) RESULT_ERROR.get(result);
                MemorySegment messageSegment = (MemorySegment) RESULT_MESSAGE.get(result);
                String message = readMessage(messageSegment);
                RuntimeException failure = (error == 0) ? null
                    : (error == 2 ? null : new SlateDbException(error, message));
                freeResult(result, failure);
                if (failure != null) {
                    throw failure;
                }
                if (error == 2) {
                    return null;
                }
                return copyValue(valueOut, null);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static MemorySegment readerScan(
            MemorySegment readerHandle,
            byte[] startKey,
            byte[] endKey,
            ScanOptions options
        ) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(READER_HANDLE_LAYOUT);
                READER_HANDLE_PTR.set(handleStruct, readerHandle);
                MemorySegment startKeySegment = startKey == null ? MemorySegment.NULL : toByteArray(arena, startKey);
                MemorySegment endKeySegment = endKey == null ? MemorySegment.NULL : toByteArray(arena, endKey);
                MemorySegment optionsSegment = toScanOptions(arena, options);
                MemorySegment iteratorOut = arena.allocate(ValueLayout.ADDRESS);
                MemorySegment result = (MemorySegment) readerScanHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    startKeySegment,
                    startKey == null ? 0L : (long) startKey.length,
                    endKeySegment,
                    endKey == null ? 0L : (long) endKey.length,
                    optionsSegment,
                    iteratorOut
                );
                checkResult(result);
                MemorySegment iterPtr = iteratorOut.get(ValueLayout.ADDRESS, 0);
                if (iterPtr == null || iterPtr.equals(MemorySegment.NULL)) {
                    throw new SlateDbException(-1, "SlateDB returned a null iterator");
                }
                return iterPtr;
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static MemorySegment readerScanPrefix(
            MemorySegment readerHandle,
            byte[] prefix,
            ScanOptions options
        ) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(READER_HANDLE_LAYOUT);
                READER_HANDLE_PTR.set(handleStruct, readerHandle);
                MemorySegment prefixSegment = prefix == null ? MemorySegment.NULL : toByteArray(arena, prefix);
                MemorySegment optionsSegment = toScanOptions(arena, options);
                MemorySegment iteratorOut = arena.allocate(ValueLayout.ADDRESS);
                MemorySegment result = (MemorySegment) readerScanPrefixHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    prefixSegment,
                    prefix == null ? 0L : (long) prefix.length,
                    optionsSegment,
                    iteratorOut
                );
                checkResult(result);
                MemorySegment iterPtr = iteratorOut.get(ValueLayout.ADDRESS, 0);
                if (iterPtr == null || iterPtr.equals(MemorySegment.NULL)) {
                    throw new SlateDbException(-1, "SlateDB returned a null iterator");
                }
                return iterPtr;
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void readerClose(MemorySegment readerHandle) {
            if (readerHandle == null || readerHandle.equals(MemorySegment.NULL)) {
                return;
            }
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(READER_HANDLE_LAYOUT);
                READER_HANDLE_PTR.set(handleStruct, readerHandle);
                MemorySegment result =
                    (MemorySegment) readerCloseHandle.invokeExact((SegmentAllocator) arena, handleStruct);
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static KeyValue iteratorNext(MemorySegment iterPtr) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment kvOut = arena.allocate(KEY_VALUE_LAYOUT);
                MemorySegment result = (MemorySegment) iteratorNextHandle.invokeExact(
                    (SegmentAllocator) arena,
                    iterPtr,
                    kvOut
                );
                int error = (int) RESULT_ERROR.get(result);
                MemorySegment messageSegment = (MemorySegment) RESULT_MESSAGE.get(result);
                String message = readMessage(messageSegment);
                RuntimeException failure = (error == 0) ? null
                    : (error == 2 ? null : new SlateDbException(error, message));
                freeResult(result, failure);
                if (failure != null) {
                    throw failure;
                }
                if (error == 2) {
                    return null;
                }
                return copyKeyValuePair(kvOut);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void iteratorSeek(MemorySegment iterPtr, byte[] key) {
            Objects.requireNonNull(key, "key");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment keySegment = toByteArray(arena, key);
                MemorySegment result = (MemorySegment) iteratorSeekHandle.invokeExact(
                    (SegmentAllocator) arena,
                    iterPtr,
                    keySegment,
                    (long) key.length
                );
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void iteratorClose(MemorySegment iterPtr) {
            if (iterPtr == null || iterPtr.equals(MemorySegment.NULL)) {
                return;
            }
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment result =
                    (MemorySegment) iteratorCloseHandle.invokeExact((SegmentAllocator) arena, iterPtr);
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void freeScanResult(MemorySegment scanResult) {
            ensureInitialized();
            try {
                freeScanResultHandle.invokeExact(scanResult);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void writeBatchPut(MemorySegment batchPtr, byte[] key, byte[] value) {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment keySegment = toByteArray(arena, key);
                MemorySegment valueSegment = toByteArray(arena, value);
                MemorySegment result = (MemorySegment) writeBatchPutHandle.invokeExact(
                    (SegmentAllocator) arena,
                    batchPtr,
                    keySegment,
                    (long) key.length,
                    valueSegment,
                    (long) value.length
                );
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void writeBatchPutWithOptions(MemorySegment batchPtr, byte[] key, byte[] value, PutOptions options) {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment keySegment = toByteArray(arena, key);
                MemorySegment valueSegment = toByteArray(arena, value);
                MemorySegment optionsSegment = toPutOptions(arena, options);
                MemorySegment result = (MemorySegment) writeBatchPutWithOptionsHandle.invokeExact(
                    (SegmentAllocator) arena,
                    batchPtr,
                    keySegment,
                    (long) key.length,
                    valueSegment,
                    (long) value.length,
                    optionsSegment
                );
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void writeBatchDelete(MemorySegment batchPtr, byte[] key) {
            Objects.requireNonNull(key, "key");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment keySegment = toByteArray(arena, key);
                MemorySegment result = (MemorySegment) writeBatchDeleteHandle.invokeExact(
                    (SegmentAllocator) arena,
                    batchPtr,
                    keySegment,
                    (long) key.length
                );
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void writeBatchWrite(MemorySegment handlePtr, MemorySegment batchPtr, WriteOptions options) {
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment optionsSegment = toWriteOptions(arena, options);
                MemorySegment result = (MemorySegment) writeBatchWriteHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    batchPtr,
                    optionsSegment
                );
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void writeBatchClose(MemorySegment batchPtr) {
            if (batchPtr == null || batchPtr.equals(MemorySegment.NULL)) {
                return;
            }
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment result = (MemorySegment) writeBatchCloseHandle.invokeExact(
                    (SegmentAllocator) arena,
                    batchPtr
                );
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static void put(MemorySegment handlePtr, byte[] key, byte[] value) {
            put(handlePtr, key, value, null, null);
        }

        static byte[] get(MemorySegment handlePtr, byte[] key) {
            return get(handlePtr, key, null);
        }

        private static void ensureInitialized() {
            if (!initialized) {
                loadLibrary();
            }
        }

        private static void initHandles() {
            synchronized (INIT_LOCK) {
                if (initialized) {
                    return;
                }
                SymbolLookup lookup = SymbolLookup.loaderLookup().or(LINKER.defaultLookup());
                initLoggingHandle = downcall(lookup, "slatedb_init_logging",
                    FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));
                settingsDefaultHandle = downcall(lookup, "slatedb_settings_default",
                    FunctionDescriptor.of(ValueLayout.ADDRESS));
                settingsFromFileHandle = downcall(lookup, "slatedb_settings_from_file",
                    FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
                settingsFromEnvHandle = downcall(lookup, "slatedb_settings_from_env",
                    FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
                settingsLoadHandle = downcall(lookup, "slatedb_settings_load",
                    FunctionDescriptor.of(ValueLayout.ADDRESS));
                openHandle = downcall(lookup, "slatedb_open",
                    FunctionDescriptor.of(HANDLE_RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
                deleteHandle = downcall(lookup, "slatedb_delete_with_options",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        HANDLE_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS));
                flushHandle = downcall(lookup, "slatedb_flush",
                    FunctionDescriptor.of(RESULT_LAYOUT, HANDLE_LAYOUT));
                scanHandle = downcall(lookup, "slatedb_scan_with_options",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        HANDLE_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
                scanPrefixHandle = downcall(lookup, "slatedb_scan_prefix_with_options",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        HANDLE_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
                metricsHandle = downcall(lookup, "slatedb_metrics",
                    FunctionDescriptor.of(RESULT_LAYOUT, HANDLE_LAYOUT, ValueLayout.ADDRESS));
                builderNewHandle = downcall(lookup, "slatedb_builder_new",
                    FunctionDescriptor.of(ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
                builderWithSettingsHandle = downcall(lookup, "slatedb_builder_with_settings",
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
                builderWithSstBlockSizeHandle = downcall(lookup, "slatedb_builder_with_sst_block_size",
                    FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));
                builderBuildHandle = downcall(lookup, "slatedb_builder_build",
                    FunctionDescriptor.of(HANDLE_LAYOUT, ValueLayout.ADDRESS));
                builderFreeHandle = downcall(lookup, "slatedb_builder_free",
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
                readerOpenHandle = downcall(lookup, "slatedb_reader_open",
                    FunctionDescriptor.of(READER_HANDLE_RESULT_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
                readerGetHandle = downcall(lookup, "slatedb_reader_get_with_options",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        READER_HANDLE_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
                readerScanHandle = downcall(lookup, "slatedb_reader_scan_with_options",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        READER_HANDLE_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
                readerScanPrefixHandle = downcall(lookup, "slatedb_reader_scan_prefix_with_options",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        READER_HANDLE_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
                readerCloseHandle = downcall(lookup, "slatedb_reader_close",
                    FunctionDescriptor.of(RESULT_LAYOUT, READER_HANDLE_LAYOUT));
                iteratorNextHandle = downcall(lookup, "slatedb_iterator_next",
                    FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
                iteratorSeekHandle = downcall(lookup, "slatedb_iterator_seek",
                    FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
                iteratorCloseHandle = downcall(lookup, "slatedb_iterator_close",
                    FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));
                putHandle = downcall(lookup, "slatedb_put_with_options",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        HANDLE_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
                getHandle = downcall(lookup, "slatedb_get_with_options",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        HANDLE_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
                closeHandle = downcall(lookup, "slatedb_close",
                    FunctionDescriptor.of(RESULT_LAYOUT, HANDLE_LAYOUT));
                freeResultHandle = downcall(lookup, "slatedb_free_result",
                    FunctionDescriptor.ofVoid(RESULT_LAYOUT));
                freeValueHandle = downcall(lookup, "slatedb_free_value",
                    FunctionDescriptor.ofVoid(VALUE_LAYOUT));
                freeScanResultHandle = downcall(lookup, "slatedb_free_scan_result",
                    FunctionDescriptor.ofVoid(SCAN_RESULT_LAYOUT));
                freeCStringHandle = downcall(lookup, "free",
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
                writeBatchNewHandle = downcall(lookup, "slatedb_write_batch_new",
                    FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));
                writeBatchPutHandle = downcall(lookup, "slatedb_write_batch_put",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG));
                writeBatchPutWithOptionsHandle = downcall(lookup, "slatedb_write_batch_put_with_options",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS));
                writeBatchDeleteHandle = downcall(lookup, "slatedb_write_batch_delete",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG));
                writeBatchWriteHandle = downcall(lookup, "slatedb_write_batch_write",
                    FunctionDescriptor.of(RESULT_LAYOUT,
                        HANDLE_LAYOUT,
                        ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS));
                writeBatchCloseHandle = downcall(lookup, "slatedb_write_batch_close",
                    FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));
                initialized = true;
            }
        }

        private static MethodHandle downcall(SymbolLookup lookup, String symbol, FunctionDescriptor descriptor) {
            MemorySegment address = lookup.find(symbol)
                .orElseThrow(() -> new IllegalStateException("Missing native symbol: " + symbol));
            return LINKER.downcallHandle(address, descriptor);
        }

        private static MemorySegment toCString(Arena arena, String value) {
            if (value == null) {
                return MemorySegment.NULL;
            }
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            MemorySegment segment = arena.allocate(bytes.length + 1, 1);
            MemorySegment.copy(MemorySegment.ofArray(bytes), 0, segment, 0, bytes.length);
            segment.set(ValueLayout.JAVA_BYTE, bytes.length, (byte) 0);
            return segment;
        }

        private static MemorySegment toByteArray(Arena arena, byte[] value) {
            MemorySegment segment = arena.allocate(value.length, 1);
            if (value.length > 0) {
                MemorySegment.copy(MemorySegment.ofArray(value), 0, segment, 0, value.length);
            }
            return segment;
        }

        private static MemorySegment toPutOptions(Arena arena, PutOptions options) {
            if (options == null) {
                return MemorySegment.NULL;
            }
            MemorySegment segment = arena.allocate(PUT_OPTIONS_LAYOUT);
            PUT_OPTIONS_TTL_TYPE.set(segment, options.ttlType.code);
            PUT_OPTIONS_TTL_VALUE.set(segment, options.ttlValueMs);
            return segment;
        }

        private static MemorySegment toWriteOptions(Arena arena, WriteOptions options) {
            if (options == null) {
                return MemorySegment.NULL;
            }
            MemorySegment segment = arena.allocate(WRITE_OPTIONS_LAYOUT);
            WRITE_OPTIONS_AWAIT_DURABLE.set(segment, options.awaitDurable);
            return segment;
        }

        private static MemorySegment toReadOptions(Arena arena, ReadOptions options) {
            if (options == null) {
                return MemorySegment.NULL;
            }
            MemorySegment segment = arena.allocate(READ_OPTIONS_LAYOUT);
            READ_OPTIONS_DURABILITY.set(segment, options.durabilityFilter.code);
            READ_OPTIONS_DIRTY.set(segment, options.dirty);
            READ_OPTIONS_CACHE_BLOCKS.set(segment, options.cacheBlocks);
            return segment;
        }

        private static MemorySegment toScanOptions(Arena arena, ScanOptions options) {
            if (options == null) {
                return MemorySegment.NULL;
            }
            MemorySegment segment = arena.allocate(SCAN_OPTIONS_LAYOUT);
            SCAN_OPTIONS_DURABILITY.set(segment, options.durabilityFilter.code);
            SCAN_OPTIONS_DIRTY.set(segment, options.dirty);
            SCAN_OPTIONS_READ_AHEAD_BYTES.set(segment, options.readAheadBytes);
            SCAN_OPTIONS_CACHE_BLOCKS.set(segment, options.cacheBlocks);
            SCAN_OPTIONS_MAX_FETCH_TASKS.set(segment, options.maxFetchTasks);
            return segment;
        }

        private static MemorySegment toReaderOptions(Arena arena, ReaderOptions options) {
            if (options == null) {
                return MemorySegment.NULL;
            }
            MemorySegment segment = arena.allocate(READER_OPTIONS_LAYOUT);
            READER_OPTIONS_MANIFEST_POLL_INTERVAL.set(segment, options.manifestPollIntervalMs);
            READER_OPTIONS_CHECKPOINT_LIFETIME.set(segment, options.checkpointLifetimeMs);
            READER_OPTIONS_MAX_MEMTABLE_BYTES.set(segment, options.maxMemtableBytes);
            READER_OPTIONS_SKIP_WAL_REPLAY.set(segment, options.skipWalReplay);
            return segment;
        }

        private static String readMessage(MemorySegment messageSegment) {
            if (messageSegment == null || messageSegment.equals(MemorySegment.NULL)) {
                return null;
            }
            long len = 0;
            while (messageSegment.get(ValueLayout.JAVA_BYTE, len) != 0) {
                len++;
                if (len > Integer.MAX_VALUE) {
                    throw new IllegalStateException("SlateDB error message is too large");
                }
            }
            byte[] bytes = new byte[(int) len];
            MemorySegment.copy(messageSegment, 0, MemorySegment.ofArray(bytes), 0, len);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private static String readCStringAndFree(MemorySegment cstr, String errorMessage) {
            if (cstr == null || cstr.equals(MemorySegment.NULL)) {
                throw new IllegalStateException(errorMessage);
            }
            String value = readMessage(cstr);
            freeCString(cstr, null);
            return value;
        }

        private static byte[] copyValue(MemorySegment valueOut, RuntimeException pending) {
            long len = (long) VALUE_LEN.get(valueOut);
            MemorySegment data = (MemorySegment) VALUE_DATA.get(valueOut);
            if (len < 0 || len > Integer.MAX_VALUE) {
                RuntimeException sizeError = new IllegalStateException("SlateDB value too large: " + len);
                freeValue(valueOut, sizeError);
                throw sizeError;
            }
            byte[] bytes = new byte[(int) len];
            if (len > 0) {
                MemorySegment.copy(data, 0, MemorySegment.ofArray(bytes), 0, len);
            }
            freeValue(valueOut, pending);
            return bytes;
        }

        private static KeyValue copyKeyValuePair(MemorySegment kvOut) {
            MemorySegment keySegment = kvOut.asSlice(KEY_VALUE_KEY_OFFSET, VALUE_LAYOUT.byteSize());
            MemorySegment valueSegment = kvOut.asSlice(KEY_VALUE_VALUE_OFFSET, VALUE_LAYOUT.byteSize());
            RuntimeException failure = null;
            byte[] keyBytes = null;
            try {
                keyBytes = copyValue(keySegment, null);
            } catch (RuntimeException e) {
                failure = e;
            }
            byte[] valueBytes = null;
            try {
                valueBytes = copyValue(valueSegment, failure);
            } catch (RuntimeException e) {
                if (failure != null) {
                    failure.addSuppressed(e);
                    throw failure;
                }
                throw e;
            }
            if (failure != null) {
                throw failure;
            }
            return new KeyValue(keyBytes, valueBytes);
        }

        private static void checkResult(MemorySegment result) {
            int error = (int) RESULT_ERROR.get(result);
            MemorySegment messageSegment = (MemorySegment) RESULT_MESSAGE.get(result);
            String message = readMessage(messageSegment);
            RuntimeException failure = (error == 0) ? null : new SlateDbException(error, message);
            freeResult(result, failure);
            if (failure != null) {
                throw failure;
            }
        }

        private static void freeResult(MemorySegment result, RuntimeException pending) {
            try {
                freeResultHandle.invokeExact(result);
            } catch (Throwable t) {
                RuntimeException failure = new RuntimeException("Failed to free SlateDB result", t);
                if (pending != null) {
                    pending.addSuppressed(failure);
                } else {
                    throw failure;
                }
            }
        }

        private static void freeValue(MemorySegment value, RuntimeException pending) {
            try {
                freeValueHandle.invokeExact(value);
            } catch (Throwable t) {
                RuntimeException failure = new RuntimeException("Failed to free SlateDB value", t);
                if (pending != null) {
                    pending.addSuppressed(failure);
                } else {
                    throw failure;
                }
            }
        }

        private static void freeCString(MemorySegment cstr, RuntimeException pending) {
            try {
                freeCStringHandle.invokeExact(cstr);
            } catch (Throwable t) {
                RuntimeException failure = new RuntimeException("Failed to free SlateDB string", t);
                if (pending != null) {
                    pending.addSuppressed(failure);
                } else {
                    throw failure;
                }
            }
        }

        private static RuntimeException wrap(Throwable t) {
            if (t instanceof RuntimeException) {
                return (RuntimeException) t;
            }
            return new RuntimeException(t);
        }
    }

    /**
     * Runtime exception thrown when SlateDB returns an error code.
     */
    public static final class SlateDbException extends RuntimeException {
        private final int errorCode;

        /**
         * Creates a new exception with the SlateDB error code and message.
         */
        public SlateDbException(int errorCode, String message) {
            super(message == null ? ("SlateDB error " + errorCode) : message);
            this.errorCode = errorCode;
        }

        /**
         * Returns the native error code.
         */
        public int getErrorCode() {
            return errorCode;
        }
    }

    /**
     * Durability level used for reads and scans.
     */
    public enum Durability {
        MEMORY(0),
        REMOTE(1);

        private final int code;

        Durability(int code) {
            this.code = code;
        }
    }

    /**
     * TTL behavior for put operations.
     */
    public enum TtlType {
        DEFAULT(0),
        NO_EXPIRY(1),
        EXPIRE_AFTER(2);

        private final int code;

        TtlType(int code) {
            this.code = code;
        }
    }

    /**
     * Supported SST block sizes for the builder.
     */
    public enum SstBlockSize {
        KIB_1((byte) 1, 1024),
        KIB_2((byte) 2, 2048),
        KIB_4((byte) 3, 4096),
        KIB_8((byte) 4, 8192),
        KIB_16((byte) 5, 16384),
        KIB_32((byte) 6, 32768),
        KIB_64((byte) 7, 65536);

        private final byte code;
        private final int bytes;

        SstBlockSize(byte code, int bytes) {
            this.code = code;
            this.bytes = bytes;
        }

        /**
         * Returns the block size in bytes.
         */
        public int bytes() {
            return bytes;
        }
    }

    /**
     * Options for put operations.
     */
    public static final class PutOptions {
        private final TtlType ttlType;
        private final long ttlValueMs;

        private PutOptions(TtlType ttlType, long ttlValueMs) {
            this.ttlType = Objects.requireNonNull(ttlType, "ttlType");
            if (ttlValueMs < 0) {
                throw new IllegalArgumentException("ttlValueMs must be >= 0");
            }
            this.ttlValueMs = ttlValueMs;
        }

        /**
         * Uses SlateDB default TTL behavior.
         */
        public static PutOptions defaultTtl() {
            return new PutOptions(TtlType.DEFAULT, 0);
        }

        /**
         * Disables TTL expiry for the entry.
         */
        public static PutOptions noExpiry() {
            return new PutOptions(TtlType.NO_EXPIRY, 0);
        }

        /**
         * Expires the entry after the provided duration.
         */
        public static PutOptions expireAfter(Duration ttl) {
            Objects.requireNonNull(ttl, "ttl");
            return expireAfterMillis(ttl.toMillis());
        }

        /**
         * Expires the entry after the provided number of milliseconds.
         */
        public static PutOptions expireAfterMillis(long ttlMillis) {
            return new PutOptions(TtlType.EXPIRE_AFTER, ttlMillis);
        }

        public TtlType ttlType() {
            return ttlType;
        }

        public long ttlValueMs() {
            return ttlValueMs;
        }
    }

    /**
     * Options for write operations.
     */
    public static final class WriteOptions {
        public static final WriteOptions DEFAULT = new WriteOptions(true);

        private final boolean awaitDurable;

        /**
         * Creates write options.
         *
         * @param awaitDurable whether the call should await durability in object storage
         */
        public WriteOptions(boolean awaitDurable) {
            this.awaitDurable = awaitDurable;
        }

        public boolean awaitDurable() {
            return awaitDurable;
        }
    }

    /**
     * Options for read operations.
     */
    public static final class ReadOptions {
        public static final ReadOptions DEFAULT = builder().build();

        private final Durability durabilityFilter;
        private final boolean dirty;
        private final boolean cacheBlocks;

        private ReadOptions(Builder builder) {
            this.durabilityFilter = builder.durabilityFilter;
            this.dirty = builder.dirty;
            this.cacheBlocks = builder.cacheBlocks;
        }

        public Durability durabilityFilter() {
            return durabilityFilter;
        }

        public boolean dirty() {
            return dirty;
        }

        public boolean cacheBlocks() {
            return cacheBlocks;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private Durability durabilityFilter = Durability.MEMORY;
            private boolean dirty;
            private boolean cacheBlocks = true;

            public Builder durabilityFilter(Durability durabilityFilter) {
                this.durabilityFilter = Objects.requireNonNull(durabilityFilter, "durabilityFilter");
                return this;
            }

            public Builder dirty(boolean dirty) {
                this.dirty = dirty;
                return this;
            }

            public Builder cacheBlocks(boolean cacheBlocks) {
                this.cacheBlocks = cacheBlocks;
                return this;
            }

            public ReadOptions build() {
                return new ReadOptions(this);
            }
        }
    }

    /**
     * Options for scan operations.
     */
    public static final class ScanOptions {
        public static final ScanOptions DEFAULT = builder().build();

        private final Durability durabilityFilter;
        private final boolean dirty;
        private final long readAheadBytes;
        private final boolean cacheBlocks;
        private final long maxFetchTasks;

        private ScanOptions(Builder builder) {
            this.durabilityFilter = builder.durabilityFilter;
            this.dirty = builder.dirty;
            this.readAheadBytes = builder.readAheadBytes;
            this.cacheBlocks = builder.cacheBlocks;
            this.maxFetchTasks = builder.maxFetchTasks;
        }

        public Durability durabilityFilter() {
            return durabilityFilter;
        }

        public boolean dirty() {
            return dirty;
        }

        public long readAheadBytes() {
            return readAheadBytes;
        }

        public boolean cacheBlocks() {
            return cacheBlocks;
        }

        public long maxFetchTasks() {
            return maxFetchTasks;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private Durability durabilityFilter = Durability.MEMORY;
            private boolean dirty;
            private long readAheadBytes = 1;
            private boolean cacheBlocks;
            private long maxFetchTasks = 1;

            public Builder durabilityFilter(Durability durabilityFilter) {
                this.durabilityFilter = Objects.requireNonNull(durabilityFilter, "durabilityFilter");
                return this;
            }

            public Builder dirty(boolean dirty) {
                this.dirty = dirty;
                return this;
            }

            public Builder readAheadBytes(long readAheadBytes) {
                if (readAheadBytes < 0) {
                    throw new IllegalArgumentException("readAheadBytes must be >= 0");
                }
                this.readAheadBytes = readAheadBytes;
                return this;
            }

            public Builder cacheBlocks(boolean cacheBlocks) {
                this.cacheBlocks = cacheBlocks;
                return this;
            }

            public Builder maxFetchTasks(long maxFetchTasks) {
                if (maxFetchTasks < 0) {
                    throw new IllegalArgumentException("maxFetchTasks must be >= 0");
                }
                this.maxFetchTasks = maxFetchTasks;
                return this;
            }

            public ScanOptions build() {
                return new ScanOptions(this);
            }
        }
    }

    /**
     * Options for opening a read-only {@link SlateDbReader}.
     */
    public static final class ReaderOptions {
        private final long manifestPollIntervalMs;
        private final long checkpointLifetimeMs;
        private final long maxMemtableBytes;
        private final boolean skipWalReplay;

        private ReaderOptions(Builder builder) {
            this.manifestPollIntervalMs = builder.manifestPollIntervalMs;
            this.checkpointLifetimeMs = builder.checkpointLifetimeMs;
            this.maxMemtableBytes = builder.maxMemtableBytes;
            this.skipWalReplay = builder.skipWalReplay;
        }

        public long manifestPollIntervalMs() {
            return manifestPollIntervalMs;
        }

        public long checkpointLifetimeMs() {
            return checkpointLifetimeMs;
        }

        public long maxMemtableBytes() {
            return maxMemtableBytes;
        }

        public boolean skipWalReplay() {
            return skipWalReplay;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private long manifestPollIntervalMs;
            private long checkpointLifetimeMs;
            private long maxMemtableBytes;
            private boolean skipWalReplay;

            public Builder manifestPollInterval(Duration interval) {
                Objects.requireNonNull(interval, "interval");
                this.manifestPollIntervalMs = interval.toMillis();
                return this;
            }

            public Builder checkpointLifetime(Duration lifetime) {
                Objects.requireNonNull(lifetime, "lifetime");
                this.checkpointLifetimeMs = lifetime.toMillis();
                return this;
            }

            public Builder maxMemtableBytes(long maxMemtableBytes) {
                if (maxMemtableBytes < 0) {
                    throw new IllegalArgumentException("maxMemtableBytes must be >= 0");
                }
                this.maxMemtableBytes = maxMemtableBytes;
                return this;
            }

            public Builder skipWalReplay(boolean skipWalReplay) {
                this.skipWalReplay = skipWalReplay;
                return this;
            }

            public ReaderOptions build() {
                return new ReaderOptions(this);
            }
        }
    }

    /**
     * Builder for creating a SlateDB instance with custom settings.
     */
    public static final class Builder implements AutoCloseable {
        private MemorySegment builderPtr;
        private boolean closed;

        private Builder(MemorySegment builderPtr) {
            this.builderPtr = builderPtr;
        }

        /**
         * Applies settings JSON to this builder.
         */
        public Builder withSettingsJson(String settingsJson) {
            ensureOpen();
            Native.builderWithSettings(builderPtr, settingsJson);
            return this;
        }

        /**
         * Sets the SST block size used by the database.
         */
        public Builder withSstBlockSize(SstBlockSize blockSize) {
            ensureOpen();
            Native.builderWithSstBlockSize(builderPtr, blockSize);
            return this;
        }

        /**
         * Builds and opens the database. The builder is consumed after this call.
         */
        public SlateDb build() {
            ensureOpen();
            MemorySegment handlePtr = Native.builderBuild(builderPtr);
            builderPtr = MemorySegment.NULL;
            closed = true;
            return new SlateDb(handlePtr);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            Native.builderFree(builderPtr);
            builderPtr = MemorySegment.NULL;
            closed = true;
        }

        private void ensureOpen() {
            if (closed || builderPtr == null || builderPtr.equals(MemorySegment.NULL)) {
                throw new IllegalStateException("Builder is closed");
            }
        }
    }

    /**
     * Read-only SlateDB handle.
     */
    public static final class SlateDbReader implements AutoCloseable {
        private final MemorySegment handle;

        private SlateDbReader(MemorySegment handle) {
            this.handle = handle;
        }

        /**
         * Returns the value for the given key, or {@code null} if not found.
         */
        public byte[] get(byte[] key) {
            return get(key, null);
        }

        /**
         * Returns the value for the given key using custom read options, or {@code null} if not found.
         */
        public byte[] get(byte[] key, ReadOptions options) {
            return Native.readerGet(handle, key, options);
        }

        /**
         * Creates a scan iterator over the range {@code [startKey, endKey)} using default scan options.
         */
        public ScanIterator scan(byte[] startKey, byte[] endKey) {
            return scan(startKey, endKey, null);
        }

        /**
         * Creates a scan iterator over the range {@code [startKey, endKey)} using custom scan options.
         */
        public ScanIterator scan(byte[] startKey, byte[] endKey, ScanOptions options) {
            return new ScanIterator(Native.readerScan(handle, startKey, endKey, options));
        }

        /**
         * Creates a scan iterator for the provided key prefix using default scan options.
         */
        public ScanIterator scanPrefix(byte[] prefix) {
            return scanPrefix(prefix, null);
        }

        /**
         * Creates a scan iterator for the provided key prefix using custom scan options.
         */
        public ScanIterator scanPrefix(byte[] prefix, ScanOptions options) {
            return new ScanIterator(Native.readerScanPrefix(handle, prefix, options));
        }

        @Override
        public void close() {
            Native.readerClose(handle);
        }
    }

    /**
     * Key/value pair returned by scan iterators.
     */
    public record KeyValue(byte[] key, byte[] value) {}

    /**
     * Iterator over scan results. Always close after use.
     */
    public static final class ScanIterator implements AutoCloseable {
        private MemorySegment iterPtr;
        private boolean closed;

        private ScanIterator(MemorySegment iterPtr) {
            this.iterPtr = iterPtr;
        }

        /**
         * Returns the next key/value pair, or {@code null} when the iterator is exhausted.
         */
        public KeyValue next() {
            ensureOpen();
            return Native.iteratorNext(iterPtr);
        }

        /**
         * Seeks to the first entry whose key is greater than or equal to the provided key.
         */
        public void seek(byte[] key) {
            ensureOpen();
            Native.iteratorSeek(iterPtr, key);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            Native.iteratorClose(iterPtr);
            iterPtr = MemorySegment.NULL;
            closed = true;
        }

        private void ensureOpen() {
            if (closed || iterPtr == null || iterPtr.equals(MemorySegment.NULL)) {
                throw new IllegalStateException("Iterator is closed");
            }
        }
    }

    /**
     * A batch of write operations that can be executed atomically.
     */
    public static final class WriteBatch implements AutoCloseable {
        private MemorySegment batchPtr;
        private boolean closed;
        private boolean consumed;

        private WriteBatch(MemorySegment batchPtr) {
            this.batchPtr = batchPtr;
        }

        /**
         * Adds a key/value pair to the batch using default put options.
         */
        public void put(byte[] key, byte[] value) {
            ensureOpen();
            Native.writeBatchPut(batchPtr, key, value);
        }

        /**
         * Adds a key/value pair to the batch using custom put options.
         */
        public void put(byte[] key, byte[] value, PutOptions options) {
            ensureOpen();
            Native.writeBatchPutWithOptions(batchPtr, key, value, options);
        }

        /**
         * Adds a delete operation to the batch.
         */
        public void delete(byte[] key) {
            ensureOpen();
            Native.writeBatchDelete(batchPtr, key);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            Native.writeBatchClose(batchPtr);
            closed = true;
            batchPtr = MemorySegment.NULL;
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("WriteBatch is closed");
            }
            if (consumed) {
                throw new IllegalStateException("WriteBatch already consumed");
            }
        }
    }
}
