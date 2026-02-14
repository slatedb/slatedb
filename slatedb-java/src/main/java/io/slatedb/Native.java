package io.slatedb;

import io.slatedb.SlateDb.SlateDbException;
import io.slatedb.SlateDbConfig.*;

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
import java.util.Objects;

final class Native {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final Object INIT_LOCK = new Object();

    private static final GroupLayout RESULT_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("kind"),
        ValueLayout.JAVA_INT.withName("close_reason"),
        ValueLayout.ADDRESS.withName("message")
    );

    private static final GroupLayout READ_OPTIONS_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_BYTE.withName("durability_filter"),
        ValueLayout.JAVA_BOOLEAN.withName("dirty"),
        ValueLayout.JAVA_BOOLEAN.withName("cache_blocks")
    );

    private static final GroupLayout PUT_OPTIONS_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_BYTE.withName("ttl_type"),
        MemoryLayout.paddingLayout(7),
        ValueLayout.JAVA_LONG.withName("ttl_value")
    );

    private static final GroupLayout WRITE_OPTIONS_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_BOOLEAN.withName("await_durable")
    );

    private static final GroupLayout BOUND_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_BYTE.withName("kind"),
        MemoryLayout.paddingLayout(7),
        ValueLayout.ADDRESS.withName("data"),
        ValueLayout.JAVA_LONG.withName("len")
    );

    private static final GroupLayout RANGE_LAYOUT = MemoryLayout.structLayout(
        BOUND_LAYOUT.withName("start"),
        BOUND_LAYOUT.withName("end")
    );

    private static final GroupLayout SCAN_OPTIONS_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_BYTE.withName("durability_filter"),
        ValueLayout.JAVA_BOOLEAN.withName("dirty"),
        MemoryLayout.paddingLayout(6),
        ValueLayout.JAVA_LONG.withName("read_ahead_bytes"),
        ValueLayout.JAVA_BOOLEAN.withName("cache_blocks"),
        MemoryLayout.paddingLayout(7),
        ValueLayout.JAVA_LONG.withName("max_fetch_tasks")
    );

    private static final GroupLayout READER_OPTIONS_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_LONG.withName("manifest_poll_interval_ms"),
        ValueLayout.JAVA_LONG.withName("checkpoint_lifetime_ms"),
        ValueLayout.JAVA_LONG.withName("max_memtable_bytes"),
        ValueLayout.JAVA_BOOLEAN.withName("skip_wal_replay"),
        MemoryLayout.paddingLayout(7)
    );

    private static final VarHandle RESULT_KIND =
        RESULT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("kind"));
    private static final VarHandle RESULT_MESSAGE =
        RESULT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("message"));

    private static final VarHandle READ_OPTIONS_DURABILITY =
        READ_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("durability_filter"));
    private static final VarHandle READ_OPTIONS_DIRTY =
        READ_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("dirty"));
    private static final VarHandle READ_OPTIONS_CACHE_BLOCKS =
        READ_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("cache_blocks"));

    private static final VarHandle PUT_OPTIONS_TTL_TYPE =
        PUT_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("ttl_type"));
    private static final VarHandle PUT_OPTIONS_TTL_VALUE =
        PUT_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("ttl_value"));

    private static final VarHandle WRITE_OPTIONS_AWAIT_DURABLE =
        WRITE_OPTIONS_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("await_durable"));

    private static final VarHandle BOUND_KIND =
        BOUND_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("kind"));
    private static final VarHandle BOUND_DATA =
        BOUND_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("data"));
    private static final VarHandle BOUND_LEN =
        BOUND_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("len"));

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

    private static final int ERROR_KIND_NONE = 0;
    private static final int ERROR_KIND_INVALID = 4;

    private static final byte BOUND_KIND_UNBOUNDED = 0;
    private static final byte BOUND_KIND_INCLUDED = 1;
    private static final byte BOUND_KIND_EXCLUDED = 2;

    private static final byte LOG_LEVEL_ERROR = 1;
    private static final byte LOG_LEVEL_WARN = 2;
    private static final byte LOG_LEVEL_INFO = 3;
    private static final byte LOG_LEVEL_DEBUG = 4;
    private static final byte LOG_LEVEL_TRACE = 5;

    private static final boolean VARHANDLE_REQUIRES_OFFSET =
        RESULT_KIND.coordinateTypes().size() == 2;

    private static volatile boolean initialized;

    private static MethodHandle initLoggingHandle;
    private static MethodHandle settingsDefaultHandle;
    private static MethodHandle settingsFromFileHandle;
    private static MethodHandle settingsFromJsonHandle;
    private static MethodHandle settingsFromEnvHandle;
    private static MethodHandle settingsLoadHandle;
    private static MethodHandle settingsToJsonHandle;
    private static MethodHandle settingsCloseHandle;

    private static MethodHandle objectStoreFromUrlHandle;
    private static MethodHandle objectStoreFromEnvHandle;
    private static MethodHandle objectStoreCloseHandle;

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
    private static MethodHandle freeBytesHandle;

    private static final GroupLayout WRITE_HANDLE_LAYOUT = MemoryLayout.structLayout(
        ValueLayout.JAVA_LONG.withName("seq"),
        ValueLayout.JAVA_LONG.withName("create_ts"),
        ValueLayout.JAVA_BOOLEAN.withName("create_ts_present"),
        MemoryLayout.paddingLayout(7)
    ).withName("slatedb_write_handle_t");

    private static final VarHandle WRITE_HANDLE_SEQ =
        WRITE_HANDLE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("seq"));
    private static final VarHandle WRITE_HANDLE_CREATE_TS =
        WRITE_HANDLE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("create_ts"));
    private static final VarHandle WRITE_HANDLE_CREATE_TS_PRESENT =
        WRITE_HANDLE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("create_ts_present"));

    private static int getInt(VarHandle handle, MemorySegment segment) {
        return (int) (VARHANDLE_REQUIRES_OFFSET ? handle.get(segment, 0L) : handle.get(segment));
    }

    private static long getLong(VarHandle handle, MemorySegment segment) {
        return (long) (VARHANDLE_REQUIRES_OFFSET ? handle.get(segment, 0L) : handle.get(segment));
    }

    private static MemorySegment getAddress(VarHandle handle, MemorySegment segment) {
        return (MemorySegment) (VARHANDLE_REQUIRES_OFFSET ? handle.get(segment, 0L) : handle.get(segment));
    }

    private static void setByte(VarHandle handle, MemorySegment segment, byte value) {
        if (VARHANDLE_REQUIRES_OFFSET) {
            handle.set(segment, 0L, value);
        } else {
            handle.set(segment, value);
        }
    }

    private static void setLong(VarHandle handle, MemorySegment segment, long value) {
        if (VARHANDLE_REQUIRES_OFFSET) {
            handle.set(segment, 0L, value);
        } else {
            handle.set(segment, value);
        }
    }

    private static void setBoolean(VarHandle handle, MemorySegment segment, boolean value) {
        if (VARHANDLE_REQUIRES_OFFSET) {
            handle.set(segment, 0L, value);
        } else {
            handle.set(segment, value);
        }
    }

    private static void setAddress(VarHandle handle, MemorySegment segment, MemorySegment value) {
        if (VARHANDLE_REQUIRES_OFFSET) {
            handle.set(segment, 0L, value);
        } else {
            handle.set(segment, value);
        }
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
            byte nativeLevel = toNativeLogLevel(level);
            MemorySegment result = (MemorySegment) initLoggingHandle.invokeExact(
                (SegmentAllocator) arena,
                nativeLevel
            );
            checkResult(result);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static String settingsDefault() {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment settingsOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) settingsDefaultHandle.invokeExact(
                (SegmentAllocator) arena,
                settingsOut
            );
            checkResult(result);
            MemorySegment settings = settingsOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(settings)) {
                throw new IllegalStateException("slatedb_settings_default returned a null settings handle");
            }
            return settingsToJsonAndClose(settings);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static String settingsFromFile(String path) {
        Objects.requireNonNull(path, "path");
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pathSegment = toCString(arena, path);
            MemorySegment settingsOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) settingsFromFileHandle.invokeExact(
                (SegmentAllocator) arena,
                pathSegment,
                settingsOut
            );
            checkResult(result);
            MemorySegment settings = settingsOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(settings)) {
                throw new IllegalStateException("slatedb_settings_from_file returned a null settings handle");
            }
            return settingsToJsonAndClose(settings);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static String settingsFromEnv(String prefix) {
        Objects.requireNonNull(prefix, "prefix");
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment prefixSegment = toCString(arena, prefix);
            MemorySegment settingsOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) settingsFromEnvHandle.invokeExact(
                (SegmentAllocator) arena,
                prefixSegment,
                settingsOut
            );
            checkResult(result);
            MemorySegment settings = settingsOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(settings)) {
                throw new IllegalStateException("slatedb_settings_from_env returned a null settings handle");
            }
            return settingsToJsonAndClose(settings);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static String settingsLoad() {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment settingsOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) settingsLoadHandle.invokeExact(
                (SegmentAllocator) arena,
                settingsOut
            );
            checkResult(result);
            MemorySegment settings = settingsOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(settings)) {
                throw new IllegalStateException("slatedb_settings_load returned a null settings handle");
            }
            return settingsToJsonAndClose(settings);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static MemorySegment open(String path, String url, String envFile) {
        Objects.requireNonNull(path, "path");
        ensureInitialized();

        MemorySegment objectStore = MemorySegment.NULL;
        RuntimeException pending = null;
        try {
            objectStore = resolveObjectStore(url, envFile);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment pathSegment = toCString(arena, path);
                MemorySegment dbOut = arena.allocate(ValueLayout.ADDRESS);
                MemorySegment result = (MemorySegment) openHandle.invokeExact(
                    (SegmentAllocator) arena,
                    pathSegment,
                    objectStore,
                    dbOut
                );
                checkResult(result);
                MemorySegment db = dbOut.get(ValueLayout.ADDRESS, 0);
                if (isNull(db)) {
                    throw new SlateDbException(-1, "SlateDB returned a null db handle");
                }
                return db;
            }
        } catch (RuntimeException e) {
            pending = e;
            throw e;
        } catch (Throwable t) {
            pending = wrap(t);
            throw pending;
        } finally {
            closeObjectStore(objectStore, pending);
        }
    }

    static MemorySegment newBuilder(String path, String url, String envFile) {
        Objects.requireNonNull(path, "path");
        ensureInitialized();

        MemorySegment objectStore = MemorySegment.NULL;
        RuntimeException pending = null;
        try {
            objectStore = resolveObjectStore(url, envFile);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment pathSegment = toCString(arena, path);
                MemorySegment builderOut = arena.allocate(ValueLayout.ADDRESS);
                MemorySegment result = (MemorySegment) builderNewHandle.invokeExact(
                    (SegmentAllocator) arena,
                    pathSegment,
                    objectStore,
                    builderOut
                );
                checkResult(result);
                MemorySegment builder = builderOut.get(ValueLayout.ADDRESS, 0);
                if (isNull(builder)) {
                    throw new SlateDbException(-1, "SlateDB returned a null builder");
                }
                return builder;
            }
        } catch (RuntimeException e) {
            pending = e;
            throw e;
        } catch (Throwable t) {
            pending = wrap(t);
            throw pending;
        } finally {
            closeObjectStore(objectStore, pending);
        }
    }

    static void builderWithSettings(MemorySegment builderPtr, String settingsJson) {
        Objects.requireNonNull(settingsJson, "settingsJson");
        ensureInitialized();

        MemorySegment settings = MemorySegment.NULL;
        RuntimeException pending = null;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment settingsSegment = toCString(arena, settingsJson);
            MemorySegment settingsOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment parseResult = (MemorySegment) settingsFromJsonHandle.invokeExact(
                (SegmentAllocator) arena,
                settingsSegment,
                settingsOut
            );
            checkResultAllowInvalidArgument(parseResult, "Invalid settings json");
            settings = settingsOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(settings)) {
                throw new IllegalStateException("slatedb_settings_from_json returned a null settings handle");
            }

            MemorySegment result = (MemorySegment) builderWithSettingsHandle.invokeExact(
                (SegmentAllocator) arena,
                builderPtr,
                settings
            );
            checkResultAllowInvalidArgument(result, "Invalid settings json");
        } catch (RuntimeException e) {
            pending = e;
            throw e;
        } catch (Throwable t) {
            pending = wrap(t);
            throw pending;
        } finally {
            closeSettings(settings, pending);
        }
    }

    static void builderWithSstBlockSize(MemorySegment builderPtr, SstBlockSize blockSize) {
        Objects.requireNonNull(blockSize, "blockSize");
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = (MemorySegment) builderWithSstBlockSizeHandle.invokeExact(
                (SegmentAllocator) arena,
                builderPtr,
                blockSize.code()
            );
            checkResultAllowInvalidArgument(result, "Invalid SST block size: " + blockSize);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static MemorySegment builderBuild(MemorySegment builderPtr) {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment dbOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) builderBuildHandle.invokeExact(
                (SegmentAllocator) arena,
                builderPtr,
                dbOut
            );
            checkResult(result);
            MemorySegment db = dbOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(db)) {
                throw new SlateDbException(-1, "SlateDB builder returned a null db handle");
            }
            return db;
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static void builderFree(MemorySegment builderPtr) {
        if (isNull(builderPtr)) {
            return;
        }
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = (MemorySegment) builderFreeHandle.invokeExact(
                (SegmentAllocator) arena,
                builderPtr
            );
            checkResult(result);
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

        MemorySegment objectStore = MemorySegment.NULL;
        RuntimeException pending = null;
        try {
            objectStore = resolveObjectStore(url, envFile);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment pathSegment = toCString(arena, path);
                MemorySegment checkpointSegment = toCString(arena, checkpointId);
                MemorySegment optionsSegment = toReaderOptions(arena, options);
                MemorySegment readerOut = arena.allocate(ValueLayout.ADDRESS);
                MemorySegment result = (MemorySegment) readerOpenHandle.invokeExact(
                    (SegmentAllocator) arena,
                    pathSegment,
                    objectStore,
                    checkpointSegment,
                    optionsSegment,
                    readerOut
                );
                checkResult(result);
                MemorySegment reader = readerOut.get(ValueLayout.ADDRESS, 0);
                if (isNull(reader)) {
                    throw new SlateDbException(-1, "SlateDB returned a null reader handle");
                }
                return reader;
            }
        } catch (RuntimeException e) {
            pending = e;
            throw e;
        } catch (Throwable t) {
            pending = wrap(t);
            throw pending;
        } finally {
            closeObjectStore(objectStore, pending);
        }
    }

    static MemorySegment newWriteBatch() {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment batchOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) writeBatchNewHandle.invokeExact(
                (SegmentAllocator) arena,
                batchOut
            );
            checkResult(result);
            MemorySegment batchPtr = batchOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(batchPtr)) {
                throw new SlateDbException(-1, "SlateDB returned a null SlateDbWriteBatch");
            }
            return batchPtr;
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static void close(MemorySegment handlePtr) {
        if (isNull(handlePtr)) {
            return;
        }
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = (MemorySegment) closeHandle.invokeExact((SegmentAllocator) arena, handlePtr);
            checkResult(result);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static SlateDbWriteHandle delete(MemorySegment handlePtr, byte[] key, WriteOptions options) {
        Objects.requireNonNull(key, "key");
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keySegment = toByteArray(arena, key);
            MemorySegment optionsSegment = toWriteOptions(arena, options);
            MemorySegment outHandle = arena.allocate(WRITE_HANDLE_LAYOUT);

            MemorySegment result = (MemorySegment) deleteHandle.invokeExact(
                (SegmentAllocator) arena,
                handlePtr,
                keySegment,
                (long) key.length,
                optionsSegment,
                outHandle
            );
            checkResult(result);
            return readWriteHandle(outHandle);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static void flush(MemorySegment handlePtr) {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = (MemorySegment) flushHandle.invokeExact((SegmentAllocator) arena, handlePtr);
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
            MemorySegment rangeSegment = toRange(arena, startKey, endKey);
            MemorySegment optionsSegment = toScanOptions(arena, options);
            MemorySegment iteratorOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) scanHandle.invokeExact(
                (SegmentAllocator) arena,
                handlePtr,
                rangeSegment,
                optionsSegment,
                iteratorOut
            );
            checkResult(result);
            MemorySegment iterPtr = iteratorOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(iterPtr)) {
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
            MemorySegment prefixSegment = prefix == null ? MemorySegment.NULL : toByteArray(arena, prefix);
            MemorySegment optionsSegment = toScanOptions(arena, options);
            MemorySegment iteratorOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) scanPrefixHandle.invokeExact(
                (SegmentAllocator) arena,
                handlePtr,
                prefixSegment,
                prefix == null ? 0L : (long) prefix.length,
                optionsSegment,
                iteratorOut
            );
            checkResult(result);
            MemorySegment iterPtr = iteratorOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(iterPtr)) {
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
            MemorySegment valueOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment lenOut = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment result = (MemorySegment) metricsHandle.invokeExact(
                (SegmentAllocator) arena,
                handlePtr,
                valueOut,
                lenOut
            );
            checkResult(result);
            MemorySegment data = valueOut.get(ValueLayout.ADDRESS, 0);
            long len = lenOut.get(ValueLayout.JAVA_LONG, 0);
            byte[] bytes = copyOwnedBytes(data, len, null);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static byte[] get(MemorySegment handlePtr, byte[] key, ReadOptions options) {
        Objects.requireNonNull(key, "key");
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keySegment = toByteArray(arena, key);
            MemorySegment optionsSegment = toReadOptions(arena, options);
            MemorySegment presentOut = arena.allocate(ValueLayout.JAVA_BOOLEAN);
            MemorySegment valueOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment valueLenOut = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment result = (MemorySegment) getHandle.invokeExact(
                (SegmentAllocator) arena,
                handlePtr,
                keySegment,
                (long) key.length,
                optionsSegment,
                presentOut,
                valueOut,
                valueLenOut
            );
            checkResult(result);
            if (!presentOut.get(ValueLayout.JAVA_BOOLEAN, 0)) {
                return null;
            }
            return copyOwnedBytes(
                valueOut.get(ValueLayout.ADDRESS, 0),
                valueLenOut.get(ValueLayout.JAVA_LONG, 0),
                null
            );
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static SlateDbWriteHandle put(
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
            MemorySegment keySegment = toByteArray(arena, key);
            MemorySegment valueSegment = toByteArray(arena, value);
            MemorySegment putOptionsSegment = toPutOptions(arena, putOptions);
            MemorySegment writeOptionsSegment = toWriteOptions(arena, writeOptions);
            MemorySegment outHandle = arena.allocate(WRITE_HANDLE_LAYOUT);

            MemorySegment result = (MemorySegment) putHandle.invokeExact(
                (SegmentAllocator) arena,
                handlePtr,
                keySegment,
                (long) key.length,
                valueSegment,
                (long) value.length,
                putOptionsSegment,
                writeOptionsSegment,
                outHandle
            );
            checkResult(result);
            return readWriteHandle(outHandle);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static byte[] readerGet(MemorySegment readerHandle, byte[] key, ReadOptions options) {
        Objects.requireNonNull(key, "key");
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keySegment = toByteArray(arena, key);
            MemorySegment optionsSegment = toReadOptions(arena, options);
            MemorySegment presentOut = arena.allocate(ValueLayout.JAVA_BOOLEAN);
            MemorySegment valueOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment valueLenOut = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment result = (MemorySegment) readerGetHandle.invokeExact(
                (SegmentAllocator) arena,
                readerHandle,
                keySegment,
                (long) key.length,
                optionsSegment,
                presentOut,
                valueOut,
                valueLenOut
            );
            checkResult(result);
            if (!presentOut.get(ValueLayout.JAVA_BOOLEAN, 0)) {
                return null;
            }
            return copyOwnedBytes(
                valueOut.get(ValueLayout.ADDRESS, 0),
                valueLenOut.get(ValueLayout.JAVA_LONG, 0),
                null
            );
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
            MemorySegment rangeSegment = toRange(arena, startKey, endKey);
            MemorySegment optionsSegment = toScanOptions(arena, options);
            MemorySegment iteratorOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) readerScanHandle.invokeExact(
                (SegmentAllocator) arena,
                readerHandle,
                rangeSegment,
                optionsSegment,
                iteratorOut
            );
            checkResult(result);
            MemorySegment iterPtr = iteratorOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(iterPtr)) {
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
            MemorySegment prefixSegment = prefix == null ? MemorySegment.NULL : toByteArray(arena, prefix);
            MemorySegment optionsSegment = toScanOptions(arena, options);
            MemorySegment iteratorOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result = (MemorySegment) readerScanPrefixHandle.invokeExact(
                (SegmentAllocator) arena,
                readerHandle,
                prefixSegment,
                prefix == null ? 0L : (long) prefix.length,
                optionsSegment,
                iteratorOut
            );
            checkResult(result);
            MemorySegment iterPtr = iteratorOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(iterPtr)) {
                throw new SlateDbException(-1, "SlateDB returned a null iterator");
            }
            return iterPtr;
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static void readerClose(MemorySegment readerHandle) {
        if (isNull(readerHandle)) {
            return;
        }
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = (MemorySegment) readerCloseHandle.invokeExact(
                (SegmentAllocator) arena,
                readerHandle
            );
            checkResult(result);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static SlateDbKeyValue iteratorNext(MemorySegment iterPtr) {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment presentOut = arena.allocate(ValueLayout.JAVA_BOOLEAN);
            MemorySegment keyOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment keyLenOut = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment valueOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment valueLenOut = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment result = (MemorySegment) iteratorNextHandle.invokeExact(
                (SegmentAllocator) arena,
                iterPtr,
                presentOut,
                keyOut,
                keyLenOut,
                valueOut,
                valueLenOut
            );
            checkResult(result);
            if (!presentOut.get(ValueLayout.JAVA_BOOLEAN, 0)) {
                return null;
            }

            MemorySegment keyPtr = keyOut.get(ValueLayout.ADDRESS, 0);
            long keyLen = keyLenOut.get(ValueLayout.JAVA_LONG, 0);
            MemorySegment valuePtr = valueOut.get(ValueLayout.ADDRESS, 0);
            long valueLen = valueLenOut.get(ValueLayout.JAVA_LONG, 0);

            RuntimeException failure = null;
            byte[] key = null;
            try {
                key = copyOwnedBytes(keyPtr, keyLen, null);
            } catch (RuntimeException e) {
                failure = e;
            }

            byte[] value = null;
            try {
                value = copyOwnedBytes(valuePtr, valueLen, failure);
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

            return new SlateDbKeyValue(key, value);
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
        if (isNull(iterPtr)) {
            return;
        }
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = (MemorySegment) iteratorCloseHandle.invokeExact(
                (SegmentAllocator) arena,
                iterPtr
            );
            checkResult(result);
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

    static SlateDbWriteHandle writeBatchWrite(MemorySegment handlePtr, MemorySegment batchPtr, WriteOptions options) {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment optionsSegment = toWriteOptions(arena, options);
            MemorySegment outHandle = arena.allocate(WRITE_HANDLE_LAYOUT);

            MemorySegment result = (MemorySegment) writeBatchWriteHandle.invokeExact(
                (SegmentAllocator) arena,
                handlePtr,
                batchPtr,
                optionsSegment,
                outHandle
            );
            checkResult(result);
            return readWriteHandle(outHandle);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static void writeBatchClose(MemorySegment batchPtr) {
        if (isNull(batchPtr)) {
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

    static SlateDbWriteHandle put(MemorySegment handlePtr, byte[] key, byte[] value) {
        return put(handlePtr, key, value, null, null);
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

            initLoggingHandle = downcall(lookup, "slatedb_logging_init",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.JAVA_BYTE));

            settingsDefaultHandle = downcall(lookup, "slatedb_settings_default",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));
            settingsFromFileHandle = downcall(lookup, "slatedb_settings_from_file",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            settingsFromJsonHandle = downcall(lookup, "slatedb_settings_from_json",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            settingsFromEnvHandle = downcall(lookup, "slatedb_settings_from_env",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            settingsLoadHandle = downcall(lookup, "slatedb_settings_load",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));
            settingsToJsonHandle = downcall(lookup, "slatedb_settings_to_json",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            settingsCloseHandle = downcall(lookup, "slatedb_settings_close",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));

            objectStoreFromUrlHandle = downcall(lookup, "slatedb_object_store_from_url",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            objectStoreFromEnvHandle = downcall(lookup, "slatedb_object_store_from_env",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            objectStoreCloseHandle = downcall(lookup, "slatedb_object_store_close",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));

            openHandle = downcall(lookup, "slatedb_db_open",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            deleteHandle = downcall(lookup, "slatedb_db_delete_with_options",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            flushHandle = downcall(lookup, "slatedb_db_flush",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));
            scanHandle = downcall(lookup, "slatedb_db_scan_with_options",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    RANGE_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            scanPrefixHandle = downcall(lookup, "slatedb_db_scan_prefix_with_options",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            metricsHandle = downcall(lookup, "slatedb_db_metrics",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));

            builderNewHandle = downcall(lookup, "slatedb_db_builder_new",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            builderWithSettingsHandle = downcall(lookup, "slatedb_db_builder_with_settings",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            builderWithSstBlockSizeHandle = downcall(lookup, "slatedb_db_builder_with_sst_block_size",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));
            builderBuildHandle = downcall(lookup, "slatedb_db_builder_build",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            builderFreeHandle = downcall(lookup, "slatedb_db_builder_close",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));

            readerOpenHandle = downcall(lookup, "slatedb_db_reader_open",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            readerGetHandle = downcall(lookup, "slatedb_db_reader_get_with_options",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            readerScanHandle = downcall(lookup, "slatedb_db_reader_scan_with_options",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    RANGE_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            readerScanPrefixHandle = downcall(lookup, "slatedb_db_reader_scan_prefix_with_options",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            readerCloseHandle = downcall(lookup, "slatedb_db_reader_close",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));

            iteratorNextHandle = downcall(lookup, "slatedb_iterator_next",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            iteratorSeekHandle = downcall(lookup, "slatedb_iterator_seek",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
            iteratorCloseHandle = downcall(lookup, "slatedb_iterator_close",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));

            putHandle = downcall(lookup, "slatedb_db_put_with_options",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            getHandle = downcall(lookup, "slatedb_db_get_with_options",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            closeHandle = downcall(lookup, "slatedb_db_close",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));

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
            writeBatchWriteHandle = downcall(lookup, "slatedb_db_write_with_options",
                FunctionDescriptor.of(RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            writeBatchCloseHandle = downcall(lookup, "slatedb_write_batch_close",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));

            freeResultHandle = downcall(lookup, "slatedb_result_free",
                FunctionDescriptor.ofVoid(RESULT_LAYOUT));
            freeBytesHandle = downcall(lookup, "slatedb_bytes_free",
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

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
        setByte(PUT_OPTIONS_TTL_TYPE, segment, (byte) options.ttlType().code());
        setLong(PUT_OPTIONS_TTL_VALUE, segment, options.ttlValueMs());
        return segment;
    }

    private static MemorySegment toWriteOptions(Arena arena, WriteOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment segment = arena.allocate(WRITE_OPTIONS_LAYOUT);
        setBoolean(WRITE_OPTIONS_AWAIT_DURABLE, segment, options.awaitDurable());
        return segment;
    }

    private static MemorySegment toReadOptions(Arena arena, ReadOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment segment = arena.allocate(READ_OPTIONS_LAYOUT);
        setByte(READ_OPTIONS_DURABILITY, segment, (byte) options.durabilityFilter().code());
        setBoolean(READ_OPTIONS_DIRTY, segment, options.dirty());
        setBoolean(READ_OPTIONS_CACHE_BLOCKS, segment, options.cacheBlocks());
        return segment;
    }

    private static MemorySegment toScanOptions(Arena arena, ScanOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment segment = arena.allocate(SCAN_OPTIONS_LAYOUT);
        setByte(SCAN_OPTIONS_DURABILITY, segment, (byte) options.durabilityFilter().code());
        setBoolean(SCAN_OPTIONS_DIRTY, segment, options.dirty());
        setLong(SCAN_OPTIONS_READ_AHEAD_BYTES, segment, options.readAheadBytes());
        setBoolean(SCAN_OPTIONS_CACHE_BLOCKS, segment, options.cacheBlocks());
        setLong(SCAN_OPTIONS_MAX_FETCH_TASKS, segment, options.maxFetchTasks());
        return segment;
    }

    private static MemorySegment toReaderOptions(Arena arena, ReaderOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment segment = arena.allocate(READER_OPTIONS_LAYOUT);
        setLong(READER_OPTIONS_MANIFEST_POLL_INTERVAL, segment, options.manifestPollIntervalMs());
        setLong(READER_OPTIONS_CHECKPOINT_LIFETIME, segment, options.checkpointLifetimeMs());
        setLong(READER_OPTIONS_MAX_MEMTABLE_BYTES, segment, options.maxMemtableBytes());
        setBoolean(READER_OPTIONS_SKIP_WAL_REPLAY, segment, options.skipWalReplay());
        return segment;
    }

    private static MemorySegment toRange(Arena arena, byte[] startKey, byte[] endKey) {
        MemorySegment range = arena.allocate(RANGE_LAYOUT);
        MemorySegment startBound = range.asSlice(0, BOUND_LAYOUT.byteSize());
        MemorySegment endBound = range.asSlice(BOUND_LAYOUT.byteSize(), BOUND_LAYOUT.byteSize());

        if (startKey == null) {
            setByte(BOUND_KIND, startBound, BOUND_KIND_UNBOUNDED);
            setAddress(BOUND_DATA, startBound, MemorySegment.NULL);
            setLong(BOUND_LEN, startBound, 0L);
        } else {
            MemorySegment startKeySegment = toByteArray(arena, startKey);
            setByte(BOUND_KIND, startBound, BOUND_KIND_INCLUDED);
            setAddress(BOUND_DATA, startBound, startKeySegment);
            setLong(BOUND_LEN, startBound, startKey.length);
        }

        if (endKey == null) {
            setByte(BOUND_KIND, endBound, BOUND_KIND_UNBOUNDED);
            setAddress(BOUND_DATA, endBound, MemorySegment.NULL);
            setLong(BOUND_LEN, endBound, 0L);
        } else {
            MemorySegment endKeySegment = toByteArray(arena, endKey);
            setByte(BOUND_KIND, endBound, BOUND_KIND_EXCLUDED);
            setAddress(BOUND_DATA, endBound, endKeySegment);
            setLong(BOUND_LEN, endBound, endKey.length);
        }

        return range;
    }



    private static SlateDbWriteHandle readWriteHandle(MemorySegment segment) {
        long seq = (long) WRITE_HANDLE_SEQ.get(segment, 0L);
        boolean createTsPresent = (boolean) WRITE_HANDLE_CREATE_TS_PRESENT.get(segment, 0L);
        Long createTs = null;
        if (createTsPresent) {
            createTs = (long) WRITE_HANDLE_CREATE_TS.get(segment, 0L);
        }
        return new SlateDbWriteHandle(seq, createTs);
    }

    private static String readMessage(MemorySegment messageSegment) {
        if (isNull(messageSegment)) {
            return null;
        }

        MemorySegment bytesSegment = messageSegment.reinterpret((long) Integer.MAX_VALUE + 1);
        long len = 0;
        while (true) {
            if (len > Integer.MAX_VALUE) {
                throw new IllegalStateException("SlateDB error message is too large");
            }
            if (bytesSegment.get(ValueLayout.JAVA_BYTE, len) == 0) {
                break;
            }
            len++;
        }

        byte[] bytes = new byte[(int) len];
        MemorySegment.copy(bytesSegment, 0, MemorySegment.ofArray(bytes), 0, len);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static MemorySegment resolveObjectStore(String url, String envFile) {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment objectStoreOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result;

            if (url != null) {
                MemorySegment urlSegment = toCString(arena, url);
                result = (MemorySegment) objectStoreFromUrlHandle.invokeExact(
                    (SegmentAllocator) arena,
                    urlSegment,
                    objectStoreOut
                );
            } else {
                MemorySegment envSegment = toCString(arena, envFile);
                result = (MemorySegment) objectStoreFromEnvHandle.invokeExact(
                    (SegmentAllocator) arena,
                    envSegment,
                    objectStoreOut
                );
            }

            checkResult(result);
            MemorySegment objectStore = objectStoreOut.get(ValueLayout.ADDRESS, 0);
            if (isNull(objectStore)) {
                throw new SlateDbException(-1, "SlateDB returned a null object store handle");
            }
            return objectStore;
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    private static String settingsToJsonAndClose(MemorySegment settings) {
        RuntimeException pending = null;
        try {
            return settingsToJson(settings);
        } catch (RuntimeException e) {
            pending = e;
            throw e;
        } finally {
            closeSettings(settings, pending);
        }
    }

    private static String settingsToJson(MemorySegment settings) {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment jsonOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment jsonLenOut = arena.allocate(ValueLayout.JAVA_LONG);
            MemorySegment result = (MemorySegment) settingsToJsonHandle.invokeExact(
                (SegmentAllocator) arena,
                settings,
                jsonOut,
                jsonLenOut
            );
            checkResult(result);
            MemorySegment json = jsonOut.get(ValueLayout.ADDRESS, 0);
            long jsonLen = jsonLenOut.get(ValueLayout.JAVA_LONG, 0);
            byte[] bytes = copyOwnedBytes(json, jsonLen, null);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    private static byte[] copyOwnedBytes(MemorySegment data, long len, RuntimeException pending) {
        if (len < 0 || len > Integer.MAX_VALUE) {
            RuntimeException sizeError = new IllegalStateException("SlateDB value too large: " + len);
            freeBytes(data, len, sizeError);
            throw sizeError;
        }

        byte[] bytes = new byte[(int) len];
        if (len > 0) {
            if (isNull(data)) {
                RuntimeException nullData =
                    new IllegalStateException("SlateDB returned null data pointer with len=" + len);
                freeBytes(data, len, nullData);
                throw nullData;
            }
            MemorySegment dataSegment = data.reinterpret(len);
            MemorySegment.copy(dataSegment, 0, MemorySegment.ofArray(bytes), 0, len);
        }

        freeBytes(data, len, pending);
        return bytes;
    }

    private static void closeSettings(MemorySegment settings, RuntimeException pending) {
        if (isNull(settings)) {
            return;
        }

        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = (MemorySegment) settingsCloseHandle.invokeExact(
                (SegmentAllocator) arena,
                settings
            );
            checkResult(result);
        } catch (Throwable t) {
            RuntimeException failure = new RuntimeException("Failed to close SlateDB settings handle", t);
            if (pending != null) {
                pending.addSuppressed(failure);
            } else {
                throw failure;
            }
        }
    }

    private static void closeObjectStore(MemorySegment objectStore, RuntimeException pending) {
        if (isNull(objectStore)) {
            return;
        }

        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment result = (MemorySegment) objectStoreCloseHandle.invokeExact(
                (SegmentAllocator) arena,
                objectStore
            );
            checkResult(result);
        } catch (Throwable t) {
            RuntimeException failure = new RuntimeException("Failed to close SlateDB object store handle", t);
            if (pending != null) {
                pending.addSuppressed(failure);
            } else {
                throw failure;
            }
        }
    }

    private static void checkResult(final MemorySegment result) {
        final var error = getInt(RESULT_KIND, result);
        final var messageSegment = getAddress(RESULT_MESSAGE, result);
        final var message = readMessage(messageSegment);
        final RuntimeException failure = (error == ERROR_KIND_NONE) ? null : new SlateDbException(error, message);
        freeResult(result, failure);
        if (failure != null) {
            throw failure;
        }
    }

    private static void checkResultAllowInvalidArgument(MemorySegment result, String defaultInvalidMessage) {
        int error = getInt(RESULT_KIND, result);
        MemorySegment messageSegment = getAddress(RESULT_MESSAGE, result);
        String message = readMessage(messageSegment);

        RuntimeException failure = null;
        if (error != ERROR_KIND_NONE) {
            if (error == ERROR_KIND_INVALID) {
                String detail = (message == null || message.isBlank()) ? defaultInvalidMessage : message;
                failure = new IllegalArgumentException(detail);
            } else {
                failure = new SlateDbException(error, message);
            }
        }

        freeResult(result, failure);
        if (failure != null) {
            throw failure;
        }
    }

    private static void freeResult(final MemorySegment result, final RuntimeException pending) {
        try {
            freeResultHandle.invokeExact(result);
        } catch (final Throwable t) {
            final var failure = new RuntimeException("Failed to free SlateDB result", t);
            if (pending != null) {
                pending.addSuppressed(failure);
            } else {
                throw failure;
            }
        }
    }

    private static void freeBytes(MemorySegment data, long len, RuntimeException pending) {
        if (isNull(data) || len <= 0) {
            return;
        }

        try {
            freeBytesHandle.invokeExact(data, len);
        } catch (Throwable t) {
            RuntimeException failure = new RuntimeException("Failed to free SlateDB bytes", t);
            if (pending != null) {
                pending.addSuppressed(failure);
            } else {
                throw failure;
            }
        }
    }

    private static byte toNativeLogLevel(String level) {
        if (level == null) {
            throw new IllegalArgumentException("level must not be null");
        }
        return switch (level.toLowerCase()) {
            case "error" -> LOG_LEVEL_ERROR;
            case "warn" -> LOG_LEVEL_WARN;
            case "info" -> LOG_LEVEL_INFO;
            case "debug" -> LOG_LEVEL_DEBUG;
            case "trace" -> LOG_LEVEL_TRACE;
            default -> throw new IllegalArgumentException("Unsupported log level: " + level);
        };
    }

    private static boolean isNull(MemorySegment segment) {
        return segment == null || segment.equals(MemorySegment.NULL);
    }

    private static RuntimeException wrap(Throwable t) {
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        }
        return new RuntimeException(t);
    }
}
