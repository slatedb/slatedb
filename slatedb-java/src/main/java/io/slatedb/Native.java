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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class Native {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final Object INIT_LOCK = new Object();

    private static final GroupLayout HANDLE_LAYOUT =
        MemoryLayout.structLayout(ValueLayout.ADDRESS.withName("ptr"));
    private static final GroupLayout RESULT_LAYOUT = createResultLayout();
    private static final GroupLayout HANDLE_RESULT_LAYOUT =
        MemoryLayout.structLayout(HANDLE_LAYOUT.withName("handle"), RESULT_LAYOUT.withName("result"));
    private static final GroupLayout BUILDER_RESULT_LAYOUT =
        MemoryLayout.structLayout(ValueLayout.ADDRESS.withName("builder"), RESULT_LAYOUT.withName("result"));
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
    private static final VarHandle RESULT_NONE =
            RESULT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("none"));
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
    private static final VarHandle BUILDER_RESULT_BUILDER_PTR =
        BUILDER_RESULT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("builder"));
    private static final VarHandle BUILDER_RESULT_ERROR =
        BUILDER_RESULT_LAYOUT.varHandle(
            MemoryLayout.PathElement.groupElement("result"),
            MemoryLayout.PathElement.groupElement("error"));
    private static final VarHandle BUILDER_RESULT_MESSAGE =
        BUILDER_RESULT_LAYOUT.varHandle(
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

    private static final int ERROR_INVALID_ARGUMENT = 1;
    private static final boolean VARHANDLE_REQUIRES_OFFSET =
        RESULT_ERROR.coordinateTypes().size() == 2;

    private static int getInt(VarHandle handle, MemorySegment segment) {
        return (int) (VARHANDLE_REQUIRES_OFFSET ? handle.get(segment, 0L) : handle.get(segment));
    }

    private static long getLong(VarHandle handle, MemorySegment segment) {
        return (long) (VARHANDLE_REQUIRES_OFFSET ? handle.get(segment, 0L) : handle.get(segment));
    }

    private static boolean getBoolean(VarHandle handle, MemorySegment segment) {
        return (boolean) (VARHANDLE_REQUIRES_OFFSET ? handle.get(segment, 0L) : handle.get(segment));
    }

    private static MemorySegment getAddress(VarHandle handle, MemorySegment segment) {
        return (MemorySegment) (VARHANDLE_REQUIRES_OFFSET ? handle.get(segment, 0L) : handle.get(segment));
    }

    private static void setInt(VarHandle handle, MemorySegment segment, int value) {
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

    private static final long HANDLE_RESULT_RESULT_OFFSET =
        HANDLE_RESULT_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("result"));
    private static final long BUILDER_RESULT_RESULT_OFFSET =
        BUILDER_RESULT_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("result"));
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
        final List<MemoryLayout> elements = new ArrayList<>();
        elements.add(ValueLayout.JAVA_INT.withName("error"));
        elements.add(ValueLayout.JAVA_BOOLEAN.withName("none"));
        final long size = ValueLayout.JAVA_INT.byteSize() + ValueLayout.JAVA_BOOLEAN.byteSize();
        final long padding = Math.max(0, ValueLayout.ADDRESS.byteAlignment() - size);
        if (padding != 0) {
            elements.add(MemoryLayout.paddingLayout(padding));
        }
        elements.add(ValueLayout.ADDRESS.withName("message"));
        return MemoryLayout.structLayout(elements.toArray(new MemoryLayout[0]));
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
            int error = (int) getInt(HANDLE_RESULT_ERROR, handleResult);
            MemorySegment messageSegment = (MemorySegment) getAddress(HANDLE_RESULT_MESSAGE, handleResult);
            String message = readMessage(messageSegment);
            MemorySegment resultSlice = handleResult.asSlice(HANDLE_RESULT_RESULT_OFFSET, RESULT_LAYOUT.byteSize());
            RuntimeException failure = (error == 0) ? null : new SlateDbException(error, message);
            freeResult(resultSlice, failure);
            if (failure != null) {
                throw failure;
            }
            MemorySegment handlePtr = (MemorySegment) getAddress(HANDLE_RESULT_HANDLE_PTR, handleResult);
            if (handlePtr == null || handlePtr.equals(MemorySegment.NULL)) {
                throw new SlateDbException(-1, "SlateDB returned a null handle");
            }
            return handlePtr;
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static MemorySegment newBuilder(String path, String url, String envFile) {
        Objects.requireNonNull(path, "path");
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment pathSegment = toCString(arena, path);
            MemorySegment urlSegment = toCString(arena, url);
            MemorySegment envSegment = toCString(arena, envFile);
            MemorySegment builderResult = (MemorySegment) builderNewHandle.invokeExact(
                (SegmentAllocator) arena,
                pathSegment,
                urlSegment,
                envSegment
            );
            int error = (int) getInt(BUILDER_RESULT_ERROR, builderResult);
            MemorySegment messageSegment = (MemorySegment) getAddress(BUILDER_RESULT_MESSAGE, builderResult);
            String message = readMessage(messageSegment);
            RuntimeException failure = (error == 0) ? null : new SlateDbException(error, message);
            MemorySegment resultSlice =
                builderResult.asSlice(BUILDER_RESULT_RESULT_OFFSET, RESULT_LAYOUT.byteSize());
            freeResult(resultSlice, failure);
            if (failure != null) {
                throw failure;
            }
            MemorySegment builderPtr = (MemorySegment) getAddress(BUILDER_RESULT_BUILDER_PTR, builderResult);
            if (builderPtr == null || builderPtr.equals(MemorySegment.NULL)) {
                throw new SlateDbException(-1, "SlateDB returned a null builder");
            }
            return builderPtr;
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static void builderWithSettings(MemorySegment builderPtr, String settingsJson) {
        Objects.requireNonNull(settingsJson, "settingsJson");
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment settingsSegment = toCString(arena, settingsJson);
            MemorySegment result = (MemorySegment) builderWithSettingsHandle.invokeExact(
                (SegmentAllocator) arena,
                builderPtr,
                settingsSegment
            );
            checkResultAllowInvalidArgument(result, "Invalid settings JSON");
        } catch (Throwable t) {
            throw wrap(t);
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
            MemorySegment handleResult =
                (MemorySegment) builderBuildHandle.invokeExact((SegmentAllocator) arena, builderPtr);
            int error = (int) getInt(HANDLE_RESULT_ERROR, handleResult);
            MemorySegment messageSegment = (MemorySegment) getAddress(HANDLE_RESULT_MESSAGE, handleResult);
            String message = readMessage(messageSegment);
            MemorySegment resultSlice =
                handleResult.asSlice(HANDLE_RESULT_RESULT_OFFSET, RESULT_LAYOUT.byteSize());
            RuntimeException failure = (error == 0) ? null : new SlateDbException(error, message);
            freeResult(resultSlice, failure);
            if (failure != null) {
                throw failure;
            }
            MemorySegment handlePtr = (MemorySegment) getAddress(HANDLE_RESULT_HANDLE_PTR, handleResult);
            if (handlePtr == null || handlePtr.equals(MemorySegment.NULL)) {
                throw new SlateDbException(-1, "SlateDB builder returned a null handle");
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
            int error = (int) getInt(READER_HANDLE_RESULT_ERROR, handleResult);
            MemorySegment messageSegment = (MemorySegment) getAddress(READER_HANDLE_RESULT_MESSAGE, handleResult);
            String message = readMessage(messageSegment);
            MemorySegment resultSlice =
                handleResult.asSlice(READER_HANDLE_RESULT_OFFSET, RESULT_LAYOUT.byteSize());
            RuntimeException failure = (error == 0) ? null : new SlateDbException(error, message);
            freeResult(resultSlice, failure);
            if (failure != null) {
                throw failure;
            }
            MemorySegment handlePtr = (MemorySegment) getAddress(READER_HANDLE_RESULT_HANDLE_PTR, handleResult);
            if (handlePtr == null || handlePtr.equals(MemorySegment.NULL)) {
                throw new SlateDbException(-1, "SlateDB returned a null reader handle");
            }
            return handlePtr;
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static MemorySegment newWriteBatch() {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment batchOut = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment result =
                (MemorySegment) writeBatchNewHandle.invokeExact((SegmentAllocator) arena, batchOut);
            checkResult(result);
            MemorySegment batchPtr = batchOut.get(ValueLayout.ADDRESS, 0);
            if (batchPtr == null || batchPtr.equals(MemorySegment.NULL)) {
                throw new SlateDbException(-1, "SlateDB returned a null SlateDbWriteBatch");
            }
            return batchPtr;
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
            setAddress(HANDLE_PTR, handleStruct, handlePtr);
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
            setAddress(HANDLE_PTR, handleStruct, handlePtr);
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
            setAddress(HANDLE_PTR, handleStruct, handlePtr);
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
            setAddress(HANDLE_PTR, handleStruct, handlePtr);
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
            setAddress(HANDLE_PTR, handleStruct, handlePtr);
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
            setAddress(HANDLE_PTR, handleStruct, handlePtr);
            MemorySegment valueOut = arena.allocate(VALUE_LAYOUT);
            MemorySegment result = (MemorySegment) metricsHandle.invokeExact(
                (SegmentAllocator) arena,
                handleStruct,
                valueOut
            );
            checkResult(result);
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
            setAddress(HANDLE_PTR, handleStruct, handlePtr);
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
            checkResult(result);
            if (getBoolean(RESULT_NONE, result)) {
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
            setAddress(HANDLE_PTR, handleStruct, handlePtr);
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
            setAddress(READER_HANDLE_PTR, handleStruct, readerHandle);
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
            checkResult(result);
            if (getBoolean(RESULT_NONE, result)) {
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
            setAddress(READER_HANDLE_PTR, handleStruct, readerHandle);
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
            setAddress(READER_HANDLE_PTR, handleStruct, readerHandle);
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
            setAddress(READER_HANDLE_PTR, handleStruct, readerHandle);
            MemorySegment result =
                (MemorySegment) readerCloseHandle.invokeExact((SegmentAllocator) arena, handleStruct);
            checkResult(result);
        } catch (Throwable t) {
            throw wrap(t);
        }
    }

    static SlateDbKeyValue iteratorNext(MemorySegment iterPtr) {
        ensureInitialized();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment kvOut = arena.allocate(KEY_VALUE_LAYOUT);
            MemorySegment result = (MemorySegment) iteratorNextHandle.invokeExact(
                (SegmentAllocator) arena,
                iterPtr,
                kvOut
            );
            checkResult(result);
            if (getBoolean(RESULT_NONE, result)) {
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
            setAddress(HANDLE_PTR, handleStruct, handlePtr);
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
                FunctionDescriptor.of(BUILDER_RESULT_LAYOUT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
            builderWithSettingsHandle = downcall(lookup, "slatedb_builder_with_settings",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
            builderWithSstBlockSizeHandle = downcall(lookup, "slatedb_builder_with_sst_block_size",
                FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.JAVA_BYTE));
            builderBuildHandle = downcall(lookup, "slatedb_builder_build",
                FunctionDescriptor.of(HANDLE_RESULT_LAYOUT, ValueLayout.ADDRESS));
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
        setInt(PUT_OPTIONS_TTL_TYPE, segment, options.ttlType().code());
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
        setInt(READ_OPTIONS_DURABILITY, segment, options.durabilityFilter().code());
        setBoolean(READ_OPTIONS_DIRTY, segment, options.dirty());
        setBoolean(READ_OPTIONS_CACHE_BLOCKS, segment, options.cacheBlocks());
        return segment;
    }

    private static MemorySegment toScanOptions(Arena arena, ScanOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment segment = arena.allocate(SCAN_OPTIONS_LAYOUT);
        setInt(SCAN_OPTIONS_DURABILITY, segment, options.durabilityFilter().code());
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

    private static String readMessage(MemorySegment messageSegment) {
        if (messageSegment == null || messageSegment.equals(MemorySegment.NULL)) {
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

    private static String readCStringAndFree(MemorySegment cstr, String errorMessage) {
        if (cstr == null || cstr.equals(MemorySegment.NULL)) {
            throw new IllegalStateException(errorMessage);
        }
        String value = readMessage(cstr);
        freeCString(cstr, null);
        return value;
    }

    private static byte[] copyValue(MemorySegment valueOut, RuntimeException pending) {
        long len = (long) getLong(VALUE_LEN, valueOut);
        MemorySegment data = (MemorySegment) getAddress(VALUE_DATA, valueOut);
        if (len < 0 || len > Integer.MAX_VALUE) {
            RuntimeException sizeError = new IllegalStateException("SlateDB value too large: " + len);
            freeValue(valueOut, sizeError);
            throw sizeError;
        }
        byte[] bytes = new byte[(int) len];
        if (len > 0) {
            MemorySegment dataSegment = data.reinterpret(len);
            MemorySegment.copy(dataSegment, 0, MemorySegment.ofArray(bytes), 0, len);
        }
        freeValue(valueOut, pending);
        return bytes;
    }

    private static SlateDbKeyValue copyKeyValuePair(MemorySegment kvOut) {
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
        return new SlateDbKeyValue(keyBytes, valueBytes);
    }

    /**
     * Checks whether given `CSdbResult` is successful or not. If result error code is not successful throws
     * {@link SlateDbException} exception.
     * @param result a `CSdbResult` result to check.
     */
    private static void checkResult(final MemorySegment result) {
        final var error = getInt(RESULT_ERROR, result);
        final var messageSegment = getAddress(RESULT_MESSAGE, result);
        final var message = readMessage(messageSegment);
        final RuntimeException failure = (error == 0) ? null : new SlateDbException(error, message);
        freeResult(result, failure);
        if (failure != null) {
            throw failure;
        }
    }

    private static void checkResultAllowInvalidArgument(MemorySegment result, String defaultInvalidMessage) {
        int error = (int) getInt(RESULT_ERROR, result);
        MemorySegment messageSegment = (MemorySegment) getAddress(RESULT_MESSAGE, result);
        String message = readMessage(messageSegment);
        RuntimeException failure = null;
        if (error != 0) {
            if (error == ERROR_INVALID_ARGUMENT) {
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
