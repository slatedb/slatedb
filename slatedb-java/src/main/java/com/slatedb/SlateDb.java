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
     * Opens a SlateDB handle.
     */
    public static SlateDb open(String path, String url, String envFile) {
        return new SlateDb(Native.open(path, url, envFile));
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
     * Returns the value for the given key, or {@code null} if not found.
     */
    public byte[] get(byte[] key) {
        return Native.get(handle, key);
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

        private static final long HANDLE_RESULT_RESULT_OFFSET =
            HANDLE_RESULT_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("result"));

        private static volatile boolean initialized;
        private static MethodHandle initLoggingHandle;
        private static MethodHandle openHandle;
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
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment keySegment = toByteArray(arena, key);
                MemorySegment valueSegment = toByteArray(arena, value);
                MemorySegment result = (MemorySegment) putHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    keySegment,
                    (long) key.length,
                    valueSegment,
                    (long) value.length,
                    MemorySegment.NULL,
                    MemorySegment.NULL
                );
                checkResult(result);
            } catch (Throwable t) {
                throw wrap(t);
            }
        }

        static byte[] get(MemorySegment handlePtr, byte[] key) {
            Objects.requireNonNull(key, "key");
            ensureInitialized();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment handleStruct = arena.allocate(HANDLE_LAYOUT);
                HANDLE_PTR.set(handleStruct, handlePtr);
                MemorySegment keySegment = toByteArray(arena, key);
                MemorySegment valueOut = arena.allocate(VALUE_LAYOUT);
                MemorySegment result = (MemorySegment) getHandle.invokeExact(
                    (SegmentAllocator) arena,
                    handleStruct,
                    keySegment,
                    (long) key.length,
                    MemorySegment.NULL,
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
                freeValue(valueOut, null);
                return bytes;
            } catch (Throwable t) {
                throw wrap(t);
            }
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
                SymbolLookup lookup = SymbolLookup.loaderLookup();
                initLoggingHandle = downcall(lookup, "slatedb_init_logging",
                    FunctionDescriptor.of(RESULT_LAYOUT, ValueLayout.ADDRESS));
                openHandle = downcall(lookup, "slatedb_open",
                    FunctionDescriptor.of(HANDLE_RESULT_LAYOUT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
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

        private static RuntimeException wrap(Throwable t) {
            if (t instanceof RuntimeException) {
                return (RuntimeException) t;
            }
            return new RuntimeException(t);
        }
    }

    public static final class SlateDbException extends RuntimeException {
        private final int errorCode;

        public SlateDbException(int errorCode, String message) {
            super(message == null ? ("SlateDB error " + errorCode) : message);
            this.errorCode = errorCode;
        }

        public int getErrorCode() {
            return errorCode;
        }
    }

    public enum TtlType {
        DEFAULT(0),
        NO_EXPIRY(1),
        EXPIRE_AFTER(2);

        private final int code;

        TtlType(int code) {
            this.code = code;
        }
    }

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

        public static PutOptions defaultTtl() {
            return new PutOptions(TtlType.DEFAULT, 0);
        }

        public static PutOptions noExpiry() {
            return new PutOptions(TtlType.NO_EXPIRY, 0);
        }

        public static PutOptions expireAfter(Duration ttl) {
            Objects.requireNonNull(ttl, "ttl");
            return expireAfterMillis(ttl.toMillis());
        }

        public static PutOptions expireAfterMillis(long ttlMillis) {
            return new PutOptions(TtlType.EXPIRE_AFTER, ttlMillis);
        }
    }

    public static final class WriteOptions {
        public static final WriteOptions DEFAULT = new WriteOptions(true);

        private final boolean awaitDurable;

        public WriteOptions(boolean awaitDurable) {
            this.awaitDurable = awaitDurable;
        }
    }

    public static final class WriteBatch implements AutoCloseable {
        private MemorySegment batchPtr;
        private boolean closed;
        private boolean consumed;

        private WriteBatch(MemorySegment batchPtr) {
            this.batchPtr = batchPtr;
        }

        public void put(byte[] key, byte[] value) {
            ensureOpen();
            Native.writeBatchPut(batchPtr, key, value);
        }

        public void put(byte[] key, byte[] value, PutOptions options) {
            ensureOpen();
            Native.writeBatchPutWithOptions(batchPtr, key, value, options);
        }

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
