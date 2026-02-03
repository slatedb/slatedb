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

        private static final VarHandle HANDLE_PTR =
            HANDLE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("ptr"));
        private static final VarHandle RESULT_ERROR =
            RESULT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("error"));
        private static final VarHandle RESULT_MESSAGE =
            RESULT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("message"));
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
        private static MethodHandle closeHandle;
        private static MethodHandle freeResultHandle;

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
                closeHandle = downcall(lookup, "slatedb_close",
                    FunctionDescriptor.of(RESULT_LAYOUT, HANDLE_LAYOUT));
                freeResultHandle = downcall(lookup, "slatedb_free_result",
                    FunctionDescriptor.ofVoid(RESULT_LAYOUT));
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
}
