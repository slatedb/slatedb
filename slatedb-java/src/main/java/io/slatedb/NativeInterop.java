package io.slatedb;

import io.slatedb.SlateDbConfig.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/// Shared FFI helpers used by the SlateDB Java wrappers.
///
/// Centralizes:
/// - result checking and exception mapping
/// - Rust-owned byte buffer extraction + release
/// - options/range marshaling to C ABI structs
final class NativeInterop {
    private static final int SLATEDB_ERROR_KIND_NONE = 0;
    private static final int SLATEDB_ERROR_KIND_CLOSED = 2;

    private static final int SLATEDB_CLOSE_REASON_NONE = 0;
    private static final int SLATEDB_CLOSE_REASON_CLEAN = 1;
    private static final int SLATEDB_CLOSE_REASON_FENCED = 2;
    private static final int SLATEDB_CLOSE_REASON_PANIC = 3;

    private NativeInterop() {
    }

    /// Validates a `slatedb_result_t`, frees its message payload, and throws on error.
    static void checkResult(MemorySegment result) {
        int kind = slatedb_result_t.kind(result);
        int closeReason = slatedb_result_t.close_reason(result);

        if (kind == SLATEDB_ERROR_KIND_NONE) {
            Native.slatedb_result_free(result);
            return;
        }

        String message;
        try {
            message = readOptionalCString(slatedb_result_t.message(result));
        } finally {
            Native.slatedb_result_free(result);
        }

        if (kind == SLATEDB_ERROR_KIND_CLOSED) {
            String closeReasonLabel = closeReasonLabel(closeReason);
            if (message == null || message.isBlank()) {
                message = "SlateDB is closed (" + closeReasonLabel + ")";
            } else {
                message = message + " (close reason: " + closeReasonLabel + ")";
            }
        }

        throw new SlateDb.SlateDbException(kind, message);
    }

    /// Copies an owned Rust buffer into a Java byte array and releases native memory.
    static byte[] copyAndFreeBytes(MemorySegment dataPtr, long len) {
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0");
        }

        if (dataPtr.equals(MemorySegment.NULL)) {
            if (len == 0) {
                return new byte[0];
            }
            throw new IllegalStateException("native byte pointer is null but len is " + len);
        }

        if (len == 0) {
            return new byte[0];
        }

        try {
            return dataPtr.reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
        } finally {
            Native.slatedb_bytes_free(dataPtr, len);
        }
    }

    /// Reads `uint8_t** + uintptr_t*` out parameters and returns a copied/free'd Java byte array.
    static byte[] takeOwnedBytes(MemorySegment outDataPtr, MemorySegment outLenPtr) {
        Objects.requireNonNull(outDataPtr, "outDataPtr");
        Objects.requireNonNull(outLenPtr, "outLenPtr");
        MemorySegment dataPtr = outDataPtr.get(Native.C_POINTER, 0);
        long len = outLenPtr.get(Native.C_LONG, 0);
        return copyAndFreeBytes(dataPtr, len);
    }

    /// Reads `uint8_t** + uintptr_t*` out parameters and returns `null` when `present == false`.
    static byte[] takeOwnedBytesIfPresent(boolean present, MemorySegment outDataPtr, MemorySegment outLenPtr) {
        Objects.requireNonNull(outDataPtr, "outDataPtr");
        Objects.requireNonNull(outLenPtr, "outLenPtr");

        MemorySegment dataPtr = outDataPtr.get(Native.C_POINTER, 0);
        long len = outLenPtr.get(Native.C_LONG, 0);

        if (!present) {
            if (!dataPtr.equals(MemorySegment.NULL) && len > 0) {
                Native.slatedb_bytes_free(dataPtr, len);
            }
            return null;
        }

        return copyAndFreeBytes(dataPtr, len);
    }

    /// Marshals an optional read options object to `slatedb_read_options_t*`.
    static MemorySegment marshalReadOptions(Arena arena, ReadOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_read_options_t.allocate(arena);
        slatedb_read_options_t.durability_filter(nativeOptions, toNativeDurability(options.durabilityFilter()));
        slatedb_read_options_t.dirty(nativeOptions, options.dirty());
        slatedb_read_options_t.cache_blocks(nativeOptions, options.cacheBlocks());
        return nativeOptions;
    }

    /// Marshals an optional scan options object to `slatedb_scan_options_t*`.
    static MemorySegment marshalScanOptions(Arena arena, ScanOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_scan_options_t.allocate(arena);
        slatedb_scan_options_t.durability_filter(nativeOptions, toNativeDurability(options.durabilityFilter()));
        slatedb_scan_options_t.dirty(nativeOptions, options.dirty());
        slatedb_scan_options_t.read_ahead_bytes(nativeOptions, options.readAheadBytes());
        slatedb_scan_options_t.cache_blocks(nativeOptions, options.cacheBlocks());
        slatedb_scan_options_t.max_fetch_tasks(nativeOptions, options.maxFetchTasks());
        return nativeOptions;
    }

    /// Marshals an optional write options object to `slatedb_write_options_t*`.
    static MemorySegment marshalWriteOptions(Arena arena, WriteOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_write_options_t.allocate(arena);
        slatedb_write_options_t.await_durable(nativeOptions, options.awaitDurable());
        return nativeOptions;
    }

    /// Marshals an optional put options object to `slatedb_put_options_t*`.
    static MemorySegment marshalPutOptions(Arena arena, PutOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_put_options_t.allocate(arena);
        slatedb_put_options_t.ttl_type(nativeOptions, toNativeTtl(options.ttlType()));
        slatedb_put_options_t.ttl_value(nativeOptions, options.ttlValueMs());
        return nativeOptions;
    }

    /// Marshals an optional reader options object to `slatedb_db_reader_options_t*`.
    static MemorySegment marshalReaderOptions(Arena arena, ReaderOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_db_reader_options_t.allocate(arena);
        slatedb_db_reader_options_t.manifest_poll_interval_ms(nativeOptions, options.manifestPollIntervalMs());
        slatedb_db_reader_options_t.checkpoint_lifetime_ms(nativeOptions, options.checkpointLifetimeMs());
        slatedb_db_reader_options_t.max_memtable_bytes(nativeOptions, options.maxMemtableBytes());
        slatedb_db_reader_options_t.skip_wal_replay(nativeOptions, options.skipWalReplay());
        return nativeOptions;
    }

    /// Marshals `[startKey, endKey)` to `slatedb_range_t`.
    static MemorySegment marshalRange(Arena arena, byte[] startKey, byte[] endKey) {
        MemorySegment range = slatedb_range_t.allocate(arena);

        MemorySegment start = slatedb_bound_t.allocate(arena);
        fillBound(arena, start, startKey, true);
        slatedb_range_t.start(range, start);

        MemorySegment end = slatedb_bound_t.allocate(arena);
        fillBound(arena, end, endKey, false);
        slatedb_range_t.end(range, end);

        return range;
    }

    /// Marshals UTF-8 bytes to an FFI pointer (`uint8_t*`), allowing null for empty arrays.
    static MemorySegment marshalBytes(Arena arena, byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length == 0) {
            return MemorySegment.NULL;
        }

        MemorySegment nativeBytes = arena.allocate(bytes.length, 1);
        MemorySegment.copy(MemorySegment.ofArray(bytes), 0, nativeBytes, 0, bytes.length);
        return nativeBytes;
    }

    /// Marshals an optional Java string to a nullable C string pointer.
    static MemorySegment marshalNullableCString(Arena arena, String value) {
        if (value == null) {
            return MemorySegment.NULL;
        }
        return marshalCString(arena, value);
    }

    /// Marshals a Java string to a null-terminated UTF-8 C string pointer.
    static MemorySegment marshalCString(Arena arena, String value) {
        Objects.requireNonNull(value, "value");
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        MemorySegment nativeString = arena.allocate(utf8.length + 1L, 1);
        if (utf8.length > 0) {
            MemorySegment.copy(MemorySegment.ofArray(utf8), 0, nativeString, 0, utf8.length);
        }
        nativeString.set(ValueLayout.JAVA_BYTE, utf8.length, (byte) 0);
        return nativeString;
    }

    private static void fillBound(Arena arena, MemorySegment bound, byte[] value, boolean inclusive) {
        if (value == null) {
            slatedb_bound_t.kind(bound, (byte) Native.SLATEDB_BOUND_KIND_UNBOUNDED());
            slatedb_bound_t.data(bound, MemorySegment.NULL);
            slatedb_bound_t.len(bound, 0L);
            return;
        }

        slatedb_bound_t.kind(
            bound,
            (byte) (inclusive ? Native.SLATEDB_BOUND_KIND_INCLUDED() : Native.SLATEDB_BOUND_KIND_EXCLUDED())
        );
        MemorySegment bytes = marshalBytes(arena, value);
        slatedb_bound_t.data(bound, bytes);
        slatedb_bound_t.len(bound, value.length);
    }

    private static byte toNativeDurability(Durability durability) {
        return switch (durability) {
            case MEMORY -> (byte) Native.SLATEDB_DURABILITY_FILTER_MEMORY();
            case REMOTE -> (byte) Native.SLATEDB_DURABILITY_FILTER_REMOTE();
        };
    }

    private static byte toNativeTtl(TtlType ttlType) {
        return switch (ttlType) {
            case DEFAULT -> (byte) Native.SLATEDB_TTL_TYPE_DEFAULT();
            case NO_EXPIRY -> (byte) Native.SLATEDB_TTL_TYPE_NO_EXPIRY();
            case EXPIRE_AFTER -> (byte) Native.SLATEDB_TTL_TYPE_EXPIRE_AFTER();
        };
    }

    private static String readOptionalCString(MemorySegment cStringPtr) {
        if (cStringPtr.equals(MemorySegment.NULL)) {
            return null;
        }
        return cStringPtr.getString(0);
    }

    private static String closeReasonLabel(int closeReason) {
        return switch (closeReason) {
            case SLATEDB_CLOSE_REASON_NONE -> "none";
            case SLATEDB_CLOSE_REASON_CLEAN -> "clean";
            case SLATEDB_CLOSE_REASON_FENCED -> "fenced";
            case SLATEDB_CLOSE_REASON_PANIC -> "panic";
            default -> "unknown(" + closeReason + ")";
        };
    }
}
