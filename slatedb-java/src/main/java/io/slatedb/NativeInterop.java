package io.slatedb;

import io.slatedb.SlateDbConfig.*;
import io.slatedb.ffi.*;
import io.slatedb.ffi.Native;
import io.slatedb.ffi.slatedb_row_entry_t;
import io.slatedb.ffi.slatedb_wal_file_metadata_t;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/// Java-typed wrappers around generated jextract bindings.
///
/// Each method mirrors a `slatedb_*` C function as closely as possible while
/// handling:
/// - `Arena` lifetime
/// - `slatedb_result_t` checking and error mapping
/// - Java handle lifecycle management to close native resources using AutoCloseable
/// - Standard Java types
/// - Rust-owned buffer cleanup (`slatedb_result_free`, `slatedb_bytes_free`)
final class NativeInterop {
    private NativeInterop() {
    }

    static abstract class NativeHandle implements AutoCloseable {
        private final String handleType;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private volatile MemorySegment segment;

        NativeHandle(String handleType, MemorySegment segment) {
            this.handleType = Objects.requireNonNull(handleType, "handleType");
            this.segment = requireNativeHandle(segment, handleType);
        }

        final MemorySegment segment() {
            MemorySegment current = segment;
            if (closed.get() || current.equals(MemorySegment.NULL)) {
                throw new IllegalStateException(handleType + " is closed");
            }
            return current;
        }

        final boolean isClosed() {
            return closed.get();
        }

        @Override
        public final void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }

            MemorySegment current = segment;
            try {
                closeNative(current);
            } finally {
                segment = MemorySegment.NULL;
            }
        }

        protected abstract void closeNative(MemorySegment segment);
    }

    static final class ObjectStoreHandle extends NativeHandle {
        private ObjectStoreHandle(MemorySegment segment) {
            super("ObjectStoreHandle", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.slatedb_object_store_close(arena, segment));
            }
        }
    }

    static final class SettingsHandle extends NativeHandle {
        private SettingsHandle(MemorySegment segment) {
            super("SettingsHandle", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.slatedb_settings_close(arena, segment));
            }
        }
    }

    static final class DbBuilderHandle extends NativeHandle {
        private DbBuilderHandle(MemorySegment segment) {
            super("DbBuilderHandle", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.slatedb_db_builder_close(arena, segment));
            }
        }
    }

    static final class DbHandle extends NativeHandle {
        private DbHandle(MemorySegment segment) {
            super("DbHandle", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.slatedb_db_close(arena, segment));
            }
        }
    }

    static final class ReaderHandle extends NativeHandle {
        private ReaderHandle(MemorySegment segment) {
            super("ReaderHandle", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.slatedb_db_reader_close(arena, segment));
            }
        }
    }

    static final class IteratorHandle extends NativeHandle {
        private IteratorHandle(MemorySegment segment) {
            super("IteratorHandle", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.slatedb_iterator_close(arena, segment));
            }
        }
    }

    static final class WriteBatchHandle extends NativeHandle {
        private WriteBatchHandle(MemorySegment segment) {
            super("WriteBatchHandle", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.slatedb_write_batch_close(arena, segment));
            }
        }
    }

    static final class WalReaderHandle extends NativeHandle {
        private WalReaderHandle(MemorySegment segment) {
            super("WalReaderHandle", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.slatedb_wal_reader_close(arena, segment));
            }
        }
    }

    static final class WalFileHandle extends NativeHandle {
        private WalFileHandle(MemorySegment segment) {
            super("WalFileHandle", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.slatedb_wal_file_close(arena, segment));
            }
        }
    }

    static final class WalFileIteratorHandle extends NativeHandle {
        private WalFileIteratorHandle(MemorySegment segment) {
            super("WalFileIteratorHandle", segment);
        }

        @Override
        protected void closeNative(MemorySegment segment) {
            try (Arena arena = Arena.ofConfined()) {
                checkResult(Native.slatedb_wal_file_iterator_close(arena, segment));
            }
        }
    }

    static final class WriteHandleHandle {
        private final long seq;
        private final long createTs;

        private WriteHandleHandle(long seq, long createTs) {
            this.seq = seq;
            this.createTs = createTs;
        }

        long seq() {
            return seq;
        }

        long createTs() {
            return createTs;
        }
    }

    static ObjectStoreHandle resolveObjectStore(String url, String envFile) {
        if (url != null) {
            return slatedb_object_store_from_url(url);
        }
        return slatedb_object_store_from_env(envFile);
    }

    static void slatedb_logging_init(byte level) {
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_logging_init(arena, level));
        }
    }

    static void slatedb_logging_init(LogLevel level) {
        Objects.requireNonNull(level, "level");
        slatedb_logging_init(level.code());
    }

    static void slatedb_logging_set_level(byte level) {
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_logging_set_level(arena, level));
        }
    }

    static void slatedb_logging_set_level(LogLevel level) {
        Objects.requireNonNull(level, "level");
        slatedb_logging_set_level(level.code());
    }

    static void slatedb_logging_set_callback(MemorySegment callback, MemorySegment context, MemorySegment freeContext) {
        Objects.requireNonNull(callback, "callback");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_logging_set_callback(arena, callback, nullToNullSegment(context), nullToNullSegment(freeContext)));
        }
    }

    static void slatedb_logging_clear_callback() {
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_logging_clear_callback(arena));
        }
    }

    static ObjectStoreHandle slatedb_object_store_from_url(String url) {
        Objects.requireNonNull(url, "url");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outObjectStore = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_object_store_from_url(arena, marshalCString(arena, url), outObjectStore));
            return new ObjectStoreHandle(outObjectStore.get(Native.C_POINTER, 0));
        }
    }

    static ObjectStoreHandle slatedb_object_store_from_env(String envFile) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outObjectStore = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_object_store_from_env(
                    arena,
                    marshalNullableCString(arena, envFile),
                    outObjectStore
                )
            );
            return new ObjectStoreHandle(outObjectStore.get(Native.C_POINTER, 0));
        }
    }

    static void slatedb_object_store_close(ObjectStoreHandle objectStore) {
        Objects.requireNonNull(objectStore, "objectStore");
        objectStore.close();
    }

    static SettingsHandle slatedb_settings_default() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_settings_default(arena, outSettings));
            return new SettingsHandle(outSettings.get(Native.C_POINTER, 0));
        }
    }

    static SettingsHandle slatedb_settings_from_file(String path) {
        Objects.requireNonNull(path, "path");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_settings_from_file(arena, marshalCString(arena, path), outSettings));
            return new SettingsHandle(outSettings.get(Native.C_POINTER, 0));
        }
    }

    static SettingsHandle slatedb_settings_from_json(String json) {
        Objects.requireNonNull(json, "json");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_settings_from_json(arena, marshalCString(arena, json), outSettings));
            return new SettingsHandle(outSettings.get(Native.C_POINTER, 0));
        }
    }

    static SettingsHandle slatedb_settings_from_env(String prefix) {
        Objects.requireNonNull(prefix, "prefix");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_settings_from_env(arena, marshalCString(arena, prefix), outSettings));
            return new SettingsHandle(outSettings.get(Native.C_POINTER, 0));
        }
    }

    static SettingsHandle slatedb_settings_from_env_with_default(String prefix, SettingsHandle defaultSettings) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(defaultSettings, "defaultSettings");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_settings_from_env_with_default(
                    arena,
                    marshalCString(arena, prefix),
                    defaultSettings.segment(),
                    outSettings
                )
            );
            return new SettingsHandle(outSettings.get(Native.C_POINTER, 0));
        }
    }

    static SettingsHandle slatedb_settings_load() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_settings_load(arena, outSettings));
            return new SettingsHandle(outSettings.get(Native.C_POINTER, 0));
        }
    }

    static void slatedb_settings_apply_kv(SettingsHandle settings, byte[] key, byte[] valueJson) {
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valueJson, "valueJson");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_settings_apply_kv(
                    arena,
                    settings.segment(),
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, valueJson),
                    valueJson.length
                )
            );
        }
    }

    static byte[] slatedb_settings_to_json(SettingsHandle settings) {
        Objects.requireNonNull(settings, "settings");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outJson = arena.allocate(Native.C_POINTER);
            MemorySegment outJsonLen = arena.allocate(Native.C_LONG);
            checkResult(Native.slatedb_settings_to_json(arena, settings.segment(), outJson, outJsonLen));
            return takeOwnedBytes(outJson, outJsonLen);
        }
    }

    static String slatedb_settings_to_json_string(SettingsHandle settings) {
        Objects.requireNonNull(settings, "settings");
        return new String(slatedb_settings_to_json(settings), StandardCharsets.UTF_8);
    }

    static void slatedb_settings_close(SettingsHandle settings) {
        Objects.requireNonNull(settings, "settings");
        settings.close();
    }

    static DbBuilderHandle slatedb_db_builder_new(String path, ObjectStoreHandle objectStore) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(objectStore, "objectStore");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outBuilder = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_db_builder_new(
                    arena,
                    marshalCString(arena, path),
                    objectStore.segment(),
                    outBuilder
                )
            );
            return new DbBuilderHandle(outBuilder.get(Native.C_POINTER, 0));
        }
    }

    static void slatedb_db_builder_with_wal_object_store(DbBuilderHandle builder, ObjectStoreHandle walObjectStore) {
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(walObjectStore, "walObjectStore");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_builder_with_wal_object_store(arena, builder.segment(), walObjectStore.segment()));
        }
    }

    static void slatedb_db_builder_with_seed(DbBuilderHandle builder, long seed) {
        Objects.requireNonNull(builder, "builder");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_builder_with_seed(arena, builder.segment(), seed));
        }
    }

    static void slatedb_db_builder_with_sst_block_size(DbBuilderHandle builder, SstBlockSize sstBlockSize) {
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(sstBlockSize, "sstBlockSize");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_builder_with_sst_block_size(arena, builder.segment(), sstBlockSize.code()));
        }
    }

    static void slatedb_db_builder_with_settings(DbBuilderHandle builder, SettingsHandle settings) {
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(settings, "settings");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_builder_with_settings(arena, builder.segment(), settings.segment()));
        }
    }

    static void slatedb_db_builder_with_merge_operator(
        DbBuilderHandle builder,
        MemorySegment mergeOperator,
        MemorySegment mergeOperatorContext,
        MemorySegment freeMergeResult,
        MemorySegment freeMergeOperatorContext
    ) {
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(mergeOperator, "mergeOperator");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_db_builder_with_merge_operator(
                    arena,
                    builder.segment(),
                    mergeOperator,
                    nullToNullSegment(mergeOperatorContext),
                    nullToNullSegment(freeMergeResult),
                    nullToNullSegment(freeMergeOperatorContext)
                )
            );
        }
    }

    static DbHandle slatedb_db_builder_build(DbBuilderHandle builder) {
        Objects.requireNonNull(builder, "builder");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outDb = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_db_builder_build(arena, builder.segment(), outDb));
            return new DbHandle(outDb.get(Native.C_POINTER, 0));
        }
    }

    static void slatedb_db_builder_close(DbBuilderHandle builder) {
        Objects.requireNonNull(builder, "builder");
        builder.close();
    }

    static DbHandle slatedb_db_open(String path, ObjectStoreHandle objectStore) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(objectStore, "objectStore");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outDb = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_db_open(
                    arena,
                    marshalCString(arena, path),
                    objectStore.segment(),
                    outDb
                )
            );
            return new DbHandle(outDb.get(Native.C_POINTER, 0));
        }
    }

    static void slatedb_db_status(DbHandle db) {
        Objects.requireNonNull(db, "db");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_status(arena, db.segment()));
        }
    }

    static byte[] slatedb_db_metrics(DbHandle db) {
        Objects.requireNonNull(db, "db");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outJson = arena.allocate(Native.C_POINTER);
            MemorySegment outJsonLen = arena.allocate(Native.C_LONG);
            checkResult(Native.slatedb_db_metrics(arena, db.segment(), outJson, outJsonLen));
            return takeOwnedBytes(outJson, outJsonLen);
        }
    }

    static String slatedb_db_metrics_string(DbHandle db) {
        Objects.requireNonNull(db, "db");
        return new String(slatedb_db_metrics(db), StandardCharsets.UTF_8);
    }

    static MetricGetResult slatedb_db_metric_get(DbHandle db, String name) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(name, "name");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPresent = arena.allocate(Native.C_BOOL);
            MemorySegment outValue = arena.allocate(Native.C_LONG_LONG);
            checkResult(
                Native.slatedb_db_metric_get(
                    arena,
                    db.segment(),
                    marshalCString(arena, name),
                    outPresent,
                    outValue
                )
            );
            return new MetricGetResult(
                outPresent.get(Native.C_BOOL, 0),
                outValue.get(Native.C_LONG_LONG, 0)
            );
        }
    }

    static byte[] slatedb_db_get(DbHandle db, byte[] key) {
        Objects.requireNonNull(db, "db");
        return slatedb_db_get_with_options(db, key, null);
    }

    static byte[] slatedb_db_get_with_options(DbHandle db, byte[] key, ReadOptions readOptions) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPresent = arena.allocate(Native.C_BOOL);
            MemorySegment outVal = arena.allocate(Native.C_POINTER);
            MemorySegment outValLen = arena.allocate(Native.C_LONG);

            checkResult(
                Native.slatedb_db_get_with_options(
                    arena,
                    db.segment(),
                    marshalBytes(arena, key),
                    key.length,
                    marshalReadOptions(arena, readOptions),
                    outPresent,
                    outVal,
                    outValLen
                )
            );

            boolean present = outPresent.get(Native.C_BOOL, 0);
            return takeOwnedBytesIfPresent(present, outVal, outValLen);
        }
    }

    static SlateDbWriteHandle slatedb_db_put(DbHandle db, byte[] key, byte[] value) {
        return slatedb_db_put_with_options(db, key, value, null, null);
    }

    static SlateDbWriteHandle slatedb_db_put_with_options(
        DbHandle db,
        byte[] key,
        byte[] value,
        PutOptions putOptions,
        WriteOptions writeOptions
    ) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outHandle = slatedb_write_handle_t.allocate(arena);
            checkResult(
                Native.slatedb_db_put_with_options(
                    arena,
                    db.segment(),
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length,
                    marshalPutOptions(arena, putOptions),
                    marshalWriteOptions(arena, writeOptions),
                    outHandle
                )
            );
            return readWriteHandle(outHandle);
        }
    }

    static SlateDbWriteHandle slatedb_db_delete(DbHandle db, byte[] key) {
        return slatedb_db_delete_with_options(db, key, null);
    }

    static SlateDbWriteHandle slatedb_db_delete_with_options(DbHandle db, byte[] key, WriteOptions writeOptions) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outHandle = slatedb_write_handle_t.allocate(arena);
            checkResult(
                Native.slatedb_db_delete_with_options(
                    arena,
                    db.segment(),
                    marshalBytes(arena, key),
                    key.length,
                    marshalWriteOptions(arena, writeOptions),
                    outHandle
                )
            );
            return readWriteHandle(outHandle);
        }
    }

    static SlateDbWriteHandle slatedb_db_merge(DbHandle db, byte[] key, byte[] value) {
        return slatedb_db_merge_with_options(db, key, value, null, null);
    }

    static SlateDbWriteHandle slatedb_db_merge_with_options(
        DbHandle db,
        byte[] key,
        byte[] value,
        PutOptions mergeOptions,
        WriteOptions writeOptions
    ) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outHandle = slatedb_write_handle_t.allocate(arena);
            checkResult(
                Native.slatedb_db_merge_with_options(
                    arena,
                    db.segment(),
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length,
                    marshalMergeOptions(arena, mergeOptions),
                    marshalWriteOptions(arena, writeOptions),
                    outHandle
                )
            );
            return readWriteHandle(outHandle);
        }
    }

    static SlateDbWriteHandle slatedb_db_write(DbHandle db, WriteBatchHandle writeBatch) {
        return slatedb_db_write_with_options(db, writeBatch, null);
    }

    static SlateDbWriteHandle slatedb_db_write_with_options(DbHandle db, WriteBatchHandle writeBatch, WriteOptions writeOptions) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(writeBatch, "writeBatch");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outHandle = slatedb_write_handle_t.allocate(arena);
            checkResult(
                Native.slatedb_db_write_with_options(
                    arena,
                    db.segment(),
                    writeBatch.segment(),
                    marshalWriteOptions(arena, writeOptions),
                    outHandle
                )
            );
            return readWriteHandle(outHandle);
        }
    }

    static IteratorHandle slatedb_db_scan(DbHandle db, byte[] startKey, byte[] endKey) {
        Objects.requireNonNull(db, "db");
        return slatedb_db_scan_with_options(db, startKey, endKey, null);
    }

    static IteratorHandle slatedb_db_scan_with_options(
        DbHandle db,
        byte[] startKey,
        byte[] endKey,
        ScanOptions scanOptions
    ) {
        Objects.requireNonNull(db, "db");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outIterator = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_db_scan_with_options(
                    arena,
                    db.segment(),
                    marshalRange(arena, startKey, endKey),
                    marshalScanOptions(arena, scanOptions),
                    outIterator
                )
            );
            return new IteratorHandle(outIterator.get(Native.C_POINTER, 0));
        }
    }

    static IteratorHandle slatedb_db_scan_prefix(DbHandle db, byte[] prefix) {
        Objects.requireNonNull(db, "db");
        return slatedb_db_scan_prefix_with_options(db, prefix, null);
    }

    static IteratorHandle slatedb_db_scan_prefix_with_options(DbHandle db, byte[] prefix, ScanOptions scanOptions) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(prefix, "prefix");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outIterator = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_db_scan_prefix_with_options(
                    arena,
                    db.segment(),
                    marshalBytes(arena, prefix),
                    prefix.length,
                    marshalScanOptions(arena, scanOptions),
                    outIterator
                )
            );
            return new IteratorHandle(outIterator.get(Native.C_POINTER, 0));
        }
    }

    private static SlateDbWriteHandle readWriteHandle(MemorySegment segment) {
        long seq = slatedb_write_handle_t.seq(segment);
        long createTs = slatedb_write_handle_t.create_ts(segment);
        return new SlateDbWriteHandle(new WriteHandleHandle(seq, createTs));
    }

    static void slatedb_db_flush(DbHandle db) {
        Objects.requireNonNull(db, "db");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_flush(arena, db.segment()));
        }
    }

    static void slatedb_db_flush_with_options(DbHandle db, FlushType flushType) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(flushType, "flushType");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_flush_with_options(arena, db.segment(), marshalFlushOptions(arena, flushType)));
        }
    }

    static void slatedb_db_close(DbHandle db) {
        Objects.requireNonNull(db, "db");
        db.close();
    }

    static ReaderHandle slatedb_db_reader_open(
        String path,
        ObjectStoreHandle objectStore,
        String checkpointId,
        ReaderOptions readerOptions
    ) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(objectStore, "objectStore");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outReader = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_db_reader_open(
                    arena,
                    marshalCString(arena, path),
                    objectStore.segment(),
                    marshalNullableCString(arena, checkpointId),
                    marshalReaderOptions(arena, readerOptions),
                    outReader
                )
            );
            return new ReaderHandle(outReader.get(Native.C_POINTER, 0));
        }
    }

    static byte[] slatedb_db_reader_get(ReaderHandle reader, byte[] key) {
        Objects.requireNonNull(reader, "reader");
        return slatedb_db_reader_get_with_options(reader, key, null);
    }

    static byte[] slatedb_db_reader_get_with_options(ReaderHandle reader, byte[] key, ReadOptions readOptions) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPresent = arena.allocate(Native.C_BOOL);
            MemorySegment outVal = arena.allocate(Native.C_POINTER);
            MemorySegment outValLen = arena.allocate(Native.C_LONG);

            checkResult(
                Native.slatedb_db_reader_get_with_options(
                    arena,
                    reader.segment(),
                    marshalBytes(arena, key),
                    key.length,
                    marshalReadOptions(arena, readOptions),
                    outPresent,
                    outVal,
                    outValLen
                )
            );

            boolean present = outPresent.get(Native.C_BOOL, 0);
            return takeOwnedBytesIfPresent(present, outVal, outValLen);
        }
    }

    static IteratorHandle slatedb_db_reader_scan(ReaderHandle reader, byte[] startKey, byte[] endKey) {
        Objects.requireNonNull(reader, "reader");
        return slatedb_db_reader_scan_with_options(reader, startKey, endKey, null);
    }

    static IteratorHandle slatedb_db_reader_scan_with_options(
        ReaderHandle reader,
        byte[] startKey,
        byte[] endKey,
        ScanOptions scanOptions
    ) {
        Objects.requireNonNull(reader, "reader");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outIterator = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_db_reader_scan_with_options(
                    arena,
                    reader.segment(),
                    marshalRange(arena, startKey, endKey),
                    marshalScanOptions(arena, scanOptions),
                    outIterator
                )
            );
            return new IteratorHandle(outIterator.get(Native.C_POINTER, 0));
        }
    }

    static IteratorHandle slatedb_db_reader_scan_prefix(ReaderHandle reader, byte[] prefix) {
        Objects.requireNonNull(reader, "reader");
        return slatedb_db_reader_scan_prefix_with_options(reader, prefix, null);
    }

    static IteratorHandle slatedb_db_reader_scan_prefix_with_options(
        ReaderHandle reader,
        byte[] prefix,
        ScanOptions scanOptions
    ) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(prefix, "prefix");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outIterator = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_db_reader_scan_prefix_with_options(
                    arena,
                    reader.segment(),
                    marshalBytes(arena, prefix),
                    prefix.length,
                    marshalScanOptions(arena, scanOptions),
                    outIterator
                )
            );
            return new IteratorHandle(outIterator.get(Native.C_POINTER, 0));
        }
    }

    static void slatedb_db_reader_close(ReaderHandle reader) {
        Objects.requireNonNull(reader, "reader");
        reader.close();
    }

    static IteratorNextResult slatedb_iterator_next(IteratorHandle iterator) {
        Objects.requireNonNull(iterator, "iterator");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPresent = arena.allocate(Native.C_BOOL);
            MemorySegment outKey = arena.allocate(Native.C_POINTER);
            MemorySegment outKeyLen = arena.allocate(Native.C_LONG);
            MemorySegment outVal = arena.allocate(Native.C_POINTER);
            MemorySegment outValLen = arena.allocate(Native.C_LONG);

            checkResult(
                Native.slatedb_iterator_next(
                    arena,
                    iterator.segment(),
                    outPresent,
                    outKey,
                    outKeyLen,
                    outVal,
                    outValLen
                )
            );

            boolean present = outPresent.get(Native.C_BOOL, 0);
            if (!present) {
                return new IteratorNextResult(false, null, null);
            }

            byte[] key = takeOwnedBytes(outKey, outKeyLen);
            byte[] value = takeOwnedBytes(outVal, outValLen);
            return new IteratorNextResult(true, key, value);
        }
    }

    static void slatedb_iterator_seek(IteratorHandle iterator, byte[] key) {
        Objects.requireNonNull(iterator, "iterator");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_iterator_seek(arena, iterator.segment(), marshalBytes(arena, key), key.length));
        }
    }

    static void slatedb_iterator_close(IteratorHandle iterator) {
        Objects.requireNonNull(iterator, "iterator");
        iterator.close();
    }

    static WriteBatchHandle slatedb_write_batch_new() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outWriteBatch = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_write_batch_new(arena, outWriteBatch));
            return new WriteBatchHandle(outWriteBatch.get(Native.C_POINTER, 0));
        }
    }

    static void slatedb_write_batch_put(WriteBatchHandle writeBatch, byte[] key, byte[] value) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_write_batch_put(
                    arena,
                    writeBatch.segment(),
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length
                )
            );
        }
    }

    static void slatedb_write_batch_put_with_options(
        WriteBatchHandle writeBatch,
        byte[] key,
        byte[] value,
        PutOptions putOptions
    ) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_write_batch_put_with_options(
                    arena,
                    writeBatch.segment(),
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length,
                    marshalPutOptions(arena, putOptions)
                )
            );
        }
    }

    static void slatedb_write_batch_merge(WriteBatchHandle writeBatch, byte[] key, byte[] value) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_write_batch_merge(
                    arena,
                    writeBatch.segment(),
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length
                )
            );
        }
    }

    static void slatedb_write_batch_merge_with_options(
        WriteBatchHandle writeBatch,
        byte[] key,
        byte[] value,
        PutOptions mergeOptions
    ) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_write_batch_merge_with_options(
                    arena,
                    writeBatch.segment(),
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length,
                    marshalMergeOptions(arena, mergeOptions)
                )
            );
        }
    }

    static void slatedb_write_batch_delete(WriteBatchHandle writeBatch, byte[] key) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_write_batch_delete(
                    arena,
                    writeBatch.segment(),
                    marshalBytes(arena, key),
                    key.length
                )
            );
        }
    }

    static void slatedb_write_batch_close(WriteBatchHandle writeBatch) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        writeBatch.close();
    }

    static void slatedb_result_free(MemorySegment result) {
        Objects.requireNonNull(result, "result");
        Native.slatedb_result_free(result);
    }

    static void slatedb_bytes_free(MemorySegment data, long len) {
        Objects.requireNonNull(data, "data");
        Native.slatedb_bytes_free(data, len);
    }

    static WalReaderHandle slatedb_wal_reader_new(String path, ObjectStoreHandle objectStore) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(objectStore, "objectStore");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outReader = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_wal_reader_new(
                    arena,
                    marshalCString(arena, path),
                    objectStore.segment(),
                    outReader
                )
            );
            return new WalReaderHandle(outReader.get(Native.C_POINTER, 0));
        }
    }

    static java.util.List<WalFileHandle> slatedb_wal_reader_list(
        WalReaderHandle reader,
        Long startId,
        Long endId
    ) {
        Objects.requireNonNull(reader, "reader");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outFiles = arena.allocate(Native.C_POINTER);
            MemorySegment outCount = arena.allocate(Native.C_LONG);

            checkResult(
                Native.slatedb_wal_reader_list(
                    arena,
                    reader.segment(),
                    marshalU64Range(arena, startId, endId),
                    outFiles,
                    outCount
                )
            );

            MemorySegment filesPtr = outFiles.get(Native.C_POINTER, 0);
            long count = outCount.get(Native.C_LONG, 0);

            if (filesPtr.equals(MemorySegment.NULL) || count == 0) {
                return java.util.List.of();
            }

            MemorySegment filesArray = filesPtr.reinterpret(count * Native.C_POINTER.byteSize());
            java.util.List<WalFileHandle> handles = new java.util.ArrayList<>((int) count);
            for (long i = 0; i < count; i++) {
                MemorySegment filePtr = filesArray.get(Native.C_POINTER, i * Native.C_POINTER.byteSize());
                handles.add(new WalFileHandle(filePtr));
            }
            Native.slatedb_wal_files_free(filesPtr, count);
            return handles;
        }
    }

    static WalFileHandle slatedb_wal_reader_get(WalReaderHandle reader, long id) {
        Objects.requireNonNull(reader, "reader");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outFile = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_wal_reader_get(arena, reader.segment(), id, outFile));
            return new WalFileHandle(outFile.get(Native.C_POINTER, 0));
        }
    }

    static void slatedb_wal_reader_close(WalReaderHandle reader) {
        Objects.requireNonNull(reader, "reader");
        reader.close();
    }

    static long slatedb_wal_file_id(WalFileHandle file) {
        Objects.requireNonNull(file, "file");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outId = arena.allocate(Native.C_LONG_LONG);
            checkResult(Native.slatedb_wal_file_id(arena, file.segment(), outId));
            return outId.get(Native.C_LONG_LONG, 0);
        }
    }

    static long slatedb_wal_file_next_id(WalFileHandle file) {
        Objects.requireNonNull(file, "file");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outId = arena.allocate(Native.C_LONG_LONG);
            checkResult(Native.slatedb_wal_file_next_id(arena, file.segment(), outId));
            return outId.get(Native.C_LONG_LONG, 0);
        }
    }

    static WalFileHandle slatedb_wal_file_next_file(WalFileHandle file) {
        Objects.requireNonNull(file, "file");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outFile = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_wal_file_next_file(arena, file.segment(), outFile));
            return new WalFileHandle(outFile.get(Native.C_POINTER, 0));
        }
    }

    static SlateDbWalFileMetadata slatedb_wal_file_metadata(WalFileHandle file) {
        Objects.requireNonNull(file, "file");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outMeta = slatedb_wal_file_metadata_t.allocate(arena);
            checkResult(Native.slatedb_wal_file_metadata(arena, file.segment(), outMeta));

            long secs = slatedb_wal_file_metadata_t.last_modified_secs(outMeta);
            int nanos = slatedb_wal_file_metadata_t.last_modified_nanos(outMeta);
            long sizeBytes = slatedb_wal_file_metadata_t.size_bytes(outMeta);
            MemorySegment locationPtr = slatedb_wal_file_metadata_t.location(outMeta);
            long locationLen = slatedb_wal_file_metadata_t.location_len(outMeta);

            String location = new String(
                locationPtr.reinterpret(locationLen).toArray(ValueLayout.JAVA_BYTE),
                java.nio.charset.StandardCharsets.UTF_8
            );

            Native.slatedb_wal_file_metadata_free(outMeta);

            return new SlateDbWalFileMetadata(
                java.time.Instant.ofEpochSecond(secs, Integer.toUnsignedLong(nanos)),
                sizeBytes,
                location
            );
        }
    }

    static WalFileIteratorHandle slatedb_wal_file_iterator(WalFileHandle file) {
        Objects.requireNonNull(file, "file");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outIter = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_wal_file_iterator(arena, file.segment(), outIter));
            return new WalFileIteratorHandle(outIter.get(Native.C_POINTER, 0));
        }
    }

    static void slatedb_wal_file_close(WalFileHandle file) {
        Objects.requireNonNull(file, "file");
        file.close();
    }

    static WalIteratorNextResult slatedb_wal_file_iterator_next(WalFileIteratorHandle iter) {
        Objects.requireNonNull(iter, "iter");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPresent = arena.allocate(Native.C_BOOL);
            MemorySegment outEntry = slatedb_row_entry_t.allocate(arena);

            checkResult(
                Native.slatedb_wal_file_iterator_next(arena, iter.segment(), outPresent, outEntry)
            );

            boolean present = outPresent.get(Native.C_BOOL, 0);
            if (!present) {
                return new WalIteratorNextResult(false, (byte) 0, null, null, 0, null, null);
            }

            byte kind = slatedb_row_entry_t.kind(outEntry);
            MemorySegment keyPtr = slatedb_row_entry_t.key(outEntry);
            long keyLen = slatedb_row_entry_t.key_len(outEntry);
            MemorySegment valuePtr = slatedb_row_entry_t.value(outEntry);
            long valueLen = slatedb_row_entry_t.value_len(outEntry);
            long seq = slatedb_row_entry_t.seq(outEntry);
            boolean hasCreateTs = slatedb_row_entry_t.create_ts_present(outEntry);
            long createTsRaw = slatedb_row_entry_t.create_ts(outEntry);
            boolean hasExpireTs = slatedb_row_entry_t.expire_ts_present(outEntry);
            long expireTsRaw = slatedb_row_entry_t.expire_ts(outEntry);

            byte[] key = keyPtr.reinterpret(keyLen).toArray(ValueLayout.JAVA_BYTE);
            byte[] value = valuePtr.equals(MemorySegment.NULL)
                ? null
                : valuePtr.reinterpret(valueLen).toArray(ValueLayout.JAVA_BYTE);

            Native.slatedb_row_entry_free(outEntry);

            return new WalIteratorNextResult(
                true,
                kind,
                key,
                value,
                seq,
                hasCreateTs ? Optional.of(createTsRaw) : Optional.empty(),
                hasExpireTs ? Optional.of(expireTsRaw) : Optional.empty()
            );
        }
    }

    static void slatedb_wal_file_iterator_close(WalFileIteratorHandle iter) {
        Objects.requireNonNull(iter, "iter");
        iter.close();
    }

    record WalIteratorNextResult(
        boolean present,
        byte kind,
        byte[] key,
        byte[] value,
        long seq,
        Optional<Long> createTs,
        Optional<Long> expireTs
    ) {
    }

    private static MemorySegment marshalU64Range(Arena arena, Long startId, Long endId) {
        MemorySegment range = slatedb_range_t.allocate(arena);

        MemorySegment start = slatedb_bound_t.allocate(arena);
        fillU64Bound(arena, start, startId, true);
        slatedb_range_t.start(range, start);

        MemorySegment end = slatedb_bound_t.allocate(arena);
        fillU64Bound(arena, end, endId, false);
        slatedb_range_t.end(range, end);

        return range;
    }

    private static void fillU64Bound(Arena arena, MemorySegment bound, Long value, boolean inclusive) {
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
        MemorySegment val = arena.allocate(ValueLayout.JAVA_LONG);
        val.set(ValueLayout.JAVA_LONG, 0, value);
        slatedb_bound_t.data(bound, val);
        slatedb_bound_t.len(bound, 0L);
    }

    static final class MetricGetResult {
        private final boolean present;
        private final long value;

        MetricGetResult(boolean present, long value) {
            this.present = present;
            this.value = value;
        }

        boolean present() {
            return present;
        }

        long value() {
            return value;
        }
    }

    static final class IteratorNextResult {
        private final boolean present;
        private final byte[] key;
        private final byte[] value;

        IteratorNextResult(boolean present, byte[] key, byte[] value) {
            this.present = present;
            this.key = key;
            this.value = value;
        }

        boolean present() {
            return present;
        }

        byte[] key() {
            return key;
        }

        byte[] value() {
            return value;
        }
    }

    private static MemorySegment requireNativeHandle(MemorySegment segment, String handleType) {
        Objects.requireNonNull(segment, handleType + " segment");
        if (segment.equals(MemorySegment.NULL)) {
            throw new IllegalArgumentException(handleType + " segment must not be NULL");
        }
        return segment;
    }

    private static void checkResult(MemorySegment result) {
        int kindCode = slatedb_result_t.kind(result);
        int closeReasonCode = slatedb_result_t.close_reason(result);
        ErrorKind kind = ErrorKind.fromCode(kindCode);
        CloseReason closeReason = CloseReason.fromCode(closeReasonCode);

        if (kind == ErrorKind.NONE) {
            Native.slatedb_result_free(result);
            return;
        }

        String message;
        try {
            message = readOptionalCString(slatedb_result_t.message(result));
        } finally {
            Native.slatedb_result_free(result);
        }

        if (kind == ErrorKind.CLOSED) {
            String closeReasonLabel = closeReasonLabel(closeReason, closeReasonCode);
            if (message == null || message.isBlank()) {
                message = "SlateDB is closed (" + closeReasonLabel + ")";
            } else {
                message = message + " (close reason: " + closeReasonLabel + ")";
            }
        }

        throw toException(kind, closeReason, closeReasonCode, message);
    }

    private static SlateDbException toException(
        ErrorKind kind,
        CloseReason closeReason,
        int closeReasonCode,
        String message
    ) {
        return switch (kind) {
            case TRANSACTION -> new SlateDbException.TransactionException(message);
            case CLOSED -> new SlateDbException.ClosedException(
                closeReasonLabel(closeReason, closeReasonCode),
                closeReasonCode,
                message
            );
            case UNAVAILABLE -> new SlateDbException.UnavailableException(message);
            case INVALID -> new SlateDbException.InvalidException(message);
            case DATA -> new SlateDbException.DataException(message);
            case INTERNAL -> new SlateDbException.InternalException(message);
            case NONE, UNKNOWN -> new SlateDbException.UnknownException(message);
        };
    }

    private static byte[] copyAndFreeBytes(MemorySegment dataPtr, long len) {
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

    private static byte[] takeOwnedBytes(MemorySegment outDataPtr, MemorySegment outLenPtr) {
        Objects.requireNonNull(outDataPtr, "outDataPtr");
        Objects.requireNonNull(outLenPtr, "outLenPtr");
        MemorySegment dataPtr = outDataPtr.get(Native.C_POINTER, 0);
        long len = outLenPtr.get(Native.C_LONG, 0);
        return copyAndFreeBytes(dataPtr, len);
    }

    private static byte[] takeOwnedBytesIfPresent(boolean present, MemorySegment outDataPtr, MemorySegment outLenPtr) {
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

    private static MemorySegment marshalReadOptions(Arena arena, ReadOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_read_options_t.allocate(arena);
        slatedb_read_options_t.durability_filter(nativeOptions, options.durabilityFilter().code());
        slatedb_read_options_t.dirty(nativeOptions, options.dirty());
        slatedb_read_options_t.cache_blocks(nativeOptions, options.cacheBlocks());
        return nativeOptions;
    }

    private static MemorySegment marshalScanOptions(Arena arena, ScanOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_scan_options_t.allocate(arena);
        slatedb_scan_options_t.durability_filter(nativeOptions, options.durabilityFilter().code());
        slatedb_scan_options_t.dirty(nativeOptions, options.dirty());
        slatedb_scan_options_t.read_ahead_bytes(nativeOptions, options.readAheadBytes());
        slatedb_scan_options_t.cache_blocks(nativeOptions, options.cacheBlocks());
        slatedb_scan_options_t.max_fetch_tasks(nativeOptions, options.maxFetchTasks());
        return nativeOptions;
    }

    private static MemorySegment marshalWriteOptions(Arena arena, WriteOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_write_options_t.allocate(arena);
        slatedb_write_options_t.await_durable(nativeOptions, options.awaitDurable());
        return nativeOptions;
    }

    private static MemorySegment marshalPutOptions(Arena arena, PutOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_put_options_t.allocate(arena);
        slatedb_put_options_t.ttl_type(nativeOptions, options.ttlType().code());
        slatedb_put_options_t.ttl_value(nativeOptions, options.ttlValueMs());
        return nativeOptions;
    }

    private static MemorySegment marshalMergeOptions(Arena arena, PutOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_merge_options_t.allocate(arena);
        slatedb_merge_options_t.ttl_type(nativeOptions, options.ttlType().code());
        slatedb_merge_options_t.ttl_value(nativeOptions, options.ttlValueMs());
        return nativeOptions;
    }

    private static MemorySegment marshalFlushOptions(Arena arena, FlushType flushType) {
        MemorySegment nativeOptions = slatedb_flush_options_t.allocate(arena);
        slatedb_flush_options_t.flush_type(nativeOptions, flushType.code());
        return nativeOptions;
    }

    private static MemorySegment marshalReaderOptions(Arena arena, ReaderOptions options) {
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

    private static MemorySegment marshalRange(Arena arena, byte[] startKey, byte[] endKey) {
        MemorySegment range = slatedb_range_t.allocate(arena);

        MemorySegment start = slatedb_bound_t.allocate(arena);
        fillBound(arena, start, startKey, true);
        slatedb_range_t.start(range, start);

        MemorySegment end = slatedb_bound_t.allocate(arena);
        fillBound(arena, end, endKey, false);
        slatedb_range_t.end(range, end);

        return range;
    }

    private static MemorySegment marshalBytes(Arena arena, byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length == 0) {
            return MemorySegment.NULL;
        }

        MemorySegment nativeBytes = arena.allocate(bytes.length, 1);
        MemorySegment.copy(MemorySegment.ofArray(bytes), 0, nativeBytes, 0, bytes.length);
        return nativeBytes;
    }

    private static MemorySegment marshalNullableCString(Arena arena, String value) {
        if (value == null) {
            return MemorySegment.NULL;
        }
        return marshalCString(arena, value);
    }

    private static MemorySegment marshalCString(Arena arena, String value) {
        Objects.requireNonNull(value, "value");
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        MemorySegment nativeString = arena.allocate(utf8.length + 1L, 1);
        if (utf8.length > 0) {
            MemorySegment.copy(MemorySegment.ofArray(utf8), 0, nativeString, 0, utf8.length);
        }
        nativeString.set(ValueLayout.JAVA_BYTE, utf8.length, (byte) 0);
        return nativeString;
    }

    private static MemorySegment nullToNullSegment(MemorySegment segment) {
        return segment == null ? MemorySegment.NULL : segment;
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

    private static String readOptionalCString(MemorySegment cStringPtr) {
        if (cStringPtr.equals(MemorySegment.NULL)) {
            return null;
        }
        return cStringPtr.getString(0);
    }

    private static String closeReasonLabel(
        CloseReason closeReason,
        int closeReasonCode
    ) {
        return switch (closeReason) {
            case NONE -> "none";
            case CLEAN -> "clean";
            case FENCED -> "fenced";
            case PANIC -> "panic";
            case UNKNOWN -> "unknown(" + closeReasonCode + ")";
        };
    }

    private enum ErrorKind {
        NONE(0),
        TRANSACTION(1),
        CLOSED(2),
        UNAVAILABLE(3),
        INVALID(4),
        DATA(5),
        INTERNAL(6),
        UNKNOWN(255);

        private final int code;

        ErrorKind(int code) {
            this.code = code;
        }

        static ErrorKind fromCode(int code) {
            for (ErrorKind value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            return UNKNOWN;
        }
    }

    private enum CloseReason {
        NONE(0),
        CLEAN(1),
        FENCED(2),
        PANIC(3),
        UNKNOWN(255);

        private final int code;

        CloseReason(int code) {
            this.code = code;
        }

        static CloseReason fromCode(int code) {
            for (CloseReason value : values()) {
                if (value.code == code) {
                    return value;
                }
            }
            return UNKNOWN;
        }
    }
}
