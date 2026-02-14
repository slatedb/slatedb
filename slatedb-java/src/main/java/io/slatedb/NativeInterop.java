package io.slatedb;

import io.slatedb.SlateDbConfig.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/// Java-typed wrappers around generated jextract bindings in [Native].
///
/// Each method mirrors a `slatedb_*` C function as closely as possible while
/// handling:
/// - `Arena` lifetime
/// - `slatedb_result_t` checking and error mapping
/// - Rust-owned buffer cleanup (`slatedb_result_free`, `slatedb_bytes_free`)
final class NativeInterop {
    private static final int SLATEDB_ERROR_KIND_NONE = 0;
    private static final int SLATEDB_ERROR_KIND_CLOSED = 2;

    private static final int SLATEDB_CLOSE_REASON_NONE = 0;
    private static final int SLATEDB_CLOSE_REASON_CLEAN = 1;
    private static final int SLATEDB_CLOSE_REASON_FENCED = 2;
    private static final int SLATEDB_CLOSE_REASON_PANIC = 3;

    private NativeInterop() {
    }

    static void loadLibrary() {
        System.loadLibrary("slatedb_c");
    }

    static void loadLibrary(String absolutePath) {
        Objects.requireNonNull(absolutePath, "absolutePath");
        System.load(absolutePath);
    }

    static MemorySegment resolveObjectStore(String url, String envFile) {
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

    static void slatedb_logging_set_level(byte level) {
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_logging_set_level(arena, level));
        }
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

    static MemorySegment slatedb_object_store_from_url(String url) {
        Objects.requireNonNull(url, "url");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outObjectStore = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_object_store_from_url(arena, marshalCString(arena, url), outObjectStore));
            return outObjectStore.get(Native.C_POINTER, 0);
        }
    }

    static MemorySegment slatedb_object_store_from_env(String envFile) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outObjectStore = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_object_store_from_env(
                    arena,
                    marshalNullableCString(arena, envFile),
                    outObjectStore
                )
            );
            return outObjectStore.get(Native.C_POINTER, 0);
        }
    }

    static void slatedb_object_store_close(MemorySegment objectStore) {
        Objects.requireNonNull(objectStore, "objectStore");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_object_store_close(arena, objectStore));
        }
    }

    static MemorySegment slatedb_settings_default() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_settings_default(arena, outSettings));
            return outSettings.get(Native.C_POINTER, 0);
        }
    }

    static MemorySegment slatedb_settings_from_file(String path) {
        Objects.requireNonNull(path, "path");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_settings_from_file(arena, marshalCString(arena, path), outSettings));
            return outSettings.get(Native.C_POINTER, 0);
        }
    }

    static MemorySegment slatedb_settings_from_json(String json) {
        Objects.requireNonNull(json, "json");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_settings_from_json(arena, marshalCString(arena, json), outSettings));
            return outSettings.get(Native.C_POINTER, 0);
        }
    }

    static MemorySegment slatedb_settings_from_env(String prefix) {
        Objects.requireNonNull(prefix, "prefix");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_settings_from_env(arena, marshalCString(arena, prefix), outSettings));
            return outSettings.get(Native.C_POINTER, 0);
        }
    }

    static MemorySegment slatedb_settings_from_env_with_default(String prefix, MemorySegment defaultSettings) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(defaultSettings, "defaultSettings");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_settings_from_env_with_default(
                    arena,
                    marshalCString(arena, prefix),
                    defaultSettings,
                    outSettings
                )
            );
            return outSettings.get(Native.C_POINTER, 0);
        }
    }

    static MemorySegment slatedb_settings_load() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outSettings = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_settings_load(arena, outSettings));
            return outSettings.get(Native.C_POINTER, 0);
        }
    }

    static void slatedb_settings_apply_kv(MemorySegment settings, byte[] key, byte[] valueJson) {
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(valueJson, "valueJson");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_settings_apply_kv(
                    arena,
                    settings,
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, valueJson),
                    valueJson.length
                )
            );
        }
    }

    static byte[] slatedb_settings_to_json(MemorySegment settings) {
        Objects.requireNonNull(settings, "settings");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outJson = arena.allocate(Native.C_POINTER);
            MemorySegment outJsonLen = arena.allocate(Native.C_LONG);
            checkResult(Native.slatedb_settings_to_json(arena, settings, outJson, outJsonLen));
            return takeOwnedBytes(outJson, outJsonLen);
        }
    }

    static String slatedb_settings_to_json_string(MemorySegment settings) {
        return new String(slatedb_settings_to_json(settings), StandardCharsets.UTF_8);
    }

    static void slatedb_settings_close(MemorySegment settings) {
        Objects.requireNonNull(settings, "settings");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_settings_close(arena, settings));
        }
    }

    static MemorySegment slatedb_db_builder_new(String path, MemorySegment objectStore) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(objectStore, "objectStore");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outBuilder = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_db_builder_new(
                    arena,
                    marshalCString(arena, path),
                    objectStore,
                    outBuilder
                )
            );
            return outBuilder.get(Native.C_POINTER, 0);
        }
    }

    static void slatedb_db_builder_with_wal_object_store(MemorySegment builder, MemorySegment walObjectStore) {
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(walObjectStore, "walObjectStore");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_builder_with_wal_object_store(arena, builder, walObjectStore));
        }
    }

    static void slatedb_db_builder_with_seed(MemorySegment builder, long seed) {
        Objects.requireNonNull(builder, "builder");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_builder_with_seed(arena, builder, seed));
        }
    }

    static void slatedb_db_builder_with_sst_block_size(MemorySegment builder, byte sstBlockSize) {
        Objects.requireNonNull(builder, "builder");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_builder_with_sst_block_size(arena, builder, sstBlockSize));
        }
    }

    static void slatedb_db_builder_with_settings(MemorySegment builder, MemorySegment settings) {
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(settings, "settings");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_builder_with_settings(arena, builder, settings));
        }
    }

    static void slatedb_db_builder_with_merge_operator(
        MemorySegment builder,
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
                    builder,
                    mergeOperator,
                    nullToNullSegment(mergeOperatorContext),
                    nullToNullSegment(freeMergeResult),
                    nullToNullSegment(freeMergeOperatorContext)
                )
            );
        }
    }

    static MemorySegment slatedb_db_builder_build(MemorySegment builder) {
        Objects.requireNonNull(builder, "builder");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outDb = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_db_builder_build(arena, builder, outDb));
            return outDb.get(Native.C_POINTER, 0);
        }
    }

    static void slatedb_db_builder_close(MemorySegment builder) {
        Objects.requireNonNull(builder, "builder");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_builder_close(arena, builder));
        }
    }

    static MemorySegment slatedb_db_open(String path, MemorySegment objectStore) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(objectStore, "objectStore");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outDb = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_db_open(
                    arena,
                    marshalCString(arena, path),
                    objectStore,
                    outDb
                )
            );
            return outDb.get(Native.C_POINTER, 0);
        }
    }

    static void slatedb_db_status(MemorySegment db) {
        Objects.requireNonNull(db, "db");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_status(arena, db));
        }
    }

    static byte[] slatedb_db_metrics(MemorySegment db) {
        Objects.requireNonNull(db, "db");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outJson = arena.allocate(Native.C_POINTER);
            MemorySegment outJsonLen = arena.allocate(Native.C_LONG);
            checkResult(Native.slatedb_db_metrics(arena, db, outJson, outJsonLen));
            return takeOwnedBytes(outJson, outJsonLen);
        }
    }

    static String slatedb_db_metrics_string(MemorySegment db) {
        return new String(slatedb_db_metrics(db), StandardCharsets.UTF_8);
    }

    static MetricGetResult slatedb_db_metric_get(MemorySegment db, String name) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(name, "name");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPresent = arena.allocate(Native.C_BOOL);
            MemorySegment outValue = arena.allocate(Native.C_LONG_LONG);
            checkResult(
                Native.slatedb_db_metric_get(
                    arena,
                    db,
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

    static byte[] slatedb_db_get(MemorySegment db, byte[] key) {
        return slatedb_db_get_with_options(db, key, null);
    }

    static byte[] slatedb_db_get_with_options(MemorySegment db, byte[] key, ReadOptions readOptions) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPresent = arena.allocate(Native.C_BOOL);
            MemorySegment outVal = arena.allocate(Native.C_POINTER);
            MemorySegment outValLen = arena.allocate(Native.C_LONG);

            checkResult(
                Native.slatedb_db_get_with_options(
                    arena,
                    db,
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

    static void slatedb_db_put(MemorySegment db, byte[] key, byte[] value) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_db_put(
                    arena,
                    db,
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length
                )
            );
        }
    }

    static void slatedb_db_put_with_options(
        MemorySegment db,
        byte[] key,
        byte[] value,
        PutOptions putOptions,
        WriteOptions writeOptions
    ) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_db_put_with_options(
                    arena,
                    db,
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length,
                    marshalPutOptions(arena, putOptions),
                    marshalWriteOptions(arena, writeOptions)
                )
            );
        }
    }

    static void slatedb_db_delete(MemorySegment db, byte[] key) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_delete(arena, db, marshalBytes(arena, key), key.length));
        }
    }

    static void slatedb_db_delete_with_options(MemorySegment db, byte[] key, WriteOptions writeOptions) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_db_delete_with_options(
                    arena,
                    db,
                    marshalBytes(arena, key),
                    key.length,
                    marshalWriteOptions(arena, writeOptions)
                )
            );
        }
    }

    static void slatedb_db_merge(MemorySegment db, byte[] key, byte[] value) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_db_merge(
                    arena,
                    db,
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length
                )
            );
        }
    }

    static void slatedb_db_merge_with_options(
        MemorySegment db,
        byte[] key,
        byte[] value,
        PutOptions mergeOptions,
        WriteOptions writeOptions
    ) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_db_merge_with_options(
                    arena,
                    db,
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length,
                    marshalMergeOptions(arena, mergeOptions),
                    marshalWriteOptions(arena, writeOptions)
                )
            );
        }
    }

    static void slatedb_db_write(MemorySegment db, MemorySegment writeBatch) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(writeBatch, "writeBatch");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_write(arena, db, writeBatch));
        }
    }

    static void slatedb_db_write_with_options(MemorySegment db, MemorySegment writeBatch, WriteOptions writeOptions) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(writeBatch, "writeBatch");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_db_write_with_options(
                    arena,
                    db,
                    writeBatch,
                    marshalWriteOptions(arena, writeOptions)
                )
            );
        }
    }

    static MemorySegment slatedb_db_scan(MemorySegment db, byte[] startKey, byte[] endKey) {
        return slatedb_db_scan_with_options(db, startKey, endKey, null);
    }

    static MemorySegment slatedb_db_scan_with_options(
        MemorySegment db,
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
                    db,
                    marshalRange(arena, startKey, endKey),
                    marshalScanOptions(arena, scanOptions),
                    outIterator
                )
            );
            return outIterator.get(Native.C_POINTER, 0);
        }
    }

    static MemorySegment slatedb_db_scan_prefix(MemorySegment db, byte[] prefix) {
        return slatedb_db_scan_prefix_with_options(db, prefix, null);
    }

    static MemorySegment slatedb_db_scan_prefix_with_options(MemorySegment db, byte[] prefix, ScanOptions scanOptions) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(prefix, "prefix");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outIterator = arena.allocate(Native.C_POINTER);
            checkResult(
                Native.slatedb_db_scan_prefix_with_options(
                    arena,
                    db,
                    marshalBytes(arena, prefix),
                    prefix.length,
                    marshalScanOptions(arena, scanOptions),
                    outIterator
                )
            );
            return outIterator.get(Native.C_POINTER, 0);
        }
    }

    static void slatedb_db_flush(MemorySegment db) {
        Objects.requireNonNull(db, "db");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_flush(arena, db));
        }
    }

    static void slatedb_db_flush_with_options(MemorySegment db, byte flushType) {
        Objects.requireNonNull(db, "db");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_flush_with_options(arena, db, marshalFlushOptions(arena, flushType)));
        }
    }

    static void slatedb_db_close(MemorySegment db) {
        Objects.requireNonNull(db, "db");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_close(arena, db));
        }
    }

    static MemorySegment slatedb_db_reader_open(
        String path,
        MemorySegment objectStore,
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
                    objectStore,
                    marshalNullableCString(arena, checkpointId),
                    marshalReaderOptions(arena, readerOptions),
                    outReader
                )
            );
            return outReader.get(Native.C_POINTER, 0);
        }
    }

    static byte[] slatedb_db_reader_get(MemorySegment reader, byte[] key) {
        return slatedb_db_reader_get_with_options(reader, key, null);
    }

    static byte[] slatedb_db_reader_get_with_options(MemorySegment reader, byte[] key, ReadOptions readOptions) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outPresent = arena.allocate(Native.C_BOOL);
            MemorySegment outVal = arena.allocate(Native.C_POINTER);
            MemorySegment outValLen = arena.allocate(Native.C_LONG);

            checkResult(
                Native.slatedb_db_reader_get_with_options(
                    arena,
                    reader,
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

    static MemorySegment slatedb_db_reader_scan(MemorySegment reader, byte[] startKey, byte[] endKey) {
        return slatedb_db_reader_scan_with_options(reader, startKey, endKey, null);
    }

    static MemorySegment slatedb_db_reader_scan_with_options(
        MemorySegment reader,
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
                    reader,
                    marshalRange(arena, startKey, endKey),
                    marshalScanOptions(arena, scanOptions),
                    outIterator
                )
            );
            return outIterator.get(Native.C_POINTER, 0);
        }
    }

    static MemorySegment slatedb_db_reader_scan_prefix(MemorySegment reader, byte[] prefix) {
        return slatedb_db_reader_scan_prefix_with_options(reader, prefix, null);
    }

    static MemorySegment slatedb_db_reader_scan_prefix_with_options(
        MemorySegment reader,
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
                    reader,
                    marshalBytes(arena, prefix),
                    prefix.length,
                    marshalScanOptions(arena, scanOptions),
                    outIterator
                )
            );
            return outIterator.get(Native.C_POINTER, 0);
        }
    }

    static void slatedb_db_reader_close(MemorySegment reader) {
        Objects.requireNonNull(reader, "reader");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_db_reader_close(arena, reader));
        }
    }

    static IteratorNextResult slatedb_iterator_next(MemorySegment iterator) {
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
                    iterator,
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

    static void slatedb_iterator_seek(MemorySegment iterator, byte[] key) {
        Objects.requireNonNull(iterator, "iterator");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_iterator_seek(arena, iterator, marshalBytes(arena, key), key.length));
        }
    }

    static void slatedb_iterator_close(MemorySegment iterator) {
        Objects.requireNonNull(iterator, "iterator");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_iterator_close(arena, iterator));
        }
    }

    static MemorySegment slatedb_write_batch_new() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outWriteBatch = arena.allocate(Native.C_POINTER);
            checkResult(Native.slatedb_write_batch_new(arena, outWriteBatch));
            return outWriteBatch.get(Native.C_POINTER, 0);
        }
    }

    static void slatedb_write_batch_put(MemorySegment writeBatch, byte[] key, byte[] value) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_write_batch_put(
                    arena,
                    writeBatch,
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length
                )
            );
        }
    }

    static void slatedb_write_batch_put_with_options(MemorySegment writeBatch, byte[] key, byte[] value, PutOptions putOptions) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_write_batch_put_with_options(
                    arena,
                    writeBatch,
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length,
                    marshalPutOptions(arena, putOptions)
                )
            );
        }
    }

    static void slatedb_write_batch_merge(MemorySegment writeBatch, byte[] key, byte[] value) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_write_batch_merge(
                    arena,
                    writeBatch,
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length
                )
            );
        }
    }

    static void slatedb_write_batch_merge_with_options(
        MemorySegment writeBatch,
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
                    writeBatch,
                    marshalBytes(arena, key),
                    key.length,
                    marshalBytes(arena, value),
                    value.length,
                    marshalMergeOptions(arena, mergeOptions)
                )
            );
        }
    }

    static void slatedb_write_batch_delete(MemorySegment writeBatch, byte[] key) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        Objects.requireNonNull(key, "key");

        try (Arena arena = Arena.ofConfined()) {
            checkResult(
                Native.slatedb_write_batch_delete(
                    arena,
                    writeBatch,
                    marshalBytes(arena, key),
                    key.length
                )
            );
        }
    }

    static void slatedb_write_batch_close(MemorySegment writeBatch) {
        Objects.requireNonNull(writeBatch, "writeBatch");
        try (Arena arena = Arena.ofConfined()) {
            checkResult(Native.slatedb_write_batch_close(arena, writeBatch));
        }
    }

    static void slatedb_result_free(MemorySegment result) {
        Objects.requireNonNull(result, "result");
        Native.slatedb_result_free(result);
    }

    static void slatedb_bytes_free(MemorySegment data, long len) {
        Objects.requireNonNull(data, "data");
        Native.slatedb_bytes_free(data, len);
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

    private static void checkResult(MemorySegment result) {
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
        slatedb_read_options_t.durability_filter(nativeOptions, toNativeDurability(options.durabilityFilter()));
        slatedb_read_options_t.dirty(nativeOptions, options.dirty());
        slatedb_read_options_t.cache_blocks(nativeOptions, options.cacheBlocks());
        return nativeOptions;
    }

    private static MemorySegment marshalScanOptions(Arena arena, ScanOptions options) {
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
        slatedb_put_options_t.ttl_type(nativeOptions, toNativeTtl(options.ttlType()));
        slatedb_put_options_t.ttl_value(nativeOptions, options.ttlValueMs());
        return nativeOptions;
    }

    private static MemorySegment marshalMergeOptions(Arena arena, PutOptions options) {
        if (options == null) {
            return MemorySegment.NULL;
        }
        MemorySegment nativeOptions = slatedb_merge_options_t.allocate(arena);
        slatedb_merge_options_t.ttl_type(nativeOptions, toNativeTtl(options.ttlType()));
        slatedb_merge_options_t.ttl_value(nativeOptions, options.ttlValueMs());
        return nativeOptions;
    }

    private static MemorySegment marshalFlushOptions(Arena arena, byte flushType) {
        MemorySegment nativeOptions = slatedb_flush_options_t.allocate(arena);
        slatedb_flush_options_t.flush_type(nativeOptions, flushType);
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
