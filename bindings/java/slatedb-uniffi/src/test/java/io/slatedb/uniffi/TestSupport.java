package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

final class TestSupport {
    static final long TIMEOUT_SECONDS = 30;
    static final Duration WAIT_TIMEOUT = Duration.ofSeconds(60);
    static final Duration WAIT_STEP = Duration.ofMillis(25);
    static final String TEST_DB_PATH = "test-db";

    private TestSupport() {}

    @FunctionalInterface
    interface DbBuilderConfigurer {
        void configure(DbBuilder builder) throws Exception;
    }

    @FunctionalInterface
    interface DbReaderBuilderConfigurer {
        void configure(DbReaderBuilder builder) throws Exception;
    }

    @FunctionalInterface
    interface CheckedRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    interface CheckedBooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }

    static final class ManagedDb implements AutoCloseable {
        private final Db db;
        private boolean open = true;

        ManagedDb(Db db) {
            this.db = db;
        }

        Db db() {
            return db;
        }

        void markClosed() {
            open = false;
        }

        @Override
        public void close() throws Exception {
            if (open) {
                await(db.shutdown());
            }
            db.close();
        }
    }

    static final class ManagedReader implements AutoCloseable {
        private final DbReader reader;
        private boolean open = true;

        ManagedReader(DbReader reader) {
            this.reader = reader;
        }

        DbReader reader() {
            return reader;
        }

        void markClosed() {
            open = false;
        }

        @Override
        public void close() throws Exception {
            if (open) {
                await(reader.shutdown());
            }
            reader.close();
        }
    }

    static final class ConcatMergeOperator implements MergeOperator {
        @Override
        public byte[] merge(byte[] key, byte[] existingValue, byte[] operand)
                throws MergeOperatorCallbackException {
            byte[] existing = existingValue == null ? new byte[0] : existingValue;
            byte[] merged = Arrays.copyOf(existing, existing.length + operand.length);
            System.arraycopy(operand, 0, merged, existing.length, operand.length);
            return merged;
        }
    }

    static final class LogCollector implements LogCallback {
        private final List<LogRecord> records = new ArrayList<>();

        @Override
        public synchronized void log(LogRecord record) {
            records.add(cloneLogRecord(record));
        }

        synchronized LogRecord matchingRecord(Predicate<LogRecord> predicate) {
            for (LogRecord record : records) {
                if (predicate.test(record)) {
                    return cloneLogRecord(record);
                }
            }
            return null;
        }
    }

    static ObjectStore newMemoryStore() throws Exception {
        return ObjectStore.resolve("memory:///");
    }

    static ManagedDb openDb(ObjectStore store) throws Exception {
        return openDb(TEST_DB_PATH, store, null);
    }

    static ManagedDb openDb(ObjectStore store, DbBuilderConfigurer configurer) throws Exception {
        return openDb(TEST_DB_PATH, store, configurer);
    }

    static ManagedDb openDb(String path, ObjectStore store, DbBuilderConfigurer configurer)
            throws Exception {
        try (DbBuilder builder = new DbBuilder(path, store)) {
            if (configurer != null) {
                configurer.configure(builder);
            }
            return new ManagedDb(await(builder.build()));
        }
    }

    static ManagedReader openReader(ObjectStore store) throws Exception {
        return openReader(TEST_DB_PATH, store, null);
    }

    static ManagedReader openReader(ObjectStore store, DbReaderBuilderConfigurer configurer)
            throws Exception {
        return openReader(TEST_DB_PATH, store, configurer);
    }

    static ManagedReader openReader(String path, ObjectStore store, DbReaderBuilderConfigurer configurer)
            throws Exception {
        try (DbReaderBuilder builder = new DbReaderBuilder(path, store)) {
            if (configurer != null) {
                configurer.configure(builder);
            }
            return new ManagedReader(await(builder.build()));
        }
    }

    static WalReader openWalReader(ObjectStore store) throws Exception {
        return new WalReader(TEST_DB_PATH, store);
    }

    static void seedWalFiles(ObjectStore store) throws Exception {
        try (ManagedDb handle = openDb(store, builder -> builder.withMergeOperator(new ConcatMergeOperator()))) {
            Db db = handle.db();
            await(db.put(bytes("a"), bytes("1")));
            await(db.put(bytes("b"), bytes("2")));
            await(db.flushWithOptions(new FlushOptions(FlushType.WAL)));

            await(db.delete(bytes("a")));
            await(db.flushWithOptions(new FlushOptions(FlushType.WAL)));

            await(db.merge(bytes("m"), bytes("x")));
            await(db.flushWithOptions(new FlushOptions(FlushType.WAL)));
        }
    }

    static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    static <E extends Throwable> E awaitFailure(Class<E> errorType, CompletableFuture<?> future)
            throws Exception {
        try {
            await(future);
        } catch (Throwable thrown) {
            Throwable cause = unwrap(thrown);
            if (errorType.isInstance(cause)) {
                return errorType.cast(cause);
            }
            throw new AssertionError(
                    "expected " + errorType.getName() + " but got " + cause.getClass().getName(), cause);
        }
        return fail("expected " + errorType.getName());
    }

    static <E extends Throwable> E expectFailure(Class<E> errorType, CheckedRunnable action)
            throws Exception {
        try {
            action.run();
        } catch (Throwable thrown) {
            Throwable cause = unwrap(thrown);
            if (errorType.isInstance(cause)) {
                return errorType.cast(cause);
            }
            throw new AssertionError(
                    "expected " + errorType.getName() + " but got " + cause.getClass().getName(), cause);
        }
        return fail("expected " + errorType.getName());
    }

    static void waitUntil(Duration timeout, Duration step, CheckedBooleanSupplier check) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        Throwable lastError = null;

        while (true) {
            try {
                if (check.getAsBoolean()) {
                    return;
                }
                lastError = null;
            } catch (Throwable thrown) {
                lastError = thrown;
            }

            if (System.nanoTime() >= deadline) {
                break;
            }

            Thread.sleep(step.toMillis());
        }

        if (lastError != null) {
            throw new AssertionError("timed out after " + timeout, lastError);
        }
        fail("timed out after " + timeout);
    }

    static List<KeyValue> drainIterator(DbIterator iterator) throws Exception {
        List<KeyValue> rows = new ArrayList<>();
        while (true) {
            KeyValue row = await(iterator.next());
            if (row == null) {
                return rows;
            }
            rows.add(row);
        }
    }

    static List<RowEntry> drainWalIterator(WalFileIterator iterator) throws Exception {
        List<RowEntry> rows = new ArrayList<>();
        while (true) {
            RowEntry row = await(iterator.next());
            if (row == null) {
                return rows;
            }
            rows.add(row);
        }
    }

    static void closeAll(Iterable<? extends AutoCloseable> closeables) throws Exception {
        Exception firstError = null;
        for (AutoCloseable closeable : closeables) {
            try {
                if (closeable != null) {
                    closeable.close();
                }
            } catch (Exception error) {
                if (firstError == null) {
                    firstError = error;
                } else {
                    firstError.addSuppressed(error);
                }
            }
        }

        if (firstError != null) {
            throw firstError;
        }
    }

    static void assertRows(List<KeyValue> rows, String[] expectedKeys, String[] expectedValues) {
        assertEquals(expectedKeys.length, rows.size(), "unexpected row count");
        assertEquals(expectedValues.length, rows.size(), "unexpected value count");

        for (int i = 0; i < rows.size(); i++) {
            KeyValue row = rows.get(i);
            assertArrayEquals(bytes(expectedKeys[i]), row.key(), "unexpected key at row " + i);
            assertArrayEquals(bytes(expectedValues[i]), row.value(), "unexpected value at row " + i);
        }
    }

    static void assertWalRow(RowEntry row, RowEntryKind kind, String key, String value) {
        assertEquals(kind, row.kind(), "unexpected row kind");
        assertArrayEquals(bytes(key), row.key(), "unexpected row key");
        if (value == null) {
            assertNull(row.value(), "expected null row value");
        } else {
            assertArrayEquals(bytes(value), row.value(), "unexpected row value");
        }
    }

    static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    static String uniquePath(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    static ReadOptions readOptions() {
        return new ReadOptions(DurabilityLevel.MEMORY, false, true);
    }

    static ScanOptions scanOptions(long readAheadBytes, boolean cacheBlocks, long maxFetchTasks) {
        return new ScanOptions(
                DurabilityLevel.MEMORY, false, readAheadBytes, cacheBlocks, maxFetchTasks);
    }

    static ReaderOptions readerOptions(boolean skipWalReplay) {
        return new ReaderOptions(100L, 1000L, 64L * 1024 * 1024, skipWalReplay);
    }

    private static Throwable unwrap(Throwable thrown) {
        Throwable current = thrown;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static LogRecord cloneLogRecord(LogRecord record) {
        return new LogRecord(
                record.level(),
                record.target(),
                record.message(),
                record.modulePath(),
                record.file(),
                record.line());
    }
}
