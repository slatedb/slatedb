package io.slatedb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class SlatedbTest {
    private static final class CounterMergeOperator implements MergeOperator {
        @Override
        public byte[] merge(byte[] key, byte[] existingValue, byte[] operand)
                throws MergeOperatorCallbackException {
            long existing = existingValue == null ? 0L : littleEndianLong(existingValue);
            return littleEndianBytes(existing + littleEndianLong(operand));
        }
    }

    private static final class FailingMergeOperator implements MergeOperator {
        @Override
        public byte[] merge(byte[] key, byte[] existingValue, byte[] operand)
                throws MergeOperatorCallbackException {
            throw new MergeOperatorCallbackException.Failed("bad operand");
        }
    }

    @Test
    void defaultSettingsAndBuilderLifecycle() throws Exception {
        String settingsJson = Slatedb.defaultSettingsJson();
        assertTrue(settingsJson.startsWith("{"));

        try (ObjectStore store = Slatedb.resolveObjectStore("memory:///");
             DbBuilder builder = new DbBuilder("builder-lifecycle", store)) {
            builder.withSettingsJson(settingsJson);

            try (Db db = await(builder.build())) {
                db.status();
                await(db.shutdown());
            }

            assertThrows(SlatedbException.Invalid.class, () -> builder.withSeed(7L));
        }
    }

    @Test
    void putGetSnapshotAndShutdown() throws Exception {
        try (ObjectStore store = Slatedb.resolveObjectStore("memory:///");
             DbBuilder builder = new DbBuilder("snapshot-roundtrip", store);
             Db db = await(builder.build())) {
            await(db.put(bytes("k1"), bytes("v1")));

            try (DbSnapshot snapshot = await(db.snapshot())) {
                await(db.put(bytes("k2"), bytes("v2")));

                assertArrayEquals(bytes("v1"), await(db.get(bytes("k1"))));
                assertArrayEquals(bytes("v2"), await(db.get(bytes("k2"))));
                assertNull(await(snapshot.get(bytes("k2"))));
            }

            await(db.shutdown());
            assertThrows(SlatedbException.Closed.class, db::status);
        }
    }

    @Test
    void writeBatchUsesGeneratedOperationTypes() throws Exception {
        try (ObjectStore store = Slatedb.resolveObjectStore("memory:///");
             DbBuilder builder = new DbBuilder("write-batch", store)) {
            builder.withMergeOperator(new CounterMergeOperator());

            try (Db db = await(builder.build())) {
            await(db.write(List.of(
                    new DbWriteOperation.Put(
                            bytes("counter"),
                            littleEndianBytes(1L),
                            new DbPutOptions(new Ttl.Default())),
                    new DbWriteOperation.Merge(
                            bytes("counter"),
                            littleEndianBytes(2L),
                            new DbMergeOptions(new Ttl.Default()))
            )));

                assertEquals(3L, littleEndianLong(await(db.get(bytes("counter")))));
                await(db.shutdown());
            }
        }
    }

    @Test
    void mergeOperatorRoundTrip() throws Exception {
        try (ObjectStore store = Slatedb.resolveObjectStore("memory:///");
             DbBuilder builder = new DbBuilder("merge-roundtrip", store)) {
            builder.withMergeOperator(new CounterMergeOperator());

            try (Db db = await(builder.build())) {
                await(db.put(bytes("counter"), littleEndianBytes(1L)));
                await(db.merge(bytes("counter"), littleEndianBytes(2L)));

                assertEquals(3L, littleEndianLong(await(db.get(bytes("counter")))));
                await(db.shutdown());
            }
        }
    }

    @Test
    void mergeOperatorFailurePropagates() throws Exception {
        try (ObjectStore store = Slatedb.resolveObjectStore("memory:///");
             DbBuilder builder = new DbBuilder("merge-failure", store)) {
            builder.withMergeOperator(new FailingMergeOperator());

            try (Db db = await(builder.build())) {
                await(db.put(bytes("counter"), littleEndianBytes(1L)));

                SlatedbException.Invalid error = assertThrows(
                        SlatedbException.Invalid.class,
                        () -> await(db.merge(bytes("counter"), littleEndianBytes(2L))));
                assertTrue(error.getMessage().contains("bad operand"));

                await(db.shutdown());
            }
        }
    }

    @Test
    void iteratorAndTransactionLifecycle() throws Exception {
        try (ObjectStore store = Slatedb.resolveObjectStore("memory:///");
             DbBuilder builder = new DbBuilder("iterator-transaction", store);
             Db db = await(builder.build())) {
            await(db.put(bytes("a"), bytes("1")));
            await(db.put(bytes("b"), bytes("2")));
            await(db.put(bytes("c"), bytes("3")));

            try (DbIterator iterator = await(db.scan(new DbKeyRange(
                    bytes("a"),
                    true,
                    bytes("z"),
                    false)))) {
                KeyValue first = await(iterator.next());
                assertNotNull(first);
                assertArrayEquals(bytes("a"), first.key());

                await(iterator.seek(bytes("c")));
                KeyValue third = await(iterator.next());
                assertNotNull(third);
                assertArrayEquals(bytes("c"), third.key());

                assertThrows(
                        SlatedbException.Invalid.class,
                        () -> await(iterator.seek(new byte[0])));
            }

            try (DbTransaction transaction = await(db.begin(IsolationLevel.SNAPSHOT))) {
                await(transaction.put(bytes("tx"), bytes("value")));
                assertArrayEquals(bytes("value"), await(transaction.get(bytes("tx"))));
                WriteHandle handle = await(transaction.commit());
                assertNotNull(handle);
                assertTrue(handle.seqnum() > 0);
                assertThrows(SlatedbException.Invalid.class, () -> await(transaction.commit()));
            }

            await(db.shutdown());
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static byte[] littleEndianBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(value)
                .array();
    }

    private static long littleEndianLong(byte[] value) {
        return ByteBuffer.wrap(value)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getLong();
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return future.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            throw new RuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (TimeoutException e) {
            throw new AssertionError("future timed out", e);
        }
    }
}
