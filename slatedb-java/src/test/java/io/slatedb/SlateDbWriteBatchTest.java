package io.slatedb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;

class SlateDbWriteBatchTest {
    private static SlateDb openWithConcatMergeOperator(TestSupport.DbContext context) throws Exception {
        try (SlateDb.Builder builder = SlateDb.builder(
            context.dbPath().toAbsolutePath().toString(),
            context.objectStoreUrl(),
            null
        )) {
            builder.withMergeOperator((key, existingValue, operand) -> {
                byte[] base = existingValue == null ? new byte[0] : existingValue;
                byte[] merged = Arrays.copyOf(base, base.length + operand.length);
                System.arraycopy(operand, 0, merged, base.length, operand.length);
                return merged;
            });
            return builder.build();
        }
    }

    @Test
    void writeBatchPutAndDelete() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key1 = "batch-key-1".getBytes(StandardCharsets.UTF_8);
        byte[] key2 = "batch-key-2".getBytes(StandardCharsets.UTF_8);
        byte[] value1 = "batch-value-1".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "batch-value-2".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null);
             SlateDbWriteBatch batch = SlateDb.newWriteBatch()) {
            batch.put(key1, value1);
            batch.put(key2, value2);
            batch.delete(key1);

            db.write(batch);

            Assertions.assertNull(db.get(key1));
            Assertions.assertArrayEquals(value2, db.get(key2));
        }
    }

    @Test
    void writeBatchPutWithOptionsAndWriteOptions() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key = "batch-opts-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "batch-opts-value".getBytes(StandardCharsets.UTF_8);

        SlateDbConfig.PutOptions putOptions = SlateDbConfig.PutOptions.expireAfter(Duration.ofMinutes(5));
        SlateDbConfig.WriteOptions writeOptions = new SlateDbConfig.WriteOptions(false);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null);
             SlateDbWriteBatch batch = SlateDb.newWriteBatch()) {
            batch.put(key, value, putOptions);
            db.write(batch, writeOptions);

            Assertions.assertArrayEquals(value, db.get(key));
        }
    }

    @Test
    void writeBatchCloseIsIdempotent() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            SlateDbWriteBatch batch = SlateDb.newWriteBatch();
            batch.put("idempotent-key".getBytes(StandardCharsets.UTF_8),
                "idempotent-value".getBytes(StandardCharsets.UTF_8));
            db.write(batch);
            batch.close();
            Assertions.assertDoesNotThrow(batch::close);
        }
    }

    @Test
    void writeBatchMergeOperations() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key = "batch-merge-key".getBytes(StandardCharsets.UTF_8);
        byte[] first = "a".getBytes(StandardCharsets.UTF_8);
        byte[] second = "b".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = openWithConcatMergeOperator(context);
             SlateDbWriteBatch batch = SlateDb.newWriteBatch()) {
            batch.merge(key, first);
            batch.merge(key, second);
            db.write(batch);

            Assertions.assertArrayEquals("ab".getBytes(StandardCharsets.UTF_8), db.get(key));
        }
    }

    @Test
    void writeBatchMergeWithOptions() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key = "batch-merge-opts-key".getBytes(StandardCharsets.UTF_8);
        byte[] first = "x".getBytes(StandardCharsets.UTF_8);
        byte[] second = "y".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = openWithConcatMergeOperator(context);
             SlateDbWriteBatch batch = SlateDb.newWriteBatch()) {
            batch.merge(key, first, SlateDbConfig.MergeOptions.noExpiry());
            batch.merge(key, second, SlateDbConfig.MergeOptions.noExpiry());
            db.write(batch);

            Assertions.assertArrayEquals("xy".getBytes(StandardCharsets.UTF_8), db.get(key));
        }
    }
}
