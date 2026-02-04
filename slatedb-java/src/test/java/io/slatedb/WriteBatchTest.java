package io.slatedb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

class WriteBatchTest {
    @Test
    void writeBatchPutAndDelete() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key1 = "batch-key-1".getBytes(StandardCharsets.UTF_8);
        byte[] key2 = "batch-key-2".getBytes(StandardCharsets.UTF_8);
        byte[] value1 = "batch-value-1".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "batch-value-2".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null);
             WriteBatch batch = SlateDb.newWriteBatch()) {
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
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key = "batch-opts-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "batch-opts-value".getBytes(StandardCharsets.UTF_8);

        SlateDb.PutOptions putOptions = SlateDb.PutOptions.expireAfter(Duration.ofMinutes(5));
        SlateDb.WriteOptions writeOptions = new SlateDb.WriteOptions(false);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null);
             WriteBatch batch = SlateDb.newWriteBatch()) {
            batch.put(key, value, putOptions);
            db.write(batch, writeOptions);

            Assertions.assertArrayEquals(value, db.get(key));
        }
    }

    @Test
    void writeBatchLifecycleGuards() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key = "batch-guard-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "batch-guard-value".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null);
             WriteBatch batch = SlateDb.newWriteBatch()) {
            batch.put(key, value);
            db.write(batch);

            Assertions.assertThrows(IllegalStateException.class, () -> batch.put(key, value));
            Assertions.assertThrows(IllegalStateException.class, () -> db.write(batch));

            batch.close();
            Assertions.assertThrows(IllegalStateException.class, () -> batch.delete(key));
            Assertions.assertDoesNotThrow(batch::close);
        }
    }

    @Test
    void writeBatchFailureConsumesBatch() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key = "batch-fail-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "batch-fail-value".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null);
             WriteBatch batch = SlateDb.newWriteBatch()) {
            batch.put(key, value);
            db.write(batch);

            Field consumedField = WriteBatch.class.getDeclaredField("consumed");
            consumedField.setAccessible(true);
            consumedField.setBoolean(batch, false);

            Assertions.assertThrows(RuntimeException.class, () -> db.write(batch));
            Assertions.assertThrows(IllegalStateException.class, () -> batch.put(key, value));
            Assertions.assertThrows(IllegalStateException.class, () -> db.write(batch));
        }
    }

    @Test
    void writeRejectsClosedBatch() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            WriteBatch batch = SlateDb.newWriteBatch();
            batch.close();
            Assertions.assertThrows(IllegalStateException.class, () -> db.write(batch));
        }
    }
}
