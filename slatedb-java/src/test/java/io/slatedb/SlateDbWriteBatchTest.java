package io.slatedb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

class SlateDbWriteBatchTest {
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
}
