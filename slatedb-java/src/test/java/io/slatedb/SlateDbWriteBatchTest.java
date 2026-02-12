package io.slatedb;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import org.junit.jupiter.api.Test;

import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.time.Duration;

class SlateDbWriteBatchTest {
    @Test
    void writeBatchPutAndDelete() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        final var key1 = wrap("batch-key-1".getBytes(UTF_8));
        final var key2 = wrap("batch-key-2".getBytes(UTF_8));
        final var value1 = wrap("batch-value-1".getBytes(UTF_8));
        final var value2 = wrap("batch-value-2".getBytes(UTF_8));

        try (final var db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null);
             final var batch = SlateDb.newWriteBatch()) {
            batch.put(key1, value1);
            batch.put(key2, value2);
            batch.delete(key1);

            db.write(batch);

            assertNull(db.get(key1));
            assertEquals(value2, db.get(key2));
        }
    }

    @Test
    void writeBatchPutWithOptionsAndWriteOptions() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        final var key = wrap("batch-opts-key".getBytes(UTF_8));
        final var value = wrap("batch-opts-value".getBytes(UTF_8));

        SlateDbConfig.PutOptions putOptions = SlateDbConfig.PutOptions.expireAfter(Duration.ofMinutes(5));
        SlateDbConfig.WriteOptions writeOptions = new SlateDbConfig.WriteOptions(false);

        try (final var db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null);
             final var batch = SlateDb.newWriteBatch()) {
            batch.put(key, value, putOptions);
            db.write(batch, writeOptions);

            assertEquals(value, db.get(key));
        }
    }

    @Test
    void writeBatchCloseIsIdempotent() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        try (final var db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            final var batch = SlateDb.newWriteBatch();
            batch.put(wrap("idempotent-key".getBytes(UTF_8)), wrap("idempotent-value".getBytes(UTF_8)));
            db.write(batch);
            batch.close();
            assertDoesNotThrow(batch::close);
        }
    }
}
