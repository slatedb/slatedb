package io.slatedb;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SlateDbWriteHandleTest {

    @Test
    void testPutReturnsWriteHandle() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            byte[] key = "key1".getBytes(StandardCharsets.UTF_8);
            byte[] value = "value1".getBytes(StandardCharsets.UTF_8);

            SlateDbWriteHandle wh = db.put(key, value);

            assertNotNull(wh);
            assertEquals(1L, wh.seq());
            assertTrue(wh.createTs() > 0);
        }
    }

    @Test
    void testDeleteReturnsWriteHandle() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            byte[] key = "key1".getBytes(StandardCharsets.UTF_8);
            byte[] value = "value1".getBytes(StandardCharsets.UTF_8);

            // Put first (seq=1)
            db.put(key, value);

            // Delete (seq=2)
            SlateDbWriteHandle wh = db.delete(key);

            assertNotNull(wh);
            assertEquals(2L, wh.seq());
            assertTrue(wh.createTs() > 0);
        }
    }

    @Test
    void testWriteBatchReturnsWriteHandle() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            try (SlateDbWriteBatch batch = SlateDb.newWriteBatch()) {
                batch.put("k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8));
                batch.put("k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8));

                SlateDbWriteHandle wh = db.write(batch);

                assertNotNull(wh);
                assertEquals(1L, wh.seq());
                assertTrue(wh.createTs() > 0);
            }
        }
    }

    @Test
    void testIncrementingSequenceNumbers() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            byte[] key = "key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "value".getBytes(StandardCharsets.UTF_8);

            SlateDbWriteHandle wh1 = db.put(key, value);
            assertEquals(1L, wh1.seq());

            SlateDbWriteHandle wh2 = db.put(key, value);
            assertEquals(2L, wh2.seq());

            SlateDbWriteHandle wh3 = db.delete(key);
            assertEquals(3L, wh3.seq());
        }
    }

    @Test
    void testPutWithOptionsReturnsWriteHandle() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            byte[] key = "key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "value".getBytes(StandardCharsets.UTF_8);

            SlateDbWriteHandle wh = db.put(
                key,
                value,
                SlateDbConfig.PutOptions.noExpiry(),
                new SlateDbConfig.WriteOptions(true)
            );

            assertNotNull(wh);
            assertEquals(1L, wh.seq());
            assertTrue(wh.createTs() > 0);
        }
    }
}
