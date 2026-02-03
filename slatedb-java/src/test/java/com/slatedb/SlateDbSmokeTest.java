package com.slatedb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class SlateDbSmokeTest {
    @Test
    void openPutGetClose() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key = "smoke-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "smoke-value".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(
            context.dbPath().toAbsolutePath().toString(),
            context.objectStoreUrl(),
            null
        )) {
            db.put(key, value);
            byte[] loaded = db.get(key);
            Assertions.assertArrayEquals(value, loaded);
        }
    }
}
