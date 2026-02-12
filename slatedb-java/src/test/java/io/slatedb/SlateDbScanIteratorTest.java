package io.slatedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;

class SlateDbScanIteratorTest {
    @Test
    void scanAndSeekWithOptions() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] keyA = "scan-a".getBytes(UTF_8);
        byte[] keyB = "scan-b".getBytes(UTF_8);
        byte[] keyC = "scan-c".getBytes(UTF_8);
        byte[] valueA = "value-a".getBytes(UTF_8);
        byte[] valueB = "value-b".getBytes(UTF_8);
        byte[] valueC = "value-c".getBytes(UTF_8);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            db.put(wrap(keyA), wrap(valueA));
            db.put(wrap(keyB), wrap(valueB));
            db.put(wrap(keyC), wrap(valueC));

            SlateDbConfig.ScanOptions scanOptions = SlateDbConfig.ScanOptions.builder()
                .durabilityFilter(SlateDbConfig.Durability.MEMORY)
                .readAheadBytes(1)
                .cacheBlocks(false)
                .maxFetchTasks(1)
                .build();

            try (SlateDbScanIterator iter = db.scan(null, null, scanOptions)) {
                SlateDbKeyValue first = iter.next();
                assertNotNull(first);

                iter.seek(wrap(keyB));
                SlateDbKeyValue afterSeek = iter.next();
                assertNotNull(afterSeek);
                assertEquals(ByteBuffer.wrap(keyB), afterSeek.key());
                assertEquals(ByteBuffer.wrap(valueB), afterSeek.value());
            }

            try (SlateDbScanIterator iter = db.scanPrefix(wrap("scan-".getBytes(UTF_8)))) {
                int count = 0;
                while (iter.next() != null) {
                    count++;
                }
                assertEquals(3, count);
            }
        }
    }
}
