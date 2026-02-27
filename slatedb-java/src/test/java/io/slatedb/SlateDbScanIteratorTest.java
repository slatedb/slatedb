package io.slatedb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class SlateDbScanIteratorTest {
    @Test
    void seekAcrossBatchBoundaries() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        int totalEntries = 100; // > 64 (DEFAULT_BATCH_SIZE)

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            for (int i = 0; i < totalEntries; i++) {
                String key = String.format("key-%04d", i);
                String value = String.format("val-%04d", i);
                db.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
            }

            // Scan all entries to verify count
            try (SlateDbScanIterator iter = db.scan(null, null)) {
                int count = 0;
                while (iter.next() != null) {
                    count++;
                }
                Assertions.assertEquals(totalEntries, count, "should iterate all entries across batch boundaries");
            }

            // Seek to a key in the second batch (past position 64)
            byte[] seekTarget = String.format("key-%04d", 80).getBytes(StandardCharsets.UTF_8);
            byte[] expectedValue = String.format("val-%04d", 80).getBytes(StandardCharsets.UTF_8);

            try (SlateDbScanIterator iter = db.scan(null, null)) {
                // Consume some entries from the first batch
                for (int i = 0; i < 10; i++) {
                    Assertions.assertNotNull(iter.next());
                }

                // Seek past the first batch boundary
                iter.seek(seekTarget);
                SlateDbKeyValue afterSeek = iter.next();
                Assertions.assertNotNull(afterSeek);
                Assertions.assertArrayEquals(seekTarget, afterSeek.key());
                Assertions.assertArrayEquals(expectedValue, afterSeek.value());

                // Continue iterating to the end
                int remaining = 0;
                while (iter.next() != null) {
                    remaining++;
                }
                Assertions.assertEquals(totalEntries - 81, remaining, "should iterate remaining entries after seek");
            }
        }
    }

    @Test
    void scanAndSeekWithOptions() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] keyA = "scan-a".getBytes(StandardCharsets.UTF_8);
        byte[] keyB = "scan-b".getBytes(StandardCharsets.UTF_8);
        byte[] keyC = "scan-c".getBytes(StandardCharsets.UTF_8);
        byte[] valueA = "value-a".getBytes(StandardCharsets.UTF_8);
        byte[] valueB = "value-b".getBytes(StandardCharsets.UTF_8);
        byte[] valueC = "value-c".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            db.put(keyA, valueA);
            db.put(keyB, valueB);
            db.put(keyC, valueC);

            SlateDbConfig.ScanOptions scanOptions = SlateDbConfig.ScanOptions.builder()
                .durabilityFilter(SlateDbConfig.Durability.MEMORY)
                .readAheadBytes(1)
                .cacheBlocks(false)
                .maxFetchTasks(1)
                .build();

            try (SlateDbScanIterator iter = db.scan(null, null, scanOptions)) {
                SlateDbKeyValue first = iter.next();
                Assertions.assertNotNull(first);

                iter.seek(keyB);
                SlateDbKeyValue afterSeek = iter.next();
                Assertions.assertNotNull(afterSeek);
                Assertions.assertArrayEquals(keyB, afterSeek.key());
                Assertions.assertArrayEquals(valueB, afterSeek.value());
            }

            try (SlateDbScanIterator iter = db.scanPrefix("scan-".getBytes(StandardCharsets.UTF_8))) {
                int count = 0;
                while (iter.next() != null) {
                    count++;
                }
                Assertions.assertEquals(3, count);
            }
        }
    }
}
