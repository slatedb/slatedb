package io.slatedb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class SlateDbScanIteratorTest {
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

    @Test
    void scanNextRow() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] keyA = "row-a".getBytes(StandardCharsets.UTF_8);
        byte[] keyB = "row-b".getBytes(StandardCharsets.UTF_8);
        byte[] valueA = "val-a".getBytes(StandardCharsets.UTF_8);
        byte[] valueB = "val-b".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            long seqA = db.put(keyA, valueA).seq();
            long seqB = db.put(keyB, valueB).seq();

            try (SlateDbScanIterator iter = db.scanPrefix("row-".getBytes(StandardCharsets.UTF_8))) {
                RowEntry rowA = iter.nextRow();
                Assertions.assertNotNull(rowA);
                Assertions.assertArrayEquals(keyA, rowA.key());
                Assertions.assertArrayEquals(valueA, rowA.value());
                Assertions.assertEquals(seqA, rowA.seq());

                RowEntry rowB = iter.nextRow();
                Assertions.assertNotNull(rowB);
                Assertions.assertArrayEquals(keyB, rowB.key());
                Assertions.assertArrayEquals(valueB, rowB.value());
                Assertions.assertEquals(seqB, rowB.seq());

                Assertions.assertNull(iter.nextRow());
            }
        }
    }
}
