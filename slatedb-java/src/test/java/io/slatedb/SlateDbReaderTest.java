package io.slatedb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

class SlateDbReaderTest {
    @Test
    void readerGetAndScan() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();
        String readerObjectStoreUrl = "file://" + context.objectStoreRoot().toAbsolutePath();

        byte[] key = "reader-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "reader-value".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(
            context.dbPath().toAbsolutePath().toString(),
            readerObjectStoreUrl,
            null
        )) {
            db.put(key, value);
            db.flush();
        }

        SlateDb.ReaderOptions readerOptions = SlateDb.ReaderOptions.builder()
            .manifestPollInterval(Duration.ofMillis(200))
            .checkpointLifetime(Duration.ofSeconds(1))
            .maxMemtableBytes(1024 * 1024)
            .skipWalReplay(false)
            .build();

        try (SlateDbReader reader = SlateDb.openReader(
            context.dbPath().toAbsolutePath().toString(),
            readerObjectStoreUrl,
            null,
            null,
            readerOptions
        )) {
            Assertions.assertArrayEquals(value, reader.get(key));

            try (SlateDbScanIterator iter = reader.scanPrefix("reader-".getBytes(StandardCharsets.UTF_8))) {
                SlateDbKeyValue kv = iter.next();
                Assertions.assertNotNull(kv);
                Assertions.assertArrayEquals(key, kv.key());
                Assertions.assertArrayEquals(value, kv.value());
            }
        }
    }

    @Test
    void readerCloseIsIdempotentAndGuardsMethods() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();
        String readerObjectStoreUrl = "file://" + context.objectStoreRoot().toAbsolutePath();

        byte[] key = "reader-close-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "reader-close-value".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(
            context.dbPath().toAbsolutePath().toString(),
            readerObjectStoreUrl,
            null
        )) {
            db.put(key, value);
            db.flush();
        }

        SlateDbReader reader = SlateDb.openReader(
            context.dbPath().toAbsolutePath().toString(),
            readerObjectStoreUrl,
            null,
            null,
            null
        );

        try {
            Assertions.assertArrayEquals(value, reader.get(key));
            reader.close();
            Assertions.assertDoesNotThrow(reader::close);
        } finally {
            reader.close();
        }
    }
}
