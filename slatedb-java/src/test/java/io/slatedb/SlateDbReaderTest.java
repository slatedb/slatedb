package io.slatedb;

import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;

class SlateDbReaderTest {
    private static final SlateDbConfig.ReaderOptions DEFAULT_READER_OPTIONS = SlateDbConfig.ReaderOptions.builder()
        .manifestPollInterval(Duration.ofMillis(200))
        .checkpointLifetime(Duration.ofSeconds(1))
        .maxMemtableBytes(1024 * 1024)
        .skipWalReplay(false)
        .build();

    private static void createSlateDB(
        final TestSupport.DbContext context,
        final String url,
        final byte[] key,
        final byte[] value
    ) {
        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), url, null)) {
            db.put(key, value);
            db.flush();
        }
    }

    @Test
    void readerGetAndScan() throws Exception {
        TestSupport.ensureLoggingInitialized();
        final var context = TestSupport.createDbContext();
        final var readerObjectStoreUrl = "file://" + context.objectStoreRoot().toAbsolutePath();

        final var key = "reader-key".getBytes(UTF_8);
        final var value = "reader-value".getBytes(UTF_8);
        createSlateDB(context, readerObjectStoreUrl, key, value);

        try (final var reader = SlateDb.openReader(
            context.dbPath().toAbsolutePath().toString(),
            readerObjectStoreUrl,
            null,
            null,
            DEFAULT_READER_OPTIONS
        )) {
            assertArrayEquals(value, reader.get(key));

            try (SlateDbScanIterator iter = reader.scanPrefix("reader-".getBytes(UTF_8))) {
                final var kv = iter.next();
                assertNotNull(kv);
                assertArrayEquals(key, kv.key());
                assertArrayEquals(value, kv.value());
            }
        }
    }

    @Test
    void readerGetMissingKey() throws Exception {
        TestSupport.ensureLoggingInitialized();
        final var context = TestSupport.createDbContext();
        final var readerObjectStoreUrl = "file://" + context.objectStoreRoot().toAbsolutePath();

        createSlateDB(context, readerObjectStoreUrl, "key".getBytes(UTF_8), "value".getBytes(UTF_8));

        try (final var reader = SlateDb.openReader(
            context.dbPath().toAbsolutePath().toString(),
            readerObjectStoreUrl,
            null,
            null,
            DEFAULT_READER_OPTIONS
        )) {
            assertNull(reader.get("missing".getBytes(UTF_8)));
        }
    }

    @Test
    void readerCloseIsIdempotentAndGuardsMethods() throws Exception {
        TestSupport.ensureLoggingInitialized();
        final var context = TestSupport.createDbContext();
        final var readerObjectStoreUrl = "file://" + context.objectStoreRoot().toAbsolutePath();

        final var key = "reader-close-key".getBytes(UTF_8);
        final var value = "reader-close-value".getBytes(UTF_8);
        createSlateDB(context, readerObjectStoreUrl, key, value);

        final var reader = SlateDb.openReader(
            context.dbPath().toAbsolutePath().toString(),
            readerObjectStoreUrl,
            null,
            null,
            null
        );

        try {
            assertArrayEquals(value, reader.get(key));
            reader.close();
            assertDoesNotThrow(reader::close);
        } finally {
            reader.close();
        }
    }
}
