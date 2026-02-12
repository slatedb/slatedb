package io.slatedb;

import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.wrap;
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
        final ByteBuffer key,
        final ByteBuffer value
    ) {
        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), url, null)) {
            db.put(key, value);
            db.flush();
        }
    }

    @Test
    void readerGetAndScan() throws Exception {
        TestSupport.ensureNativeReady();
        final var context = TestSupport.createDbContext();
        final var readerObjectStoreUrl = "file://" + context.objectStoreRoot().toAbsolutePath();

        final var key = wrap("reader-key".getBytes(UTF_8));
        final var value = wrap("reader-value".getBytes(UTF_8));
        createSlateDB(context, readerObjectStoreUrl, key, value);

        try (final var reader = SlateDb.openReader(
            context.dbPath().toAbsolutePath().toString(),
            readerObjectStoreUrl,
            null,
            null,
            DEFAULT_READER_OPTIONS
        )) {
            assertEquals(value, reader.get(key));

            try (SlateDbScanIterator iter = reader.scanPrefix(wrap("reader-".getBytes(UTF_8)))) {
                final var kv = iter.next();
                assertNotNull(kv);
                assertEquals(key, kv.key());
                assertEquals(value, kv.value());
            }
        }
    }

    @Test
    void readerGetMissingKey() throws Exception {
        TestSupport.ensureNativeReady();
        final var context = TestSupport.createDbContext();
        final var readerObjectStoreUrl = "file://" + context.objectStoreRoot().toAbsolutePath();

        createSlateDB(context, readerObjectStoreUrl, wrap("key".getBytes(UTF_8)), wrap("value".getBytes(UTF_8)));

        try (final var reader = SlateDb.openReader(
            context.dbPath().toAbsolutePath().toString(),
            readerObjectStoreUrl,
            null,
            null,
            DEFAULT_READER_OPTIONS
        )) {
            assertNull(reader.get(wrap("missing".getBytes(UTF_8))));
        }
    }

    @Test
    void readerCloseIsIdempotentAndGuardsMethods() throws Exception {
        TestSupport.ensureNativeReady();
        final var context = TestSupport.createDbContext();
        final var readerObjectStoreUrl = "file://" + context.objectStoreRoot().toAbsolutePath();

        final var key = wrap("reader-close-key".getBytes(UTF_8));
        final var value = wrap("reader-close-value".getBytes(UTF_8));
        createSlateDB(context, readerObjectStoreUrl, key, value);

        final var reader = SlateDb.openReader(
            context.dbPath().toAbsolutePath().toString(),
            readerObjectStoreUrl,
            null,
            null,
            null
        );

        try {
            assertEquals(value, reader.get(key));
            reader.close();
            assertDoesNotThrow(reader::close);
        } finally {
            reader.close();
        }
    }
}
