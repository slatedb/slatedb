package io.slatedb;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

class SlateDbTest {
    @Test
    void openPutGetClose() throws Exception {
        TestSupport.ensureLoggingInitialized();
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

    @Test
    void closeIsIdempotentAndGuardsMethods() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key = "close-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "close-value".getBytes(StandardCharsets.UTF_8);

        SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null);
        try {
            db.put(key, value);
        } finally {
            db.close();
        }

        Assertions.assertDoesNotThrow(db::close);
    }

    @Test
    void settingsFunctionsReturnJson() throws Exception {
        TestSupport.ensureLoggingInitialized();

        String defaults = SlateDb.settingsDefault();
        Assertions.assertNotNull(defaults);
        Assertions.assertFalse(defaults.isBlank());

        Path settingsFile = Files.createTempFile("slatedb-settings", ".json");
        Files.writeString(settingsFile, "{}");
        String fromFile = SlateDb.settingsFromFile(settingsFile.toString());
        Assertions.assertNotNull(fromFile);
        Assertions.assertFalse(fromFile.isBlank());

        String fromEnv = SlateDb.settingsFromEnv("SLATEDB_TEST_");
        Assertions.assertNotNull(fromEnv);
        Assertions.assertFalse(fromEnv.isBlank());

        Path loadConfig = Path.of("SlateDb.json");
        boolean created = false;
        byte[] original = null;
        if (Files.exists(loadConfig)) {
            original = Files.readAllBytes(loadConfig);
        } else {
            Files.writeString(loadConfig, "{}");
            created = true;
        }
        try {
            String loaded = SlateDb.settingsLoad();
            Assertions.assertNotNull(loaded);
            Assertions.assertFalse(loaded.isBlank());
        } finally {
            if (created) {
                Files.deleteIfExists(loadConfig);
            } else if (original != null) {
                Files.write(loadConfig, original);
            }
        }
    }

    @Test
    void builderCreatesDatabase() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        String settingsJson = SlateDb.settingsDefault();
        try (SlateDb.Builder builder = SlateDb.builder(
            context.dbPath().toAbsolutePath().toString(),
            context.objectStoreUrl(),
            null
        )) {
            builder.withSettingsJson(settingsJson)
                .withSstBlockSize(SlateDbConfig.SstBlockSize.KIB_4);
            try (SlateDb db = builder.build()) {
                byte[] key = "builder-key".getBytes(StandardCharsets.UTF_8);
                byte[] value = "builder-value".getBytes(StandardCharsets.UTF_8);
                db.put(key, value);
                Assertions.assertArrayEquals(value, db.get(key));
            }
        }
    }

    @Test
    void builderInvalidUrlReportsNativeError() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        SlateDbException failure = Assertions.assertThrows(
            SlateDbException.class,
            () -> SlateDb.builder(context.dbPath().toAbsolutePath().toString(), "bogus://", null)
        );
        Assertions.assertFalse(failure instanceof SlateDbException.ClosedException);
        Assertions.assertNotNull(failure.getMessage());
        Assertions.assertFalse(failure.getMessage().isBlank());
    }

    @Test
    void closedExceptionCarriesCloseReasonDetails() {
        SlateDbException closed = new SlateDbException.ClosedException("fenced", 2, "closed");
        SlateDbException.ClosedException closedError = assertInstanceOf(SlateDbException.ClosedException.class, closed);
        assertEquals("fenced", closedError.getCloseReason());
        assertEquals(2, closedError.getCloseReasonCode());
    }

    @Test
    void builderWithInvalidSettingsJsonThrowsIllegalArgumentException() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        try (SlateDb.Builder builder = SlateDb.builder(
            context.dbPath().toAbsolutePath().toString(),
            context.objectStoreUrl(),
            null
        )) {
            IllegalArgumentException failure = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> builder.withSettingsJson("{\"broken\":")
            );
            Assertions.assertNotNull(failure.getMessage());
            Assertions.assertFalse(failure.getMessage().isBlank());
        }
    }

    @Test
    void builderBuildFailureClosesBuilder() throws Exception {
        TestSupport.ensureLoggingInitialized();
        TestSupport.DbContext context = TestSupport.createDbContext();

        Path cacheRootDir = Files.createTempDirectory("slatedb-java-cache");

        SlateDb.Builder builder = SlateDb.builder(
            context.dbPath().toAbsolutePath().toString(),
            context.objectStoreUrl(),
            null
        );

        SlateDb db = null;
        try {
            String cacheRoot = cacheRootDir.toAbsolutePath().toString().replace("\\", "\\\\");
            String settingsJson = SlateDb.settingsDefault()
                .replace("\"root_folder\":null", "\"root_folder\":\"" + cacheRoot + "\"")
                .replace("\"part_size_bytes\":4194304", "\"part_size_bytes\":0");
            builder.withSettingsJson(settingsJson);
            db = builder.build();
            Assertions.fail("Expected builder.build() to fail with an invalid cache part size");
        } catch (RuntimeException expected) {
        } finally {
            if (db != null) {
                db.close();
            }
            Assertions.assertDoesNotThrow(builder::close);
            Files.deleteIfExists(cacheRootDir);
        }
    }

    @Test
    void putGetDeleteWithOptionsAndMetrics() throws Exception {
        TestSupport.ensureLoggingInitialized();
        final var context = TestSupport.createDbContext();

        final var key = "opts-key".getBytes(StandardCharsets.UTF_8);
        final var value = "opts-value".getBytes(StandardCharsets.UTF_8);

        try (final SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            db.put(key, value, SlateDbConfig.PutOptions.noExpiry(), new SlateDbConfig.WriteOptions(false));
            final var readOptions = SlateDbConfig.ReadOptions.builder()
                .durabilityFilter(SlateDbConfig.Durability.MEMORY)
                .dirty(false)
                .cacheBlocks(true)
                .build();
            assertArrayEquals(value, db.get(key, readOptions));

            db.delete(key, new SlateDbConfig.WriteOptions(false));
            assertNull(db.get(key));

            final var metrics = db.metrics();
            assertNotNull(metrics);
            assertTrue(metrics.startsWith("{"));
        }
    }
}
