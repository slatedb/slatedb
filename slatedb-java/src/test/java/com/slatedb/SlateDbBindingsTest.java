package com.slatedb;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

class SlateDbBindingsTest {
    @Test
    void settingsFunctionsReturnJson() throws Exception {
        TestSupport.ensureNativeReady();

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
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        String settingsJson = SlateDb.settingsDefault();
        try (SlateDb.Builder builder = SlateDb.builder(
            context.dbPath().toAbsolutePath().toString(),
            context.objectStoreUrl(),
            null
        )) {
            builder.withSettingsJson(settingsJson)
                .withSstBlockSize(SlateDb.SstBlockSize.KIB_4);
            try (SlateDb db = builder.build()) {
                byte[] key = "builder-key".getBytes(StandardCharsets.UTF_8);
                byte[] value = "builder-value".getBytes(StandardCharsets.UTF_8);
                db.put(key, value);
                Assertions.assertArrayEquals(value, db.get(key));
            }
        }
    }

    @Test
    void putGetDeleteWithOptionsAndMetrics() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key = "opts-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "opts-value".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            db.put(key, value, SlateDb.PutOptions.noExpiry(), new SlateDb.WriteOptions(false));
            SlateDb.ReadOptions readOptions = SlateDb.ReadOptions.builder()
                .durabilityFilter(SlateDb.Durability.MEMORY)
                .dirty(false)
                .cacheBlocks(true)
                .build();
            Assertions.assertArrayEquals(value, db.get(key, readOptions));

            db.delete(key, new SlateDb.WriteOptions(false));
            Assertions.assertNull(db.get(key));

            String metrics = db.metrics();
            Assertions.assertNotNull(metrics);
            Assertions.assertTrue(metrics.startsWith("{"));
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

            SlateDb.ScanOptions scanOptions = SlateDb.ScanOptions.builder()
                .durabilityFilter(SlateDb.Durability.MEMORY)
                .readAheadBytes(1)
                .cacheBlocks(false)
                .maxFetchTasks(1)
                .build();

            try (SlateDb.ScanIterator iter = db.scan(null, null, scanOptions)) {
                SlateDb.KeyValue first = iter.next();
                Assertions.assertNotNull(first);

                iter.seek(keyB);
                SlateDb.KeyValue afterSeek = iter.next();
                Assertions.assertNotNull(afterSeek);
                Assertions.assertArrayEquals(keyB, afterSeek.key());
                Assertions.assertArrayEquals(valueB, afterSeek.value());
            }

            try (SlateDb.ScanIterator iter = db.scanPrefix("scan-".getBytes(StandardCharsets.UTF_8))) {
                int count = 0;
                while (iter.next() != null) {
                    count++;
                }
                Assertions.assertEquals(3, count);
            }
        }
    }

    @Test
    void readerGetAndScan() throws Exception {
        TestSupport.ensureNativeReady();
        TestSupport.DbContext context = TestSupport.createDbContext();

        byte[] key = "reader-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "reader-value".getBytes(StandardCharsets.UTF_8);

        try (SlateDb db = SlateDb.open(context.dbPath().toAbsolutePath().toString(), context.objectStoreUrl(), null)) {
            db.put(key, value);
            db.flush();
        }

        SlateDb.ReaderOptions readerOptions = SlateDb.ReaderOptions.builder()
            .manifestPollInterval(Duration.ofMillis(200))
            .checkpointLifetime(Duration.ofSeconds(1))
            .maxMemtableBytes(1024 * 1024)
            .skipWalReplay(false)
            .build();

        try (SlateDb.SlateDbReader reader = SlateDb.openReader(
            context.dbPath().toAbsolutePath().toString(),
            context.objectStoreUrl(),
            null,
            null,
            readerOptions
        )) {
            Assertions.assertArrayEquals(value, reader.get(key));

            try (SlateDb.ScanIterator iter = reader.scanPrefix("reader-".getBytes(StandardCharsets.UTF_8))) {
                SlateDb.KeyValue kv = iter.next();
                Assertions.assertNotNull(kv);
                Assertions.assertArrayEquals(key, kv.key());
                Assertions.assertArrayEquals(value, kv.value());
            }
        }
    }
}
